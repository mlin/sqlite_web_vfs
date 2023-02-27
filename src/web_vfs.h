#pragma once

#include "HTTP.h"
#include "SQLiteVFS.h"
#include "ThreadPool.h"
#include <future>
#include <set>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace WebVFS {

#include "dbi.h"

class Timer {
    unsigned long long t0_;

  public:
    Timer() {
        timeval t0;
        gettimeofday(&t0, nullptr);
        t0_ = t0.tv_sec * 1000000ULL + t0.tv_usec;
    }

    unsigned long long micros() {
        timeval tv;
        gettimeofday(&tv, nullptr);
        return tv.tv_sec * 1000000ULL + tv.tv_usec - t0_;
    }
};

// Extent: a contiguous portion of the remote file which we keep cached in memory. Three size
// options to balance latency, throughput, read amplification, HTTP request count.
struct Extent {
    enum Size { SM, MD, LG };

    size_t small_KiB;
    Size size;
    size_t rank;

    static size_t Bytes(Size sz, size_t small_KiB) {
        switch (sz) {
        case Size::SM:
            return small_KiB << 10;
        case Size::MD:
            return small_KiB << 14;
        default:
            break;
        }
        return small_KiB << 18;
    }

    Extent(Size size_, size_t rank_, size_t small_KiB_)
        : small_KiB(small_KiB_), size(size_), rank(rank_) {}
    Extent() : small_KiB(64), size(Size::SM), rank(0) {} // don't use; for STL

    size_t Bytes(Size sz) const { return Bytes(sz, small_KiB); }

    bool operator<(const Extent &rhs) const {
        assert(rhs.small_KiB == small_KiB);
        return size < rhs.size || (size == rhs.size && rank < rhs.rank);
    }

    Extent Prev() const {
        if (rank == 0) {
            throw std::runtime_error("illegal Extent::Prev()");
        }
        return Extent(size, rank - 1, small_KiB);
    }

    Extent Next() const { return Extent(size, rank + 1, small_KiB); }

    Extent Promote() const {
        if (size == Size::SM) {
            return Extent(Size::MD, rank / 16, small_KiB);
        }
        if (size == Size::MD) {
            return Extent(Size::LG, rank / 16, small_KiB);
        }
        return *this;
    }

    uint64_t Offset() const { return Bytes(size) * rank; }

    size_t Bytes() const { return Bytes(size); }

    bool Contains(uint64_t offset, size_t length) const {
        return offset >= Offset() && offset + length <= Offset() + Bytes();
    }

    bool Contains(const Extent &rhs) const { return Contains(rhs.Offset(), rhs.Bytes()); }

    bool Exists(uint64_t file_size_) const { return Offset() < file_size_; }

    std::string str(uint64_t file_size) const {
        auto hi = std::min((Offset() + Bytes() - 1), file_size - 1);
        std::ostringstream fmt_range;
        fmt_range << "bytes=" << Offset() << "-" << hi;
        return fmt_range.str();
    }
};

class File : public SQLiteVFS::File {
    const std::string uri_, filename_;
    const sqlite_int64 file_size_;
    const size_t small_KiB_;
    const std::unique_ptr<HTTP::CURLpool> curlpool_;
    std::unique_ptr<dbiHelper> dbi_;

    // Extents cached for potential reuse, with a last-use timestamp for LRU eviction.
    // Note: The purpose of keeping extents cached is to anticipate future Read requests for nearby
    //       database pages. It is NOT to serve repeat Reads for the same page, which is the job of
    //       the SQLite page cache in front of the VFS.
    using shared_string = std::shared_ptr<const std::string>;
    struct ResidentExtent {
        const Extent extent;
        shared_string data;
        uint64_t seqno;
        bool used = false; // true after a prefetched extent is actually used
        ResidentExtent(Extent extent_, const shared_string &data_, uint64_t seqno_)
            : extent(extent_), data(data_), seqno(seqno_) {}
    };
    std::map<Extent, ResidentExtent> resident_;
    std::map<uint64_t, Extent> usage_; // secondary index of resident_ ordered by seqno (for LRU)
    uint64_t extent_seqno_ = 0;        // timestamp
    ThreadPoolWithEnqueueFast threadpool_;

    // The main thread locks mu_ to update resident_ and usage_, but may read them without.
    // Background threads may (only) read them with mu_ locked.
    // The other state below always requires mu_ to read or write.
    std::mutex mu_;

#define SQLITE_WEB_LOG(msg_level, msg)                                                             \
    if (log_.level() >= msg_level) {                                                               \
        std::unique_lock<std::mutex> log_lock(mu_);                                                \
        SQLITE_VFS_LOG(msg_level, '[' << filename_ << "] " << msg)                                 \
    }

    // state for [pre]fetch on background threads
    std::deque<Extent> fetch_queue_;             // queue of fetch ops
    std::map<Extent, size_t> fetch_queue2_;      // secondary index of fetch_queue_
    std::set<Extent> fetch_wip_;                 // fetch ops currently underway
    std::map<Extent, shared_string> fetch_done_; // completed fetches waiting to be merged
    std::map<Extent, int> fetch_error_;          // error codes
    std::condition_variable fetch_cv_;           // on add to fetch_done_ or fetch_error_

    // performance counters
    uint64_t read_count_ = 0, dbi_read_count_ = 0, fetch_count_ = 0, ideal_prefetch_count_ = 0,
             wasted_prefetch_count_ = 0, read_bytes_ = 0, fetch_bytes_ = 0, stalled_micros_ = 0;

    // Run HTTP GET request for an extent
    // mu_ MUST NOT be locked for this (otherwise it'll hang, due to logging)
    int FetchExtent(Extent extent, shared_string &data) {
        Timer t;
        const std::string protocol = uri_.substr(0, 6) == "https:" ? "HTTPS" : "HTTP";
        try {
            HTTP::headers reqhdrs, reshdrs;
            reqhdrs["range"] = extent.str(file_size_);
            SQLITE_WEB_LOG(5, protocol << " GET " << reqhdrs["range"] << " ...")

            long status = -1;
            bool retried = false;
            std::shared_ptr<std::string> body(new std::string());
            HTTP::RetryOptions options;
            options.min_response_body =
                std::min(uint64_t(extent.Bytes()), uint64_t(file_size_ - extent.Offset()));
            options.connpool = curlpool_.get();
            options.on_retry = [&](HTTP::Method method, const std::string &url,
                                   const HTTP::headers &request_headers, CURLcode rc,
                                   long response_code, const HTTP::headers &response_headers,
                                   const std::string &response_body, unsigned int attempt) {
                retried = true;
                if (log_.level() >= 3) {
                    std::string msg = curl_easy_strerror(rc);
                    if (rc == CURLE_OK) {
                        if (response_code < 200 || response_code >= 300) {
                            msg = "status = " + std::to_string(response_code);
                        } else {
                            msg = "unexpected response body size " +
                                  std::to_string(response_body.size()) +
                                  " != " + std::to_string(options.min_response_body);
                        }
                    }
                    SQLITE_WEB_LOG(3, protocol << " GET " << reqhdrs["range"] << " retrying " << msg
                                               << " (attempt " << attempt << " of "
                                               << options.max_tries << "; " << (t.micros() / 1000)
                                               << "ms elapsed)")
                }
            };
            auto rc = HTTP::RetryGet(uri_, reqhdrs, status, reshdrs, *body, options);
            if (rc != CURLE_OK) {
                SQLITE_WEB_LOG(1, protocol << " GET " << reqhdrs["range"] << ' '
                                           << curl_easy_strerror(rc) << " (" << (t.micros() / 1000)
                                           << "ms)")
                return SQLITE_IOERR_READ;
            }
            if (status < 200 || status >= 300) {
                SQLITE_WEB_LOG(1, protocol << " GET " << reqhdrs["range"] << " error status = "
                                           << status << " (" << (t.micros() / 1000) << "ms)")
                return SQLITE_IOERR_READ;
            }
            if (body->size() != options.min_response_body) {
                SQLITE_WEB_LOG(1, protocol << " GET " << reqhdrs["range"]
                                           << " incorrect response body length = " << body->size()
                                           << ", expected = " << extent.Bytes())
                return SQLITE_IOERR_SHORT_READ;
            }
            data = body;
            if (log_.level() >= 4) {
                SQLITE_WEB_LOG(4, protocol << " GET " << reqhdrs["range"] << " OK ("
                                           << data->size() / 1024 << "KiB, " << (t.micros() / 1000)
                                           << "ms)")
            } else if (log_.level() >= 2 && retried) {
                SQLITE_WEB_LOG(4, protocol << " GET " << reqhdrs["range"] << " OK after retry ("
                                           << data->size() / 1024 << "KiB, " << (t.micros() / 1000)
                                           << "ms)")
            }
            return SQLITE_OK;
        } catch (std::bad_alloc &) {
            return SQLITE_IOERR_NOMEM;
        } catch (std::exception &exn) {
            SQLITE_WEB_LOG(1, protocol << " GET: " << exn.what());
            return SQLITE_IOERR_READ;
        }
    }

    static void *FetchJob(void *v) {
        File *self = (File *)v;

        std::unique_lock<std::mutex> lock(self->mu_);

        // dequeue a desired extent
        if (self->fetch_queue_.empty()) {
            return nullptr;
        }
        Extent extent = self->fetch_queue_.front();
        self->fetch_queue_.pop_front();
        auto fq2c = self->fetch_queue2_.find(extent);
        assert(fq2c != self->fetch_queue2_.end() && fq2c->second);
        fq2c->second -= 1;
        if (!fq2c->second) {
            self->fetch_queue2_.erase(extent);
        }

        // coalesce request if same extent or one containing it is already wip or done
        Extent container = extent;
        for (int i = 0; i < 3; ++i) {
            if (self->resident_.find(container) != self->resident_.end() ||
                self->fetch_wip_.find(container) != self->fetch_wip_.end() ||
                self->fetch_done_.find(container) != self->fetch_done_.end()) {
                return nullptr;
            }
            container = container.Promote();
        }

        // run HTTP GET
        self->fetch_wip_.insert(extent);
        lock.unlock();
        shared_string buf;
        int rc = self->FetchExtent(extent, buf);
        lock.lock();

        // record result
        self->fetch_wip_.erase(extent);
        assert(self->fetch_done_.find(extent) == self->fetch_done_.end() &&
               self->fetch_error_.find(extent) == self->fetch_error_.end());
        if (rc == SQLITE_OK) {
            self->fetch_count_++;
            self->fetch_bytes_ += buf->size();
            self->fetch_done_.emplace(extent, buf);
        } else {
            self->fetch_error_[extent] = rc;
        }
        lock.unlock();
        self->fetch_cv_.notify_all();

        return nullptr;
    }

    void EnqueueFetch(std::unique_lock<std::mutex> &lock, Extent extent, bool front = false) {
        assert(lock.owns_lock());
        auto fq2c = fetch_queue2_.find(extent);
        if (front) {
            fetch_queue_.push_front(extent);
        } else if (fq2c == fetch_queue2_.end()) {
            fetch_queue_.push_back(extent);
        } else {
            return;
        }
        if (fq2c == fetch_queue2_.end()) {
            fetch_queue2_[extent] = 1;
        } else {
            fq2c->second += 1;
        }
        threadpool_.EnqueueFast(this, FetchJob, nullptr);
    }

    void UpdateResident(std::unique_lock<std::mutex> &lock) {
        // main thread collects results of recent background fetch jobs
        assert(lock.owns_lock());

        // surface any errors recorded
        auto err = fetch_error_.begin();
        if (err != fetch_error_.end()) {
            int rc = err->second;
            fetch_error_.erase(err);
            throw rc;
        }

        // merge successfully fetched extents into resident_ (& update usage_)
        for (auto p = fetch_done_.begin(); p != fetch_done_.end(); p = fetch_done_.erase(p)) {
            Extent extent = p->first;
            if (resident_.find(extent) == resident_.end()) {
                usage_[++extent_seqno_] = extent;
                resident_.emplace(extent, ResidentExtent(extent, p->second, extent_seqno_));
            }
        }
    }

    Extent ExtentContaining(uint64_t offset, size_t length) {
        auto rk = offset / Extent::Bytes(Extent::Size::SM, small_KiB_);
        auto hi = offset + std::max(length, size_t(1)) - 1;
        if (rk == hi / Extent::Bytes(Extent::Size::SM, small_KiB_)) {
            return Extent(Extent::Size::SM, rk, small_KiB_);
        }
        throw std::runtime_error("unaligned Read");
    }

    std::map<Extent, ResidentExtent>::iterator FindResidentExtent(uint64_t offset, size_t length) {
        Extent extent = ExtentContaining(offset, length), extent2 = extent.Promote(),
               extent3 = extent2.Promote();
        // prefer largest
        auto line = resident_.find(extent3);
        if (line != resident_.end())
            return line;
        line = resident_.find(extent2);
        if (line != resident_.end())
            return line;
        line = resident_.find(extent);
        return line;
    }

    bool ResidentAndUsed(Extent extent) {
        auto p = resident_.find(extent);
        return p != resident_.end() && p->second.used;
    }

    void EvictResident(std::unique_lock<std::mutex> &lock, size_t n) {
        assert(lock.owns_lock());
        while (resident_.size() > n) {
            auto lru = usage_.begin();
            auto lru_line = resident_.find(lru->second);
            assert(lru_line != resident_.end());
            if (!lru_line->second.used) {
                wasted_prefetch_count_++;
            }
            resident_.erase(lru_line);
            usage_.erase(lru->first);
        }
    }

    ResidentExtent EnsureResidentExtent(uint64_t offset, size_t length) {
        auto last_used = usage_.rbegin();
        if (last_used != usage_.rend() && last_used->second.Contains(offset, length)) {
            // most-recently used extent still has the page we need (hot path, doesn't lock mu_)
            auto p = resident_.find(last_used->second);
            assert(p != resident_.end());
            return p->second;
        }

        std::unique_lock<std::mutex> lock(mu_);
        // find a resident extent containing desired page
        UpdateResident(lock);
        auto line = FindResidentExtent(offset, length);
        bool blocked = false;

        if (line == resident_.end()) {
            // needed extent not resident: front-enqueue job to fetch it
            Extent extent = ExtentContaining(offset, length);
            // promote up to medium if we already used the previous small or medium extent
            Extent container = extent.Promote();
            if ((extent.rank > 0 && ResidentAndUsed(extent.Prev())) ||
                ResidentAndUsed(extent.Next()) ||
                (container.rank > 0 && ResidentAndUsed(container.Prev())) ||
                ResidentAndUsed(container.Next())) {
                extent = container;
            }
            EnqueueFetch(lock, extent, true);
            // wait for it
            do {
                fetch_cv_.wait(lock);
                UpdateResident(lock);
            } while ((line = FindResidentExtent(offset, length)) == resident_.end());
            blocked = true;
        }

        Extent extent = line->first;
        ResidentExtent &res = line->second;
        assert(!(extent < res.extent || res.extent < extent));
        // LRU bookkeeping: delete old seqno_ from the secondary index, then record the new one
        assert(usage_.find(res.seqno) != usage_.end());
        usage_.erase(res.seqno); // old seqno
        usage_[++extent_seqno_] = extent;
        res.seqno = extent_seqno_;
        if (!blocked && !res.used) {
            ideal_prefetch_count_++;
        }
        res.used = true;
        // now evict LRU extents if needed
        EvictResident(lock, 32);

        // if appropriate, initiate prefetch of nearby/surrounding extents
        if (extent.size < Extent::Size::LG) {
            // if we used the previous or next extent of the same size, initiate prefetch of the
            // next size up
            if ((extent.rank > 0 && ResidentAndUsed(extent.Prev())) ||
                (ResidentAndUsed(extent.Next()))) {
                EnqueueFetch(lock, extent.Promote());
            }
        } else {
            // large size: prefetch up to 4 subsequent/preceding extents if we seem to have
            // momentum in a contiguous scan
            Extent pred = extent, succ = extent;
            for (int ofs = 0; ofs < 4 && pred.rank; ++ofs) {
                pred = pred.Prev();
                succ = succ.Next();
                if (ResidentAndUsed(pred) && succ.Exists(file_size_)) {
                    EnqueueFetch(lock, succ);
                } else {
                    break;
                }
            }
            pred = extent;
            succ = extent;
            for (int ofs = 0; ofs < 4 && pred.rank; ++ofs) {
                pred = pred.Prev();
                succ = succ.Next();
                if (ResidentAndUsed(succ)) {
                    EnqueueFetch(lock, pred);
                } else {
                    break;
                }
            }
        }

        return res;
    }

    int Read(void *zBuf, int iAmt, sqlite3_int64 iOfst) override {
        Timer t;
        if (iAmt < 0 || iOfst < 0) {
            return SQLITE_IOERR_READ;
        }
        try {
            int dbi_rc = SQLITE_NOTFOUND;
            if (dbi_) {
                // shortcut: serve page from .dbi if available
                dbi_rc = dbi_->Seek(iOfst);
                if (dbi_rc == SQLITE_OK) {
                    if (dbi_->PageSize() >= iAmt) {
                        memcpy(zBuf, dbi_->PageData(), iAmt);
                        dbi_read_count_++;
                    } else {
                        SQLITE_WEB_LOG(1, "unexpected page size " << dbi_->PageSize()
                                                                  << " in .dbi  @ offset " << iOfst)
                    }
                } else if (dbi_rc != SQLITE_NOTFOUND) {
                    SQLITE_WEB_LOG(1, " failed reading page @ offset "
                                          << iOfst << " from .dbi: " << dbi_->GetLastError())
                }
            }
            if (dbi_rc != SQLITE_OK) {
                // main path
                ResidentExtent resext = EnsureResidentExtent(iOfst, iAmt);
                assert(resext.extent.Contains(iOfst, iAmt));
                assert(resext.data->size() + resext.extent.Offset() >= iOfst + iAmt);
                memcpy(zBuf, resext.data->c_str() + (iOfst - resext.extent.Offset()), iAmt);
            }
            read_count_++;
            read_bytes_ += iAmt;
            stalled_micros_ += t.micros();
            return SQLITE_OK;
        } catch (int rc) {
            return SQLITE_IOERR_READ;
        } catch (std::bad_alloc &) {
            return SQLITE_IOERR_NOMEM;
        }
    }

    int Close() override {
        {
            std::unique_lock<std::mutex> lock(mu_);
            fetch_queue_.clear();
            fetch_queue2_.clear();
            // TODO: abort ongoing requests via atomic<bool> passed into libcurl progress function
            lock.unlock();
            threadpool_.Barrier();
            lock.lock();
            UpdateResident(lock);
            EvictResident(lock, 0); // ensure we count wasted prefetches
        }
        if (log_.level() >= 4) {
            std::ostringstream msg;
            msg << "page reads: " << read_count_;
            if (dbi_) {
                msg << " (from .dbi: " << dbi_read_count_ << ")";
            }
            SQLITE_WEB_LOG(
                4, msg.str() << ", HTTP GETs: " << fetch_count_ << " (prefetches ideal: "
                             << ideal_prefetch_count_ << ", wasted: " << wasted_prefetch_count_
                             << "), bytes read / downloaded / filesize: " << read_bytes_ << " / "
                             << fetch_bytes_ << " / " << file_size_ << ", stalled for "
                             << (stalled_micros_ / 1000)
                             << "ms; total connections: " << curlpool_->cumulative_connections())
        }
        // deletes this:
        return SQLiteVFS::File::Close();
    }

    int Write(const void *zBuf, int iAmt, sqlite3_int64 iOfst) override { return SQLITE_MISUSE; }
    int Truncate(sqlite3_int64 size) override { return SQLITE_MISUSE; }
    int Sync(int flags) override { return SQLITE_MISUSE; }
    int FileSize(sqlite3_int64 *pSize) override {
        *pSize = (sqlite3_int64)file_size_;
        return SQLITE_OK;
    }
    int Lock(int eLock) override { return SQLITE_OK; }
    int Unlock(int eLock) override { return SQLITE_OK; }
    int CheckReservedLock(int *pResOut) override {
        *pResOut = 0;
        return SQLITE_OK;
    }
    int FileControl(int op, void *pArg) override { return SQLITE_NOTFOUND; }
    int SectorSize() override { return 0; }
    int DeviceCharacteristics() override { return SQLITE_IOCAP_IMMUTABLE; }

    int ShmMap(int iPg, int pgsz, int isWrite, void volatile **pp) override {
        return SQLITE_MISUSE;
    }
    int ShmLock(int offset, int n, int flags) override { return SQLITE_MISUSE; }
    void ShmBarrier() override {}
    int ShmUnmap(int deleteFlag) override { return SQLITE_MISUSE; }

    int Fetch(sqlite3_int64 iOfst, int iAmt, void **pp) override { return SQLITE_MISUSE; }
    int Unfetch(sqlite3_int64 iOfst, void *p) override { return SQLITE_MISUSE; }

  public:
    File(const char *zName, const std::string &uri, const std::string &filename,
         sqlite_int64 file_size, std::unique_ptr<HTTP::CURLpool> &&curlpool,
         std::unique_ptr<dbiHelper> &&dbi, size_t small_KiB)
        : SQLiteVFS::File(zName), uri_(uri), filename_(filename), file_size_(file_size),
          curlpool_(std::move(curlpool)), dbi_(std::move(dbi)), threadpool_(4, 16),
          small_KiB_(small_KiB) {
        methods_.iVersion = 1;
        log_.DetectLevel(zName, 2);
    }
};

class VFS : public SQLiteVFS::Wrapper {
  protected:
    std::string last_error_;

    int Open(const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags) override {
        if (!zName || strcmp(zName, "/__web__")) {
            return wrapped_->xOpen(wrapped_, zName, pFile, flags, pOutFlags);
        }

        const char *encoded_uri = sqlite3_uri_parameter(zName, "web_url");
        if (!encoded_uri || !encoded_uri[0]) {
            last_error_ = "set web_url query parameter to percent-encoded URI";
            return SQLITE_CANTOPEN;
        }
        if (!(flags & SQLITE_OPEN_READONLY)) {
            last_error_ = "web access is read-only";
            return SQLITE_CANTOPEN;
        }
        SQLiteVFS::Logger log_(zName, 2); // intentionally shadow this->log_
        bool insecure = sqlite3_uri_int64(zName, "web_insecure", 0) == 1;
        const char *env_insecure = getenv("SQLITE_WEB_INSECURE");
        if (env_insecure && *env_insecure) {
            errno = 0;
            unsigned long env_insecure_i = strtoul(env_insecure, nullptr, 10);
            if (errno == 0 && env_insecure_i == 1) {
                insecure = true;
            }
        }
        auto small_KiB = sqlite3_uri_int64(zName, "web_small_KiB", 64);
        const char *env_small_KiB = getenv("SQLITE_WEB_SMALL_KIB");
        if (env_small_KiB && *env_small_KiB) {
            errno = 0;
            long long env_small_KiB_i = strtoll(env_small_KiB, nullptr, 10);
            if (errno == 0) {
                small_KiB = env_small_KiB_i;
            }
        }
        small_KiB = std::max(1LL, small_KiB);
        bool no_dbi = sqlite3_uri_boolean(zName, "web_nodbi", 0);
        const char *env_nodbi = getenv("SQLITE_WEB_NODBI");
        if (env_nodbi && *env_nodbi) {
            errno = 0;
            unsigned long env_nodbi_i = strtoul(env_nodbi, nullptr, 10);
            if (errno == 0 && env_nodbi_i > 0) {
                no_dbi = true;
            }
        }
        const char *encoded_dbi_uri =
            no_dbi ? nullptr : sqlite3_uri_parameter(zName, "web_dbi_url");

        SQLITE_VFS_LOG(5, "Load & init libcurl ...")
        int rc = HTTP::global_init();
        if (rc != CURLE_OK) {
            if (rc == CURLE_NOT_BUILT_IN) {
                last_error_ = "failed to load required symbols from libcurl; try upgrading libcurl";
            } else {
                last_error_ = "failed to load libcurl";
            }
            SQLITE_VFS_LOG(1, last_error_);
            return SQLITE_ERROR;
        }
        SQLITE_VFS_LOG(4, "Load & init libcurl OK")

        // get desired URI
        try {
            std::string uri, dbi_uri, reencoded_dbi_uri;
            std::unique_ptr<HTTP::CURLpool> curlpool;
            curlpool.reset(new HTTP::CURLpool(4, insecure));
            auto conn = curlpool->checkout();
            if (!conn->unescape(encoded_uri, uri)) {
                last_error_ = "Failed percent-decoding web_url";
                return SQLITE_CANTOPEN;
            }
            if (!no_dbi && encoded_dbi_uri && encoded_dbi_uri[0] &&
                !conn->unescape(encoded_dbi_uri, dbi_uri)) {
                last_error_ = "Failed percent-decoding web_dbi_url";
                return SQLITE_CANTOPEN;
            }

            SQLITE_VFS_LOG(3, "opening " << uri)

            // spawn background thread to sniff .dbi
            bool dbi_explicit = !dbi_uri.empty() && !no_dbi;
            if (!dbi_explicit && !no_dbi && uri.find('?') == std::string::npos) {
                dbi_uri = uri + ".dbi";
            }
            std::future<int> dbi_fut;
            std::unique_ptr<dbiHelper> dbi;
            std::string dbi_error;
            if (!dbi_uri.empty()) {
                dbi_fut = std::async(std::launch::async, [&] {
                    return dbiHelper::Open(conn.get(), dbi_uri, insecure, log_.level(), dbi,
                                           dbi_error);
                });
            }

            // read main database header
            unsigned long long file_size = 0, page_size = 0;
            std::string db_header;
            rc = FetchDatabaseHeader(log_, uri, curlpool.get(), file_size, page_size, db_header);
            if (rc != SQLITE_OK) {
                return rc;
            }
            // WebVFS::File logic assumes small extent is at least 1 page
            small_KiB = std::max(1LL, std::max((long long)page_size / 1024LL, small_KiB));

            // collect result of .dbi sniff
            int dbi_rc = SQLITE_NOTFOUND;
            if (dbi_fut.valid() && (dbi_rc = dbi_fut.get()) == SQLITE_OK) {
                // verify header match between main database & .dbi
                assert(dbi);
                std::string dbi_header;
                dbi_rc = dbi->MainDatabaseHeader(dbi_header);
                if (dbi_rc == SQLITE_OK && dbi_header != db_header) {
                    dbi_rc = SQLITE_CORRUPT;
                    dbi_error = ".dbi does not match main database file";
                    SQLITE_VFS_LOG(2, "[" << FileNameForLog(uri) << "] " << dbi_error << ": "
                                          << dbi_uri)
                }
            }
            if (dbi_rc != SQLITE_OK) {
                dbi.reset();
                if (dbi_error.empty()) {
                    dbi_error = sqlite3_errstr(dbi_rc);
                }
                if (!no_dbi && ((dbi_explicit && log_.level() >= 2) || log_.level() >= 3)) {
                    SQLITE_VFS_LOG(2, "[" << FileNameForLog(uri) << "] opened without .dbi ("
                                          << dbi_error << ")")
                }
            } else {
                SQLITE_VFS_LOG(3, "[" << FileNameForLog(uri) << "] opened with .dbi")
            }
            curlpool->checkin(conn);

            // Instantiate WebFile; caller will be responsible for calling xClose() on it, which
            // will make it self-delete.
            auto webfile = new File(zName, uri, FileNameForLog(uri), file_size, std::move(curlpool),
                                    std::move(dbi), size_t(small_KiB));
            webfile->InitHandle(pFile);
            *pOutFlags = flags;
            return SQLITE_OK;
        } catch (std::bad_alloc &) {
            return SQLITE_IOERR_NOMEM;
        }
    }

    int GetLastError(int nByte, char *zErrMsg) override {
        if (nByte && last_error_.size()) {
            strncpy(zErrMsg, last_error_.c_str(), nByte);
            zErrMsg[nByte - 1] = 0;
            return SQLITE_OK;
        }
        return SQLiteVFS::Wrapper::GetLastError(nByte, zErrMsg);
    }

    std::string FileNameForLog(const std::string &uri) {
        std::string ans = uri;
        auto p = ans.find('?');
        if (p != std::string::npos) {
            ans = ans.substr(0, p);
        }
        p = ans.rfind('/');
        if (p != std::string::npos) {
            ans = ans.substr(p + 1);
        }
        if (ans.size() > 97) {
            ans = ans.substr(0, 97) + "...";
        }
        return ans;
    }

    int FetchDatabaseHeader(SQLiteVFS::Logger &log_, const std::string &uri,
                            HTTP::CURLpool *connpool, unsigned long long &db_file_size,
                            unsigned long long &page_size, std::string &db_header) {
        // GET range: bytes=0-99 to read the database file's header and detect its size.
        db_file_size = page_size = 0;
        db_header.clear();
        Timer t;
        const std::string protocol = uri.substr(0, 6) == "https:" ? "HTTPS" : "HTTP",
                          filename = FileNameForLog(uri);
        HTTP::headers reqhdrs, reshdrs;
        reqhdrs["range"] = "bytes=0-99";

        SQLITE_VFS_LOG(5,
                       "[" << filename << "] " << protocol << " GET " << reqhdrs["range"] << " ...")

        long status = -1;
        HTTP::RetryOptions options;
        options.connpool = connpool;
        CURLcode rc = HTTP::RetryGet(uri, reqhdrs, status, reshdrs, db_header, options);
        if (rc != CURLE_OK) {
            last_error_ = "[" + filename + "] reading database header: ";
            last_error_ += curl_easy_strerror(rc);
            SQLITE_VFS_LOG(1, last_error_)
            return SQLITE_IOERR_READ;
        }
        if (status < 200 || status >= 300) {
            last_error_ = "[" + filename +
                          "] reading database header: error status = " + std::to_string(status);
            SQLITE_VFS_LOG(1, last_error_)
            return SQLITE_CANTOPEN;
        }
        if (db_header.size() != 100 ||
            db_header.substr(0, 16) != std::string("SQLite format 3\000", 16)) {
            last_error_ = "[" + filename + "] remote content isn't a SQLite3 database file";
            SQLITE_VFS_LOG(1, last_error_)
            return SQLITE_CORRUPT;
        }

        // parse content-range
        auto size_it = reshdrs.find("content-range");
        if (size_it != reshdrs.end()) {
            SQLITE_VFS_LOG(5, "[" << filename << "] " << protocol
                                  << " content-range: " << size_it->second)
            const std::string &cr = size_it->second;
            if (cr.substr(0, 6) == "bytes ") {
                auto slash = cr.find('/');
                if (slash != std::string::npos) {
                    std::string size_txt = cr.substr(slash + 1);
                    const char *size_str = size_txt.c_str();
                    char *endptr = nullptr;
                    errno = 0;
                    db_file_size = strtoull(size_str, &endptr, 10);
                    if (errno || endptr != size_str + size_txt.size()) {
                        db_file_size = 0;
                    }
                }
            }
        }
        if (!db_file_size) {
            last_error_ = "[" + filename + "] " + protocol +
                          " GET bytes=0-99: empty file or invalid content-range header";
            SQLITE_VFS_LOG(1, last_error_)
            return SQLITE_IOERR_READ;
        }

        // read the page size & page count from the header; their product should equal file size.
        // https://github.com/sqlite/sqlite/blob/8d889afc0d81839bde67731d14263026facc89d1/src/shell.c.in#L5451-L5485
        page_size = (uint8_t(db_header[16]) << 8) + uint8_t(db_header[17]);
        if (page_size == 1) {
            page_size = 65536;
        }
        uint32_t page_count = 0;
        for (int ofs = 28; ofs < 32; ++ofs) {
            page_count <<= 8;
            page_count += uint8_t(db_header[ofs]);
        }
        SQLITE_VFS_LOG(4, "[" << filename << "] database geometry detected: " << db_file_size
                              << " bytes = " << page_size << " bytes/page * " << page_count
                              << " pages (" << (t.micros() / 1000) << "ms)")
        if (page_size < 512 || page_size > 65536 || page_count == 0 ||
            db_file_size != page_size * page_count) {
            last_error_ = "[" + filename +
                          "] database corrupt or truncated; content-range file size doesn't "
                          "match in-header database size";
            SQLITE_VFS_LOG(1, last_error_)
            return SQLITE_CORRUPT;
        }
        return SQLITE_OK;
    }
};

} // namespace WebVFS
