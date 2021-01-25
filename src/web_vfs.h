#pragma once

#include "HTTP.h"
#include "SQLiteVFS.h"
#include "ThreadPool.h"

namespace WebVFS {

using std::cerr;
using std::endl;

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

class File : public SQLiteVFS::File {
    std::string uri_, filename_;
    sqlite_int64 file_size_;
    std::unique_ptr<HTTP::CURLpool> curlpool_;
    unsigned long log_level_ = 1;

    int Read(void *zBuf, int iAmt, sqlite3_int64 iOfst) override {
        if (iAmt == 0) {
            return SQLITE_OK;
        }
        Timer t;
        try {
            HTTP::headers reqhdrs, reshdrs;
            std::ostringstream fmt_range;
            fmt_range << "bytes=" << iOfst << "-" << (iOfst + iAmt - 1);
            reqhdrs["range"] = fmt_range.str();

            long status = -1;
            bool retried = false;
            std::string body;
            HTTP::RetryOptions options;
            options.min_response_body = iAmt;
            options.connpool = curlpool_.get();
            options.on_retry = [&retried](HTTP::Method method, const std::string &url,
                                          const HTTP::headers &request_headers, CURLcode rc,
                                          long response_code, const HTTP::headers &response_headers,
                                          const std::string &response_body,
                                          unsigned int attempt) { retried = true; };
            auto rc = HTTP::RetryGet(uri_, reqhdrs, status, reshdrs, body, options);
            if (rc != CURLE_OK) {
                if (log_level_) {
                    cerr << "HTTP GET " << filename_ << ' ' << reqhdrs["range"] << ' '
                         << curl_easy_strerror(rc) << endl;
                }
                return SQLITE_IOERR_READ;
            }
            if (status < 200 || status >= 300) {
                if (log_level_) {
                    cerr << "HTTP GET " << filename_ << ' ' << reqhdrs["range"]
                         << " error status = " << status << endl;
                }
                return SQLITE_IOERR_READ;
            }
            if (body.size() != iAmt) {
                if (log_level_) {
                    cerr << "HTTP GET " << filename_ << ' ' << reqhdrs["range"]
                         << " incorrect response body length = " << body.size()
                         << ", expected = " << iAmt << endl;
                }
                return SQLITE_IOERR_SHORT_READ;
            }
            if (log_level_ > 1) {
                cerr << "HTTP GET " << filename_ << ' ' << reqhdrs["range"] << " OK ("
                     << (t.micros() / 1000) << "ms)" << endl;
            } else if (log_level_ && retried) {
                cerr << "HTTP GET " << filename_ << ' ' << reqhdrs["range"] << " OK after retry ("
                     << (t.micros() / 1000) << "ms)" << endl;
            }

            memcpy(zBuf, body.c_str(), iAmt);
            return SQLITE_OK;
        } catch (std::bad_alloc &) {
            return SQLITE_IOERR_NOMEM;
        } catch (std::exception &) {
            return SQLITE_IOERR_READ;
        }
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
    File(const std::string &uri, const std::string &filename, sqlite_int64 file_size,
         std::unique_ptr<HTTP::CURLpool> &&curlpool, unsigned long log_level = 1)
        : uri_(uri), filename_(filename), file_size_(file_size), curlpool_(std::move(curlpool)),
          log_level_(log_level) {
        methods_.iVersion = 1;
    }
    virtual ~File() = default;
};

class VFS : public SQLiteVFS::Wrapper {
  protected:
    std::string last_error_;

    int Open(const char *zName, sqlite3_file *pFile, int flags, int *pOutFlags) override {
        if (!zName || strcmp(zName, "/__web__")) {
            return SQLiteVFS::Wrapper::Open(zName, pFile, flags, pOutFlags);
        }
        const char *encoded_uri = sqlite3_uri_parameter(zName, "web_uri");
        if (!encoded_uri || !encoded_uri[0]) {
            last_error_ = "set web_uri query parameter to URI-encoded URI";
            return SQLITE_CANTOPEN;
        }
        if (!(flags & SQLITE_OPEN_READONLY)) {
            last_error_ = "web access is read-only";
            return SQLITE_CANTOPEN;
        }
        unsigned long log_level = 1;
        const char *env_log = getenv("SQLITE_WEB_LOG");
        if (env_log && *env_log) {
            log_level = strtoul(env_log, nullptr, 10);
            if (log_level == ULONG_MAX) {
                log_level = 1;
            }
        }

        // get desired URI
        try {
            std::string uri;
            std::unique_ptr<HTTP::CURLpool> curlpool;
            curlpool.reset(new HTTP::CURLpool(4));
            auto conn = curlpool->checkout();
            if (!conn->unescape(encoded_uri, uri)) {
                last_error_ = "Failed URI-decoding web_uri";
                return SQLITE_CANTOPEN;
            }
            curlpool->checkin(conn);
            std::string filename = JustFileName(uri);
            Timer t;

            // HEAD request to determine the database file's existence & size
            HTTP::headers reqhdrs, reshdrs;
            long status = -1;
            HTTP::RetryOptions options;
            options.connpool = curlpool.get();
            CURLcode rc = HTTP::RetryHead(uri, reqhdrs, status, reshdrs, options);
            if (rc != CURLE_OK) {
                last_error_ = "HTTP HEAD " + filename + ": ";
                last_error_ += curl_easy_strerror(rc);
                if (log_level) {
                    cerr << last_error_ << endl;
                }
                return SQLITE_IOERR_READ;
            }
            if (status < 200 || status >= 300) {
                last_error_ =
                    "HTTP HEAD " + filename + ": error status = " + std::to_string(status);
                if (log_level) {
                    cerr << last_error_ << endl;
                }
                return SQLITE_CANTOPEN;
            }

            // parse content-length
            long long file_size = HTTP::ReadContentLengthHeader(reshdrs);
            if (file_size < 0) {
                last_error_ =
                    "HTTP HEAD " + filename + ":response lacking valid content-length header";
                if (log_level) {
                    cerr << last_error_ << endl;
                }
                return SQLITE_IOERR_READ;
            }
            if (log_level > 1) {
                cerr << "HTTP HEAD " << filename << " content-length: " << file_size << " ("
                     << (t.micros() / 1000) << "ms)" << endl;
            }

            // Instantiate WebFile; caller will be responsible for calling xClose() on it, which
            // will make it self-delete.
            auto webfile = new File(uri, filename, file_size, std::move(curlpool), log_level);
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

    std::string JustFileName(const std::string &uri) {
        std::string ans = uri;
        auto p = ans.find('?');
        if (p != std::string::npos) {
            ans = ans.substr(0, p);
        }
        p = ans.rfind('/');
        if (p != std::string::npos) {
            ans = ans.substr(p + 1);
        }
        return ans;
    }
};

} // namespace WebVFS
