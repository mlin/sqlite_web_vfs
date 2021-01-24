#pragma once

#include "HTTP.h"
#include "SQLiteVFS.h"
#include "ThreadPool.h"

using std::endl;
#ifndef NDEBUG
#define DBG std::cerr << __FILE__ << ":" << __LINE__ << ": "
#else
#define DBG false && std::cerr
#endif

class WebFile : public SQLiteVFS::File {
    std::string uri_;
    unsigned long long file_size_;
    std::unique_ptr<HTTP::CURLpool> curlpool_;

    int Read(void *zBuf, int iAmt, sqlite3_int64 iOfst) override {
        if (iAmt == 0) {
            return SQLITE_OK;
        }
        try {
            HTTP::headers reqhdrs, reshdrs;
            std::ostringstream fmt_range;
            fmt_range << "bytes=" << iOfst << "-" << (iOfst + iAmt - 1);
            reqhdrs["range"] = fmt_range.str();

            long status = -1;
            std::ostringstream bodystream; // TODO: write directly into zBuf
            auto rc = HTTP::Get(uri_, reqhdrs, status, reshdrs, bodystream, curlpool_.get());
            if (rc != CURLE_OK || status < 200 || status >= 300) {
                return SQLITE_IOERR_READ;
            }
            auto body = bodystream.str();
            if (body.size() != iAmt) {
                return SQLITE_IOERR_SHORT_READ;
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
    WebFile(const std::string &uri, unsigned long long file_size,
            std::unique_ptr<HTTP::CURLpool> &&curlpool)
        : uri_(uri), file_size_(file_size), curlpool_(std::move(curlpool)) {
        methods_.iVersion = 1;
    }
    virtual ~WebFile() = default;
};

class WebVFS : public SQLiteVFS::Wrapper {
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

            // HEAD request to determine the database file's existence & size
            HTTP::headers reqhdrs, reshdrs;
            long status = -1;
            CURLcode rc = HTTP::Head(uri, reqhdrs, status, reshdrs, curlpool.get());
            if (rc != CURLE_OK) {
                last_error_ += "HEAD failed: ";
                last_error_ += curl_easy_strerror(rc);
                return SQLITE_IOERR_READ;
            }
            if (status < 200 || status >= 300) {
                return SQLITE_CANTOPEN;
            }

            // parse content-length
            auto size_it = reshdrs.find("content-length");
            if (size_it == reshdrs.end()) {
                last_error_ = "HTTP HEAD response lacking content-length header";
                return SQLITE_IOERR_READ;
            }
            const char *size_str = size_it->second.c_str();
            char *endptr = nullptr;
            errno = 0;
            unsigned long long file_size = strtoull(size_str, &endptr, 10);
            if (errno || endptr != size_str + strlen(size_str)) {
                last_error_ = "HTTP HEAD response with unreadable content-length header";
                return SQLITE_IOERR_READ;
            }

            // Instantiate WebFile; caller will be responsible for calling xClose() on it, which
            // will make it self-delete.
            auto webfile = new WebFile(uri, file_size, std::move(curlpool));
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
};
