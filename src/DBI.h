#pragma once

class DBI {
    std::shared_ptr<sqlite3> dbiconn_ = nullptr;
    std::shared_ptr<sqlite3_stmt> cursor_;
    std::string last_error_;

    DBI(std::shared_ptr<sqlite3> dbiconn) : dbiconn_(dbiconn) {}

    int Prepare(std::string &error) noexcept {
        // TODO: VACUUM INTO local temp file, reopen it
        // TODO: pragma integrity_check?
        int rc =
            sqlite3_exec(dbiconn_.get(), "pragma cache_size = -1048576", nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
            error = sqlite3_errmsg(dbiconn_.get());
            return rc;
        }
        sqlite3_stmt *raw_get_page = nullptr;
        rc = sqlite3_prepare_v3(dbiconn_.get(), "select data from web_dbi_pages where offset = ?",
                                -1, 0, &raw_get_page, nullptr);
        if (rc != SQLITE_OK) {
            error = "invalid .dbi (wrong internal schema)";
            return rc;
        }
        cursor_ = std::shared_ptr<sqlite3_stmt>(raw_get_page, sqlite3_finalize);
        return Seek(0);
    }

  public:
    virtual ~DBI() {}

    static int Open(HTTP::CURLconn *curlconn, const std::string &dbi_url, bool web_insecure,
                    int web_log, std::unique_ptr<DBI> &dbi, std::string &error) noexcept {
        error.clear();

        std::string open_uri = "?mode=ro&immutable=1";
        if (dbi_url.substr(0, 5) == "file:") {
            open_uri = dbi_url + open_uri;
        } else {
            std::string encoded_url;
            if (!curlconn->escape(dbi_url, encoded_url) || encoded_url.size() < dbi_url.size()) {
                error = "Failed percent-encoding dbi_url";
                return SQLITE_CANTOPEN;
            }

            open_uri = "file:/__web__" + open_uri + "&vfs=web&web_nodbi=1&web_url=" + encoded_url;

            if (web_insecure) {
                open_uri += "&web_insecure=1";
            }
            open_uri += "&web_gobig=1&web_log=" + std::to_string(web_log);
        }

        sqlite3 *raw_dbiconn = nullptr;
        int rc =
            sqlite3_open_v2(open_uri.c_str(), &raw_dbiconn,
                            SQLITE_OPEN_URI | SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, nullptr);
        if (rc != SQLITE_OK) {
            error = sqlite3_errstr(rc);
            return rc;
        }
        dbi.reset(new DBI(std::shared_ptr<sqlite3>(raw_dbiconn, sqlite3_close_v2)));
        rc = dbi->Prepare(error);
        if (rc != SQLITE_OK) {
            dbi.reset();
        }
        return SQLITE_OK;
    }

    std::string GetLastError() const noexcept { return last_error_; }

    // Seek cursor to the page at given offset; if successful, returns SQLITE_OK and page of size
    // PageSize() can be found at PageData().
    // Otherwise, PageSize() and PageData() are undefined; returns SQLITE_NOTFOUND if desired page
    // isn't present in DBI, or any other error code.
    int Seek(sqlite_int64 offset) noexcept {
        int rc = sqlite3_reset(cursor_.get());
        if (rc != SQLITE_OK) {
            last_error_ = sqlite3_errstr(rc);
            return rc;
        }
        rc = sqlite3_bind_int64(cursor_.get(), 1, offset);
        if (rc != SQLITE_OK) {
            last_error_ = sqlite3_errmsg(dbiconn_.get());
            return rc;
        }
        rc = sqlite3_step(cursor_.get());
        if (rc != SQLITE_ROW) {
            if (rc == SQLITE_DONE) {
                return SQLITE_NOTFOUND;
            }
            last_error_ = sqlite3_errmsg(dbiconn_.get());
            return SQLITE_ERROR;
        }
        if (sqlite3_column_type(cursor_.get(), 0) != SQLITE_BLOB) {
            last_error_ = "corrupt .dbi (invalid stored page blob)";
            return SQLITE_CORRUPT;
        }
        auto sz = sqlite3_column_bytes(cursor_.get(), 0);
        if (sz < 512 || sz > 65536) {
            last_error_ = "corrupt .dbi (invalid stored page size)";
            return SQLITE_CORRUPT;
        }
        return SQLITE_OK;
    }

    int PageSize() const noexcept { return sqlite3_column_bytes(cursor_.get(), 0); }

    const void *PageData() const noexcept { return sqlite3_column_blob(cursor_.get(), 0); }

    // Seek to page 1 (offset 0) and set header to its first 100 bytes
    int MainDatabaseHeader(std::string &header) noexcept {
        int rc = Seek(0);
        if (rc == SQLITE_NOTFOUND) {
            last_error_ = "corrupt .dbi (page 1 missing)";
            return SQLITE_CORRUPT;
        }
        if (rc != SQLITE_OK) {
            return rc;
        }
        assert(PageSize() > 100);
        header.assign((const char *)PageData(), 100);
        return SQLITE_OK;
    }
};
