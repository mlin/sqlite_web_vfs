#pragma once

class dbiHelper {
    std::shared_ptr<sqlite3> dbiconn_ = nullptr;
    std::shared_ptr<sqlite3_stmt> cursor_;
    std::string temp_dbifile_, last_error_;

    dbiHelper(std::shared_ptr<sqlite3> dbiconn) : dbiconn_(dbiconn) {}

    int Prepare(bool web, std::string &error) noexcept {
        int rc;
        if (web) {
            // download remote .dbi to local temporary file and reopen that
            temp_dbifile_ = unixTempFileDir();
            if (!temp_dbifile_.empty() && temp_dbifile_[temp_dbifile_.size() - 1] != '/') {
                temp_dbifile_ += '/';
            }
            temp_dbifile_ += "sqlite_web_vfs_dbi.XXXXXX";
            int rc = mkstemp((char *)temp_dbifile_.data());
            if (rc < 0) {
                error = "failed generating temporary filename for .dbi download: ";
                error += sqlite3_errmsg(dbiconn_.get());
                return SQLITE_ERROR;
            }
            close(rc);

            // download from web_vfs by VACUUM INTO, which buys us parallel, chunked downloads
            // with retry logic!
            auto vacuum_sql = "vacuum into '" + temp_dbifile_ + "'";
            rc = sqlite3_exec(dbiconn_.get(), vacuum_sql.c_str(), nullptr, nullptr, nullptr);
            if (rc != SQLITE_OK) {
                error = "failed downloading .dbi to temporary file: ";
                error += sqlite3_errmsg(dbiconn_.get());
                return rc;
            }
            dbiconn_.reset();

            std::string temp_uri = "file:" + temp_dbifile_ + "?mode=ro&immutable=1";
            sqlite3 *raw_dbiconn = nullptr;
            rc = sqlite3_open_v2(temp_uri.c_str(), &raw_dbiconn,
                                 SQLITE_OPEN_URI | SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
                                 nullptr);
            if (rc != SQLITE_OK) {
                error = "failed opening .dbi after downloading: ";
                error += sqlite3_errstr(rc);
                return rc;
            }
            dbiconn_ = std::shared_ptr<sqlite3>(raw_dbiconn, sqlite3_close_v2);
        }

        // tune connection and prepare query
        rc = sqlite3_exec(dbiconn_.get(), "pragma cache_size = -16384", nullptr, nullptr, nullptr);
        if (rc != SQLITE_OK) {
            error = sqlite3_errmsg(dbiconn_.get());
            return rc;
        }
        // TODO: check pragma application_id = 0x646269;
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

    /*
    ** Originally from sqlite os_unix.c:
    **
    ** Return the name of a directory in which to put temporary files.
    ** If no suitable temporary file directory can be found, return NULL.
    */
    const char *unixTempFileDir(void) {
        static const char *azDirs[] = {0, 0, "/tmp", "."};
        unsigned int i = 0;
        struct stat buf;
        const char *zDir = 0;

        if (!azDirs[0])
            azDirs[0] = getenv("SQLITE_TMPDIR");
        if (!azDirs[1])
            azDirs[1] = getenv("TMPDIR");
        while (1) {
            if (zDir != 0 && stat(zDir, &buf) == 0 && S_ISDIR(buf.st_mode) &&
                access(zDir, 03) == 0) {
                return zDir;
            }
            if (i >= sizeof(azDirs) / sizeof(azDirs[0]))
                break;
            zDir = azDirs[i++];
        }
        return 0;
    }

  public:
    virtual ~dbiHelper() {
        dbiconn_.reset();
        if (!temp_dbifile_.empty()) {
            unlink(temp_dbifile_.c_str());
        }
    }

    static int Open(HTTP::CURLconn *curlconn, const std::string &dbi_url, bool web_insecure,
                    int log_level, std::unique_ptr<dbiHelper> &dbi, std::string &error) noexcept {
        error.clear();

        bool web = true;
        std::string open_uri = "?mode=ro&immutable=1";
        if (dbi_url.substr(0, 5) == "file:") {
            open_uri = dbi_url + open_uri;
            web = false;
        } else {
            // Open .dbi through web_vfs. This is quite tricky and cute: the process of opening one
            // web_vfs db file triggers opening another. (&web_nodbi=1 prevents infinite recursion)
            std::string encoded_url;
            if (!curlconn->escape(dbi_url, encoded_url) || encoded_url.size() < dbi_url.size()) {
                error = "Failed percent-encoding dbi_url";
                return SQLITE_CANTOPEN;
            }

            open_uri = "file:/__web__" + open_uri + "&vfs=web&web_nodbi=1&web_url=" + encoded_url;

            if (web_insecure) {
                open_uri += "&web_insecure=1";
            }
            open_uri += "&web_small_KiB=1048576&vfs_log=" + std::to_string(log_level);
        }

        sqlite3 *raw_dbiconn = nullptr;
        int rc =
            sqlite3_open_v2(open_uri.c_str(), &raw_dbiconn,
                            SQLITE_OPEN_URI | SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, nullptr);
        if (rc != SQLITE_OK) {
            error = sqlite3_errstr(rc);
            return rc;
        }
        dbi.reset(new dbiHelper(std::shared_ptr<sqlite3>(raw_dbiconn, sqlite3_close_v2)));
        rc = dbi->Prepare(web, error);
        if (rc != SQLITE_OK) {
            dbi.reset();
            return rc;
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
