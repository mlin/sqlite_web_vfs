# sqlite_web_vfs

This [SQLite3 virtual filesystem extension](https://www.sqlite.org/vfs.html) provides read-only access to database files over HTTP(S), including S3 and the like, without involving a [FUSE mount](https://en.wikipedia.org/wiki/Filesystem_in_Userspace) (a fine alternative when available). **See also** the companion projects [sqlite_zstd_vfs](https://github.com/mlin/sqlite_zstd_vfs/) and [Genomics Extension for SQLite](https://github.com/mlin/GenomicSQLite), which include sqlite_web_vfs along with other features, most notably compression of the database file.

With the [extension loaded](https://sqlite.org/loadext.html), use the normal SQLite3 API to open the special URI: 

```
file:/__web__?mode=ro&immutable=1&vfs=web&web_url={{PERCENT_ENCODED_URL}}
```

where `{{PERCENT_ENCODED_URL}}` is the database file's complete http(s) URL passed through [percent-encoding](https://en.wikipedia.org/wiki/Percent-encoding) (doubly encoding its own query string, if any). The URL server must support [GET range](https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests) requests, and the content must be immutable for the session.

**USE AT YOUR OWN RISK:** This project is not associated with the SQLite developers.

### Quick example

A Python program to access the [Chinook sample database](https://github.com/lerocha/chinook-database) on GitHub directly:

```python3
import sqlite3
import urllib.parse

CHINOOK_URL = "https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"

con = sqlite3.connect(":memory:")  # just to load_extension
con.enable_load_extension(True)
con.load_extension("web_vfs")      # web_vfs.{so,dylib} in current directory
con = sqlite3.connect(
    f"file:/__web__?vfs=web&mode=ro&immutable=1&web_url={urllib.parse.quote(CHINOOK_URL)}",
    uri=True,
)
schema = list(con.execute("select type, name from sqlite_master"))
print(schema)
```

### Build from source

![CI](https://github.com/mlin/sqlite_web_vfs/workflows/CI/badge.svg?branch=main)

Requirements:

* Linux or macOS
* C++11 build system with CMake
* SQLite3 and libcurl dev packages
* (Tests only) python3 and pytest

```
cmake -DCMAKE_BUILD_TYPE=Release -B build . && cmake --build build -j8
env -C build ctest -V
```

The extension library is `build/web_vfs.so` or `build/web_vfs.dylib`.


### Configuration

The extension logs a message to standard error upon any fatally failed HTTP request, and requests that succeed after having to be retried. The latter can be suppressed by setting `&web_log=1` in the open URI, or by setting environment `SQLITE_WEB_LOG=1` in the environment. The log level can be set to 0 to suppress all standard error logging, or increased up to 5 for individual request/response logging.

To disable TLS certificate and hostname verification, set `&web_insecure=1` or `SQLITE_WEB_INSECURE=1`.

### Basic optimization

SQLite reads one small page at a time (default 4 KiB), which would be inefficient to serve with HTTP requests one-to-one. Instead, the extension adaptively consolidates page fetching into larger HTTP requests, and concurrently reads ahead on background threads. This works well for point lookups and queries that scan largely-contiguous slices of tables and indexes (and a modest number thereof). It's less suitable for big multi-way joins and other aggressively random access patterns; in those cases, it's better to download the database file upfront to open locally.

Readers should enlarge their [page cache](https://www.sqlite.org/pragma.html#pragma_cache_size) capacity as much as feasible, while budgeting an additional ~640 MiB RAM for the extension's prefetch buffers. (That ought to be enough for anybody.)

To optimize a database file to be served over the web, write it with the largest possible [page size](https://www.sqlite.org/pragma.html#pragma_page_size) of 64 KiB, and [VACUUM](https://sqlite.org/lang_vacuum.html) it once the contents are finalized. These steps minimize the random accesses needed for queries.

### Advanced optimization: helper .dbi files

To further streamline the access pattern, the extension can utilize a small .dbi helper file served alongside the main database file. Opening a given `web_url`, the extension automatically probes for it by appending `.dbi` to the URL (unless it has a query string). If it doesn't find that for any reason, main database access falls back to non-dbi mode. Increase the log level to 3 or higher to see which mode is used. 

The automatic probe can be overridden by setting `&web_dbi_url=` to different percent-encoded URL for the .dbi file, or to a local `file:/path/to.dbi` downloaded beforehand. Use the latter approach to save multiple connections from each having to fetch the .dbi separately. Lastly, set `&web_nodbi=1` or `SQLITE_WEB_NODBI=1` to disable this feature entirely.

The included [`sqlite_web_dbi.py`](sqlite_web_dbi.py) utility generates the .dbi helper for a SQLite database file given on the command line. If the main database file is subsequently changed, any previous .dbi must be discarded. (The extension makes a reasonable effort to detect out-of-date .dbi and fall back to non-dbi mode, but this cannot be guaranteed.)

The .dbi helper is never required, but often helpful for accessing a large database with high request latency. It copies small portions of the main database file that are key for navigating within, but typically scattered throughout (even after vacuum). These include interior nodes of SQLite's b-trees, and various metadata tables. By prefetching them in the compact .dbi, the reader may elide sequences of scattered requests to collect them from the main file.
