# sqlite_web_vfs

This [SQLite3 virtual filesystem extension](https://www.sqlite.org/vfs.html) provides read-only access to database files over HTTP(S), including S3 and the like, without going through a FUSE mount (a fine alternative when available).

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

### Optimization

SQLite reads one small page at a time (default 4 KiB), which would be inefficient to serve with HTTP requests one-to-one. Instead, the extension adaptively consolidates page fetching into larger HTTP requests, and concurrently reads ahead on background threads. This works well for point lookups and queries that scan largely-contiguous slices of tables and indexes (and a modest number thereof). It's less suitable for big multi-way joins and other aggressively random access patterns; in those cases, it's better to download the database file upfront to open locally.

It's a good idea to [VACUUM](https://sqlite.org/lang_vacuum.html) a database file before serving it over the web, and to increase the reader's [page cache size](https://www.sqlite.org/pragma.html#pragma_cache_size).

### Logging

By default, the extension reports errors on standard error (including HTTP errors that succeed after retry). This can be disabled by setting `&web_log=0` in the open URI, or by setting the environment variable `SQLITE_WEB_LOG=0`, the latter taking precedence. The log level can also be set to 2 or 3 to log every HTTP request made.
