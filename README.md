# sqlite_web_vfs

This [SQLite3 virtual filesystem extension](https://www.sqlite.org/vfs.html) provides read-only access to database files over HTTP(S), including S3 and the like, without going through a FUSE mount (a fine alternative when available).

With the [extension loaded](https://sqlite.org/loadext.html), use the normal SQLite3 API to open the special URI: 

```
file:/__web__?mode=ro&immutable=1&vfs=web&web_url={{PERCENT_ENCODED_URL}}
```

where `{{PERCENT_ENCODED_URL}}` is the database file's complete http(s) URL passed through [percent-encoding](https://en.wikipedia.org/wiki/Percent-encoding) (doubly encoding its own query string, if any). The URL server must support [GET range](https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests) requests, and the content must be immutable for the session.

SQLite reads one small page at a time (default 4 KiB), which would be inefficient to serve with HTTP requests one-to-one. Instead, the extension adaptively consolidates page fetching into larger HTTP requests, and concurrently reads ahead on background threads. This works well for point lookups and queries that scan largely-contiguous slices of tables and indexes (and a modest number thereof). It's less suitable for big multi-way joins and other aggressively random access patterns; in those cases, it's better to download the database file upfront to open locally.

It's a good idea to [VACUUM](https://sqlite.org/lang_vacuum.html) a database file before serving it over the web, and to increase the reader's [page cache size](https://www.sqlite.org/pragma.html#pragma_cache_size).

**USE AT YOUR OWN RISK:** This project is not associated with the SQLite developers.

### Build from source

![CI](https://github.com/mlin/sqlite_web_vfs/workflows/CI/badge.svg?branch=main)

Requirements:

* Linux or macOS
* C++11 build system with CMake
* Dev packages for SQLite3 and libcurl
* (Tests only) python3 and pytest

```
cmake -DCMAKE_BUILD_TYPE=Release -B build . && cmake --build build -j8
env -C build ctest -V
```

The extension library is `build/web_vfs.so` or `build/web_vfs.dylib`.
