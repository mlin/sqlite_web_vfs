#!/usr/bin/env python3
"""
Creates a .dbi index file to accelerate web access for an immutable SQLite database file.

Identifies key pages of the main database file that readers use frequently:
    - interior pages of all tables and indexes
    - SQLite system tables
    - first page
Copies these into a .dbi file, itself a small SQLite database (typically <1% of the full database,
YMMV), with their original positions.

Those key pages are typically scattered all throughout the main database file (even after vacuum).
Therefore, a reader with a prefetched .dbi file can significantly streamline their random access
pattern for subsequent queries.

Consistency of the page data can be ensured within reason by checking that the SQLite header (the
first 100 bytes of the main database file) matches that stored in the .dbi file (in pageno=1).

Background:
    https://www.sqlite.org/fileformat.html
    https://sqlite.org/dbstat.html
"""

import sys
import os
import argparse
import itertools
import contextlib
import multiprocessing
import sqlite3


def main(argv):
    parser = argparse.ArgumentParser(
        prog=os.path.basename(argv[0]),
        description="Create .dbi index file to accelerate web access for an immutable SQLite database",
    )
    parser.add_argument("dbfile", type=str, help="immutable SQLite database filename")
    parser.add_argument(
        "-o",
        dest="dbifile",
        type=str,
        default=None,
        help="output .dbi filename (default: dbfile.dbi)",
    )
    parser.add_argument(
        "-f", "--force", action="store_true", help="remove existing output .dbi file, if any"
    )
    args = parser.parse_args(argv[1:])

    # read basic db info
    header = read_db_header(args.dbfile)
    dbfile_mtime = os.stat(args.dbfile).st_mtime
    page_size, btrees = read_db_btrees(args.dbfile)

    if not btrees:
        print(f"[WARN] database appears to be empty", file=sys.stderr)
    if not args.dbifile:
        args.dbifile = args.dbfile + ".dbi"
    if os.path.exists(args.dbifile):
        if os.path.isfile(args.dbifile) and args.force:
            print(f"[WARN] deleting existing {args.dbifile}", file=sys.stderr)
            os.unlink(args.dbifile)
        else:
            print(f"delete existing {args.dbifile} and try again", file=sys.stderr)
            sys.exit(1)
    if os.path.basename(args.dbifile) != os.path.basename(args.dbfile) + ".dbi":
        print(
            f"[WARN] index filename should be {os.path.basename(args.dbfile)}.dbi", file=sys.stderr
        )
    print(
        f"indexing for web access: {args.dbfile} ({os.path.getsize(args.dbfile):,} bytes)",
        file=sys.stderr,
    )
    print(f"page size: {page_size:,} bytes", file=sys.stderr)
    print(f"scanning: {len(btrees):,} btrees", file=sys.stderr)
    sys.stderr.flush()

    # identify the desired pages
    desired_pagenos = collect_pagenos(args.dbfile, btrees)
    desired_pagenos.add(1)
    print(f"pages to copy: {len(desired_pagenos):,}", file=sys.stderr)
    sys.stderr.flush()

    try:
        # write .dbi file
        write_dbi(args.dbfile, page_size, desired_pagenos, args.dbifile)
        # check database file hasn't been modified
        assert (
            os.stat(args.dbfile).st_mtime == dbfile_mtime and read_db_header(args.dbfile) == header
        ), "Database file was concurrently modified"
    except:
        try:
            os.unlink(args.dbifile)
        except:
            print(f"[WARN] failed to remove {args.dbifile} upon error", file=sys.stderr)
        raise

    print(f"wrote: {args.dbifile} ({os.path.getsize(args.dbifile):,} bytes)", file=sys.stderr)


def read_db_header(dbfile):
    "Read the SQLite database file header (first 100 bytes)"
    with open(dbfile, "rb") as infile:
        header = infile.read(100)
    assert (
        isinstance(header, bytes) and len(header) == 100 and header[:16] == b"SQLite format 3\000"
    ), "Unable to read SQLite database header"
    return header


def read_db_btrees(dbfile):
    "List the table and index names"
    dbh = sqlite3.connect(dbfile)
    page_size = next(dbh.execute("pragma page_size"))[0]
    btrees = [row[0] for row in dbh.execute("select name from sqlite_master")]
    dbh.close()
    assert page_size in (512, 1024, 2048, 4096, 8192, 16384, 32768, 65536)
    btrees.append("sqlite_master")
    return page_size, btrees


def collect_pagenos(dbfile, btrees):
    "List the interior page numbers for all the given table & index names"
    with multiprocessing.Pool(min(8, multiprocessing.cpu_count())) as pool:
        return set(
            itertools.chain(
                *pool.map(collect_pagenos_worker, [(dbfile, btree) for btree in btrees])
            )
        )


def collect_pagenos_worker(inp):
    dbfile = inp[0]
    btree = inp[1]
    dbh = sqlite3.connect(dbfile)
    # ref: https://github.com/sqlite/sqlite/blob/master/src/dbstat.c
    query = "select pageno from dbstat where name=?"
    if not btree.startswith("sqlite_"):
        # include all pages of sqlite system tables
        query += " and pagetype='internal'"
    pagenos = [row[0] for row in dbh.execute(query, (btree,))]
    dbh.close()
    return pagenos


def write_dbi(dbfile, page_size, pagenos, dbifile):
    "generate .dbi file, copying the content of the given page numbers"

    with contextlib.ExitStack() as stack:
        cursor = open(dbfile, "rb")
        stack.enter_context(cursor)
        cursor.seek(0, os.SEEK_END)
        dbfile_size = cursor.tell()
        assert dbfile_size and not (dbfile_size % page_size)

        assert not os.path.exists(dbifile), "remove existing .dbi file " + dbifile
        dbih = sqlite3.connect(dbifile)
        stack.callback(lambda dbih: dbih.close(), dbih)

        # application_id = "DBI"
        # small page size minimizes bin-packing overhead to store blobs of comparable size
        dbih.executescript(
            """
            pragma application_id = 0x444249;
            pragma page_size = 1024;
            pragma locking_mode = EXCLUSIVE
            """
        )

        with dbih:  # transaction
            dbih.executescript(
                """
                create table web_dbi_meta(key text primary key, value blob) without rowid;
                create table web_dbi_pages(offset integer primary key, data blob not null)
                """
            )

            for pageno in pagenos:
                assert pageno > 0
                ofs = (pageno - 1) * page_size
                assert ofs + page_size <= dbfile_size
                cursor.seek(ofs)
                pagedata = cursor.read(page_size)
                assert len(pagedata) == page_size
                dbih.execute("insert into web_dbi_pages(offset,data) values(?,?)", (ofs, pagedata))

            for kv in [("page_size", page_size), ("dbfile_size", dbfile_size)]:
                dbih.execute("insert into web_dbi_meta(key,value) values(?,?)", kv)

        dbih.executescript("vacuum")


if __name__ == "__main__":
    main(sys.argv)
