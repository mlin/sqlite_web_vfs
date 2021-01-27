import sys
import os
import sqlite3
import urllib.parse
import pytest

HERE = os.path.dirname(__file__)
BUILD = os.path.abspath(os.path.join(HERE, "..", "build"))
CHINOOK_URI = "https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"


def test_chinook():
    CHINOOK_COUNTS = {
        "Album": 347,
        "Artist": 275,
        "Customer": 59,
        "Employee": 8,
        "Genre": 25,
        "Invoice": 412,
        "InvoiceLine": 2240,
        "MediaType": 5,
        "Playlist": 18,
        "PlaylistTrack": 8715,
        "Track": 3503,
    }

    con = sqlite3.connect(f":memory:")
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "web_vfs"))

    con = sqlite3.connect(
        f"file:/__web__?vfs=web&mode=ro&web_uri={urllib.parse.quote(CHINOOK_URI)}", uri=True
    )
    schema = list(con.execute("select type, name from sqlite_master"))
    print(schema)
    assert set(p[1] for p in schema if p[0] == "table") == set(CHINOOK_COUNTS.keys())
    sys.stdout.flush()

    print(
        list(
            con.execute(
                """
                select e.*, count(i.invoiceid) as 'Total Number of Sales'
                from employee as e
                    join customer as c on e.employeeid = c.supportrepid
                    join invoice as i on i.customerid = c.customerid
                group by e.employeeid
                """
            )
        )
    )
    sys.stdout.flush()

    for ty, tbl in schema:
        if ty == "table":
            ct = next(con.execute(f"select count(*) from {tbl}"))[0]
            print(f"{tbl}\t" + str(ct))
            assert ct == CHINOOK_COUNTS[tbl]
