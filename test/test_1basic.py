import sys
import os
import sqlite3
import urllib.parse
import pytest

HERE = os.path.dirname(__file__)
BUILD = os.path.abspath(os.path.join(HERE, "..", "build"))
CHINOOK_URI = "https://github.com/lerocha/chinook-database/raw/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"


def test_master():
    con = sqlite3.connect(f":memory:")
    con.enable_load_extension(True)
    con.load_extension(os.path.join(BUILD, "web_vfs"))

    con = sqlite3.connect(
        f"file:/__web__?vfs=web&mode=ro&web_uri={urllib.parse.quote(CHINOOK_URI)}", uri=True
    )
    print(list(con.execute("select * from sqlite_master")))

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
