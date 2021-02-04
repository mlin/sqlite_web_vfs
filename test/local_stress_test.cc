#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"
#include "sqlite3.h"

#include "../src/web_vfs.h"
#include "test_httpd.h"

using namespace std;

const short TEST_HTTPD_PORT = 8192;
const int Q8_ITERATIONS = 16;
const char *Q8 = R"(
select
    o_year,
    sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
from
    (select
        strftime("%Y", o_orderdate) as o_year,
        l_extendedprice * (1-l_discount) as volume,
        n2.n_name as nation
    from part, supplier, lineitem, orders, customer, nation n1, nation n2, region
    where
        p_partkey = l_partkey
        and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey
        and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey
        and n1.n_regionkey = r_regionkey
        and r_name = 'AMERICA'
        and s_nationkey = n2.n_nationkey
        and o_orderdate between date('1995-01-01') and date('1996-12-31')
        and p_type = 'ECONOMY ANODIZED STEEL')
    as all_nations
group by o_year order by o_year;
)";

void setup() {
    static bool first = true;
    if (first) {
        int rc = system("aria2c -c -d /tmp -s 10 -x 10 --retry-wait 2 "
                        "https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H.db");
        REQUIRE(rc == 0);
        (new WebVFS::VFS())->Register("web");
        first = false;
    }
}

TEST_CASE("TPC-H Q8") {
    setup();

    map<string, string> httpd_files;
    httpd_files["/TPC-H.db"] = "/tmp/TPC-H.db";
    TestHTTPd httpd;
    REQUIRE(httpd.Start(TEST_HTTPD_PORT, httpd_files));

    string db_url = "file:/__web__?mode=ro&immutable=1&vfs=web&web_url=http://localhost:";
    db_url += to_string(TEST_HTTPD_PORT);
    db_url += "/TPC-H.db";

    sqlite3 *db = nullptr;
    int rc = sqlite3_open_v2(db_url.c_str(), &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, nullptr);
    REQUIRE(rc == SQLITE_OK);

    size_t rows = 0;
    rc = sqlite3_exec(
        db, "SELECT count(1) FROM sqlite_master",
        [](void *ctx, int n_col, char **cols, char **col_names) -> int {
            cout << cols[0] << endl;
            (*(size_t *)ctx)++;
            return 0;
        },
        &rows, nullptr);
    REQUIRE(rows == 1);

    rc = sqlite3_exec(db, "PRAGMA cache_size=-10000", nullptr, nullptr, nullptr);
    REQUIRE(rc == SQLITE_OK);

    for (int i = 0; i < Q8_ITERATIONS; ++i) {
        rows = 0;
        rc = sqlite3_exec(
            db, Q8,
            [](void *ctx, int n_col, char **cols, char **col_names) -> int {
                cout << cols[0] << "\t" << cols[1] << endl;
                (*(size_t *)ctx)++;
                return 0;
            },
            &rows, nullptr);
        REQUIRE(rows == 2);
        cout << endl << (i+1) << " / " << Q8_ITERATIONS << endl << endl;
    }

    sqlite3_close(db);
}

// TLS basic example:
// https://github.com/rboulton/libmicrohttpd/blob/master/src/examples/https_fileserver_example.c
