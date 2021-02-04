#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"
#include "sqlite3.h"

#include "../src/web_vfs.h"
#include "test_httpd.h"

using namespace std;

const short TEST_HTTPD_PORT = 8192;
const int Q8_ITERATIONS = 1;
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

// Test Certificate from libmicrohttpd https_fileserver_example.c
const char cert_pem[] = "-----BEGIN CERTIFICATE-----\n"
                        "MIICpjCCAZCgAwIBAgIESEPtjjALBgkqhkiG9w0BAQUwADAeFw0wODA2MDIxMjU0\n"
                        "MzhaFw0wOTA2MDIxMjU0NDZaMAAwggEfMAsGCSqGSIb3DQEBAQOCAQ4AMIIBCQKC\n"
                        "AQC03TyUvK5HmUAirRp067taIEO4bibh5nqolUoUdo/LeblMQV+qnrv/RNAMTx5X\n"
                        "fNLZ45/kbM9geF8qY0vsPyQvP4jumzK0LOJYuIwmHaUm9vbXnYieILiwCuTgjaud\n"
                        "3VkZDoQ9fteIo+6we9UTpVqZpxpbLulBMh/VsvX0cPJ1VFC7rT59o9hAUlFf9jX/\n"
                        "GmKdYI79MtgVx0OPBjmmSD6kicBBfmfgkO7bIGwlRtsIyMznxbHu6VuoX/eVxrTv\n"
                        "rmCwgEXLWRZ6ru8MQl5YfqeGXXRVwMeXU961KefbuvmEPccgCxm8FZ1C1cnDHFXh\n"
                        "siSgAzMBjC/b6KVhNQ4KnUdZAgMBAAGjLzAtMAwGA1UdEwEB/wQCMAAwHQYDVR0O\n"
                        "BBYEFJcUvpjvE5fF/yzUshkWDpdYiQh/MAsGCSqGSIb3DQEBBQOCAQEARP7eKSB2\n"
                        "RNd6XjEjK0SrxtoTnxS3nw9sfcS7/qD1+XHdObtDFqGNSjGYFB3Gpx8fpQhCXdoN\n"
                        "8QUs3/5ZVa5yjZMQewWBgz8kNbnbH40F2y81MHITxxCe1Y+qqHWwVaYLsiOTqj2/\n"
                        "0S3QjEJ9tvklmg7JX09HC4m5QRYfWBeQLD1u8ZjA1Sf1xJriomFVyRLI2VPO2bNe\n"
                        "JDMXWuP+8kMC7gEvUnJ7A92Y2yrhu3QI3bjPk8uSpHea19Q77tul1UVBJ5g+zpH3\n"
                        "OsF5p0MyaVf09GTzcLds5nE/osTdXGUyHJapWReVmPm3Zn6gqYlnzD99z+DPIgIV\n"
                        "RhZvQx74NQnS6g==\n"
                        "-----END CERTIFICATE-----\n";

const char key_pem[] = "-----BEGIN RSA PRIVATE KEY-----\n"
                       "MIIEowIBAAKCAQEAtN08lLyuR5lAIq0adOu7WiBDuG4m4eZ6qJVKFHaPy3m5TEFf\n"
                       "qp67/0TQDE8eV3zS2eOf5GzPYHhfKmNL7D8kLz+I7psytCziWLiMJh2lJvb2152I\n"
                       "niC4sArk4I2rnd1ZGQ6EPX7XiKPusHvVE6VamacaWy7pQTIf1bL19HDydVRQu60+\n"
                       "faPYQFJRX/Y1/xpinWCO/TLYFcdDjwY5pkg+pInAQX5n4JDu2yBsJUbbCMjM58Wx\n"
                       "7ulbqF/3lca0765gsIBFy1kWeq7vDEJeWH6nhl10VcDHl1PetSnn27r5hD3HIAsZ\n"
                       "vBWdQtXJwxxV4bIkoAMzAYwv2+ilYTUOCp1HWQIDAQABAoIBAArOQv3R7gmqDspj\n"
                       "lDaTFOz0C4e70QfjGMX0sWnakYnDGn6DU19iv3GnX1S072ejtgc9kcJ4e8VUO79R\n"
                       "EmqpdRR7k8dJr3RTUCyjzf/C+qiCzcmhCFYGN3KRHA6MeEnkvRuBogX4i5EG1k5l\n"
                       "/5t+YBTZBnqXKWlzQLKoUAiMLPg0eRWh+6q7H4N7kdWWBmTpako7TEqpIwuEnPGx\n"
                       "u3EPuTR+LN6lF55WBePbCHccUHUQaXuav18NuDkcJmCiMArK9SKb+h0RqLD6oMI/\n"
                       "dKD6n8cZXeMBkK+C8U/K0sN2hFHACsu30b9XfdnljgP9v+BP8GhnB0nCB6tNBCPo\n"
                       "32srOwECgYEAxWh3iBT4lWqL6bZavVbnhmvtif4nHv2t2/hOs/CAq8iLAw0oWGZc\n"
                       "+JEZTUDMvFRlulr0kcaWra+4fN3OmJnjeuFXZq52lfMgXBIKBmoSaZpIh2aDY1Rd\n"
                       "RbEse7nQl9hTEPmYspiXLGtnAXW7HuWqVfFFP3ya8rUS3t4d07Hig8ECgYEA6ou6\n"
                       "OHiBRTbtDqLIv8NghARc/AqwNWgEc9PelCPe5bdCOLBEyFjqKiT2MttnSSUc2Zob\n"
                       "XhYkHC6zN1Mlq30N0e3Q61YK9LxMdU1vsluXxNq2rfK1Scb1oOlOOtlbV3zA3VRF\n"
                       "hV3t1nOA9tFmUrwZi0CUMWJE/zbPAyhwWotKyZkCgYEAh0kFicPdbABdrCglXVae\n"
                       "SnfSjVwYkVuGd5Ze0WADvjYsVkYBHTvhgRNnRJMg+/vWz3Sf4Ps4rgUbqK8Vc20b\n"
                       "AU5G6H6tlCvPRGm0ZxrwTWDHTcuKRVs+pJE8C/qWoklE/AAhjluWVoGwUMbPGuiH\n"
                       "6Gf1bgHF6oj/Sq7rv/VLZ8ECgYBeq7ml05YyLuJutuwa4yzQ/MXfghzv4aVyb0F3\n"
                       "QCdXR6o2IYgR6jnSewrZKlA9aPqFJrwHNR6sNXlnSmt5Fcf/RWO/qgJQGLUv3+rG\n"
                       "7kuLTNDR05azSdiZc7J89ID3Bkb+z2YkV+6JUiPq/Ei1+nDBEXb/m+/HqALU/nyj\n"
                       "P3gXeQKBgBusb8Rbd+KgxSA0hwY6aoRTPRt8LNvXdsB9vRcKKHUFQvxUWiUSS+L9\n"
                       "/Qu1sJbrUquKOHqksV5wCnWnAKyJNJlhHuBToqQTgKXjuNmVdYSe631saiI7PHyC\n"
                       "eRJ6DxULPxABytJrYCRrNqmXi5TCiqR2mtfalEMOPxz8rUU8dYyx\n"
                       "-----END RSA PRIVATE KEY-----\n";

void setup() {
    static bool first = true;
    if (first) {
        int rc = system("aria2c -c -d /tmp -s 10 -x 10 --retry-wait 2 "
                        "https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H.db");
        REQUIRE(rc == 0);
        (new WebVFS::VFS())->Register("web");
        atexit([]() { curl_global_cleanup(); }); // otherwise valgrind shows leak warnings
        first = false;
    }
}

TEST_CASE("TPC-H Q8") {
    setup();

    map<string, string> httpd_files;
    httpd_files["/TPC-H.db"] = "/tmp/TPC-H.db";
    TestHTTPd httpd;
    REQUIRE(httpd.Start(TEST_HTTPD_PORT, httpd_files, cert_pem, key_pem));

    string db_url =
        "file:/__web__?mode=ro&immutable=1&vfs=web&web_insecure=1&web_url=https://localhost:";
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
        cout << endl << (i + 1) << " / " << Q8_ITERATIONS << endl << endl;
    }

    sqlite3_close(db);
}
