#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"
#include "sqlite3.h"

#include "../src/web_vfs.h"
#include "test_httpd.h"

using namespace std;

void setup() {
    static bool first = true;
    if (first) {
        (new WebVFS::VFS())->Register("web");
        int rc = system("aria2c -c -d /tmp -s 10 -x 10 --retry-wait 2 "
                        "https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H.db");
        REQUIRE(rc == 0);
        first = false;
    }
}

TEST_CASE("TPC-H") { setup(); }
