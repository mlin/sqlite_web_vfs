// Tests for Content-Range and Content-Length parsing and for retaining
// only the final response's headers, excluding trailers.
#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "../src/HTTP.h"

namespace {

struct CRCase {
    const char *input;
    uint64_t first, last; // the range the caller expects the header to name
    bool ok;
    bool total_known;
    uint64_t total;
    CRCase(const char *i, uint64_t f, uint64_t l, bool o)
        : input(i), first(f), last(l), ok(o), total_known(false), total(0) {}
    CRCase(const char *i, uint64_t f, uint64_t l, bool o, bool tk, uint64_t t)
        : input(i), first(f), last(l), ok(o), total_known(tk), total(t) {}
};

} // namespace

TEST_CASE("ParseContentRange exact-match table") {
    const CRCase cases[] = {
        // the header names exactly the expected range
        {"bytes 0-99/77824", 0, 99, true, true, 77824},
        {"bytes 65536-77823/77824", 65536, 77823, true, true, 77824},
        {"bytes 0-99/*", 0, 99, true, false, 0},
        {"bytes 0-0/1", 0, 0, true, true, 1},
        {"BYTES 10-19/100", 10, 19, true, true, 100},
        {"bYtEs 10-19/100", 10, 19, true, true, 100},
        {"  bytes 10-19/100  ", 10, 19, true, true, 100},
        {"\tbytes 10-19/100\t", 10, 19, true, true, 100},
        {"bytes 007-009/0100", 7, 9, true, true, 100}, // leading zeros are digits
        {"bytes 0-99/100 ", 0, 99, true, true, 100},   // trailing OWS is allowed
        {"bytes 0-18446744073709551615/*", 0, UINT64_MAX, true, false, 0},
        {"bytes 18446744073709551615-18446744073709551615/*", UINT64_MAX, UINT64_MAX, true, false,
         0},
        {"bytes 0-99/18446744073709551615", 0, 99, true, true, UINT64_MAX},
        // An inconsistent total parses; callers enforce their total policies.
        {"bytes 0-100/100", 0, 100, true, true, 100},
        // the header names a different range than expected
        {"bytes 4096-8191/77824", 0, 4095, false},
        {"bytes 0-99/77824", 0, 98, false},
        {"bytes 0-99/77824", 1, 99, false},
        {"bytes 99-0/100", 0, 99, false},
        // total out of uint64 range
        {"bytes 0-99/18446744073709551616", 0, 99, false},
        {"bytes 0-99/99999999999999999999999999", 0, 99, false},
        // malformed syntax (the expectation is arbitrary)
        {"bytes 0-99", 0, 99, false},                 // missing /total
        {"bytes 0-99/", 0, 99, false},                // empty total
        {"bytes /100", 0, 99, false},                 // missing range entirely
        {"bytes -99/100", 0, 99, false},              // empty first
        {"bytes 0-/100", 0, 99, false},               // empty last
        {"bytes 0-99/100 x", 0, 99, false},           // junk suffix
        {"bytes 0-99/100x", 0, 99, false},            // suffix junk
        {"bytes 0-99/100,200-299/600", 0, 99, false}, // multiple ranges
        {"bytes 0-99, 200-299/600", 0, 99, false},
        {"bytes +0-99/100", 0, 99, false}, // sign
        {"bytes 0-+99/100", 0, 99, false},
        {"bytes 0-99/+100", 0, 99, false},
        {"bytes -0-99/100", 0, 99, false},
        {"bytes 0 -99/100", 0, 99, false}, // embedded whitespace
        {"bytes 0- 99/100", 0, 99, false},
        {"bytes 0-99 /100", 0, 99, false},
        {"bytes 0-99/ 100", 0, 99, false},
        {"bytes  0-99/100", 0, 99, false}, // two spaces after unit
        {"bytes\t0-99/100", 0, 99, false}, // tab is not the grammar's SP
        {"byte 0-99/100", 0, 99, false},   // wrong unit
        {"bytesx 0-99/100", 0, 99, false},
        {"items 0-99/100", 0, 99, false},
        {"bytes 0.0-99/100", 0, 99, false},
        {"bytes 0x0-99/100", 0, 99, false},
        {"", 0, 99, false},
        {" ", 0, 99, false},
        {"bytes", 0, 99, false},
        {"bytes ", 0, 99, false},
        // range endpoints beyond uint64 match no expectation
        {"bytes 18446744073709551616-18446744073709551617/*", 0, 99, false},
        {"bytes 0-18446744073709551616/*", 0, 99, false},
        {"bytes 99999999999999999999999999-99/100", 0, 99, false},
        {"bytes */100", 0, 99, false}, // unsatisfied form: not this parser
        {"bytes */*", 0, 99, false},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        const CRCase &c = cases[i];
        CAPTURE(i, c.input);
        bool total_known = true; // sentinels: must survive a failed parse
        uint64_t total = 424242;
        bool ok = HTTP::ParseContentRange(c.input, c.first, c.last, total_known, total);
        REQUIRE(ok == c.ok);
        if (ok) {
            REQUIRE(total_known == c.total_known);
            if (c.total_known) {
                REQUIRE(total == c.total);
            }
        } else {
            REQUIRE(total_known);
            REQUIRE(total == 424242);
        }
    }

    // Reject oversized values without changing outputs; accept leading zeros.
    bool total_known = true;
    uint64_t total = 424242;
    REQUIRE(!HTTP::ParseContentRange("bytes " + std::string(300, '0') + "-99/77824", 0, 99,
                                     total_known, total));
    REQUIRE(total_known);
    REQUIRE(total == 424242);
    REQUIRE(!HTTP::ParseContentRange(std::string(300, ' ') + "bytes 0-99/77824", 0, 99, total_known,
                                     total));
    REQUIRE(total_known);
    REQUIRE(total == 424242);
    REQUIRE(HTTP::ParseContentRange("bytes 000000000000000000000-99/077824", 0, 99, total_known,
                                    total));
    REQUIRE(total_known);
    REQUIRE(total == 77824);
}

TEST_CASE("ReadContentLengthHeader") {
    HTTP::headers h;
    h["content-length"] = "77824";
    REQUIRE(HTTP::ReadContentLengthHeader(h) == 77824);
    h["content-length"] = " 42 ";
    REQUIRE(HTTP::ReadContentLengthHeader(h) == 42);
    h["content-length"] = "9223372036854775807"; // LLONG_MAX
    REQUIRE(HTTP::ReadContentLengthHeader(h) == LLONG_MAX);
    h["content-length"] = "9223372036854775808"; // LLONG_MAX + 1: unreadable
    REQUIRE(HTTP::ReadContentLengthHeader(h) == -2);
    h["content-length"] = "18446744073709551616"; // 2^64: overflow
    REQUIRE(HTTP::ReadContentLengthHeader(h) == -2);
    h["content-length"] = "+42"; // Reject signs, which strtoull accepts.
    REQUIRE(HTTP::ReadContentLengthHeader(h) == -2);
    h["content-length"] = "42x";
    REQUIRE(HTTP::ReadContentLengthHeader(h) == -2);
    h["content-length"] = "";
    REQUIRE(HTTP::ReadContentLengthHeader(h) == -2);
}

namespace {

struct HeaderFeed {
    HTTP::headers h;
    HTTP::HeaderSink sink;
    HeaderFeed() : sink(h) {}
    void Feed(const char *line) {
        std::string s(line);
        HTTP::headerfunction(&s[0], 1, s.size(), &sink);
    }
    void Reset() {
        h.clear();
        sink.complete = false;
    }
};

} // namespace

TEST_CASE("headerfunction keeps only the final response's headers") {
    HeaderFeed f;
    // one ordinary response
    f.Feed("HTTP/1.1 206 Partial Content\r\n");
    f.Feed("Content-Range: bytes 0-99/77824\r\n");
    f.Feed("Content-Length: 100\r\n");
    f.Feed("\r\n");
    REQUIRE(f.h.size() == 2);
    REQUIRE(f.h["content-range"] == "bytes 0-99/77824");

    // an interim response's Content-Range must not leak into a final
    // response that omits the header
    f.Reset();
    f.Feed("HTTP/1.1 103 Early Hints\r\n");
    f.Feed("Content-Range: bytes 0-99/77824\r\n");
    f.Feed("\r\n");
    f.Feed("HTTP/1.1 206 Partial Content\r\n");
    f.Feed("Content-Length: 100\r\n");
    f.Feed("\r\n");
    REQUIRE(f.h.count("content-range") == 0);
    REQUIRE(f.h["content-length"] == "100");

    // when both responses carry the header, the final response's wins
    f.Reset();
    f.Feed("HTTP/1.1 301 Moved Permanently\r\n");
    f.Feed("Content-Range: bytes 5-9/10\r\n");
    f.Feed("\r\n");
    f.Feed("HTTP/1.1 206 Partial Content\r\n");
    f.Feed("Content-Range: bytes 0-99/77824\r\n");
    f.Feed("\r\n");
    REQUIRE(f.h["content-range"] == "bytes 0-99/77824");

    // a trailer (delivered after the header section's blank line) must
    // not supply a header the final response omitted
    f.Reset();
    f.Feed("HTTP/1.1 206 Partial Content\r\n");
    f.Feed("Transfer-Encoding: chunked\r\n");
    f.Feed("\r\n");
    f.Feed("Content-Range: bytes 0-99/77824\r\n"); // trailer
    f.Feed("\r\n");
    REQUIRE(f.h.count("content-range") == 0);
    REQUIRE(f.h["transfer-encoding"] == "chunked");

    // nor overwrite one it carried
    f.Reset();
    f.Feed("HTTP/1.1 206 Partial Content\r\n");
    f.Feed("Content-Range: bytes 5-9/10\r\n");
    f.Feed("\r\n");
    f.Feed("Content-Range: bytes 0-99/77824\r\n"); // trailer
    f.Feed("\r\n");
    REQUIRE(f.h["content-range"] == "bytes 5-9/10");
}
