// Unit tests for the HTTP header machinery in src/HTTP.h: the strict
// Content-Range parser, the shared unsigned-decimal reader, the
// Content-Length reader, and the libcurl header callback's handling of
// multi-response performs (only the final response's headers may remain
// visible to callers).
#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "../src/HTTP.h"

namespace {

struct CRCase {
    const char *input;
    bool ok;
    uint64_t first, last, total;
    bool total_known;
    CRCase(const char *i, bool o)
        : input(i), ok(o), first(0), last(0), total(0), total_known(false) {}
    CRCase(const char *i, bool o, uint64_t f, uint64_t l, uint64_t t, bool tk)
        : input(i), ok(o), first(f), last(l), total(t), total_known(tk) {}
};

} // namespace

TEST_CASE("ParseContentRange table") {
    const CRCase cases[] = {
        // valid forms
        {"bytes 0-99/77824", true, 0, 99, 77824, true},
        {"bytes 65536-77823/77824", true, 65536, 77823, 77824, true},
        {"bytes 0-99/*", true, 0, 99, 0, false},
        {"bytes 0-0/1", true, 0, 0, 1, true},
        {"BYTES 10-19/100", true, 10, 19, 100, true},
        {"bYtEs 10-19/100", true, 10, 19, 100, true},
        {"  bytes 10-19/100  ", true, 10, 19, 100, true},
        {"\tbytes 10-19/100\t", true, 10, 19, 100, true},
        {"bytes 007-009/0100", true, 7, 9, 100, true}, // leading zeros are digits
        {"bytes 0-18446744073709551615/*", true, 0, UINT64_MAX, 0, false},
        {"bytes 18446744073709551615-18446744073709551615/*", true, UINT64_MAX, UINT64_MAX, 0,
         false},
        {"bytes 0-18446744073709551614/18446744073709551615", true, 0, UINT64_MAX - 1, UINT64_MAX,
         true},
        // malformed syntax
        {"bytes 0-99", false},                       // missing /total
        {"bytes 0-99/", false},                      // empty total
        {"bytes /100", false},                       // missing range entirely
        {"bytes -99/100", false},                    // empty first
        {"bytes 0-/100", false},                     // empty last
        {"bytes 0-99/100 ", true, 0, 99, 100, true}, // trailing OWS is trimmed
        {"bytes 0-99/100 x", false},                 // junk suffix
        {"bytes 0-99/100x", false},                  // suffix junk
        {"bytes 0-99/100,200-299/600", false},       // multiple ranges (comma form)
        {"bytes 0-99, 200-299/600", false},
        {"bytes +0-99/100", false}, // sign
        {"bytes 0-+99/100", false},
        {"bytes 0-99/+100", false},
        {"bytes -0-99/100", false},
        {"bytes 0 -99/100", false}, // embedded whitespace
        {"bytes 0- 99/100", false},
        {"bytes 0-99 /100", false},
        {"bytes 0-99/ 100", false},
        {"bytes  0-99/100", false}, // two spaces after unit
        {"bytes\t0-99/100", false}, // tab is not the grammar's SP
        {"byte 0-99/100", false},   // wrong unit
        {"bytesx 0-99/100", false},
        {"items 0-99/100", false},
        {"bytes 0.0-99/100", false},
        {"bytes 0x0-99/100", false},
        {"", false},
        {" ", false},
        {"bytes", false},
        {"bytes ", false},
        // numeric overflow
        {"bytes 18446744073709551616-18446744073709551617/*", false}, // 2^64
        {"bytes 0-18446744073709551616/*", false},
        {"bytes 0-99/18446744073709551616", false},
        {"bytes 99999999999999999999999999-99/100", false},
        // range/total relationships
        {"bytes 99-0/100", false},  // first > last
        {"bytes 0-100/100", false}, // last >= total
        {"bytes 0-99/99", false},   // last >= total
        {"bytes 0-99/0", false},    // total zero
        {"bytes */100", false},     // unsatisfied form: not this parser
        {"bytes */*", false},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        const CRCase &c = cases[i];
        CAPTURE(i, c.input);
        HTTP::ContentRange out;
        out.first = 111;
        out.last = 222;
        out.total = 333;
        out.total_known = true;
        bool ok = HTTP::ParseContentRange(c.input, out);
        REQUIRE(ok == c.ok);
        if (ok) {
            REQUIRE(out.first == c.first);
            REQUIRE(out.last == c.last);
            REQUIRE(out.total_known == c.total_known);
            if (c.total_known) {
                REQUIRE(out.total == c.total);
            }
        } else {
            // failed parses leave out untouched
            REQUIRE(out.first == 111);
            REQUIRE(out.last == 222);
            REQUIRE(out.total == 333);
            REQUIRE(out.total_known);
        }
    }
}

TEST_CASE("ParseUnsignedDecimal") {
    const std::string s = "123x 18446744073709551615 18446744073709551616 +7";
    size_t pos = 0;
    uint64_t out = 999;
    REQUIRE(HTTP::ParseUnsignedDecimal(s, pos, s.size(), out));
    REQUIRE(out == 123);
    REQUIRE(pos == 3); // stops at the first non-digit
    // no digits at pos: fails and leaves out untouched
    out = 999;
    REQUIRE(!HTTP::ParseUnsignedDecimal(s, pos, s.size(), out));
    REQUIRE(out == 999);
    // UINT64_MAX parses exactly
    pos = 5;
    REQUIRE(HTTP::ParseUnsignedDecimal(s, pos, s.size(), out));
    REQUIRE(out == UINT64_MAX);
    // 2^64 overflows
    pos = 26;
    out = 999;
    REQUIRE(!HTTP::ParseUnsignedDecimal(s, pos, s.size(), out));
    REQUIRE(out == 999);
    // a sign is not a digit
    pos = 46;
    REQUIRE(!HTTP::ParseUnsignedDecimal(s, pos, s.size(), out));
    // empty range
    pos = 0;
    REQUIRE(!HTTP::ParseUnsignedDecimal(s, pos, 0, out));
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
    h["content-length"] = "+42"; // strtoull accepted a sign; the strict reader does not
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
