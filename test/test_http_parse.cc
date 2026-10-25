// Tests for Content-Range and Content-Length parsing and for retaining
// only the final response's headers, excluding trailers.
#define CATCH_CONFIG_MAIN
#include "catch2/catch.hpp"

#include "../src/HTTP.h"

namespace {

struct CRCase {
    const char *input;
    uint64_t first, last; // the range the caller expects the header to name
    uint64_t expected_total;
    bool allow_unknown_total;
    bool ok;
};

} // namespace

TEST_CASE("MatchesContentRange exact-match table") {
    const CRCase cases[] = {
        // the header names exactly the expected range and total
        {"bytes 0-99/77824", 0, 99, 77824, false, true},
        {"bytes 65536-77823/77824", 65536, 77823, 77824, false, true},
        {"bytes 0-0/1", 0, 0, 1, false, true},
        {"BYTES 10-19/100", 10, 19, 100, false, true},
        {"bYtEs 10-19/100", 10, 19, 100, false, true},
        {"  bytes 10-19/100  ", 10, 19, 100, false, true},
        {"\tbytes 10-19/100\t", 10, 19, 100, false, true},
        {"bytes 007-009/0100", 7, 9, 100, false, true}, // leading zeros are digits
        {"bytes 0-99/100 ", 0, 99, 100, false, true},   // trailing OWS is allowed
        {"bytes 0-99/18446744073709551615", 0, 99, UINT64_MAX, false, true},
        // "*" is accepted only where unknown totals are allowed
        {"bytes 0-99/*", 0, 99, 77824, true, true},
        {"bytes 0-99/*", 0, 99, 77824, false, false},
        {"bytes 0-18446744073709551615/*", 0, UINT64_MAX, 0, true, true},
        {"bytes 18446744073709551615-18446744073709551615/*", UINT64_MAX, UINT64_MAX, 0, true,
         true},
        // the header names a different range than expected
        {"bytes 4096-8191/77824", 0, 4095, 77824, false, false},
        {"bytes 0-99/77824", 0, 98, 77824, false, false},
        {"bytes 0-99/77824", 1, 99, 77824, false, false},
        {"bytes 99-0/100", 0, 99, 100, false, false},
        // a present total must equal the expected total, "*" allowance or not
        {"bytes 0-99/77825", 0, 99, 77824, false, false},
        {"bytes 0-99/77825", 0, 99, 77824, true, false},
        // a total beyond uint64 simply cannot equal the expectation
        {"bytes 0-99/18446744073709551616", 0, 99, 77824, false, false},
        {"bytes 0-99/99999999999999999999999999", 0, 99, 77824, false, false},
        // malformed syntax (the expectation is arbitrary)
        {"bytes 0-99", 0, 99, 100, false, false},                 // missing /total
        {"bytes 0-99/", 0, 99, 100, false, false},                // empty total
        {"bytes /100", 0, 99, 100, false, false},                 // missing range entirely
        {"bytes -99/100", 0, 99, 100, false, false},              // empty first
        {"bytes 0-/100", 0, 99, 100, false, false},               // empty last
        {"bytes 0-99/100 x", 0, 99, 100, false, false},           // junk suffix
        {"bytes 0-99/100x", 0, 99, 100, false, false},            // suffix junk
        {"bytes 0-99/100,200-299/600", 0, 99, 100, false, false}, // multiple ranges
        {"bytes 0-99, 200-299/600", 0, 99, 100, false, false},
        {"bytes +0-99/100", 0, 99, 100, false, false}, // sign
        {"bytes 0-+99/100", 0, 99, 100, false, false},
        {"bytes 0-99/+100", 0, 99, 100, false, false},
        {"bytes -0-99/100", 0, 99, 100, false, false},
        {"bytes 0 -99/100", 0, 99, 100, false, false}, // embedded whitespace
        {"bytes 0- 99/100", 0, 99, 100, false, false},
        {"bytes 0-99 /100", 0, 99, 100, false, false},
        {"bytes 0-99/ 100", 0, 99, 100, false, false},
        {"bytes  0-99/100", 0, 99, 100, false, false}, // two spaces after unit
        {"bytes\t0-99/100", 0, 99, 100, false, false}, // tab is not the grammar's SP
        {"byte 0-99/100", 0, 99, 100, false, false},   // wrong unit
        {"bytesx 0-99/100", 0, 99, 100, false, false},
        {"items 0-99/100", 0, 99, 100, false, false},
        {"bytes 0.0-99/100", 0, 99, 100, false, false},
        {"bytes 0x0-99/100", 0, 99, 100, false, false},
        {"", 0, 99, 100, false, false},
        {" ", 0, 99, 100, false, false},
        {"bytes", 0, 99, 100, false, false},
        {"bytes ", 0, 99, 100, false, false},
        // range endpoints beyond uint64 match no expectation
        {"bytes 18446744073709551616-18446744073709551617/*", 0, 99, 0, true, false},
        {"bytes 0-18446744073709551616/*", 0, 99, 0, true, false},
        {"bytes 99999999999999999999999999-99/100", 0, 99, 100, false, false},
        {"bytes */100", 0, 99, 100, false, false}, // unsatisfied form: not this matcher
        {"bytes */*", 0, 99, 0, true, false},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        const CRCase &c = cases[i];
        CAPTURE(i, c.input);
        REQUIRE(HTTP::MatchesContentRange(c.input, c.first, c.last, c.expected_total,
                                          c.allow_unknown_total) == c.ok);
    }

    // Reject oversized values; accept leading zeros within the limit.
    REQUIRE(!HTTP::MatchesContentRange("bytes " + std::string(300, '0') + "-99/77824", 0, 99, 77824,
                                       false));
    REQUIRE(!HTTP::MatchesContentRange(std::string(300, ' ') + "bytes 0-99/77824", 0, 99, 77824,
                                       false));
    REQUIRE(
        HTTP::MatchesContentRange("bytes 000000000000000000000-99/077824", 0, 99, 77824, false));
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
