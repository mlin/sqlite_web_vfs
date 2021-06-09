/** Wrappers for libcurl HTTP operations.
 */
#pragma once

#include <algorithm>
#include <curl/curl.h>
#include <functional>
#include <iostream>
#include <list>
#include <locale>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <unistd.h>

// lazycurl: because libcurl.{so,dylib} has a large dependency tree, here's a mechanism to avoid
// linking it at build time, instead using dlopen() and dlsym() so that the program can defer
// loading it until necessary -- by invoking HTTP::global_init().
// Similar overrides may be needed for libcurl API functions we need to use.
#ifdef HTTP_LAZYCURL
#include <dlfcn.h>
extern "C" {
struct lazycurl_api {
    CURLcode (*global_init)(long);
    CURL *(*easy_init)();
    void (*easy_cleanup)(CURL *);
    CURLcode (*easy_getinfo)(CURL *, CURLINFO, ...);
    CURLcode (*easy_setopt)(CURL *, CURLoption, ...);
    CURLcode (*easy_perform)(CURL *);
    char *(*easy_escape)(CURL *, const char *, int);
    char *(*easy_unescape)(CURL *, const char *, int, int *);
    const char *(*easy_strerror)(int);
    curl_slist *(*slist_append)(curl_slist *, const char *);
    void (*slist_free_all)(curl_slist *);
    void (*free)(void *);
};
}
static lazycurl_api __lazycurl;
#define curl_global_init __lazycurl.global_init
#define curl_easy_init __lazycurl.easy_init
#define curl_easy_cleanup __lazycurl.easy_cleanup
#ifdef curl_easy_getinfo
#undef curl_easy_getinfo
#endif
#define curl_easy_getinfo __lazycurl.easy_getinfo
#ifdef curl_easy_setopt
#undef curl_easy_setopt
#endif
#define curl_easy_setopt __lazycurl.easy_setopt
#define curl_easy_perform __lazycurl.easy_perform
#define curl_easy_escape __lazycurl.easy_escape
#define curl_easy_unescape __lazycurl.easy_unescape
#define curl_easy_strerror __lazycurl.easy_strerror
#define curl_slist_append __lazycurl.slist_append
#define curl_slist_free_all __lazycurl.slist_free_all
#define curl_free __lazycurl.free
#endif

namespace HTTP {

#ifndef HTTP_LAZYCURL
CURLcode global_init() { return curl_global_init(CURL_GLOBAL_ALL); }
#else
CURLcode global_init() {
#if defined(__APPLE__)
    static const char *libname[] = {"libcurl.4.dylib", "libcurl.dylib"};
#else
    static const char *libname[] = {"libcurl.so.4", "libcurl.so"};
#endif
    static void *hlib = nullptr;
    if (hlib) {
        return CURLE_OK;
    }
    for (int i = 0; !hlib && i < sizeof(libname) / sizeof(const char *); ++i) {
        hlib = dlopen(libname[i], RTLD_NOW | RTLD_GLOBAL);
    }
    if (!hlib) {
        return CURLE_FAILED_INIT;
    }
    if ((__lazycurl.global_init = (CURLcode(*)(long))dlsym(hlib, "curl_global_init")) &&
        (__lazycurl.easy_init = (CURL(*(*)()))dlsym(hlib, "curl_easy_init")) &&
        (__lazycurl.easy_cleanup = (void (*)(CURL *))dlsym(hlib, "curl_easy_cleanup")) &&
        (__lazycurl.easy_getinfo =
             (CURLcode(*)(CURL *, CURLINFO, ...))dlsym(hlib, "curl_easy_getinfo")) &&
        (__lazycurl.easy_setopt =
             (CURLcode(*)(CURL *, CURLoption, ...))dlsym(hlib, "curl_easy_setopt")) &&
        (__lazycurl.easy_perform = (CURLcode(*)(CURL *))dlsym(hlib, "curl_easy_perform")) &&
        (__lazycurl.easy_escape =
             (char(*(*)(CURL *, const char *, int)))dlsym(hlib, "curl_easy_escape")) &&
        (__lazycurl.easy_unescape =
             (char(*(*)(CURL *, const char *, int, int *)))dlsym(hlib, "curl_easy_unescape")) &&
        (__lazycurl.easy_strerror = (const char(*(*)(int)))dlsym(hlib, "curl_easy_strerror")) &&
        (__lazycurl.slist_append =
             (curl_slist(*(*)(curl_slist *, const char *)))dlsym(hlib, "curl_slist_append")) &&
        (__lazycurl.slist_free_all = (void (*)(curl_slist *))dlsym(hlib, "curl_slist_free_all")) &&
        (__lazycurl.free = (void (*)(void *))dlsym(hlib, "curl_free"))) {
        return __lazycurl.global_init(CURL_GLOBAL_ALL);
    }
    return CURLE_NOT_BUILT_IN;
}
#endif

// Helper class to scope a CURL handle
class CURLconn {
    CURL *h_;

  public:
    CURLconn(bool insecure = false) : h_(nullptr) {
        h_ = curl_easy_init();
        if (!h_) {
            throw std::bad_alloc();
        }
        if (insecure) {
            curl_easy_setopt(h_, CURLOPT_SSL_VERIFYPEER, 0);
            curl_easy_setopt(h_, CURLOPT_SSL_VERIFYHOST, 0);
        }
    }
    virtual ~CURLconn() {
        if (h_) {
            curl_easy_cleanup(h_);
        }
    }
    operator CURL *() const { return h_; }

    bool escape(const std::string &in, std::string &out) {
        char *pOut = curl_easy_escape(h_, in.c_str(), in.size());
        if (!pOut) {
            return false;
        }
        out = pOut;
        curl_free(pOut);
        return true;
    }

    bool unescape(const std::string &in, std::string &out) {
        int outlength = -1;
        char *pOut = curl_easy_unescape(h_, in.c_str(), in.size(), &outlength);
        if (!pOut || outlength < 0) {
            curl_free(pOut);
            return false;
        }
        out.assign(pOut, (size_t)outlength);
        curl_free(pOut);
        return true;
    }
};

// A pool of CURL handles, which can persist server connections in between requests. Any number of
// handles can be checked out; at most `size` handles will be kept one checked back in. (Since we
// use blocking operations, `size` should usually be set to the number of threads that can make
// concurrent requests.)
class CURLpool {
    unsigned int size_;
    bool insecure_;
    std::queue<std::unique_ptr<CURLconn>> pool_;
    std::mutex mu_;
    unsigned int cumulative_connections_ = 0;

  public:
    CURLpool(const unsigned int size, bool insecure = false) : size_(size), insecure_(insecure) {}

    std::unique_ptr<CURLconn> checkout() {
        std::lock_guard<std::mutex> lock(mu_);
        std::unique_ptr<CURLconn> ans;
        if (pool_.empty()) {
            ans.reset(new CURLconn(insecure_));
            ++cumulative_connections_;
        } else {
            ans.reset(pool_.front().release());
            pool_.pop();
        }
        return ans;
    }

    void checkin(std::unique_ptr<CURLconn> &p) {
        std::lock_guard<std::mutex> lock(mu_);
        if (pool_.size() < size_) {
            pool_.push(std::move(p));
        } else {
            p.reset();
        }
    }

    unsigned int cumulative_connections() const { return cumulative_connections_; }
};

using headers = std::map<std::string, std::string>;

// Helper class for providing HTTP request headers to libcurl
class RequestHeadersHelper {
    std::list<std::string> bufs;
    curl_slist *slist_;

  public:
    RequestHeadersHelper(const headers &headers) : slist_(NULL) {
        for (auto it = headers.cbegin(); it != headers.cend(); it++) {
            std::ostringstream stm;
            stm << it->first << ": " << it->second;
            bufs.push_back(stm.str());
            slist_ = curl_slist_append(slist_, bufs.back().c_str());
        }
    }
    virtual ~RequestHeadersHelper() { curl_slist_free_all(slist_); }
    operator curl_slist *() const { return slist_; }
};

// functions for receiving responses from libcurl
size_t writefunction(char *ptr, size_t size, size_t nmemb, void *userdata) {
    size *= nmemb;
    std::ostream *response_body = reinterpret_cast<std::ostream *>(userdata);
    response_body->write(ptr, size);
    if (response_body->fail())
        return 0;
    return size;
}

std::string &ltrim(std::string &s) {
    s.erase(s.begin(),
            std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
    return s;
}

std::string &rtrim(std::string &s) {
    s.erase(
        std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(),
        s.end());
    return s;
}

std::string &trim(std::string &s) { return ltrim(rtrim(s)); }

size_t headerfunction(char *ptr, size_t size, size_t nmemb, void *userdata) {
    size *= nmemb;
    headers &h = *reinterpret_cast<headers *>(userdata);

    size_t sep;
    for (sep = 0; sep < size; sep++) {
        if (ptr[sep] == ':')
            break;
    }

    std::string k, v;
    k.assign(ptr, sep);
    k = trim(k);

    if (k.size()) {
        if (sep < size - 1) {
            v.assign(ptr + sep + 1, size - sep - 1);
            v = trim(v);
            if (v.size()) {
                std::transform(k.begin(), k.end(), k.begin(), ::tolower); // lowercase key
                h[k] = v;
            }
        }
    }

    return size;
}

// Read content-length response header
// return: >= 0 the value
//         -1 header absent
//         -2 header present, but unreadable
long long ReadContentLengthHeader(const headers &response_headers) {
    auto size_it = response_headers.find("content-length");
    if (size_it == response_headers.end()) {
        return 1;
    }
    std::string size = size_it->second;
    size = trim(size);
    const char *size_str = size.c_str();
    char *endptr = nullptr;
    errno = 0;
    unsigned long long file_size = strtoull(size_str, &endptr, 10);
    if (errno || endptr != size_str + size.size() || file_size > LLONG_MAX) {
        return -2;
    }
    return (long long)file_size;
}

enum class Method { GET, HEAD };
// helper macros
#define CURLcall(call)                                                                             \
    if ((c = call) != CURLE_OK)                                                                    \
    return c
#define CURLsetopt(x, y, z) CURLcall(curl_easy_setopt(x, y, z))

CURLcode Request(Method method, const std::string &url, const headers &request_headers,
                 long &response_code, headers &response_headers, std::ostream &response_body,
                 CURLpool *pool = nullptr) {
    CURLcode c;

    std::unique_ptr<CURLconn> conn;

    if (pool) {
        conn = pool->checkout();
    } else {
        conn.reset(new CURLconn());
    }

    CURLsetopt(*conn, CURLOPT_URL, url.c_str());

    switch (method) {
    case Method::GET:
        CURLsetopt(*conn, CURLOPT_HTTPGET, 1);
        break;
    case Method::HEAD:
        CURLsetopt(*conn, CURLOPT_NOBODY, 1);
        break;
    }

    RequestHeadersHelper headers4curl(request_headers);
    CURLsetopt(*conn, CURLOPT_HTTPHEADER, ((curl_slist *)headers4curl));

    CURLsetopt(*conn, CURLOPT_WRITEDATA, &response_body);
    CURLsetopt(*conn, CURLOPT_WRITEFUNCTION, writefunction);

    response_headers.clear();
    CURLsetopt(*conn, CURLOPT_WRITEHEADER, &response_headers);
    CURLsetopt(*conn, CURLOPT_HEADERFUNCTION, headerfunction);

    CURLsetopt(*conn, CURLOPT_FOLLOWLOCATION, 1);
    CURLsetopt(*conn, CURLOPT_MAXREDIRS, 4);
    CURLsetopt(*conn, CURLOPT_CONNECTTIMEOUT, 10);

    CURLcall(curl_easy_perform(*conn));

    CURLcall(curl_easy_getinfo(*conn, CURLINFO_RESPONSE_CODE, &response_code));

    if (pool) {
        pool->checkin(conn);
    }

    return CURLE_OK;
}

CURLcode Get(const std::string &url, const headers &request_headers, long &response_code,
             headers &response_headers, std::ostream &response_body, CURLpool *pool = nullptr) {
    return Request(Method::GET, url, request_headers, response_code, response_headers,
                   response_body, pool);
}

CURLcode Head(const std::string &url, const headers &request_headers, long &response_code,
              headers &response_headers, CURLpool *pool = nullptr) {
    std::ostringstream dummy;
    CURLcode ans =
        Request(Method::HEAD, url, request_headers, response_code, response_headers, dummy, pool);
    return ans;
}

// Parameters controlling request retry logic. Retryable errors:
// - Connection errors
// - 5xx response codes
// - Mismatched content-length header & actual body size
struct RetryOptions {
    // Maximum number of attempts (including the first one)
    unsigned int max_tries = 5;
    // Microseconds to wait before the first retry attempt
    useconds_t initial_delay = 100000;
    // On each subsequent retry, the delay is multiplied by this factor
    unsigned int backoff_factor = 4;

    // Retry if response body has fewer bytes
    size_t min_response_body = 0;

    HTTP::CURLpool *connpool = nullptr;

    // callback to invoke on retryable error (e.g. for logging)
    std::function<void(Method method, const std::string &url, const headers &request_headers,
                       CURLcode rc, long response_code, const headers &response_headers,
                       const std::string &response_body, unsigned int attempt)>
        on_retry = nullptr;
};

CURLcode RetryRequest(Method method, const std::string &url, const headers &request_headers,
                      long &response_code, headers &response_headers, std::string &response_body,
                      const RetryOptions &options) {
    useconds_t delay = options.initial_delay;
    std::ostringstream response_body_stream;
    CURLcode rc;
    response_body.clear();

    for (unsigned int i = 0; i < options.max_tries; ++i) {
        if (i) {
            if (options.on_retry) {
                options.on_retry(method, url, request_headers, rc, response_code, response_headers,
                                 response_body, i + 1);
            }
            usleep(delay);
            delay *= options.backoff_factor;
        }

        response_code = -1;
        response_headers.clear();
        response_body_stream.str(""); // ostringstream::clear() doesn't do this!
        rc = HTTP::Request(method, url, request_headers, response_code, response_headers,
                           response_body_stream, options.connpool);
        if (rc == CURLE_OK) {
            if (response_code >= 200 && response_code < 300) {
                response_body = std::move(response_body_stream.str());
                long long content_length = ReadContentLengthHeader(response_headers);
                if (response_body.size() >= options.min_response_body &&
                    (method == Method::HEAD || content_length < 0 ||
                     response_body.size() == content_length)) {
                    return CURLE_OK;
                }
            } else if (response_code == 429) {
                // TODO: honor Retry-After (within reason)
            } else if (response_code < 500 || response_code >= 600) {
                return CURLE_OK;
            }
        }
    }

    return rc;
}

CURLcode RetryGet(const std::string &url, const headers &request_headers, long &response_code,
                  headers &response_headers, std::string &response_body,
                  const RetryOptions &options) {
    return RetryRequest(Method::GET, url, request_headers, response_code, response_headers,
                        response_body, options);
}

CURLcode RetryHead(const std::string &url, const headers &request_headers, long &response_code,
                   headers &response_headers, const RetryOptions &options) {
    std::string dummy;
    return RetryRequest(Method::HEAD, url, request_headers, response_code, response_headers, dummy,
                        options);
}

} // namespace HTTP
