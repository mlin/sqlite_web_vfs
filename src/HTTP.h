/** Wrappers for libcurl HTTP operations.
 *  Program must curl_global_init(CURL_GLOBAL_ALL) before using this
 */
#pragma once

#include <algorithm>
#include <cctype>
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

namespace HTTP {

// Helper class to scope a CURL handle
class CURLconn {
    CURL *h_;

  public:
    CURLconn() : h_(nullptr) {
        h_ = curl_easy_init();
        if (!h_) {
            throw std::bad_alloc();
        }
    }
    virtual ~CURLconn() {
        if (h_)
            curl_easy_cleanup(h_);
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

// A very simple pool of CURL handles, which can persist server connections in
// between requests. Any number of handles can be checked out; at most 'size'
// handles will be kept once checked back in. (Since we use blocking
// operations, 'size' should probably be set to the number of threads that
// could make concurrent requests)
class CURLpool {
    unsigned int size_;
    std::queue<std::unique_ptr<CURLconn>> pool_;
    std::mutex mu_;

  public:
    CURLpool(const unsigned int size) : size_(size) {}

    std::unique_ptr<CURLconn> checkout() {
        std::lock_guard<std::mutex> lock(mu_);
        std::unique_ptr<CURLconn> ans;
        if (pool_.empty()) {
            ans.reset(new CURLconn());
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

enum class Method { GET, HEAD };
// helper macros
#define CURLcall(call)                                                                             \
    if ((c = call) != CURLE_OK)                                                                    \
    return c
#define CURLsetopt(x, y, z) CURLcall(curl_easy_setopt(x, y, z))

CURLcode request(Method method, const std::string url, const headers &request_headers,
                 long &response_code, headers &response_headers, std::ostream &response_body,
                 CURLpool *pool) {
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

    CURLcall(curl_easy_perform(*conn));

    CURLcall(curl_easy_getinfo(*conn, CURLINFO_RESPONSE_CODE, &response_code));

    if (pool) {
        pool->checkin(conn);
    }

    return CURLE_OK;
}

CURLcode Get(const std::string url, const headers &request_headers, long &response_code,
             headers &response_headers, std::ostream &response_body, CURLpool *pool = nullptr) {
    return request(Method::GET, url, request_headers, response_code, response_headers,
                   response_body, pool);
}

CURLcode Head(const std::string url, const headers &request_headers, long &response_code,
              headers &response_headers, CURLpool *pool = nullptr) {
    std::ostringstream dummy;
    CURLcode ans =
        request(Method::HEAD, url, request_headers, response_code, response_headers, dummy, pool);
    return ans;
}

} // namespace HTTP
