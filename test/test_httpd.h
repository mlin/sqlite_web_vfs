#pragma once

#define _FILE_OFFSET_BITS 64
#include <fcntl.h>
#include <microhttpd.h>
#include <stdarg.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <map>
#include <memory>
#include <string>

class TestHTTPd {
    unsigned short port_;
    std::map<std::string, std::string> files_;
    MHD_Daemon *d_ = nullptr;
    unsigned int requests_to_fail_ = 0;
    double p_fail_ = 0.0;

    friend MHD_Result on_request(void *cls, struct MHD_Connection *connection, const char *url,
                                 const char *method, const char *version, const char *upload_data,
                                 size_t *upload_data_size, void **con_cls);

    MHD_Result OnRequest(MHD_Connection *connection, const char *url, const char *method,
                         const char *version, const char *upload_data, size_t *upload_data_size,
                         void **con_cls);

  public:
    virtual ~TestHTTPd();

    bool Start(unsigned short port, const std::map<std::string, std::string> &files);
    void FailNextRequests(unsigned int n) { requests_to_fail_ = n; }
    void SetFailProbability(double p_fail) { p_fail_ = p_fail; }
    void Stop();
};
