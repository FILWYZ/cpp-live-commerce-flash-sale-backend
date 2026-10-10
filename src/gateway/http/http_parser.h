#pragma once

#include "common/error/status.h"

#include <string>
#include <string_view>
#include <unordered_map>

namespace live::gateway::http {

struct HttpRequest {
    std::string method;
    std::string target;
    std::string version;
    std::string request_id;
    std::string client_ip;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

class HttpRequestParser {
public:
    static common::Status parse(std::string_view raw, HttpRequest* request);
    // consumed is the number of bytes belonging to the first complete request.
    // It enables HTTP keep-alive and pipelined requests without copying the
    // unread bytes out of the connection buffer.
    static common::Status parse(std::string_view raw, HttpRequest* request,
                                std::size_t* consumed);
};

}  // namespace live::gateway::http
