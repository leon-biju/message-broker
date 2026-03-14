#ifndef INCLUDE_BROKER_PROTOCOL_HPP_
#define INCLUDE_BROKER_PROTOCOL_HPP_
#include <string>

struct Message {
    int type;
    std::string data;
};

#endif
