/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: decimal128 utils
 */

#ifndef OMNI_RUNTIME_DATA_OPERATIONS_H
#define OMNI_RUNTIME_DATA_OPERATIONS_H

#include <string>
#include <regex>

namespace omniruntime {
namespace type {
inline int StringToInt(const std::string &s, int32_t &result)
{
    if (!regex_match(s, std::regex("[[:blank:]]*([+-])?[[:digit:]]+[[:blank:]]*"))) {
        return -1;
    }
    int status = 0;
    try {
        result = stoi(s);
    } catch (std::exception &e) {
        status = 1;
    }
    return status;
}

inline int StringToLong(const std::string &s, int64_t &result)
{
    if (!regex_match(s, std::regex("[[:blank:]]*([+-])?[[:digit:]]+[[:blank:]]*"))) {
        return -1;
    }
    int status = 0;
    try {
        result = stol(s);
    } catch (std::exception &e) {
        status = 1;
    }
    return status;
}

inline int StringToDouble(const std::string &s, double &result)
{
    if (!regex_match(s,
        std::regex("[[:blank:]]*([+-])?[[:digit:]]+([.][[:digit:]]+)?([eE][+-]?[[:digit:]]+)?[[:blank:]]*"))) {
        return -1;
    }
    int status = 0;
    try {
        result = stod(s);
    } catch (std::exception &e) {
        status = 1;
    }
    return status;
}
}
}
#endif // OMNI_RUNTIME_DATA_OPERATIONS_H
