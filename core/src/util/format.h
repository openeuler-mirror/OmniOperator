/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Test Native Class
 */

#pragma once

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace omniruntime {
template <typename T>
void ToStringVector(std::vector<std::string> &vec, T &&arg)
{
    std::ostringstream oss;
    oss << (std::forward<T>(arg));
    vec.push_back(oss.str());
}

template <typename T, typename... Args>
void ToStringVector(std::vector<std::string> &vec, T &&arg, Args &&... args)
{
    ToStringVector(vec, std::forward<T>(arg));
    ToStringVector(vec, std::forward<Args>(args)...);
}

template <typename... Args>
std::string Format(const std::string &fmt, Args &&... args)
{
    if constexpr (sizeof...(Args) == 0) {
        return fmt;
    } else {
        std::vector<std::string> argVec;
        ToStringVector(argVec, std::forward<Args>(args)...);

        std::ostringstream oss;
        std::size_t argIndex = 0;
        for (std::size_t i = 0; i < fmt.size(); ++i) {
            if (fmt[i] == '{' && i + 1 < fmt.size() && fmt[i + 1] == '}') {
                if (argIndex < argVec.size()) {
                    oss << argVec[argIndex++];
                    ++i;
                } else {
                    throw std::runtime_error("Not enough arguments for format string");
                }
            } else {
                oss << fmt[i];
            }
        }
        return oss.str();
    }
}
}
