/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 *
 */
#ifndef OMNI_RUNTIME_OMNI_EXCEPTION_H
#define OMNI_RUNTIME_OMNI_EXCEPTION_H

#include <exception>
#include <iostream>
#include "trace_util.h"
#include "debug.h"

namespace omniruntime {
namespace exception {
class OmniException : public std::exception {
public:
    OmniException(const std::string &errorCode, const std::string &message) : errorCode(errorCode), message(message) {}

    OmniException(const char *errorCode, const char *message) : errorCode(errorCode), message(message) {}

    const char *what() const noexcept override
    {
        FillMessage();
        return elaborateMessage.c_str();
    }

private:
    void FillMessage() const
    {
        elaborateMessage += "Error Code: " + errorCode + "\n";
        elaborateMessage += "Reason: " + message + "\n";
        elaborateMessage += "Stack: " + TraceUtil::GetStack() + "\n";
    }

    std::string errorCode;
    std::string message;
    mutable std::string elaborateMessage;
};
} // namespace omniruntime
} // namespace exception
#endif // OMNI_RUNTIME_OMNI_EXCEPTION_H