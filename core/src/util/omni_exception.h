/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 *
 */
#ifndef OMNI_RUNTIME_OMNI_EXCEPTION_H
#define OMNI_RUNTIME_OMNI_EXCEPTION_H

#include <exception>
#include <iostream>
#include "format.h"
#include "trace_util.h"
#include "debug.h"
#include "util/compiler_util.h"

namespace omniruntime::exception {
class OmniException : public std::exception {
public:
    OmniException(const std::string &errorCode, const std::string &message) : errorCode(errorCode), message(message) {}

    OmniException(const char *errorCode, const char *message) : errorCode(errorCode), message(message) {}

    OmniException(const char *message) : errorCode("RUNTIME_ERROR:"), message(message) {}

    OmniException(const std::string &message) : errorCode("RUNTIME_ERROR:"), message(message) {}

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
}

#define OMNI_THROW(errorCode, ...)                                                                                 \
    do {                                                                                                           \
        throw omniruntime::exception::OmniException(errorCode, omniruntime::Format(__VA_ARGS__)); \
    } while (0)

#define OMNI_CHECK(expr, errMessage)                                                 \
    do {                                                                             \
        if (__builtin_expect(!(expr), 0)) {                                           \
            throw omniruntime::exception::OmniException("CHECK_ERROR:", errMessage); \
        }                                                                            \
    } while (0)

#endif // OMNI_RUNTIME_OMNI_EXCEPTION_H
