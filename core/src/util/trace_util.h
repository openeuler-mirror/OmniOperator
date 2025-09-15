/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_TRACE_UTIL_H
#define OMNI_RUNTIME_TRACE_UTIL_H

#include <string>
#include <sstream>
#include <execinfo.h>

#define ARRAY_LEN 16

class TraceUtil {
public:
    static std::string GetStack()
    {
        void *array[ARRAY_LEN];
        int stackNum = backtrace(array, ARRAY_LEN);
        if (stackNum < 1) {
            return "";
        }
        std::stringstream ss;
        char **stacktrace = backtrace_symbols(array, stackNum);
        for (int i = 1; i < stackNum - 1; ++i) {
            ss << stacktrace[i] << std::endl << "\t";
        }
        ss << stacktrace[stackNum - 1];
        free(stacktrace);
        return ss.str();
    }
};

#endif // OMNI_RUNTIME_TRACE_UTIL_H
