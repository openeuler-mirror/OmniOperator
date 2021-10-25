/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_TRACE_UTIL_H
#define OMNI_RUNTIME_TRACE_UTIL_H

#include <string>
#include <sstream>
#include <execinfo.h>

class TraceUtil {
public:
    static std::string GetStack()
    {
        static int size = 16;
        void *array[size];
        int stack_num = backtrace(array, size);
        if (stack_num < 1) {
            return "";
        }
        std::stringstream ss;
        char **stacktrace = backtrace_symbols(array, stack_num);
        for (int i = 1; i < stack_num - 1; ++i) {
            ss << stacktrace[i] << std::endl << "\t";
        }
        ss << stacktrace[stack_num - 1];
        free(stacktrace);
        return ss.str();
    }
};

#endif // OMNI_RUNTIME_TRACE_UTIL_H
