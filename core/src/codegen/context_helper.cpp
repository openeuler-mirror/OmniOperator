/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry  function  implementation
 */
#include "context_helper.h"

using namespace omniruntime::op;
using namespace omniruntime::type;

namespace omniruntime::codegen {
#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

extern "C" DLLEXPORT
{
    char *ArenaAllocatorMalloc(int64_t contextPtr, int32_t size)
    {
        auto context = reinterpret_cast<ExecutionContext *>(contextPtr);
        return reinterpret_cast<char *>(context->GetArena()->Allocate(size));
    }

    bool ArenaAllocatorReset(int64_t contextPtr)
    {
        auto context = reinterpret_cast<ExecutionContext *>(contextPtr);
        context->GetArena()->Reset();
        return true;
    }

    bool SetError(int64_t contextPtr, std::string errorMessage)
    {
        auto context = reinterpret_cast<ExecutionContext *>(contextPtr);
        if (!context->HasError()) {
            context->SetError(errorMessage);
        }
        return true;
    }

    bool HasError(int64_t contextPtr)
    {
        auto context = reinterpret_cast<ExecutionContext *>(contextPtr);
        return context->HasError();
    }

    std::string GetDataString(DataTypeId type, int count, ...)
    {
        va_list v;
        va_start(v, count);
        std::ostringstream errorMessage;
        switch (type) {
            case OMNI_CHAR:
            case OMNI_VARCHAR:
                errorMessage << "VARCHAR";
                break;
            case OMNI_INT:
                errorMessage << "INTEGER";
                break;
            case OMNI_LONG:
                errorMessage << "BIGINT";
                break;
            case OMNI_TIMESTAMP:
                errorMessage << "TIMESTAMP";
                break;
            case OMNI_DOUBLE:
                errorMessage << "DOUBLE";
                break;
            case OMNI_DECIMAL64:
            case OMNI_DECIMAL128:
                errorMessage << "DECIMAL(" << va_arg(v, int32_t) << ", " << va_arg(v, int32_t) << ")";
                break;
            default:
                errorMessage << "No Support data type";
                break;
        }
        va_end(v);
        return errorMessage.str();
    }
}
}