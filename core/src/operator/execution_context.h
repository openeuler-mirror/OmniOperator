/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */
#ifndef EXECUTION_CONTEXT_H
#define EXECUTION_CONTEXT_H

#include "memory/simple_arena_allocator.h"

namespace omniruntime {
namespace op {
// execution context during operator
class ExecutionContext {
public:
    explicit ExecutionContext() : arena() {}

    ~ExecutionContext() = default;

    mem::SimpleArenaAllocator *GetArena()
    {
        return &arena;
    }

    void SetError(std::string &message)
    {
        hasError = true;
        errorMessage = message;
    }

    bool HasError() const
    {
        return hasError;
    }

    std::string GetError() const
    {
        return errorMessage;
    }

    void SetError()
    {
        hasError = false;
    }
    char *AllocContinue(size_t totalBytes, char const *& start) {
        if(start == nullptr){
            char *ret = reinterpret_cast<char*>(arena.Allocate(totalBytes));
            start = ret;
            return ret;
        }else {
            return reinterpret_cast<char*>(arena.AllocateContinue(totalBytes,start));
        }
    }
private:
    mem::SimpleArenaAllocator arena;
    bool hasError = false;
    std::string errorMessage;
};
} // namespace op
} // namespace omniruntime
#endif // EXECUTION_CONTEXT_H