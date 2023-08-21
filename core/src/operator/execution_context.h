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
    explicit ExecutionContext(int64_t minChunkSize = 4096) : arena(minChunkSize) {}

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

    void ResetError()
    {
        hasError = false;
    }

private:
    mem::SimpleArenaAllocator arena;
    bool hasError = false;
    std::string errorMessage;
};
} // namespace op
} // namespace omniruntime
#endif // EXECUTION_CONTEXT_H