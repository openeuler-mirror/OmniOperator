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

    ~ExecutionContext() {}

    mem::SimpleArenaAllocator *GetArena()
    {
        return &arena;
    }

    void SetError(std::string message)
    {
        hasError = true;
        errorMessage = std::move(message);
    }

    bool HasError() const
    {
        return hasError;
    }

    std::string GetError() const
    {
        return errorMessage;
    }

private:
    mem::SimpleArenaAllocator arena;
    bool hasError = false;
    std::string errorMessage;
};
} // namespace op
} // namespace omniruntime
#endif // EXECUTION_CONTEXT_H