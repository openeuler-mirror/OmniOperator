/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include "gtest/gtest.h"
#include "operator/execution_context.h"

using namespace omniruntime::op;
using namespace std;


TEST(ExecutionContext, testAllocateAndReset)
{
    ExecutionContext *executionContext = new ExecutionContext();
    int64_t chunkSize = 4096;

    // Small allocations should come from the same chunk.
    int64_t smallSize = 100;
    for (int64_t i = 0; i < 20; ++i) {
        auto p = executionContext->GetArena()->Allocate(smallSize);
        EXPECT_NE(p, nullptr);
        EXPECT_EQ(executionContext->GetArena()->TotalBytes(), chunkSize);
        EXPECT_EQ(executionContext->GetArena()->AvailBytes(), chunkSize - (i + 1) * smallSize);
    }

    // large allocations require separate chunks
    int64_t largeSize = 100 * chunkSize;
    auto p = executionContext->GetArena()->Allocate(largeSize);
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(executionContext->GetArena()->TotalBytes(), chunkSize + largeSize);
    EXPECT_EQ(executionContext->GetArena()->AvailBytes(), 0);

    executionContext->GetArena()->Reset();
    EXPECT_EQ(executionContext->GetArena()->TotalBytes(), chunkSize);
    EXPECT_EQ(executionContext->GetArena()->AvailBytes(), chunkSize);

    delete executionContext;
}
