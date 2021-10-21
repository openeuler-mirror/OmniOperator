/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include "gtest/gtest.h"
#include "../../src/operator/execution_context.h"

using namespace omniruntime::op;
using namespace std;


TEST(ExecutionContext, testAllocateAndReset)
{
    ExecutionContext *executionContext = new ExecutionContext();
    int64_t chunkSize = 4096;

    // Small allocations should come from the same chunk.
    int64_t smallSize = 100;
    for (int64_t i = 0; i < 20; ++i) {
        auto p = executionContext->getArena()->Allocate(smallSize);
        EXPECT_NE(p, nullptr);
        EXPECT_EQ(executionContext->getArena()->TotalBytes(), chunkSize);
        EXPECT_EQ(executionContext->getArena()->AvailBytes(), chunkSize - (i + 1) * smallSize);
    }

    // large allocations require separate chunks
    int64_t largeSize = 100 * chunkSize;
    auto p = executionContext->getArena()->Allocate(largeSize);
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(executionContext->getArena()->TotalBytes(), chunkSize + largeSize);
    EXPECT_EQ(executionContext->getArena()->AvailBytes(), 0);

    executionContext->getArena()->Reset();
    EXPECT_EQ(executionContext->getArena()->TotalBytes(), chunkSize);
    EXPECT_EQ(executionContext->getArena()->AvailBytes(), chunkSize);

    delete executionContext;
}














