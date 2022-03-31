/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "memory/simple_arena_allocator.h"

using namespace omniruntime::mem;

TEST(SimpleArenaAllocator, testAllocate)
{
    int64_t chunkSize = 4096;
    SimpleArenaAllocator arena(chunkSize);

    // Small allocations should come from the same chunk.
    int64_t smallSize = 100;
    for (int64_t i = 0; i < 20; ++i) {
        auto p = arena.Allocate(smallSize);
        EXPECT_NE(p, nullptr);
        EXPECT_EQ(arena.TotalBytes(), chunkSize);
        EXPECT_EQ(arena.AvailBytes(), chunkSize - (i + 1) * smallSize);
    }

    // large allocations require separate chunks
    int64_t largeSize = 100 * chunkSize;
    auto p = arena.Allocate(largeSize);
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(arena.TotalBytes(), chunkSize + largeSize);
    EXPECT_EQ(arena.AvailBytes(), 0);
}

// small followed by big, then reset
TEST(SimpleArenaAllocator, testResetSmallToBig)
{
    int64_t chunkSize = 4096;
    SimpleArenaAllocator arena(chunkSize);

    int64_t smallSize = 100;
    auto p = arena.Allocate(smallSize);
    EXPECT_NE(p, nullptr);

    int64_t largeSize = 100 * chunkSize;
    p = arena.Allocate(largeSize);
    EXPECT_NE(p, nullptr);

    EXPECT_EQ(arena.TotalBytes(), chunkSize + largeSize);
    EXPECT_EQ(arena.AvailBytes(), 0);
    arena.Reset();
    EXPECT_EQ(arena.TotalBytes(), chunkSize);
    EXPECT_EQ(arena.AvailBytes(), chunkSize);

    // should re-use buffer after reset.
    p = arena.Allocate(smallSize);
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(arena.TotalBytes(), chunkSize);
    EXPECT_EQ(arena.AvailBytes(), chunkSize - smallSize);
}

// big followed by small, then reset
TEST(SimpleArenaAllocator, testResetFromBigToSmall)
{
    int64_t chunkSize = 4096;
    SimpleArenaAllocator arena(chunkSize);

    int64_t largeSize = 100 * chunkSize;
    auto p = arena.Allocate(largeSize);
    EXPECT_NE(p, nullptr);

    int64_t smallSize = 100;
    p = arena.Allocate(smallSize);
    EXPECT_NE(p, nullptr);

    EXPECT_EQ(arena.TotalBytes(), chunkSize + largeSize);
    EXPECT_EQ(arena.AvailBytes(), chunkSize - smallSize);
    arena.Reset();
    EXPECT_EQ(arena.TotalBytes(), largeSize);
    EXPECT_EQ(arena.AvailBytes(), largeSize);

    // should re-use buffer after reset.
    p = arena.Allocate(smallSize);
    EXPECT_NE(p, nullptr);
    EXPECT_EQ(arena.TotalBytes(), largeSize);
    EXPECT_EQ(arena.AvailBytes(), largeSize - smallSize);
}