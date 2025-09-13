/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "type/date32.h"

using namespace omniruntime::type;

namespace Date32Test {
TEST(Date32, add_normal)
{
    auto left = new Date32(20210302);
    auto right = new Date32(302);
    auto result = *left + *right;
    EXPECT_EQ(result.Value(), 20210604);
    delete left;
    delete right;
}

TEST(Date32, add_negate)
{
    auto left = new Date32(20210302);
    auto right = new Date32(-201);
    auto result = *left + *right;
    EXPECT_EQ(result.Value(), 20210101);
    delete left;
    delete right;
}

TEST(Date32, sub_normal)
{
    auto left = new Date32(20210302);
    auto right = new Date32(201);
    auto result = *left - *right;
    EXPECT_EQ(result.Value(), 20210101);
    delete left;
    delete right;
}

TEST(Date32, sub_negate)
{
    auto left = new Date32(20210302);
    auto right = new Date32(-302);
    auto result = *left - *right;
    EXPECT_EQ(result.Value(), 20210604);
    delete left;
    delete right;
}


TEST(Date32, compare_eq)
{
    auto left = new Date32(20210302);
    auto right = new Date32(20210302);
    auto result = *left == *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Date32, compare_ne)
{
    auto left = new Date32(20210302);
    auto right = new Date32(20210303);
    auto result = *left != *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Date32, compare_le1)
{
    auto left = new Date32(20210302);
    auto right = new Date32(20210303);
    auto result = *left <= *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Date32, compare_le2)
{
    auto left = new Date32(20210302);
    auto right = new Date32(20210302);
    auto result = *left <= *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Date32, compare_lt)
{
    auto left = new Date32(20210302);
    auto right = new Date32(20210303);
    auto result = *left < *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Date32, compare_ge1)
{
    auto left = new Date32(20210303);
    auto right = new Date32(20210302);
    auto result = *left >= *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Date32, compare_ge2)
{
    auto left = new Date32(20210302);
    auto right = new Date32(20210302);
    auto result = *left >= *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Date32, compare_gt)
{
    auto left = new Date32(20210303);
    auto right = new Date32(20210302);
    auto result = *left > *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}
}
