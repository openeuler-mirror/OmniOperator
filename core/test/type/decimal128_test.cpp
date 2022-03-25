/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "vector/vector_common.h"

using namespace omniruntime::type;

namespace Decimal128Test {
TEST(Decimal128, abs_negate)
{
    auto value = new Decimal128(-12, 2);
    auto result = value->Abs();
    EXPECT_EQ(result.HighBits(), 11);
    EXPECT_EQ(result.LowBits(), 0xFFFFFFFFFFFFFFFE);
    delete value;
}

TEST(Decimal128, abs_positive)
{
    auto value = new Decimal128(12, 2);
    auto result = value->Negate();
    EXPECT_EQ(result.HighBits(), 0xFFFFFFFFFFFFFFF3);
    EXPECT_EQ(result.LowBits(), 0xFFFFFFFFFFFFFFFE);
    delete value;
}

TEST(Decimal128, negate)
{
    auto value = new Decimal128(-12, 2);
    auto result = value->Negate();
    EXPECT_EQ(result.HighBits(), 11);
    EXPECT_EQ(result.LowBits(), 0xFFFFFFFFFFFFFFFE);
    delete value;
}

TEST(Decimal128, add_normal)
{
    auto left = new Decimal128(12, 2);
    auto right = new Decimal128(12, 2);
    auto result = *left + *right;
    EXPECT_EQ(result.HighBits(), 24);
    EXPECT_EQ(result.LowBits(), 4);
    delete left;
    delete right;
}

TEST(Decimal128, add_negate)
{
    auto left = new Decimal128(12, 2);
    auto right = new Decimal128(-12, -2);
    auto result = *left + *right;
    EXPECT_EQ(result.HighBits(), 1);
    EXPECT_EQ(result.LowBits(), 0);
    delete left;
    delete right;
}

TEST(Decimal128, sub_normal)
{
    auto left = new Decimal128(12, 2);
    auto right = new Decimal128(11, 1);
    auto result = *left - *right;
    EXPECT_EQ(result.HighBits(), 1);
    EXPECT_EQ(result.LowBits(), 1);
    delete left;
    delete right;
}

TEST(Decimal128, sub_low_browing)
{
    auto left = new Decimal128(12, 2);
    auto right = new Decimal128(11, 9);
    auto result = *left - *right;
    EXPECT_EQ(result.HighBits(), 0);
    EXPECT_EQ(result.LowBits(), 0xFFFFFFFFFFFFFFF9); // -7
    delete left;
    delete right;
}

TEST(Decimal128, sub_low_browing_negate)
{
    auto left = new Decimal128(11, 2);
    auto right = new Decimal128(11, 9);
    auto result = *left - *right;
    EXPECT_EQ(result.HighBits(), 0xFFFFFFFFFFFFFFFF); // -1
    EXPECT_EQ(result.LowBits(), 0xFFFFFFFFFFFFFFF9);  // -7
    delete left;
    delete right;
}

TEST(Decimal128, multiple_positive)
{
    auto left = new Decimal128(11, 2);
    auto right = new Decimal128(0, 2);
    auto result = *left * *right;
    EXPECT_EQ(result.HighBits(), 22);
    EXPECT_EQ(result.LowBits(), 4);
    delete left;
    delete right;
}

TEST(Decimal128, multiple_negate)
{
    auto left = new Decimal128(0, 2);
    auto right = new Decimal128(0, -1);
    auto result = *left * *right;
    EXPECT_EQ(result.HighBits(), 1);                 // 1
    EXPECT_EQ(result.LowBits(), 0xFFFFFFFFFFFFFFFE); // -2
}

TEST(Decimal128, multiple_integer)
{
    auto left = new Decimal128(6, 2);
    int32_t right = 2;
    auto result = *left * right;
    EXPECT_EQ(result.HighBits(), 12);
    EXPECT_EQ(result.LowBits(), 4);
    delete left;
}

TEST(Decimal128, divide_positive1)
{
    auto left = new Decimal128(12, 2);
    auto right = new Decimal128(12, 2);
    auto result = *left / *right;
    EXPECT_EQ(result.HighBits(), 0);
    EXPECT_EQ(result.LowBits(), 1);
    delete left;
    delete right;
}

TEST(Decimal128, divide_positive2)
{
    auto left = new Decimal128(12, 2);
    auto right = new Decimal128(0, 2);
    auto result = *left / *right;
    EXPECT_EQ(result.HighBits(), 6);
    EXPECT_EQ(result.LowBits(), 1);
    delete left;
    delete right;
}

TEST(Decimal128, divide_positive3)
{
    auto left = new Decimal128(12, 3);
    auto right = new Decimal128(0, 2);
    auto result = *left / *right;
    EXPECT_EQ(result.HighBits(), 6);
    EXPECT_EQ(result.LowBits(), 1);
    delete left;
    delete right;
}

TEST(Decimal128, divide_positive4)
{
    auto left = new Decimal128(24, 5);
    auto right = new Decimal128(12, 2);
    auto result = *left / *right;
    EXPECT_EQ(result.HighBits(), 0);
    EXPECT_EQ(result.LowBits(), 2);
    result = *left % *right;
    EXPECT_EQ(result.HighBits(), 0);
    EXPECT_EQ(result.LowBits(), 1);
    delete left;
    delete right;
}

TEST(Decimal128, divide_positive5)
{
    auto left = new Decimal128(240, 5);
    auto right = new Decimal128(12, 2);
    auto result = *left / *right;
    EXPECT_EQ(result.HighBits(), 0);
    EXPECT_EQ(result.LowBits(), 19);
    delete left;
    delete right;
}

TEST(Decimal128, divide_integer)
{
    auto left = new Decimal128(12, 2);
    auto right = 2;
    auto result = *left / right;
    EXPECT_EQ(result.HighBits(), 6);
    EXPECT_EQ(result.LowBits(), 1);
    delete left;
}

TEST(Decimal128, reminder_no)
{
    auto left = new Decimal128(12, 2);
    auto right = new Decimal128(0, 2);
    auto result = *left % *right;
    EXPECT_EQ(result.HighBits(), 0);
    EXPECT_EQ(result.LowBits(), 0);
    delete left;
    delete right;
}

TEST(Decimal128, reminder)
{
    auto left = new Decimal128(12, 3);
    auto right = new Decimal128(0, 2);
    auto result = *left % *right;
    EXPECT_EQ(result.HighBits(), 0);
    EXPECT_EQ(result.LowBits(), 1);
    delete left;
    delete right;
}

TEST(Decimal128, compare_eq)
{
    auto left = new Decimal128(12, 2);
    auto right = new Decimal128(12, 2);
    auto result = *left == *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Decimal128, compare_ne)
{
    auto left = new Decimal128(12, 3);
    auto right = new Decimal128(12, 2);
    auto result = *left != *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Decimal128, compare_le1)
{
    auto left = new Decimal128(12, 2);
    auto right = new Decimal128(12, 2);
    auto result = *left <= *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Decimal128, compare_le2)
{
    auto left = new Decimal128(-12, 2);
    auto right = new Decimal128(12, 2);
    auto result = *left <= *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Decimal128, compare_lt)
{
    auto left = new Decimal128(12, 1);
    auto right = new Decimal128(12, 2);
    auto result = *left < *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Decimal128, compare_ge1)
{
    auto left = new Decimal128(12, 3);
    auto right = new Decimal128(12, 2);
    auto result = *left >= *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Decimal128, compare_ge2)
{
    auto left = new Decimal128(12, 2);
    auto right = new Decimal128(12, 2);
    auto result = *left >= *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}

TEST(Decimal128, compare_gt)
{
    auto left = new Decimal128(12, 2);
    auto right = new Decimal128(-12, 2);
    auto result = *left > *right;
    EXPECT_EQ(result, true);
    delete left;
    delete right;
}
}
