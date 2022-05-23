/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "type/decimal_operations.h"
#include "vector/vector_common.h"

using namespace omniruntime::type;

namespace Decimal128Test {
TEST(Decimal128, abs_test)
{
    Decimal128 zero = DecimalOperations::UnscaledDecimal(0);
    Decimal128 one = DecimalOperations::UnscaledDecimal(1);
    Decimal128 negativeOne = DecimalOperations::UnscaledDecimal(-1);
    auto result1 = DecimalOperations::AbsExact(zero);
    auto result2 = DecimalOperations::AbsExact(one);
    auto result3 = DecimalOperations::AbsExact(negativeOne);
    EXPECT_EQ(result1.HighBits(), zero.HighBits());
    EXPECT_EQ(result1.LowBits(), zero.LowBits());
    EXPECT_EQ(result2.HighBits(), one.HighBits());
    EXPECT_EQ(result2.LowBits(), one.LowBits());
    EXPECT_EQ(result3.HighBits(), one.HighBits());
    EXPECT_EQ(result3.LowBits(), one.LowBits());
}

TEST(Decimal128, negate)
{
    Decimal128 value = DecimalOperations::UnscaledDecimal(-2);
    Decimal128 result = DecimalOperations::NegateExact(value);
    Decimal128 positive = DecimalOperations::UnscaledDecimal(2);
    EXPECT_EQ(result.HighBits(), positive.HighBits());
    EXPECT_EQ(result.LowBits(), positive.LowBits());
}

TEST(Decimal128, add_normal)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(2);
    Decimal128 right = DecimalOperations::UnscaledDecimal(2);
    Decimal128 result;
    DecimalOperations::AddWithOverflow(left, right, result);
    EXPECT_EQ(result.HighBits(), 0);
    EXPECT_EQ(result.LowBits(), 4);
}

TEST(Decimal128, add_negate)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(2);
    Decimal128 right = DecimalOperations::UnscaledDecimal(-2);
    Decimal128 result;
    DecimalOperations::AddWithOverflow(left, right, result);
    Decimal128 negativeOne = DecimalOperations::UnscaledDecimal(0);
    EXPECT_EQ(result.HighBits(), negativeOne.HighBits());
    EXPECT_EQ(result.LowBits(), negativeOne.LowBits());
}

TEST(Decimal128, subtract_positive_positive)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(2);
    Decimal128 right = DecimalOperations::UnscaledDecimal(1);
    Decimal128 result;
    DecimalOperations::Subtract(left, right, result);
    Decimal128 expected = DecimalOperations::UnscaledDecimal(1);
    EXPECT_EQ(expected, result);
}

TEST(Decimal128, subtract_negative_negative)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(-2);
    Decimal128 right = DecimalOperations::UnscaledDecimal(-1);
    Decimal128 result;
    DecimalOperations::Subtract(left, right, result);
    Decimal128 expected = DecimalOperations::UnscaledDecimal(-1);
    EXPECT_EQ(expected, result);
}

TEST(Decimal128, subtract_positive_negative)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(2);
    Decimal128 right = DecimalOperations::UnscaledDecimal(-1);
    Decimal128 result;
    DecimalOperations::Subtract(left, right, result);
    Decimal128 expected = DecimalOperations::UnscaledDecimal(3);
    EXPECT_EQ(expected, result);
}

TEST(Decimal128, subtract_negative_positive)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(-2);
    Decimal128 right = DecimalOperations::UnscaledDecimal(1);
    Decimal128 result;
    DecimalOperations::Subtract(left, right, result);
    Decimal128 expected = DecimalOperations::UnscaledDecimal(-3);
    EXPECT_EQ(expected, result);
}

TEST(Decimal128, multiple_positive)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(2);
    Decimal128 right = DecimalOperations::UnscaledDecimal(2);
    Decimal128 result;
    DecimalOperations::Multiply(left, right, result);
    EXPECT_EQ(result.HighBits(), 0);
    EXPECT_EQ(result.LowBits(), 4);
}

TEST(Decimal128, multiple_negate)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(-2);
    Decimal128 right = DecimalOperations::UnscaledDecimal(2);
    Decimal128 result;
    DecimalOperations::Multiply(left, right, result);
    Decimal128 expected = DecimalOperations::UnscaledDecimal(-4);
    EXPECT_EQ(result.HighBits(), expected.HighBits());
    EXPECT_EQ(result.LowBits(), expected.LowBits());
}

TEST(Decimal128, multiple_mix)
{
    Decimal128 left = Decimal128(1L<<63, 234527000012345L);
    Decimal128 right = Decimal128(0L, 1000000);
    Decimal128 result;
    DecimalOperations::Multiply(left, right, result);
    Decimal128 expected = Decimal128(-9223372036854775796L, 13166071127830380608);
    EXPECT_EQ(result.HighBits(), expected.HighBits());
    EXPECT_EQ(result.LowBits(), expected.LowBits());
}

TEST(Decimal128, divide_positive1)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(2);
    Decimal128 right = DecimalOperations::UnscaledDecimal(2);
    auto result = DecimalOperations::DivideRoundUp(left, right, 0, 0);
    EXPECT_EQ(result.HighBits(), 0);
    EXPECT_EQ(result.LowBits(), 1);
}

TEST(Decimal128, divide_positive2)
{
    Decimal128 left(12, 2);
    Decimal128 right(0, 2);
    auto result = DecimalOperations::DivideRoundUp(left, right, 0, 0);
    EXPECT_EQ(result.HighBits(), 6);
    EXPECT_EQ(result.LowBits(), 1);
}

TEST(Decimal128, divide_positive3)
{
    Decimal128 left(12, 3);
    Decimal128 right(0, 2);
    auto result = DecimalOperations::DivideRoundUp(left, right, 0, 0);
    EXPECT_EQ(result.HighBits(), 6);
    EXPECT_EQ(result.LowBits(), 2);
}

TEST(Decimal128, divide_dividend_smaller_than_divisor)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(78340625600);
    Decimal128 right = DecimalOperations::UnscaledDecimal(2729300525);
    auto result = DecimalOperations::DivideRoundUp(left, right, 0, 0);
    Decimal128 expected(0, 29);
    EXPECT_EQ(result, expected);
}

TEST(Decimal128, divide_positive_round_up)
{
    Decimal128 left(12, 4);
    Decimal128 right(0, 3);
    auto result = DecimalOperations::DivideRoundUp(left, right, 0, 0);
    EXPECT_EQ(result.HighBits(), 4);
    EXPECT_EQ(result.LowBits(), 1);
}

TEST(Decimal128, divide_negative)
{
    Decimal128 left(Decimal128::SIGN_LONG_MASK, 4);
    Decimal128 right(0, 2);
    auto result = DecimalOperations::DivideRoundUp(left, right, 0, 0);
    Decimal128 expected(Decimal128::SIGN_LONG_MASK, 2);
    EXPECT_EQ(result.HighBits(), expected.HighBits());
    EXPECT_EQ(result.LowBits(), expected.LowBits());
}

TEST(Decimal128, divide_negative1)
{
    Decimal128 left(Decimal128::SIGN_LONG_MASK, 4);
    Decimal128 right(Decimal128::SIGN_LONG_MASK, 2);
    auto result = DecimalOperations::DivideRoundUp(left, right, 0, 0);
    Decimal128 expected(0, 2);
    EXPECT_EQ(result.HighBits(), expected.HighBits());
    EXPECT_EQ(result.LowBits(), expected.LowBits());
}

TEST(Decimal128, divide_negative2)
{
    Decimal128 left(0, 4);
    Decimal128 right(Decimal128::SIGN_LONG_MASK, 2);
    auto result = DecimalOperations::DivideRoundUp(left, right, 0, 0);
    Decimal128 expected(Decimal128::SIGN_LONG_MASK, 2);
    EXPECT_EQ(result.HighBits(), expected.HighBits());
    EXPECT_EQ(result.LowBits(), expected.LowBits());
}

TEST(Decimal128, divide_negative_round_up)
{
    Decimal128 left(0, 4);
    Decimal128 right(Decimal128::SIGN_LONG_MASK, 3);
    auto result = DecimalOperations::DivideRoundUp(left, right, 0, 0);
    Decimal128 expected(Decimal128::SIGN_LONG_MASK, 1);
    EXPECT_EQ(result.HighBits(), expected.HighBits());
    EXPECT_EQ(result.LowBits(), expected.LowBits());
}

TEST(Decimal128, positive_dividend_positive_divisor_and_with_scale_factor)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(124861912500);
    Decimal128 right = DecimalOperations::UnscaledDecimal(1652201977500);
    auto result = DecimalOperations::DivideRoundUp(left, right, 16, 0);
    Decimal128 expected(0, 755730317481720);
    EXPECT_EQ(result, expected);
}

TEST(Decimal128, negative_dividend_positive_divisor_and_with_scale_factor)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(-124861912500);
    Decimal128 right = DecimalOperations::UnscaledDecimal(1652201977500);
    auto result = DecimalOperations::DivideRoundUp(left, right, 16, 0);
    Decimal128 expected(-9223372036854775808, 755730317481720);
    EXPECT_EQ(result, expected);
}

TEST(Decimal128, negative_dividend_negative_divisor_and_with_scale_factor)
{
    Decimal128 left = DecimalOperations::UnscaledDecimal(-124861912500);
    Decimal128 right = DecimalOperations::UnscaledDecimal(-1652201977500);
    auto result = DecimalOperations::DivideRoundUp(left, right, 16, 0);
    Decimal128 expected(0, 755730317481720);
    EXPECT_EQ(result, expected);
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

TEST(Decimal128, compare_negative)
{
    Decimal128 left(0x8000'0000'0000'0000LL, 1);
    Decimal128 right(0x8000'0000'0000'0000LL, 2);
    auto result = left < right;
    EXPECT_EQ(result, false);
}

TEST(Decimal128, div_roundup)
{
    Decimal128 lValue(0x1381e4, 0xfddf26f775600000);
    Decimal128 rValue(0x0, 0x13ba38720);

    Decimal128 expectValue(0x0, 4453370194541067);
    Decimal128 result = DecimalOperations::DivideRoundUp(lValue, rValue, 0, 0);
    EXPECT_EQ(result, expectValue);
}

TEST(Decimal128, div_roundup_2)
{
    Decimal128 lValue(0x000000000c476a81, 0xd22a79fa4fc30000);
    Decimal128 rValue(0x0000000000000000, 0x0000010473b0c563);

    Decimal128 expectValue(0x0, 3397145127548828);
    Decimal128 result = DecimalOperations::DivideRoundUp(lValue, rValue, 0, 0);
    EXPECT_EQ(result, expectValue);
}

TEST(DecimalTest, compare_after_sum)
{
    Decimal128 d1 = DecimalOperations::UnscaledDecimal(-1);
    Decimal128 d2 = DecimalOperations::UnscaledDecimal(-2);
    Decimal128 d3;
    DecimalOperations::AddWithOverflow(d1, d2, d3);
    Decimal128 d4 = DecimalOperations::UnscaledDecimal(-2);
    Decimal128 d5 = DecimalOperations::UnscaledDecimal(-3);
    Decimal128 d6;
    DecimalOperations::AddWithOverflow(d4, d5, d6);
    auto result = d3 > d6;
    EXPECT_EQ(result, true);
}
}
