/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "type/decimal_operations.h"
#include "vector/vector_common.h"

using namespace omniruntime::type;

namespace Decimal128Test {
Decimal128 Negate(const Decimal128 &value)
{
    int64_t high = value.HighBits();
    int64_t low = value.LowBits();
    Decimal128 tmp(DecimalOperations::NegateHigh(high, low), low);
    return tmp;
}

bool AssertDivide(Decimal128 &dividend, Decimal128 &divisor, int32_t dividendScaleFactor, int32_t divisorScaleFactor,
    Decimal128 &expectQuotient, Decimal128 &expectRemainder)
{
    if (dividendScaleFactor >= Decimal128::MAX_LONG_PRECISION) {
        std::cout << "error" << std::endl;
    }
    if (divisorScaleFactor >= Decimal128::MAX_LONG_PRECISION) {
        std::cout << "error" << std::endl;
    }

    int64_t dividendHigh = dividend.HighBits();
    int64_t dividendLow = dividend.LowBits();
    int64_t divisorHigh = divisor.HighBits();
    int64_t divisorLow = divisor.LowBits();

    bool dividendIsNegative = dividendHigh < 0;
    bool divisorIsNegative = divisorHigh < 0;
    bool quotientIsNegative = (dividendIsNegative != divisorIsNegative);

    if (dividendIsNegative) {
        int64_t tmpHigh = DecimalOperations::NegateHigh(dividendHigh, dividendLow);
        dividendHigh = tmpHigh;
    }

    if (divisorIsNegative) {
        int64_t tmpHigh = DecimalOperations::NegateHigh(divisorHigh, divisorLow);
        divisorHigh = tmpHigh;
    }

    Decimal128 actualQuotient;
    Decimal128 actualRemainder;
    DecimalOperations::DividePositives(dividendLow, dividendHigh, dividendScaleFactor, divisorLow, divisorHigh,
        divisorScaleFactor, actualQuotient, actualRemainder);
    omniruntime::type::DecimalOperations::ThrowIfOverflows(actualQuotient);
    if (quotientIsNegative) {
        actualQuotient = Negate(actualQuotient);
    }

    if ((actualQuotient.operator == (expectQuotient)) && (actualRemainder.operator == (expectRemainder))) {
        return true;
    } else {
        EXPECT_EQ(actualQuotient, expectQuotient);
        EXPECT_EQ(actualRemainder, expectRemainder);
        return false;
    }
}

bool AssertDivideAllSign(Decimal128 &dividend, Decimal128 &divisor, int32_t dividendScaleFactor,
    int32_t divisorScaleFactor, Decimal128 &expectQuotient, Decimal128 &expectRemainder)
{
    Decimal128 negateDividend = Negate(dividend);
    Decimal128 negateDivisor = Negate(divisor);
    Decimal128 negateExQuotient = Negate(expectQuotient);
    Decimal128 negateExRemainder = Negate(expectRemainder);

    bool allSignResult1 =
        AssertDivide(dividend, divisor, dividendScaleFactor, divisorScaleFactor, expectQuotient, expectRemainder);
    EXPECT_EQ(allSignResult1, true);

    bool allSignResult2 = AssertDivide(dividend, negateDivisor, dividendScaleFactor, divisorScaleFactor,
        negateExQuotient, expectRemainder);
    EXPECT_EQ(allSignResult2, true);

    bool allSignResult3 = AssertDivide(negateDividend, divisor, dividendScaleFactor, divisorScaleFactor,
        negateExQuotient, expectRemainder);
    EXPECT_EQ(allSignResult3, true);

    bool allSignResult4 = AssertDivide(negateDividend, negateDivisor, dividendScaleFactor, divisorScaleFactor,
        expectQuotient, expectRemainder);
    EXPECT_EQ(allSignResult4, true);

    return allSignResult1 && allSignResult2 && allSignResult3 && allSignResult4;
}

static const int DECIMAL128_HALF_BIT_LENGTH = 64;
static const __int128 INT_128_MIN = __int128(1) << 127;
static const __int128 INT_128_MAX = ~INT_128_MIN;
bool AssertAddReturnOverflow(Decimal128 &lvalue, Decimal128 &rvalue)
{
    // expect:
    // overflow = (lvalue + rvalue)/(2^127)
    // res = (lvalue + rvalue)%(2^127)
    Decimal128 result;
    long overflow = DecimalOperations::AddWithOverflow(lvalue, rvalue, result);

    __int128 left = (__int128(lvalue.HighBits() & (~(1L << 63))) << 64) + lvalue.LowBits();
    __int128 right = (__int128(rvalue.HighBits() & (~(1L << 63))) << 64) + rvalue.LowBits();
    __int128 res = (__int128(result.HighBits() & (~(1L << 63))) << 64) + result.LowBits();

    left *= ((lvalue.HighBits() >> (DECIMAL128_HALF_BIT_LENGTH - 1)) & 1) == 1 ? -1 : 1;
    right *= ((rvalue.HighBits() >> (DECIMAL128_HALF_BIT_LENGTH - 1)) & 1) == 1 ? -1 : 1;
    res *= ((result.HighBits() >> (DECIMAL128_HALF_BIT_LENGTH - 1)) & 1) == 1 ? -1 : 1;

    __int128 actualResult = left + right;
    __int128 quotient = 0;
    __int128 remainder = 0;
    if (left > 0 && right > 0 && (actualResult) < 0) {
        // actualResult = INT_128_MIN + overflow - 1
        quotient = 1;
        remainder = (actualResult - INT_128_MIN + 1) - 1;
    } else if (left < 0 && right < 0 && actualResult > 0) {
        // actualResult = INT_128_MAX + overflow + 1
        quotient = -1;
        remainder = actualResult - INT_128_MAX - 1;
    } else if (actualResult == INT_128_MIN) {
        // actualResult = -2^127
        quotient = -1;
        remainder = 0;
    } else {
        // -2^127 < actualResult <= 2^127 -1
        quotient = 0;
        remainder = actualResult;
    }
    return quotient == overflow && (res == remainder);
}

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
    Decimal128 value = DecimalOperations::UnscaledDecimal(2);
    Decimal128 result = DecimalOperations::NegateExact(value);
    Decimal128 positive = DecimalOperations::UnscaledDecimal(-2);
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
    Decimal128 left = Decimal128(1L << 63, 234527000012345L);
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


TEST(Decimal128, div_roundup_3)
{
    Decimal128 lValue1(0x2, 0x0000000300000004);
    Decimal128 rValue1(0x100000002, 0x0000000300000004);
    Decimal128 expectQuotient1(0x0, 0x0);
    Decimal128 expectRemainder1(0x2, 0x0000000300000004);
    bool result1 = AssertDivideAllSign(lValue1, rValue1, 0, 0, expectQuotient1, expectRemainder1);
    EXPECT_EQ(result1, true);

    Decimal128 lValue2(0x0, 0x03);
    Decimal128 rValue2(0x0, 0xffffffff);
    Decimal128 expectQuotient2(0x0, 0x0);
    Decimal128 expectRemainder2(0x0, 0x03);
    bool result2 = AssertDivideAllSign(lValue2, rValue2, 0, 0, expectQuotient2, expectRemainder2);
    EXPECT_EQ(result2, true);

    Decimal128 lValue3(0x0, 0xffffffff);
    Decimal128 rValue3(0x0, 0x01);
    Decimal128 expectQuotient3(0x0, 0xffffffff);
    Decimal128 expectRemainder3(0x0, 0x00);
    bool result3 = AssertDivideAllSign(lValue3, rValue3, 0, 0, expectQuotient3, expectRemainder3);
    EXPECT_EQ(result3, true);

    Decimal128 lValue4(0x0, 0xffffffff);
    Decimal128 rValue4(0x0, 0xffffffff);
    Decimal128 expectQuotient4(0x0, 0x01);
    Decimal128 expectRemainder4(0x0, 0x00);
    bool result4 = AssertDivideAllSign(lValue4, rValue4, 0, 0, expectQuotient4, expectRemainder4);
    EXPECT_EQ(result4, true);

    Decimal128 lValue5(0x0, 0xffffffff);
    Decimal128 rValue5(0x0, 0x03);
    Decimal128 expectQuotient5(0x0, 0x55555555);
    Decimal128 expectRemainder5(0x0, 0x00);
    bool result5 = AssertDivideAllSign(lValue5, rValue5, 0, 0, expectQuotient5, expectRemainder5);
    EXPECT_EQ(result5, true);

    // 18446744073709551615/4294967295
    Decimal128 lValue6(0x0, 0xffffffffffffffff);
    Decimal128 rValue6(0x0, 0xffffffff);
    Decimal128 expectQuotient6(0x0, 0x100000001);
    Decimal128 expectRemainder6(0x0, 0x00);
    bool result6 = AssertDivideAllSign(lValue6, rValue6, 0, 0, expectQuotient6, expectRemainder6);
    EXPECT_EQ(result6, true);

    // 18446744069414584319/4294967295
    Decimal128 lValue7(0x0, 0xfffffffeffffffff);
    Decimal128 rValue7(0x0, 0xffffffff);
    Decimal128 expectQuotient7(0x0, 0xffffffff);
    Decimal128 expectRemainder7(0x0, 0xfffffffe);
    bool result7 = AssertDivideAllSign(lValue7, rValue7, 0, 0, expectQuotient7, expectRemainder7);
    EXPECT_EQ(result7, true);

    // 20014547621496/39612
    Decimal128 lValue8(0x0, 0x0000123400005678);
    Decimal128 rValue8(0x0, 0x00009abc);
    Decimal128 expectQuotient8(0x0, 0x1e1dba76);
    Decimal128 expectRemainder8(0x0, 0x6bd0);
    bool result8 = AssertDivideAllSign(lValue8, rValue8, 0, 0, expectQuotient8, expectRemainder8);
    EXPECT_EQ(result8, true);

    // 30064771072/12884901888
    Decimal128 lValue9(0x0, 0x700000000);
    Decimal128 rValue9(0x0, 0x300000000);
    Decimal128 expectQuotient9(0x0, 0x02);
    Decimal128 expectRemainder9(0x0, 0x100000000);
    bool result9 = AssertDivideAllSign(lValue9, rValue9, 0, 0, expectQuotient9, expectRemainder9);
    EXPECT_EQ(result9, true);

    // 30064771077/12884901888
    Decimal128 lValue10(0x0, 0x700000005);
    Decimal128 rValue10(0x0, 0x300000000);
    Decimal128 expectQuotient10(0x0, 0x02);
    Decimal128 expectRemainder10(0x0, 0x100000005);
    bool result10 = AssertDivideAllSign(lValue10, rValue10, 0, 0, expectQuotient10, expectRemainder10);
    EXPECT_EQ(result10, true);

    // 25769803776/8589934592
    Decimal128 lValue11(0x0, 0x600000000);
    Decimal128 rValue11(0x0, 0x200000000);
    Decimal128 expectQuotient11(0x0, 0x03);
    Decimal128 expectRemainder11(0x0, 0x00);
    bool result11 = AssertDivideAllSign(lValue11, rValue11, 0, 0, expectQuotient11, expectRemainder11);
    EXPECT_EQ(result11, true);

    // 2147483648/1073741825
    Decimal128 lValue12(0x0, 0x80000000);
    Decimal128 rValue12(0x0, 0x40000001);
    Decimal128 expectQuotient12(0x0, 0x01);
    Decimal128 expectRemainder12(0x0, 0x3fffffff);
    bool result12 = AssertDivideAllSign(lValue12, rValue12, 0, 0, expectQuotient12, expectRemainder12);
    EXPECT_EQ(result12, true);

    // 9223372036854775808/1073741825
    Decimal128 lValue13(0x0, 0x8000000000000000);
    Decimal128 rValue13(0x0, 0x40000001);
    Decimal128 expectQuotient13(0x0, 0x1fffffff8);
    Decimal128 expectRemainder13(0x0, 0x08);
    bool result13 = AssertDivideAllSign(lValue13, rValue13, 0, 0, expectQuotient13, expectRemainder13);
    EXPECT_EQ(result13, true);

    // 9223372036854775808/4611686018427387905
    Decimal128 lValue14(0x0, 0x8000000000000000);
    Decimal128 rValue14(0x0, 0x4000000000000001);
    Decimal128 expectQuotient14(0x0, 0x01);
    Decimal128 expectRemainder14(0x0, 0x3fffffffffffffff);
    bool result14 = AssertDivideAllSign(lValue14, rValue14, 0, 0, expectQuotient14, expectRemainder14);
    EXPECT_EQ(result14, true);

    // 207661668792474/207661668792474
    Decimal128 lValue15(0x0, 0x0000bcde0000789a);
    Decimal128 rValue15(0x0, 0x0000bcde0000789a);
    Decimal128 expectQuotient15(0x0, 0x01);
    Decimal128 expectRemainder15(0x0, 0x00);
    bool result15 = AssertDivideAllSign(lValue15, rValue15, 0, 0, expectQuotient15, expectRemainder15);
    EXPECT_EQ(result15, true);

    // 207661668792475/207661668792474
    Decimal128 lValue16(0x0, 0x0000bcde0000789b);
    Decimal128 rValue16(0x0, 0x0000bcde0000789a);
    Decimal128 expectQuotient16(0x0, 0x01);
    Decimal128 expectRemainder16(0x0, 0x01);
    bool result16 = AssertDivideAllSign(lValue16, rValue16, 0, 0, expectQuotient16, expectRemainder16);
    EXPECT_EQ(result16, true);

    // 207661668792473/207661668792474
    Decimal128 lValue17(0x0, 0x0000bcde00007899);
    Decimal128 rValue17(0x0, 0x0000bcde0000789a);
    Decimal128 expectQuotient17(0x0, 0x00);
    Decimal128 expectRemainder17(0x0, 0x0000bcde00007899);
    bool result17 = AssertDivideAllSign(lValue17, rValue17, 0, 0, expectQuotient17, expectRemainder17);
    EXPECT_EQ(result17, true);

    // 281470681808895/281470681808895
    Decimal128 lValue18(0x0, 0x0000ffff0000ffff);
    Decimal128 rValue18(0x0, 0x0000ffff0000ffff);
    Decimal128 expectQuotient18(0x0, 0x01);
    Decimal128 expectRemainder18(0x0, 0x00);
    bool result18 = AssertDivideAllSign(lValue18, rValue18, 0, 0, expectQuotient18, expectRemainder18);
    EXPECT_EQ(result18, true);

    // 281470681808895/281470681743360
    Decimal128 lValue19(0x0, 0x0000ffff0000ffff);
    Decimal128 rValue19(0x0, 0x0000ffff00000000);
    Decimal128 expectQuotient19(0x0, 0x01);
    Decimal128 expectRemainder19(0x0, 0xffff);
    bool result19 = AssertDivideAllSign(lValue19, rValue19, 0, 0, expectQuotient19, expectRemainder19);
    EXPECT_EQ(result19, true);

    // 5368002601758163503531/4294967296
    Decimal128 lValue20(0x0123, 0x00004567000089ab);
    Decimal128 rValue20(0x0, 0x100000000);
    Decimal128 expectQuotient20(0x0, 0x12300004567);
    Decimal128 expectRemainder20(0x0, 0x89ab);
    bool result20 = AssertDivideAllSign(lValue20, rValue20, 0, 0, expectQuotient20, expectRemainder20);
    EXPECT_EQ(result20, true);

    // 604462910088780974129152/140737488420863
    Decimal128 lValue21(0x8000, 0x0000fffe00000000);
    Decimal128 rValue21(0x0, 0x80000000ffff);
    Decimal128 expectQuotient21(0x0, 0xffffffff);
    Decimal128 expectRemainder21(0x0, 0x7fff0000ffff);
    bool result21 = AssertDivideAllSign(lValue21, rValue21, 0, 0, expectQuotient21, expectRemainder21);
    EXPECT_EQ(result21, true);

    // 39614081257132168796771975171/9903520314283042199192993793
    Decimal128 lValue22(0x80000000, 0x0000000000000003);
    Decimal128 rValue22(0x20000000, 0x0000000000000001);
    Decimal128 expectQuotient22(0x0, 0x03);
    Decimal128 expectRemainder22(0x20000000, 0x00);
    bool result22 = AssertDivideAllSign(lValue22, rValue22, 0, 0, expectQuotient22, expectRemainder22);
    EXPECT_EQ(result22, true);

    // 604462909807314587353091/151115727451828646838273
    Decimal128 lValue23(0x8000, 0x0000000000000003);
    Decimal128 rValue23(0x2000, 0x0000000000000001);
    Decimal128 expectQuotient23(0x0, 0x03);
    Decimal128 expectRemainder23(0x2000, 0x00);
    bool result23 = AssertDivideAllSign(lValue23, rValue23, 0, 0, expectQuotient23, expectRemainder23);
    EXPECT_EQ(result23, true);

    // 2596069201709362459734969208012800/604462909807314587353089
    Decimal128 lValue24(0x00007fff00008000, 0x00);
    Decimal128 rValue24(0x8000, 0x0000000000000001);
    Decimal128 expectQuotient24(0x0, 0xfffe0000);
    Decimal128 expectRemainder24(0x7fff, 0xffffffff00020000);
    bool result24 = AssertDivideAllSign(lValue24, rValue24, 0, 0, expectQuotient24, expectRemainder24);
    EXPECT_EQ(result24, true);

    // 2596148429267413814546714551386112/604462909807314587418623
    Decimal128 lValue25(0x800000000000, 0x0000fffe00000000);
    Decimal128 rValue25(0x8000, 0xffff);
    Decimal128 expectQuotient25(0x0, 0xffffffff);
    Decimal128 expectRemainder25(0x7fff, 0xffffffff0000ffff);
    bool result25 = AssertDivideAllSign(lValue25, rValue25, 0, 0, expectQuotient25, expectRemainder25);
    EXPECT_EQ(result25, true);

    // -18446744065119617024/39614081257132168796772040703
    Decimal128 lValue26(0x8000000000000000, 0xfffffffe00000000);
    Decimal128 rValue26(0x80000000, 0x0000ffff);
    Decimal128 expectQuotient26(0x8000000000000000, 0x00);
    Decimal128 expectRemainder26(0x00, 0xfffffffe00000000);
    bool result26 = AssertDivideAllSign(lValue26, rValue26, 0, 0, expectQuotient26, expectRemainder26);
    EXPECT_EQ(result26, true);

    // -18446744065119617024/39614081257132168801066942463
    Decimal128 lValue27(0x8000000000000000, 0xfffffffe00000000);
    Decimal128 rValue27(0x80000000, 0xffffffff);
    Decimal128 expectQuotient27(0x8000000000000000, 0x00);
    Decimal128 expectRemainder27(0x00, 0xfffffffe00000000);
    bool result27 = AssertDivideAllSign(lValue27, rValue27, 0, 0, expectQuotient27, expectRemainder27);
    EXPECT_EQ(result27, true);

    // with scale
    // 100000000000000000000000/111111111111111111111111
    Decimal128 lValue28(0x152d, 0x02c7e14af6800000);
    Decimal128 rValue28(0x1787, 0x586c4fa8a01c71c7);
    Decimal128 expectQuotient28(0x0, 0x00);
    Decimal128 expectRemainder28(0x0000314dc6448d93, 0x38c15b0a00000000);
    bool result28 = AssertDivideAllSign(lValue28, rValue28, 10, 10, expectQuotient28, expectRemainder28);
    EXPECT_EQ(result28, true);

    // 100000000000000000000000/111111111111111111111111
    Decimal128 lValue29(0x152d, 0x02c7e14af6800000);
    Decimal128 rValue29(0x1787, 0x19debd01c7);
    Decimal128 expectQuotient29(0x0, 0x00);
    Decimal128 expectRemainder29(0x0785ee10d5da46d9, 0x00f436a000000000);
    bool result29 = AssertDivideAllSign(lValue29, rValue29, 14, 14, expectQuotient29, expectRemainder29);
    EXPECT_EQ(result29, true);

    // 99999999999999999999999999999999999999/99999999999999999999999999999999999999
    Decimal128 lValue30(0x4b3b4ca85a86c47a, 0x098a223fffffffff);
    Decimal128 rValue30(0x4b3b4ca85a86c47a, 0x098a223fffffffff);
    Decimal128 expectQuotient30(0x0, 0x01);
    Decimal128 expectRemainder30(0x00, 0x00);
    bool result30 = AssertDivideAllSign(lValue30, rValue30, 0, 0, expectQuotient30, expectRemainder30);
    EXPECT_EQ(result30, true);

    // test throw,omniruntme dont support decimal256,but olk is support this case
    // 99999999999999999999999999999999999999/99999999999999999999999999999999999999
    Decimal128 lValue31(0x4b3b4ca85a86c47a, 0x098a223fffffffff);
    Decimal128 rValue31(0x4b3b4ca85a86c47a, 0x098a223fffffffff);
    Decimal128 expectQuotient31(0x0, 0x0a);
    Decimal128 expectRemainder31(0x00, 0x00);
    EXPECT_THROW(AssertDivideAllSign(lValue31, rValue31, 2, 1, expectQuotient31, expectRemainder31), std::exception);

    // test throw
    Decimal128 lValue32(0x4B3B4CA85A86C47B, 0x00);
    Decimal128 rValue32(0x00, 0x01);
    Decimal128 expectQuotient32(0x0, 0x0a);
    Decimal128 expectRemainder32(0x0, 0x00);
    EXPECT_THROW(AssertDivideAllSign(lValue32, rValue32, 0, 0, expectQuotient32, expectRemainder32), std::exception);
}


TEST(Decimal128, add)
{
    // 0 + 0 = 0
    Decimal128 lValue1(0x0, 0x0);
    Decimal128 rValue1(0x0, 0x0);
    Decimal128 result1;
    DecimalOperations::AddWithOverflow(lValue1, rValue1, result1);
    EXPECT_EQ(Decimal128(0x0, 0x0), result1);

    // 1 + 0 = 1
    Decimal128 lValue2(0x0, 0x1);
    Decimal128 rValue2(0x0, 0x0);
    Decimal128 result2;
    DecimalOperations::AddWithOverflow(lValue2, rValue2, result2);
    EXPECT_EQ(Decimal128(0x0, 0x1), result2);

    // 1 + 1 = 2
    Decimal128 lValue3(0x0, 0x1);
    Decimal128 rValue3(0x0, 0x1);
    Decimal128 result3;
    DecimalOperations::AddWithOverflow(lValue3, rValue3, result3);
    EXPECT_EQ(Decimal128(0x0, 0x2), result3);

    // 4294967296 + 0 = 4294967296
    Decimal128 lValue4(0x0, 0x100000000);
    Decimal128 rValue4(0x0, 0x0);
    Decimal128 result4;
    DecimalOperations::AddWithOverflow(lValue4, rValue4, result4);
    EXPECT_EQ(Decimal128(0x0, 0x100000000), result4);

    // 2147483648 + 2147483648 = 4294967296
    Decimal128 lValue5(0x0, 0x80000000);
    Decimal128 rValue5(0x0, 0x80000000);
    Decimal128 result5;
    DecimalOperations::AddWithOverflow(lValue5, rValue5, result5);
    EXPECT_EQ(Decimal128(0x0, 0x100000000), result5);

    // 4294967296 + 8589934592 = 12884901888
    Decimal128 lValue6(0x0, 0x100000000);
    Decimal128 rValue6(0x0, 0x200000000);
    Decimal128 result6;
    DecimalOperations::AddWithOverflow(lValue6, rValue6, result6);
    EXPECT_EQ(Decimal128(0x0, 0x300000000), result6);
}

TEST(Decimal128, DISABLED_addReturnOverflow)
{
    // 99999999999999999999999999999999999999 + 99999999999999999999999999999999999999
    Decimal128 lValue2(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 rValue2(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    EXPECT_TRUE(AssertAddReturnOverflow(lValue2, rValue2));

    // -99999999999999999999999999999999999999 + (-99999999999999999999999999999999999999)
    Decimal128 lValue5(0xCB3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 rValue5(0xCB3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    EXPECT_TRUE(AssertAddReturnOverflow(lValue5, rValue5));
}

TEST(Decimal128, addReturnOverflow)
{
    // 2 + 2
    Decimal128 lValue1(0x0, 0x2);
    Decimal128 rValue1(0x0, 0x2);
    EXPECT_TRUE(AssertAddReturnOverflow(lValue1, rValue1));

    // -99999999999999999999999999999999999999 + 99999999999999999999999999999999999999
    Decimal128 lValue3(0xCB3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 rValue3(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    EXPECT_TRUE(AssertAddReturnOverflow(lValue3, rValue3));

    // 99999999999999999999999999999999999999 + (-99999999999999999999999999999999999999)
    Decimal128 lValue4(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 rValue4(0xCB3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    EXPECT_TRUE(AssertAddReturnOverflow(lValue4, rValue4));

    // -99999999999999999999999999999999999998 + 1
    Decimal128 lValue6(0xCB3B4CA85A86C47A, 0x98A223FFFFFFFFE);
    Decimal128 rValue6(0x0, 0x1);
    EXPECT_TRUE(AssertAddReturnOverflow(lValue6, rValue6));
}

TEST(Decimal128, multiply)
{
    // 0 * MAX_DECIMAL = 0
    Decimal128 lValue1(0x0, 0x0);
    Decimal128 rValue1(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 result1;
    DecimalOperations::Multiply(lValue1, rValue1, result1);
    EXPECT_EQ(Decimal128(0x0, 0x0), result1);

    // 1 * MAX_DECIMAL = MAX_DECIMAL
    Decimal128 lValue2(0x0, 0x1);
    Decimal128 rValue2(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 result2;
    DecimalOperations::Multiply(lValue2, rValue2, result2);
    EXPECT_EQ(Decimal128(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF), result2);

    // 1 * MIN_DECIMAL = MIN_DECIMAL
    Decimal128 lValue3(0x0, 0x1);
    Decimal128 rValue3(0xCB3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 result3;
    DecimalOperations::Multiply(lValue3, rValue3, result3);
    EXPECT_EQ(Decimal128(0xCB3B4CA85A86C47A, 0x98A223FFFFFFFFF), result3);

    // -1 * MAX_DECIMAL = MIN_DECIMAL
    Decimal128 lValue4(0x8000000000000000, 0x1);
    Decimal128 rValue4(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 result4;
    DecimalOperations::Multiply(lValue4, rValue4, result4);
    EXPECT_EQ(Decimal128(0xCB3B4CA85A86C47A, 0x98A223FFFFFFFFF), result4);

    // -1 * MIN_DECIMAL = MAX_DECIMAL
    Decimal128 lValue5(0x8000000000000000, 0x1);
    Decimal128 rValue5(0xCB3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 result5;
    DecimalOperations::Multiply(lValue5, rValue5, result5);
    EXPECT_EQ(Decimal128(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF), result5);

    // 18446744073709551615 * 72057594037927935 = 1329227995784915854385005392532865025
    Decimal128 lValue6(0x0, 0xFFFFFFFFFFFFFFFF);
    Decimal128 rValue6(0x0, 0xFFFFFFFFFFFFFF);
    Decimal128 result6;
    DecimalOperations::Multiply(lValue6, rValue6, result6);
    EXPECT_EQ(Decimal128(0xFFFFFFFFFFFFFE, 0xFF00000000000001), result6);

    // 18446742976727070720 * 4107341382742775296 = 75767070805130745462670906105260933120
    Decimal128 lValue7(0x0, 0xFFFFFF0096BFB800);
    Decimal128 rValue7(0x0, 0x39003539D9A51600);
    Decimal128 result7;
    DecimalOperations::Multiply(lValue7, rValue7, result7);
    EXPECT_EQ(Decimal128(0x39003500FB00AB76, 0x1CDBB17E11D00000), result7);

    // Integer.MAX_VALUE * Integer.MIN_VALUE = -4611686016279904256
    Decimal128 lValue8(0x0, 0x7FFFFFFF);
    Decimal128 rValue8(0x8000000000000000, 0x80000000);
    Decimal128 result8;
    DecimalOperations::Multiply(lValue8, rValue8, result8);
    EXPECT_EQ(Decimal128(0x8000000000000000, 0x3FFFFFFF80000000), result8);

    // 99999999999999 * -1000000000000000000000000 = -99999999999999000000000000000000000000
    Decimal128 lValue9(0x0, 0x5AF3107A3FFF);
    Decimal128 rValue9(0x800000000000D3C2, 0x1BCECCEDA1000000);
    Decimal128 result9;
    DecimalOperations::Multiply(lValue9, rValue9, result9);
    EXPECT_EQ(Decimal128(0xCB3B4CA85A85F0B7, 0xEDBB55525F000000), result9);

    // 12380837221737387489365741632769922889 * 3 = 37142511665212162468097224898309768667
    Decimal128 lValue10(0x950766754840EB8, 0xDF69328A17B69749);
    Decimal128 rValue10(0x0, 0x3);
    Decimal128 result10;
    DecimalOperations::Multiply(lValue10, rValue10, result10);
    EXPECT_EQ(Decimal128(0x1BF16335FD8C2C2A, 0x9E3B979E4723C5DB), result10);
}

TEST(Decimal128, multiplyByInt)
{
    // 0 * 1 = 0
    Decimal128 lValue1(0x0, 0x0);
    Decimal128 rValue1(0x0, 0x1);
    Decimal128 result1;
    DecimalOperations::Multiply(lValue1, rValue1, result1);
    EXPECT_EQ(Decimal128(0x0, 0x0), result1);

    // 2 * Integer.MAX_VALUE = 4294967294
    Decimal128 lValue2(0x0, 0x2);
    Decimal128 rValue2(0x0, 0x7FFFFFFF);
    Decimal128 result2;
    DecimalOperations::Multiply(lValue2, rValue2, result2);
    EXPECT_EQ(Decimal128(0x0, 0xFFFFFFFE), result2);

    // Integer.MAX_VALUE * -3 = -6442450941
    Decimal128 lValue3(0x0, 0x7FFFFFFF);
    Decimal128 rValue3(0x8000000000000000, 0x3);
    Decimal128 result3;
    DecimalOperations::Multiply(lValue3, rValue3, result3);
    EXPECT_EQ(Decimal128(0x8000000000000000, 0x17FFFFFFD), result3);

    // Integer.MIN_VALUE * -3 = 6442450944
    Decimal128 lValue4(0x8000000000000000, 0x80000000);
    Decimal128 rValue4(0x8000000000000000, 0x3);
    Decimal128 result4;
    DecimalOperations::Multiply(lValue4, rValue4, result4);
    EXPECT_EQ(Decimal128(0x0, 0x180000000), result4);

    // 1267650600228229401496703205375 * 2 = 2535301200456458802993406410750
    Decimal128 lValue5(0xFFFFFFFFF, 0xFFFFFFFFFFFFFFFF);
    Decimal128 rValue5(0x0, 0x2);
    Decimal128 result5;
    DecimalOperations::Multiply(lValue5, rValue5, result5);
    EXPECT_EQ(Decimal128(0x1FFFFFFFFF, 0xFFFFFFFFFFFFFFFE), result5);
}

TEST(Decimal128, multiplyOverflow)
{
    // 99999999999999 * -10000000000000000000000000
    Decimal128 lValue1(0x0, 0x5AF3107A3FFF);
    Decimal128 rValue1(0x8000000000108B2A, 0x161401484A000000);
    Decimal128 result1;
    EXPECT_THROW(DecimalOperations::Multiply(lValue1, rValue1, result1), std::exception);

    // MAX_DECIMAL * 10
    Decimal128 lValue2(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 rValue2(0x0, 0xA);
    Decimal128 result2;
    EXPECT_THROW(DecimalOperations::Multiply(lValue2, rValue2, result2), std::exception);
}

TEST(Decimal128, shiftLeftMultiPrecision)
{
    std::vector<int32_t> number1 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    std::vector<int32_t> result1 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    DecimalOperations::ShiftLeftMultiPrecision(number1, 4, 0);
    EXPECT_EQ(number1, result1);

    std::vector<int32_t> number2 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    std::vector<int32_t> result2 { static_cast<int32_t>(0b01000010100010110100001010001010),
        static_cast<int32_t>(0b10101101001011010110101010101011),
        static_cast<int32_t>(0b10100101111100011111000101010100),
        static_cast<int32_t>(0b11111110000000110101010101010110),
        static_cast<int32_t>(0b00000000000000000000000000000001) };
    DecimalOperations::ShiftLeftMultiPrecision(number2, 5, 1);
    EXPECT_EQ(number2, result2);

    std::vector<int32_t> number3 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    std::vector<int32_t> result3 { static_cast<int32_t>(0b10000000000000000000000000000000),
        static_cast<int32_t>(0b11010000101000101101000010100010),
        static_cast<int32_t>(0b00101011010010110101101010101010),
        static_cast<int32_t>(0b10101001011111000111110001010101),
        static_cast<int32_t>(0b01111111100000001101010101010101) };
    DecimalOperations::ShiftLeftMultiPrecision(number3, 5, 31);
    EXPECT_EQ(number3, result3);

    std::vector<int32_t> number4 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    std::vector<int32_t> result4 { static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011) };
    DecimalOperations::ShiftLeftMultiPrecision(number4, 5, 32);
    EXPECT_EQ(number4, result4);

    std::vector<int32_t> number5 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    std::vector<int32_t> result5 { static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b01000010100010110100001010001010),
        static_cast<int32_t>(0b10101101001011010110101010101011),
        static_cast<int32_t>(0b10100101111100011111000101010100),
        static_cast<int32_t>(0b11111110000000110101010101010110),
        static_cast<int32_t>(0b00000000000000000000000000000001) };
    DecimalOperations::ShiftLeftMultiPrecision(number5, 6, 33);
    EXPECT_EQ(number5, result5);

    std::vector<int32_t> number6 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    std::vector<int32_t> result6 { static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b00101000101101000010100010100000),
        static_cast<int32_t>(0b11010010110101101010101010110100),
        static_cast<int32_t>(0b01011111000111110001010101001010),
        static_cast<int32_t>(0b11100000001101010101010101101010),
        static_cast<int32_t>(0b00000000000000000000000000011111) };
    DecimalOperations::ShiftLeftMultiPrecision(number6, 6, 37);
    EXPECT_EQ(number6, result6);

    std::vector<int32_t> number7 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    std::vector<int32_t> result7 { static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011) };
    DecimalOperations::ShiftLeftMultiPrecision(number7, 6, 64);
    EXPECT_EQ(number7, result7);
}

TEST(Decimal128, shiftRightMultiPrecision)
{
    std::vector<int32_t> number1 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    std::vector<int32_t> result1 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    DecimalOperations::ShiftRightMultiPrecision(number1, 4, 0);
    EXPECT_EQ(number1, result1);

    std::vector<int32_t> number2 { static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011) };
    std::vector<int32_t> result2 { static_cast<int32_t>(0b10000000000000000000000000000000),
        static_cast<int32_t>(0b11010000101000101101000010100010),
        static_cast<int32_t>(0b00101011010010110101101010101010),
        static_cast<int32_t>(0b10101001011111000111110001010101),
        static_cast<int32_t>(0b01111111100000001101010101010101) };
    DecimalOperations::ShiftRightMultiPrecision(number2, 5, 1);
    EXPECT_EQ(number2, result2);

    std::vector<int32_t> number3 { static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011) };
    std::vector<int32_t> result3 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    DecimalOperations::ShiftRightMultiPrecision(number3, 5, 32);
    EXPECT_EQ(number3, result3);

    std::vector<int32_t> number4 { static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011) };
    std::vector<int32_t> result4 { static_cast<int32_t>(0b10000000000000000000000000000000),
        static_cast<int32_t>(0b11010000101000101101000010100010),
        static_cast<int32_t>(0b00101011010010110101101010101010),
        static_cast<int32_t>(0b10101001011111000111110001010101),
        static_cast<int32_t>(0b01111111100000001101010101010101),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    DecimalOperations::ShiftRightMultiPrecision(number4, 6, 33);
    EXPECT_EQ(number4, result4);

    std::vector<int32_t> number5 { static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011) };
    std::vector<int32_t> result5 { static_cast<int32_t>(0b00101000000000000000000000000000),
        static_cast<int32_t>(0b10101101000010100010110100001010),
        static_cast<int32_t>(0b01010010101101001011010110101010),
        static_cast<int32_t>(0b01011010100101111100011111000101),
        static_cast<int32_t>(0b00000111111110000000110101010101),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    DecimalOperations::ShiftRightMultiPrecision(number5, 6, 37);
    EXPECT_EQ(number5, result5);

    std::vector<int32_t> number6 { static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011) };
    std::vector<int32_t> result6 { static_cast<int32_t>(0b10100001010001011010000101000101),
        static_cast<int32_t>(0b01010110100101101011010101010101),
        static_cast<int32_t>(0b01010010111110001111100010101010),
        static_cast<int32_t>(0b11111111000000011010101010101011),
        static_cast<int32_t>(0b00000000000000000000000000000000),
        static_cast<int32_t>(0b00000000000000000000000000000000) };
    DecimalOperations::ShiftRightMultiPrecision(number6, 6, 64);
    EXPECT_EQ(number6, result6);
}

TEST(Decimal128, rescaleLE18)
{
    // 10  (0) = 10
    Decimal128 value1(0x0, 0xA);
    Decimal128 result1;
    DecimalOperations::Rescale128(value1, 0, result1);
    EXPECT_EQ(Decimal128(0x0, 0xA), result1);

    // 15  (-1) = 2
    Decimal128 value3(0x0, 0xF);
    Decimal128 result3;
    DecimalOperations::Rescale128(value3, -1, result3);
    EXPECT_EQ(Decimal128(0x0, 0x2), result3);

    // 1050  (-3) = 1
    Decimal128 value4(0x0, 0x41A);
    Decimal128 result4;
    DecimalOperations::Rescale128(value4, -3, result4);
    EXPECT_EQ(Decimal128(0x0, 0x1), result4);

    // 15  (1) = 150
    Decimal128 value5(0x0, 0xF);
    Decimal128 result5;
    DecimalOperations::Rescale128(value5, 1, result5);
    EXPECT_EQ(Decimal128(0x0, 0x96), result5);

    // -14  (-1) = -1
    Decimal128 value6(0x8000000000000000, 0xE);
    Decimal128 result6;
    DecimalOperations::Rescale128(value6, -1, result6);
    EXPECT_EQ(Decimal128(0x8000000000000000, 0x1), result6);

    // -14  (1) = -140
    Decimal128 value7(0x8000000000000000, 0xE);
    Decimal128 result7;
    DecimalOperations::Rescale128(value7, 1, result7);
    EXPECT_EQ(Decimal128(0x8000000000000000, 0x8C), result7);

    // 0  (1) = 0
    Decimal128 value8(0x0, 0x0);
    Decimal128 result8;
    DecimalOperations::Rescale128(value8, 1, result8);
    EXPECT_EQ(Decimal128(0x0, 0x0), result8);

    // 5  (-1) = 1
    Decimal128 value9(0x0, 0x5);
    Decimal128 result9;
    DecimalOperations::Rescale128(value9, -1, result9);
    EXPECT_EQ(Decimal128(0x0, 0x1), result9);

    // 10  (10) = 100000000000
    Decimal128 value10(0x0, 0xA);
    Decimal128 result10;
    DecimalOperations::Rescale128(value10, 10, result10);
    EXPECT_EQ(Decimal128(0x0, 0x174876E800), result10);

    // 150500000000000000000  (-18) = 151
    Decimal128 value14(0x8, 0x289B689DE84A0000);
    Decimal128 result14;
    DecimalOperations::Rescale128(value14, -18, result14);
    EXPECT_EQ(Decimal128(0x0, 0x97), result14);

    // -140000000000000000000  (-18) = -140
    Decimal128 value15(0x8000000000000007, 0x96E3EA3F8AB00000);
    Decimal128 result15;
    DecimalOperations::Rescale128(value15, -18, result15);
    EXPECT_EQ(Decimal128(0x8000000000000000, 0x8C), result15);

    // 9223372036854775808  (-18) = 9
    Decimal128 value16(0x0, 0x8000000000000000);
    Decimal128 result16;
    DecimalOperations::Rescale128(value16, -18, result16);
    EXPECT_EQ(Decimal128(0x0, 0x9), result16);

    // 4611686018427387904  (-18) = 5
    Decimal128 value17(0x0, 0x4000000000000000);
    Decimal128 result17;
    DecimalOperations::Rescale128(value17, -18, result17);
    EXPECT_EQ(Decimal128(0x0, 0x5), result17);

    // 99999999999999999999999999999999999999  (-1) = 10000000000000000000000000000000000000
    Decimal128 value19(0x4B3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 result19;
    DecimalOperations::Rescale128(value19, -1, result19);
    EXPECT_EQ(Decimal128(0x785EE10D5DA46D9, 0xF436A000000000), result19);

    // -99999999999999999999999999999999999999  (-10) = -10000000000000000000000000000
    Decimal128 value20(0xCB3B4CA85A86C47A, 0x98A223FFFFFFFFF);
    Decimal128 result20;
    DecimalOperations::Rescale128(value20, -10, result20);
    EXPECT_EQ(Decimal128(0x80000000204FCE5E, 0x3E25026110000000), result20);
}

TEST(Decimal128, DISABLED_rescaleGT18)
{
    // 10  (-20) = 0
    Decimal128 value2(0x0, 0xA);
    Decimal128 result2;
    DecimalOperations::Rescale128(value2, -20, result2);
    EXPECT_EQ(Decimal128(0x0, 0x0), result2);

    // 150000000000000000000  (-20) = 2
    Decimal128 value11(0x8, 0x21AB0D4414980000);
    Decimal128 result11;
    DecimalOperations::Rescale128(value11, -20, result11);
    EXPECT_EQ(Decimal128(0x0, 0x2), result11);

    // -140000000000000000000  (-20) = -1
    Decimal128 value12(0x8000000000000007, 0x96E3EA3F8AB00000);
    Decimal128 result12;
    DecimalOperations::Rescale128(value12, -20, result12);
    EXPECT_EQ(Decimal128(0x8000000000000000, 0x1), result12);

    // 50000000000000000000  (-20) = 1
    Decimal128 value13(0x2, 0xB5E3AF16B1880000);
    Decimal128 result13;
    DecimalOperations::Rescale128(value13, -20, result13);
    EXPECT_EQ(Decimal128(0x0, 0x1), result13);

    // 4611686018427387904  (-19) = 0
    Decimal128 value18(0x0, 0x4000000000000000);
    Decimal128 result18;
    DecimalOperations::Rescale128(value18, -19, result18);
    EXPECT_EQ(Decimal128(0x0, 0x0), result18);

    // 1  (37) = 10000000000000000000000000000000000000
    Decimal128 value21(0x0, 0x1);
    Decimal128 result21;
    DecimalOperations::Rescale128(value21, 37, result21);
    EXPECT_EQ(Decimal128(0x785EE10D5DA46D9, 0xF436A000000000), result21);

    // -1  (37) = -10000000000000000000000000000000000000
    Decimal128 value22(0x8000000000000000, 0x1);
    Decimal128 result22;
    DecimalOperations::Rescale128(value22, 37, result22);
    EXPECT_EQ(Decimal128(0x8785EE10D5DA46D9, 0xF436A000000000), result22);

    // 10000000000000000000000000000000000000  (-37) = 1
    Decimal128 value23(0x785EE10D5DA46D9, 0xF436A000000000);
    Decimal128 result23;
    DecimalOperations::Rescale128(value23, -37, result23);
    EXPECT_EQ(Decimal128(0x0, 0x1), result23);
}

TEST(Decimal128, shiftLeftDestructive)
{
    // 446319580078125 << 19 = 234000000000000000000
    Decimal128 value1(0x0, 0x195ECE006E02D);
    Decimal128 result1(0xC, 0xAF67003701680000);
    DecimalOperations::ShiftLeftDestructive(value1, 19);
    EXPECT_EQ(value1, result1);

    // 2 << 10 = 2048
    Decimal128 value2(0x0, 0x2);
    Decimal128 result2(0x0, 0x800);
    DecimalOperations::ShiftLeftDestructive(value2, 10);
    EXPECT_EQ(value2, result2);

    // 34 << 10 = 34816
    Decimal128 value3(0x0, 0x22);
    Decimal128 result3(0x0, 0x8800);
    DecimalOperations::ShiftLeftDestructive(value3, 10);
    EXPECT_EQ(value3, result3);

    // 2 << 100 = 2535301200456458802993406410752
    Decimal128 value4(0x0, 0x2);
    Decimal128 result4(0x2000000000, 0x0);
    DecimalOperations::ShiftLeftDestructive(value4, 100);
    EXPECT_EQ(value4, result4);

    // 34 << 100 = 43100120407759799650887908982784
    Decimal128 value5(0x0, 0x22);
    Decimal128 result5(0x22000000000, 0x0);
    DecimalOperations::ShiftLeftDestructive(value5, 100);
    EXPECT_EQ(value5, result5);

    // 1180591620717411303424 << 30 = 1267650600228229401496703205376
    Decimal128 value6(0x40, 0x0);
    Decimal128 result6(0x1000000000, 0x0);
    DecimalOperations::ShiftLeftDestructive(value6, 30);
    EXPECT_EQ(value6, result6);

    // 1180591620717411303426 << 30 = 1267650600228229401498850689024
    Decimal128 value7(0x40, 0x2);
    Decimal128 result7(0x1000000000, 0x80000000);
    DecimalOperations::ShiftLeftDestructive(value7, 30);
    EXPECT_EQ(value7, result7);

    // 81129638414606681695789005144064 << 20 = 85070591730234615865843651857942052864
    Decimal128 value8(0x40000000000, 0x0);
    Decimal128 result8(0x4000000000000000, 0x0);
    DecimalOperations::ShiftLeftDestructive(value8, 20);
    EXPECT_EQ(value8, result8);

    // 81129638414606681695789005144066 << 20 = 85070591730234615865843651857944150016
    Decimal128 value9(0x40000000000, 0x2);
    Decimal128 result9(0x4000000000000000, 0x200000);
    DecimalOperations::ShiftLeftDestructive(value9, 20);
    EXPECT_EQ(value9, result9);
}
}