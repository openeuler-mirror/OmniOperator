/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "type/decimal_operations.h"
#include "vector/vector_common.h"
#include "operator/execution_context.h"
#include "operator/aggregation/aggregation.h"

using namespace omniruntime::type;

namespace DecimalOperationsTest {
TEST(DecimalOperations, sum_encode_and_decode_decimal)
{
    using namespace omniruntime::op;
    AggregateState state;
    ExecutionContext executionContext;
    state.val = executionContext.GetArena()->Allocate(24);

    // encode phase
    Decimal128 oldDec;
    int64_t oldOverflow = 1;
    oldDec.SetValue(1, 2);
    DecimalOperations::EncodeSumDecimal(static_cast<DecimalSumState *>(state.val), oldDec, oldOverflow);
    // decode phase
    Decimal128 newDec;
    newDec.SetValue(0, 0);
    int64_t newOverflow = 1;
    DecimalOperations::DecodeSumDecimal(static_cast<DecimalSumState *>(state.val), newDec, newOverflow);

    EXPECT_EQ(newOverflow, oldOverflow);
    EXPECT_EQ(newDec, oldDec);
}

TEST(DecimalOperations, add_with_overflow)
{
    Decimal128 left = 3;
    Decimal128 right = 6;
    Decimal128 result = 0;
    int64_t expectValue = 9;
    long overflow = DecimalOperations::AddWithOverflow(left, right, result);
    EXPECT_EQ(result, expectValue);
    EXPECT_EQ(overflow, 0);

    Decimal128 left1;
    Decimal128 right1 = 2;
    Decimal128 result1 = 0;
    int64_t c = 1LL << 63;
    left1.SetValue(c, 3);
    Decimal128 expectValue1(0x8000000000000000LL, 1);
    long overflow1 = DecimalOperations::AddWithOverflow(left1, right1, result1);
    EXPECT_EQ(result1.HighBits(), expectValue1.HighBits());
    EXPECT_EQ(result1.LowBits(), expectValue1.LowBits());
    EXPECT_EQ(overflow1, 0);

    Decimal128 left2;
    Decimal128 right2 = 2;
    Decimal128 result2 = 0;
    int64_t c2 = 1LL << 63;
    left2.SetValue(c2, 1);
    int64_t expectValue3 = 1;
    long overflow2 = DecimalOperations::AddWithOverflow(left2, right2, result2);
    EXPECT_EQ(result2, expectValue3);
    EXPECT_EQ(overflow2, 0);

    Decimal128 left3;
    Decimal128 right3;
    Decimal128 result3 = 0;
    int64_t c3 = 1LL << 63;
    left3.SetValue(c3, 1);
    right3.SetValue(c3, 1);
    int64_t expectValue4 = 2;
    int64_t expectValue5 = 0x8000000000000000;
    long overflow3 = DecimalOperations::AddWithOverflow(left3, right3, result3);
    EXPECT_EQ(result3.HighBits(), expectValue5);
    EXPECT_EQ(result3.LowBits(), expectValue4);
    EXPECT_EQ(overflow3, 0);

    Decimal128 one;
    Decimal128 negativeOne;
    one.SetValue(0, 1);
    negativeOne.SetValue(1LL << 63, 1);
    Decimal128 expected(0, 0);
    Decimal128 result4;
    int64_t overflow4 = DecimalOperations::AddWithOverflow(one, negativeOne, result4);
    EXPECT_EQ(result4.HighBits(), expected.HighBits());
    EXPECT_EQ(result4.LowBits(), expected.LowBits());
    EXPECT_EQ(overflow4, 0);
}

TEST(DecimalOperations, exceeds_or_equal_ten_to_thirty_eight)
{
    Decimal128 result;
    int64_t c3 = 0;
    result.SetValue(c3, 1);
    bool overflow = DecimalOperations::ExceedsOrEqualTenToThirtyEight(result);
    EXPECT_EQ(overflow, false);

    int64_t c4 = -1;
    result.SetValue(c4, 1);
    bool re = DecimalOperations::ExceedsOrEqualTenToThirtyEight(result);
    EXPECT_EQ(re, true);

    int64_t c5 = 0x4b3b4ca85a86c47aL;
    result.SetValue(c5, 1);
    bool flag = DecimalOperations::ExceedsOrEqualTenToThirtyEight(result);
    EXPECT_EQ(flag, false);
}

TEST(DecimalOperations, decode_avg_decimal)
{
    using namespace omniruntime::op;
    AggregateState state;
    ExecutionContext executionContext;
    state.val = executionContext.GetArena()->Allocate(24);

    Decimal128 oldDec;
    int64_t oldOther = 1;
    int64_t oldOverflow = 1;
    oldDec.SetValue(2, 3);
    DecimalOperations::EncodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), oldDec, oldOverflow, oldOther);

    // decode phase
    Decimal128 newDec;
    newDec.SetValue(0, 0);
    int64_t newOverflow = 0;
    int64_t newOther = 0;
    DecimalOperations::DecodeAvgDecimal(static_cast<DecimalAverageState *>(state.val), newDec, newOverflow, newOther);
    EXPECT_EQ(newOverflow, oldOverflow);
    EXPECT_EQ(newOverflow, oldOverflow);
}

TEST(DecimalOperations, unscaled_decimal)
{
    int64_t newOverflow = 1;
    int64_t oldOverflow = -1;
    Decimal128 decimal128 = DecimalOperations::UnscaledDecimal(newOverflow);
    Decimal128 decimal = DecimalOperations::UnscaledDecimal(oldOverflow);
    EXPECT_EQ(decimal128.LowBits(), decimal.LowBits());
}

TEST(DecimalOperations, divide)
{
    Decimal128 dividend(Decimal128::SIGN_LONG_MASK, 2000000000001635618);
    Decimal128 divisor(0, 4);
    Decimal128 result;
    DecimalOperations::DivideRoundUp(dividend, divisor, 0, 0, result);
    int64_t low = result.LowBits();
    int64_t shortResult = DecimalOperations::IsNegative(result) ? -low : low;
    int64_t expectedVal = -500000000000408905;
    EXPECT_EQ(expectedVal, shortResult);
}

TEST(DecimalOperations, divide_unsigned)
{
    int64_t dividend = 78340625600;
    int64_t divisor = -1565666771;
    int64_t quotient = DecimalOperations::DivideUnsignedLong(dividend, divisor);
    EXPECT_EQ(28, quotient);
}

TEST(DecimalOperations, rescale_decimal64)
{
    int64_t val = 10LL;
    int64_t rescaled;
    DecimalOperations::Rescale64(val, 2, rescaled);
    EXPECT_EQ(1000LL, rescaled);
}

TEST(DecimalOperations, rescale_decimal128)
{
    Decimal128 val = 10LL;
    Decimal128 result;
    DecimalOperations::Rescale128(val, 2, result);
    Decimal128 expected(0, 1000LL);
    EXPECT_EQ(expected, result);
}

TEST(DecimalOperations, rescale_decimal64_to_128)
{
    int64_t val = 10LL;
    Decimal128 result;
    DecimalOperations::Rescale64To128(val, 2, result);
    Decimal128 expected(0, 1000LL);
    EXPECT_EQ(expected, result);
}
}
