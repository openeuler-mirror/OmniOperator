/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "type/decimal_operations.h"
#include "vector_common.h"
#include "operator/execution_context.h"
#include "operator/aggregation/aggregation.h"

using namespace omniruntime::vec;

TEST(DecimalOperations, sum_encode_and_decode_decimal)
{
    using namespace omniruntime::op;
    AggregateState state;
    ExecutionContext executionContext;
    state.val = executionContext.getArena()->Allocate(24);

    // encode phase
    Decimal128 oldDec;
    int64_t oldOverflow = 1;
    oldDec.SetValue(1, 2);
    DecimalOperations::EncodeSumDecimal(state.val, oldDec, oldOverflow);
    // decode phase
    Decimal128 newDec;
    newDec.SetValue(0, 0);
    int64_t newOverflow = 1;
    DecimalOperations::DecodeSumDecimal(state.val, newDec, newOverflow);

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
    int64_t expectValue1 = 0x8000000000000000;
    int64_t expectValue2 = 1;
    long overflow1 = DecimalOperations::AddWithOverflow(left1, right1, result1);
    EXPECT_EQ(result1.HighBits(), expectValue1);
    EXPECT_EQ(result1.LowBits(), expectValue2);
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
    bool  flag = DecimalOperations::ExceedsOrEqualTenToThirtyEight(result);
    EXPECT_EQ(flag, false);
}

TEST(DecimalOperations, decode_avg_decimal)
{
    using namespace omniruntime::op;
    AggregateState state;
    ExecutionContext executionContext;
    state.val = executionContext.getArena()->Allocate(24);

    Decimal128 oldDec;
    int64_t oldOther = 1;
    int64_t oldOverflow = 1;
    oldDec.SetValue(2, 3);
    DecimalOperations::EncodeAvgDecimal(state.val, oldDec, oldOverflow, oldOther);

    // decode phase
    Decimal128 newDec;
    newDec.SetValue(0, 0);
    int64_t newOverflow = 0;
    int64_t newOther = 0;
    DecimalOperations::DecodeAvgDecimal(state.val, newDec, newOverflow, newOther);
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

TEST(DecimalOperations, roundUp)
{
    Decimal128 newDec1 = 2;
    Decimal128 newDec2 = 1;
    Decimal128 newDec3 = 3;
    Decimal128 newDec4 = 2;
    Decimal128 expectValue = 4;
    DecimalOperations::RoundUp(newDec1,newDec2,newDec3,newDec4);
    EXPECT_EQ(newDec3, expectValue);

    newDec4 = 0;
    DecimalOperations::RoundUp(newDec1,newDec2,newDec3,newDec4);
    EXPECT_EQ(newDec3, expectValue);
}
