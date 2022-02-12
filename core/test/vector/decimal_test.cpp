/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "type/decimalOperations.h"
#include "vector_common.h"
#include "operator/execution_context.h"
#include "operator/aggregation/aggregation.h"

using namespace omniruntime::vec;

TEST(DecimalOperations, encode_and_decode_decimal)
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
    int64_t newOverflow = 0;
    DecimalOperations::DecodeSumDecimal(state.val, newDec, newOverflow);
    EXPECT_EQ(newOverflow, oldOverflow);
    EXPECT_EQ(newDec, oldDec);
}

TEST(DecimalOperations, addWithOverflow)
{
    Decimal128 left = 3;
    Decimal128 right = 6;
    Decimal128 result = 0;
    long overflow = DecimalOperations::AddWithOverflow(left, right, result);
    EXPECT_EQ(result, 0x00000000000000000000000000000009);
    EXPECT_EQ(overflow, 0);

    Decimal128 left1;
    Decimal128 right1 = 2;
    Decimal128 result1 = 0;
    int64_t c = 1LL << 63;
    left1.SetValue(c, 3);
    long overflow1 = DecimalOperations::AddWithOverflow(left1, right1, result1);
    EXPECT_EQ(result1.HighBits(), 0x8000000000000000);
    EXPECT_EQ(result1.LowBits(), 0x0000000000000001);
    EXPECT_EQ(overflow1, 0);


    Decimal128 left2;
    Decimal128 right2 = 2;
    Decimal128 result2 = 0;
    int64_t c2 = 1LL << 63;
    left2.SetValue(c2, 1);
    long overflow2 = DecimalOperations::AddWithOverflow(left2, right2, result2);
    EXPECT_EQ(result2, 0x00000000000000000000000000000001);
    EXPECT_EQ(overflow2, 0);


    Decimal128 left3;
    Decimal128 right3;
    Decimal128 result3 = 0;
    int64_t c3 = 1LL << 63;
    left3.SetValue(c3, 1);
    right3.SetValue(c3, 1);
    long overflow3 = DecimalOperations::AddWithOverflow(left3, right3, result3);
    EXPECT_EQ(result3.HighBits(), 0x8000000000000000);
    EXPECT_EQ(result3.LowBits(), 0x0000000000000002);
    EXPECT_EQ(overflow3, 0);

}