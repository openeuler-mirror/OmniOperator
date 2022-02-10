//
// Created by root on 2/8/22.
//

#include "type/decimalOperations.h"
#include "gtest/gtest.h"
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

    Decimal128 left=3; Decimal128 right=1; Decimal128 result=0;
     long overflow = DecimalOperations:: AddWithOverflow(left, right, result);

    //std::cout<<"overflow is "<<overflow<<std::endl;
    std::cout<<"result_low is "<<result.LowBits()<<std::endl;
    std::cout<<"result_high is "<<result.HighBits()<<std::endl;
    std::cout<<"result is "<<result<<std::endl;
    EXPECT_EQ(overflow, 0);
    Decimal128 left1; Decimal128 right1=2; Decimal128 result1=0;

    left1.SetValue(-1,1);
    std::cout<<"left1high is "<<left1.HighBits()<<std::endl;
    std::cout<<"left1low is "<<left1.LowBits()<<std::endl;




    if((left1.Abs(left1))<(right1.Abs(right1))){
        std::cout<<"left1 < right1 "<<left1<<std::endl;
    }else if((left1.Abs(left1))>(right1.Abs(right1))){
        std::cout<<"left1 > right1 "<<left1<<std::endl;
    }else {
        std::cout<<"left1 = right1 "<<left1.HighBits()<<std::endl;
    }


    long overflow1 = DecimalOperations:: AddWithOverflow(left1,right1,result1);
    std::cout<<"result1high is "<<result1.HighBits()<<std::endl;
    std::cout<<"result1LowBits is "<<result1.LowBits()<<std::endl;

    std::cout<<"result1 is "<<result1<<std::endl;
    std::cout<<"overflow1 is "<<overflow1<<std::endl;
    EXPECT_EQ(overflow1, 0);



}