/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2023. All rights reserved.
 */

#include <gtest/gtest-param-test.h>
#include <sstream>
#include <sys/time.h>

#include "aggregator_multi_stage_no_groupby.h"
#include "aggregator_multi_stage_with_groupby.h"

namespace omniruntime {
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace TestUtil;

class MultiStageCompleteTest : public ::testing::TestWithParam<
    std::tuple<std::string, DataTypeId, DataTypeId, int32_t, bool, bool, bool, bool>> {};

template <DataTypeId IN_ID, DataTypeId OUT_ID>
static std::unique_ptr<AggregatorTester> CreateKnowInputOutput(const std::string aggFuncName, const int32_t nullPercent,
    const bool isDict, const bool hasMask, const bool nullWhenOverflow, const bool groupby)
{
    if (groupby) {
        return std::make_unique<HashAggregatorTesterTemplate<IN_ID, OUT_ID>>(aggFuncName, nullPercent, isDict, hasMask,
            nullWhenOverflow);
    } else {
        return std::make_unique<AggregatorTesterTemplate<IN_ID, OUT_ID>>(aggFuncName, nullPercent, isDict, hasMask,
            nullWhenOverflow);
    }
}

template <DataTypeId IN_ID>
static std::unique_ptr<AggregatorTester> CreateKnowInput(const DataTypeId outId, const std::string aggFuncName,
    const int32_t nullPercent, const bool isDict, const bool hasMask, const bool nullWhenOverflow, const bool groupby)
{
    switch (outId) {
        case OMNI_BOOLEAN:
            return CreateKnowInputOutput<IN_ID, OMNI_BOOLEAN>(aggFuncName, nullPercent, isDict, hasMask,
                nullWhenOverflow, groupby);
        case OMNI_SHORT:
            return CreateKnowInputOutput<IN_ID, OMNI_SHORT>(aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        case OMNI_INT:
            return CreateKnowInputOutput<IN_ID, OMNI_INT>(aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        case OMNI_LONG:
            return CreateKnowInputOutput<IN_ID, OMNI_LONG>(aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        case OMNI_DOUBLE:
            return CreateKnowInputOutput<IN_ID, OMNI_DOUBLE>(aggFuncName, nullPercent, isDict, hasMask,
                nullWhenOverflow, groupby);
        case OMNI_DECIMAL64:
            return CreateKnowInputOutput<IN_ID, OMNI_DECIMAL64>(aggFuncName, nullPercent, isDict, hasMask,
                nullWhenOverflow, groupby);
        case OMNI_DECIMAL128:
            return CreateKnowInputOutput<IN_ID, OMNI_DECIMAL128>(aggFuncName, nullPercent, isDict, hasMask,
                nullWhenOverflow, groupby);
        case OMNI_VARCHAR:
            return CreateKnowInputOutput<IN_ID, OMNI_VARCHAR>(aggFuncName, nullPercent, isDict, hasMask,
                nullWhenOverflow, groupby);
        case OMNI_CHAR:
            return CreateKnowInputOutput<IN_ID, OMNI_CHAR>(aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        default:
            throw OmniException("Invalid Argument", "Invalid type " + TypeUtil::TypeToStringLog(outId));
    }
}

static std::unique_ptr<AggregatorTester> CreateAggregatorTester(const std::string aggFuncName, const DataTypeId inId,
    const DataTypeId outId, const int32_t nullPercent, const bool isDict, const bool hasMask,
    const bool nullWhenOverflow, const bool groupby)
{
    switch (inId) {
        case OMNI_BOOLEAN:
            return CreateKnowInput<OMNI_BOOLEAN>(outId, aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        case OMNI_SHORT:
            return CreateKnowInput<OMNI_SHORT>(outId, aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        case OMNI_INT:
            return CreateKnowInput<OMNI_INT>(outId, aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        case OMNI_LONG:
            return CreateKnowInput<OMNI_LONG>(outId, aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        case OMNI_DOUBLE:
            return CreateKnowInput<OMNI_DOUBLE>(outId, aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        case OMNI_DECIMAL64:
            return CreateKnowInput<OMNI_DECIMAL64>(outId, aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        case OMNI_DECIMAL128:
            return CreateKnowInput<OMNI_DECIMAL128>(outId, aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        case OMNI_VARCHAR:
            return CreateKnowInput<OMNI_VARCHAR>(outId, aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        case OMNI_CHAR:
            return CreateKnowInput<OMNI_CHAR>(outId, aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow,
                groupby);
        default:
            throw OmniException("Invalid Argument", "Invalid type " + TypeUtil::TypeToStringLog(inId));
    }
}

static void RunAggregatorTest(std::unique_ptr<AggregatorTester> tester, const bool isSupported, const double error)
{
    const std::string expectedExceptionMessage = tester->GetExpectedExceptionMessage();
    int32_t valueColIdx = tester->GetValueColumnIndex();
    auto partialFactory = tester->CreatePartialFactory();
    EXPECT_TRUE(partialFactory != nullptr);

    auto finalFactory = tester->CreateFinalFactory();
    EXPECT_TRUE(finalFactory != nullptr);

    op::Operator *aggPartial1;
    try {
        aggPartial1 = partialFactory->CreateOperator();
    } catch (OmniException &e) {
        if (!isSupported) {
            return;
        }
        throw e;
    }
    EXPECT_TRUE(aggPartial1 != nullptr);

    op::Operator *aggPartial2;
    try {
        aggPartial2 = partialFactory->CreateOperator();
    } catch (OmniException &e) {
        op::Operator::DeleteOperator(aggPartial1);
        if (!isSupported) {
            return;
        }
        throw e;
    }
    EXPECT_TRUE(aggPartial2 != nullptr);

    op::Operator *aggFinal;
    try {
        aggFinal = finalFactory->CreateOperator();
    } catch (OmniException &e) {
        op::Operator::DeleteOperator(aggPartial1);
        op::Operator::DeleteOperator(aggPartial2);
        if (!isSupported) {
            return;
        }
        throw e;
    }
    EXPECT_TRUE(aggFinal != nullptr);

    // op::Operator 1 (partial)
    std::vector<VectorBatch *> input1 = tester->BuildAggInput(VEC_BATCH_NUM, ROW_SIZE);
    EXPECT_TRUE(input1.size() > 0);
    VectorBatch *expectedResult1 = nullptr;
    bool overflow1 = tester->GeneratePartialExpectedResult(&expectedResult1, input1);

    aggPartial1->Init();
    for (VectorBatch *input : input1) {
        aggPartial1->AddInput(input);
    }

    std::vector<VectorBatch *> result1;
    try {
        int32_t vecBatchCount = aggPartial1->GetOutput(result1);
        EXPECT_EQ(vecBatchCount, 1);
        EXPECT_EQ(result1[0]->GetVectorCount(), expectedResult1->GetVectorCount());
        EXPECT_EQ(result1[0]->GetRowCount(), expectedResult1->GetRowCount());
    } catch (OmniException &e) {
        op::Operator::DeleteOperator(aggPartial1);
        op::Operator::DeleteOperator(aggPartial2);
        op::Operator::DeleteOperator(aggFinal);
        VectorHelper::FreeVecBatch(expectedResult1);

        if (expectedExceptionMessage.length() == 0 || std::string(e.what()).find(expectedExceptionMessage, 0) < 0) {
            throw e;
        }
        return;
    }
    if (overflow1) {
        EXPECT_EQ(expectedExceptionMessage.length(), 0);
        EXPECT_TRUE(ValidateOverflow("Partial1", valueColIdx, expectedResult1, result1[0]));
    }
    op::Operator::DeleteOperator(aggPartial1);

    // op::Operator 2 (partial)
    std::vector<VectorBatch *> input2 = tester->BuildAggInput(VEC_BATCH_NUM, ROW_SIZE);
    EXPECT_TRUE(input2.size() > 0);
    VectorBatch *expectedResult2 = nullptr;
    bool overflow2 = tester->GeneratePartialExpectedResult(&expectedResult2, input2);

    aggPartial2->Init();
    for (VectorBatch *input : input2) {
        aggPartial2->AddInput(input);
    }

    std::vector<VectorBatch *> result2;
    try {
        int32_t vecBatchCount = aggPartial2->GetOutput(result2);
        EXPECT_EQ(vecBatchCount, 1);
        EXPECT_EQ(result2[0]->GetVectorCount(), expectedResult2->GetVectorCount());
        EXPECT_EQ(result2[0]->GetRowCount(), expectedResult2->GetRowCount());
    } catch (OmniException &e) {
        VectorHelper::FreeVecBatch(expectedResult1);
        VectorHelper::FreeVecBatches(result1);
        op::Operator::DeleteOperator(aggPartial2);
        op::Operator::DeleteOperator(aggFinal);
        VectorHelper::FreeVecBatch(expectedResult2);

        if (expectedExceptionMessage.length() == 0 || std::string(e.what()).find(expectedExceptionMessage, 0) < 0) {
            throw e;
        }
        return;
    }
    if (overflow2) {
        EXPECT_EQ(expectedExceptionMessage.length(), 0);
        EXPECT_TRUE(ValidateOverflow("Partial2", valueColIdx, expectedResult2, result2[0]));
    }
    op::Operator::DeleteOperator(aggPartial2);

    // Second stage (final)
    std::vector<VectorBatch *> expectedResults{ expectedResult1, expectedResult2 };
    VectorBatch *expectedResultFinal = nullptr;
    bool overflowFinal = tester->GenerateFinalExpectedResult(&expectedResultFinal, expectedResults);

    aggFinal->Init();

    for (uint32_t i = 0; i < result1.size(); ++i) {
        aggFinal->AddInput(result1[i]);
    }
    for (uint32_t i = 0; i < result2.size(); ++i) {
        aggFinal->AddInput(result2[i]);
    }

    std::vector<VectorBatch *> finalResult;
    try {
        int32_t vecBatchCount = aggFinal->GetOutput(finalResult);
        EXPECT_EQ(vecBatchCount, 1);
        EXPECT_EQ(finalResult[0]->GetVectorCount(), expectedResultFinal->GetVectorCount());
        EXPECT_EQ(finalResult[0]->GetRowCount(), expectedResultFinal->GetRowCount());
    } catch (OmniException &e) {
        op::Operator::DeleteOperator(aggFinal);
        VectorHelper::FreeVecBatch(expectedResultFinal);

        if (expectedExceptionMessage.length() == 0 || std::string(e.what()).find(expectedExceptionMessage, 0) < 0) {
            throw e;
        }
        return;
    }
    if (overflowFinal) {
        EXPECT_EQ(expectedExceptionMessage.length(), 0);
        EXPECT_TRUE(ValidateOverflow("Final", valueColIdx, expectedResultFinal, finalResult[0]));
    }

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(finalResult[0], expectedResultFinal, error));

    op::Operator::DeleteOperator(aggFinal);
    VectorHelper::FreeVecBatch(expectedResultFinal);
    VectorHelper::FreeVecBatches(finalResult);
}

TEST_P(MultiStageCompleteTest, verify_correctness)
{
    const std::string aggFuncName = std::get<0>(GetParam());
    const DataTypeId inId = std::get<1>(GetParam());
    const DataTypeId outId = std::get<2>(GetParam());
    const int32_t nullPercent = std::get<3>(GetParam());
    const bool isDict = std::get<4>(GetParam());
    const bool hasMask = std::get<5>(GetParam());
    const bool nullWhenOverflow = std::get<6>(GetParam());
    const bool groupby = std::get<7>(GetParam());

    // aggregation on double input uses SIMD vectorization
    // but calculation of expected result does not use SIMD vectorization.
    // Therefore, actual result can be slightly different from expected result.
    // we set error = 0.0001 to handle this situation
    // When outout is not double (it is numeric) expected result could be off by +1/-1
    // for example, actual result could be 23.500123 (which when converted to numeric value will be 24)
    //  expected result, however, could be 23.499923 (which when converted to numeric value will be 23))
    double error = inId == OMNI_DOUBLE ? (outId == OMNI_DOUBLE ? 0.001 : 1) : 0;

    char *randSeedStr = std::getenv("TEST_RAND_SEED");
    unsigned int randSeed;
    if (randSeedStr != nullptr && strlen(randSeedStr) > 0) {
        randSeed = static_cast<unsigned int>(atoi(randSeedStr));
    } else {
        struct timeval time;
        gettimeofday(&time, nullptr);
        randSeed = static_cast<unsigned int>((time.tv_sec * 1000) + (time.tv_usec / 1000));
    }
    printf("Random seed: %d\n", randSeed);
    srand(randSeed);

    RunAggregatorTest(std::move(
        CreateAggregatorTester(aggFuncName, inId, outId, nullPercent, isDict, hasMask, nullWhenOverflow, groupby)),
        CheckSupported(aggFuncName, inId, outId), error);
}

INSTANTIATE_TEST_CASE_P(AggregatorTest, MultiStageCompleteTest,
    ::testing::Combine(::testing::Values("sum", "min", "max", "avg"),
    ::testing::Values(OMNI_SHORT, OMNI_LONG, OMNI_DOUBLE, OMNI_DECIMAL64, OMNI_DECIMAL128, OMNI_VARCHAR),
    ::testing::Values(OMNI_SHORT, OMNI_LONG, OMNI_DOUBLE, OMNI_DECIMAL64, OMNI_DECIMAL128, OMNI_VARCHAR),
    ::testing::Values(0, 25), // nullPercent
    ::testing::Bool(),        // isDict
    ::testing::Bool(),        // hasMask
    ::testing::Values(true),  // nullWhenOverflow
    ::testing::Bool()         // groupby
    ),
    [](const testing::TestParamInfo<MultiStageCompleteTest::ParamType> &info) {
        return std::get<0>(info.param) + "_" + TypeUtil::TypeToStringLog(std::get<1>(info.param)) + "_" +
            TypeUtil::TypeToStringLog(std::get<2>(info.param)) + "_" + std::to_string(std::get<3>(info.param)) + "_" +
            (std::get<4>(info.param) ? "dict_" : "flat_") + (std::get<5>(info.param) ? "withMask_" : "noMask_") +
            (std::get<6>(info.param) ? "overflowNull_" : "overflowExcep_") +
            (std::get<7>(info.param) ? "withGroupBy" : "noGroupBy");
    });
}
