/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window operator implementations
 */
#include "gtest/gtest.h"
#include "operator/window/window_expr.h"
#include "../util/test_util.h"
#include "vector/vector_helper.h"
#include "jit_context/jit_context.h"

using namespace std;
using namespace omniruntime::vec;

const int32_t DATA_SIZE = 6;

TEST(NativeOmniWindowWithExprOperatorTest, testMaxWithExpr)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2);

    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_AGGREGATION_TYPE_MAX};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), DoubleVecType() }));
    VecTypes outputTypes(std::vector<VecType>({ DoubleVecType() }));

    std::string argumentChannels[1] = { "ADD:3(#2, 50:3)" };

    // create expression objects
    Parser parser;
    std::vector<Expr *> argumentChannelsExprs = parser.ParseExpressions(argumentChannels, 1, sourceTypes);

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 1);

    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 3, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    WindowWithExprOperator *windowOperator =
        dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(
        std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), DoubleVecType(), DoubleVecType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 1, 5, 2};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 5.5, 1.1, 4.4};
    double expectData4[DATA_SIZE] = {56.6, 53.3, 52.2, 55.5, 51.1, 54.4};
    double expectData5[DATA_SIZE] = {56.6, 56.6, 52.2, 55.5, 51.1, 54.4};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4, expectData5);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, testRowNumberPartition)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};
    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ LongVecType() }));
    string argumentChannels[0] = {};
    std::vector<Expr *> argumentChannelsExprs = {};

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 0);

    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 3, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    int64_t expectData2[DATA_SIZE] = {3, 0, 4, 1, 5, 2};
    double expectData3[DATA_SIZE] = {3.3, 6.6, 2.2, 5.5, 1.1, 4.4};
    int64_t expectData4[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, testRowNumber)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[2] = {2, 1};
    int32_t sortCols[0] = {};
    int32_t ascendings[0] = {};
    int32_t nullFirsts[0] = {};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t partitionCols[1] = {2};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ LongVecType() }));
    string argumentChannels[0] = {};
    std::vector<Expr *> argumentChannelsExprs = {};

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 2, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 0, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 0);
    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 2, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 0, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto test = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));
    WindowWithExprOperator *windowOperator = test;
    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ DoubleVecType(), LongVecType(), LongVecType() }));
    double expectData1[DATA_SIZE] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int64_t expectData2[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    int64_t expectData3[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, testRankPartition)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_RANK};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ LongVecType() }));
    string argumentChannels[0] = {};
    std::vector<Expr *> argumentChannelsExprs = {};

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 0);
    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 3, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 1, 5, 2};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 5.5, 1.1, 4.4};
    int64_t expectData4[DATA_SIZE] = {1, 1, 1, 2, 1, 2};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, testRank)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[3] = {1, 2, 0};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_RANK};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ LongVecType() }));
    string argumentChannels[0] = {};
    std::vector<Expr *> argumentChannelsExprs = {};

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 3, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 0);
    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 3, partitionCols, 0, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ LongVecType(), DoubleVecType(), IntVecType(), LongVecType() }));
    int64_t expectData1[DATA_SIZE] = {8, 8, 5, 4, 2, 1};
    double expectData2[DATA_SIZE] = {6.6, 3.3, 1.1, 2.2, 4.4, 5.5};
    int32_t expectData3[DATA_SIZE] = {0, 0, 2, 1, 2, 1};
    int64_t expectData4[DATA_SIZE] = {1, 1, 3, 4, 5, 6};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}


TEST(NativeOmniWindowWithExprOperatorTest, testRowNumberAndRankPartition)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[2] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    string argumentChannels[2] = {"-1", "-1"};

    // create expression objects
    Parser parser;
    std::vector<Expr *> argumentChannelsExprs = parser.ParseExpressions(argumentChannels, 2, sourceTypes);

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 3, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 0);

    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 3, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(
        std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType(), LongVecType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 1, 5, 2};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 5.5, 1.1, 4.4};
    int64_t expectData4[DATA_SIZE] = {1, 1, 1, 2, 1, 2};
    int64_t expectData5[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4, expectData5);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, testRowNumberAndRankPartitionWithNull)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2);
    vecBatch->GetVector(0)->SetValueNull(1);
    vecBatch->GetVector(0)->SetValueNull(5);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[2] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ LongVecType(), LongVecType() }));
    string argumentChannels[2] = {"-1", "-1"};

    // create expression objects
    Parser parser;
    std::vector<Expr *> argumentChannelsExprs = parser.ParseExpressions(argumentChannels, 2, sourceTypes);

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 3, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 0);
    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 3, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(
        std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType(), LongVecType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 2, 1};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 2, 5, 1};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 1.1, 5.5};
    int64_t expectData4[DATA_SIZE] = {1, 1, 1, 1, 1, 2};
    int64_t expectData5[DATA_SIZE] = {1, 2, 1, 1, 1, 2};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4, expectData5);
    expectVecBatch->GetVector(0)->SetValueNull(4);
    expectVecBatch->GetVector(0)->SetValueNull(5);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, testRankWithAllDataTypes)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[9] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[9] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK,
                                      OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    string argumentChannels[0] = {};
    std::vector<Expr *> argumentChannelsExprs = {};

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 9, windowFunctionTypes, 9, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 0);
    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 9, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t expectData3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t expectData4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double expectData5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    int64_t expectData9[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData10[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData11[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData12[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData13[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData14[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData15[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData16[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData17[DATA_SIZE] = {1, 1, 1, 1, 1, 1};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}


TEST(NativeOmniWindowWithExprOperatorTest, testRowNumberkWithAllDataTypes)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[9] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[9] = {OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER,
                                      OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER,
                                      OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    string argumentChannels[0] = {};
    std::vector<Expr *> argumentChannelsExprs = {};

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 9, windowFunctionTypes, 9, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 0);
    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 9, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t expectData3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t expectData4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double expectData5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    int64_t expectData9[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData10[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData11[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData12[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData13[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData14[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData15[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData16[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData17[DATA_SIZE] = {1, 2, 1, 2, 1, 2};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, DISABLED_testSumWithAllDataTypes)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(2, 2), Decimal128(2, 2), Decimal128(3, 3), Decimal128(3, 3)};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[9] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[7] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal128VecType(1, 1), DoubleVecType(),
        Decimal128VecType(2, 2) }));
    string argumentChannels[7] = {"ADD:1(2:1, #0)", "#1", "#2", "#3", "#4", "#5", "#8"};

    // create expression objects
    Parser parser;
    std::vector<Expr *> argumentChannelsExprs = parser.ParseExpressions(argumentChannels, 7, sourceTypes);

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 9, windowFunctionTypes, 7, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 7);

    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 9, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), IntVecType(), IntVecType(),
        Date32VecType(omniruntime::vec::DAY), Date32VecType(omniruntime::vec::MILLI), LongVecType(),
        Decimal128VecType(1, 1), DoubleVecType(), Decimal128VecType(2, 2) }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t expectData3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t expectData4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double expectData5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(2, 2), Decimal128(2, 2), Decimal128(3, 3), Decimal128(3, 3)};
    int32_t expectData9[DATA_SIZE] = {3, 3, 4, 4, 5, 5};
    int32_t expectData10[DATA_SIZE] = {6, 6, 8, 8, 10, 10};
    int32_t expectData11[DATA_SIZE] = {22, 22, 44, 44, 66, 66};
    int32_t expectData12[DATA_SIZE] = {222, 222, 444, 444, 666, 666};
    int64_t expectData13[DATA_SIZE] = {2222, 2222, 4444, 4444, 6666, 6666};
    Decimal128 expectData14[DATA_SIZE] = {Decimal128(22222), Decimal128(22222), Decimal128(44444), Decimal128(44444), Decimal128(66666), Decimal128(66666)};
    double expectData15[DATA_SIZE] = {2.2, 2.2, 4.4, 4.4, 6.6, 6.6};
    Decimal128 expectData16[DATA_SIZE] = {Decimal128(2, 2), Decimal128(2, 2), Decimal128(4, 4), Decimal128(4, 4), Decimal128(6, 6), Decimal128(6, 6)};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, DISABLED_testAvgWithAllDataTypes)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0)};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[9] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[7] = {OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ DoubleVecType(), DoubleVecType(), DoubleVecType(), DoubleVecType(),
        DoubleVecType(), DoubleVecType(), DoubleVecType() }));
    string argumentChannels[7] = {"#0", "ADD:1(2:1, #1)", "#2", "#3", "#4", "#5", "#8"};

    // create expression objects
    Parser parser;
    std::vector<Expr *> argumentChannelsExprs = parser.ParseExpressions(argumentChannels, 7, sourceTypes);

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 9, windowFunctionTypes, 7, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 7);
    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 9, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), IntVecType(), DoubleVecType(), DoubleVecType(),
        DoubleVecType(), DoubleVecType(), DoubleVecType(), DoubleVecType(), DoubleVecType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0)};
    int32_t expectData9[DATA_SIZE] = {13, 35, 35, 57, 57, 79};
    double expectData10[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    double expectData11[DATA_SIZE] = {24, 24, 46, 46, 68, 68};
    double expectData12[DATA_SIZE] = {222, 222, 444, 444, 666, 666};
    double expectData13[DATA_SIZE] = {2222, 2222, 4444, 4444, 6666, 6666};
    double expectData14[DATA_SIZE] = {22222, 22222, 44444, 44444, 66666, 66666};
    double expectData15[DATA_SIZE] = {2.2, 2.2, 4.4, 4.4, 6.6, 6.6};
    double expectData16[DATA_SIZE] = {0, 0, 0, 0, 0, 0};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, testMaxWithAllDataTypes)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[9] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[8] = {OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        VarcharVecType(3), Decimal128VecType(2, 2) }));
    string argumentChannels[8] = {"#0", "#1", "#2", "ADD:2(2:2, #3)", "#4", "#5", "#7", "#8"};

    // create expression objects
    Parser parser;
    std::vector<Expr *> argumentChannelsExprs = parser.ParseExpressions(argumentChannels, 8, sourceTypes);

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 9, windowFunctionTypes, 8, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 8);
    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 9, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), LongVecType(), IntVecType(),
        Date32VecType(omniruntime::vec::DAY), Date32VecType(omniruntime::vec::MILLI), LongVecType(),
        Decimal64VecType(1, 1), DoubleVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};
    int64_t expectData9[DATA_SIZE] = {1113, 3335, 3335, 5557, 5557, 7779};
    int32_t expectData10[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData11[DATA_SIZE] = {33, 33, 55, 55, 77, 77};
    int32_t expectData12[DATA_SIZE] = {333, 333, 555, 555, 777, 777};
    int64_t expectData13[DATA_SIZE] = {3335, 3335, 5557, 5557, 7779, 7779};
    int64_t expectData14[DATA_SIZE] = {33333, 33333, 55555, 55555, 77777, 77777};
    double expectData15[DATA_SIZE] = {3.3, 3.3, 5.5, 5.5, 7.7, 7.7};
    std::string expectData16[DATA_SIZE] = {"s3", "s3", "s5", "s5", "s7", "s7"};
    Decimal128 expectData17[DATA_SIZE] = {Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7), Decimal128(7, 7)};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, testMinWithAllDataTypes)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[9] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[8] = {OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        VarcharVecType(3), Decimal128VecType(2, 2) }));
    string argumentChannels[8] = {"#0", "#1", "#2", "ADD:2(#3, 2:2)", "#4", "#5", "#7", "#8"};

    Parser parser;
    std::vector<Expr *> argumentChannelsExprs = parser.ParseExpressions(argumentChannels, 8, sourceTypes);


    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 9, windowFunctionTypes, 8, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 8);
    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 9, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), LongVecType(), IntVecType(),
        Date32VecType(omniruntime::vec::DAY), Date32VecType(omniruntime::vec::MILLI), LongVecType(),
        Decimal64VecType(1, 1), DoubleVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};
    int64_t expectData9[DATA_SIZE] = {1113, 3335, 3335, 5557, 5557, 7779};
    int32_t expectData10[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData11[DATA_SIZE] = {11, 11, 33, 33, 55, 55};
    int32_t expectData12[DATA_SIZE] = {111, 111, 333, 333, 555, 555};
    int64_t expectData13[DATA_SIZE] = {1113, 1113, 3335, 3335, 5557, 5557};
    int64_t expectData14[DATA_SIZE] = {11111, 11111, 33333, 33333, 55555, 55555};
    double expectData15[DATA_SIZE] = {1.1, 1.1, 3.3, 3.3, 5.5, 5.5};
    std::string expectData16[DATA_SIZE] = {"s1", "s1", "s3", "s3", "s5", "s5"};
    Decimal128 expectData17[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5)};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, testCountWithAllDataTypes)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[9] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[8] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    string argumentChannels[8] = {"#0", "#1", "SUBTRACT:1(#2, 2:1)", "#3", "#4", "#5", "#7", "#8"};
    Parser parser;
    std::vector<Expr *> argumentChannelsExprs = parser.ParseExpressions(argumentChannels, 8, sourceTypes);

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 9, windowFunctionTypes, 8, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 8);
    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 9, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), IntVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};
    int32_t expectData9[DATA_SIZE] = {109, 331, 331, 553, 553, 775};
    int64_t expectData10[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData11[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData12[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData13[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData14[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData15[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData16[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData17[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowWithExprOperatorTest, testDictionaryVector)
{
    using namespace omniruntime::op;
    using namespace omniruntime::expressions;
    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};

    int32_t ids[] = {0, 1, 2, 3, 4, 5};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8);
    for (int32_t i = 0; i < sourceTypes.GetSize(); i++) {
        DictionaryVector *dictionaryVector = new DictionaryVector(vecBatch->GetVector(i), ids, DATA_SIZE);
        vecBatch->SetVector(i, dictionaryVector);
    }

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[9] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[7] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MIN};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes outputTypes(std::vector<VecType>({ LongVecType(), LongVecType(), LongVecType(), LongVecType(),
        DoubleVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    string argumentChannels[7] = {"#0", "#1", "ADD:2(2:2, #3)", "#4", "#5", "#7", "#8"};

    Parser parser;
    std::vector<Expr *> argumentChannelsExprs = parser.ParseExpressions(argumentChannels, 7, sourceTypes);

    // dealing data with the operator
    WindowWithExprOperatorFactory *operatorFactory = WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
        sourceTypes, outputCols, 9, windowFunctionTypes, 7, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings,
        nullFirsts, 1, preSortedChannelPrefix, expectedPositions, outputTypes, argumentChannelsExprs, 7);
    JitContext *jitContext = CreateWindowWithExprJitContext(sourceTypes, outputCols, 9, partitionCols, 1, sortCols,
        ascendings, nullFirsts, 1, outputTypes, argumentChannelsExprs);
    operatorFactory->SetJitContext(jitContext);
    auto windowOperator = dynamic_cast<WindowWithExprOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), DoubleVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};
    int64_t expectData9[DATA_SIZE] = {1113, 3335, 3335, 5557, 5557, 7779};
    int64_t expectData10[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData11[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData12[DATA_SIZE] = {4448, 4448, 8892, 8892, 13336, 13336};
    int64_t expectData13[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    double expectData14[DATA_SIZE] = {2.2, 2.2, 4.4, 4.4, 6.6, 6.6};
    std::string expectData15[DATA_SIZE] = {"s3", "s3", "s5", "s5", "s7", "s7"};
    Decimal128 expectData16[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5)};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    DeleteOperatorFactory(operatorFactory);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}