/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window operator implementations
 */
#include "gtest/gtest.h"
#include "../../src/operator/sort/sort.h"
#include "../../src/operator/window/window.h"
#include "../util/test_util.h"
#include <time.h>
#include <vector>
#include <iostream>
#include <chrono>
#include "../../src/operator/optimization.h"
#include "../../src/vector/vector_helper.h"
#include "../../src/jit/jit.h"
#include "../../libconfig.h"

using namespace std;
using namespace omniruntime::vec;

const int32_t DATA_SIZE = 6;

const auto PARAM_INDEX0 = 0;
const auto PARAM_INDEX1 = 1;
const auto PARAM_INDEX2 = 2;
const auto PARAM_INDEX3 = 3;
const auto PARAM_INDEX4 = 4;
const auto PARAM_INDEX5 = 5;
const auto PARAM_INDEX6 = 6;
const auto PARAM_INDEX7 = 7;


JitContext *GetContext(const omniruntime::jit::Specialization *compareToSp,
    const omniruntime::jit::Specialization *getOutputSp);

void InitParams(omniruntime::jit::ParamValue &pSortCols, omniruntime::jit::ParamValue &pSortColTypes,
    omniruntime::jit::ParamValue &pSortAscendings, omniruntime::jit::ParamValue &pSortNullFirsts,
    omniruntime::jit::ParamValue &pSortColCount, omniruntime::jit::ParamValue &pSourceTypes,
    omniruntime::jit::ParamValue &pOutputCols, omniruntime::jit::ParamValue &pOutputColCount,
    omniruntime::jit::Specialization *&compareToSp, omniruntime::jit::Specialization *&getOutputSp);

JitContext *CreateTestWindowJitContext(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount, int32_t *allTypes, int32_t allCount)
{
    using namespace omniruntime::jit;
    int32_t finalSortColsCount = sortColsCount + partitionCount;
    int32_t finalSortCols[finalSortColsCount];
    int32_t finalSortAscendings[finalSortColsCount];
    int32_t finalSortNullFirsts[finalSortColsCount];
    for (int32_t i = 0; i < partitionCount; i++) {
        finalSortCols[i] = partitionCols[i];
        finalSortAscendings[i] = true;
        finalSortNullFirsts[i] = false;
    }
    for (int32_t i = partitionCount; i < partitionCount + sortColsCount; i++) {
        finalSortCols[i] = sortCols[i - partitionCount];
        finalSortAscendings[i] = sortAscendings[i - partitionCount];
        finalSortNullFirsts[i] = sortNullFirsts[i - partitionCount];
    }

    int32_t finalSortColTypes[finalSortColsCount];
    for (int32_t i = 0; i < finalSortColsCount; i++) {
        finalSortColTypes[i] = sourceTypes[finalSortCols[i]];
    }
    int32_t finalOutputCols[allCount];
    int32_t finalOutputColsCount = 0;
    for (int32_t i = 0; i < outputColsCount; i++) {
        finalOutputCols[finalOutputColsCount] = outputCols[i];
        finalOutputColsCount++;
    }
    for (int32_t i = typesCount; i < allCount; i++) {
        finalOutputCols[finalOutputColsCount] = i;
        finalOutputColsCount++;
    }

    ParamValue pSortCols = ParamValue(finalSortCols, finalSortColsCount);
    ParamValue pSortColTypes = ParamValue(finalSortColTypes, finalSortColsCount);
    ParamValue pSortAscendings = ParamValue(finalSortAscendings, finalSortColsCount);
    ParamValue pSortNullFirsts = ParamValue(finalSortNullFirsts, finalSortColsCount);
    ParamValue pSortColCount = ParamValue(&finalSortColsCount);

    ParamValue pSourceTypes = ParamValue(sourceTypes, typesCount);
    ParamValue pOutputCols = ParamValue(outputCols, outputColsCount);
    ParamValue pOutputColCount = ParamValue(&outputColsCount);
    omniruntime::jit::Specialization *compareToSp = nullptr;
    omniruntime::jit::Specialization *getOutputSp = nullptr;
    InitParams(pSortCols, pSortColTypes, pSortAscendings, pSortNullFirsts, pSortColCount, pSourceTypes, pOutputCols,
        pOutputColCount, compareToSp, getOutputSp);

    return GetContext(compareToSp, getOutputSp);
}

void InitParams(omniruntime::jit::ParamValue &pSortCols, omniruntime::jit::ParamValue &pSortColTypes,
    omniruntime::jit::ParamValue &pSortAscendings, omniruntime::jit::ParamValue &pSortNullFirsts,
    omniruntime::jit::ParamValue &pSortColCount, omniruntime::jit::ParamValue &pSourceTypes,
    omniruntime::jit::ParamValue &pOutputCols, omniruntime::jit::ParamValue &pOutputColCount,
    omniruntime::jit::Specialization *&compareToSp, omniruntime::jit::Specialization *&getOutputSp)
{
    compareToSp = new omniruntime::jit::Specialization();
    getOutputSp = new omniruntime::jit::Specialization();

    compareToSp->AddSpecializedParam(PARAM_INDEX0, &pSortCols);
    compareToSp->AddSpecializedParam(PARAM_INDEX1, &pSortColTypes);
    compareToSp->AddSpecializedParam(PARAM_INDEX2, &pSortAscendings);
    compareToSp->AddSpecializedParam(PARAM_INDEX3, &pSortNullFirsts);
    compareToSp->AddSpecializedParam(PARAM_INDEX4, &pSortColCount);
    getOutputSp->AddSpecializedParam(PARAM_INDEX1, &pOutputCols);
    getOutputSp->AddSpecializedParam(PARAM_INDEX2, &pOutputColCount);
    getOutputSp->AddSpecializedParam(PARAM_INDEX4, &pSourceTypes);
}

JitContext *GetContext(const omniruntime::jit::Specialization *compareToSp,
    const omniruntime::jit::Specialization *getOutputSp)
{
    using omniruntime::jit::Optimization;
    map<string, omniruntime::jit::Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp } };
    auto *windowContext = new omniruntime::jit::Context(
            GenerateOperatorTemplatePath("window"), map<string, omniruntime::jit::Specialization>());
    auto *pagesIndexContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("pages_index"), pagesIndexSps);
    omniruntime::jit::Jit *jit = new omniruntime::jit::Jit(vector<omniruntime::jit::Context> { *windowContext, *pagesIndexContext });
    jit->Specialize();
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    delete compareToSp;
    delete getOutputSp;
    delete windowContext;
    delete pagesIndexContext;
    delete jit;

    return jitContext;
}

JitContext *CreateTestWindowJitContextWithFactory(omniruntime::op::WindowOperatorFactory *windowOperatorFactory)
{
    return CreateTestWindowJitContext(windowOperatorFactory->GetSourceTypes(), windowOperatorFactory->GetTypesCount(),
        windowOperatorFactory->GetOutputCols(), windowOperatorFactory->GetOutputColsCount(),
        windowOperatorFactory->GetPartitionCols(), windowOperatorFactory->GetPartitionCount(),
        windowOperatorFactory->GetSortCols(), windowOperatorFactory->GetSortAscendings(),
        windowOperatorFactory->GetSortNullFirsts(), windowOperatorFactory->GetSortColCount(),
        windowOperatorFactory->GetAllTypes(), windowOperatorFactory->GetAllCount());
}

TEST(NativeOmniWindowOperatorTest, testRowNumberPartition)
{
    using namespace omniruntime::op;

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
    int32_t windowFunctionTypes[1] = {WIN_ROW_NUMBER};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType() }));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

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

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testRowNumber)
{
    using namespace omniruntime::op;

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
    int32_t windowFunctionTypes[1] = {WIN_ROW_NUMBER};
    int32_t partitionCols[1] = {2};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType() }));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 0,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *test = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));
    WindowOperator *windowOperator = test;
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

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testRankPartition)
{
    using namespace omniruntime::op;

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
    int32_t windowFunctionTypes[1] = {WIN_RANK};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType() }));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

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

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testRank)
{
    using namespace omniruntime::op;

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
    int32_t windowFunctionTypes[1] = {WIN_RANK};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType() }));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

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

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}


TEST(NativeOmniWindowOperatorTest, testRowNumberAndRankPartition)
{
    using namespace omniruntime::op;

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
    int32_t windowFunctionTypes[2] = {WIN_RANK, WIN_ROW_NUMBER};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(
        std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType(), LongVecType() }));
    int32_t argumentChannels[2] = {-1, -1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

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

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testRowNumberAndRankPartitionWithNull)
{
    using namespace omniruntime::op;

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
    int32_t windowFunctionTypes[2] = {WIN_RANK, WIN_ROW_NUMBER};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(
        std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType(), LongVecType() }));
    int32_t argumentChannels[2] = {-1, -1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

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

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testRowNumberAndRankPartitionWithNullWithoutSort)
{
    using namespace omniruntime::op;

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
    int32_t sortCols[0] = {};
    int32_t ascendings[0] = {};
    int32_t nullFirsts[0] = {};
    int32_t windowFunctionTypes[2] = {WIN_RANK, WIN_ROW_NUMBER};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(
            std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType(), LongVecType() }));
    int32_t argumentChannels[2] = {-1, -1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
                                                                                                3, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 0,
                                                                                                preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(
            std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType(), LongVecType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 2, 1};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 2, 1, 5};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 5.5, 1.1};
    int64_t expectData4[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData5[DATA_SIZE] = {1, 2, 1, 1, 1, 2};
    VectorBatch *expectVecBatch =
            CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4, expectData5);
    expectVecBatch->GetVector(0)->SetValueNull(4);
    expectVecBatch->GetVector(0)->SetValueNull(5);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testAggregationPartitionWithNull)
{
    using namespace omniruntime::op;

    // construct input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2);

    vecBatch->GetVector(0)->SetValueNull(1);
    vecBatch->GetVector(0)->SetValueNull(5);

    vecBatch->GetVector(1)->SetValueNull(3);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[5] = {WIN_SUM, WIN_COUNT, WIN_AVG, WIN_MAX, WIN_MIN};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType(), LongVecType(),
        DoubleVecType(), DoubleVecType(), LongVecType() }));

    int32_t argumentChannels[5] = {1, 1, 1, 2, 1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 5, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 5);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType(),
        LongVecType(), DoubleVecType(), DoubleVecType(), LongVecType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 1, 1};
    int64_t expectData2[DATA_SIZE] = {8, 4, 4, 2, 5, 1};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 1.1, 5.5};
    int64_t expectData4[DATA_SIZE] = {8, 8, 4, 2, 5, 6};
    int64_t expectData5[DATA_SIZE] = {1, 1, 1, 1, 1, 2};
    double expectData6[DATA_SIZE] = {8, 8, 4, 2, 5, 3};
    double expectData7[DATA_SIZE] = {6.6, 6.6, 2.2, 4.4, 1.1, 5.5};
    int64_t expectData8[DATA_SIZE] = {8, 8, 4, 2, 5, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6, expectData7, expectData8);
    expectVecBatch->GetVector(0)->SetValueNull(4);
    expectVecBatch->GetVector(0)->SetValueNull(5);
    expectVecBatch->GetVector(1)->SetValueNull(1);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testAggregationPartitionWithNullWithoutSort)
{
    using namespace omniruntime::op;

    // construct input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2);

    vecBatch->GetVector(0)->SetValueNull(1);
    vecBatch->GetVector(0)->SetValueNull(5);

    vecBatch->GetVector(1)->SetValueNull(3);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[0] = {};
    int32_t ascendings[0] = {};
    int32_t nullFirsts[0] = {};
    int32_t windowFunctionTypes[5] = {WIN_SUM, WIN_COUNT, WIN_AVG, WIN_MAX, WIN_MIN};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType(), LongVecType(),
                                             DoubleVecType(), DoubleVecType(), LongVecType() }));

    int32_t argumentChannels[5] = {1, 1, 1, 2, 1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
                                                                                                3, windowFunctionTypes, 5, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 0,
                                                                                                preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 5);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), LongVecType(), DoubleVecType(), LongVecType(),
                                                LongVecType(), DoubleVecType(), DoubleVecType(), LongVecType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 1, 1};
    int64_t expectData2[DATA_SIZE] = {8, 4, 4, 2, 1, 5};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 5.5, 1.1};
    int64_t expectData4[DATA_SIZE] = {8, 8, 4, 2, 6, 6};
    int64_t expectData5[DATA_SIZE] = {1, 1, 1, 1, 2, 2};
    double expectData6[DATA_SIZE] = {8, 8, 4, 2, 3, 3};
    double expectData7[DATA_SIZE] = {6.6, 6.6, 2.2, 4.4, 5.5, 5.5};
    int64_t expectData8[DATA_SIZE] = {8, 8, 4, 2, 1, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
                                                    expectData4, expectData5, expectData6, expectData7, expectData8);
    expectVecBatch->GetVector(0)->SetValueNull(4);
    expectVecBatch->GetVector(0)->SetValueNull(5);
    expectVecBatch->GetVector(1)->SetValueNull(1);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testRankWithAllDataTypes)
{
    using namespace omniruntime::op;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2) , CharVecType(3)}));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    const int32_t colCount = 10;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[colCount] = {WIN_RANK, WIN_RANK, WIN_RANK, WIN_RANK, WIN_RANK,
                                      WIN_RANK, WIN_RANK, WIN_RANK, WIN_RANK, WIN_RANK};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3),
        LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(),  LongVecType()}));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, colCount, partitionCols, 1,
        preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3),
        LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType()}));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t expectData3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t expectData4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double expectData5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int64_t expectData10[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData11[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData12[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData13[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData14[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData15[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData16[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData17[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData18[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData19[DATA_SIZE] = {1, 1, 1, 1, 1, 1};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17,
        expectData18, expectData19);

    VectorHelper::PrintVecBatch(outputVecBatches[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}


TEST(NativeOmniWindowOperatorTest, testRowNumberkWithAllDataTypes)
{
    using namespace omniruntime::op;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3)}));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    const int32_t colCount = 10;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[colCount] = {WIN_ROW_NUMBER, WIN_ROW_NUMBER, WIN_ROW_NUMBER,
                                      WIN_ROW_NUMBER, WIN_ROW_NUMBER, WIN_ROW_NUMBER,
                                      WIN_ROW_NUMBER, WIN_ROW_NUMBER, WIN_ROW_NUMBER, WIN_ROW_NUMBER};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3),
        LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType()}));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, colCount, partitionCols, 1,
        preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3),
        LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType()}));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t expectData3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t expectData4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double expectData5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int64_t expectData10[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData11[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData12[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData13[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData14[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData15[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData16[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData17[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData18[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData19[DATA_SIZE] = {1, 2, 1, 2, 1, 2};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18,
        expectData19);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, DISABLED_testSumWithAllDataTypes)
{
    using namespace omniruntime::op;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1),DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3)}));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(2, 2), Decimal128(2, 2), Decimal128(3, 3), Decimal128(3, 3)};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8,
                          data9);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    const int32_t colCount = 10;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[7] = {WIN_SUM, WIN_SUM, WIN_SUM, WIN_SUM, WIN_SUM, WIN_SUM, WIN_SUM};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3), IntVecType(),
        Date32VecType(omniruntime::vec::DAY), Date32VecType(omniruntime::vec::MILLI), LongVecType(),
        Decimal128VecType(1, 1), DoubleVecType(), Decimal128VecType(2, 2) }));
    int32_t argumentChannels[7] = {0, 1, 2, 3, 4, 5, 8};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 7, partitionCols, 1,
        preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 7);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3), IntVecType(),
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
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int32_t expectData10[DATA_SIZE] = {2, 2, 4, 4, 6, 6};
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

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, DISABLED_testAvgWithAllDataTypes)
{
    using namespace omniruntime::op;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3)}));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0)};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8,
                          data9);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    const int32_t colCount = 10;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[7] = {WIN_AVG, WIN_AVG, WIN_AVG, WIN_AVG, WIN_AVG, WIN_AVG, WIN_AVG};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3),
        DoubleVecType(), DoubleVecType(), DoubleVecType(),
        DoubleVecType(), DoubleVecType(), DoubleVecType(), DoubleVecType() }));
    int32_t argumentChannels[7] = {0, 1, 2, 3, 4, 5, 8};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 7, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 7);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3),
        DoubleVecType(), DoubleVecType(), DoubleVecType(),
        DoubleVecType(), DoubleVecType(), DoubleVecType(), DoubleVecType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0)};
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    double expectData10[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    double expectData11[DATA_SIZE] = {22, 22, 44, 44, 66, 66};
    double expectData12[DATA_SIZE] = {222, 222, 444, 444, 666, 666};
    double expectData13[DATA_SIZE] = {2222, 2222, 4444, 4444, 6666, 6666};
    double expectData14[DATA_SIZE] = {22222, 22222, 44444, 44444, 66666, 66666};
    double expectData15[DATA_SIZE] = {2.2, 2.2, 4.4, 4.4, 6.6, 6.6};
    double expectData16[DATA_SIZE] = {0, 0, 0, 0, 0, 0};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testMaxWithAllDataTypes)
{
    using namespace omniruntime::op;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3)}));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};
    std::string data9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    const int32_t colCount = 10;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[9] = {WIN_MAX, WIN_MAX, WIN_MAX, WIN_MAX, WIN_MAX, WIN_MAX, WIN_MAX, WIN_MAX, WIN_MAX};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3), IntVecType(),
        Date32VecType(omniruntime::vec::DAY), Date32VecType(omniruntime::vec::MILLI), LongVecType(),
        Decimal64VecType(1, 1), DoubleVecType(), VarcharVecType(3), Decimal128VecType(2, 2),
        CharVecType(3)}));
    int32_t argumentChannels[9] = {0, 1, 2, 3, 4, 5, 7, 8, 9};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 9, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 9);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3), IntVecType(),
        Date32VecType(omniruntime::vec::DAY), Date32VecType(omniruntime::vec::MILLI), LongVecType(),
        Decimal64VecType(1, 1), DoubleVecType(), VarcharVecType(3), Decimal128VecType(2, 2),
        CharVecType(3)}));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};
    std::string expectData9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int32_t expectData10[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData11[DATA_SIZE] = {33, 33, 55, 55, 77, 77};
    int32_t expectData12[DATA_SIZE] = {333, 333, 555, 555, 777, 777};
    int64_t expectData13[DATA_SIZE] = {3333, 3333, 5555, 5555, 7777, 7777};
    int64_t expectData14[DATA_SIZE] = {33333, 33333, 55555, 55555, 77777, 77777};
    double expectData15[DATA_SIZE] = {3.3, 3.3, 5.5, 5.5, 7.7, 7.7};
    std::string expectData16[DATA_SIZE] = {"s3", "s3", "s5", "s5", "s7", "s7"};
    Decimal128 expectData17[DATA_SIZE] = {Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7), Decimal128(7, 7)};
    std::string expectData18[DATA_SIZE] = {"c3", "c3", "c5", "c5", "c7", "c7"};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testMinWithAllDataTypes)
{
    using namespace omniruntime::op;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};
    std::string data9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    const int32_t colCount = 10;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[9] = {WIN_MIN, WIN_MIN, WIN_MIN, WIN_MIN, WIN_MIN, WIN_MIN, WIN_MIN, WIN_MIN, WIN_MIN};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3), IntVecType(),
        Date32VecType(omniruntime::vec::DAY), Date32VecType(omniruntime::vec::MILLI), LongVecType(),
        Decimal64VecType(1, 1), DoubleVecType(), VarcharVecType(3), Decimal128VecType(2, 2),
        CharVecType(3)}));
    int32_t argumentChannels[9] = {0, 1, 2, 3, 4, 5, 7, 8, 9};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 9, partitionCols, 1,
        preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 9);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3), IntVecType(),
        Date32VecType(omniruntime::vec::DAY), Date32VecType(omniruntime::vec::MILLI), LongVecType(),
        Decimal64VecType(1, 1), DoubleVecType(), VarcharVecType(3), Decimal128VecType(2, 2),
        CharVecType(3)}));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};
    std::string expectData9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int32_t expectData10[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData11[DATA_SIZE] = {11, 11, 33, 33, 55, 55};
    int32_t expectData12[DATA_SIZE] = {111, 111, 333, 333, 555, 555};
    int64_t expectData13[DATA_SIZE] = {1111, 1111, 3333, 3333, 5555, 5555};
    int64_t expectData14[DATA_SIZE] = {11111, 11111, 33333, 33333, 55555, 55555};
    double expectData15[DATA_SIZE] = {1.1, 1.1, 3.3, 3.3, 5.5, 5.5};
    std::string expectData16[DATA_SIZE] = {"s1", "s1", "s3", "s3", "s5", "s5"};
    Decimal128 expectData17[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5)};
    std::string expectData18[DATA_SIZE] = {"c1", "c1", "c3", "c3", "c5", "c5"};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testCountWithAllDataTypes)
{
    using namespace omniruntime::op;

    // construct the input data
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};
    std::string data9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};

    VectorBatch *vecBatch =
        CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6, data7, data8, data9);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    const int32_t colCount = 10;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[9] = {WIN_COUNT, WIN_COUNT, WIN_COUNT, WIN_COUNT, WIN_COUNT, WIN_COUNT, WIN_COUNT,
                                      WIN_COUNT, WIN_COUNT};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3),
        LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType(), LongVecType() }));
    int32_t argumentChannels[9] = {0, 1, 2, 3, 4, 5, 7, 8, 9};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 9, partitionCols, 1,
        preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 9);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), CharVecType(3),
        LongVecType(), LongVecType(), LongVecType(),
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
    std::string expectData9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int64_t expectData10[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData11[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData12[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData13[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData14[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData15[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData16[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData17[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData18[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testDictionaryVector)
{
    using namespace omniruntime::op;

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
        // dictionary will slice the vector, we should release original vector
        delete vecBatch->GetVector(i);
        vecBatch->SetVector(i, dictionaryVector);
    }

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t outputCols[9] = {0, 1, 2, 3, 4, 5, 6, 7, 8};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[7] = {WIN_RANK, WIN_ROW_NUMBER, WIN_SUM, WIN_COUNT, WIN_AVG, WIN_MAX, WIN_MIN};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    VecTypes allTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), DoubleVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t argumentChannels[7] = {0, 1, 3, 4, 5, 7, 8};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        9, windowFunctionTypes, 7, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 7);
    JitContext *jitContext = CreateTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    // construct the output data
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), Date32VecType(omniruntime::vec::DAY),
        Date32VecType(omniruntime::vec::MILLI), LongVecType(), Decimal64VecType(1, 1), DoubleVecType(),
        BooleanVecType(), VarcharVecType(3), Decimal128VecType(2, 2), LongVecType(), LongVecType(), LongVecType(),
        LongVecType(), DoubleVecType(), VarcharVecType(3), Decimal128VecType(2, 2) }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5), Decimal128(7, 7)};
    int64_t expectData9[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData10[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData11[DATA_SIZE] = {4444, 4444, 8888, 8888, 13332, 13332};
    int64_t expectData12[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    double expectData13[DATA_SIZE] = {2.2, 2.2, 4.4, 4.4, 6.6, 6.6};
    std::string expectData14[DATA_SIZE] = {"s3", "s3", "s5", "s5", "s7", "s7"};
    Decimal128 expectData15[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5)};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatches(outputVecBatches);
}
