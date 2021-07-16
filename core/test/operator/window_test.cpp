#include "gtest/gtest.h"
#include "../../src/operator/sort/sort.h"
#include "../../src/operator/window/window.h"
#include "../util/test_util.h"
#include <time.h>
#include <vector>
#include <iostream>
#include <chrono>
#include <src/vector/vector_helper.h>
#include <src/operator/optimization.h>
#include "../../src/jit/jit.h"

using namespace std;

JitContext *createTestWindowJitContext(int32_t *sourceTypes, int32_t typesCount, int32_t *outputCols,
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

    ParamValue p_sortCols = ParamValue(finalSortCols, finalSortColsCount);
    ParamValue p_sortColTypes = ParamValue(finalSortColTypes, finalSortColsCount);
    ParamValue p_sortAscendings = ParamValue(finalSortAscendings, finalSortColsCount);
    ParamValue p_sortNullFirsts = ParamValue(finalSortNullFirsts, finalSortColsCount);
    ParamValue p_sortColCount = ParamValue(&finalSortColsCount);

    ParamValue p_sourceTypes = ParamValue(sourceTypes, typesCount);
    ParamValue p_outputCols = ParamValue(outputCols, outputColsCount);
    ParamValue p_outputColCount = ParamValue(&outputColsCount);

    auto *compareToSp = new Specialization();
    compareToSp->addSpecializedParam(1, &p_sortCols);
    compareToSp->addSpecializedParam(2, &p_sortColTypes);
    compareToSp->addSpecializedParam(3, &p_sortAscendings);
    compareToSp->addSpecializedParam(4, &p_sortNullFirsts);
    compareToSp->addSpecializedParam(5, &p_sortColCount);
    auto *getOutputSp = new Specialization();
    getOutputSp->addSpecializedParam(1, &p_outputCols);
    getOutputSp->addSpecializedParam(2, &p_outputColCount);
    getOutputSp->addSpecializedParam(4, &p_sourceTypes);
    std::map<std::string, Specialization> pagesIndexSps = { { OMNIJIT_PAGE_INDEX_COMPARE_TO, *compareToSp },
        { OMNIJIT_PAGE_INDEX_GET_OUTPUT, *getOutputSp } };
    auto *windowContext = new omniruntime::jit::Context("window", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>(), true);
    auto *sortContext = new omniruntime::jit::Context("sort", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    auto *aggContext = new omniruntime::jit::Context("aggregator", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    auto *windowFunctionContext = new omniruntime::jit::Context("window_function",
        std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *windowPartitionContext = new omniruntime::jit::Context("window_partition",
        std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *hashUtilContext = new omniruntime::jit::Context("hash_util", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    auto *pagesHashStrategyContext = new omniruntime::jit::Context("pages_hash_strategy",
        std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(),
        std::vector<std::string>(), std::vector<std::string>());
    auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", pagesIndexSps, std::vector<std::string>(),
        std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *windowContext, *sortContext, *aggContext,
        *windowFunctionContext, *windowPartitionContext, *hashUtilContext, *pagesHashStrategyContext,
        *memoryPoolContext, *pagesIndexContext });
    auto createOperatorFunc = jit->specialize();
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
    return jitContext;
}

JitContext *createTestWindowJitContextWithFactory(omniruntime::op::WindowOperatorFactory *windowOperatorFactory)
{
    return createTestWindowJitContext(windowOperatorFactory->getSourceTypes(), windowOperatorFactory->getTypesCount(),
        windowOperatorFactory->getOutputCols(), windowOperatorFactory->getOutputColsCount(),
        windowOperatorFactory->getPartitionCols(), windowOperatorFactory->getPartitionCount(),
        windowOperatorFactory->getSortCols(), windowOperatorFactory->getSortAscendings(),
        windowOperatorFactory->getSortNullFirsts(), windowOperatorFactory->getSortColCount(),
        windowOperatorFactory->getAllTypes(), windowOperatorFactory->getAllCount());
}

TEST(NativeOmniWindowOperatorTest, testRowNumberPartition)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t DATA_SIZE = 6;
    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch *vecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    column0->setValues(0, data0, DATA_SIZE);
    LongVector *column1 = new LongVector(nullptr, DATA_SIZE);
    column1->setValues(0, data1, DATA_SIZE);
    DoubleVector *column2 = new DoubleVector(nullptr, DATA_SIZE);
    column2->setValues(0, data2, DATA_SIZE);

    vecBatch->setVector(0, column0);
    vecBatch->setVector(1, column1);
    vecBatch->setVector(2, column2);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {WIN_ROW_NUMBER};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    int32_t allTypes[4] = {1, 2, 3, 2};
    int32_t argumentChannels[0] = {};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::createWindowOperatorFactory(sourceTypes, 3,
        outputCols, 3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, 4, argumentChannels, 0);
    JitContext *jitContext = createTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(createTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    IntVector *expectCol1 = new IntVector(nullptr, DATA_SIZE);
    expectCol1->setValues(0, expectData1, DATA_SIZE);
    int64_t expectData2[DATA_SIZE] = {3, 0, 4, 1, 5, 2};
    LongVector *expectCol2 = new LongVector(nullptr, DATA_SIZE);
    expectCol2->setValues(0, expectData2, DATA_SIZE);
    double expectData3[DATA_SIZE] = {3.3, 6.6, 2.2, 5.5, 1.1, 4.4};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, DATA_SIZE);
    expectCol3->setValues(0, expectData3, DATA_SIZE);
    int64_t expectData4[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    LongVector *expectCol4 = new LongVector(nullptr, DATA_SIZE);
    expectCol4->setValues(0, expectData4, DATA_SIZE);
    VectorBatch *expectVecBatch = new VectorBatch(4);
    expectVecBatch->setVector(0, expectCol1);
    expectVecBatch->setVector(1, expectCol2);
    expectVecBatch->setVector(2, expectCol3);
    expectVecBatch->setVector(3, expectCol4);
    EXPECT_TRUE(vecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::freeVecBatch(vecBatch);
    VectorHelper::freeVecBatch(expectVecBatch);
    VectorHelper::freeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testRowNumber)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t DATA_SIZE = 6;
    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch *vecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    column0->setValues(0, data0, DATA_SIZE);
    LongVector *column1 = new LongVector(nullptr, DATA_SIZE);
    column1->setValues(0, data1, DATA_SIZE);
    DoubleVector *column2 = new DoubleVector(nullptr, DATA_SIZE);
    column2->setValues(0, data2, DATA_SIZE);

    vecBatch->setVector(0, column0);
    vecBatch->setVector(1, column1);
    vecBatch->setVector(2, column2);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[2] = {2, 1};
    int32_t sortCols[0] = {};
    int32_t ascendings[0] = {};
    int32_t nullFirsts[0] = {};
    int32_t windowFunctionTypes[1] = {WIN_ROW_NUMBER};
    int32_t partitionCols[1] = {2};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    int32_t allTypes[4] = {1, 2, 3, 2};
    int32_t argumentChannels[0] = {};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::createWindowOperatorFactory(sourceTypes, 3,
        outputCols, 2, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 0,
        preSortedChannelPrefix, expectedPositions, allTypes, 4, argumentChannels, 0);
    JitContext *jitContext = createTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(createTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    double expectData1[DATA_SIZE] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    DoubleVector *expectCol1 = new DoubleVector(nullptr, DATA_SIZE);
    expectCol1->setValues(0, expectData1, DATA_SIZE);
    int64_t expectData2[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    LongVector *expectCol2 = new LongVector(nullptr, DATA_SIZE);
    expectCol2->setValues(0, expectData2, DATA_SIZE);
    int64_t expectData3[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    LongVector *expectCol3 = new LongVector(nullptr, DATA_SIZE);
    expectCol3->setValues(0, expectData3, DATA_SIZE);
    VectorBatch *expectVecBatch = new VectorBatch(3);
    expectVecBatch->setVector(0, expectCol1);
    expectVecBatch->setVector(1, expectCol2);
    expectVecBatch->setVector(2, expectCol3);
    EXPECT_TRUE(vecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::freeVecBatch(vecBatch);
    VectorHelper::freeVecBatch(expectVecBatch);
    VectorHelper::freeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testRankPartition)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t DATA_SIZE = 6;
    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch **vecBatches = (VectorBatch **)malloc(1 * sizeof(VectorBatch *));
    VectorBatch *vecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    column0->setValues(0, data0, DATA_SIZE);
    LongVector *column1 = new LongVector(nullptr, DATA_SIZE);
    column1->setValues(0, data1, DATA_SIZE);
    DoubleVector *column2 = new DoubleVector(nullptr, DATA_SIZE);
    column2->setValues(0, data2, DATA_SIZE);

    vecBatch->setVector(0, column0);
    vecBatch->setVector(1, column1);
    vecBatch->setVector(2, column2);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {WIN_RANK};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    int32_t allTypes[4] = {1, 2, 3, 2};
    int32_t argumentChannels[0] = {};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::createWindowOperatorFactory(sourceTypes, 3,
        outputCols, 3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, 4, argumentChannels, 0);
    JitContext *jitContext = createTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(createTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    IntVector *expectCol1 = new IntVector(nullptr, DATA_SIZE);
    expectCol1->setValues(0, expectData1, DATA_SIZE);
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 1, 5, 2};
    LongVector *expectCol2 = new LongVector(nullptr, DATA_SIZE);
    expectCol2->setValues(0, expectData2, DATA_SIZE);
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 5.5, 1.1, 4.4};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, DATA_SIZE);
    expectCol3->setValues(0, expectData3, DATA_SIZE);
    int64_t expectData4[DATA_SIZE] = {1, 1, 1, 2, 1, 2};
    LongVector *expectCol4 = new LongVector(nullptr, DATA_SIZE);
    expectCol4->setValues(0, expectData4, DATA_SIZE);

    VectorBatch *expectVecBatch = new VectorBatch(4);
    expectVecBatch->setVector(0, expectCol1);
    expectVecBatch->setVector(1, expectCol2);
    expectVecBatch->setVector(2, expectCol3);
    expectVecBatch->setVector(3, expectCol4);
    EXPECT_TRUE(vecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::freeVecBatch(vecBatch);
    VectorHelper::freeVecBatch(expectVecBatch);
    VectorHelper::freeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testRank)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t DATA_SIZE = 6;
    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch *vecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    column0->setValues(0, data0, DATA_SIZE);
    LongVector *column1 = new LongVector(nullptr, DATA_SIZE);
    column1->setValues(0, data1, DATA_SIZE);
    DoubleVector *column2 = new DoubleVector(nullptr, DATA_SIZE);
    column2->setValues(0, data2, DATA_SIZE);

    vecBatch->setVector(0, column0);
    vecBatch->setVector(1, column1);
    vecBatch->setVector(2, column2);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[3] = {1, 2, 0};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {WIN_RANK};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    int32_t allTypes[4] = {1, 2, 3, 2};
    int32_t argumentChannels[0] = {};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::createWindowOperatorFactory(sourceTypes, 3,
        outputCols, 3, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, 4, argumentChannels, 0);
    JitContext *jitContext = createTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(createTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    int64_t expectData1[DATA_SIZE] = {8, 8, 5, 4, 2, 1};
    LongVector *expectCol1 = new LongVector(nullptr, DATA_SIZE);
    expectCol1->setValues(0, expectData1, DATA_SIZE);
    double expectData2[DATA_SIZE] = {6.6, 3.3, 1.1, 2.2, 4.4, 5.5};
    DoubleVector *expectCol2 = new DoubleVector(nullptr, DATA_SIZE);
    expectCol2->setValues(0, expectData2, DATA_SIZE);
    int32_t expectData3[DATA_SIZE] = {0, 0, 2, 1, 2, 1};
    IntVector *expectCol3 = new IntVector(nullptr, DATA_SIZE);
    expectCol3->setValues(0, expectData3, DATA_SIZE);
    int64_t expectData4[DATA_SIZE] = {1, 1, 3, 4, 5, 6};
    LongVector *expectCol4 = new LongVector(nullptr, DATA_SIZE);
    expectCol4->setValues(0, expectData4, DATA_SIZE);
    VectorBatch *expectVecBatch = new VectorBatch(4);
    expectVecBatch->setVector(0, expectCol1);
    expectVecBatch->setVector(1, expectCol2);
    expectVecBatch->setVector(2, expectCol3);
    expectVecBatch->setVector(3, expectCol4);
    EXPECT_TRUE(vecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::freeVecBatch(vecBatch);
    VectorHelper::freeVecBatch(expectVecBatch);
    VectorHelper::freeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testRowNumberAndRankPartition)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t DATA_SIZE = 6;
    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch *vecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    column0->setValues(0, data0, DATA_SIZE);
    LongVector *column1 = new LongVector(nullptr, DATA_SIZE);
    column1->setValues(0, data1, DATA_SIZE);
    DoubleVector *column2 = new DoubleVector(nullptr, DATA_SIZE);
    column2->setValues(0, data2, DATA_SIZE);

    vecBatch->setVector(0, column0);
    vecBatch->setVector(1, column1);
    vecBatch->setVector(2, column2);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[2] = {WIN_RANK, WIN_ROW_NUMBER};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    int32_t allTypes[5] = {1, 2, 3, 2, 2};
    int32_t argumentChannels[2] = {-1,-1};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::createWindowOperatorFactory(sourceTypes, 3,
        outputCols, 3, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, 5, argumentChannels, 0);
    JitContext *jitContext = createTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(createTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    IntVector *expectCol1 = new IntVector(nullptr, DATA_SIZE);
    expectCol1->setValues(0, expectData1, DATA_SIZE);
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 1, 5, 2};
    LongVector *expectCol2 = new LongVector(nullptr, DATA_SIZE);
    expectCol2->setValues(0, expectData2, DATA_SIZE);
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 5.5, 1.1, 4.4};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, DATA_SIZE);
    expectCol3->setValues(0, expectData3, DATA_SIZE);
    int64_t expectData4[DATA_SIZE] = {1, 1, 1, 2, 1, 2};
    LongVector *expectCol4 = new LongVector(nullptr, DATA_SIZE);
    expectCol4->setValues(0, expectData4, DATA_SIZE);
    int64_t expectData5[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    LongVector *expectCol5 = new LongVector(nullptr, DATA_SIZE);
    expectCol5->setValues(0, expectData5, DATA_SIZE);
    VectorBatch *expectVecBatch = new VectorBatch(5);
    expectVecBatch->setVector(0, expectCol1);
    expectVecBatch->setVector(1, expectCol2);
    expectVecBatch->setVector(2, expectCol3);
    expectVecBatch->setVector(3, expectCol4);
    expectVecBatch->setVector(4, expectCol5);
    EXPECT_TRUE(vecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::freeVecBatch(vecBatch);
    VectorHelper::freeVecBatch(expectVecBatch);
    VectorHelper::freeVecBatches(outputVecBatches);
}

TEST(NativeOmniWindowOperatorTest, testAggregationPartition)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t DATA_SIZE = 6;
    // prepare data
    int32_t data0[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch *vecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    column0->setValues(0, data0, DATA_SIZE);
    LongVector *column1 = new LongVector(nullptr, DATA_SIZE);
    column1->setValues(0, data1, DATA_SIZE);
    DoubleVector *column2 = new DoubleVector(nullptr, DATA_SIZE);
    column2->setValues(0, data2, DATA_SIZE);

    vecBatch->setVector(0, column0);
    vecBatch->setVector(1, column1);
    vecBatch->setVector(2, column2);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t outputCols[3] = {0, 1, 2};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[5] = {WIN_SUM, WIN_COUNT, WIN_AVG, WIN_MAX, WIN_MIN};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    int32_t allTypes[8] = {1, 2, 3, 2, 2, 3, 1, 2};
    int32_t argumentChannels[5] = {1, 1, 1, 0, 1};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::createWindowOperatorFactory(sourceTypes, 3,
        outputCols, 3, windowFunctionTypes, 5, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, 8, argumentChannels, 5);
    JitContext *jitContext = createTestWindowJitContextWithFactory(operatorFactory);
    operatorFactory->SetJitContext(jitContext);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(createTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatches;
    windowOperator->GetOutput(outputVecBatches);

    int32_t expectData1[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    IntVector *expectCol1 = new IntVector(nullptr, DATA_SIZE);
    expectCol1->setValues(0, expectData1, DATA_SIZE);
    int64_t expectData2[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    LongVector *expectCol2 = new LongVector(nullptr, DATA_SIZE);
    expectCol2->setValues(0, expectData2, DATA_SIZE);
    double expectData3[DATA_SIZE] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, DATA_SIZE);
    expectCol3->setValues(0, expectData3, DATA_SIZE);
    int64_t expectData4[DATA_SIZE] = {5, 9, 12, 14, 15, 15};
    LongVector *expectCol4 = new LongVector(nullptr, DATA_SIZE);
    expectCol4->setValues(0, expectData4, DATA_SIZE);
    int64_t expectData5[DATA_SIZE] = {1, 2, 3, 4, 5, 6};
    LongVector *expectCol5 = new LongVector(nullptr, DATA_SIZE);
    expectCol5->setValues(0, expectData5, DATA_SIZE);
    double expectData6[DATA_SIZE] = {5.0, 4.5, 4.0, 3.5, 3.0, 2.5};
    DoubleVector *expectCol6 = new DoubleVector(nullptr, DATA_SIZE);
    expectCol6->setValues(0, expectData6, DATA_SIZE);
    int32_t expectData7[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    IntVector *expectCol7 = new IntVector(nullptr, DATA_SIZE);
    expectCol7->setValues(0, expectData7, DATA_SIZE);
    int64_t expectData8[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    LongVector *expectCol8 = new LongVector(nullptr, DATA_SIZE);
    expectCol8->setValues(0, expectData8, DATA_SIZE);
    VectorBatch *expectVecBatch = new VectorBatch(8);
    expectVecBatch->setVector(0, expectCol1);
    expectVecBatch->setVector(1, expectCol2);
    expectVecBatch->setVector(2, expectCol3);
    expectVecBatch->setVector(3, expectCol4);
    expectVecBatch->setVector(4, expectCol5);
    expectVecBatch->setVector(5, expectCol6);
    expectVecBatch->setVector(6, expectCol7);
    expectVecBatch->setVector(7, expectCol8);
    EXPECT_TRUE(vecBatchMatch(outputVecBatches[0], expectVecBatch));

    delete jitContext;
    delete windowOperator;
    delete operatorFactory;
    VectorHelper::freeVecBatch(vecBatch);
    VectorHelper::freeVecBatch(expectVecBatch);
    VectorHelper::freeVecBatches(outputVecBatches);
}
