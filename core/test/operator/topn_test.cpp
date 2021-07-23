/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "gtest/gtest.h"
#include "../../src/operator/topn/topn.h"
#include "../util/test_util.h"
#include "../../src/vector/vector_helper.h"
#include <vector>
#include <iostream>
#include <chrono>
#include <src/operator/optimization.h>
#include "../../src/jit/jit.h"



JitContext *CreateTestTopNJitContext(int32_t *sourceTypes, int32_t sourceTypesCount, int32_t sortColsCount)
{
    using namespace omniruntime::jit;
    ParamValue pSourceTypes = ParamValue(sourceTypes, sourceTypesCount);
    ParamValue pSortColCount = ParamValue(&sortColsCount);

    auto * topNCompareSp = new Specialization();
    topNCompareSp->addSpecializedParam(4, &pSortColCount);
    topNCompareSp->addSpecializedParam(5, &pSourceTypes);

    std::map<string, Specialization> topNCompareSps={{OMNIJIT_TOPN_COMPARE, *topNCompareSp}};

    auto *topNContext = new omniruntime::jit::Context("topn",topNCompareSps,std::vector<std::string >(),std::vector<std::string >(),true);

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*topNContext});
    auto createOperatorFunc = jit->specialize();

    JitContext *jitContext = new JitContext;
    jitContext->func =createOperatorFunc;
    return jitContext;
}


TEST(NativeOmniTopNOperatorTest, testTopNAscOneColumn) {
    using namespace omniruntime::op;
    using namespace std;

    // construct input data
    const int32_t dataSize = 7;
    const int32_t expectedDataSize = 5;

    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 4, 5, 2, 3};

    VectorBatch *inputVecBatch = new VectorBatch(1);
    IntVector *column0 = new IntVector(nullptr, dataSize);
    column0->SetValues(0, data0, dataSize);
    inputVecBatch->SetVector(0, column0);

    int32_t sourceTypes[1] = {1};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory=new TopNOperatorFactory(sourceTypes, 1, expectedDataSize, sortCols, ascendings, nullFirsts,
                                                                     1);
    JitContext *jitContext=CreateTestTopNJitContext(sourceTypes, 1, 1);
    topNOperatorFactory->SetJitContext(nullptr);
//    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = dynamic_cast<TopNOperator *>(createTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {0, 1, 2, 2, 3};
    IntVector *expectCol1 = new IntVector(nullptr, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    printVecBatch(outputVecorBatchs[0]);
    EXPECT_TRUE(vecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    delete topNOperator;
    delete topNOperatorFactory;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
}

TEST(NativeOmniTopNOperatorTest, testTopNDescOneColumn) {
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize = 5;

    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};

    VectorBatch *inputVecBatch = new VectorBatch(1);
    IntVector *column0 = new IntVector(nullptr, dataSize);
    column0->SetValues(0, data0, dataSize);
    inputVecBatch->SetVector(0, column0);

    int32_t sourceTypes[1] = {1};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory=new TopNOperatorFactory(sourceTypes, 1, expectedDataSize, sortCols, ascendings, nullFirsts,
                                                                     1);

    JitContext *jitContext=CreateTestTopNJitContext(sourceTypes, 1, 1);
    topNOperatorFactory->SetJitContext(nullptr);

    TopNOperator *topNOperator = dynamic_cast<TopNOperator *>(createTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {2, 2, 1, 1, 0};
    IntVector *expectCol1 = new IntVector(nullptr, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    printVecBatch(outputVecorBatchs[0]);
    EXPECT_TRUE(vecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    delete topNOperator;
    delete topNOperatorFactory;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
}

TEST(NativeOmniTopNOperatorTest, testTopNAscMultiColumn)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, dataSize);
    column0->SetValues(0, data0, dataSize);
    LongVector * column1=new LongVector(nullptr, dataSize);
    column1->SetValues(0, data1, dataSize);
    DoubleVector *column2 = new DoubleVector(nullptr, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {false, false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory=new TopNOperatorFactory(sourceTypes, 3, expectedDataSize, sortCols, ascendings, nullFirsts,
                                                                     2);
    JitContext *jitContext=CreateTestTopNJitContext(sourceTypes, 3, 2);
    topNOperatorFactory->SetJitContext(nullptr);

    TopNOperator *topNOperator = dynamic_cast<TopNOperator *>(createTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {0, 0, 1, 1, 2};
    IntVector *expectCol1 = new IntVector(nullptr, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {0, 3, 1, 4, 2};
    LongVector *expectCol2 = new LongVector(nullptr, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {6.6, 3.3, 5.5, 2.2, 4.4};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);

    printVecBatch(outputVecorBatchs[0]);

    EXPECT_TRUE(vecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    delete topNOperator;
    delete topNOperatorFactory;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
}

TEST(NativeOmniTopNOperatorTest, testTopNDescMultiColumn) {
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, dataSize);
    column0->SetValues(0, data0, dataSize);
    LongVector * column1=new LongVector(nullptr, dataSize);
    column1->SetValues(0, data1, dataSize);
    DoubleVector *column2 = new DoubleVector(nullptr, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, false};
    int32_t nullFirsts[2] = {false, false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory=new TopNOperatorFactory(sourceTypes, 3, expectedDataSize, sortCols, ascendings, nullFirsts,
                                                                     2);
    JitContext *jitContext=CreateTestTopNJitContext(sourceTypes, 3, 2);
    topNOperatorFactory->SetJitContext(nullptr);

    TopNOperator *topNOperator = dynamic_cast<TopNOperator *>(createTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {0, 0, 1, 1, 2};
    IntVector *expectCol1 = new IntVector(nullptr, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {3, 0, 4, 1, 5};
    LongVector *expectCol2 = new LongVector(nullptr, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {3.3, 6.6, 2.2, 5.5, 1.1};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);

    printVecBatch(outputVecorBatchs[0]);
    EXPECT_TRUE(vecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    delete topNOperator;
    delete topNOperatorFactory;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
}



