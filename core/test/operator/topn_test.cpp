//
// Created by root on 6/21/21.
//

#include "gtest/gtest.h"
#include "../../src/operator/topn/topn.h"
#include "../util/test_util.h"
#include "../../src/vector/vector_helper.h"
#include <vector>
#include <iostream>
#include <chrono>
#include <src/operator/optimization.h>
#include "../../src/jit/jit.h"



JitContext *createTestTopNJitContext(
        int32_t *sourceTypes,
        int32_t sourceTypesCount,
        int32_t sortColsCount)
{
    using namespace omniruntime::jit;
    ParamValue p_sourceTypes = ParamValue(sourceTypes, sourceTypesCount);
    ParamValue p_sortColCount= ParamValue(&sortColsCount);

    auto * topNCompareSp=new Specialization();
    topNCompareSp->addSpecializedParam(4, &p_sortColCount);
    topNCompareSp->addSpecializedParam(5, &p_sourceTypes);

    std::map<string,Specialization> topNCompareSps={{OMNIJIT_TOPN_COMPARE,*topNCompareSp}};

    auto *topNContext=new omniruntime::jit::Context("topn",topNCompareSps,std::vector<std::string>(),std::vector<std::string>(),true);

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
    const int32_t DATA_SIZE = 6;
    const int32_t EXPECTED_DATA_SIZE = 5;

    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};

    VectorBatch *inputVecBatch = new VectorBatch(1);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    column0->SetValues(0,data0, DATA_SIZE);
    inputVecBatch->SetVector(0, column0);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t sourceTypes[1] = {1};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory=new TopNOperatorFactory(sourceTypes, 1, EXPECTED_DATA_SIZE, sortCols, ascendings, nullFirsts,
                                                                     1);
    JitContext *jitContext=createTestTopNJitContext(sourceTypes, 1, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = (TopNOperator *)createTestOperator(topNOperatorFactory);

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[EXPECTED_DATA_SIZE] = {0, 0, 1, 1, 2};
    IntVector *expectCol1 = new IntVector(nullptr, EXPECTED_DATA_SIZE);
    expectCol1->SetValues(0, expectData1,EXPECTED_DATA_SIZE);
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0,expectCol1);

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
    const int32_t DATA_SIZE = 6;
    const int32_t EXPECTED_DATA_SIZE = 5;

    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};

    VectorBatch *inputVecBatch = new VectorBatch(1);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    column0->SetValues(0,data0, DATA_SIZE);
    inputVecBatch->SetVector(0, column0);

    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t sourceTypes[1] = {1};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory=new TopNOperatorFactory(sourceTypes, 1, EXPECTED_DATA_SIZE, sortCols, ascendings, nullFirsts,
                                                                     1);

    JitContext *jitContext=createTestTopNJitContext(sourceTypes, 1, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = (TopNOperator *)createTestOperator(topNOperatorFactory);

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[EXPECTED_DATA_SIZE] = {2, 2, 1, 1, 0};
    IntVector *expectCol1 = new IntVector(nullptr, EXPECTED_DATA_SIZE);
    expectCol1->SetValues(0, expectData1,EXPECTED_DATA_SIZE);
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0,expectCol1);

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
    const int32_t DATA_SIZE = 6;
    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    column0->SetValues(0,data0, DATA_SIZE);
    LongVector * column1=new LongVector(nullptr,DATA_SIZE);
    column1->SetValues(0, data1, DATA_SIZE);
    DoubleVector *column2 = new DoubleVector(nullptr, DATA_SIZE);
    column2->SetValues(0, data2, DATA_SIZE);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);


    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {false, false};
    const int32_t EXPECTED_DATA_SIZE = 5;

    TopNOperatorFactory *topNOperatorFactory=new TopNOperatorFactory(sourceTypes, 3, EXPECTED_DATA_SIZE, sortCols, ascendings, nullFirsts,
                                                                     2);
    JitContext *jitContext=createTestTopNJitContext(sourceTypes, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = (TopNOperator *)createTestOperator(topNOperatorFactory);

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[EXPECTED_DATA_SIZE] = {0, 0, 1, 1, 2};
    IntVector *expectCol1 = new IntVector(nullptr, EXPECTED_DATA_SIZE);
    expectCol1->SetValues(0, expectData1, EXPECTED_DATA_SIZE);
    int64_t expectData2[EXPECTED_DATA_SIZE] = {0, 3, 1, 4, 2};
    LongVector *expectCol2 = new LongVector(nullptr, EXPECTED_DATA_SIZE);
    expectCol2->SetValues(0, expectData2, EXPECTED_DATA_SIZE);
    double expectData3[EXPECTED_DATA_SIZE] = {6.6, 3.3, 5.5, 2.2, 4.4};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, EXPECTED_DATA_SIZE);
    expectCol3->SetValues(0, expectData3, EXPECTED_DATA_SIZE);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0,expectCol1);
    expectVecorBatch->SetVector(1,expectCol2);
    expectVecorBatch->SetVector(2,expectCol3);

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
    const int32_t DATA_SIZE = 6;
    // prepare data
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    column0->SetValues(0,data0, DATA_SIZE);
    LongVector * column1=new LongVector(nullptr,DATA_SIZE);
    column1->SetValues(0, data1, DATA_SIZE);
    DoubleVector *column2 = new DoubleVector(nullptr, DATA_SIZE);
    column2->SetValues(0, data2, DATA_SIZE);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);


    int32_t rowCount = DATA_SIZE;
    int32_t rowCounts[1] = {rowCount};

    int32_t sourceTypes[3] = {1, 2, 3};
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, false};
    int32_t nullFirsts[2] = {false, false};
    const int32_t EXPECTED_DATA_SIZE = 5;

    TopNOperatorFactory *topNOperatorFactory=new TopNOperatorFactory(sourceTypes, 3, EXPECTED_DATA_SIZE, sortCols, ascendings, nullFirsts,
                                                                     2);
    JitContext *jitContext=createTestTopNJitContext(sourceTypes, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = (TopNOperator *)createTestOperator(topNOperatorFactory);

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[EXPECTED_DATA_SIZE] = {0, 0, 1, 1, 2};
    IntVector *expectCol1 = new IntVector(nullptr, EXPECTED_DATA_SIZE);
    expectCol1->SetValues(0, expectData1, EXPECTED_DATA_SIZE);
    int64_t expectData2[EXPECTED_DATA_SIZE] = {3, 0, 4, 1, 5};
    LongVector *expectCol2 = new LongVector(nullptr, EXPECTED_DATA_SIZE);
    expectCol2->SetValues(0, expectData2, EXPECTED_DATA_SIZE);
    double expectData3[EXPECTED_DATA_SIZE] = {3.3, 6.6, 2.2, 5.5, 1.1};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, EXPECTED_DATA_SIZE);
    expectCol3->SetValues(0, expectData3, EXPECTED_DATA_SIZE);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0,expectCol1);
    expectVecorBatch->SetVector(1,expectCol2);
    expectVecorBatch->SetVector(2,expectCol3);

    printVecBatch(outputVecorBatchs[0]);
    EXPECT_TRUE(vecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    delete topNOperator;
    delete topNOperatorFactory;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
}



