/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <vector>
#include <iostream>
#include <chrono>
#include "gtest/gtest.h"
#include "../../src/operator/topn/topn.h"
#include "../util/test_util.h"
#include "../../src/vector/vector_helper.h"
#include <src/operator/optimization.h>
#include "../../src/jit/jit.h"

using namespace omniruntime::vec;

JitContext *CreateTestTopNJitContext(VecTypes &sourceTypes, int32_t *sortCols, int32_t sourceTypesCount,
    int32_t sortColsCount)
{
    using namespace omniruntime::jit;
    ParamValue pSourceTypes = ParamValue(sourceTypes.GetIds(), sourceTypesCount);
    ParamValue pSortCols = ParamValue(sortCols, sortColsCount);
    ParamValue pSortColCount = ParamValue(&sortColsCount);

    auto *topNCompareSp = new Specialization();
    topNCompareSp->AddSpecializedParam(4, &pSortColCount);
    topNCompareSp->AddSpecializedParam(5, &pSortCols);
    topNCompareSp->AddSpecializedParam(6, &pSourceTypes);

    std::map<std::string, Specialization> topNCompareSps = { { OMNIJIT_TOPN_COMPARE, *topNCompareSp } };

    auto *topNContext = new omniruntime::jit::Context("topn", topNCompareSps, std::vector<std::string>(), true);

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context> { *topNContext });
    auto createOperatorFunc = jit->Specialize(std::vector<Optimization>());

    JitContext *jitContext = new JitContext;
    jitContext->func = createOperatorFunc;
    return jitContext;
}

TEST(NativeOmniTopNOperatorTest, TestTopNAscOneColumnPerformance) {
    using namespace omniruntime::op;
    using namespace std;

    // construct input data
    const int32_t dataSize = 100000000;
    const int32_t expectedDataSize = 5;

    // prepare data
    int32_t *data0=new int32_t[dataSize] ;
    for (int i = 0; i < dataSize; ++i){
        data0[i] = rand();
    }

    VectorBatch *inputVecBatch = new VectorBatch(1);
    IntVector *column0 = new IntVector(nullptr, dataSize);
    column0->SetValues(0, data0, dataSize);
    inputVecBatch->SetVector(0, column0);

    std::vector<VecType> types = { IntVecType::Instance() };
    VecTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory=new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts,
                                                                     1);
    JitContext *jitContext=CreateTestTopNJitContext(sourceTypes, sortCols,1, 1);
    topNOperatorFactory->SetJitContext(jitContext);


    auto s=clock();

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);

    auto e = clock();
    cout<<"topn performance takes: "<<(double)(e-s)/CLOCKS_PER_SEC<<endl;

    int32_t expectData1[expectedDataSize] = {7, 37, 51, 95, 95};
    IntVector *expectCol1 = new IntVector(nullptr, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    VectorHelper::PrintVecBatch(outputVecorBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    delete topNOperator;
    delete topNOperatorFactory;
    delete jitContext;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
}


TEST(NativeOmniTopNOperatorTest, TestTopNAscOneColumn)
{
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

    std::vector<VecType> types = { IntVecType::Instance() };
    VecTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 1, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {0, 1, 2, 2, 3};
    IntVector *expectCol1 = new IntVector(nullptr, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    VectorHelper::PrintVecBatch(outputVecorBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    delete topNOperator;
    delete topNOperatorFactory;
    delete jitContext;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
}

TEST(NativeOmniTopNOperatorTest, TestTopNDescOneColumn)
{
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

    std::vector<VecType> types = { IntVecType::Instance() };
    VecTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);

    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 1, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator =static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {2, 2, 1, 1, 0};
    IntVector *expectCol1 = new IntVector(nullptr, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    VectorHelper::PrintVecBatch(outputVecorBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    delete topNOperator;
    delete topNOperatorFactory;
    delete jitContext;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
}

TEST(NativeOmniTopNOperatorTest, TestTopNAscMultiColumn)
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
    LongVector *column1 = new LongVector(nullptr, dataSize);
    column1->SetValues(0, data1, dataSize);
    DoubleVector *column2 = new DoubleVector(nullptr, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    std::vector<VecType> types = { IntVecType::Instance(), LongVecType::Instance(), DoubleVecType::Instance() };
    VecTypes sourceTypes(types);
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {false, false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
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

    VectorHelper::PrintVecBatch(outputVecorBatchs[0]);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    delete topNOperator;
    delete topNOperatorFactory;
    delete jitContext;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
}

TEST(NativeOmniTopNOperatorTest, TestTopNDescMultiColumn){
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
    LongVector *column1 = new LongVector(nullptr, dataSize);
    column1->SetValues(0, data1, dataSize);
    DoubleVector *column2 = new DoubleVector(nullptr, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    std::vector<VecType> types = { IntVecType::Instance(), LongVecType::Instance(), DoubleVecType::Instance() };
    VecTypes sourceTypes(types);
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, false};
    int32_t nullFirsts[2] = {false, false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
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

    VectorHelper::PrintVecBatch(outputVecorBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    delete topNOperator;
    delete topNOperatorFactory;
    delete jitContext;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
}

TEST(NativeOmniTopNOperatorTest, TestTopNDescMultiColumnSortOnlyOneColumn)
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
    LongVector *column1 = new LongVector(nullptr, dataSize);
    column1->SetValues(0, data1, dataSize);
    DoubleVector *column2 = new DoubleVector(nullptr, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    std::vector<VecType> types = { IntVecType::Instance(), LongVecType::Instance(), DoubleVecType::Instance() };
    VecTypes sourceTypes(types);
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {2, 1, 0, 2, 1};
    IntVector *expectCol1 = new IntVector(nullptr, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {5, 4, 3, 2, 1};
    LongVector *expectCol2 = new LongVector(nullptr, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {1.1, 2.2, 3.3, 4.4, 5.5};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);

    VectorHelper::PrintVecBatch(outputVecorBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    delete topNOperator;
    delete topNOperatorFactory;
    delete jitContext;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
}

TEST(NativeOmniTopNOperatorTest, TestTopNAscMultiColumnNullFirstAndDictionaryVec)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(),DoubleVecType() }));
    VectorBatch *inputVecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {true, true};
    const int32_t expectedDataSize = 5;
    inputVecBatch->GetVector(0)->SetValueNull(dataSize - 1);
    inputVecBatch->GetVector(1)->SetValueNull(dataSize -1);

    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VecType vecType = sourceTypes.Get()[2];
    inputVecBatch->SetVector(2, CreateDictionaryVector(vecType, dataSize, ids, dataSize, data2));

    TopNOperatorFactory *topNOperatorFactory =
            new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVectorBatches;
    topNOperator->GetOutput(outputVectorBatches);
    int32_t expectData1[expectedDataSize] = {2,0, 0, 1, 1};
    IntVector *expectCol1 = new IntVector(nullptr, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {5,0, 3, 1, 4};
    LongVector *expectCol2 = new LongVector(nullptr, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {1.1,6.6, 3.3, 5.5, 2.2};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);
    expectVecorBatch->GetVector(0)->SetValueNull(0);
    expectVecorBatch->GetVector(1)->SetValueNull(0);

    VectorHelper::PrintVecBatch(outputVectorBatches[0]);

    EXPECT_TRUE(VecBatchMatch(outputVectorBatches[0], expectVecorBatch));
    EXPECT_TRUE(outputVectorBatches[0]->GetVector(0)->IsValueNull(0));
    EXPECT_TRUE(!outputVectorBatches[0]->GetVector(0)->IsValueNull(2));

    delete topNOperator;
    delete topNOperatorFactory;
    delete jitContext;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVectorBatches);
}

TEST(NativeOmniTopNOperatorTest, TestTopNAscMultiColumnNullFirst)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(),DoubleVecType() }));
    VectorBatch *inputVecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {true, true};
    const int32_t expectedDataSize = 5;
    inputVecBatch->GetVector(0)->SetValueNull(dataSize - 1);
    inputVecBatch->GetVector(1)->SetValueNull(dataSize -1);


    TopNOperatorFactory *topNOperatorFactory =
            new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVectorBatches;
    topNOperator->GetOutput(outputVectorBatches);
    int32_t expectData1[expectedDataSize] = {2,0, 0, 1, 1};
    IntVector *expectCol1 = new IntVector(nullptr, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {5,0, 3, 1, 4};
    LongVector *expectCol2 = new LongVector(nullptr, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {1.1,6.6, 3.3, 5.5, 2.2};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);
    expectVecorBatch->GetVector(0)->SetValueNull(0);
    expectVecorBatch->GetVector(1)->SetValueNull(0);

    VectorHelper::PrintVecBatch(outputVectorBatches[0]);

    EXPECT_TRUE(VecBatchMatch(outputVectorBatches[0], expectVecorBatch));
    EXPECT_TRUE(outputVectorBatches[0]->GetVector(0)->IsValueNull(0));
    EXPECT_TRUE(!outputVectorBatches[0]->GetVector(0)->IsValueNull(2));

    delete topNOperator;
    delete topNOperatorFactory;
    delete jitContext;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVectorBatches);
}


TEST(NativeOmniTopNOperatorTest, TestTopNAscMultiColumnNullLast)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {-1, 1, -1, 0, 1, -1};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, -1};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(),DoubleVecType() }));
    VectorBatch *inputVecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {false, true};
    const int32_t expectedDataSize = 5;
    inputVecBatch->GetVector(0)->SetValueNull(5);
    inputVecBatch->GetVector(0)->SetValueNull(2);
    inputVecBatch->GetVector(0)->SetValueNull(0);
    inputVecBatch->GetVector(1)->SetValueNull(5);


    TopNOperatorFactory *topNOperatorFactory =
            new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVectorBatches;
    topNOperator->GetOutput(outputVectorBatches);
    int32_t expectData1[expectedDataSize] = {0, 1, 1, -1, -1};
    IntVector *expectCol1 = new IntVector(nullptr, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {3, 1, 4, -1, 0};
    LongVector *expectCol2 = new LongVector(nullptr, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {3.3, 5.5, 2.2, 1.1, 6.6};
    DoubleVector *expectCol3 = new DoubleVector(nullptr, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);
    expectVecorBatch->GetVector(0)->SetValueNull(3);
    expectVecorBatch->GetVector(0)->SetValueNull(4);
    expectVecorBatch->GetVector(1)->SetValueNull(3);

    VectorHelper::PrintVecBatch(outputVectorBatches[0]);

    EXPECT_TRUE(VecBatchMatch(outputVectorBatches[0], expectVecorBatch));

    EXPECT_TRUE(outputVectorBatches[0]->GetVector(0)->IsValueNull(3));
    EXPECT_TRUE(!outputVectorBatches[0]->GetVector(0)->IsValueNull(2));

    delete topNOperator;
    delete topNOperatorFactory;
    delete jitContext;
    VectorHelper::FreeVecBatch(inputVecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVectorBatches);
}

TEST(NativeOmniTopNOperatorTest, TestTopNDate32AndDecimal64Column)
{
    using namespace omniruntime::op;

    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize=5;

    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int64_t data2[dataSize] = {66, 55, 44, 33, 22, 11};
    VecTypes sourceTypes(std::vector<VecType>({ Date32VecType(DAY), LongVecType(), Decimal64VecType(2,0) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =
            new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    topNOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    int32_t expectData0[expectedDataSize] = {2, 2, 1, 1, 0};
    int64_t expectData1[expectedDataSize] = {5, 2, 4, 1, 3};
    int64_t expectData2[expectedDataSize] = {11, 44, 22, 55, 33};
    VecTypes expectedTypes(std::vector<VecType>({ Date32VecType(DAY), LongVecType(), Decimal64VecType(2,1) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(sourceTypes, expectedDataSize, expectData0,expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    delete topNOperator;
    DeleteOperatorFactory(topNOperatorFactory);
}

TEST(NativeOmniTopNOperatorTest, TestTopNDecimal128Column)
{
    using namespace omniruntime::op;

    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize=5;

    // prepare data
    Decimal128 data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    Decimal128 data2[dataSize] = {66, 55, 44, 33, 22, 11};
    VecTypes sourceTypes(std::vector<VecType>({ Decimal128VecType(2,1), LongVecType(), Decimal128VecType(2,1) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =
            new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    topNOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    Decimal128 expectData0[expectedDataSize] = {2, 2, 1, 1, 0};
    int64_t expectData1[expectedDataSize] = {5, 2, 4, 1, 3};
    Decimal128 expectData2[expectedDataSize] = {11, 44, 22, 55, 33};
    VecTypes expectedTypes(std::vector<VecType>({ Decimal64VecType(2,1), LongVecType(), Decimal64VecType(2,1) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(sourceTypes, expectedDataSize, expectData0,expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    delete topNOperator;
    DeleteOperatorFactory(topNOperatorFactory);
}


TEST(NativeOmniTopNTest, TestTopNDoubleCharColumn)
{
    using namespace omniruntime::op;

    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize=5;
    // prepare data
    std::string data0[dataSize] = {"0", "1", "2", "0", "1", "2"};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    std::string data2[dataSize] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1"};
    VecTypes sourceTypes(std::vector<VecType>({ VarcharVecType(1), LongVecType(), VarcharVecType(3) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =
            new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    topNOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    std::string expectData0[expectedDataSize]={"2","2","1","1","0"};
    int64_t expectData1[expectedDataSize] = {5, 2, 4, 1, 3};
    std::string expectData2[expectedDataSize] = {"1.1", "4.4", "2.2", "5.5", "3.3"};
    VecTypes expectedTypes(std::vector<VecType>({ VarcharVecType(1),LongVecType(), VarcharVecType(3) }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, expectedDataSize, expectData0,expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    delete topNOperator;
    DeleteOperatorFactory(topNOperatorFactory);
}

TEST(NativeOmniTopNTest, TestTopNDoubleCharAndBooleanColumn)
{
    using namespace omniruntime::op;

    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize=5;
    // prepare data
    std::string data0[dataSize] = {"0", "1", "2", "0", "1", "2"};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    bool data2[dataSize] = {false, false, false,true,true,true};
    VecTypes sourceTypes(std::vector<VecType>({ VarcharVecType(1), LongVecType(), BooleanVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    const int32_t sortColCount=2;
    int32_t sortCols[sortColCount] = {0, 2};
    int32_t ascendings[sortColCount] = {false, false};
    int32_t nullFirsts[sortColCount] = {true, true};

    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =
            new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, sortColCount);
    JitContext *jitContext = CreateTestTopNJitContext(sourceTypes, sortCols, 3, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    topNOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    std::string expectData0[expectedDataSize]={"2","2","1","1","0"};
    int64_t expectData1[expectedDataSize] = {5, 2, 4, 1, 3};
    bool expectData2[expectedDataSize] = {true, false, true, false, true};
    VecTypes expectedTypes(std::vector<VecType>({ VarcharVecType(1),LongVecType(), BooleanVecType() }));
    VectorBatch *expectVecBatch = CreateVectorBatch(expectedTypes, expectedDataSize, expectData0,expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(vecBatch);
    delete topNOperator;
    DeleteOperatorFactory(topNOperatorFactory);
}
