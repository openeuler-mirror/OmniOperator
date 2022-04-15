/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <vector>
#include <iostream>
#include <chrono>
#include "gtest/gtest.h"
#include "operator/topn/topn.h"
#include "vector/vector_helper.h"
#include "util/perf_util.h"
#include "jit_context/jit_context.h"
#include "../util/test_util.h"
#include "../../libconfig.h"

using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace std;
using namespace TestUtil;

namespace TopnTest {
TEST(NativeOmniTopNOperatorTest, TestTopNAscOneColumnPerformance)
{
    // construct input data
    const int32_t dataSize = 100000000;
    const int32_t expectedDataSize = 5;

    // prepare data
    int32_t *data0 = new int32_t[dataSize];
    for (int i = 0; i < dataSize; ++i) {
        data0[i] = dataSize - i;
    }

    VectorBatch *inputVecBatch1 = new VectorBatch(1);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNAscOneColumnPerformance");
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    inputVecBatch1->SetVector(0, column0);

    std::vector<DataType> types = { IntDataType::Instance() };
    DataTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    auto *perfUtil = new PerfUtil();
    perfUtil->Init();
    perfUtil->Reset();
    perfUtil->Start();
    auto s = clock();

    topNOperator->AddInput(inputVecBatch1);
    perfUtil->Stop();
    long instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("TopN with OmniJit, used %lld instructions\n", perfUtil->GetData());
    }

    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);

    auto e = clock();
    cout << "topn with OmniJit performance takes: " << (double)(e - s) / CLOCKS_PER_SEC << endl;

    int32_t expectData1[expectedDataSize] = {1, 2, 3, 4, 5};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    TopNOperatorFactory *topNOperatorFactory2 =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);

    TopNOperator *topNOperator2 = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory2));
    VectorBatch *inputVecBatch2 = DuplicateVectorBatch(inputVecBatch1);

    perfUtil->Init();
    perfUtil->Reset();
    perfUtil->Start();
    auto s2 = clock();

    topNOperator2->AddInput(inputVecBatch2);
    perfUtil->Stop();
    instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("TopN without OmniJit, used %lld instructions\n", perfUtil->GetData());
    }

    vector<VectorBatch *> outputVecorBatchs2;
    topNOperator2->GetOutput(outputVecorBatchs2);

    auto e2 = clock();
    cout << "topn without OmniJit performance takes: " << (double)(e2 - s2) / CLOCKS_PER_SEC << endl;

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs2[0], expectVecorBatch));

    delete[] data0;
    Operator::DeleteOperator(topNOperator);
    Operator::DeleteOperator(topNOperator2);
    DeleteOperatorFactory(topNOperatorFactory);
    DeleteOperatorFactory(topNOperatorFactory2);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    VectorHelper::FreeVecBatches(outputVecorBatchs2);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNInstruct)
{
    // construct input data
    const int32_t dataSize = 100000000;
    const int32_t expectedDataSize = 5;

    // prepare data
    int32_t *data0 = new int32_t[dataSize];
    for (int i = 0; i < dataSize; ++i) {
        data0[i] = dataSize % expectedDataSize;
    }

    VectorBatch *inputVecBatch1 = new VectorBatch(1);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNInstruct");
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    inputVecBatch1->SetVector(0, column0);
    VectorBatch *inputVecBatch2 = DuplicateVectorBatch(inputVecBatch1);

    std::vector<DataType> types = { IntDataType::Instance() };
    DataTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 1);
    topNOperatorFactory->SetJitContext(jitContext);
    auto s = clock();

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    auto *perfUtil = new PerfUtil();
    perfUtil->Init();
    perfUtil->Reset();
    perfUtil->Start();
    topNOperator->AddInput(inputVecBatch1);
    perfUtil->Stop();
    long instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("TopN with OmniJit, used %lld instructions\n", perfUtil->GetData());
    }
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    auto e = clock();
    cout << "topn with OmniJit performance takes: " << (double)(e - s) / CLOCKS_PER_SEC << endl;

    TopNOperatorFactory *topNOperatorFactoryWithoutJit =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    auto s2 = clock();
    auto topNOp = topNOperatorFactoryWithoutJit->CreateOperator();
    perfUtil->Init();
    perfUtil->Reset();
    perfUtil->Start();
    topNOp->AddInput(inputVecBatch2);
    perfUtil->Stop();
    instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("TopN without OmniJit, used %lld instructions\n", perfUtil->GetData());
    }
    vector<VectorBatch *> outputVecorBatchsWithoutJit;
    topNOp->GetOutput(outputVecorBatchsWithoutJit);
    auto e2 = clock();
    cout << "topn without OmniJit performance takes: " << (double)(e2 - s2) / CLOCKS_PER_SEC << endl;

    int32_t expectData1[expectedDataSize] = {7, 37, 51, 95, 95};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    delete[] data0;
    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    DeleteOperatorFactory(topNOperatorFactoryWithoutJit);
    Operator::DeleteOperator(topNOp);
    delete perfUtil;
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    VectorHelper::FreeVecBatches(outputVecorBatchsWithoutJit);
    delete vecAllocator;
}
TEST(NativeOmniTopNOperatorTest, TestTopNAscOneColumnPerformanceVarChar)
{
    // construct input data
    const int32_t dataSize = 1000;
    const int32_t expectedDataSize = 5;

    // prepare data
    VectorBatch *inputVecBatch = new VectorBatch(1);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNAscOneColumnPerformanceVarChar");
    VarcharVector *column0 = new VarcharVector(vecAllocator, dataSize, dataSize);
    for (int i = 0; i < dataSize; ++i) {
        std::string str = std::to_string(i % 10);
        column0->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    inputVecBatch->SetVector(0, column0);

    std::vector<DataType> types = { VarcharDataType(3) };
    DataTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    auto s = clock();

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);

    auto e = clock();
    cout << "topn performance takes: " << (double)(e - s) / CLOCKS_PER_SEC << endl;

    string expectData1[expectedDataSize] = {"0", "0", "0", "0", "0"};
    VarcharVector *expectCol1 = new VarcharVector(vecAllocator, expectedDataSize, expectedDataSize);
    for (int i = 0; i < 5; ++i) {
        string str = expectData1[i];
        expectCol1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNAscOneColumn)
{
    // construct input data
    const int32_t dataSize = 7;
    const int32_t expectedDataSize = 5;

    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 4, 5, 2, 3};

    VectorBatch *inputVecBatch = new VectorBatch(1);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNAscOneColumn");
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    inputVecBatch->SetVector(0, column0);

    std::vector<DataType> types = { IntDataType::Instance() };
    DataTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {0, 1, 2, 2, 3};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNAscOneColumnVarChar)
{
    // construct input data
    const int32_t dataSize = 7;
    const int32_t expectedDataSize = 5;

    // prepare data
    string data0[dataSize] = {"0", "1", "2", "4", "5", "2", "3"};

    VectorBatch *inputVecBatch = new VectorBatch(1);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNAscOneColumnVarChar");
    VarcharVector *column0 = new VarcharVector(vecAllocator, dataSize, dataSize);
    for (int i = 0; i < dataSize; ++i) {
        string str = data0[i];
        column0->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    inputVecBatch->SetVector(0, column0);

    std::vector<DataType> types = { VarcharDataType(3) };
    DataTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    string expectData1[expectedDataSize] = {"0", "1", "2", "2", "3"};
    VarcharVector *expectCol1 = new VarcharVector(vecAllocator, expectedDataSize, expectedDataSize);
    for (int i = 0; i < 5; ++i) {
        string str = expectData1[i];
        expectCol1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNAscOneColumnChar)
{
    // construct input data
    const int32_t dataSize = 7;
    const int32_t expectedDataSize = 5;

    // prepare data
    string data0[dataSize] = {"0", "1", "2", "4", "5", "2", "3"};

    VectorBatch *inputVecBatch = new VectorBatch(1);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNAscOneColumnChar");
    VarcharVector *column0 = new VarcharVector(vecAllocator, dataSize, dataSize);
    for (int i = 0; i < dataSize; ++i) {
        string str = data0[i];
        column0->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    inputVecBatch->SetVector(0, column0);

    std::vector<DataType> types = { CharDataType(3) };
    DataTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    string expectData1[expectedDataSize] = {"0", "1", "2", "2", "3"};
    VarcharVector *expectCol1 = new VarcharVector(vecAllocator, expectedDataSize, expectedDataSize);
    for (int i = 0; i < 5; ++i) {
        string str = expectData1[i];
        expectCol1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNDescOneColumn)
{
    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize = 5;

    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};

    VectorBatch *inputVecBatch = new VectorBatch(1);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNDescOneColumn");
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    inputVecBatch->SetVector(0, column0);

    std::vector<DataType> types = { IntDataType::Instance() };
    DataTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {2, 2, 1, 1, 0};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNDescOneColumnVarChar)
{
    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize = 5;

    // prepare data
    std::string data0[dataSize] = {"0", "1", "2", "0", "1", "2"};
    VectorBatch *inputVecBatch = new VectorBatch(1);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNDescOneColumnVarChar");
    VarcharVector *column0 = new VarcharVector(vecAllocator, dataSize, dataSize);
    for (int i = 0; i < dataSize; ++i) {
        std::string str = data0[i];
        column0->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    inputVecBatch->SetVector(0, column0);

    std::vector<DataType> types = { VarcharDataType(3) };
    DataTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    std::string expectData1[expectedDataSize] = {"2", "2", "1", "1", "0"};
    VarcharVector *expectCol1 =
        new VarcharVector(VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNDescOneColumnVarChar"),
        expectedDataSize, expectedDataSize);
    for (int i = 0; i < 5; ++i) {
        std::string str = expectData1[i];
        expectCol1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNDescOneColumnChar)
{
    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize = 5;

    // prepare data
    std::string data0[dataSize] = {"0", "1", "2", "0", "1", "2"};
    VectorBatch *inputVecBatch = new VectorBatch(1);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNDescOneColumnChar");
    VarcharVector *column0 = new VarcharVector(vecAllocator, dataSize, dataSize);
    for (int i = 0; i < dataSize; ++i) {
        std::string str = data0[i];
        column0->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    inputVecBatch->SetVector(0, column0);

    std::vector<DataType> types = { CharDataType(3) };
    DataTypes sourceTypes(types);
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    std::string expectData1[expectedDataSize] = {"2", "2", "1", "1", "0"};
    VarcharVector *expectCol1 =
        new VarcharVector(VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNDescOneColumnChar"),
        expectedDataSize, expectedDataSize);
    for (int i = 0; i < 5; ++i) {
        std::string str = expectData1[i];
        expectCol1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    VectorBatch *expectVecorBatch = new VectorBatch(1);
    expectVecorBatch->SetVector(0, expectCol1);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
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

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNAscMultiColumn");
    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    LongVector *column1 = new LongVector(vecAllocator, dataSize);
    column1->SetValues(0, data1, dataSize);
    DoubleVector *column2 = new DoubleVector(vecAllocator, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    std::vector<DataType> types = { IntDataType::Instance(), LongDataType::Instance(), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {false, false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {0, 0, 1, 1, 2};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {0, 3, 1, 4, 2};
    LongVector *expectCol2 = new LongVector(vecAllocator, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {6.6, 3.3, 5.5, 2.2, 4.4};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNAscMultiColumnVarChar)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    std::string data1[dataSize] = {"0", "1", "2", "3", "4", "5"};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNAscMultiColumnVarChar");
    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    VarcharVector *column1 = new VarcharVector(vecAllocator, dataSize, dataSize);
    for (int i = 0; i < dataSize; ++i) {
        std::string str = data1[i];
        column1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    DoubleVector *column2 = new DoubleVector(vecAllocator, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    std::vector<DataType> types = { IntDataType::Instance(), VarcharDataType(3), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {false, false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {0, 0, 1, 1, 2};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    std::string expectData2[expectedDataSize] = {"0", "3", "1", "4", "2"};
    VarcharVector *expectCol2 = new VarcharVector(vecAllocator, expectedDataSize, expectedDataSize);
    for (int i = 0; i < expectedDataSize; ++i) {
        std::string str = expectData2[i];
        expectCol2->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    double expectData3[expectedDataSize] = {6.6, 3.3, 5.5, 2.2, 4.4};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNAscMultiColumnChar)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    std::string data1[dataSize] = {"0", "1", "2", "3", "4", "5"};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNAscMultiColumnChar");
    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    VarcharVector *column1 = new VarcharVector(vecAllocator, dataSize, dataSize);
    for (int i = 0; i < dataSize; ++i) {
        std::string str = data1[i];
        column1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    DoubleVector *column2 = new DoubleVector(vecAllocator, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    std::vector<DataType> types = { IntDataType::Instance(), CharDataType(3), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {false, false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {0, 0, 1, 1, 2};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    std::string expectData2[expectedDataSize] = {"0", "3", "1", "4", "2"};
    VarcharVector *expectCol2 = new VarcharVector(vecAllocator, expectedDataSize, expectedDataSize);
    for (int i = 0; i < expectedDataSize; ++i) {
        std::string str = expectData2[i];
        expectCol2->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    double expectData3[expectedDataSize] = {6.6, 3.3, 5.5, 2.2, 4.4};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNDescMultiColumn)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNDescMultiColumn");
    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    LongVector *column1 = new LongVector(vecAllocator, dataSize);
    column1->SetValues(0, data1, dataSize);
    DoubleVector *column2 = new DoubleVector(vecAllocator, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    std::vector<DataType> types = { IntDataType::Instance(), LongDataType::Instance(), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, false};
    int32_t nullFirsts[2] = {false, false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {0, 0, 1, 1, 2};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {3, 0, 4, 1, 5};
    LongVector *expectCol2 = new LongVector(vecAllocator, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {3.3, 6.6, 2.2, 5.5, 1.1};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNDescMultiColumnVarChar)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    std::string data1[dataSize] = {"0", "1", "2", "3", "4", "5"};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNDescMultiColumnVarChar");
    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    VarcharVector *column1 = new VarcharVector(vecAllocator, dataSize, dataSize);
    for (int i = 0; i < dataSize; ++i) {
        std::string str = data1[i];
        column1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    DoubleVector *column2 = new DoubleVector(vecAllocator, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    std::vector<DataType> types = { IntDataType::Instance(), VarcharDataType(3), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, false};
    int32_t nullFirsts[2] = {false, false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {0, 0, 1, 1, 2};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    std::string expectData2[expectedDataSize] = {"3", "0", "4", "1", "5"};
    VarcharVector *expectCol2 = new VarcharVector(vecAllocator, expectedDataSize, expectedDataSize);
    for (int i = 0; i < expectedDataSize; ++i) {
        std::string str = expectData2[i];
        expectCol2->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    double expectData3[expectedDataSize] = {3.3, 6.6, 2.2, 5.5, 1.1};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNDescMultiColumnChar)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    std::string data1[dataSize] = {"0", "1", "2", "3", "4", "5"};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNDescMultiColumnChar");
    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    VarcharVector *column1 = new VarcharVector(vecAllocator, dataSize, dataSize);
    for (int i = 0; i < dataSize; ++i) {
        std::string str = data1[i];
        column1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    DoubleVector *column2 = new DoubleVector(vecAllocator, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    std::vector<DataType> types = { IntDataType::Instance(), CharDataType(3), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);
    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, false};
    int32_t nullFirsts[2] = {false, false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {0, 0, 1, 1, 2};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    std::string expectData2[expectedDataSize] = {"3", "0", "4", "1", "5"};
    VarcharVector *expectCol2 = new VarcharVector(vecAllocator, expectedDataSize, expectedDataSize);
    for (int i = 0; i < expectedDataSize; ++i) {
        std::string str = expectData2[i];
        expectCol2->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    double expectData3[expectedDataSize] = {3.3, 6.6, 2.2, 5.5, 1.1};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNAscMultiColumnNullFirstAndDictionaryVecVarChar)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    std::string data1[dataSize] = {"0", "1", "2", "3", "4", "5"};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
        "topn_TestTopNAscMultiColumnNullFirstAndDictionaryVecVarChar");
    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    VarcharVector *column1 = new VarcharVector(vecAllocator, dataSize, dataSize);
    for (int i = 0; i < dataSize; ++i) {
        std::string str = data1[i];
        column1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    DoubleVector *column2 = new DoubleVector(vecAllocator, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), VarcharDataType(3), DoubleDataType() }));

    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {true, true};
    const int32_t expectedDataSize = 5;
    inputVecBatch->GetVector(0)->SetValueNull(dataSize - 1);
    inputVecBatch->GetVector(1)->SetValueNull(dataSize - 1);

    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    DataType dataType = sourceTypes.Get()[2];
    delete inputVecBatch->GetVector(2);
    inputVecBatch->SetVector(2, CreateDictionaryVector(dataType, dataSize, ids, dataSize, data2));

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVectorBatches;
    topNOperator->GetOutput(outputVectorBatches);
    int32_t expectData1[expectedDataSize] = {2, 0, 0, 1, 1};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    std::string expectData2[expectedDataSize] = {"5", "0", "3", "1", "4"};
    VarcharVector *expectCol2 = new VarcharVector(vecAllocator, expectedDataSize, expectedDataSize);
    for (int i = 0; i < expectedDataSize; ++i) {
        std::string str = expectData2[i];
        expectCol2->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    double expectData3[expectedDataSize] = {1.1, 6.6, 3.3, 5.5, 2.2};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);
    expectVecorBatch->GetVector(0)->SetValueNull(0);
    expectVecorBatch->GetVector(1)->SetValueNull(0);

    EXPECT_TRUE(VecBatchMatch(outputVectorBatches[0], expectVecorBatch));
    EXPECT_TRUE(outputVectorBatches[0]->GetVector(0)->IsValueNull(0));
    EXPECT_TRUE(!outputVectorBatches[0]->GetVector(0)->IsValueNull(2));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVectorBatches);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNAscMultiColumnNullFirstAndDictionaryChar)
{
    using namespace omniruntime::op;
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    std::string data1[dataSize] = {"0", "1", "2", "3", "4", "5"};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};

    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
        "topn_TestTopNAscMultiColumnNullFirstAndDictionaryChar");
    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    VarcharVector *column1 = new VarcharVector(vecAllocator, dataSize, dataSize);
    for (int i = 0; i < dataSize; ++i) {
        std::string str = data1[i];
        column1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    DoubleVector *column2 = new DoubleVector(vecAllocator, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), CharDataType(3), DoubleDataType() }));

    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {true, true};
    const int32_t expectedDataSize = 5;
    inputVecBatch->GetVector(0)->SetValueNull(dataSize - 1);
    inputVecBatch->GetVector(1)->SetValueNull(dataSize - 1);

    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    DataType dataType = sourceTypes.Get()[2];
    delete inputVecBatch->GetVector(2);
    inputVecBatch->SetVector(2, CreateDictionaryVector(dataType, dataSize, ids, dataSize, data2));

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVectorBatches;
    topNOperator->GetOutput(outputVectorBatches);
    int32_t expectData1[expectedDataSize] = {2, 0, 0, 1, 1};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    std::string expectData2[expectedDataSize] = {"5", "0", "3", "1", "4"};
    VarcharVector *expectCol2 = new VarcharVector(vecAllocator, expectedDataSize, expectedDataSize);
    for (int i = 0; i < expectedDataSize; ++i) {
        std::string str = expectData2[i];
        expectCol2->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.size());
    }
    double expectData3[expectedDataSize] = {1.1, 6.6, 3.3, 5.5, 2.2};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);
    expectVecorBatch->GetVector(0)->SetValueNull(0);
    expectVecorBatch->GetVector(1)->SetValueNull(0);

    EXPECT_TRUE(VecBatchMatch(outputVectorBatches[0], expectVecorBatch));
    EXPECT_TRUE(outputVectorBatches[0]->GetVector(0)->IsValueNull(0));
    EXPECT_TRUE(!outputVectorBatches[0]->GetVector(0)->IsValueNull(2));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVectorBatches);
    delete vecAllocator;
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

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNDescMultiColumnSortOnlyOneColumn");
    VectorBatch *inputVecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(vecAllocator, dataSize);
    column0->SetValues(0, data0, dataSize);
    LongVector *column1 = new LongVector(vecAllocator, dataSize);
    column1->SetValues(0, data1, dataSize);
    DoubleVector *column2 = new DoubleVector(vecAllocator, dataSize);
    column2->SetValues(0, data2, dataSize);
    inputVecBatch->SetVector(0, column0);
    inputVecBatch->SetVector(1, column1);
    inputVecBatch->SetVector(2, column2);

    std::vector<DataType> types = { IntDataType::Instance(), LongDataType::Instance(), DoubleDataType::Instance() };
    DataTypes sourceTypes(types);
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    const int32_t expectedDataSize = 5;

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 1);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 1);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVecorBatchs;
    topNOperator->GetOutput(outputVecorBatchs);
    int32_t expectData1[expectedDataSize] = {2, 1, 0, 2, 1};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {5, 4, 3, 2, 1};
    LongVector *expectCol2 = new LongVector(vecAllocator, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {1.1, 2.2, 3.3, 4.4, 5.5};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);

    EXPECT_TRUE(VecBatchMatch(outputVecorBatchs[0], expectVecorBatch));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecorBatchs);
    delete vecAllocator;
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

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType(), DoubleDataType() }));
    VectorBatch *inputVecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {true, true};
    const int32_t expectedDataSize = 5;
    static_cast<IntVector *>(inputVecBatch->GetVector(0))->SetValueNull(dataSize - 1);
    static_cast<LongVector *>(inputVecBatch->GetVector(1))->SetValueNull(dataSize - 1);

    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    DataType dataType = sourceTypes.Get()[2];
    delete inputVecBatch->GetVector(2);
    inputVecBatch->SetVector(2, CreateDictionaryVector(dataType, dataSize, ids, dataSize, data2));

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
        "topn_TestTopNAscMultiColumnNullFirstAndDictionaryVec");
    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVectorBatches;
    topNOperator->GetOutput(outputVectorBatches);
    int32_t expectData1[expectedDataSize] = {2, 0, 0, 1, 1};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {5, 0, 3, 1, 4};
    LongVector *expectCol2 = new LongVector(vecAllocator, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {1.1, 6.6, 3.3, 5.5, 2.2};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);
    expectVecorBatch->GetVector(0)->SetValueNull(0);
    expectVecorBatch->GetVector(1)->SetValueNull(0);

    EXPECT_TRUE(VecBatchMatch(outputVectorBatches[0], expectVecorBatch));
    EXPECT_TRUE(outputVectorBatches[0]->GetVector(0)->IsValueNull(0));
    EXPECT_TRUE(!outputVectorBatches[0]->GetVector(0)->IsValueNull(2));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVectorBatches);
    delete vecAllocator;
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

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType(), DoubleDataType() }));
    VectorBatch *inputVecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t sortCols[2] = {0, 1};
    int32_t ascendings[2] = {true, true};
    int32_t nullFirsts[2] = {true, true};
    const int32_t expectedDataSize = 5;
    inputVecBatch->GetVector(0)->SetValueNull(dataSize - 1);
    inputVecBatch->GetVector(1)->SetValueNull(dataSize - 1);

    TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNAscMultiColumnNullFirst");
    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVectorBatches;
    topNOperator->GetOutput(outputVectorBatches);
    int32_t expectData1[expectedDataSize] = {2, 0, 0, 1, 1};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {5, 0, 3, 1, 4};
    LongVector *expectCol2 = new LongVector(vecAllocator, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {1.1, 6.6, 3.3, 5.5, 2.2};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);
    expectVecorBatch->GetVector(0)->SetValueNull(0);
    expectVecorBatch->GetVector(1)->SetValueNull(0);

    EXPECT_TRUE(VecBatchMatch(outputVectorBatches[0], expectVecorBatch));
    EXPECT_TRUE(outputVectorBatches[0]->GetVector(0)->IsValueNull(0));
    EXPECT_TRUE(!outputVectorBatches[0]->GetVector(0)->IsValueNull(2));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVectorBatches);
    delete vecAllocator;
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

    DataTypes sourceTypes(std::vector<DataType>({ IntDataType(), LongDataType(), DoubleDataType() }));
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
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("topn_TestTopNAscMultiColumnNullLast");
    topNOperator->AddInput(inputVecBatch);
    std::vector<VectorBatch *> outputVectorBatches;
    topNOperator->GetOutput(outputVectorBatches);
    int32_t expectData1[expectedDataSize] = {0, 1, 1, -1, -1};
    IntVector *expectCol1 = new IntVector(vecAllocator, expectedDataSize);
    expectCol1->SetValues(0, expectData1, expectedDataSize);
    int64_t expectData2[expectedDataSize] = {3, 1, 4, -1, 0};
    LongVector *expectCol2 = new LongVector(vecAllocator, expectedDataSize);
    expectCol2->SetValues(0, expectData2, expectedDataSize);
    double expectData3[expectedDataSize] = {3.3, 5.5, 2.2, 1.1, 6.6};
    DoubleVector *expectCol3 = new DoubleVector(vecAllocator, expectedDataSize);
    expectCol3->SetValues(0, expectData3, expectedDataSize);
    VectorBatch *expectVecorBatch = new VectorBatch(3);
    expectVecorBatch->SetVector(0, expectCol1);
    expectVecorBatch->SetVector(1, expectCol2);
    expectVecorBatch->SetVector(2, expectCol3);
    expectVecorBatch->GetVector(0)->SetValueNull(3);
    expectVecorBatch->GetVector(0)->SetValueNull(4);
    expectVecorBatch->GetVector(1)->SetValueNull(3);

    EXPECT_TRUE(VecBatchMatch(outputVectorBatches[0], expectVecorBatch));

    EXPECT_TRUE(outputVectorBatches[0]->GetVector(0)->IsValueNull(3));
    EXPECT_TRUE(!outputVectorBatches[0]->GetVector(0)->IsValueNull(2));

    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVectorBatches);
    delete vecAllocator;
}

TEST(NativeOmniTopNOperatorTest, TestTopNDate32AndDecimal64Column)
{
    using namespace omniruntime::op;

    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize = 5;

    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int64_t data2[dataSize] = {66, 55, 44, 33, 22, 11};
    DataTypes sourceTypes(std::vector<DataType>({ Date32DataType(DAY), LongDataType(), Decimal64DataType(2, 0) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    topNOperator->GetOutput(outputVecBatches);

    int32_t expectData0[expectedDataSize] = {2, 2, 1, 1, 0};
    int64_t expectData1[expectedDataSize] = {5, 2, 4, 1, 3};
    int64_t expectData2[expectedDataSize] = {11, 44, 22, 55, 33};
    DataTypes expectedTypes(std::vector<DataType>({ Date32DataType(DAY), LongDataType(), Decimal64DataType(2, 1) }));
    VectorBatch *expectVecBatch =
        CreateVectorBatch(sourceTypes, expectedDataSize, expectData0, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
}

TEST(NativeOmniTopNOperatorTest, TestTopNDecimal128Column)
{
    using namespace omniruntime::op;

    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize = 5;

    // prepare data
    Decimal128 data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    Decimal128 data2[dataSize] = {66, 55, 44, 33, 22, 11};
    DataTypes sourceTypes(
        std::vector<DataType>({ Decimal128DataType(2, 1), LongDataType(), Decimal128DataType(2, 1) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);
    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    topNOperator->GetOutput(outputVecBatches);

    Decimal128 expectData0[expectedDataSize] = {2, 2, 1, 1, 0};
    int64_t expectData1[expectedDataSize] = {5, 2, 4, 1, 3};
    Decimal128 expectData2[expectedDataSize] = {11, 44, 22, 55, 33};
    DataTypes expectedTypes(
        std::vector<DataType>({ Decimal64DataType(2, 1), LongDataType(), Decimal64DataType(2, 1) }));
    VectorBatch *expectVecBatch =
        CreateVectorBatch(sourceTypes, expectedDataSize, expectData0, expectData1, expectData2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
}

TEST(NativeOmniTopNTest, TestTopNDoubleCharColumn)
{
    using namespace omniruntime::op;

    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize = 5;
    // prepare data
    std::string data0[dataSize] = {"0", "1", "2", "0", "1", "2"};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    std::string data2[dataSize] = {"6.6", "5.5", "4.4", "3.3", "2.2", "1.1"};
    DataTypes sourceTypes(std::vector<DataType>({ VarcharDataType(1), LongDataType(), VarcharDataType(3) }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    int32_t sortCols[2] = {0, 2};
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, 2);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, 2);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    topNOperator->GetOutput(outputVecBatches);

    std::string expectData0[expectedDataSize]={"2", "2", "1", "1", "0"};
    int64_t expectData1[expectedDataSize] = {5, 2, 4, 1, 3};
    std::string expectData2[expectedDataSize] = {"1.1", "4.4", "2.2", "5.5", "3.3"};
    DataTypes expectedTypes(std::vector<DataType>({ VarcharDataType(1), LongDataType(), VarcharDataType(3) }));
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, expectedDataSize, expectData0, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
}

TEST(NativeOmniTopNTest, TestTopNDoubleCharAndBooleanColumn)
{
    using namespace omniruntime::op;

    // construct input data
    const int32_t dataSize = 6;
    const int32_t expectedDataSize = 5;
    // prepare data
    std::string data0[dataSize] = {"0", "1", "2", "0", "1", "2"};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    bool data2[dataSize] = {false, false, false, true, true, true};
    DataTypes sourceTypes(std::vector<DataType>({ VarcharDataType(1), LongDataType(), BooleanDataType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data0, data1, data2);

    const int32_t sortColCount = 2;
    int32_t sortCols[sortColCount] = {0, 2};
    int32_t ascendings[sortColCount] = {false, false};
    int32_t nullFirsts[sortColCount] = {true, true};

    omniruntime::op::TopNOperatorFactory *topNOperatorFactory =
        new TopNOperatorFactory(sourceTypes, expectedDataSize, sortCols, ascendings, nullFirsts, sortColCount);
    JitContext *jitContext = CreateTopNJitContext(sourceTypes, sortCols, sortColCount);
    topNOperatorFactory->SetJitContext(jitContext);

    TopNOperator *topNOperator = static_cast<TopNOperator *>(CreateTestOperator(topNOperatorFactory));

    topNOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    topNOperator->GetOutput(outputVecBatches);

    std::string expectData0[expectedDataSize]={"2", "2", "1", "1", "0"};
    int64_t expectData1[expectedDataSize] = {5, 2, 4, 1, 3};
    bool expectData2[expectedDataSize] = {true, false, true, false, true};
    DataTypes expectedTypes(std::vector<DataType>({ VarcharDataType(1), LongDataType(), BooleanDataType() }));
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectedTypes, expectedDataSize, expectData0, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    Operator::DeleteOperator(topNOperator);
    DeleteOperatorFactory(topNOperatorFactory);
}
}
