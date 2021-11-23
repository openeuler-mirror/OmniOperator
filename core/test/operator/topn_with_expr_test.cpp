/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "gtest/gtest.h"
#include "../../src/operator/topn/topn_expr.h"
#include "../../src/vector/vector_helper.h"
#include "../../src/jit/jit.h"
#include "../../src/operator/optimization.h"
#include "../util/test_util.h"

using namespace omniruntime::vec;

JitContext *CreateTestTopNWithExprJitContext(VecTypes &sourceTypes, int32_t *sortCols, int32_t sourceTypesCount,
    int32_t sortColsCount)
{
    using namespace omniruntime::jit;
    using namespace std;
    ParamValue pSourceTypes = ParamValue(sourceTypes.GetIds(), sourceTypesCount);
    ParamValue pSortCols = ParamValue(sortCols, sortColsCount);
    ParamValue pSortColCount = ParamValue(&sortColsCount);

    auto *topNCompareSp = new Specialization();
    topNCompareSp->AddSpecializedParam(4, &pSortColCount);
    topNCompareSp->AddSpecializedParam(5, &pSortCols);
    topNCompareSp->AddSpecializedParam(6, &pSourceTypes);

    map<string, Specialization> topNCompareSps = { { OMNIJIT_TOPN_COMPARE, *topNCompareSp } };

    auto *topNWithExprContext = new omniruntime::jit::Context(
        GenerateOperatorTemplatePath("topn_expr"), map<string, Specialization>());
    auto *topNContext = new omniruntime::jit::Context(GenerateOperatorTemplatePath("topn"), topNCompareSps);

    Jit *jit = new Jit(vector<omniruntime::jit::Context> { *topNWithExprContext, *topNContext });
    jit->Specialize(vector<Optimization>());
    auto createOperatorFunc = jit->GetJitedFunction("CreateOperator");

    JitContext *jitContext = new JitContext;
    jitContext->func = createOperatorFunc;

    delete jit;
    delete topNWithExprContext;
    delete topNContext;
    delete topNCompareSp;

    return jitContext;
}

TEST(NativeOmniTopNWithExprOperatorTest, TestTopNWithAllExpr)
{
    using namespace omniruntime::op;
    using namespace std;

    // construct input data
    const int32_t dataSize = 8;
    const int32_t expectedDataSize = 5;
    const int32_t sortKeyCnt = 2;

    // prepare data
    int32_t data1[dataSize] = {5, 8, 8, 6, 8, 4, 13, 15};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L};
    int64_t data3[dataSize] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};

    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), LongVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::string sortKeys[sortKeyCnt] = {"ADD:1(#0, 5)", "MODULUS:2(#2, 3)"};
    int32_t ascendings[sortKeyCnt] = {false, true};
    int32_t nullFirsts[sortKeyCnt] = {false, false};
    int32_t sortCols[sortKeyCnt] = {3, 4};

    JitContext *jitContext = CreateTestTopNWithExprJitContext(sourceTypes, sortCols, sortKeyCnt,
        sortKeyCnt);
    TopNWithExprOperatorFactory *topNWithExprOperatorFactory =
        new TopNWithExprOperatorFactory(sourceTypes, expectedDataSize, sortKeys, ascendings, nullFirsts,
        sortKeyCnt);
    topNWithExprOperatorFactory->SetJitContext(jitContext);
    TopNWithExprOperator *topNWithExprOperator = static_cast<TopNWithExprOperator *>(
        CreateTestOperator(topNWithExprOperatorFactory));

    topNWithExprOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatchs;
    topNWithExprOperator->GetOutput(outputVecBatchs);

    int32_t expData1[dataSize] = {15, 13, 8, 8, 8};
    int64_t expData2[dataSize] = {23, 0, 5, 4, 3};
    int64_t expData3[dataSize] = {8, 7, 3, 1, 2};
    int32_t expData4[dataSize] = {20, 18, 13, 13, 13};
    int64_t expData5[dataSize] = {2, 1, 0, 1, 2};
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), LongVecType(), LongVecType(), IntVecType(),
        LongVecType() }));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3,
        expData4, expData5);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    delete topNWithExprOperator;
    DeleteOperatorFactory(topNWithExprOperatorFactory);

    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}

TEST(NativeOmniTopNWithExprOperatorTest, TestTopNWithPartialExpr)
{
    using namespace omniruntime::op;
    using namespace std;

    // construct input data
    const int32_t dataSize = 8;
    const int32_t expectedDataSize = 5;
    const int32_t sortKeyCnt = 2;

    // prepare data
    int32_t data1[dataSize] = {5, 8, 8, 6, 8, 4, 13, 15};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L};
    int64_t data3[dataSize] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};

    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), LongVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::string sortKeys[sortKeyCnt] = {"#0", "MODULUS:2(#2, 3)"};
    int32_t ascendings[sortKeyCnt] = {false, true};
    int32_t nullFirsts[sortKeyCnt] = {false, false};
    int32_t sortCols[sortKeyCnt] = {3, 4};

    JitContext *jitContext = CreateTestTopNWithExprJitContext(sourceTypes, sortCols, sortKeyCnt,
        sortKeyCnt);
    TopNWithExprOperatorFactory *topNWithExprOperatorFactory =
        new TopNWithExprOperatorFactory(sourceTypes, expectedDataSize, sortKeys, ascendings, nullFirsts,
        sortKeyCnt);
    topNWithExprOperatorFactory->SetJitContext(jitContext);
    TopNWithExprOperator *topNWithExprOperator = static_cast<TopNWithExprOperator *>(
        CreateTestOperator(topNWithExprOperatorFactory));

    topNWithExprOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatchs;
    topNWithExprOperator->GetOutput(outputVecBatchs);

    int32_t expData1[dataSize] = {15, 13, 8, 8, 8};
    int64_t expData2[dataSize] = {23, 0, 5, 4, 3};
    int64_t expData3[dataSize] = {8, 7, 3, 1, 2};
    int64_t expData4[dataSize] = {2, 1, 0, 1, 2};
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), LongVecType(), LongVecType(), LongVecType()}));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3,
        expData4);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    delete topNWithExprOperator;
    DeleteOperatorFactory(topNWithExprOperatorFactory);

    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}

TEST(NativeOmniTopNWithExprOperatorTest, TestTopNWithNoExpr)
{
    using namespace omniruntime::op;
    using namespace std;

    // construct input data
    const int32_t dataSize = 8;
    const int32_t expectedDataSize = 5;
    const int32_t sortKeyCnt = 2;

    // prepare data
    int32_t data1[dataSize] = {5, 8, 8, 6, 8, 4, 13, 15};
    int64_t data2[dataSize] = {2L, 5L, 3L, 11L, 4L, 3L, 0L, 23L};
    int64_t data3[dataSize] = {5L, 3L, 2L, 6L, 1L, 4L, 7L, 8L};

    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), LongVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3);

    std::string sortKeys[sortKeyCnt] = {"#0", "#2"};
    int32_t ascendings[sortKeyCnt] = {false, true};
    int32_t nullFirsts[sortKeyCnt] = {false, false};
    int32_t sortCols[sortKeyCnt] = {3, 4};

    JitContext *jitContext = CreateTestTopNWithExprJitContext(sourceTypes, sortCols, sortKeyCnt,
        sortKeyCnt);
    TopNWithExprOperatorFactory *topNWithExprOperatorFactory =
        new TopNWithExprOperatorFactory(sourceTypes, expectedDataSize, sortKeys, ascendings, nullFirsts,
        sortKeyCnt);
    topNWithExprOperatorFactory->SetJitContext(jitContext);
    TopNWithExprOperator *topNWithExprOperator = static_cast<TopNWithExprOperator *>(
        CreateTestOperator(topNWithExprOperatorFactory));

    topNWithExprOperator->AddInput(vecBatch);
    vector<VectorBatch *> outputVecBatchs;
    topNWithExprOperator->GetOutput(outputVecBatchs);

    int32_t expData1[dataSize] = {15, 13, 8, 8, 8};
    int64_t expData2[dataSize] = {23, 0, 4, 3, 5};
    int64_t expData3[dataSize] = {8, 7, 1, 2, 3};
    VecTypes expectTypes(std::vector<VecType>({ IntVecType(), LongVecType(), LongVecType() }));
    VectorBatch *expectVecorBatch = CreateVectorBatch(expectTypes, expectedDataSize, expData1, expData2, expData3);

    VectorHelper::PrintVecBatch(outputVecBatchs[0]);
    EXPECT_TRUE(VecBatchMatch(outputVecBatchs[0], expectVecorBatch));

    delete topNWithExprOperator;
    DeleteOperatorFactory(topNWithExprOperatorFactory);

    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(expectVecorBatch);
    VectorHelper::FreeVecBatches(outputVecBatchs);
}
