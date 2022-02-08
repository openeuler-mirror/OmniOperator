/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort operator test implementations
 */

#include "gtest/gtest.h"
#include "../../src/operator/sort/sort_expr.h"
#include "../../src/vector/vector_helper.h"
#include "../util/test_util.h"
#include "../../src/operator/jit_context/jit_context.h"

using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::expressions;

TEST(SortWithExprTest, TestSortZeroExprColumns)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int outputCols[2] = {0, 1};
    std::string sortKeys[2] = {"#0", "#1"};
    Parser parser;
    std::vector<Expr *> sortExprs = parser.ParseExpressions(sortKeys, 2, sourceTypes);
    int ascendings[2] = {true, false};
    int nullFirsts[2] = {true, true};

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 2,
        sortExprs, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortWithExprJitContext(sourceTypes, outputCols, 2, sortExprs, ascendings, nullFirsts);
    operatorFactory->SetJitContext(jitContext);
    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(SortWithExprTest, TestSortOneExprColumns)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int outputCols[2] = {0, 1};
    std::string sortKeys[2] = {"#0", "ADD:2(#1, 50:2)"};
    Parser parser;
    std::vector<Expr *> sortExprs = parser.ParseExpressions(sortKeys, 2, sourceTypes);
    int ascendings[2] = {true, false};
    int nullFirsts[2] = {true, true};

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 2,
        sortExprs, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortWithExprJitContext(sourceTypes, outputCols, 2, sortExprs, ascendings, nullFirsts);
    operatorFactory->SetJitContext(jitContext);
    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(SortWithExprTest, TestSortTwoExprColumns)
{
    const int32_t dataSize = 5;
    int32_t data1[dataSize] = {4, 3, 2, 1, 0};
    int64_t data2[dataSize] = {0, 1, 2, 3, 4};
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType() }));
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, dataSize, data1, data2);

    int outputCols[2] = {0, 1};
    std::string sortKeys[2] = {"ADD:1(#0, 50:1)", "ADD:2(#1, 50:2)"};
    Parser parser;
    std::vector<Expr *> sortExprs = parser.ParseExpressions(sortKeys, 2, sourceTypes);
    int ascendings[2] = {true, false};
    int nullFirsts[2] = {true, true};

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 2,
        sortExprs, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortWithExprJitContext(sourceTypes, outputCols, 2, sortExprs, ascendings, nullFirsts);
    operatorFactory->SetJitContext(jitContext);
    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int32_t expectData1[dataSize] = {0, 1, 2, 3, 4};
    int64_t expectData2[dataSize] = {4, 3, 2, 1, 0};
    auto expectVecBatch = CreateVectorBatch(sourceTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    // free memory
    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(SortWithExprTest, TestSortTwoExprDictionaryColumns)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int64_t data2[dataSize] = {66, 55, 44, 33, 22, 11};
    void *datas[3] = {data0, data1, data2};
    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), LongVecType() }));
    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    VectorBatch *vecBatch = new VectorBatch(3, dataSize);
    for (int32_t i = 0; i < 3; i++) {
        VecType vecType = sourceTypes.Get()[i];
        vecBatch->SetVector(i, CreateDictionaryVector(vecType, dataSize, ids, dataSize, datas[i]));
    }

    int32_t outputCols[2] = {1, 2};
    std::string sortKeys[2] = {"ADD:1(#0, 50:1)", "ADD:2(50:2, #2)"};
    Parser parser;
    std::vector<Expr *> sortExprs = parser.ParseExpressions(sortKeys, 2, sourceTypes);
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 2,
        sortExprs, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortWithExprJitContext(sourceTypes, outputCols, 2, sortExprs, ascendings, nullFirsts);
    operatorFactory->SetJitContext(jitContext);
    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int64_t expectData1[dataSize] = {5, 2, 4, 1, 3, 0};
    int64_t expectData2[dataSize] = {11, 44, 22, 55, 33, 66};
    VecTypes expectedTypes(std::vector<VecType> { LongVecType(), LongVecType() });
    auto expectVecBatch = CreateVectorBatch(expectedTypes, dataSize, expectData1, expectData2);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(SortWithExprTest, TestSortOneVarcharExprColumn)
{
    VarcharVecType type(10);
    const int32_t dataSize = 4;
    const int32_t vecCount = 1;
    std::string values[dataSize] = {"hello", "world", "omni", "runtime"};
    VarcharVector *vector = CreateVarcharVector(type, values, dataSize);
    VectorBatch *vecBatch = new VectorBatch(vecCount, dataSize);
    vecBatch->SetVector(0, vector);

    VecTypes sourceTypes(std::vector<VecType>({ type }));
    int32_t outputCols[vecCount] = {0};
    std::string sortKeys[vecCount] = {"substr:15(#0, 1:1, 4:1)"};
    Parser parser;
    std::vector<Expr *> sortExprs = parser.ParseExpressions(sortKeys, 1, sourceTypes);
    int32_t ascendings[vecCount] = {true};
    int32_t nullFirsts[vecCount] = {true};

    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols,
        vecCount, sortExprs, ascendings, nullFirsts, vecCount);
    auto jitContext =
        CreateSortWithExprJitContext(sourceTypes, outputCols, vecCount, sortExprs, ascendings, nullFirsts);
    operatorFactory->SetJitContext(jitContext);
    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);
    VectorHelper::PrintVecBatch(outputVecBatches[0]);

    std::string expectValues[dataSize] = {"hello", "omni", "runtime", "world"};
    auto expectVector = CreateVarcharVector(type, expectValues, dataSize);
    auto expectVecBatch = new VectorBatch(vecCount, dataSize);
    expectVecBatch->SetVector(0, expectVector);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}

TEST(SortWithExprTest, TestSortTwoExprDictionaryWithNull)
{
    // construct input data
    const int32_t dataSize = 6;
    // prepare data
    int32_t data0[dataSize] = {0, 1, 2, 0, 1, 2};
    int64_t data1[dataSize] = {0, 1, 2, 3, 4, 5};
    int64_t data2[dataSize] = {66, 55, 44, 33, 22, 11};

    auto vec0 = CreateVector<IntVector>(data0, dataSize);
    auto vec1 = CreateVector<LongVector>(data1, dataSize);
    auto vec2 = CreateVector<LongVector>(data2, dataSize);
    for (int i = 0; i < dataSize; i = i + 2) {
        vec0->SetValueNull(i);
        vec1->SetValueNull(i);
        vec2->SetValueNull(i);
    }

    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    auto dictVec0 = new DictionaryVector(vec0, ids, dataSize);
    auto dictVec2 = new DictionaryVector(vec2, ids, dataSize);
    delete vec0;
    delete vec2;
    auto slicedVec0 = dictVec0->Slice(1, 5);
    auto slicedVec1 = vec1->Slice(1, 5);
    auto slicedVec2 = dictVec2->Slice(1, 5);
    delete dictVec0;
    delete vec1;
    delete dictVec2;

    auto vecBatch = new VectorBatch(3, 5);
    vecBatch->SetVector(0, slicedVec0);
    vecBatch->SetVector(1, slicedVec1);
    vecBatch->SetVector(2, slicedVec2);

    VecTypes sourceTypes(std::vector<VecType>({ IntVecType(), LongVecType(), LongVecType() }));
    int32_t outputCols[2] = {1, 2};
    std::string sortKeys[2] = {"ADD:1(#0, 50:1)", "ADD:2(50:2, #2)"};
    Parser parser;
    std::vector<Expr *> sortExprs = parser.ParseExpressions(sortKeys, 2, sourceTypes);
    int32_t ascendings[2] = {false, true};
    int32_t nullFirsts[2] = {true, true};
    auto operatorFactory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(sourceTypes, outputCols, 2,
        sortExprs, ascendings, nullFirsts, 2);
    auto jitContext = CreateSortWithExprJitContext(sourceTypes, outputCols, 2, sortExprs, ascendings, nullFirsts);
    operatorFactory->SetJitContext(jitContext);
    auto sortOperator = static_cast<SortWithExprOperator *>(CreateTestOperator(operatorFactory));
    sortOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> outputVecBatches;
    sortOperator->GetOutput(outputVecBatches);

    int64_t expectData1[5] = {0, 0, 5, 1, 3};
    int64_t expectData2[5] = {0, 0, 11, 55, 33};
    VecTypes expectedTypes(std::vector<VecType> { LongVecType(), LongVecType() });
    auto expectVecBatch = CreateVectorBatch(expectedTypes, 5, expectData1, expectData2);
    expectVecBatch->GetVector(0)->SetValueNull(0);
    expectVecBatch->GetVector(0)->SetValueNull(1);
    expectVecBatch->GetVector(1)->SetValueNull(0);
    expectVecBatch->GetVector(1)->SetValueNull(1);
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expectVecBatch));

    VectorHelper::FreeVecBatches(outputVecBatches);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(sortOperator);
    DeleteOperatorFactory(operatorFactory);
}
