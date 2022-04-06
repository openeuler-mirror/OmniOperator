/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort operator test implementations
 */

#include "gtest/gtest.h"
#include "operator/join/hash_builder_expr.h"
#include "operator/join/lookup_join_expr.h"
#include "vector/vector_helper.h"
#include "jit_context/jit_context.h"
#include "../util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;
using namespace TestUtil;

namespace JoinWithExprTest {
void DeleteJoinExprOperatorFactory(HashBuilderWithExprOperatorFactory *hashBuilderOperatorFactory,
    LookupJoinWithExprOperatorFactory *lookupJoinOperatorFactory)
{
    if (hashBuilderOperatorFactory != nullptr) {
        DeleteOperatorFactory(hashBuilderOperatorFactory);
    }

    if (lookupJoinOperatorFactory != nullptr) {
        DeleteOperatorFactory(lookupJoinOperatorFactory);
    }
}

std::vector<omniruntime::expressions::Expr *> CreateBuildHashKeys()
{
    omniruntime::expressions::FieldExpr *addLeft = new omniruntime::expressions::FieldExpr(1, LongType());
    omniruntime::expressions::LiteralExpr *addRight = new omniruntime::expressions::LiteralExpr(50L, LongType());
    omniruntime::expressions::BinaryExpr *addExpr = new omniruntime::expressions::BinaryExpr(
        omniruntime::expressions::Operator::ADD, addLeft, addRight, LongType());
    std::vector<omniruntime::expressions::Expr *> buildHashKeysExprs = { addExpr };
    return buildHashKeysExprs;
}

std::vector<omniruntime::expressions::Expr *> CreateProbeHashKeys()
{
    omniruntime::expressions::LiteralExpr *addLeftProbe = new omniruntime::expressions::LiteralExpr(50L, LongType());
    omniruntime::expressions::FieldExpr *addRightProbe = new omniruntime::expressions::FieldExpr(1, LongType());
    omniruntime::expressions::BinaryExpr *addExprProbe = new omniruntime::expressions::BinaryExpr(
        omniruntime::expressions::Operator::ADD, addLeftProbe, addRightProbe, LongType());
    std::vector<omniruntime::expressions::Expr *> probeHashKeysExprs = { addExprProbe };
    return probeHashKeysExprs;
}

TEST(JoinWithExprTest, TestInnerEqualityJoinOnKeyWithExpr)
{
    // construct input data
    const int32_t dataSize = 4;
    DataTypes buildTypes(std::vector<DataType>({ LongDataType(), LongDataType() }));
    int64_t buildData0[] = {1, 2, 3, 4};
    int64_t buildData1[] = {111, 11, 333, 33};
    VectorBatch *buildVecBatch = new VectorBatch(2, dataSize);
    buildVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(buildData0, dataSize));
    DataType dataType = buildTypes.Get()[1];
    int32_t ids[] = {0, 1, 2, 3};
    buildVecBatch->SetVector(1, CreateDictionaryVector(dataType, dataSize, ids, dataSize, buildData1));

    std::vector<omniruntime::expressions::Expr *> buildHashKeys = CreateBuildHashKeys();
    int32_t hashKeysCount = 1;
    std::string filter = "";
    int32_t hashTableCount = 1;
    HashBuilderWithExprOperatorFactory *hashBuilderWithExprOperatorFactory =
        HashBuilderWithExprOperatorFactory::CreateHashBuilderWithExprOperatorFactory(buildTypes, buildHashKeys,
        hashKeysCount, filter, hashTableCount);
    JitContext *buildContext = CreateHashBuilderWithExprJitContext(buildTypes, buildHashKeys, hashTableCount); // here
    hashBuilderWithExprOperatorFactory->SetJitContext(buildContext);
    auto *hashBuilderWithExprOperator = CreateTestOperator(hashBuilderWithExprOperatorFactory);
    hashBuilderWithExprOperator->AddInput(buildVecBatch);
    std::vector<VectorBatch *> hashBuilderOutput;
    hashBuilderWithExprOperator->GetOutput(hashBuilderOutput);

    DataTypes probeTypes(std::vector<DataType>({ LongDataType(), LongDataType() }));
    int64_t probeData0[] = {1, 2, 3, 4};
    int64_t probeData1[] = {11, 22, 33, 44};
    VectorBatch *probeVecBatch = new VectorBatch(2, dataSize);
    probeVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(probeData0, dataSize));
    DataType probeDataType = probeTypes.Get()[1];
    probeVecBatch->SetVector(1, CreateDictionaryVector(probeDataType, dataSize, ids, dataSize, probeData1));

    int32_t probeOutputCols[2]= {0, 1};
    int32_t probeOutputColsCount = 2;
    std::vector<omniruntime::expressions::Expr *> probeHashKeys = CreateProbeHashKeys();
    int32_t probeHashKeysCount = 1;
    int32_t buildOutputCols[2] = {0, 1};
    int32_t buildOutputColsCount = 2;
    DataTypes buildOutputTypes(std::vector<DataType>({ LongDataType(), LongDataType() }));
    auto hashBuilderFactoryAddr = (int64_t)hashBuilderWithExprOperatorFactory;
    auto lookupJoinWithExprOperatorFactory = LookupJoinWithExprOperatorFactory::CreateLookupJoinWithExprOperatorFactory(
        probeTypes, probeOutputCols, probeOutputColsCount, probeHashKeys, probeHashKeysCount, buildOutputCols,
        buildOutputTypes, JoinType::OMNI_JOIN_TYPE_INNER, hashBuilderFactoryAddr);
    auto probeContext = CreateLookupJoinWithExprJitContext(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashKeys, buildOutputTypes, buildOutputCols);
    lookupJoinWithExprOperatorFactory->SetJitContext(probeContext);
    auto lookupJoinWithExprOperator = CreateTestOperator(lookupJoinWithExprOperatorFactory);
    lookupJoinWithExprOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> lookupJoinOutput;
    lookupJoinWithExprOperator->GetOutput(lookupJoinOutput);

    EXPECT_EQ(lookupJoinOutput.size(), 1);
    VectorHelper::PrintVecBatch(lookupJoinOutput[0]);
    const int32_t expectedDataSize = 2;
    int64_t expectedDatas[4][expectedDataSize] = {
            {1, 3},
            {11, 33},
            {2, 4},
            {11, 33}};
    AssertVecBatchEquals(lookupJoinOutput[0], probeTypes.GetSize() + buildOutputColsCount, expectedDataSize,
        expectedDatas[0], expectedDatas[1], expectedDatas[2], expectedDatas[3]);

    VectorHelper::FreeVecBatches(lookupJoinOutput);
    omniruntime::op::Operator::DeleteOperator(hashBuilderWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(lookupJoinWithExprOperator);
    DeleteJoinExprOperatorFactory(hashBuilderWithExprOperatorFactory, lookupJoinWithExprOperatorFactory);
}

TEST(JoinWithExprTest, TestInnerEqualityJoinOnKeyWithoutExpr)
{
    // construct input data
    const int32_t dataSize = 4;
    DataTypes buildTypes(std::vector<DataType>({ LongDataType(), LongDataType() }));
    int64_t buildData0[] = {1, 2, 3, 4};
    int64_t buildData1[] = {111, 11, 333, 33};
    VectorBatch *buildVecBatch = new VectorBatch(2, dataSize);
    buildVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(buildData0, dataSize));
    DataType dataType = buildTypes.Get()[1];
    int32_t ids[] = {0, 1, 2, 3};
    buildVecBatch->SetVector(1, CreateDictionaryVector(dataType, dataSize, ids, dataSize, buildData1));

    std::vector<omniruntime::expressions::Expr *> buildHashKeys = { new omniruntime::expressions::FieldExpr(1,
        LongType()) };
    int32_t hashKeysCount = 1;
    std::string filter = "";
    int32_t hashTableCount = 1;
    HashBuilderWithExprOperatorFactory *hashBuilderWithExprOperatorFactory =
        HashBuilderWithExprOperatorFactory::CreateHashBuilderWithExprOperatorFactory(buildTypes, buildHashKeys,
        hashKeysCount, filter, hashTableCount);
    JitContext *buildContext = CreateHashBuilderWithExprJitContext(buildTypes, buildHashKeys, hashTableCount);
    hashBuilderWithExprOperatorFactory->SetJitContext(buildContext);
    auto *hashBuilderWithExprOperator = CreateTestOperator(hashBuilderWithExprOperatorFactory);
    hashBuilderWithExprOperator->AddInput(buildVecBatch);
    std::vector<VectorBatch *> hashBuilderOutput;
    hashBuilderWithExprOperator->GetOutput(hashBuilderOutput);

    DataTypes probeTypes(std::vector<DataType>({ LongDataType(), LongDataType() }));
    int64_t probeData0[] = {1, 2, 3, 4};
    int64_t probeData1[] = {11, 22, 33, 44};
    VectorBatch *probeVecBatch = new VectorBatch(2, dataSize);
    probeVecBatch->SetVector(0, CreateVector<LongVector, int64_t>(probeData0, dataSize));
    DataType probeDataType = probeTypes.Get()[1];
    probeVecBatch->SetVector(1, CreateDictionaryVector(probeDataType, dataSize, ids, dataSize, probeData1));

    int32_t probeOutputCols[2]= {0, 1};
    int32_t probeOutputColsCount = 2;
    std::vector<omniruntime::expressions::Expr *> probeHashKeys = { new omniruntime::expressions::FieldExpr(1,
        LongType()) };
    int32_t probeHashKeysCount = 1;
    int32_t buildOutputCols[2] = {0, 1};
    int32_t buildOutputColsCount = 2;
    DataTypes buildOutputTypes(std::vector<DataType>({ LongDataType(), LongDataType() }));
    auto hashBuilderFactoryAddr = (int64_t)hashBuilderWithExprOperatorFactory;
    auto lookupJoinWithExprOperatorFactory = LookupJoinWithExprOperatorFactory::CreateLookupJoinWithExprOperatorFactory(
        probeTypes, probeOutputCols, probeOutputColsCount, probeHashKeys, probeHashKeysCount, buildOutputCols,
        buildOutputTypes, JoinType::OMNI_JOIN_TYPE_INNER, hashBuilderFactoryAddr);
    auto probeContext = CreateLookupJoinWithExprJitContext(probeTypes, probeOutputCols, probeOutputColsCount,
        probeHashKeys, buildOutputTypes, buildOutputCols);
    lookupJoinWithExprOperatorFactory->SetJitContext(probeContext);
    auto lookupJoinWithExprOperator = CreateTestOperator(lookupJoinWithExprOperatorFactory);
    lookupJoinWithExprOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> lookupJoinOutput;
    lookupJoinWithExprOperator->GetOutput(lookupJoinOutput);

    EXPECT_EQ(lookupJoinOutput.size(), 1);
    VectorHelper::PrintVecBatch(lookupJoinOutput[0]);
    const int32_t expectedDataSize = 2;
    int64_t expectedDatas[4][expectedDataSize] = {
            {1, 3},
            {11, 33},
            {2, 4},
            {11, 33}};
    AssertVecBatchEquals(lookupJoinOutput[0], probeTypes.GetSize() + buildOutputColsCount, expectedDataSize,
        expectedDatas[0], expectedDatas[1], expectedDatas[2], expectedDatas[3]);

    VectorHelper::FreeVecBatches(lookupJoinOutput);
    omniruntime::op::Operator::DeleteOperator(hashBuilderWithExprOperator);
    omniruntime::op::Operator::DeleteOperator(lookupJoinWithExprOperator);
    DeleteJoinExprOperatorFactory(hashBuilderWithExprOperatorFactory, lookupJoinWithExprOperatorFactory);
}
}