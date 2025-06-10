/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * @Description: lookup join operator test implementations
 */
#include <vector>
#include <thread>
#include <random>

#include "gtest/gtest.h"
#include "operator/join/hash_builder.h"
#include "operator/join/lookup_join.h"
#include "operator/join/lookup_join_wrapper.h"
#include "plannode/planNode.h"
#include "vector/vector_helper.h"
#include "util/config_util.h"
#include "util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace TestUtil;
using std::map;
using std::string;
using std::vector;

namespace JoinWithPlanNodeTest {
void DeleteHashBuilderAndLookupJoinOperatorFactory(HashBuilderOperatorFactory *hashBuilderOperatorFactory,
    LookupJoinOperatorFactory *lookupJoinOperatorFactory)
{
    delete hashBuilderOperatorFactory;
    delete lookupJoinOperatorFactory;
}

std::shared_ptr<const HashJoinNode> ConstructSimpleJoinKeyHashJoinNode(JoinType joinType, bool nullAware, bool isShuffle, ExprPtr filter)
{
    std::vector<std::shared_ptr<const FieldExpr>> leftKeys;
    std::vector<std::shared_ptr<const FieldExpr>> rightKeys;
    leftKeys.reserve(1);
    rightKeys.reserve(1);
    leftKeys.emplace_back(std::make_shared<const FieldExpr>(0, LongType()));
    rightKeys.emplace_back(std::make_shared<const FieldExpr>(0, LongType()));

    DataTypes probeTypes(std::vector<DataTypePtr>({ LongType(), LongType() }));
    DataTypes buildTypes(std::vector<DataTypePtr>({ LongType(), LongType() }));

    return std::make_shared<const HashJoinNode>("0", joinType, BuildSide::OMNI_BUILD_LEFT, nullAware, isShuffle, leftKeys, rightKeys, filter, nullptr, nullptr, probeTypes.Instance(), buildTypes.Instance());
}

VectorBatch *ConstructSimpleBuildVectorBatch()
{
    const int32_t dataSize = 10;
    DataTypes buildTypes(std::vector<DataTypePtr>({ LongType(), LongType() }));
    int64_t buildData0[dataSize] = {1, 2, 1, 2, 3, 4, 5, 6, 7, 1};
    int64_t buildData1[dataSize] = {79, 79, 70, 70, 70, 70, 70, 70, 70, 70};
    return CreateVectorBatch(buildTypes, dataSize, buildData0, buildData1);
}

VectorBatch *ConstructSimpleProbeVectorBatch()
{
    const int32_t dataSize = 10;
    DataTypes probeTypes(std::vector<DataTypePtr>({ LongType(), LongType() }));
    int64_t probeData0[] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
    int64_t probeData1[] = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
    return CreateVectorBatch(probeTypes, dataSize, probeData0, probeData1);
}

VectorBatch *ConstructSimpleExpectedVectorBatch()
{
    const uint32_t originalDataSize = 10;
    const uint32_t expectedDataSize = 18;

    int64_t expectedData0[originalDataSize] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
    int64_t expectedData1[originalDataSize] = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
    int64_t expectedData2[expectedDataSize] = {1, 1, 1, 2, 2, 3, 4, 5, 6, 1, 1, 1, 1, 1, 1, 2, 2, 3};
    int64_t expectedData3[expectedDataSize] = {79, 70, 70, 79, 70, 70, 70, 70, 70, 79, 70, 70, 79, 70, 70, 79, 70, 70};

    auto inputType = IntType();
    auto expectedVec0 = CreateVector<int64_t>(originalDataSize, expectedData0);
    auto expectedVec1 = CreateVector<int64_t>(originalDataSize, expectedData1);
    int32_t ids[expectedDataSize] = {0, 0, 0, 1, 1, 2, 3, 4, 5, 6, 6, 6, 7, 7, 7, 8, 8, 9};
    auto expectedDictVec0 =
            VectorHelper::CreateDictionary(ids, expectedDataSize, reinterpret_cast<Vector<int64_t> *>(expectedVec0));
    auto expectedDictVec1 =
            VectorHelper::CreateDictionary(ids, expectedDataSize, reinterpret_cast<Vector<int64_t> *>(expectedVec1));
    auto expectedVec2 = CreateVector<int64_t>(expectedDataSize, expectedData2);
    auto expectedVec3 = CreateVector<int64_t>(expectedDataSize, expectedData3);
    delete expectedVec0;
    delete expectedVec1;

    auto *vectorBatch = new VectorBatch(expectedDataSize);
    vectorBatch->Append(expectedDictVec0);
    vectorBatch->Append(expectedDictVec1);
    vectorBatch->Append(expectedVec2);
    vectorBatch->Append(expectedVec3);
    return vectorBatch;
}

TEST(NativeOmniJoinWithPlanNodeTest, TestInnerEqualityJoinWithOneBuildOp)
{
    auto joinNode = ConstructSimpleJoinKeyHashJoinNode(OMNI_JOIN_TYPE_INNER, false, true, nullptr);
    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(joinNode);
    auto *hashBuilderOperator = dynamic_cast<HashBuilderOperator *>(hashBuilderFactory->CreateOperator());
    VectorBatch *vecBatch = ConstructSimpleBuildVectorBatch();
    hashBuilderOperator->AddInput(vecBatch);
    VectorBatch *hashBuildOutput = nullptr;
    hashBuilderOperator->GetOutput(&hashBuildOutput);

    auto *queryConfig = new config::QueryConfig();
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::CreateLookupJoinOperatorFactory(joinNode, hashBuilderFactory, *queryConfig);
    auto *lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(lookupJoinFactory->CreateOperator());
    VectorBatch *probeVecBatch = ConstructSimpleProbeVectorBatch();
    lookupJoinOperator->AddInput(probeVecBatch);
    VectorBatch *outputVecBatch = nullptr;
    lookupJoinOperator->GetOutput(&outputVecBatch);

    VectorBatch *expectVecBatch = ConstructSimpleExpectedVectorBatch();
    EXPECT_EQ(outputVecBatch->GetRowCount(), expectVecBatch->GetRowCount());
    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    delete queryConfig;
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    omniruntime::op::Operator::DeleteOperator(hashBuilderOperator);
    omniruntime::op::Operator::DeleteOperator(lookupJoinOperator);
    DeleteHashBuilderAndLookupJoinOperatorFactory(hashBuilderFactory, lookupJoinFactory);
}

TEST(NativeOmniJoinWithPlanNodeTest, TestFullEqualityJoinWithOneBuildOp)
{
    auto joinNode = ConstructSimpleJoinKeyHashJoinNode(OMNI_JOIN_TYPE_FULL, false, true, nullptr);
    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::CreateHashBuilderOperatorFactory(joinNode);
    auto *hashBuilderOperator = dynamic_cast<HashBuilderOperator *>(CreateTestOperator(hashBuilderFactory));
    VectorBatch *vecBatch = ConstructSimpleBuildVectorBatch();
    hashBuilderOperator->AddInput(vecBatch);
    VectorBatch *hashBuildOutput = nullptr;
    hashBuilderOperator->GetOutput(&hashBuildOutput);

    LookupJoinWrapperOperatorFactory *lookupJoinWrapperOperatorFactory = LookupJoinWrapperOperatorFactory::CreateLookupJoinWrapperOperatorFactory(joinNode, hashBuilderFactory, config::QueryConfig());
    auto lookupJoinWrapperOperator = lookupJoinWrapperOperatorFactory->CreateOperator();
    VectorBatch *probeVecBatch = ConstructSimpleProbeVectorBatch();
    lookupJoinWrapperOperator->AddInput(probeVecBatch);
    VectorBatch *outputVecBatch = nullptr;
    lookupJoinWrapperOperator->GetOutput(&outputVecBatch);

    VectorBatch *expectVecBatch = ConstructSimpleExpectedVectorBatch();
    EXPECT_EQ(outputVecBatch->GetRowCount(), expectVecBatch->GetRowCount());
    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    VectorBatch *appendOutput;
    lookupJoinWrapperOperator->GetOutput(&appendOutput);
    int64_t expectedData0[1] = {0};
    int64_t expectedData1[1] = {0};
    int64_t expectedData2[1] = {7};
    int64_t expectedData3[1] = {70};
    auto expectedVec0 = CreateVector(1, expectedData0);
    auto expectedVec1 = CreateVector(1, expectedData1);
    auto expectedVec2 = CreateVector(1, expectedData2);
    auto expectedVec3 = CreateVector(1, expectedData3);
    auto vectorBatch = new VectorBatch(1);
    vectorBatch->Append(expectedVec0);
    vectorBatch->Append(expectedVec1);
    vectorBatch->Append(expectedVec2);
    vectorBatch->Append(expectedVec3);
    vectorBatch->Get(0)->SetNull(0);
    vectorBatch->Get(1)->SetNull(0);
    EXPECT_TRUE(VecBatchMatchIgnoreOrder(appendOutput, vectorBatch));

    VectorHelper::FreeVecBatch(vectorBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(appendOutput);
    omniruntime::op::Operator::DeleteOperator(lookupJoinWrapperOperator);
    omniruntime::op::Operator::DeleteOperator(hashBuilderOperator);
    delete hashBuilderFactory;
    delete lookupJoinWrapperOperatorFactory;
}