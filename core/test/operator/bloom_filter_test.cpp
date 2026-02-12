/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: ...
 */

#include "gtest/gtest.h"
#include "codegen/bloom_filter.h"
#include "util/test_util.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/non_group_aggregation.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace std;
using namespace omniruntime::TestUtil;

namespace BloomFilterTest {
TEST(BloomFilterTest, TestBloomFilterInit)
{
    std::vector<DataTypePtr> vecOfTypes = { IntType() };
    DataTypes inputTypes(vecOfTypes);
    // construct data
    const int32_t rowNum = 12;
    int32_t intVecData[rowNum] = {1, 6, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    VectorBatch *inputVectorBatch = CreateVectorBatch(inputTypes, rowNum, intVecData);

    auto *factory = BloomFilterOperatorFactory::CreateBloomFilterOperatorFactory(1);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(inputVectorBatch);

    VectorBatch *result = nullptr;
    op->GetOutput(&result);
    long longValue = (static_cast<Vector<int64_t> *>(result->Get(0)))->GetValue(0);
    BloomFilter *bfResult = (BloomFilter *)longValue;
    EXPECT_EQ(bfResult->GetNumHashFunctions(), 6);
    EXPECT_EQ(bfResult->GetBits()->GetWordsNum(), 4);
    VectorHelper::FreeVecBatch(result);
    omniruntime::op::Operator::DeleteOperator(op);
    delete factory;
}

std::unique_ptr<OperatorFactory> CreateFactory(std::vector<uint32_t> &aggFuncVec,
                                               std::vector<DataTypePtr> &inputTypeVec, std::vector<DataTypePtr> &outputTypeVec,
                                               std::vector<uint32_t> &aggColIdxVec, std::vector<uint32_t> &aggMaskVec, const bool inputRaw,
                                               const bool outputPartial)
{
    DataTypes inputTypes(inputTypeVec);
    auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggColIdxVec);
    auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(outputTypeVec));
    auto inputRawWrap = std::vector<bool>(1,inputRaw);
    auto outputPartialWrap = std::vector<bool>(1, outputPartial);
    auto factory = std::make_unique<AggregationOperatorFactory>(inputTypes, aggFuncVec, aggInputColsWrap,
                                                                aggMaskVec, aggOutputTypesWrap, inputRawWrap, outputPartialWrap, false);

    if (factory == nullptr) {
        return nullptr;
    }

    factory->Init();
    return factory;
}

BloomFilter* CreateBloomFilterFromVectorBatch(VectorBatch *vecBatch)
{
    auto baseVector = vecBatch->GetVectors();
    auto value = static_cast<Vector<LargeStringContainer<std::string_view>> *>(baseVector[0]);
    auto serializePtr = value->GetValue(0).data();

    BloomFilter* bf = new BloomFilter(const_cast<char *>(serializePtr));
    return bf;
}

TEST(BloomFilterTest, TestBloomFilterAggregator)
{
    // data
    std::vector<DataTypePtr> vecOfTypes = { LongType() };
    DataTypes inputTypes(vecOfTypes);
    // construct data
    const int32_t rowNum = 10;
    // Even numbers from 0 to 19
    int64_t inputDataVector1[rowNum];
    // Even numbers from 20 to 39
    int64_t inputDataVector2[rowNum];
    for (int i = 0; i < rowNum; i++) {
        inputDataVector1[i] = 2 * i;
        inputDataVector2[i] = 2 * (i + rowNum);
    }

    // Input of Partial1
    VectorBatch *inputVectorBatch1 = new VectorBatch(rowNum);
    Vector<int64_t> *vector1 = new Vector<int64_t>(rowNum);
    for (int i = 0; i < rowNum; i++) {
        vector1->SetValue(i, static_cast<int64_t>(inputDataVector1[i]));
    }
    inputVectorBatch1->Append(vector1);

    // Input of Partial2
    VectorBatch *inputVectorBatch2 = new VectorBatch(rowNum);
    Vector<int64_t> *vector2 = new Vector<int64_t>(rowNum);
    for (int i = 0; i < rowNum; i++) {
        vector2->SetValue(i, static_cast<int64_t>(inputDataVector2[i]));
    }
    inputVectorBatch2->Append(vector2);

    // Partial 1
    std::vector<uint32_t> partialAggFuncVector = {static_cast<uint32_t>(OMNI_AGGREGATION_TYPE_BLOOM_FILTER)};
    std::vector<DataTypePtr> partialInputTypeVector = {LongType()};
    std::vector<DataTypePtr> partialOutputTypeVector = {std::make_shared<VarBinaryDataType>(128)};
    std::vector<uint32_t> partialAggColIdxVector = {0};
    std::vector<uint32_t> partialAggMask = {static_cast<uint32_t>(-1)};
    auto partialFactory1 = CreateFactory(partialAggFuncVector, partialInputTypeVector, partialOutputTypeVector, partialAggColIdxVector, partialAggMask, true, true);

    op::Operator *aggPartial1 = partialFactory1->CreateOperator();

    // Partial 2
    std::vector<uint32_t> partialAggFuncVector2 = {static_cast<uint32_t>(OMNI_AGGREGATION_TYPE_BLOOM_FILTER)};
    std::vector<DataTypePtr> partialInputTypeVector2 = {LongType()};
    std::vector<DataTypePtr> partialOutputTypeVector2 = {std::make_shared<VarBinaryDataType>(128)};
    std::vector<uint32_t> partialAggColIdxVector2 = {0};
    std::vector<uint32_t> partialAggMask2 = {static_cast<uint32_t>(-1)};
    auto partialFactory2 = CreateFactory(partialAggFuncVector2, partialInputTypeVector2, partialOutputTypeVector2, partialAggColIdxVector2, partialAggMask2, true, true);

    op::Operator *aggPartial2 = partialFactory2->CreateOperator();

    // Final
    std::vector<uint32_t> finalAggFuncVector = {static_cast<uint32_t>(OMNI_AGGREGATION_TYPE_BLOOM_FILTER)};
    std::vector<DataTypePtr> finalInputTypeVector = {std::make_shared<VarBinaryDataType>(128)};
    std::vector<DataTypePtr> finalOutputTypeVector = {std::make_shared<VarBinaryDataType>(128)};
    std::vector<uint32_t> finalAggColIdxVector = {0};
    std::vector<uint32_t> finalAggMask = {static_cast<uint32_t>(-1)};
    auto finalFactory = CreateFactory(finalAggFuncVector, finalInputTypeVector, finalOutputTypeVector, finalAggColIdxVector, finalAggMask, false, false);

    op::Operator *aggFinal = finalFactory->CreateOperator();

    // partial 1 phase
    aggPartial1->Init();
    aggPartial1->AddInput(inputVectorBatch1);

    VectorBatch *partialOutputVecBatch1 = nullptr;
    aggPartial1->GetOutput(&partialOutputVecBatch1);

    // DeterMine whether the output of partial 1 is correct.
    auto partialBf1 = CreateBloomFilterFromVectorBatch(partialOutputVecBatch1);
    for (int i = 0; i < rowNum; i++) {
        EXPECT_TRUE(partialBf1->MightContainLong(inputDataVector1[i]));
        EXPECT_FALSE(partialBf1->MightContainLong(inputDataVector1[i] + 1));
    }

    // partial 2 phase
    aggPartial2->Init();
    aggPartial2->AddInput(inputVectorBatch2);

    VectorBatch *partialOutputVecBatch2 = nullptr;
    aggPartial2->GetOutput(&partialOutputVecBatch2);

    // DeterMine whether the output of partial 2 is correct.
    auto partialBf2 = CreateBloomFilterFromVectorBatch(partialOutputVecBatch2);
    for (int i = 0; i < rowNum; i++) {
        EXPECT_TRUE(partialBf2->MightContainLong(inputDataVector2[i]));
        EXPECT_FALSE(partialBf2->MightContainLong(inputDataVector2[i] + 1));
    }

    // final phase
    aggFinal->Init();
    aggFinal->AddInput(partialOutputVecBatch1);
    aggFinal->AddInput(partialOutputVecBatch2);

    VectorBatch *finalOutputVecBatch = nullptr;
    aggFinal->GetOutput(&finalOutputVecBatch);

    // DeterMine whether the output of final state output result is correct.
    auto finalBf = CreateBloomFilterFromVectorBatch(finalOutputVecBatch);
    for (int i = 0; i < rowNum; i++) {
        // DeterMine the input of inputDataVector1
        EXPECT_TRUE(finalBf->MightContainLong(inputDataVector1[i]));
        EXPECT_FALSE(finalBf->MightContainLong(inputDataVector1[i] + 1));
        // DeterMine the input of inputDataVector2
        EXPECT_TRUE(finalBf->MightContainLong(inputDataVector2[i]));
        EXPECT_FALSE(finalBf->MightContainLong(inputDataVector2[i] + 1));
    }

    VectorHelper::FreeVecBatch(finalOutputVecBatch);
    delete aggPartial1;
    delete aggPartial2;
    delete aggFinal;

    delete partialBf1;
    delete partialBf2;
    delete finalBf;
}

TEST(BloomFilterTest, TestBloomFilterPutLong)
{
    int32_t versionJava = 1;
    int32_t hashFuncNum = 6;
    int32_t numWords = 1048576; // 1MBytes

    int32_t byteLength = numWords * sizeof(uint64_t) + sizeof(versionJava) + sizeof(hashFuncNum) + sizeof(numWords);
    byte *in = new byte[byteLength]{ (byte)0 };
    (reinterpret_cast<int32_t *>(in))[0] = 1;
    (reinterpret_cast<int32_t *>(in))[1] = hashFuncNum;
    (reinterpret_cast<int32_t *>(in))[2] = numWords;
    BloomFilter *bf = new BloomFilter(reinterpret_cast<int8_t *>(in), versionJava);
    EXPECT_TRUE(bf->PutLong(LONG_MIN));
    EXPECT_TRUE(bf->PutLong(LONG_MAX));
    delete bf;
    delete[] in;
}

TEST(BloomFilterTest, TestBloomFilterMightContain)
{
    int32_t versionJava = 1;
    int32_t hashFuncNum = 6;
    int32_t numWords = 1048576; // 1MBytes

    int32_t byteLength = numWords * sizeof(uint64_t) + sizeof(versionJava) + sizeof(hashFuncNum) + sizeof(numWords);
    byte *in = new byte[byteLength]{ (byte)0 };
    (reinterpret_cast<int32_t *>(in))[0] = 1;
    (reinterpret_cast<int32_t *>(in))[1] = hashFuncNum;
    (reinterpret_cast<int32_t *>(in))[2] = numWords;
    BloomFilter *bf = new BloomFilter(reinterpret_cast<int8_t *>(in), versionJava);
    for (uint64_t i = 1; i < 100; i += 2) {
        EXPECT_TRUE(bf->PutLong(i));
    }

    for (int j = 1; j < 100; j += 2) {
        EXPECT_TRUE(bf->MightContainLong(j));
        EXPECT_FALSE(bf->MightContainLong(j - 1));
    }
    delete bf;
    delete[] in;
}
}