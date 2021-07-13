#include "gtest/gtest.h"
#include "../../src/operator/join/hash_builder.h"
#include "../../src/operator/join/lookup_join.h"
#include "../../src/operator/hash_util.h"
#include "../util/test_util.h"
#include "../../src/jit/param_value.h"
#include "../../src/jit/jit.h"
#include "../../src/operator/optimization.h"
#include <src/operator/sort/sort.h>
#include <vector>
#include <chrono>
#include <thread>
#include <src/vector/vector_helper.h>
#include <src/vector/dictionary_vector.h>

// TEST(NativeOmniJoinTest, testHash)
// {
//     const int32_t DATA_SIZE = 10;
//     int64_t hash = 0;
//     int32_t partition = 0;
//
//     int64_t buildData[DATA_SIZE] = {1, 2, 1, 2, 3, 4 ,5 ,6 ,7 ,1};
//     for (int32_t i = 0; i < DATA_SIZE; i++) {
//         hash = HashUtil::hashValue(buildData[i]);
//         partition = HashUtil::getRawHashPartition(hash, 1);
//         std::cout << "BUILD hash(" << buildData[i] << ") = " << hash << ", partition=" << partition << std::endl;
//     }
//
//     int64_t probeData[DATA_SIZE] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
//     for (int32_t i = 0; i < DATA_SIZE; i++) {
//         hash = HashUtil::hashValue(probeData[i]);
//         partition = HashUtil::getRawHashPartition(hash, 1);
//         std::cout << "PROBE hash(" << probeData[i] << ") = " << hash << ", partition=" << partition << std::endl;
//     }
// }
using namespace omniruntime::op;

void deleteJoinOperatorFactory(HashBuilderOperatorFactory *hashBuilderOperatorFactory, LookupJoinOperatorFactory *lookupJoinOperatorFactory)
{
    if (hashBuilderOperatorFactory != nullptr) {
        if (hashBuilderOperatorFactory->GetJitContext() != nullptr) {
            delete hashBuilderOperatorFactory->GetJitContext();
        }
        delete hashBuilderOperatorFactory;
    }

    if (lookupJoinOperatorFactory != nullptr) {
        if (lookupJoinOperatorFactory->GetJitContext() != nullptr) {
            delete lookupJoinOperatorFactory->GetJitContext();
        }
        delete lookupJoinOperatorFactory;
    }
}

JitContext *createTestHashBuilderJitContext(
        int32_t *buildTypes,
        int32_t buildTypesCount,
        int32_t *buildOutputCols,
        int32_t buildOutputColsCount,
        int32_t *buildHashCols,
        int32_t buildHashColsCount,
        int32_t operatorCount)
{
    int32_t *hashColTypes = new int32_t[buildHashColsCount];
    for (int32_t i = 0; i < buildHashColsCount; i++) {
        hashColTypes[i] = buildTypes[buildHashCols[i]];
    }

    using namespace omniruntime::jit;
    ParamValue p_hashColTypes = ParamValue(hashColTypes, buildHashColsCount);
    ParamValue p_hashColCount = ParamValue(&buildHashColsCount);

    auto *hashPositionSp = new Specialization();
    hashPositionSp->addSpecializedParam(3, &p_hashColTypes);
    hashPositionSp->addSpecializedParam(4, &p_hashColCount);

    auto *positionEqualsPositionIgnoreNullsSp = new Specialization();
    positionEqualsPositionIgnoreNullsSp->addSpecializedParam(5, &p_hashColTypes);
    positionEqualsPositionIgnoreNullsSp->addSpecializedParam(6, &p_hashColCount);

    std::map<std::string, Specialization> hashStrategySps = {
        {OMNIJIT_HASH_STRATEGY_HASH_POSITION, *hashPositionSp},
        {OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_POSITION_IGNORE_NULLS, *positionEqualsPositionIgnoreNullsSp}
    };

    auto *hashBuilderContext = new omniruntime::jit::Context("hash_builder", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>(), true);
    auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *joinHashTableContext = new omniruntime::jit::Context("join_hash_table", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *pagesHashStrategyContext = new omniruntime::jit::Context("pages_hash_strategy", hashStrategySps, std::vector<std::string>(), std::vector<std::string>());
    auto *hashUtilContext = new omniruntime::jit::Context("hash_util", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*hashBuilderContext, *pagesIndexContext, *joinHashTableContext, *pagesHashStrategyContext, *hashUtilContext});
    auto CreateOperatorFunc = jit->specialize();
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(CreateOperatorFunc);
    return jitContext;
}

JitContext *createTestLookupJoinJitContext(
        int32_t *probeTypes,
        int32_t probeTypesCount,
        int32_t *probeOutputCols,
        int32_t probeOutputColsCount,
        int32_t *probeHashCols,
        int32_t probeHashColsCount,
        int32_t *buildOutputCols,
        int32_t *buildOutputTypes,
        int32_t buildOutputColsCount,
        int64_t hashBuilderFactoryAddr)
{
    using namespace omniruntime::jit;
    ParamValue p_probeTypes = ParamValue(probeTypes, probeTypesCount);
    ParamValue p_probeOutputCols = ParamValue(probeOutputCols, probeOutputColsCount);
    ParamValue p_probeOutputColsCount = ParamValue(&probeOutputColsCount);

    auto *buildProbeColumnsSp = new Specialization();
    buildProbeColumnsSp->addSpecializedParam(2, &p_probeTypes);
    buildProbeColumnsSp->addSpecializedParam(3, &p_probeOutputCols);
    buildProbeColumnsSp->addSpecializedParam(4, &p_probeOutputColsCount);

    ParamValue p_buildOutputTypes = ParamValue(buildOutputTypes, buildOutputColsCount);
    ParamValue p_buildOutputCols = ParamValue(buildOutputCols, buildOutputColsCount);
    ParamValue p_buildOutputColsCount = ParamValue(&buildOutputColsCount);

    auto *buildBuildColumnsSp = new Specialization();
    buildBuildColumnsSp->addSpecializedParam(2, &p_buildOutputTypes);
    buildBuildColumnsSp->addSpecializedParam(3, &p_buildOutputCols);
    buildBuildColumnsSp->addSpecializedParam(4, &p_buildOutputColsCount);
    buildBuildColumnsSp->addSpecializedParam(5, &p_probeOutputColsCount);

    std::map<std::string, Specialization> lookupJoinSps = {
            {OMNIJIT_CONSTRUCT_PROBE_COLUMNS_FROM_COPY, *buildProbeColumnsSp},
            {OMNIJIT_CONSTRUCT_BUILD_COLUMNS, *buildBuildColumnsSp}
    };

    int32_t *hashColTypes = new int32_t[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }
    ParamValue p_hashColTypes = ParamValue(hashColTypes, probeHashColsCount);
    ParamValue p_hashColCount = ParamValue(&probeHashColsCount);

    auto *hashRowSp = new Specialization();
    hashRowSp->addSpecializedParam(2, &p_hashColTypes);
    hashRowSp->addSpecializedParam(3, &p_hashColCount);
    std::map<std::string, Specialization> joinHashTableSps = {
            {OMNIJIT_HASH_ROW, *hashRowSp}
    };

    auto *positionEqualsRowIgnoreNullsSp = new Specialization();
    positionEqualsRowIgnoreNullsSp->addSpecializedParam(5, &p_hashColTypes);
    positionEqualsRowIgnoreNullsSp->addSpecializedParam(6, &p_hashColCount);

    auto *hashPositionSp = new Specialization();
    hashPositionSp->addSpecializedParam(3, &p_hashColTypes);
    hashPositionSp->addSpecializedParam(4, &p_hashColCount);

    std::map<std::string, Specialization> hashStrategySps = {
            {OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS, *positionEqualsRowIgnoreNullsSp}
    };

    auto *lookupJoinContext = new omniruntime::jit::Context("lookup_join", lookupJoinSps, std::vector<std::string>(), std::vector<std::string>(), true);
    auto *joinHashTableContext = new omniruntime::jit::Context("join_hash_table", joinHashTableSps, std::vector<std::string>(), std::vector<std::string>());
    auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *pagesHashStrategyContext = new omniruntime::jit::Context("pages_hash_strategy", hashStrategySps, std::vector<std::string>(), std::vector<std::string>());
    auto *hashUtilContext = new omniruntime::jit::Context("hash_util", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*lookupJoinContext, *joinHashTableContext, *pagesIndexContext, *pagesHashStrategyContext, *hashUtilContext, *memoryPoolContext});
    auto CreateOperatorFunc = jit->specialize();
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(CreateOperatorFunc);

    return jitContext;
}

TEST(NativeOmniJoinTest, testOneHashBuilderOneColumn)
{
    // construct input data
    const int32_t DATA_SIZE = 10;
    int64_t buildData0[DATA_SIZE] = {1, 2, 1, 2, 3, 4, 5, 6, 7, 1};
    int64_t buildData1[DATA_SIZE] = {79, 79, 70, 70, 70, 70, 70, 70, 70, 70};
    LongVector *buildColumn0 = new LongVector(nullptr, DATA_SIZE);
    buildColumn0->SetValues(0, buildData0, DATA_SIZE);
    LongVector *buildColumn1 = new LongVector(nullptr, DATA_SIZE);
    buildColumn1->SetValues(0, buildData1, DATA_SIZE);

    VectorBatch *vecBatch = new VectorBatch(2);
    vecBatch->SetVector(0, buildColumn0);
    vecBatch->SetVector(1, buildColumn1);

    int32_t buildTypes[2] = {2, 2};
    int32_t typesCount = 2;
    int32_t buildOutputCols[1] = {1};
    int32_t buildOutputColsCount = 1;
    int32_t buildJoinCols[1] = {0};
    int32_t joinColsCount = 1;
    int32_t operatorCount = 1;

    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::createHashBuilderOperatorFactory(
        buildTypes, typesCount, buildOutputCols, buildOutputColsCount, buildJoinCols, joinColsCount, operatorCount);
    JitContext *hashBuilderJitContext = createTestHashBuilderJitContext(
                buildTypes, typesCount, buildOutputCols, buildOutputColsCount, buildJoinCols, joinColsCount, operatorCount);
    hashBuilderFactory->SetJitContext(hashBuilderJitContext);
    HashBuilderOperator *hashBuilderOperator = (HashBuilderOperator *)createTestOperator(hashBuilderFactory);
    hashBuilderOperator->AddInput(vecBatch);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator->GetOutput(hashBuildOutput);

    int64_t probeData0[DATA_SIZE] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
    int64_t probeData1[DATA_SIZE] = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
    LongVector *probeColumn0 = new LongVector(nullptr, DATA_SIZE);
    probeColumn0->SetValues(0, probeData0, DATA_SIZE);
    LongVector *probeColumn1 = new LongVector(nullptr, DATA_SIZE);
    probeColumn1->SetValues(0, probeData1, DATA_SIZE);

    VectorBatch *probeVecBatch = new VectorBatch(2);
    probeVecBatch->SetVector(0, probeColumn0);
    probeVecBatch->SetVector(1, probeColumn1);

    int32_t probeTypes[2] = {2, 2};
    int32_t probeTypesCount = 2;
    int32_t probeOutputCols[1]= {1};
    int32_t probeOutputColsCount = 1;
    int32_t probeHashCols[1] = {0};
    int32_t probeHashColsCount = 1;
    int32_t buildOutputTypes[1] = {2};
    int64_t hashBuilderFactoryAddr = (int64_t)hashBuilderFactory;
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::createLookupJoinOperatorFactory(
        probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
    JitContext *lookupJoinJitContext = createTestLookupJoinJitContext(
                probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
                buildOutputCols, buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
    lookupJoinFactory->SetJitContext(lookupJoinJitContext);
    LookupJoinOperator *lookupJoinOperator = (LookupJoinOperator *)createTestOperator(lookupJoinFactory);
    lookupJoinOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->GetOutput(output);
    EXPECT_EQ(output.size(), 1);
    //output[0]->printTable();

    const int32_t EXPECTED_DATA_SIZE = 18;
    EXPECT_EQ(output[0]->GetRowCount(), EXPECTED_DATA_SIZE);

    int64_t expectedData0[EXPECTED_DATA_SIZE] = {78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 82, 82, 82, 82, 82, 65};
    int64_t expectedData1[EXPECTED_DATA_SIZE] = {70, 70, 79, 70, 79, 70, 70, 70, 70, 70, 70, 79, 70, 70, 79, 70, 79, 70};
    LongVector *expectedCol0 = new LongVector(nullptr, EXPECTED_DATA_SIZE);
    expectedCol0->SetValues(0, expectedData0, EXPECTED_DATA_SIZE);
    int32_t ids[EXPECTED_DATA_SIZE] = {0, 0, 0, 1, 1, 2, 3, 4, 5, 6, 6, 6, 7, 7, 7, 8, 8, 9};
    DictionaryVector *expectedVec0 = new DictionaryVector(expectedCol0, ids, EXPECTED_DATA_SIZE);

    LongVector *expectedCol1 = new LongVector(nullptr, EXPECTED_DATA_SIZE);
    expectedCol1->SetValues(0, expectedData1, EXPECTED_DATA_SIZE);

    VectorBatch *expectVecBatch = new VectorBatch(2);
    expectVecBatch->SetVector(0, expectedVec0);
    expectVecBatch->SetVector(1, expectedCol1);

    EXPECT_TRUE(vecBatchMatch(output[0], expectVecBatch));

    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    delete hashBuilderOperator;
    delete lookupJoinOperator;
    deleteJoinOperatorFactory(hashBuilderFactory, lookupJoinFactory);
}

TEST(NativeOmniJoinTest, testTwoHashBuilderOneColumn)
{
    const int32_t DATA_SIZE = 10;
    const int32_t DATA_SIZE1 = 6;
    const int32_t DATA_SIZE2 = 4;

    int64_t buildData00[DATA_SIZE1] = {1, 1, 3, 6, 7, 1};
    int64_t buildData01[DATA_SIZE1] = {79, 70, 70, 70, 70, 70};
    LongVector *buildColumn00 = new LongVector(nullptr, DATA_SIZE1);
    buildColumn00->SetValues(0, buildData00, DATA_SIZE1);
    LongVector *buildColumn01 = new LongVector(nullptr, DATA_SIZE1);
    buildColumn01->SetValues(0, buildData01, DATA_SIZE1);
    VectorBatch *vecBatch0 = new VectorBatch(2);
    vecBatch0->SetVector(0, buildColumn00);
    vecBatch0->SetVector(1, buildColumn01);

    int64_t buildData10[DATA_SIZE2] = {2, 2, 4, 5};
    int64_t buildData11[DATA_SIZE2] = {79, 70, 70, 70};
    LongVector *buildColumn10 = new LongVector(nullptr, DATA_SIZE2);
    buildColumn10->SetValues(0, buildData10, DATA_SIZE2);
    LongVector *buildColumn11 = new LongVector(nullptr, DATA_SIZE2);
    buildColumn11->SetValues(0, buildData11, DATA_SIZE2);
    VectorBatch *vecBatch1 = new VectorBatch(2);
    vecBatch1->SetVector(0, buildColumn10);
    vecBatch1->SetVector(1, buildColumn11);

    int32_t buildTypes[2] = {2, 2};
    int32_t typesCount = 2;
    int32_t buildOutputCols[1] = {1};
    int32_t buildOutputColsCount = 1;
    int32_t buildJoinCols[1] = {0};
    int32_t joinColsCount = 1;
    int32_t operatorCount = 2;

    HashBuilderOperatorFactory *hashBuilderFactory = HashBuilderOperatorFactory::createHashBuilderOperatorFactory(
            buildTypes, typesCount, buildOutputCols, buildOutputColsCount, buildJoinCols, joinColsCount, operatorCount);
    JitContext *hashBuilderJitContext = createTestHashBuilderJitContext(
            buildTypes, typesCount, buildOutputCols, buildOutputColsCount, buildJoinCols, joinColsCount, operatorCount);
    hashBuilderFactory->SetJitContext(hashBuilderJitContext);

    HashBuilderOperator *hashBuilderOperator0 = (HashBuilderOperator *)createTestOperator(hashBuilderFactory);
    hashBuilderOperator0->AddInput(vecBatch0);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator0->GetOutput(hashBuildOutput);

    HashBuilderOperator *hashBuilderOperator1 = (HashBuilderOperator *)createTestOperator(hashBuilderFactory);
    hashBuilderOperator1->AddInput(vecBatch1);
    hashBuilderOperator1->GetOutput(hashBuildOutput);

    int64_t probeData0[DATA_SIZE] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
    int64_t probeData1[DATA_SIZE] = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
    LongVector *probeColumn0 = new LongVector(nullptr, DATA_SIZE);
    probeColumn0->SetValues(0, probeData0, DATA_SIZE);
    LongVector *probeColumn1 = new LongVector(nullptr, DATA_SIZE);
    probeColumn1->SetValues(0, probeData1, DATA_SIZE);
    VectorBatch *probeVecBatch = new VectorBatch(2);
    probeVecBatch->SetVector(0, probeColumn0);
    probeVecBatch->SetVector(1, probeColumn1);

    int32_t probeTypes[2] = {2, 2};
    int32_t probeTypesCount = 2;
    int32_t probeOutputCols[1]= {1};
    int32_t probeOutputColsCount = 1;
    int32_t probeHashCols[1] = {0};
    int32_t probeHashColsCount = 1;
    int32_t buildOutputTypes[1] = {2};
    int64_t hashBuilderFactoryAddr = (int64_t)hashBuilderFactory;
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::createLookupJoinOperatorFactory(
        probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
    JitContext *lookupJoinJitContext = createTestLookupJoinJitContext(
                probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
                buildOutputCols, buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
    lookupJoinFactory->SetJitContext(lookupJoinJitContext);

    LookupJoinOperator *lookupJoinOperator = (LookupJoinOperator *)createTestOperator(lookupJoinFactory);
    lookupJoinOperator->AddInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->GetOutput(output);
    EXPECT_EQ(output.size(), 1);
    //output[0]->printTable();

    const int32_t EXPECTED_DATA_SIZE = 18;
    EXPECT_EQ(output[0]->GetRowCount(), EXPECTED_DATA_SIZE);

    int64_t expectedData0[EXPECTED_DATA_SIZE] = {78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 82, 82, 82, 82, 82, 65};
    int64_t expectedData1[EXPECTED_DATA_SIZE] = {70, 70, 79, 70, 79, 70, 70, 70, 70, 70, 70, 79, 70, 70, 79, 70, 79, 70};
    LongVector *expectedCol0 = new LongVector(nullptr, EXPECTED_DATA_SIZE);
    expectedCol0->SetValues(0, expectedData0, EXPECTED_DATA_SIZE);
    int32_t ids[EXPECTED_DATA_SIZE] = {0, 0, 0, 1, 1, 2, 3, 4, 5, 6, 6, 6, 7, 7, 7, 8, 8, 9};
    DictionaryVector *expectedVec0 = new DictionaryVector(expectedCol0, ids, EXPECTED_DATA_SIZE);

    LongVector *expectedCol1 = new LongVector(nullptr, EXPECTED_DATA_SIZE);
    expectedCol1->SetValues(0, expectedData1, EXPECTED_DATA_SIZE);

    VectorBatch *expectVecBatch = new VectorBatch(2);
    expectVecBatch->SetVector(0, expectedVec0);
    expectVecBatch->SetVector(1, expectedCol1);

    EXPECT_TRUE(vecBatchMatch(output[0], expectVecBatch));

    VectorHelper::FreeVecBatch(vecBatch0);
    VectorHelper::FreeVecBatch(vecBatch1);
    VectorHelper::FreeVecBatch(probeVecBatch);
    VectorHelper::FreeVecBatch(expectVecBatch);
    delete hashBuilderOperator0;
    delete hashBuilderOperator1;
    delete lookupJoinOperator;
    deleteJoinOperatorFactory(hashBuilderFactory, lookupJoinFactory);
}

const int32_t VEC_BATCH_COUNT_10 = 10;
const int32_t VEC_BATCH_COUNT_1 = 1;
const int32_t BUILD_POSITION_COUNT = 1000000;
const int32_t PROBE_POSITION_COUNT = 10000;
const int32_t COLUMN_COUNT_4 = 4;
const int32_t TIME_TO_SLEEP = 100;

struct HashJoinThreadArgs
{
    bool isOriginal;
    int64_t hashBuilderFactoryAddr;
    VectorBatch **buildVecBatches;
    int32_t buildVecBatchCount;
    int32_t partitionIndex;
    int64_t lookupJoinFactoryAddr;
    VectorBatch **probeVecBatches;
    int32_t probeVecBatchCount;
};

VectorBatch **constructHashBuilderTestData(int32_t tableCount, int32_t columnCount)
{
    int32_t numbers[] = {1, 2, 3, 4, 6, 7, 8, 9, 12, 13, 75, 27, 28, 38, 36, 32, 20, 50, 37};
    int32_t numberCount = 19;
    VectorBatch **vectorBatches = new VectorBatch *[tableCount];
    int32_t positionCount = BUILD_POSITION_COUNT;

    for (int32_t vecBatchIdx = 0; vecBatchIdx < tableCount; vecBatchIdx++) {
        VectorBatch *vecBatch = new VectorBatch(columnCount);
        for (int32_t vecIdx = 0; vecIdx < columnCount; vecIdx++) {
            LongVector *vector = new LongVector(nullptr, positionCount);
            for (int32_t position = 0; position < positionCount; position++) {
                int64_t value = numbers[position % numberCount];
                vector->SetValue(position, value);
            }
            vecBatch->SetVector(vecIdx, vector);
        }
        vectorBatches[vecBatchIdx] = vecBatch;
    }

    return vectorBatches;
}

HashBuilderOperatorFactory *prepareHashBuilder(int32_t operatorCount, bool isOriginal)
{
    int32_t buildTypes[] = {2, 2, 2, 2};
    int32_t buildTypesCount = 4;
    int32_t buildOutputCols[] = {0, 1};
    int32_t buildOutputColsCount = 2;
    int32_t buildHashCols[] = {2, 3};
    int32_t buildHashColsCount = 2;
    HashBuilderOperatorFactory *hashBuilderOperatorFactory = HashBuilderOperatorFactory::createHashBuilderOperatorFactory(
            buildTypes, buildTypesCount, buildOutputCols, buildOutputColsCount, buildHashCols, buildHashColsCount, operatorCount);
    if (isOriginal) {
        hashBuilderOperatorFactory->SetJitContext(nullptr);
    }
    else {
        JitContext *hashBuilderJitContext = createTestHashBuilderJitContext(buildTypes, buildTypesCount,
                                                                            buildOutputCols, buildOutputColsCount,
                                                                            buildHashCols, buildHashColsCount,
                                                                            operatorCount);
        hashBuilderOperatorFactory->SetJitContext(hashBuilderJitContext);
    }
    return hashBuilderOperatorFactory;
}

LookupJoinOperatorFactory *prepareLookupJoin(HashBuilderOperatorFactory *hashBuilderOperatorFactory, bool isOriginal)
{
    int32_t probeTypes[] = {2, 2, 2, 2};
    int32_t probeTypesCount = 4;
    int32_t probeOutputCols[] = {0, 1};
    int32_t probeOutputColsCount = 2;
    int32_t probeHashCols[] = {2, 3};
    int32_t probeHashColsCount = 2;
    int32_t buildOutputCols[] = {0, 1};
    int32_t buildOutputColsCount = 2;
    int32_t buildOutputTypes[] = {2, 2};
    int64_t hashBuilderFactoryAddr = (int64_t)hashBuilderOperatorFactory;
    LookupJoinOperatorFactory *lookupJoinOperatorFactory = LookupJoinOperatorFactory::createLookupJoinOperatorFactory(
            probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
            buildOutputCols, buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
    if (isOriginal) {
        lookupJoinOperatorFactory->SetJitContext(nullptr);
    }
    else {
        JitContext *lookupJoinJitContext = createTestLookupJoinJitContext(
                probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
                buildOutputCols, buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
        lookupJoinOperatorFactory->SetJitContext(lookupJoinJitContext);
    }
    return lookupJoinOperatorFactory;
}

void setHashJoinThreadArgs(
        struct HashJoinThreadArgs *hashJoinThreadArgs,
        bool isOriginal,
        int64_t hashBuilderFactoryAddr,
        VectorBatch **buildVecBatches,
        int32_t buildVecBatchCount,
        int64_t lookupJoinFactoryAddr,
        VectorBatch **probeVecBatches,
        int32_t probeVecBatchCount)
{
    hashJoinThreadArgs->isOriginal = isOriginal;
    hashJoinThreadArgs->hashBuilderFactoryAddr = hashBuilderFactoryAddr;
    hashJoinThreadArgs->buildVecBatches = buildVecBatches;
    hashJoinThreadArgs->buildVecBatchCount = buildVecBatchCount;
    hashJoinThreadArgs->lookupJoinFactoryAddr = lookupJoinFactoryAddr;
    hashJoinThreadArgs->probeVecBatches = probeVecBatches;
    hashJoinThreadArgs->probeVecBatchCount = probeVecBatchCount;
    hashJoinThreadArgs->partitionIndex = -1;
}

void testHashBuilder(struct HashJoinThreadArgs *hashJoinThreadArgs)
{
    HashBuilderOperatorFactory *hashBuilderOperatorFactory = (HashBuilderOperatorFactory *)hashJoinThreadArgs->hashBuilderFactoryAddr;
    HashBuilderOperator *hashBuilderOperator;
    if (hashJoinThreadArgs->isOriginal) {
        hashBuilderOperator = dynamic_cast<HashBuilderOperator *>(hashBuilderOperatorFactory->CreateOperator());
    }
    else {
        opt_module hashBuilderModule = (opt_module)(hashBuilderOperatorFactory->GetJitContext()->func);
        hashBuilderOperator = (HashBuilderOperator *)hashBuilderModule(hashBuilderOperatorFactory);
    }
    if (hashJoinThreadArgs->partitionIndex != -1) {
        hashBuilderOperator->AddInput(hashJoinThreadArgs->buildVecBatches[hashJoinThreadArgs->partitionIndex]);
    }
    else {
        for (int i = 0; i <hashJoinThreadArgs->buildVecBatchCount; ++i) {
            hashBuilderOperator->AddInput(hashJoinThreadArgs->buildVecBatches[i]);
        }
    }
    std::vector<VectorBatch *> buildOutputTables;
    hashBuilderOperator->GetOutput(buildOutputTables);
}

void testLookupJoin(struct HashJoinThreadArgs *hashJoinThreadArgs)
{
    LookupJoinOperatorFactory *lookupJoinOperatorFactory = (LookupJoinOperatorFactory *)hashJoinThreadArgs->lookupJoinFactoryAddr;
    LookupJoinOperator *lookupJoinOperator;
    if (hashJoinThreadArgs->isOriginal) {
        lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(lookupJoinOperatorFactory->CreateOperator());
    }
    else {
        opt_module lookupJoinModule = (opt_module)(lookupJoinOperatorFactory->GetJitContext()->func);
        lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(lookupJoinModule(lookupJoinOperatorFactory));
    }
    for (int loop = 0; loop < 1000; loop++) {
        for (int i = 0; i < hashJoinThreadArgs->probeVecBatchCount; ++i) {
            lookupJoinOperator->AddInput(hashJoinThreadArgs->probeVecBatches[i]);
            std::vector<VectorBatch *> probeOutputTables;
            lookupJoinOperator->GetOutput(probeOutputTables);
        }
    }
    delete lookupJoinOperator;
}

TEST(NativeOmniJoinTest, testHashBuilderOriginalMultiThreads)
{
    VectorBatch **buildVecBatches = constructHashBuilderTestData(VEC_BATCH_COUNT_10, COLUMN_COUNT_4);
    std::cout << "finish build hash builder data" << std::endl;

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        HashBuilderOperatorFactory *hashBuilderOperatorFactory = prepareHashBuilder(threadNum, true);
        struct HashJoinThreadArgs hashJoinThreadArgs;
        setHashJoinThreadArgs(&hashJoinThreadArgs, true, (int64_t)hashBuilderOperatorFactory, buildVecBatches,
                              VEC_BATCH_COUNT_10, 0, nullptr, 0);

        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
            std::thread t(testHashBuilder, &hashJoinThreadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto &th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << "testHashBuilderOriginalMultiThreads " << threadNum << " wall_elapsed time: " << wall_elapsed
                  << "s" << std::endl;
        std::cout << "testHashBuilderOriginalMultiThreads " << threadNum << " cpu_elapsed time: "
                  << cpu_elapsed / processor_count * t_ << "s" << std::endl;

        deleteJoinOperatorFactory(hashBuilderOperatorFactory, nullptr);
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
    }

    VectorHelper::FreeVecBatches(buildVecBatches, VEC_BATCH_COUNT_10);
}

TEST(NativeOmniJoinTest, testHashBuilderJITMultiThreads)
{
    VectorBatch **buildVecBatches = constructHashBuilderTestData(VEC_BATCH_COUNT_10, COLUMN_COUNT_4);
    std::cout << "finish build hash builder data" << std::endl;

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        HashBuilderOperatorFactory *hashBuilderOperatorFactory = prepareHashBuilder(threadNum, false);
        struct HashJoinThreadArgs hashJoinThreadArgs;
        setHashJoinThreadArgs(&hashJoinThreadArgs, false, (int64_t)hashBuilderOperatorFactory, buildVecBatches,
                              VEC_BATCH_COUNT_10, 0, nullptr, 0);

        std::vector<std::thread> vecOfThreads;
        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
            std::thread t(testHashBuilder, &hashJoinThreadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << "testHashBuilderJITMultiThreads " << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << "testHashBuilderJITMultiThreads " << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;

        deleteJoinOperatorFactory(hashBuilderOperatorFactory, nullptr);
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
    }
    VectorHelper::FreeVecBatches(buildVecBatches, VEC_BATCH_COUNT_10);
}


// this test data is used for testLookupJoin*MultiThreads.
// there is one row for a thread, and the thread i will handle the vector batch i
// the numbers[i] belongs to partition i
VectorBatch **constructBuilderTestData(int32_t *numbers, int32_t numberCount)
{
    VectorBatch **vectorBatches = new VectorBatch*[numberCount];
    for (int32_t vecBatchIdx = 0; vecBatchIdx < numberCount; vecBatchIdx++) {
        VectorBatch *vectorBatch = new VectorBatch(COLUMN_COUNT_4);
        for (int32_t colIdx = 0; colIdx < COLUMN_COUNT_4; colIdx++) {
            LongVector *column = new LongVector(nullptr, 1);
            column->SetValue(0, numbers[vecBatchIdx]);
            vectorBatch->SetVector(colIdx, column);
        }
        vectorBatches[vecBatchIdx] = vectorBatch;
    }
    return vectorBatches;
}

// this test data is used for testLookupJoin*MultiThreads.
// it will output one vector batches, each vector batches has all data
VectorBatch **constructProbeTestData(int32_t *numbers, int32_t numberCount)
{
    int32_t positionCount = PROBE_POSITION_COUNT;
    VectorBatch **vecBatches = new VectorBatch *[VEC_BATCH_COUNT_1];
    for (int32_t vecBatchIdx = 0; vecBatchIdx < VEC_BATCH_COUNT_1; vecBatchIdx++) {
        VectorBatch *vecBatch = new VectorBatch(COLUMN_COUNT_4);
        for (int32_t colIdx = 0; colIdx < COLUMN_COUNT_4; colIdx++) {
            LongVector *vector = new LongVector(nullptr, positionCount);
            for (int32_t posIdx = 0; posIdx < positionCount; posIdx++) {
                int64_t value = numbers[(posIdx % numberCount)];
                vector->SetValue(posIdx, value);
            }
            vecBatch->SetVector(colIdx, vector);
        }
        vecBatches[vecBatchIdx] = vecBatch;
    }

    return vecBatches;
}

TEST(NativeOmniJoinTest, testLookupJoinOriginalMultiThreads)
{
    int32_t numbers[3][16] = {
        {6},
        {6, 13, 7, 4, 1, 9, 3, 2},
        {8, 13, 75, 12, 27, 28, 3, 2, 6, 38, 36, 32, 1, 20, 50, 37}
        };
    int32_t threadNums[] = {1, 8, 16};
    int32_t groupCount = sizeof(threadNums) / sizeof(int32_t);
    VectorBatch ***buildVecBatches = new VectorBatch **[groupCount];
    for (int32_t i = 0; i < groupCount; i++) {
        buildVecBatches[i] = constructBuilderTestData(numbers[i], threadNums[i]);
    }
    VectorBatch ***probeVecBatches = new VectorBatch **[groupCount];
    for (int32_t i = 0; i < groupCount; i++) {
        probeVecBatches[i] = constructProbeTestData(numbers[i], threadNums[i]);
    }

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    for (int32_t i = 0; i < groupCount; ++i) {
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;
        int32_t threadNum = threadNums[i];

        HashBuilderOperatorFactory *hashBuilderOperatorFactory = prepareHashBuilder(threadNum, true);
        struct HashJoinThreadArgs hashBuilderThreadArgs;
        setHashJoinThreadArgs(&hashBuilderThreadArgs, true, (int64_t)hashBuilderOperatorFactory, buildVecBatches[i],
                threadNum, 0, nullptr, 0);
        std::vector<std::thread> vecOfThreads;
        for (int32_t i = 0; i < threadNum; ++i) {
            hashBuilderThreadArgs.partitionIndex = i;
            std::thread t(testHashBuilder, &hashBuilderThreadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        vecOfThreads.clear();

        LookupJoinOperatorFactory *lookupJoinOperatorFactory = prepareLookupJoin(hashBuilderOperatorFactory, true);
        struct HashJoinThreadArgs lookupJoinThreadArgs;
        setHashJoinThreadArgs(&lookupJoinThreadArgs, true, 0, nullptr,
                0, (int64_t)lookupJoinOperatorFactory, probeVecBatches[i], VEC_BATCH_COUNT_1);

        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
            std::thread t(testLookupJoin, &lookupJoinThreadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << "testLookupJoinOriginalMultiThreads " << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << "testLookupJoinOriginalMultiThreads " << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        deleteJoinOperatorFactory(hashBuilderOperatorFactory, lookupJoinOperatorFactory);
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
    }

    for (int32_t i = 0; i < groupCount; i++) {
        VectorHelper::FreeVecBatches(buildVecBatches[i], threadNums[i]);
        VectorHelper::FreeVecBatches(probeVecBatches[i], VEC_BATCH_COUNT_1);
    }
    delete[] buildVecBatches;
    delete[] probeVecBatches;
}

TEST(NativeOmniJoinTest, testLookupJoinJITMultiThreads)
{
    int32_t numbers[3][16] = {
            {6},
            {6, 13, 7, 4, 1, 9, 3, 2},
            {8, 13, 75, 12, 27, 28, 3, 2, 6, 38, 36, 32, 1, 20, 50, 37}
    };
    int32_t threadNums[] = {1, 8, 16};
    int32_t groupCount = sizeof(threadNums) / sizeof(int32_t);
    VectorBatch ***buildVecBatches = new VectorBatch **[groupCount];
    for (int32_t i = 0; i < groupCount; i++) {
        buildVecBatches[i] = constructBuilderTestData(numbers[i], threadNums[i]);
    }
    VectorBatch ***probeVecBatches = new VectorBatch **[groupCount];
    for (int32_t i = 0; i < groupCount; i++) {
        probeVecBatches[i] = constructProbeTestData(numbers[i], threadNums[i]);
    }

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    for (int32_t i = 0; i < groupCount; ++i) {
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;
        int32_t threadNum = threadNums[i];

        HashBuilderOperatorFactory *hashBuilderOperatorFactory = prepareHashBuilder(threadNum, false);
        struct HashJoinThreadArgs hashBuilderThreadArgs;
        setHashJoinThreadArgs(&hashBuilderThreadArgs, false, (int64_t)hashBuilderOperatorFactory, buildVecBatches[i],
                              threadNum, 0, nullptr, 0);
        std::vector<std::thread> vecOfThreads;
        for (int32_t i = 0; i < threadNum; ++i) {
            hashBuilderThreadArgs.partitionIndex = i;
            std::thread t(testHashBuilder, &hashBuilderThreadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        vecOfThreads.clear();

        LookupJoinOperatorFactory *lookupJoinOperatorFactory = prepareLookupJoin(hashBuilderOperatorFactory, false);
        struct HashJoinThreadArgs lookupJoinThreadArgs;
        setHashJoinThreadArgs(&lookupJoinThreadArgs, false, 0, nullptr,
                              0, (int64_t)lookupJoinOperatorFactory, probeVecBatches[i], VEC_BATCH_COUNT_1);

        Timer timer;
        timer.setStart();
        for (int32_t i = 0; i < threadNum; ++i) {
            std::thread t(testLookupJoin, &lookupJoinThreadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse();
        double cpu_elapsed = timer.getCpuElapse();
        std::cout << "testLookupJoinOriginalMultiThreads " << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << "testLookupJoinOriginalMultiThreads " << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        deleteJoinOperatorFactory(hashBuilderOperatorFactory, lookupJoinOperatorFactory);
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
    }

    for (int32_t i = 0; i < groupCount; i++) {
        VectorHelper::FreeVecBatches(buildVecBatches[i], threadNums[i]);
        VectorHelper::FreeVecBatches(probeVecBatches[i], VEC_BATCH_COUNT_1);
    }
    delete[] buildVecBatches;
    delete[] probeVecBatches;
}
