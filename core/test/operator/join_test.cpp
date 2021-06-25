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

// TEST(NativeOmniJoinTest, testHash)
// {
//     const int32_t DATA_SIZE = 10;
//     int64_t hash = 0;
//     int32_t partition = 0;

//     int64_t buildData[DATA_SIZE] = {1, 2, 1, 2, 3, 4 ,5 ,6 ,7 ,1};
//     for (int32_t i = 0; i < DATA_SIZE; i++) {
//         hash = HashUtil::hashValue(buildData[i]);
//         partition = HashUtil::getRawHashPartition(hash, 1);
//         std::cout << "BUILD hash(" << buildData[i] << ") = " << hash << ", partition=" << partition << std::endl;
//     }

//     int64_t probeData[DATA_SIZE] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
//     for (int32_t i = 0; i < DATA_SIZE; i++) {
//         hash = HashUtil::hashValue(probeData[i]);
//         partition = HashUtil::getRawHashPartition(hash, 1);
//         std::cout << "PROBE hash(" << probeData[i] << ") = " << hash << ", partition=" << partition << std::endl;
//     }
// }
using namespace omniruntime::op;

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
    auto createOperatorFunc = jit->specialize();
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);
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
    int32_t *hashColTypes = new int32_t[probeHashColsCount];
    for (int32_t i = 0; i < probeHashColsCount; i++) {
        hashColTypes[i] = probeTypes[probeHashCols[i]];
    }

    using namespace omniruntime::jit;
    ParamValue p_hashColTypes = ParamValue(hashColTypes, probeHashColsCount);
    ParamValue p_hashColCount = ParamValue(&probeHashColsCount);

    auto *positionEqualsRowIgnoreNullsSp = new Specialization();
    positionEqualsRowIgnoreNullsSp->addSpecializedParam(5, &p_hashColTypes);
    positionEqualsRowIgnoreNullsSp->addSpecializedParam(6, &p_hashColCount);

    std::map<std::string, Specialization> hashStrategySps = {
        {OMNIJIT_HASH_STRATEGY_POSITION_EQUALS_ROW_IGNORE_NULLS, *positionEqualsRowIgnoreNullsSp}
    };

    auto *lookupJoinContext = new omniruntime::jit::Context("lookup_join", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>(), true);
    auto *joinHashTableContext = new omniruntime::jit::Context("join_hash_table", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *pagesIndexContext = new omniruntime::jit::Context("pages_index", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *pagesHashStrategyContext = new omniruntime::jit::Context("pages_hash_strategy", hashStrategySps, std::vector<std::string>(), std::vector<std::string>());
    auto *hashUtilContext = new omniruntime::jit::Context("hash_util", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());
    auto *memoryPoolContext = new omniruntime::jit::Context("memory_pool", std::map<std::string, Specialization>(), std::vector<std::string>(), std::vector<std::string>());

    Jit *jit = new Jit(std::vector<omniruntime::jit::Context>{*lookupJoinContext, *joinHashTableContext, *pagesIndexContext, *pagesHashStrategyContext, *hashUtilContext, *memoryPoolContext});
    auto createOperatorFunc = jit->specialize();
    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(createOperatorFunc);

    return jitContext;
}

TEST(NativeOmniJoinTest, testOneHashBuilderOneColumn)
{
    // construct input data
    const int32_t DATA_SIZE = 10;
    int64_t buildData0[DATA_SIZE] = {1, 2, 1, 2, 3, 4, 5, 6, 7, 1};
    int64_t buildData1[DATA_SIZE] = {79, 79, 70, 70, 70, 70, 70, 70, 70, 70};
    LongVector *buildColumn0 = new LongVector(nullptr, DATA_SIZE);
    buildColumn0->setValues(0, buildData0, DATA_SIZE);
    LongVector *buildColumn1 = new LongVector(nullptr, DATA_SIZE);
    buildColumn1->setValues(0, buildData1, DATA_SIZE);

    VectorBatch *vecBatch = new VectorBatch(2);
    vecBatch->setVector(0, buildColumn0);
    vecBatch->setVector(1, buildColumn1);

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
    hashBuilderFactory->setJitContext(hashBuilderJitContext);
    HashBuilderOperator *hashBuilderOperator = (HashBuilderOperator *)createTestOperator(hashBuilderFactory);
    hashBuilderOperator->addInput(vecBatch);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator->getOutput(hashBuildOutput);

    int64_t probeData0[DATA_SIZE] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
    int64_t probeData1[DATA_SIZE] = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
    LongVector *probeColumn0 = new LongVector(nullptr, DATA_SIZE);
    probeColumn0->setValues(0, probeData0, DATA_SIZE);
    LongVector *probeColumn1 = new LongVector(nullptr, DATA_SIZE);
    probeColumn1->setValues(0, probeData1, DATA_SIZE);

    VectorBatch *probeVecBatch = new VectorBatch(2);
    probeVecBatch->setVector(0, probeColumn0);
    probeVecBatch->setVector(1, probeColumn1);

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
    lookupJoinFactory->setJitContext(lookupJoinJitContext);
    LookupJoinOperator *lookupJoinOperator = (LookupJoinOperator *)createTestOperator(lookupJoinFactory);
    lookupJoinOperator->addInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->getOutput(output);
    EXPECT_EQ(output.size(), 1);
    //output[0]->printTable();

    const int32_t EXPECTED_DATA_SIZE = 18;
    EXPECT_EQ(output[0]->getRowCount(), EXPECTED_DATA_SIZE);

    int64_t expectedData0[EXPECTED_DATA_SIZE] = {78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 82, 82, 82, 82, 82, 65};
    int64_t expectedData1[EXPECTED_DATA_SIZE] = {70, 70, 79, 70, 79, 70, 70, 70, 70, 70, 70, 79, 70, 70, 79, 70, 79, 70};
    LongVector *expectedCol0 = new LongVector(nullptr, EXPECTED_DATA_SIZE);
    expectedCol0->setValues(0, expectedData0, EXPECTED_DATA_SIZE);
    LongVector *expectedCol1 = new LongVector(nullptr, EXPECTED_DATA_SIZE);
    expectedCol1->setValues(0, expectedData1, EXPECTED_DATA_SIZE);
    VectorBatch *expectVecBatch = new VectorBatch(2);
    expectVecBatch->setVector(0, expectedCol0);
    expectVecBatch->setVector(1, expectedCol1);

    EXPECT_TRUE(vecBatchMatch(output[0], expectVecBatch));

    VectorHelper::freeVecBatch(vecBatch);
    VectorHelper::freeVecBatch(probeVecBatch);
    VectorHelper::freeVecBatch(expectVecBatch);
}

TEST(NativeOmniJoinTest, testTwoHashBuilderOneColumn)
{
    const int32_t DATA_SIZE = 10;
    const int32_t DATA_SIZE1 = 6;
    const int32_t DATA_SIZE2 = 4;

    int64_t buildData00[DATA_SIZE1] = {1, 1, 3, 6, 7, 1};
    int64_t buildData01[DATA_SIZE1] = {79, 70, 70, 70, 70, 70};
    LongVector *buildColumn00 = new LongVector(nullptr, DATA_SIZE1);
    buildColumn00->setValues(0, buildData00, DATA_SIZE1);
    LongVector *buildColumn01 = new LongVector(nullptr, DATA_SIZE1);
    buildColumn01->setValues(0, buildData01, DATA_SIZE1);
    VectorBatch *vecBatch0 = new VectorBatch(2);
    vecBatch0->setVector(0, buildColumn00);
    vecBatch0->setVector(1, buildColumn01);

    int64_t buildData10[DATA_SIZE2] = {2, 2, 4, 5};
    int64_t buildData11[DATA_SIZE2] = {79, 70, 70, 70};
    LongVector *buildColumn10 = new LongVector(nullptr, DATA_SIZE2);
    buildColumn10->setValues(0, buildData10, DATA_SIZE2);
    LongVector *buildColumn11 = new LongVector(nullptr, DATA_SIZE2);
    buildColumn11->setValues(0, buildData11, DATA_SIZE2);
    VectorBatch *vecBatch1 = new VectorBatch(2);
    vecBatch1->setVector(0, buildColumn10);
    vecBatch1->setVector(1, buildColumn11);

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
    hashBuilderFactory->setJitContext(hashBuilderJitContext);

    HashBuilderOperator *hashBuilderOperator0 = (HashBuilderOperator *)createTestOperator(hashBuilderFactory);
    hashBuilderOperator0->addInput(vecBatch0);
    std::vector<VectorBatch *> hashBuildOutput;
    hashBuilderOperator0->getOutput(hashBuildOutput);

    HashBuilderOperator *hashBuilderOperator1 = (HashBuilderOperator *)createTestOperator(hashBuilderFactory);
    hashBuilderOperator1->addInput(vecBatch1);
    hashBuilderOperator1->getOutput(hashBuildOutput);

    int64_t probeData0[DATA_SIZE] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
    int64_t probeData1[DATA_SIZE] = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
    LongVector *probeColumn0 = new LongVector(nullptr, DATA_SIZE);
    probeColumn0->setValues(0, probeData0, DATA_SIZE);
    LongVector *probeColumn1 = new LongVector(nullptr, DATA_SIZE);
    probeColumn1->setValues(0, probeData1, DATA_SIZE);
    VectorBatch *probeVecBatch = new VectorBatch(2);
    probeVecBatch->setVector(0, probeColumn0);
    probeVecBatch->setVector(1, probeColumn1);

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
    lookupJoinFactory->setJitContext(lookupJoinJitContext);

    LookupJoinOperator *lookupJoinOperator = (LookupJoinOperator *)createTestOperator(lookupJoinFactory);
    lookupJoinOperator->addInput(probeVecBatch);
    std::vector<VectorBatch *> output;
    lookupJoinOperator->getOutput(output);
    EXPECT_EQ(output.size(), 1);
    //output[0]->printTable();

    const int32_t EXPECTED_DATA_SIZE = 18;
    EXPECT_EQ(output[0]->getRowCount(), EXPECTED_DATA_SIZE);

    VectorHelper::freeVecBatch(vecBatch0);
    VectorHelper::freeVecBatch(vecBatch1);
    VectorHelper::freeVecBatch(probeVecBatch);
    VectorHelper::freeVecBatches(output);
}

const int32_t BUILD_VEC_BATCH_COUNT = 10;
const int32_t PROBE_VEC_BATCH_COUNT = 1;
const int32_t DISTINCT_VALUE_COUNT = 4;
const int32_t REPEAT_COUNT = 25000;
const int32_t COLUMN_COUNT_4 = 4;
const int32_t TIME_TO_SLEEP = 100;

struct HashJoinThreadArgs
{
    bool isOriginal;
    int64_t hashBuilderFactoryAddr;
    VectorBatch **buildVecBatches;
    int32_t *buildRowCounts;
    int32_t buildVecBatchCount;
    int64_t lookupJoinFactoryAddr;
    VectorBatch **probeVecBatches;
    int32_t *probeRowCounts;
    int32_t probeVecBatchCount;
};

void buildHashJoinTestData(VectorBatch **vectorBatches, int32_t tableCount, int32_t columnCount)
{
    int32_t positionCount = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int64_t *data;
    int32_t size = positionCount * sizeof(int64_t);
    int32_t idx;

    for (int32_t i = 0; i < tableCount; i++) {
        VectorBatch *table = new VectorBatch(columnCount);
        for (int32_t colIdx = 0; colIdx < columnCount; colIdx++) {
            LongVector *column = new LongVector(nullptr, positionCount);
            idx = 0;
            for (int32_t j = 0; j < DISTINCT_VALUE_COUNT; j++) {
                for (int32_t k = 0; k < REPEAT_COUNT; k++) {
                    column->setValue(idx, j);
                    idx++;
                }
            }
            table->setVector(colIdx, column);
        }
        vectorBatches[i] = table;
    }
}

VectorBatch **buildHashBuilderTestData(int32_t *numbers, int32_t numberCount)
{
    VectorBatch **vectorBatches = new VectorBatch*[numberCount];
    for (int32_t vecBatchIdx = 0; vecBatchIdx < numberCount; vecBatchIdx++) {
        VectorBatch *table = new VectorBatch(COLUMN_COUNT_4);
        for (int32_t colIdx = 0; colIdx < COLUMN_COUNT_4; colIdx++) {
            LongVector *column = new LongVector(nullptr, 1);
            column->setValue(0, numbers[vecBatchIdx]);
            table->setVector(colIdx, column);
        }
        vectorBatches[vecBatchIdx] = table;
    }
    return vectorBatches;
}

VectorBatch **buildLookupJoinTestData(int32_t *numbers, int32_t numberCount)
{
    VectorBatch **vectorBatches = new VectorBatch*[BUILD_VEC_BATCH_COUNT];
    int32_t positionCount = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int64_t **datas = (int64_t **)malloc(COLUMN_COUNT_4 * sizeof(int64_t));

    for (int32_t vecBatchIdx = 0; vecBatchIdx < BUILD_VEC_BATCH_COUNT; vecBatchIdx++) {
        VectorBatch *vecBatch = new VectorBatch(COLUMN_COUNT_4);
        for (int32_t colIdx = 0; colIdx < COLUMN_COUNT_4; colIdx++) {
            datas[colIdx] = new int64_t[positionCount];
        }

        for (int32_t posIdx = 0; posIdx < positionCount; posIdx++) {
            int64_t value = numbers[(rand() % numberCount)];
            for (int32_t colIdx = 0; colIdx < COLUMN_COUNT_4; colIdx++) {
                datas[colIdx][posIdx] = value;
            }
        }

        for (int32_t colIdx = 0; colIdx < COLUMN_COUNT_4; colIdx++) {
            LongVector *column = new LongVector(nullptr, positionCount);
            column->setValues(0, datas[colIdx], positionCount);
            vecBatch->setVector(colIdx, column);
        }
        vectorBatches[vecBatchIdx] = vecBatch;
    }

    return vectorBatches;
}

int32_t *getBuildRowCounts()
{
    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int32_t *probeRowCounts = new int32_t[BUILD_VEC_BATCH_COUNT];
    for (int32_t i = 0; i < BUILD_VEC_BATCH_COUNT; i++) {
        probeRowCounts[i] = rowNum;
    }
    return probeRowCounts;
}

HashBuilderOperatorFactory *prepareHashBuilder(int32_t operatorCount)
{
    int32_t buildTypes[] = {2, 2, 2, 2};
    int32_t buildTypesCount = 4;
    int32_t buildOutputCols[] = {0, 1};
    int32_t buildOutputColsCount = 2;
    int32_t buildHashCols[] = {2, 3};
    int32_t buildHashColsCount = 2;
    HashBuilderOperatorFactory *hashBuilderOperatorFactory = HashBuilderOperatorFactory::createHashBuilderOperatorFactory(
            buildTypes, buildTypesCount, buildOutputCols, buildOutputColsCount, buildHashCols, buildHashColsCount, operatorCount);
    JitContext *hashBuilderJitContext = createTestHashBuilderJitContext(buildTypes, buildTypesCount, buildOutputCols, buildOutputColsCount, buildHashCols, buildHashColsCount, operatorCount);
    hashBuilderOperatorFactory->setJitContext(hashBuilderJitContext);

    return hashBuilderOperatorFactory;
}

LookupJoinOperatorFactory *prepareLookupJoin(HashBuilderOperatorFactory *hashBuilderOperatorFactory)
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
    JitContext *lookupJoinJitContext = createTestLookupJoinJitContext(
            probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
            buildOutputCols, buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
    lookupJoinOperatorFactory->setJitContext(lookupJoinJitContext);
    return lookupJoinOperatorFactory;
}

void setHashJoinThreadArgs(
        struct HashJoinThreadArgs *hashJoinThreadArgs,
        bool isOriginal,
        int64_t hashBuilderFactoryAddr,
        VectorBatch **buildVecBatches,
        int32_t *buildRowCounts,
        int32_t buildVecBatchCount,
        int64_t lookupJoinFactoryAddr,
        VectorBatch **probeVecBatches,
        int32_t *probeRowCounts,
        int32_t probeVecBatchCount)
{
    hashJoinThreadArgs->isOriginal = isOriginal;
    hashJoinThreadArgs->hashBuilderFactoryAddr = hashBuilderFactoryAddr;
    hashJoinThreadArgs->buildVecBatches = buildVecBatches;
    hashJoinThreadArgs->buildRowCounts = buildRowCounts;
    hashJoinThreadArgs->buildVecBatchCount = buildVecBatchCount;
    hashJoinThreadArgs->lookupJoinFactoryAddr = lookupJoinFactoryAddr;
    hashJoinThreadArgs->probeVecBatches = probeVecBatches;
    hashJoinThreadArgs->probeRowCounts = probeRowCounts;
    hashJoinThreadArgs->probeVecBatchCount = probeVecBatchCount;
}

void testHashJoin(struct HashJoinThreadArgs *hashJoinThreadArgs)
{
    HashBuilderOperatorFactory *hashBuilderOperatorFactory = (HashBuilderOperatorFactory *)hashJoinThreadArgs->hashBuilderFactoryAddr;
    LookupJoinOperatorFactory *lookupJoinOperatorFactory = (LookupJoinOperatorFactory *)hashJoinThreadArgs->lookupJoinFactoryAddr;

    HashBuilderOperator *hashBuilderOperator;
    if (hashJoinThreadArgs->isOriginal) {
        hashBuilderOperator = dynamic_cast<HashBuilderOperator *>(hashBuilderOperatorFactory->createOperator());
    }
    else {
        opt_module hashBuilderModule = (opt_module)(hashBuilderOperatorFactory->getJitContext()->func);
        hashBuilderOperator = (HashBuilderOperator *)hashBuilderModule(hashBuilderOperatorFactory);
    }
    for (int i = 0; i < BUILD_VEC_BATCH_COUNT; ++i) {
        hashBuilderOperator->addInput(hashJoinThreadArgs->buildVecBatches[i]);
    }
    std::vector<VectorBatch *> buildOutputTables;
    hashBuilderOperator->getOutput(buildOutputTables);

    LookupJoinOperator *lookupJoinOperator;
    if (hashJoinThreadArgs->isOriginal) {
        lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(lookupJoinOperatorFactory->createOperator());
    }
    else {
        opt_module lookupJoinModule = (opt_module)(lookupJoinOperatorFactory->getJitContext()->func);
        lookupJoinOperator = (LookupJoinOperator *)lookupJoinModule(lookupJoinOperatorFactory);
    }
    for (int i = 0; i < hashJoinThreadArgs->probeVecBatchCount; ++i) {
        lookupJoinOperator->addInput(hashJoinThreadArgs->probeVecBatches[i]);
    }
    std::vector<VectorBatch *> probeOutputTables;
    lookupJoinOperator->getOutput(probeOutputTables);

    delete hashBuilderOperator;
    delete lookupJoinOperator;

}

void testHashBuilder(struct HashJoinThreadArgs *hashJoinThreadArgs)
{
    HashBuilderOperatorFactory *hashBuilderOperatorFactory = (HashBuilderOperatorFactory *)hashJoinThreadArgs->hashBuilderFactoryAddr;
    HashBuilderOperator *hashBuilderOperator;
    if (hashJoinThreadArgs->isOriginal) {
        hashBuilderOperator = dynamic_cast<HashBuilderOperator *>(hashBuilderOperatorFactory->createOperator());
    }
    else {
        opt_module hashBuilderModule = (opt_module)(hashBuilderOperatorFactory->getJitContext()->func);
        hashBuilderOperator = (HashBuilderOperator *)hashBuilderModule(hashBuilderOperatorFactory);
    }
    for (int i = 0; i <hashJoinThreadArgs->buildVecBatchCount; ++i) {
        hashBuilderOperator->addInput(hashJoinThreadArgs->buildVecBatches[i]);
    }
    std::vector<VectorBatch *> buildOutputTables;
    hashBuilderOperator->getOutput(buildOutputTables);
    delete hashBuilderOperator;
}

void testLookupJoin(struct HashJoinThreadArgs *hashJoinThreadArgs)
{
    LookupJoinOperatorFactory *lookupJoinOperatorFactory = (LookupJoinOperatorFactory *)hashJoinThreadArgs->lookupJoinFactoryAddr;
    LookupJoinOperator *lookupJoinOperator;
    if (hashJoinThreadArgs->isOriginal) {
        lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(lookupJoinOperatorFactory->createOperator());
    }
    else {
        opt_module lookupJoinModule = (opt_module)(lookupJoinOperatorFactory->getJitContext()->func);
        lookupJoinOperator = (LookupJoinOperator *)lookupJoinModule(lookupJoinOperatorFactory);
    }
    for (int i = 0; i < hashJoinThreadArgs->probeVecBatchCount; ++i) {
        lookupJoinOperator->addInput(hashJoinThreadArgs->probeVecBatches[i]);
    }
    std::vector<VectorBatch *> probeOutputTables;
    lookupJoinOperator->getOutput(probeOutputTables);
    delete lookupJoinOperator;
}

TEST(NativeOmniJoinTest, testHashBuilderOriginalMultiThreads)
{
    VectorBatch **buildVecBatches = new VectorBatch*[BUILD_VEC_BATCH_COUNT];
    buildHashJoinTestData(buildVecBatches, BUILD_VEC_BATCH_COUNT, COLUMN_COUNT_4);
    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int32_t buildRowCounts[BUILD_VEC_BATCH_COUNT];
    for (int32_t i = 0; i < BUILD_VEC_BATCH_COUNT; i++) {
        buildRowCounts[i] = rowNum;
    }
    std::cout << "finish build hash builder data" << std::endl;

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        HashBuilderOperatorFactory *hashBuilderOperatorFactory = prepareHashBuilder(threadNum);
        struct HashJoinThreadArgs hashJoinThreadArgs;
        setHashJoinThreadArgs(&hashJoinThreadArgs, true, (int64_t)hashBuilderOperatorFactory, buildVecBatches, buildRowCounts,
                BUILD_VEC_BATCH_COUNT, 0, nullptr, nullptr, 0);

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
        delete hashBuilderOperatorFactory;
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
    }

    VectorHelper::freeVecBatches(buildVecBatches, BUILD_VEC_BATCH_COUNT);
}

TEST(NativeOmniJoinTest, testHashBuilderJITMultiThreads)
{
    VectorBatch **buildVecBatches = (VectorBatch **)malloc(BUILD_VEC_BATCH_COUNT * sizeof(VectorBatch *));
    buildHashJoinTestData(buildVecBatches, BUILD_VEC_BATCH_COUNT, COLUMN_COUNT_4);
    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int32_t buildRowCounts[BUILD_VEC_BATCH_COUNT];
    for (int32_t i = 0; i < BUILD_VEC_BATCH_COUNT; i++) {
        buildRowCounts[i] = rowNum;
    }
    std::cout << "finish build hash builder data" << std::endl;

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;

        int32_t threadNum = threadNums[i];
        HashBuilderOperatorFactory *hashBuilderOperatorFactory = prepareHashBuilder(threadNum);
        struct HashJoinThreadArgs hashJoinThreadArgs;
        setHashJoinThreadArgs(&hashJoinThreadArgs, false, (int64_t)hashBuilderOperatorFactory, buildVecBatches, buildRowCounts,
                BUILD_VEC_BATCH_COUNT, 0, nullptr, nullptr, 0);

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
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
    }
    VectorHelper::freeVecBatches(buildVecBatches, BUILD_VEC_BATCH_COUNT);
}

TEST(NativeOmniJoinTest, testLookupJoinOriginalMultiThreads)
{
    int32_t numbers[3][16] = {
        {6},
        {6, 13, 7, 4, 1, 9, 3, 2},
        {8, 13, 75, 12, 27, 28, 3, 2, 6, 38, 36, 32, 1, 20, 50, 37}
        };

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;
        int32_t threadNum = threadNums[i];

        VectorBatch **buildVecBatches = buildHashBuilderTestData(numbers[i], threadNum);
        HashBuilderOperatorFactory *hashBuilderOperatorFactory = prepareHashBuilder(threadNum);
        struct HashJoinThreadArgs hashBuilderThreadArgs;
        setHashJoinThreadArgs(&hashBuilderThreadArgs, true, (int64_t)hashBuilderOperatorFactory, buildVecBatches, &threadNum,
                1, 0, nullptr, nullptr, 0);
        std::vector<std::thread> vecOfThreads;
        for (int32_t i = 0; i < threadNum; ++i) {
            std::thread t(testHashBuilder, &hashBuilderThreadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        vecOfThreads.clear();

        VectorBatch **probeVecBatches = buildLookupJoinTestData(numbers[i], threadNum);
        int32_t *probeRowCounts = getBuildRowCounts();

        LookupJoinOperatorFactory *lookupJoinOperatorFactory = prepareLookupJoin(hashBuilderOperatorFactory);
        struct HashJoinThreadArgs lookupJoinThreadArgs;
        setHashJoinThreadArgs(&lookupJoinThreadArgs, true, 0, nullptr, nullptr,
                0, (int64_t)lookupJoinOperatorFactory, probeVecBatches, probeRowCounts, BUILD_VEC_BATCH_COUNT);

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
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
        VectorHelper::freeVecBatches(probeVecBatches, BUILD_VEC_BATCH_COUNT);
        VectorHelper::freeVecBatches(buildVecBatches, threadNum);
    }
}

TEST(NativeOmniJoinTest, testLookupJoinJITMultiThreads)
{
    int32_t numbers[3][16] = {
        {6},
        {6, 13, 7, 4, 1, 9, 3, 2},
        {8, 13, 75, 12, 27, 28, 3, 2, 6, 38, 36, 32, 1, 20, 50, 37}
        };

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    int threadNums[] = {1, 8, 16};
    for (int32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto t_ = threadNums[i] < processor_count ? processor_count / threadNums[i] : 1;
        int32_t threadNum = threadNums[i];

        VectorBatch **buildVecBatches = buildHashBuilderTestData(numbers[i], threadNum);
        HashBuilderOperatorFactory *hashBuilderOperatorFactory = prepareHashBuilder(threadNum);
        struct HashJoinThreadArgs hashBuilderThreadArgs;
        setHashJoinThreadArgs(&hashBuilderThreadArgs, false, (int64_t)hashBuilderOperatorFactory, buildVecBatches, &threadNum,
                1, 0, nullptr, nullptr, 0);
        std::vector<std::thread> vecOfThreads;
        for (int32_t i = 0; i < threadNum; ++i) {
            std::thread t(testHashBuilder, &hashBuilderThreadArgs);
            vecOfThreads.push_back(std::move(t));
        }
        for (auto& th : vecOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        vecOfThreads.clear();

        VectorBatch **probeVecBatches = buildLookupJoinTestData(numbers[i], threadNum);
        int32_t *probeRowCounts = getBuildRowCounts();

        LookupJoinOperatorFactory *lookupJoinOperatorFactory = prepareLookupJoin(hashBuilderOperatorFactory);
        struct HashJoinThreadArgs lookupJoinThreadArgs;
        setHashJoinThreadArgs(&lookupJoinThreadArgs, false, 0, nullptr, nullptr,
                0, (int64_t)lookupJoinOperatorFactory, probeVecBatches, probeRowCounts, BUILD_VEC_BATCH_COUNT);

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
        std::cout << "testLookupJoinJITMultiThreads " << threadNum << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << "testLookupJoinJITMultiThreads " << threadNum << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(TIME_TO_SLEEP));
        VectorHelper::freeVecBatches(probeVecBatches, BUILD_VEC_BATCH_COUNT);
        VectorHelper::freeVecBatches(buildVecBatches, threadNum);
    }
}
