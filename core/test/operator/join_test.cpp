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
    Column *buildColumn0 = new Column(buildData0, INT64, DATA_SIZE);
    Column *buildColumn1 = new Column(buildData1, INT64, DATA_SIZE);
    Table **buildTables = (Table **)malloc(1 * sizeof(Table *));
    buildTables[0] = new Table(DATA_SIZE, 2);
    buildTables[0]->setColumn(buildColumn0, INT64);
    buildTables[0]->setColumn(buildColumn1, INT64);
    int32_t rowCounts[1] = {DATA_SIZE};

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
    hashBuilderOperator->addInput(buildTables, rowCounts, 1);

    int64_t probeData0[DATA_SIZE] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
    int64_t probeData1[DATA_SIZE] = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
    Column *probeColumn0 = new Column(probeData0, INT64, DATA_SIZE);
    Column *probeColumn1 = new Column(probeData1, INT64, DATA_SIZE);
    Table *probeTable = new Table(DATA_SIZE, 2);
    probeTable->setColumn(probeColumn0, INT64);
    probeTable->setColumn(probeColumn1, INT64);

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
    lookupJoinOperator->addInput(probeTable, DATA_SIZE);
    std::vector<Table *> output;
    lookupJoinOperator->getOutput(output);
    EXPECT_EQ(output.size(), 1);
    //output[0]->printTable();

    const int32_t EXPECTED_DATA_SIZE = 18;
    EXPECT_EQ(output[0]->getPositionCount(), EXPECTED_DATA_SIZE);

    int64_t expectedData0[EXPECTED_DATA_SIZE] = {78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 78, 82, 82, 82, 82, 82, 65};
    int64_t expectedData1[EXPECTED_DATA_SIZE] = {70, 70, 79, 70, 79, 70, 70, 70, 70, 70, 70, 79, 70, 70, 79, 70, 79, 70};
    Column *expectedCol0 = new Column(expectedData0, INT64, EXPECTED_DATA_SIZE);
    Column *expectedCol1 = new Column(expectedData1, INT64, EXPECTED_DATA_SIZE);
    Table *expectTable = new Table(EXPECTED_DATA_SIZE, 2);
    expectTable->setColumn(expectedCol0, INT64);
    expectTable->setColumn(expectedCol1, INT64);

    EXPECT_TRUE(tableMatch(output[0], expectTable));
}

TEST(NativeOmniJoinTest, testTwoHashBuilderOneColumn)
{
    const int32_t DATA_SIZE = 10;
    const int32_t DATA_SIZE1 = 6;
    const int32_t DATA_SIZE2 = 4;

    int64_t buildData00[DATA_SIZE1] = {1, 1, 3, 6, 7, 1};
    int64_t buildData01[DATA_SIZE1] = {79, 70, 70, 70, 70, 70};
    Column *buildColumn00 = new Column(buildData00, INT64, DATA_SIZE1);
    Column *buildColumn01 = new Column(buildData01, INT64, DATA_SIZE1);
    Table **buildTables0 = (Table **)malloc(1 * sizeof(Table *));
    buildTables0[0] = new Table(DATA_SIZE1, 2);
    buildTables0[0]->setColumn(buildColumn00, INT64);
    buildTables0[0]->setColumn(buildColumn01, INT64);
    int32_t rowCounts0[1] = {DATA_SIZE1};

    int64_t buildData10[DATA_SIZE2] = {2, 2, 4, 5};
    int64_t buildData11[DATA_SIZE2] = {79, 70, 70, 70};
    Column *buildColumn10 = new Column(buildData10, INT64, DATA_SIZE2);
    Column *buildColumn11 = new Column(buildData11, INT64, DATA_SIZE2);
    Table **buildTables1 = (Table **)malloc(1 * sizeof(Table *));
    buildTables1[0] = new Table(DATA_SIZE2, 2);
    buildTables1[0]->setColumn(buildColumn10, INT64);
    buildTables1[0]->setColumn(buildColumn11, INT64);
    int32_t rowCounts1[1] = {DATA_SIZE2};

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
    hashBuilderOperator0->addInput(buildTables0, rowCounts0, 1);
    HashBuilderOperator *hashBuilderOperator1 = (HashBuilderOperator *)createTestOperator(hashBuilderFactory);
    hashBuilderOperator1->addInput(buildTables1, rowCounts1, 1);

    int64_t probeData0[DATA_SIZE] = {1, 2, 3, 4, 5, 6, 1, 1, 2, 3};
    int64_t probeData1[DATA_SIZE] = {78, 78, 78, 78, 78, 78, 78, 82, 82, 65};
    Column *probeColumn0 = new Column(probeData0, INT64, DATA_SIZE);
    Column *probeColumn1 = new Column(probeData1, INT64, DATA_SIZE);
    Table *probeTable = new Table(DATA_SIZE, 2);
    probeTable->setColumn(probeColumn0, INT64);
    probeTable->setColumn(probeColumn1, INT64);

    int32_t probeTypes[2] = {2, 2};
    int32_t probeTypesCount = 2;
    int32_t probeOutputCols[1]= {1};
    int32_t probeOutputColsCount = 1;
    int32_t probeHashCols[1] = {0};
    int32_t probeHashColsCount = 1;
    int32_t buildOutputTypes[1] = {2};
    int64_t hashBuilderFactoryAddr = (int64_t)hashBuilderFactory;
    LookupJoinOperatorFactory *lookupJoinFactory = LookupJoinOperatorFactory::createLookupJoinOperatorFactory(
            probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
            buildOutputCols, buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
    JitContext *lookupJoinJitContext = createTestLookupJoinJitContext(
            probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount,
            buildOutputCols, buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
    lookupJoinFactory->setJitContext(lookupJoinJitContext);

    LookupJoinOperator *lookupJoinOperator = (LookupJoinOperator *) createTestOperator(lookupJoinFactory);
    lookupJoinOperator->addInput(probeTable, DATA_SIZE);
    std::vector<Table *> output;
    lookupJoinOperator->getOutput(output);
    EXPECT_EQ(output.size(), 1);
    //output[0]->printTable();

    const int32_t EXPECTED_DATA_SIZE = 18;
    EXPECT_EQ(output[0]->getPositionCount(), EXPECTED_DATA_SIZE);
}

const int32_t BUILD_TABLE_COUNT = 10;
const int32_t PROBE_TABLE_COUNT = 1;
const int32_t DISTINCT_VALUE_COUNT = 4;
const int32_t REPEAT_COUNT = 25000;
const int32_t COLUMN_COUNT_4 = 4;
const int32_t TIME_TO_SLEEP = 100;

struct HashJoinThreadArgs
{
    bool isOriginal;
    int64_t hashBuilderFactoryAddr;
    Table **buildTables;
    int32_t *buildRowCounts;
    int32_t buildTableCount;
    int64_t lookupJoinFactoryAddr;
    Table **probeTables;
    int32_t *probeRowCounts;
    int32_t probeTableCount;
};

void buildHashJoinTestData(Table **tables, int32_t tableCount, int32_t columnCount)
{
    int32_t positionCount = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int64_t *data;
    int32_t size = positionCount * sizeof(int64_t);
    int32_t idx;

    for (int32_t i = 0; i < tableCount; i++) {
        Table *table = new Table(positionCount, columnCount);
        for (int32_t colIdx = 0; colIdx < columnCount; colIdx++) {
            data = (int64_t *)malloc(size);
            idx = 0;
            for (int32_t j = 0; j < DISTINCT_VALUE_COUNT; j++) {
                for (int32_t k = 0; k < REPEAT_COUNT; k++) {
                    data[idx] = j;
                    idx++;
                }
            }
            Column *column = new Column(data, INT64, positionCount);
            table->setColumn(column, INT64);
        }
        tables[i] = table;
    }
}

Table **buildHashBuilderTestData(int32_t *numbers, int32_t numberCount)
{
    Table **tables = (Table **)malloc(numberCount * sizeof(Table *));
    for (int32_t tableIdx = 0; tableIdx < numberCount; tableIdx++) {
        Table *table = new Table(1, COLUMN_COUNT_4);
        for (int32_t colIdx = 0; colIdx < COLUMN_COUNT_4; colIdx++) {
            int64_t *data = (int64_t *)malloc(1 * sizeof(int64_t));
            data[0] = numbers[tableIdx];
            Column *column = new Column(data, INT64, 1);
            table->setColumn(column, INT64);
        }
        tables[tableIdx] = table;
    }
    return tables;
}

Table **buildLookupJoinTestData(int32_t *numbers, int32_t numberCount)
{
    Table **tables = (Table **)malloc(BUILD_TABLE_COUNT * sizeof(Table *));
    int32_t positionCount = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int64_t **datas = (int64_t **)malloc(COLUMN_COUNT_4 * sizeof(int64_t));

    for (int32_t tableIdx = 0; tableIdx < BUILD_TABLE_COUNT; tableIdx++) {
        Table *table = new Table(positionCount, COLUMN_COUNT_4);
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
            Column *column = new Column(datas[colIdx], INT64, positionCount);
            table->setColumn(column, INT64);
        }
        tables[tableIdx] = table;
    }

    return tables;
}

int32_t *getBuildRowCounts()
{
    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int32_t *probeRowCounts = new int32_t[BUILD_TABLE_COUNT];
    for (int32_t i = 0; i < BUILD_TABLE_COUNT; i++) {
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
        Table **buildTables,
        int32_t *buildRowCounts,
        int32_t buildTableCount,
        int64_t lookupJoinFactoryAddr,
        Table **probeTables,
        int32_t *probeRowCounts,
        int32_t probeTableCount)
{
    hashJoinThreadArgs->isOriginal = isOriginal;
    hashJoinThreadArgs->hashBuilderFactoryAddr = hashBuilderFactoryAddr;
    hashJoinThreadArgs->buildTables = buildTables;
    hashJoinThreadArgs->buildRowCounts = buildRowCounts;
    hashJoinThreadArgs->buildTableCount = buildTableCount;
    hashJoinThreadArgs->lookupJoinFactoryAddr = lookupJoinFactoryAddr;
    hashJoinThreadArgs->probeTables = probeTables;
    hashJoinThreadArgs->probeRowCounts = probeRowCounts;
    hashJoinThreadArgs->probeTableCount = probeTableCount;
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
    hashBuilderOperator->addInput(hashJoinThreadArgs->buildTables, hashJoinThreadArgs->buildRowCounts, hashJoinThreadArgs->buildTableCount);
    std::vector<Table *> buildOutputTables;
    hashBuilderOperator->getOutput(buildOutputTables);

    LookupJoinOperator *lookupJoinOperator;
    if (hashJoinThreadArgs->isOriginal) {
        lookupJoinOperator = dynamic_cast<LookupJoinOperator *>(lookupJoinOperatorFactory->createOperator());
    }
    else {
        opt_module lookupJoinModule = (opt_module)(lookupJoinOperatorFactory->getJitContext()->func);
        lookupJoinOperator = (LookupJoinOperator *)lookupJoinModule(lookupJoinOperatorFactory);
    }
    lookupJoinOperator->addInput(hashJoinThreadArgs->probeTables, hashJoinThreadArgs->probeRowCounts, hashJoinThreadArgs->probeTableCount);
    std::vector<Table *> probeOutputTables;
    lookupJoinOperator->getOutput(probeOutputTables);

    delete hashBuilderOperator;
    delete lookupJoinOperator;
    freeDataInColumn(&probeOutputTables[0], probeOutputTables.size());
    freeOutputTable(probeOutputTables);
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
    hashBuilderOperator->addInput(hashJoinThreadArgs->buildTables, hashJoinThreadArgs->buildRowCounts, hashJoinThreadArgs->buildTableCount);
    std::vector<Table *> buildOutputTables;
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
    lookupJoinOperator->addInput(hashJoinThreadArgs->probeTables, hashJoinThreadArgs->probeRowCounts, hashJoinThreadArgs->probeTableCount);
    std::vector<Table *> probeOutputTables;
    lookupJoinOperator->getOutput(probeOutputTables);
    delete lookupJoinOperator;
}

TEST(NativeOmniJoinTest, testHashBuilderOriginalMultiThreads)
{
    Table **buildTables = (Table **)malloc(BUILD_TABLE_COUNT * sizeof(Table *));
    buildHashJoinTestData(buildTables, BUILD_TABLE_COUNT, COLUMN_COUNT_4);
    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int32_t buildRowCounts[BUILD_TABLE_COUNT];
    for (int32_t i = 0; i < BUILD_TABLE_COUNT; i++) {
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
        setHashJoinThreadArgs(&hashJoinThreadArgs, true, (int64_t)hashBuilderOperatorFactory, buildTables, buildRowCounts,
                BUILD_TABLE_COUNT, 0, nullptr, nullptr, 0);

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
}

TEST(NativeOmniJoinTest, testHashBuilderJITMultiThreads)
{
    Table **buildTables = (Table **)malloc(BUILD_TABLE_COUNT * sizeof(Table *));
    buildHashJoinTestData(buildTables, BUILD_TABLE_COUNT, COLUMN_COUNT_4);
    int32_t rowNum = DISTINCT_VALUE_COUNT * REPEAT_COUNT;
    int32_t buildRowCounts[BUILD_TABLE_COUNT];
    for (int32_t i = 0; i < BUILD_TABLE_COUNT; i++) {
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
        setHashJoinThreadArgs(&hashJoinThreadArgs, false, (int64_t)hashBuilderOperatorFactory, buildTables, buildRowCounts,
                BUILD_TABLE_COUNT, 0, nullptr, nullptr, 0);

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

        Table **buildTables = buildHashBuilderTestData(numbers[i], threadNum);
        HashBuilderOperatorFactory *hashBuilderOperatorFactory = prepareHashBuilder(threadNum);
        struct HashJoinThreadArgs hashBuilderThreadArgs;
        setHashJoinThreadArgs(&hashBuilderThreadArgs, true, (int64_t)hashBuilderOperatorFactory, buildTables, &threadNum,
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

        Table **probeTables = buildLookupJoinTestData(numbers[i], threadNum);
        int32_t *probeRowCounts = getBuildRowCounts();
        
        LookupJoinOperatorFactory *lookupJoinOperatorFactory = prepareLookupJoin(hashBuilderOperatorFactory);
        struct HashJoinThreadArgs lookupJoinThreadArgs;
        setHashJoinThreadArgs(&lookupJoinThreadArgs, true, 0, nullptr, nullptr,
                0, (int64_t)lookupJoinOperatorFactory, probeTables, probeRowCounts, BUILD_TABLE_COUNT);

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

        Table **buildTables = buildHashBuilderTestData(numbers[i], threadNum);
        HashBuilderOperatorFactory *hashBuilderOperatorFactory = prepareHashBuilder(threadNum);
        struct HashJoinThreadArgs hashBuilderThreadArgs;
        setHashJoinThreadArgs(&hashBuilderThreadArgs, false, (int64_t)hashBuilderOperatorFactory, buildTables, &threadNum,
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

        Table **probeTables = buildLookupJoinTestData(numbers[i], threadNum);
        int32_t *probeRowCounts = getBuildRowCounts();
        
        LookupJoinOperatorFactory *lookupJoinOperatorFactory = prepareLookupJoin(hashBuilderOperatorFactory);
        struct HashJoinThreadArgs lookupJoinThreadArgs;
        setHashJoinThreadArgs(&lookupJoinThreadArgs, false, 0, nullptr, nullptr,
                0, (int64_t)lookupJoinOperatorFactory, probeTables, probeRowCounts, BUILD_TABLE_COUNT);

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
    }
}
