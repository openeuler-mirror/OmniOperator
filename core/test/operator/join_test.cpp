#include "gtest/gtest.h"
#include "../../src/operator/join/hash_builder.h"
#include "../../src/operator/join/lookup_join.h"
#include "../../src/operator/hash_util.h"
#include "../util/test_util.h"
#include <time.h>
#include <vector>
#include <iostream>
#include <chrono>

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
    hashBuilderFactory->setJitContext(NULL);
    HashBuilderOperator *hashBuilderOperator = (HashBuilderOperator *)hashBuilderFactory->createOperator();
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
    lookupJoinFactory->setJitContext(NULL);
    LookupJoinOperator *lookupJoinOperator = (LookupJoinOperator *)lookupJoinFactory->createOperator();
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
    hashBuilderFactory->setJitContext(NULL);

    HashBuilderOperator *hashBuilderOperator0 = (HashBuilderOperator *)hashBuilderFactory->createOperator();
    hashBuilderOperator0->addInput(buildTables0, rowCounts0, 1);

    HashBuilderOperator *hashBuilderOperator1 = (HashBuilderOperator *)hashBuilderFactory->createOperator();
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
        probeTypes, probeTypesCount, probeOutputCols, probeOutputColsCount, probeHashCols, probeHashColsCount, buildOutputCols,
        buildOutputTypes, buildOutputColsCount, hashBuilderFactoryAddr);
    lookupJoinFactory->setJitContext(NULL);
    LookupJoinOperator *lookupJoinOperator = (LookupJoinOperator *)lookupJoinFactory->createOperator();
    lookupJoinOperator->addInput(probeTable, DATA_SIZE);
    std::vector<Table *> output;
    lookupJoinOperator->getOutput(output);
    EXPECT_EQ(output.size(), 1);
    //output[0]->printTable();

    const int32_t EXPECTED_DATA_SIZE = 18;
    EXPECT_EQ(output[0]->getPositionCount(), EXPECTED_DATA_SIZE);
}
