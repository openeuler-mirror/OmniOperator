#include "gtest/gtest.h"
// #include "../src/jni/filter_api.h"
#include "../../src/operator/filter/filter.h"
#include <iostream>
#include <cstring>
#include <vector>

TEST (FilterTest, TestFilterCompile) {
    // simple unit test
    std::string filterExpression = "AND(AND($operator$GT(#3, 8766), $operator$LT(#3, 9131)), AND(BETWEEN(#2, 0.05, 0.07), $operator$LT(#0, 24.0)))";

    int32_t vecCount = 4;
    int32_t *inputTypes = (int32_t *)malloc(sizeof(int32_t) * vecCount);
    *(inputTypes) = 1;
    *(inputTypes + 1) = 1;
    *(inputTypes + 2) = 3;
    *(inputTypes + 3) = 3;
    // int64_t filterAddr = filterCompile(filterExpression, inputTypes, vecCount);

    const int32_t DATA_SIZE = 1000;
    int32_t *data1 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data1[i] = i;
    }

    int32_t *data2 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data2[i] = i;
    }

    double *data3 = new double[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data3[i] = i;
    }

    double *data4 = new double[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data4[i] = i;
    }

    int64_t datas[4] = {(int64_t)data1, (int64_t)data2, (int64_t)data3, (int64_t)data4};

    // unnecessary with refactored code
    // int32_t *projectedVec = new int32_t[DATA_SIZE];
    // int64_t* projectedVecAddress = new int64_t[1];
    // projectedVecAddress[0] = (int64_t) projectedVec;

    int32_t *projectIdx = new int32_t[1];
    projectIdx[0] = 0;

    // Table* t = new Table(rowNum, columnCount);
    //         for (int i = 0; i < columnCount; i++) {
    //             void* data = table[i];
    //             ColumnType columnType = static_cast<ColumnType>(colTypes[i]);
    //             Column* col = new Column(data, columnType, rowNum);
    //             t->setColumn(col, columnType);
    //         }

    std::cout << "data initialized" << std::endl;

    Table* t = new Table(DATA_SIZE, vecCount);
    for (int i = 0; i < vecCount; i++) {
        void* data = (void*) datas[i];
        ColumnType columnType = static_cast<ColumnType>(inputTypes[i]);
        Column* col = new Column(data, columnType, DATA_SIZE);
        t->setColumn(col, columnType);
    }

    std::cout << "table created" << std::endl;

    NativeOmniFilterOperatorFactory *factory = new NativeOmniFilterOperatorFactory(filterExpression, inputTypes, vecCount, projectIdx, 1);
    NativeOmniOperator *op = factory->createOmniOperator();
    op->addInput(t, DATA_SIZE);

    std::cout << "addInput performed" << std::endl;

    std::vector<Table*>* ret = new std::vector<Table*>();

    std::cout << "table ret created" << std::endl;
    int32_t numSelectedRows = op->getOutput(*ret);

    // int32_t numSelectedRows = filterExecute(filterAddr, datas, inputTypes, vecCount, DATA_SIZE, projectedVecAddress, projectIdx, 1);
    EXPECT_EQ(numSelectedRows, 500);
    std::cout << "Number of selected rows: " << numSelectedRows << std::endl;

    delete[] (inputTypes);
    delete[] (data1); 
    delete[] (data2);
    delete[] (data3);
    delete[] (data4);
    delete[] projectIdx;
    delete t;
    delete factory;
    delete ret;

}