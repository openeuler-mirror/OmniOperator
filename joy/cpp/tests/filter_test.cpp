#include "gtest/gtest.h"
#include "../src/jni/filter_api.h"
#include <iostream>
#include <cstring>

TEST (FilterTest, TestFilterCompile) {
    // simple unit test
    std::string filterExpression = "AND(AND($operator$GT(#3, 8766), $operator$LT(#3, 9131)), AND(BETWEEN(#2, 0.05, 0.07), $operator$LT(#0, 24.0)))";

    int32_t vecCount = 4;
    int32_t *inputTypes = (int32_t *)malloc(sizeof(int32_t) * vecCount);
    *(inputTypes) = 1;
    *(inputTypes + 1) = 1;
    *(inputTypes + 2) = 3;
    *(inputTypes + 3) = 3;
    int64_t filterAddr = filterCompile(filterExpression, inputTypes, vecCount);

    const int32_t DATA_SIZE = 1000;
    int32_t *data1 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data1[i] = i;
    }

    int32_t *data2 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data2[i] = i;
    }

    int64_t datas[2] = {(int64_t)data1, (int64_t)data2};

    int32_t *projectedVec = new int32_t[DATA_SIZE];
    int64_t* projectedVecAddress = new int64_t[1];
    projectedVecAddress[0] = (int64_t) projectedVec;

    int32_t *projectIdx = new int32_t[1];
    projectIdx[0] = 0;

    int32_t numSelectedRows = filterExecute(filterAddr, datas, inputTypes, vecCount, DATA_SIZE, projectedVecAddress, projectIdx, 1);
    std::cout << "Number of selected rows: " << numSelectedRows << std::endl;
}