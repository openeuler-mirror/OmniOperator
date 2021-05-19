#include "gtest/gtest.h"
// #include "../src/jni/filter_api.h"
#include "../../src/operator/filter/filter.h"
#include <iostream>
#include <cstring>
#include <vector>

Table* createInput(const int32_t NUM_ROWS,
                    const int32_t NUM_COLS,
                    int32_t* inputTypes,
                    int64_t* allData)
{
    Table* t = new Table(NUM_ROWS, NUM_COLS);
    for (int i = 0; i < NUM_COLS; i++) {
        void* data = (void*) allData[i];
        ColumnType columnType = static_cast<ColumnType>(inputTypes[i]);
        Column* col = new Column(data, columnType, NUM_ROWS);
        t->setColumn(col, columnType);
    }
    return t;
}

bool checkOutput(Table* t, const int32_t NUM_ROWS, bool (*filter)(Table*, int32_t)) {
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        if (!filter(t, i)) {
            return false;
        }
    }
    return true;
}

// Expects 1 column of type int32
bool filter1(Table* t, int32_t index) {
    return *((int32_t*) t->getColumn(0)->getValue(index)) >= 5;
}

// Expects 2 columns of type int32, int64
bool filter2(Table* t, int32_t index) {
    int32_t val1 = *((int32_t*) t->getColumn(0)->getValue(index));
    int64_t val2 = *((int64_t*) t->getColumn(1)->getValue(index));
    // true if both values are negative
    return val1 < 0 && val2 < 0;
}

// Expects 3 columns of type int32, int64, double
bool filter3(Table* t, int32_t index) {
    int32_t val1 = *((int32_t*) t->getColumn(0)->getValue(index));
    int64_t val2 = *((int64_t*) t->getColumn(1)->getValue(index));
    double val3 = *((double*) t->getColumn(2)->getValue(index));
    // first val is multiple of 3, second val = 3 billion, third val >= 0.4.
    return val1 % 3 == 0 && val2 == (int64_t) 3e9 && val3 >= 0.4;
}

TEST (FilterTest, MultipleInputs) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    
    const int32_t NUM_ROWS = 1000;
    int32_t* data1 = new int32_t[NUM_ROWS];
    int32_t* data2 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        data1[i] = i % 10;
        data2[i] = i % 5 + 1;
    }
    int64_t allData[NUM_COLS] = {(int64_t) data1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    std::vector<Table*> ret;

    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    Filter* filter = new Filter(filter1);
    NativeOmniOperator* op = new NativeOmniFilterOperator(filter, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_TRUE(checkOutput(ret[0], numReturned, filter1));
    EXPECT_EQ(numReturned, 500);

    allData[0] = (int64_t) data2;
    Table* in2 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    op->addInput(in2, NUM_ROWS);
    numReturned = op->getOutput(ret);
    EXPECT_TRUE(checkOutput(ret[1], numReturned, filter1));
    EXPECT_EQ(numReturned, 200);

    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete in1;
    delete in2;
    delete filter;
    delete op;
    delete ret[0];
    delete ret[1];

}

TEST (FilterTest, NegativeValues) {
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    
    const int32_t NUM_ROWS = 10000;
    int32_t* data1 = new int32_t[NUM_ROWS];
    int64_t* data2 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        data1[i] = i * i % 100 + 1;
        if (i % 5 == 0) data1[i] = -data1[i];
    }
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        data2[i] = i % 100 + 3e9;
        if (i % 7 == 0) data2[i] = -data2[i];
    }
    int64_t allData[NUM_COLS] = {(int64_t) data1, (int64_t) data2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1};
    std::vector<Table*> ret;

    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    Filter* filter = new Filter(filter2);
    NativeOmniOperator* op = new NativeOmniFilterOperator(filter, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_TRUE(checkOutput(ret[0], numReturned, filter2));
    // Both values are negative for every multiple of 35.
    EXPECT_EQ(numReturned, 286);

    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete in1;
    delete filter;
    delete op;
    delete ret[0];
}

TEST (FilterTest, AllTypes) {

    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    inputTypes[2] = 3;
    
    const int32_t NUM_ROWS = 10000;
    int32_t* data1 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        data1[i] = i;
    }

    int64_t* data2 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        int64_t entry = i % 2 ? 3e9 : 0;
        data2[i] = entry;
    }

    double* data3 = new double[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        data3[i] = i % 10 / 10.0;
    }

    int64_t allData[NUM_COLS] = {(int64_t) data1, (int64_t) data2, (int64_t) data3};
    const int32_t PROJECT_COUNT = 3;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1, 2};
    std::vector<Table*> ret;

    Table* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    Filter* filter = new Filter(filter3);
    NativeOmniOperator* op = new NativeOmniFilterOperator(filter, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    op->addInput(in1, NUM_ROWS);
    int32_t numReturned = op->getOutput(ret);
    EXPECT_TRUE(checkOutput(ret[0], numReturned, filter3));
    EXPECT_EQ(numReturned, 1000);

    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete[] data3;
    delete in1;
    delete filter;
    delete op;
    delete ret[0];
}

TEST (FilterTest, Compile) {
    // simple unit test
    std::string filterExpression = "AND(AND($operator$GT(#3, 8766), $operator$LT(#3, 9131)), AND(BETWEEN(#2, 0.05, 0.07), $operator$LT(#0, 24.0)))";

    const int32_t NUM_COLS = 4;
    int32_t *inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 3;
    inputTypes[3] = 3;
    
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
    int32_t *projectIdx = new int32_t[1];
    projectIdx[0] = 0;
    Table* t = createInput(DATA_SIZE, NUM_COLS, inputTypes, datas);

    NativeOmniFilterOperatorFactory *factory = new NativeOmniFilterOperatorFactory(filterExpression, inputTypes, NUM_COLS, projectIdx, 1);
    NativeOmniOperator *op = factory->createOmniOperator();
    op->addInput(t, DATA_SIZE);
    std::vector<Table*> ret;
    int32_t numSelectedRows = op->getOutput(ret);
    EXPECT_EQ(numSelectedRows, 500);
    
    delete[] inputTypes;
    delete[] data1; 
    delete[] data2;
    delete[] data3;
    delete[] data4;
    delete[] projectIdx;
    delete t;
    delete factory;
    delete op;
    delete ret[0];
}