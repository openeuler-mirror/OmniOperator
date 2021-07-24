#include "gtest/gtest.h"
// #include "../src/jni/filter_api.h"
#include "../../src/operator/filter/filter_and_project.h"
#include <iostream>
#include <cstring>
#include <vector>
#include <chrono>
#include "../../src/vector/vector_common.h"
#include "../../src/vector/vector_helper.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;

VectorBatch* createInput(const int32_t NUM_ROWS,
                    const int32_t NUM_COLS,
                    int32_t* inputTypes,
                    int64_t* allData)
{
    VectorBatch *vecBatch = new VectorBatch(NUM_COLS, NUM_ROWS);
    vecBatch->SetVectors(inputTypes);
    for (int i = 0; i < NUM_COLS; ++i) {
        switch (inputTypes[i]) {
            case OMNI_VEC_TYPE_INT:
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], NUM_ROWS);
                break;
            case OMNI_VEC_TYPE_LONG:
                ((LongVector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *)allData[i], NUM_ROWS);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                ((DoubleVector *)vecBatch->GetVector(i))->SetValues(0, (double *)allData[i], NUM_ROWS);
                break;
            case OMNI_VEC_TYPE_SHORT:
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], NUM_ROWS);
                break;
            case OMNI_VEC_TYPE_VARCHAR: {
                for (int j = 0; j < NUM_ROWS; ++j) {
                    // std::cout << "row: " << j << std::endl;
                    int64_t addr = ((int64_t *)(allData[i]))[j];
                    std::string s ((char *)(addr));
                    // std::cout << "s: " << s << std::endl;
                    ((VarcharVector *)vecBatch->GetVector(i))->SetValue(j, const_cast<char *>(s.c_str()), s.length() + 1);
                }
                break;
            }
        }

    }
    return vecBatch;
}

bool checkOutput(VectorBatch* t, const int32_t NUM_ROWS, bool (*filter)(VectorBatch*, int32_t)) {
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        if (!filter(t, i)) {
            return false;
        }
    }
    return true;
}

// Expects 1 column of type int32
bool filter1(VectorBatch* t, int32_t index) {
    return ((IntVector *)t->GetVector(0))->GetValue(index) <= 4;
}

// Expects 2 columns of type int32, int64
bool filter2(VectorBatch* t, int32_t index) {
    int32_t val1 = ((IntVector *)t->GetVector(0))->GetValue(index);
    int64_t val2 = ((LongVector *)t->GetVector(1))->GetValue(index);
    // true if both values are negative
    return val1 < 0 && val2 < 0;
}

// Expects 3 columns of type int32, int64, double
bool filter3(VectorBatch* t, int32_t index) {
    int32_t val1 = ((IntVector *)t->GetVector(0))->GetValue(index);
    int64_t val2 = ((LongVector *)t->GetVector(1))->GetValue(index);
    double val3 = ((DoubleVector *)t->GetVector(2))->GetValue(index);
    // first val is multiple of 3, second val = 3 billion, third val >= 0.4.
    return val1 % 3 == 0 && val2 == (int64_t) 3e9 && val3 >= 0.4;
}

bool filter4(VectorBatch* t, int32_t index) {
    int32_t val0 = ((IntVector *)t->GetVector(0))->GetValue(index);
    int32_t val2 = ((IntVector *)t->GetVector(1))->GetValue(index);
    double val4 = ((DoubleVector *)t->GetVector(2))->GetValue(index);
    int64_t val5 = ((LongVector *)t->GetVector(3))->GetValue(index);
    return (val0 != 1 && val2 > 4800 && val4 < 50.8) || val5 >= 52;
}

bool filter5(VectorBatch* t, int32_t index) {
    int32_t val0 = ((IntVector *)t->GetVector(0))->GetValue(index);
    double val2 = ((DoubleVector *)t->GetVector(2))->GetValue(index);
    double val3 = ((DoubleVector *)t->GetVector(3))->GetValue(index);
    return val0 < 24 && val2 >= 0.05 && val2 <= 0.07 && val3 > 9766 && val3 < 9131;
}

bool filter6(VectorBatch* t, int32_t index) {
    // project order reversed
    int64_t val0 = ((LongVector *)t->GetVector(0))->GetValue(index);
    int64_t val1 = ((LongVector *)t->GetVector(1))->GetValue(index);
    int32_t val2 = ((IntVector *)t->GetVector(2))->GetValue(index);
    int32_t val3 = ((IntVector *)t->GetVector(3))->GetValue(index);
    return (val0 >= 0 || val1 <= -3e9) && (val2 == -12 || val3 < 50);
}

TEST (FilterTest, LessThan) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;

    const int32_t NUM_ROWS = 5000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$LESS_THAN(#0, 2000)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();

    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val < 2000);
    }
    VectorHelper::FreeVecBatch(in1);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete op;
    delete factory;
}

TEST (FilterTest, GreaterThan) {
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;

    const int32_t NUM_ROWS = 5000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int64_t* col2 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 25;
        col2[i] = 3e9;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$GREATER_THAN(#0, 20)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();

    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 800);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_TRUE(val0 > 20);
        EXPECT_EQ(val1, (int64_t) 3e9);
    }
    VectorHelper::FreeVecBatch(in1);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST (FilterTest, EqualTo) {
    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    inputTypes[2] = 3;

    const int32_t NUM_ROWS = 5000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    double* col2 = new double[NUM_ROWS];
    int64_t* col3 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col2[i] = col3[i] = i % 100;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col3, (int64_t) col2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {2, 1};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$EQUAL(#2, 50.0)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();

    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 50);
    for (int32_t i = 0; i < numReturned; i++) {
        double val0 = ((DoubleVector *)ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, 50);
        EXPECT_EQ(val0, val1);
    }
    VectorHelper::FreeVecBatch(in1);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

TEST (FilterTest, GreaterThanOrEqualTo) {
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;

    const int32_t NUM_ROWS = 5000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col2[i] = (i * (i + 2)) % 40;
        col1[i] = i;
        if (i % 45 == 0) col2[i] = 30;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {1};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$GREATER_THAN_OR_EQUAL(#1, 30)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 834);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 >= 30);
    }
    VectorHelper::FreeVecBatch(in1);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST (FilterTest, NotEqualTo) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 3;

    const int32_t NUM_ROWS = 5000;
    double* col1 = new double[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$NOT_EQUAL(#0, 0)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();

    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4999);
    double cnt = 1;
    for (int32_t i = 0; i < numReturned; i++) {
        double val0 = ((DoubleVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, cnt++);
    }
    VectorHelper::FreeVecBatch(in1);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete op;
    delete factory;
}

TEST (FilterTest, AllPass) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;

    const int32_t NUM_ROWS = 20000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = 9348;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$EQUAL(#0, 9348)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();

    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    std::cout << "numReturned: " << numReturned << std::endl;
    EXPECT_EQ(numReturned, 20000);

    VectorHelper::FreeVecBatch(in1);
    std::cout << "freed in1" << std::endl;
    // TODO: find out why freeing ret causes segfault
    // VectorHelper::freeVecBatches(ret);
//    std::cout << "freed ret" << std::endl;

    delete[] inputTypes;
    delete[] col1;
    delete op;
    delete factory;
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
        data2[i] = i % 6 + 1;
    }
    int64_t allData[NUM_COLS] = {(int64_t) data1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$LESS_THAN_OR_EQUAL(#0, 4)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();

    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(checkOutput(ret[0], numReturned, filter1));
    EXPECT_EQ(numReturned, 500);

    VectorHelper::FreeVecBatch(in1);

    allData[0] = (int64_t) data2;
    VectorBatch* in2 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    op->AddInput(in2);
    numReturned = op->GetOutput(ret);
    EXPECT_TRUE(checkOutput(ret[1], numReturned, filter1));
    EXPECT_EQ(numReturned, 668);

    VectorHelper::FreeVecBatch(in2);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete op;
    delete factory;
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
        data2[i] = i % 100 + 3e9;
        if (i % 7 == 0) data2[i] = -data2[i];
    }
    int64_t allData[NUM_COLS] = {(int64_t) data1, (int64_t) data2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1};
    std::vector<VectorBatch*> ret;

    VectorBatch* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("AND($operator$LESS_THAN_OR_EQUAL(#0, -1), $operator$LESS_THAN_OR_EQUAL(#1, -1))", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(checkOutput(ret[0], numReturned, filter2));
    // Both values are negative for every multiple of 35.
    EXPECT_EQ(numReturned, 286);

    VectorHelper::FreeVecBatch(in1);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete op;
    delete factory;
}

TEST (FilterTest, AllTypes) {

    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    inputTypes[2] = 3;

    const int32_t NUM_ROWS = 1000;
    int32_t* data1 = new int32_t[NUM_ROWS];
    int64_t* data2 = new int64_t[NUM_ROWS];
    double* data3 = new double[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        data1[i] = i % 3;
        data2[i] = i % 2 ? 3e9 : 0;
        data3[i] = i % 10 / 10.0;
    }

    int64_t allData[NUM_COLS] = {(int64_t) data1, (int64_t) data2, (int64_t) data3};
    const int32_t PROJECT_COUNT = 3;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1, 2};
    std::vector<VectorBatch*> ret;

    VectorBatch* in1 = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    std::string expr = "AND($operator$EQUAL(#0, 0), AND($operator$EQUAL(#1, 3000000000), $operator$GREATER_THAN_OR_EQUAL(#2, 0.4)))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    // std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    op->AddInput(in1);
    // std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    // std::cout << "TIME TAKEN FOR FILTER: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() << "ns" << std::endl;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(checkOutput(ret[0], numReturned, filter3));
    EXPECT_EQ(numReturned, 100);

    VectorHelper::FreeVecBatch(in1);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete[] data3;
    delete op;
    delete factory;
}

TEST (FilterTest, Compile) {
    // TPCH 6
    std::string filterExpression = "AND(AND($operator$GREATER_THAN(#3, 8766), $operator$LESS_THAN(#3, 9131)), AND(BETWEEN(#2, 0.05, 0.07), $operator$LESS_THAN(#0, 24.0)))";

    const int32_t NUM_COLS = 4;
    int32_t *inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 3;
    inputTypes[1] = 1;
    inputTypes[2] = 3;
    inputTypes[3] = 3;

    const int32_t DATA_SIZE = 10000;
    double *data1 = new double[DATA_SIZE];
    int32_t *data2 = new int32_t[DATA_SIZE];
    double *data3 = new double[DATA_SIZE];
    double *data4 = new double[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data4[i] = i;
        data3[i] = i % 10 / 100.0;
        data1[i] = i % 26;
        data2[i] = 6;
    }

    int64_t datas[4] = {(int64_t)data1, (int64_t)data2, (int64_t)data3, (int64_t)data4};
    int32_t *projectIdx = new int32_t[1];
    projectIdx[0] = 0;
    VectorBatch* t = createInput(DATA_SIZE, NUM_COLS, inputTypes, datas);

    OperatorFactory *factory = new FilterAndProjectOperatorFactory(filterExpression, inputTypes, NUM_COLS, projectIdx, 1);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numSelectedRows = op->GetOutput(ret);
    EXPECT_EQ(numSelectedRows, 100);
    // EXPECT_TRUE(checkOutput(ret[0], DATA_SIZE, filter5));

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete[] data3;
    delete[] data4;
    delete[] projectIdx;
    delete op;
    delete factory;
}

TEST (FilterTest, LogicalOperators1) {
    std::string expr = "OR($operator$GREATER_THAN_OR_EQUAL(#5, 52), AND($operator$LESS_THAN(#4, 50.8), AND(AND($operator$GREATER_THAN(#2, 4800), $operator$LESS_THAN_OR_EQUAL(#1, 9990)), AND($operator$NOT_EQUAL(#0, 1), $operator$EQUAL(#3, 3000000000)))))";

    const int32_t NUM_COLS = 6;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    // int int int long double long
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;
    inputTypes[3] = 2;
    inputTypes[4] = 3;
    inputTypes[5] = 2;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    int32_t* col3 = new int32_t[NUM_ROWS];
    int64_t* col4 = new int64_t[NUM_ROWS];
    double* col5 = new double[NUM_ROWS];
    int64_t* col6 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 3 ? 1 : 0;
        col2[i] = col3[i] = i;
        col4[i] = i % 2 ? 2999999999 : 3e9;
        col5[i] = 50 + i / 10.0;
        col6[i] = i % 55;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3, (int64_t) col4, (int64_t) col5, (int64_t) col6};
    const int32_t PROJECT_COUNT = 4;
    int32_t projectIndices[PROJECT_COUNT] = {0, 2, 4, 5};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 543);
    EXPECT_TRUE(checkOutput(ret[0], numReturned, filter4));

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete[] col5;
    delete[] col6;
    delete op;
    delete factory;
}

TEST (FilterTest, LogicalOperators2) {
    std::string expr = "AND(OR($operator$LESS_THAN(#0, 50), $operator$EQUAL(#1, -12)), OR($operator$LESS_THAN_OR_EQUAL(#2, -3000000000), $operator$GREATER_THAN_OR_EQUAL(#3, 0)))";

    const int32_t NUM_COLS = 4;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 2;
    inputTypes[3] = 2;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    int64_t* col3 = new int64_t[NUM_ROWS];
    int64_t* col4 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 100;
        col2[i] = i % 7 == 0 ? -12 : i;
        col3[i] = i % 8 == 0 ? -i - 3e9 : i + 3e9;
        col4[i] = i % 9 - 4;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3, (int64_t) col4};
    const int32_t PROJECT_COUNT = 4;
    int32_t projectIndices[PROJECT_COUNT] = {3, 2, 1, 0};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 3498);
    EXPECT_TRUE(checkOutput(ret[0], numReturned, filter6));
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete op;
    delete factory;
}

TEST (FilterTest, LogicalOperators3) {
    std::string expr = "AND($operator$NOT_EQUAL(#1, 0), OR(OR(OR($operator$EQUAL(#0, 1), $operator$EQUAL(#0, 2)), $operator$EQUAL(#0, 3)), OR(OR(OR($operator$EQUAL(55, #0), $operator$EQUAL(5, #0)), $operator$EQUAL(#0, 8)), $operator$EQUAL(#0, 13))))";
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 3;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    double* col2 = new double[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = 0;
        col2[i] = 1.5;
    }
    col1[0] = 0;
    col1[1] = 1;
    col1[2] = 1;
    col1[3] = 2;
    col1[4] = 3;
    col1[5] = 5;
    col1[6] = 8;
    col1[7] = 13;
    col2[2] = 0;
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {1, 0};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 6);
    for (int32_t i = 0; i < 6; i++) {
        double val0 = ((DoubleVector *)ret[0]->GetVector(0))->GetValue(i);
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_TRUE(val0 != 0);
        EXPECT_TRUE(val1 == col1[i + 2]);
    }
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST (FilterTest, ArithmeticAdd) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 5;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$GREATER_THAN($operator$ADD(#0, 1), 4)", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 + 1 > 4);
    }
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete op;
    delete factory;
}

TEST (FilterTest, ArithmeticSubtract) {
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int64_t* col2 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 10;
        col2[i] = i;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$LESS_THAN(0, $operator$SUBTRACT(#0, 5))", inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(0 < val0 - 5);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST (FilterTest, ArithmeticMultiply) {
    const int32_t NUM_COLS = 2;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int64_t* col2 = new int64_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 2;
        col2[i] = i % 10 + 1;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2};
    const int32_t PROJECT_COUNT = 2;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    std::string expr = "AND($operator$EQUAL(0, $operator$MULTIPLY(#0, #0)), $operator$GREATER_THAN(7, $operator$MULTIPLY(2, #1)))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, 0);
        EXPECT_TRUE(val1 * 2 < 7);
    }
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST (FilterTest, Conditional) {
    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    int32_t* col3 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 2;
        col2[i] = 50;
        col3[i] = 100;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t PROJECT_COUNT = 3;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1, 2};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    std::string expr = "$operator$EQUAL(IF($operator$EQUAL(#0, 0), $operator$ADD(#1, 5), #2), 55)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 5000);

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

TEST (FilterTest, Conditional2) {
    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    int32_t* col3 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = i % 10;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t PROJECT_COUNT = 3;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1, 2};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    std::string expr = "AND(IF($operator$EQUAL(#0, 0), $operator$LESS_THAN(#1, 3), $operator$EQUAL(#1, 4)), $operator$GREATER_THAN(#2, 3))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

TEST (FilterTest, DISABLED_ArithmeticDivide) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
}

TEST (FilterTest, In) {
    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    int32_t* col3 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 10;
        col2[i] = i % 5;
        col3[i] = i % 6 + 12;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t PROJECT_COUNT = 3;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1, 2};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    std::string expr = "IN(#0, 1, 3, 5)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 3000);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 == 1 || val0 == 3 || val0 == 5);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

TEST (FilterTest, Between) {
    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    int32_t* col3 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 5;
        col2[i] = i % 11;
        col3[i] = (i % 21) - 3;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t PROJECT_COUNT = 3;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1, 2};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    std::string expr = "BETWEEN(#1, #0, #2)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4705);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        int32_t val2 = ((IntVector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_TRUE((val0 <= val1) && (val1 <= val2));
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

TEST (FilterTest, NotEqualToAbs) {
    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;

    const int32_t NUM_ROWS = 100000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i - 32435;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    std::string expr = "$operator$NOT_EQUAL(abs(#0), 4)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 99998);

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete op;
    delete factory;
}



// Function tests
TEST (FilterTest, MathFunctionFilter1) {
    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    int32_t* col3 = new int32_t[NUM_ROWS];

    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = -1;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t PROJECT_COUNT = 3;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1, 2};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    std::string expr = "AND($operator$EQUAL(abs(#0), abs(#2)), $operator$EQUAL(abs(#0), abs(#1)))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 1000);
    std::cout << "numReturned: " << numReturned << std::endl;
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        int32_t val2 = ((IntVector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_TRUE((std::abs(val0) == std::abs(val1)) && (std::abs(val1) == std::abs(val2)));
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}



// For testing different types
TEST (FilterTest, MathFunctionFilter2) {
    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    inputTypes[2] = 1;

    const int32_t NUM_ROWS = 10000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int64_t* col2 = new int64_t[NUM_ROWS];
    int32_t* col3 = new int32_t[NUM_ROWS];

    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = -1;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t PROJECT_COUNT = 3;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1, 2};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    std::string expr = "$operator$EQUAL(abs(CAST(#0)), abs(CAST(#1)))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 2000);

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

// String filter and varcharvec testing
TEST (FilterTest, FilterString1) {
    vector<string*> strings;

    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 100;

    const int32_t NUM_ROWS = 100000;
    int64_t* col1 = new int64_t[NUM_ROWS];

    for (int32_t i = 0; i < NUM_ROWS; i++) {
        if (i % 40 == 0) {
            std::string *s = new std::string("hello");
            col1[i] = (int64_t)(s->c_str());
            strings.push_back(s);
        }
        else {
            std::string *s = new std::string("abcdefghijklmnopqrstuvwxyz");
            col1[i] = (int64_t)(s->c_str());
            strings.push_back(s);
        }
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);


    std::string expr = "$operator$EQUAL(#0, 'hello')";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 2500);


    for (auto &s : strings) {
        delete s;
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete op;
    delete factory;
}


TEST (FilterTest, Coalesce1) {
    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t NUM_ROWS = 100000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int64_t* col2 = new int64_t[NUM_ROWS];
    int32_t* col3 = new int32_t[NUM_ROWS];

    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = 100;
        col2[i] = 21;
        col3[i] = -1;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t PROJECT_COUNT = 3;
    int32_t projectIndices[PROJECT_COUNT] = {0, 1, 2};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    for (int32_t i = 0; i < NUM_ROWS; i ++) {
        if (i % 2) {
            t->GetVector(1)->SetValueNull(i);
        }
        else {
            t->GetVector(1)->SetValueNotNull(i);
        }
    }

    std::string expr = "$operator$EQUAL(21, COALESCE(#1, #0))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 50000);

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}


TEST (FilterTest, Coalesce2) {
    vector<string*> strings;

    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 100;

    const int32_t NUM_ROWS = 1000;
    int64_t* col1 = new int64_t[NUM_ROWS];

    for (int32_t i = 0; i < NUM_ROWS; i++) {
        std::string *s = new std::string("hello");
        col1[i] = (int64_t)(s->c_str());
        strings.push_back(s);
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    int32_t projectIndices[PROJECT_COUNT] = {0};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    for (int32_t i = 0; i < NUM_ROWS; i++) {
        if (i % 2) {
            t->GetVector(0)->SetValueNull(i);
        }
        else {
            // Seemingly necessary so that the bitmap doesn't get default values
            t->GetVector(0)->SetValueNotNull(i);
        };
    }

    std::string expr = "$operator$EQUAL(COALESCE(#0, 'bye'), 'hello')";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projectIndices, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 500);

    for (auto &s : strings) {
        delete s;
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete op;
    delete factory;
}
