#include "gtest/gtest.h"
#include "../../src/operator/projection/projection.h"
#include "../../src/vector/vector_common.h"
#include "../../src/vector/vector_helper.h"
#include <iostream>
#include <string>
#include <cstring>
#include <vector>
#include <chrono>

using namespace omniruntime::op;
using namespace omniruntime::vec;

namespace project_test {

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
        }

    }
    return vecBatch;
}

int32_t* makeInts(const int32_t SIZE, const int32_t START = 0) {
    int32_t* arr = new int32_t[SIZE];
    int32_t idx = 0;
    for (int32_t i = START; i < START + SIZE; i++) arr[idx++] = i;
    return arr;
}

int64_t* makeLongs(const int32_t SIZE, const int64_t START = 0) {
    int64_t* arr = new int64_t[SIZE];
    int32_t idx = 0;
    for (int64_t i = START; i < START + SIZE; i++) arr[idx++] = i;
    return arr;
}

double* makeDoubles(const int32_t SIZE, const double START = 0) {
    double* arr = new double[SIZE];
    int32_t idx = 0;
    for (double i = START; i < START + SIZE; i += 1) arr[idx++] = i;
    return arr;
}

TEST (ProjectTest, Simple) {
    const int32_t NUM_ROWS = 1000;
    int32_t* col = makeInts(NUM_ROWS);
    const int32_t NUM_COLS = 1;
    string exprs[NUM_COLS] = {"$operator$ADD(#0, 5)"};
    int32_t inputTypes[NUM_COLS] = {1};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, NUM_COLS, inputTypes, NUM_COLS);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[NUM_COLS] = {(int64_t) col};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector*) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i + 5);
    }
    
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
}

TEST (ProjectTest, Negatives) {
    const int32_t NUM_ROWS = 1000;
    int32_t* col = makeInts(NUM_ROWS, -5);
    const int32_t NUM_COLS = 1;
    string exprs[NUM_COLS] = {"$operator$SUBTRACT(#0, 500)"};
    int32_t inputTypes[NUM_COLS] = {1};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, NUM_COLS, inputTypes, NUM_COLS);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[NUM_COLS] = {(int64_t) col};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector*) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i - 505);
    }
    
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
}

TEST (ProjectTest, Longs) {
    const int32_t NUM_ROWS = 10000;
    int64_t* col = makeLongs(NUM_ROWS, -5000);
    const int32_t NUM_COLS = 1;
    string exprs[NUM_COLS] = {"$operator$MULTIPLY(#0, 5000000)"};
    int32_t inputTypes[NUM_COLS] = {2};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, NUM_COLS, inputTypes, NUM_COLS);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[NUM_COLS] = {(int64_t) col};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < 1; i++) {
        int64_t val0 = ((LongVector*) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, (int64_t) (i - 5000) * 5000000);
    }
    
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
}

TEST (ProjectTest, Doubles) {
    const int32_t NUM_ROWS = 10000;
    double* col = makeDoubles(NUM_ROWS, -5000.5);
    const int32_t NUM_COLS = 1;
    string exprs[NUM_COLS] = {"$operator$DIVIDE(#0, 2)"};
    int32_t inputTypes[NUM_COLS] = {3};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, NUM_COLS, inputTypes, NUM_COLS);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[NUM_COLS] = {(int64_t) col};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < 1; i++) {
        double val0 = ((DoubleVector*) ret[0]->GetVector(0))->GetValue(i);
        double expected = (i - 5000.5) / 2;
        EXPECT_TRUE(val0 > expected - 0.1 && val0 < expected + 0.1);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
}

TEST (ProjectTest, MultipleColumns) {
    const int32_t NUM_ROWS = 1000;
    int32_t* col1 = makeInts(NUM_ROWS);
    int32_t* col2 = makeInts(NUM_ROWS, -100);
    int64_t* col3 = makeLongs(NUM_ROWS, -10);
    const int32_t NUM_PROJECT = 2;
    string exprs[NUM_PROJECT] = {"$operator$SUBTRACT(#0, 10)", "$operator$ADD(#2, 1)"};
    const int32_t NUM_COLS = 3;
    int32_t inputTypes[NUM_COLS] = {1, 1, 2};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, NUM_PROJECT, inputTypes, NUM_COLS);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector*) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i - 10);
        int64_t val1 = ((LongVector*) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, i - 9);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;

    delete op;
    delete factory;
}

TEST (ProjectTest, DependOtherColumn) {
    const int32_t NUM_ROWS = 1000;
    int32_t* col1 = makeInts(NUM_ROWS);
    int32_t* col2 = makeInts(NUM_ROWS, -100);
    int64_t* col3 = makeLongs(NUM_ROWS);
    const int32_t NUM_PROJECT = 2;
    string exprs[NUM_PROJECT] = {"$operator$MULTIPLY(#0, #1)", "IF($operator$LESS_THAN(#0, 500), 4000000000, #2)"};
    const int32_t NUM_COLS = 3;
    int32_t inputTypes[NUM_COLS] = {1, 1, 2};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, NUM_PROJECT, inputTypes, NUM_COLS);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    VectorBatch* t = createInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    op->AddInput(t);
    vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector*) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i * (i - 100));
        int64_t val1 = ((LongVector*) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, (int64_t) (i < 500 ? 4000000000 : i));
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;

    delete op;
    delete factory;
}

}