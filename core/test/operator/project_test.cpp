/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include "gtest/gtest.h"
#include "../../src/operator/projection/projection.h"
#include "../../src/vector/vector_helper.h"
#include <string>
#include <vector>
#include <chrono>

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;
namespace project_test {

VectorBatch* CreateInput(const int32_t numRows,
                         const int32_t numCols,
                         int32_t* inputTypes,
                         int64_t* allData)
{
    auto *vecBatch = new VectorBatch(numCols, numRows);
    vecBatch->SetVectors(inputTypes);
    for (int i = 0; i < numCols; ++i) {
        switch (inputTypes[i]) {
            case OMNI_VEC_TYPE_INT:
                ((IntVector *) vecBatch->GetVector(i))->SetValues(0, (int32_t *) allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_LONG:
                ((LongVector *) vecBatch->GetVector(i))->SetValues(0, (int64_t *) allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                ((DoubleVector *) vecBatch->GetVector(i))->SetValues(0, (double *) allData[i], numRows);
                break;
            default: {
                DebugError("No such data type %d", inputTypes[i]);
                break;
            }
        }
    }
    return vecBatch;
}

int32_t* MakeInts(const int32_t size, const int32_t start = 0)
{
    int32_t* arr = new int32_t[size];
    int32_t idx = 0;
    for (int32_t i = start; i < start + size; i++) {
        arr[idx++] = i;
    }
    return arr;
}

int64_t* MakeLongs(const int32_t size, const int64_t start = 0)
{
    int64_t* arr = new int64_t[size];
    int32_t idx = 0;
    for (int64_t i = start; i < start + size; i++) {
        arr[idx++] = i;
    }
    return arr;
}

double* MakeDoubles(const int32_t size, const double start = 0)
{
    double* arr = new double[size];
    int32_t idx = 0;
    for (double i = start; i < start + size; i += 1) {
        arr[idx++] = i;
    }
    return arr;
}

TEST (ProjectTest, Simple) {
    const int32_t numRows = 1000;
    int32_t* col = MakeInts(numRows);
    const int32_t numCols = 1;
    string exprs[numCols] = {"$operator$ADD(#0, 5)"};
    int32_t inputTypes[numCols] = {1};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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
    const int32_t numRows = 1000;
    int32_t* col = MakeInts(numRows, -5);
    const int32_t numCols = 1;
    string exprs[numCols] = {"$operator$SUBTRACT(#0, 500)"};
    int32_t inputTypes[numCols] = {1};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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
    const int32_t numRows = 10000;
    int64_t* col = MakeLongs(numRows, -5000);
    const int32_t numCols = 1;
    string exprs[numCols] = {"$operator$MULTIPLY(#0, 5000000)"};
    int32_t inputTypes[numCols] = {2};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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
    const int32_t numRows = 10000;
    double* col = MakeDoubles(numRows, -5000.5);
    const int32_t numCols = 1;
    string exprs[numCols] = {"$operator$DIVIDE(#0, 2)"};
    int32_t inputTypes[numCols] = {3};
    ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator* op = factory->CreateOperator();
    int64_t allData[numCols] = {(int64_t) col};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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
   const int32_t numRows = 1000;
   int32_t* col1 = MakeInts(numRows);
   int32_t* col2 = MakeInts(numRows, -100);
   int64_t* col3 = MakeLongs(numRows, -10);
   const int32_t numProject = 2;
   string exprs[numProject] = {"$operator$SUBTRACT(#0, 10)", "$operator$ADD(#2, 1)"};
   const int32_t numCols = 3;
   int32_t inputTypes[numCols] = {1, 1, 2};
   ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
   omniruntime::op::Operator* op = factory->CreateOperator();
   int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
   VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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
   const int32_t numRows = 1000;
   int32_t* col1 = MakeInts(numRows);
   int32_t* col2 = MakeInts(numRows, -100);
   int64_t* col3 = MakeLongs(numRows);
   const int32_t numProject = 2;
   string exprs[numProject] = {"$operator$MULTIPLY(#0, #1)", "IF($operator$LESS_THAN(#0, 500), 4000000000, #2)"};
   const int32_t numCols = 3;
   int32_t inputTypes[numCols] = {1, 1, 2};
   ProjectionOperatorFactory* factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
   omniruntime::op::Operator* op = factory->CreateOperator();
   int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
   VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);
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