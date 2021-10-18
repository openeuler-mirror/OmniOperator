/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include "gtest/gtest.h"
#include "../util/test_util.h"
#include "../../src/operator/filter/filter_and_project.h"
#include <iostream>
#include <vector>
#include <chrono>
#include "../../src/vector/vector_helper.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace std;

VectorBatch* CreateInput(const int32_t numRows,
                         const int32_t numCols,
                         int32_t* inputTypeIds,
                         int64_t* allData)
{
    auto *vecBatch = new VectorBatch(numCols, numRows);
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    vecBatch->NewVectors(VectorAllocatorFactory::GetGlobalAllocator(), inputTypes);
    for (int i = 0; i < numCols; ++i) {
        switch (inputTypeIds[i]) {
            case OMNI_VEC_TYPE_INT:
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_LONG:
                ((LongVector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *)allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                ((DoubleVector *)vecBatch->GetVector(i))->SetValues(0, (double *)allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_SHORT:
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_VARCHAR: {
                for (int j = 0; j < numRows; ++j) {
                    // std::cout << "row: " << j << std::endl;
                    int64_t addr = ((int64_t *)(allData[i]))[j];
                    std::string s ((char *)(addr));
                    // std::cout << "s: " << s << std::endl;
                    ((VarcharVector *)vecBatch->GetVector(i))->SetValue(j, reinterpret_cast<const uint8_t *>(s.c_str()),
                                                                        s.length());
                }
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128:
                ((Decimal128Vector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *) allData[i], numRows);
                break;
            default: {
                LogError("No such data type %d", inputTypeIds[i]);
                break;
            }
        }

    }
    return vecBatch;
}

bool CheckOutput(VectorBatch* t, const int32_t numRows, bool (*filter)(VectorBatch*, int32_t))
{
    for (int32_t i = 0; i < numRows; i++) {
        if (!filter(t, i)) {
            return false;
        }
    }
    return true;
}

// Expects 1 column of type int32
bool Filter1(VectorBatch* t, int32_t index)
{
    int n = 4;
    return ((IntVector *)t->GetVector(0))->GetValue(index) <= n;
}

// Expects 2 columns of type int32, int64
bool Filter2(VectorBatch* t, int32_t index)
{
    int32_t val1 = ((IntVector *)t->GetVector(0))->GetValue(index);
    int64_t val2 = ((LongVector *)t->GetVector(1))->GetValue(index);
    // true if both values are negative
    return val1 < 0 && val2 < 0;
}

// Expects 3 columns of type int32, int64, double
bool Filter3(VectorBatch* t, int32_t index)
{
    int n1 = 0;
    int n2 = 1;
    int n3 = 2;
    int32_t val1 = ((IntVector *)t->GetVector(n1))->GetValue(index);
    int64_t val2 = ((LongVector *)t->GetVector(n2))->GetValue(index);
    double val3 = ((DoubleVector *)t->GetVector(n3))->GetValue(index);
    // first val is multiple of 3, second val = 3 billion, third val >= 0.4.
    return val1 % 3 == 0 && val2 == (int64_t) 3e9 && val3 >= 0.4;
}

bool Filter4(VectorBatch* t, int32_t index)
{
    int n0 = 0;
    int n1 = 1;
    int n2 = 2;
    int n3 = 3;
    int v0 = 1;
    int v2 = 4800;
    double v4 = 50.8;
    int v5 = 52;
    int32_t val0 = ((IntVector *)t->GetVector(n0))->GetValue(index);
    int32_t val2 = ((IntVector *)t->GetVector(n1))->GetValue(index);
    double val4 = ((DoubleVector *)t->GetVector(n2))->GetValue(index);
    int64_t val5 = ((LongVector *)t->GetVector(n3))->GetValue(index);
    return (val0 != v0 && val2 > v2 && val4 < v4) || val5 >= v5;
}

bool Filter5(VectorBatch* t, int32_t index)
{
    int n0 = 0;
    int n2 = 2;
    int n3 = 3;
    int v0 = 24;
    int v31 = 9766;
    int v32 = 9131;
    double v21 = 0.05;
    double v22 = 0.07;
    int32_t val0 = ((IntVector *)t->GetVector(n0))->GetValue(index);
    double val2 = ((DoubleVector *)t->GetVector(n2))->GetValue(index);
    double val3 = ((DoubleVector *)t->GetVector(n3))->GetValue(index);
    return val0 < v0 && val2 >= v21 && val2 <= v22 && val3 > v31 && val3 < v32;
}

bool Filter6(VectorBatch* t, int32_t index)
{
    int n0 = 0;
    int n1 = 1;
    int n2 = 2;
    int n3 = 3;
    int64_t v0 = 0;
    int64_t v1 = -3e9;
    int32_t v2 = -12;
    int32_t v3 = 50;
    // Project order reversed
    int64_t val0 = ((LongVector *)t->GetVector(n0))->GetValue(index);
    int64_t val1 = ((LongVector *)t->GetVector(n1))->GetValue(index);
    int32_t val2 = ((IntVector *)t->GetVector(n2))->GetValue(index);
    int32_t val3 = ((IntVector *)t->GetVector(n3))->GetValue(index);
    return (val0 >= v0 || val1 <= v1) && (val2 == v2 || val3 < v3);
}

// Expects 1 column of type Decimal128
bool Filter7(VectorBatch* t, int32_t index)
{
    int32_t n = 500000;
    Decimal128 val = ((Decimal128Vector *)t->GetVector(0))->GetValue(index);
    return Decimal128(val.HighBits(), val.LowBits()) <= n;
}

TEST(FilterTest, LessThan) {
    const int32_t numCols = 1;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;

    const int32_t numRows = 5000;
    int32_t* col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t allData[numCols] = {(int64_t) col1};
    const int32_t projectCount = 1;
    std::string projections[projectCount] = {"#0"};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$LESS_THAN:boolean(#0, 2000)",
                                                                   inputTypes, numCols,
                                                                   projections,
                                                                   projectCount);
    std::cout << "factory created" << std::endl;
    omniruntime::op::Operator* op = factory->CreateOperator();
    std::cout << "operator created" << std::endl;

    op->AddInput(in1);
    std::cout << "addinput completed" << std::endl;
    int32_t numReturned = op->GetOutput(ret);
    std::cout << "getoutput completed" << std::endl;
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

TEST(FilterTest, GreaterThan) {
    const int32_t numCols = 2;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 2;

    const int32_t numRows = 5000;
    int32_t* col1 = new int32_t[numRows];
    int64_t* col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 25;
        col2[i] = 3e9;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    const int32_t projectCount = 2;
    std::string projections[projectCount] = {"#0", "#1"};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$GREATER_THAN:boolean(#0, 20)",
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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

TEST(FilterTest, EqualTo) {
    const int32_t numCols = 3;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    inputTypes[2] = 3;

    const int32_t numRows = 5000;
    int32_t* col1 = new int32_t[numRows];
    double* col2 = new double[numRows];
    int64_t* col3 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = col3[i] = i % 100;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col3, (int64_t) col2};
    const int32_t projectCount = 2;
    std::string projections[projectCount] = {"#2", "#1"};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$EQUAL:boolean(#2, 50.0)",
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();

    std::cout << "before addinput" << std::endl;
    op->AddInput(in1);
    std::cout << "after addinput" << std::endl;
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

TEST(FilterTest, GreaterThanOrEqualTo) {
    const int32_t numCols = 2;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 1;

    const int32_t numRows = 5000;
    int32_t* col1 = new int32_t[numRows];
    int32_t* col2 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = (i * (i + 2)) % 40;
        col1[i] = i;
        if (i % 45 == 0) {
            col2[i] = 30;
        }
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    const int32_t projectCount = 1;
    std::string projections[projectCount] = {"#1"};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$GREATER_THAN_OR_EQUAL:boolean(#1, 30)",
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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

TEST(FilterTest, NotEqualTo) {
    const int32_t numCols = 1;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 3;

    const int32_t numRows = 5000;
    double* col1 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t allData[numCols] = {(int64_t) col1};
    const int32_t projectCount = 1;
    std::string projections[projectCount] = {"#0"};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$NOT_EQUAL:boolean(#0, 0)",
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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

TEST(FilterTest, AllPass) {
    const int32_t numCols = 1;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;

    const int32_t numRows = 20000;
    int32_t* col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = 9348;
    }
    int64_t allData[numCols] = {(int64_t) col1};
    const int32_t projectCount = 1;
    std::string projections[projectCount] = {"#0"};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$EQUAL:boolean(#0, 9348)",
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();

    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    std::cout << "numReturned: " << numReturned << std::endl;
    EXPECT_EQ(numReturned, 20000);

    VectorHelper::FreeVecBatch(in1);
    std::cout << "freed in1" << std::endl;
    // TODO: find out why freeing ret causes segfault
    // VectorHelper::FreeVecBatches(ret);
//    std::cout << "freed ret" << std::endl;

    delete[] inputTypes;
    delete[] col1;
    delete op;
    delete factory;
}

TEST(FilterTest, MultipleInputs) {
    const int32_t numCols = 1;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;

    const int32_t numRows = 1000;
    int32_t* data1 = new int32_t[numRows];
    int32_t* data2 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        data1[i] = i % 10;
        data2[i] = i % 6 + 1;
    }
    int64_t allData[numCols] = {(int64_t) data1};
    const int32_t projectCount = 1;
    std::string projections[projectCount] = {"#0"};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$LESS_THAN_OR_EQUAL:boolean(#0, 4)",
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();

    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter1));
    EXPECT_EQ(numReturned, 500);

    VectorHelper::FreeVecBatch(in1);

    allData[0] = (int64_t) data2;
    VectorBatch* in2 = CreateInput(numRows, numCols, inputTypes, allData);
    op->AddInput(in2);
    numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[1], numReturned, Filter1));
    EXPECT_EQ(numReturned, 668);

    VectorHelper::FreeVecBatch(in2);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete op;
    delete factory;
}

TEST(FilterTest, NegativeValues) {
    const int32_t numCols = 2;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 2;

    const int32_t numRows = 10000;
    int32_t* data1 = new int32_t[numRows];
    int64_t* data2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        data1[i] = i * i % 100 + 1;
        if (i % 5 == 0) {
            data1[i] = -data1[i];
        }
        data2[i] = i % 100 + 3e9;
        if (i % 7 == 0) {
            data2[i] = -data2[i];
        }
    }
    int64_t allData[numCols] = {(int64_t) data1, (int64_t) data2};
    const int32_t projectCount = 2;
    std::string projections[projectCount] = {"#0", "#1"};
    std::vector<VectorBatch*> ret;

    VectorBatch* in1 = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("AND:boolean($operator$LESS_THAN_OR_EQUAL:boolean(#0, -1), "
                                                                   "$operator$LESS_THAN_OR_EQUAL:boolean(#1, -1))",
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter2));
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

TEST(FilterTest, AllTypes) {

    const int32_t numCols = 3;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    inputTypes[2] = 3;

    const int32_t numRows = 1000;
    int32_t* data1 = new int32_t[numRows];
    int64_t* data2 = new int64_t[numRows];
    double* data3 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        data1[i] = i % 3;
        data2[i] = i % 2 ? 3e9 : 0;
        data3[i] = i % 10 / 10.0;
    }

    int64_t allData[numCols] = {(int64_t) data1, (int64_t) data2, (int64_t) data3};
    const int32_t projectCount = 3;
    std::string projections[projectCount] = {"#0", "#1", "#2"};
    std::vector<VectorBatch*> ret;

    VectorBatch* in1 = CreateInput(numRows, numCols, inputTypes, allData);
    std::string expr = "AND:boolean($operator$EQUAL:boolean(#0, 0), AND:boolean($operator$EQUAL:boolean(#1, 3000000000), "
                       "$operator$GREATER_THAN_OR_EQUAL:boolean(#2, 0.4)))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();

    op->AddInput(in1);


    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter3));
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

TEST(FilterTest, Compile) {
    // TPCH 6
    std::string filterExpression = "AND:boolean(AND:boolean($operator$GREATER_THAN:boolean(#3, 8766), $operator$LESS_THAN:boolean(#3, 9131)), "
                                   "AND:boolean(BETWEEN:boolean(#2, 0.05, 0.07), $operator$LESS_THAN:boolean(#0, 24.0)))";

    const int32_t numCols = 4;
    int32_t *inputTypes = new int32_t[numCols];
    inputTypes[0] = 3;
    inputTypes[1] = 1;
    inputTypes[2] = 3;
    inputTypes[3] = 3;

    const int32_t dataSize = 10000;
    double *data1 = new double[dataSize];
    int32_t *data2 = new int32_t[dataSize];
    double *data3 = new double[dataSize];
    double *data4 = new double[dataSize];
    for (int32_t i = 0; i < dataSize; ++i) {
        data4[i] = i;
        data3[i] = i % 10 / 100.0;
        data1[i] = i % 26;
        data2[i] = 6;
    }

    int64_t datas[4] = {(int64_t)data1, (int64_t)data2, (int64_t)data3, (int64_t)data4};
    std::string projections[1] = {"#0"};
    VectorBatch* t = CreateInput(dataSize, numCols, inputTypes, datas);

    OperatorFactory *factory = new FilterAndProjectOperatorFactory(filterExpression,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   1);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numSelectedRows = op->GetOutput(ret);
    EXPECT_EQ(numSelectedRows, 100);


    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete[] data3;
    delete[] data4;
    delete op;
    delete factory;
}

TEST(FilterTest, LogicalOperators1) {
    std::string expr = "OR:boolean($operator$GREATER_THAN_OR_EQUAL:boolean(#5, 52), AND:boolean($operator$LESS_THAN:boolean(#4, 50.8), "
                       "AND:boolean(AND:boolean($operator$GREATER_THAN:boolean(#2, 4800), $operator$LESS_THAN_OR_EQUAL:boolean(#1, 9990)), "
                       "AND:boolean($operator$NOT_EQUAL:boolean(#0, 1), $operator$EQUAL:boolean(#3, 3000000000)))))";

    const int32_t numCols = 6;
    int32_t* inputTypes = new int32_t[numCols];
    // int int int long double long
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;
    inputTypes[3] = 2;
    inputTypes[4] = 3;
    inputTypes[5] = 2;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    int32_t* col2 = new int32_t[numRows];
    int32_t* col3 = new int32_t[numRows];
    int64_t* col4 = new int64_t[numRows];
    double* col5 = new double[numRows];
    int64_t* col6 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 3 ? 1 : 0;
        col2[i] = col3[i] = i;
        col4[i] = i % 2 ? 2999999999 : 3e9;
        col5[i] = 50 + i / 10.0;
        col6[i] = i % 55;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3,
                                 (int64_t) col4, (int64_t) col5, (int64_t) col6};
    const int32_t projectCount = 4;
    std::string projections[projectCount] = {"#0", "#2", "#4", "#5"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 543);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter4));

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

TEST(FilterTest, LogicalOperators2) {
    std::string expr = "AND:boolean(OR:boolean($operator$LESS_THAN:boolean(#0, 50), $operator$EQUAL:boolean(#1, -12)), "
                       "OR:boolean($operator$LESS_THAN_OR_EQUAL:boolean(#2, -3000000000), $operator$GREATER_THAN_OR_EQUAL:boolean(#3, 0)))";

    const int32_t numCols = 4;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 2;
    inputTypes[3] = 2;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    int32_t* col2 = new int32_t[numRows];
    int64_t* col3 = new int64_t[numRows];
    int64_t* col4 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 100;
        col2[i] = i % 7 == 0 ? -12 : i;
        col3[i] = i % 8 == 0 ? -i - 3e9 : i + 3e9;
        col4[i] = i % 9 - 4;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3, (int64_t) col4};
    const int32_t projectCount = 4;
    std::string projections[projectCount] = {"#3", "#2", "#1", "#0"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 3498);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter6));
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

TEST(FilterTest, LogicalOperators3) {
    std::string expr = "AND:boolean($operator$NOT_EQUAL:boolean(#1, 0), OR:boolean(OR:boolean(OR:boolean($operator$EQUAL:boolean(#0, 1), "
                       "$operator$EQUAL:boolean(#0, 2)), $operator$EQUAL:boolean(#0, 3)), OR:boolean(OR:boolean(OR:boolean($operator$EQUAL:boolean(55, #0), "
                       "$operator$EQUAL:boolean(5, #0)), $operator$EQUAL:boolean(#0, 8)), $operator$EQUAL:boolean(#0, 13))))";
    const int32_t numCols = 2;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 3;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    double* col2 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
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
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    const int32_t projectCount = 2;
    std::string projections[projectCount] = {"#1", "#0"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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

TEST(FilterTest, ArithmeticAdd) {
    const int32_t numCols = 1;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 5;
    }
    int64_t allData[numCols] = {(int64_t) col1};
    const int32_t projectCount = 1;
    std::string projections[projectCount] = {"#0"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$GREATER_THAN:boolean($operator$ADD:int(#0, 1), 4)",
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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

TEST(FilterTest, ArithmeticSubtract) {
    const int32_t numCols = 2;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 2;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    int64_t* col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 10;
        col2[i] = i;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    const int32_t projectCount = 2;
    std::string projections[projectCount] = {"#0", "#1"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$LESS_THAN:boolean(0, $operator$SUBTRACT:int(#0, 5))",
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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

TEST(FilterTest, ArithmeticMultiply) {
    const int32_t numCols = 2;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 2;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    int64_t* col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 10 + 1;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    const int32_t projectCount = 2;
    std::string projections[projectCount] = {"#0", "#1"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    std::string expr = "AND:boolean($operator$EQUAL:boolean(0, $operator$MULTIPLY:int(#0, #0)), "
                       "$operator$GREATER_THAN:boolean(7, $operator$MULTIPLY:long(2, #1)))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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

TEST(FilterTest, Conditional) {
    const int32_t numCols = 3;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    int32_t* col2 = new int32_t[numRows];
    int32_t* col3 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = 50;
        col3[i] = 100;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t projectCount = 3;
    std::string projections[projectCount] = {"#0", "#1", "#2"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    std::string expr = "$operator$EQUAL:boolean(IF:int($operator$EQUAL:boolean(#0, 0), $operator$ADD:int(#1, 5), #2), 55)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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

TEST(FilterTest, Conditional2) {
    const int32_t numCols = 3;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    int32_t* col2 = new int32_t[numRows];
    int32_t* col3 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = i % 10;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t projectCount = 3;
    std::string projections[projectCount] = {"#0", "#1", "#2"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    std::string expr = "AND:boolean(IF:boolean($operator$EQUAL:boolean(#0, 0), $operator$LESS_THAN:boolean(#1, 3), "
                       "$operator$EQUAL:boolean(#1, 4)), $operator$GREATER_THAN:boolean(#2, 3))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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


TEST(FilterTest, In) {
    const int32_t numCols = 3;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    int32_t* col2 = new int32_t[numRows];
    int32_t* col3 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 10;
        col2[i] = i % 5;
        col3[i] = i % 6 + 12;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t projectCount = 3;
    std::string projections[projectCount] = {"#0", "#1", "#2"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    std::string expr = "IN(#0, 1, 3, 5)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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

TEST(FilterTest, Between) {
    const int32_t numCols = 3;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    int32_t* col2 = new int32_t[numRows];
    int32_t* col3 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 5;
        col2[i] = i % 11;
        col3[i] = (i % 21) - 3;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t projectCount = 3;
    std::string projections[projectCount] = {"#0", "#1", "#2"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    std::string expr = "BETWEEN:boolean(#1, #0, #2)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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

TEST(FilterTest, NotEqualToAbs) {
    const int32_t numCols = 1;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;

    const int32_t numRows = 100000;
    int32_t* col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i - 32435;
    }
    int64_t allData[numCols] = {(int64_t) col1};
    const int32_t projectCount = 1;
    std::string projections[projectCount] = {"#0"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    std::string expr = "$operator$NOT_EQUAL:boolean(abs:int(#0), 4)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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
TEST(FilterTest, MathFunctionFilter1) {
    const int32_t numCols = 3;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    int32_t* col2 = new int32_t[numRows];
    int32_t* col3 = new int32_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = -1;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t projectCount = 3;
    std::string projections[projectCount] = {"#0", "#1", "#2"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    std::string expr = "AND:boolean($operator$EQUAL:boolean(abs:int(#0), abs:int(#2)), $operator$EQUAL:boolean(abs:int(#0), abs:int(#1)))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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
TEST(FilterTest, MathFunctionFilter2) {
    const int32_t numCols = 3;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    inputTypes[2] = 1;

    const int32_t numRows = 10000;
    int32_t* col1 = new int32_t[numRows];
    int64_t* col2 = new int64_t[numRows];
    int32_t* col3 = new int32_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = -1;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t projectCount = 3;
    std::string projections[projectCount] = {"#0", "#1", "#2"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    std::string expr = "$operator$EQUAL:boolean(abs:double(CAST:double(#0)), abs:double(CAST:double(#1)))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
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

// String Filter and varcharvec testing
TEST(FilterTest, FilterString1) {
    vector<string*> strings;

    const int32_t numCols = 1;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_VEC_TYPE_VARCHAR;

    const int32_t numRows = 1000;
    int64_t* col1 = new int64_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
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
    int64_t allData[numCols] = {(int64_t) col1};
    const int32_t projectCount = 1;
    std::string projections[projectCount] = {"#0"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);


    std::string expr = "$operator$EQUAL:boolean(#0, 'hello')";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    std::cout << "num returned rows: " << numReturned << std::endl;
    EXPECT_EQ(numReturned, 25);

    for (int32_t i = 0; i < numReturned; i += 1000) {
        VarcharVector *vcVec = ((VarcharVector*) ret[0]->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);

        // Truncate the resulting string
        void *charArr = &actualChar;
        auto charArrCasted = static_cast<char **>(charArr);
        string actualStr (*charArrCasted, 0, len);
    }


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


TEST(FilterTest, Coalesce1) {
    const int32_t numCols = 3;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t numRows = 1000;
    int32_t* col1 = new int32_t[numRows];
    int64_t* col2 = new int64_t[numRows];
    int32_t* col3 = new int32_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = 100;
        col2[i] = 21;
        col3[i] = -1;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t projectCount = 3;
    std::string projections[projectCount] = {"#0", "#1", "#2"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2) {
            t->GetVector(1)->SetValueNull(i);
        }
        else {
            t->GetVector(1)->SetValueNotNull(i);
        }
    }

    std::string expr = "$operator$EQUAL:boolean(21, COALESCE:int(#1, #0))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 500);

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}


TEST(FilterTest, Coalesce2) {
    vector<string*> strings;

    const int32_t numCols = 1;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = OMNI_VEC_TYPE_VARCHAR;

    const int32_t numRows = 1000;
    int64_t* col1 = new int64_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        std::string *s = new std::string("hello");
        col1[i] = (int64_t)(s->c_str());
        strings.push_back(s);
    }
    int64_t allData[numCols] = {(int64_t) col1};
    const int32_t projectCount = 1;
    std::string projections[projectCount] = {"#0"};
    VectorBatch* t = CreateInput(numRows, numCols, inputTypes, allData);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2) {
            t->GetVector(0)->SetValueNull(i);
        }
        else {
            // Seemingly necessary so that the bitmap doesn't get default values
            t->GetVector(0)->SetValueNotNull(i);
        };
    }

    std::string expr = "$operator$EQUAL:boolean(COALESCE(#0, 'bye'), 'hello')";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    std::cout << "num returned rows: " << numReturned << std::endl;
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


// To run this test, ensure that externalfunctions.so has been compiled and is placed in the correct folder
// For full instructions, see the README in core/src/codegen/functions
TEST (FilterTest, DISABLED_ExternalMathFunc) {
    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 1;
    inputTypes[2] = 1;

    const int32_t NUM_ROWS = 1000;
    int32_t* col1 = new int32_t[NUM_ROWS];
    int32_t* col2 = new int32_t[NUM_ROWS];
    int32_t* col3 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 2;
        col2[i] = 2;
        col3[i] = 10;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    const int32_t PROJECT_COUNT = 3;
    std::string projections[PROJECT_COUNT] = {"#0", "#1", "#2"};
    VectorBatch* t = CreateInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    std::string expr = "$operator$EQUAL:boolean(Add1Int32(Add1Int32(#0)), IdInt32(Add1Int32(IdInt32(#1))))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projections, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 500);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_TRUE(val0 + 1 == val1);
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


// To run this test, ensure that externalfunctions.so has been compiled and is placed in the correct folder
// For full instructions, see the README in core/src/codegen/functions
TEST (FilterTest, DISABLED_ExternalStringFunc) {
    vector<string*> strings;

    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = OMNI_VEC_TYPE_VARCHAR;

    const int32_t NUM_ROWS = 1000;
    int64_t* col1 = new int64_t[NUM_ROWS];

    // column looks like:
    // hello, bye, hello, bye, hello, bye, ...
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        if (i % 2 == 0) {
            std::string *s = new std::string("hello");
            col1[i] = (int64_t)(s->c_str());
            strings.push_back(s);
        }
        else {
            if (i % 4 == 1) {
                std::string *s = new std::string("bye");
                col1[i] = (int64_t)(s->c_str());
                strings.push_back(s);
            }
            else {
                std::string *s = new std::string("asdf");
                col1[i] = (int64_t)(s->c_str());
                strings.push_back(s);
            }
        }
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    std::string projections[PROJECT_COUNT] = {"#0"};
    VectorBatch* t = CreateInput(NUM_ROWS, NUM_COLS, inputTypes, allData);


    std::string expr = "$operator$EQUAL:boolean(LengthStr(#0), 5)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projections, PROJECT_COUNT);
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


// To run this test, ensure that externalfunctions.so has been compiled and is placed in the correct folder
// For full instructions, see the README in core/src/codegen/functions
TEST (FilterTest, DISABLED_ExternalStringFunc2) {
    vector<string*> strings;

    const int32_t NUM_COLS = 1;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = OMNI_VEC_TYPE_VARCHAR;

    const int32_t NUM_ROWS = 1000;
    int64_t* col1 = new int64_t[NUM_ROWS];

    // column looks like:
    // hello, bye, hello, bye, hello, bye, ...
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        if (i % 2 == 0) {
            std::string *s = new std::string("hello");
            col1[i] = (int64_t)(s->c_str());
            strings.push_back(s);
        }
        else {
            if (i % 4 == 1) {
                std::string *s = new std::string("bye");
                col1[i] = (int64_t)(s->c_str());
                strings.push_back(s);
            }
            else {
                std::string *s = new std::string("asdf");
                col1[i] = (int64_t)(s->c_str());
                strings.push_back(s);
            }
        }
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    const int32_t PROJECT_COUNT = 1;
    std::string projections[PROJECT_COUNT] = {"#0"};
    VectorBatch* t = CreateInput(NUM_ROWS, NUM_COLS, inputTypes, allData);


    std::string expr = "$operator$EQUAL:boolean(FirstCharStr(#0), FirstCharStr('apple'))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projections, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 250);

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


// Testing multithreading
// Two operators running at once

void process(omniruntime::op::Operator* op, VectorBatch* t, std::vector<VectorBatch*> ret, int32_t* numReturned)
{
    op->AddInput(t);
    *numReturned = op->GetOutput(ret);
    VectorHelper::FreeVecBatches(ret);
    std::cout << "numSelectedRows: " << *numReturned << std::endl;
}

#include <thread>
#include <chrono>
#include <ratio>
// For testing different types
TEST (FilterTest, Multithreading) {
    const int32_t NUM_COLS = 3;
    int32_t* inputTypes = new int32_t[NUM_COLS];
    inputTypes[0] = 1;
    inputTypes[1] = 2;
    inputTypes[2] = 1;

    int32_t* inputTypes2 = new int32_t[NUM_COLS];
    inputTypes2[0] = 1;
    inputTypes2[1] = 2;
    inputTypes2[2] = 1;

    const int32_t NUM_ROWS = 100000;
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
    std::string projections[PROJECT_COUNT] = {"#0", "#1", "#2"};
    std::string projections2[PROJECT_COUNT] = {"#0", "#1", "#2"};
    VectorBatch* t = CreateInput(NUM_ROWS, NUM_COLS, inputTypes, allData);
    VectorBatch* t2 = CreateInput(NUM_ROWS, NUM_COLS, inputTypes, allData);

    std::vector<VectorBatch*> ret;
    std::vector<VectorBatch*> ret2;
    int32_t* numReturned = new int32_t;
    int32_t* numReturned2 = new int32_t;

    // find wall clock time
    auto start = std::chrono::high_resolution_clock::now();

    std::string expr = "$operator$EQUAL:boolean(abs:double(CAST:double(#0)), abs:double(CAST:double(#1)))";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr, inputTypes, NUM_COLS, projections, PROJECT_COUNT);
    omniruntime::op::Operator* op = factory->CreateOperator();
    std::thread thread1(process, op, t, ret, numReturned);


    std::string expr2 = "$operator$EQUAL:boolean(#1, 4)";
    OperatorFactory* factory2 = new FilterAndProjectOperatorFactory(expr2, inputTypes2, NUM_COLS, projections2, 3);
    omniruntime::op::Operator* op2 = factory2->CreateOperator();
    std::thread thread2(process, op2, t2, ret2, numReturned2);


    // process(op, t, ret, numReturned);
    // process(op2, t2, ret2, numReturned2);
    thread2.join();
    thread1.join();
    EXPECT_EQ(*numReturned, 20000);
    EXPECT_EQ(*numReturned2, 20000);

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Total time for multithreading test: ";
    std::cout << std::chrono::duration<double, std::milli>(end - start).count() << std::endl;

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);
    VectorHelper::FreeVecBatch(t2);
    VectorHelper::FreeVecBatches(ret2);

    delete[] inputTypes;
    delete[] inputTypes2;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete op2;
    delete factory;
    delete factory2;
    delete numReturned;
    delete numReturned2;
}

TEST(FilterTest, TestFilterDictionaryVec) {
    const int32_t numCols = 3;
    int32_t inputTypeIds[] = {1, 1, 1};

    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    IntVector *col2 = new IntVector(vecAllocator, numRows);
    IntVector *col3 = new IntVector(vecAllocator, numRows);
    int32_t ids[]= {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *dictionaryVector = new DictionaryVector(col3, ids, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
        col3->SetValue(i, (i % 21) - 3);
    }
    const int32_t projectCount = 3;
    std::string projections[projectCount] = {"#0", "#1", "#2"};
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    batch->NewVectors(vecAllocator, inputTypes);
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryVector);

    std::string expr = "BETWEEN(#1, #0, #2)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypeIds,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(batch);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 7);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i));
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, col2->GetValue(i));
        int32_t val2 = ((IntVector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, dictionaryVector->GetInt(i));
    }

    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col3;
    delete op;
    delete factory;
}

TEST(FilterTest, TestFilterDictionaryVarchar) {
    const int32_t numCols = 2;
    int32_t inputTypeIds[] = {1, 15};

    const int32_t numRows = 3;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    VarcharVector *col2 = new VarcharVector(vecAllocator, 1024, numRows);
    int32_t ids[]= {0, 1, 2};
    DictionaryVector *dictionaryVector = new DictionaryVector(col2, ids, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i * 3);
        std::string tmp = "test";
        col2->SetValue(i, reinterpret_cast<const uint8_t *>(tmp.c_str()), tmp.length());
    }
    const int32_t projectCount = 2;
    std::string projections[projectCount] = {"#0", "#1"};
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    batch->NewVectors(VectorAllocatorFactory::GetGlobalAllocator(), inputTypes);
    batch->SetVector(0, col1);
    batch->SetVector(1, dictionaryVector);

    std::string expr = "$operator$LESS_THAN:boolean(#0, 6)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypeIds,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(batch);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i));
        uint8_t *data = nullptr;
        int32_t len = ((VarcharVector *)ret[0]->GetVector(1))->GetValue(i, &data);
        std::string result(data, data + len);
        data = nullptr;
        len = dictionaryVector->GetVarchar(i, &data);
        std::string expected(data, data + len);
        EXPECT_EQ(result, expected);
    }

    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
}

TEST(FilterTest, TestFilterDictionaryVecNested) {
    const int32_t numCols = 3;
    int32_t inputTypeIds[] = {1, 1, 1};

    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    IntVector *col2 = new IntVector(vecAllocator, numRows);
    IntVector *col3 = new IntVector(vecAllocator, 3);
    int32_t data[] = {4, 5, 6};
    col3->SetValues(0, data, 3);
    int32_t ids[]= {1, 2};
    DictionaryVector *dictionaryVector = new DictionaryVector(col3, ids, 2);
    int32_t nestedIds[] = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
    DictionaryVector *dictionaryNested = new DictionaryVector(dictionaryVector, nestedIds, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
    }
    const int32_t projectCount = 3;
    std::string projections[projectCount] = {"#0", "#1", "#2"};
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    batch->NewVectors(vecAllocator, inputTypes);
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryNested);

    std::string expr = "BETWEEN(#1, #0, #2)";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypeIds,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(batch);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 6);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i));
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, col2->GetValue(i));
        int32_t val2 = ((IntVector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, dictionaryNested->GetInt(i));
    }

    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col3;
    delete dictionaryVector;
    delete op;
    delete factory;
}

TEST (FilterTest, DecimalFilterBinaryTest) {
    const int32_t numCols = 1;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 7;

    const int32_t numRows = 1000;
    int64_t* data1 = new int64_t[numRows * 2];
    int64_t* data2 = new int64_t[numRows * 2];
    for (int64_t i = 0; i < numRows; i++) {
        data1[2 * i] = (i + 1) * 1000;
        data1[2 * i + 1] = 0;
        data2[2 * i] = (i + 1) * 1;
        data2[2 * i + 1] = 0;
    }
    int64_t allData[numCols] = {(int64_t) data1};
    const int32_t projectCount = 1;
    std::string projections[projectCount] = {"#0"};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("$operator$LESS_THAN_OR_EQUAL:boolean(#0, 500000)",
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();

    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter7));
    EXPECT_EQ(numReturned, 500);

    VectorHelper::FreeVecBatch(in1);

    allData[0] = (int64_t) data2;
    VectorBatch* in2 = CreateInput(numRows, numCols, inputTypes, allData);
    op->AddInput(in2);
    numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[1], numReturned, Filter7));
    EXPECT_EQ(numReturned, 1000);

    VectorHelper::FreeVecBatch(in2);
    VectorHelper::FreeVecBatches(ret);

    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete op;
    delete factory;

}

TEST(FilterTest, DecimalFilterAbsTest) {
    const int32_t numCols = 3;
    int32_t* inputTypes = new int32_t[numCols];
    inputTypes[0] = 7;
    inputTypes[1] = 7;
    inputTypes[2] = 7;

    const int32_t numRows = 1000;
    int64_t* data1 = new int64_t[numRows * 2];
    int64_t* data2 = new int64_t[numRows * 2];
    int64_t* data3 = new int64_t[numRows * 2];
    for (int64_t i = 0; i < numRows; i++) {
        data1[2 * i] = (i + 1) * 1;
        data1[2 * i + 1] = -1000;
        data2[2 * i] = (i + 1) * 1;
        data2[2 * i + 1] = -1000;
        data3[2 * i] = (i + 1) * 1;
        data3[2 * i + 1] = -1000;
    }
    int64_t allData[numCols] = {(int64_t) data1, (int64_t) data2, (int64_t) data3};
    const int32_t projectCount = 3;
    std::string projections[projectCount] = {"#0", "#1", "#2"};
    std::vector<VectorBatch*> ret;
    VectorBatch* in1 = CreateInput(numRows, numCols, inputTypes, allData);

    OperatorFactory* factory = new FilterAndProjectOperatorFactory("AND:boolean($operator$EQUAL:boolean(abs:decimal(#0), abs:decimal(#2)), $operator$EQUAL:boolean(abs:decimal(#1), abs:decimal(#2)))",
                                                                   inputTypes,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();

    op->AddInput(in1);
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 1000);

    VectorHelper::FreeVecBatch(in1);

    delete[] inputTypes;
    delete[] data1;
    delete[] data2;
    delete[] data3;
    delete op;
    delete factory;
}

TEST(FilterTest, FilterStringWithNull) {
    vector<string*> strings;

    const int32_t numCols = 1;
    int32_t* inputTypeIds = new int32_t[numCols];
    inputTypeIds[0] = OMNI_VEC_TYPE_VARCHAR;

    const int32_t numRows = 2;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    VarcharVector *col0 = new VarcharVector(vecAllocator, 1024, numRows);
    std::string str = "hello";
    col0->SetValue(0, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    col0->SetValueNull(1);

    const int32_t projectCount = 1;
    std::string projections[projectCount] = {"#0"};

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    batch->NewVectors(vecAllocator, inputTypes);
    batch->SetVector(0, col0);


    std::string expr = "$operator$EQUAL:boolean(#0, 'hello')";
    OperatorFactory* factory = new FilterAndProjectOperatorFactory(expr,
                                                                   inputTypeIds,
                                                                   numCols,
                                                                   projections,
                                                                   projectCount);
    omniruntime::op::Operator* op = factory->CreateOperator();
    op->AddInput(batch);
    std::vector<VectorBatch*> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 1);

    for (int32_t i = 0; i < numReturned; i++) {
        VarcharVector *vcVec = ((VarcharVector*) ret[0]->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);
        std::string actualStr(actualChar, actualChar + len);
        EXPECT_EQ(actualStr, "hello");
    }


    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
}
