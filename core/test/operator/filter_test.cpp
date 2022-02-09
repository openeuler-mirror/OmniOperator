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

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace std;

VectorBatch *CreateInput(const int32_t numRows, const int32_t numCols, const int32_t *inputTypeIds, int64_t *allData)
{
    auto *vecBatch = new VectorBatch(numCols, numRows);
    vector<VecType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    vecBatch->NewVectors(VectorAllocatorFactory::GetGlobalAllocator(), inputTypes);
    for (int i = 0; i < numCols; ++i) {
        switch (inputTypeIds[i]) {
            case OMNI_VEC_TYPE_INT:
                ((IntVector *) vecBatch->GetVector(i))->SetValues(0, (int32_t *) allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_LONG:
                ((LongVector *) vecBatch->GetVector(i))->SetValues(0, (int64_t *) allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                ((DoubleVector *) vecBatch->GetVector(i))->SetValues(0, (double *) allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_SHORT:
                ((IntVector *) vecBatch->GetVector(i))->SetValues(0, (int32_t *) allData[i], numRows);
                break;
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR: {
                for (int j = 0; j < numRows; ++j) {
                    int64_t addr = ((int64_t *) (allData[i]))[j];
                    std::string s((char *) (addr));
                    ((VarcharVector *) vecBatch->GetVector(i))
                            ->SetValue(j, reinterpret_cast<const uint8_t *>(s.c_str()), s.length());
                }
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128:
                ((Decimal128Vector *) vecBatch->GetVector(i))->SetValues(0, (int64_t *) allData[i], numRows);
                break;
            default: {
                LogError("No such data type %d", inputTypeIds[i]);
                break;
            }
        }
    }
    return vecBatch;
}

bool CheckOutput(VectorBatch *t, const int32_t numRows, bool (*filter)(VectorBatch *, int32_t)) {
    for (int32_t i = 0; i < numRows; i++) {
        if (!filter(t, i)) {
            return false;
        }
    }
    return true;
}

// Expects 1 column of type int32
bool Filter1(VectorBatch *t, int32_t index) {
    int n = 4;
    return ((IntVector *) t->GetVector(0))->GetValue(index) <= n;
}

// Expects 2 columns of type int32, int64
bool Filter2(VectorBatch *t, int32_t index) {
    int32_t val1 = ((IntVector *) t->GetVector(0))->GetValue(index);
    int64_t val2 = ((LongVector *) t->GetVector(1))->GetValue(index);
    // true if both values are negative
    return val1 < 0 && val2 < 0;
}

// Expects 3 columns of type int32, int64, double
bool Filter3(VectorBatch *t, int32_t index) {
    int n1 = 0;
    int n2 = 1;
    int n3 = 2;
    int32_t val1 = ((IntVector *) t->GetVector(n1))->GetValue(index);
    int64_t val2 = ((LongVector *) t->GetVector(n2))->GetValue(index);
    double val3 = ((DoubleVector *) t->GetVector(n3))->GetValue(index);
    // first val is multiple of 3, second val = 3 billion, third val >= 0.4.
    return val1 % 3 == 0 && val2 == (int64_t) 3e9 && val3 >= 0.4;
}

bool Filter4(VectorBatch *t, int32_t index) {
    int n0 = 0;
    int n1 = 1;
    int n2 = 2;
    int n3 = 3;
    int v0 = 1;
    int v2 = 4800;
    double v4 = 50.8;
    int v5 = 52;
    int32_t val0 = ((IntVector *) t->GetVector(n0))->GetValue(index);
    int32_t val2 = ((IntVector *) t->GetVector(n1))->GetValue(index);
    double val4 = ((DoubleVector *) t->GetVector(n2))->GetValue(index);
    int64_t val5 = ((LongVector *) t->GetVector(n3))->GetValue(index);
    return (val0 != v0 && val2 > v2 && val4 < v4) || val5 >= v5;
}

bool Filter5(VectorBatch *t, int32_t index) {
    int n0 = 0;
    int n2 = 2;
    int n3 = 3;
    int v0 = 24;
    int v31 = 9766;
    int v32 = 9131;
    double v21 = 0.05;
    double v22 = 0.07;
    int32_t val0 = ((IntVector *) t->GetVector(n0))->GetValue(index);
    double val2 = ((DoubleVector *) t->GetVector(n2))->GetValue(index);
    double val3 = ((DoubleVector *) t->GetVector(n3))->GetValue(index);
    return val0 < v0 && val2 >= v21 && val2 <= v22 && val3 > v31 && val3 < v32;
}

bool Filter6(VectorBatch *t, int32_t index) {
    int n0 = 0;
    int n1 = 1;
    int n2 = 2;
    int n3 = 3;
    int64_t v0 = 0;
    int64_t v1 = -3e9;
    int32_t v2 = -12;
    int32_t v3 = 50;
    // Project order reversed
    int64_t val0 = ((LongVector *) t->GetVector(n0))->GetValue(index);
    int64_t val1 = ((LongVector *) t->GetVector(n1))->GetValue(index);
    int32_t val2 = ((IntVector *) t->GetVector(n2))->GetValue(index);
    int32_t val3 = ((IntVector *) t->GetVector(n3))->GetValue(index);
    return (val0 >= v0 || val1 <= v1) && (val2 == v2 || val3 < v3);
}

// Expects 1 column of type Decimal128
bool Filter7(VectorBatch *t, int32_t index) {
    int32_t n = 500000;
    Decimal128 val = ((Decimal128Vector *) t->GetVector(0))->GetValue(index);
    return Decimal128(val.HighBits(), val.LowBits()) <= n;
}

TEST(FilterTest, LessThan) {
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    VecTypes inputTypes(std::vector<VecType>({IntVecType()}));
    int64_t allData[numCols] = {(int64_t) col1};
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;
    std::vector<Expr*> projections = {new FieldExpr(0, IntType())};
    BinaryExpr *filterExpr = new BinaryExpr(LT, new FieldExpr(0, IntType()), new LiteralExpr(2000, IntType()), BooleanType());
    OperatorFactory *factory = new FilterAndProjectOperatorFactory(filterExpr, inputTypes,
                                                                   numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val < 2000);
    }
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}

TEST(FilterTest, LessThanWihtoutParsing) {
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    VecTypes inputTypes(std::vector<VecType>({IntVecType()}));
    int64_t allData[numCols] = {(int64_t) col1};
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;
    FieldExpr *column = new FieldExpr(0, IntType());
    FieldExpr *left = new FieldExpr(0, IntType());
    LiteralExpr *right = new LiteralExpr(2000, IntType());
    BinaryExpr *LTExpr = new BinaryExpr(LT, left, right);

    std::vector<Expr*> projections = {column};
    OperatorFactory *factory = new FilterAndProjectOperatorFactory(LTExpr, inputTypes,
                                                                   numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();


    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val < 2000);
    }
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}

TEST(FilterTest, GreaterThan) {
    const int32_t numCols = 2;
    const int32_t numRows = 5000;
    int32_t *col1 = new int32_t[numRows];
    int64_t *col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 25;
        col2[i] = 3e9;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), LongVecType()}));
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 2;
    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col1Expr = new FieldExpr(1, LongType());
    std::vector<Expr*> projections = {col0Expr, col1Expr};

    FieldExpr *gtLeft = new FieldExpr(0, IntType());
    LiteralExpr *gtRight = new LiteralExpr(20, IntType());
    BinaryExpr *gtExpr = new BinaryExpr(GT, gtLeft, gtRight, BooleanType());

    OperatorFactory *factory = new FilterAndProjectOperatorFactory(gtExpr, inputTypes, numCols, projections,
                                                                   projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 800);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_TRUE(val0 > 20);
        EXPECT_EQ(val1, (int64_t) 3e9);
    }
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST(FilterTest, EqualTo) {
    const int32_t numCols = 3;
    const int32_t numRows = 5000;
    int32_t *col1 = new int32_t[numRows];
    double *col2 = new double[numRows];
    int64_t *col3 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = col3[i] = i % 100;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col3, (int64_t) col2};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), LongVecType(), DoubleVecType()}));
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 2;

    FieldExpr *col1Expr = new FieldExpr(1, LongType());
    FieldExpr *col2Expr = new FieldExpr(2, DoubleType());
    std::vector<Expr*> projections = {col2Expr, col1Expr};

    FieldExpr *eqLeft = new FieldExpr(2, DoubleType());
    LiteralExpr *eqRight = new LiteralExpr(50, IntType());
    eqRight->doubleVal = 50;

    BinaryExpr *eqExpr = new BinaryExpr(EQ, eqLeft, eqRight, BooleanType());

    OperatorFactory *factory = new FilterAndProjectOperatorFactory(eqExpr, inputTypes, numCols,
                                                                   projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 50);
    for (int32_t i = 0; i < numReturned; i++) {
        double val0 = ((DoubleVector *) ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, 50);
        EXPECT_EQ(val0, val1);
    }
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

TEST(FilterTest, GreaterThanOrEqualTo) {
    const int32_t numCols = 2;
    const int32_t numRows = 5000;
    int32_t *col1 = new int32_t[numRows];
    int32_t *col2 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = (i * (i + 2)) % 40;
        col1[i] = i;
        if (i % 45 == 0) {
            col2[i] = 30;
        }
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType()}));
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;

    FieldExpr *col1Expr = new FieldExpr(1, IntType());
    std::vector<Expr*> projections = {col1Expr};

    FieldExpr *gteLeft = new FieldExpr(1, IntType());
    LiteralExpr *gteRight = new LiteralExpr(30, IntType());
    BinaryExpr *gteExpr = new BinaryExpr(GTE, gteLeft, gteRight, BooleanType());

    OperatorFactory *factory = new FilterAndProjectOperatorFactory(gteExpr, inputTypes, numCols, projections,
                                                                   projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 834);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 >= 30);
    }
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST(FilterTest, NotEqualTo) {
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    double *col1 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    int64_t allData[numCols] = {(int64_t) col1};
    VecTypes inputTypes(std::vector<VecType>({DoubleVecType()}));
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;
    FieldExpr *col0Expr = new FieldExpr(0, DoubleType());
    std::vector<Expr*> projections = {col0Expr};

    FieldExpr *neqLeft = new FieldExpr(0, DoubleType());
    LiteralExpr *neqRight = new LiteralExpr(0, IntType());
    neqRight->doubleVal = 0;

    BinaryExpr *neqExpr = new BinaryExpr(NEQ, neqLeft, neqRight, BooleanType());

    OperatorFactory *factory = new FilterAndProjectOperatorFactory(neqExpr, inputTypes, numCols,
                                                                   projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4999);
    double cnt = 1;
    for (int32_t i = 0; i < numReturned; i++) {
        double val0 = ((DoubleVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, cnt++);
    }
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}

TEST(FilterTest, AllPass) {
    const int32_t numCols = 1;
    const int32_t numRows = 20000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = 9348;
    }
    int64_t allData[numCols] = {(int64_t) col1};
    VecTypes inputTypes(std::vector<VecType>({IntVecType()}));
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;

    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    std::vector<Expr*> projections = {col0Expr};

    FieldExpr *eqLeft = new FieldExpr(0, IntType());
    LiteralExpr *eqRight = new LiteralExpr(9348, IntType());
    BinaryExpr *eqExpr = new BinaryExpr(EQ, eqLeft, eqRight, BooleanType());
    OperatorFactory *factory = new FilterAndProjectOperatorFactory(eqExpr, inputTypes, numCols,
                                                                   projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 20000);

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}

TEST(FilterTest, MultipleInputs) {
    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    int32_t *data1 = new int32_t[numRows];
    int32_t *data2 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        data1[i] = i % 10;
        data2[i] = i % 6 + 1;
    }
    int64_t allData[numCols] = {(int64_t) data1};
    VecTypes inputTypes(std::vector<VecType>({IntVecType()}));
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;

        FieldExpr *col0Expr = new FieldExpr(0, IntType());
    std::vector<Expr*> projections = {col0Expr};

    FieldExpr *lteLeft = new FieldExpr(0, IntType());
    LiteralExpr *lteRight = new LiteralExpr(4, IntType());
    BinaryExpr *lteExpr = new BinaryExpr(LTE, lteLeft, lteRight, BooleanType());

    OperatorFactory *factory = new FilterAndProjectOperatorFactory(lteExpr, inputTypes,
                                                                   numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter1));
    EXPECT_EQ(numReturned, 500);

    allData[0] = (int64_t) data2;
    VectorBatch *in2 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(in2);
    numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[1], numReturned, Filter1));
    EXPECT_EQ(numReturned, 668);

    VectorHelper::FreeVecBatches(ret);

    delete[] data1;
    delete[] data2;
    delete op;
    delete factory;
}

TEST(FilterTest, NegativeValues) {
    const int32_t numCols = 2;
    const int32_t numRows = 10000;
    int32_t *data1 = new int32_t[numRows];
    int64_t *data2 = new int64_t[numRows];
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
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), LongVecType()}));
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 2;

        FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col1Expr = new FieldExpr(1, LongType());
    std::vector<Expr*> projections = {col0Expr, col1Expr};

    // create the filter expression object
    FieldExpr *lte1Left = new FieldExpr(0, IntType());
    LiteralExpr * lte1Right = new LiteralExpr(-1, IntType());
    BinaryExpr *lte1Expr = new BinaryExpr(LTE, lte1Left, lte1Right, BooleanType());

    FieldExpr *lte2Left = new FieldExpr(1, LongType());
    LiteralExpr * lte2Right = new LiteralExpr(-1, IntType());
    lte2Right->longVal = -1;
    BinaryExpr *lte2Expr = new BinaryExpr(LTE, lte2Left, lte2Right, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(AND, lte1Expr, lte2Expr, BooleanType());

    OperatorFactory *factory = new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections,
                                                                   projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter2));
    // Both values are negative for every multiple of 35.
    EXPECT_EQ(numReturned, 286);

    VectorHelper::FreeVecBatches(ret);

    delete[] data1;
    delete[] data2;
    delete op;
    delete factory;
}

TEST(FilterTest, AllTypes) {
    const int32_t numCols = 3;
    const int32_t numRows = 1000;
    int32_t *data1 = new int32_t[numRows];
    int64_t *data2 = new int64_t[numRows];
    double *data3 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        data1[i] = i % 3;
        data2[i] = i % 2 ? 3e9 : 0;
        data3[i] = i % 10 / 10.0;
    }

    int64_t allData[numCols] = {(int64_t) data1, (int64_t) data2, (int64_t) data3};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), LongVecType(), DoubleVecType()}));
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 3;
    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col1Expr = new FieldExpr(1, LongType());
    FieldExpr *col2Expr = new FieldExpr(2, DoubleType());
    std::vector<Expr*> projections = {col0Expr, col1Expr, col2Expr};

    // create the filter expression object
    FieldExpr *eq2Left = new FieldExpr(1, LongType());
    LiteralExpr * eq2Right = new LiteralExpr(3000000000, LongType());
    eq2Right->longVal = 3000000000;
    BinaryExpr *eq2Expr = new BinaryExpr(EQ, eq2Left, eq2Right, BooleanType());

    FieldExpr *gteLeft = new FieldExpr(2, DoubleType());
    LiteralExpr * gteRight = new LiteralExpr(0.4, DoubleType());
    gteRight->doubleVal = 0.4;
    BinaryExpr *gteExpr = new BinaryExpr(GTE, gteLeft, gteRight, BooleanType());

    BinaryExpr *innerAndExpr = new BinaryExpr(AND, eq2Expr, gteExpr, BooleanType());

    FieldExpr *eq1Left = new FieldExpr(0, IntType());
    LiteralExpr * eq1Right = new LiteralExpr(0, IntType());
    BinaryExpr *eq1Expr = new BinaryExpr(EQ, eq1Left, eq1Right, BooleanType());

    Expr *filterExpr = new BinaryExpr(AND, eq1Expr, innerAndExpr, BooleanType());
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter3));
    EXPECT_EQ(numReturned, 100);

    VectorHelper::FreeVecBatches(ret);

    delete[] data1;
    delete[] data2;
    delete[] data3;
    delete op;
    delete factory;
}

TEST(FilterTest, Compile) {
    const int32_t numCols = 4;
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

    int64_t datas[4] = {(int64_t) data1, (int64_t) data2, (int64_t) data3, (int64_t) data4};
    VecTypes inputTypes(std::vector<VecType>({DoubleVecType(), IntVecType(), DoubleVecType(), DoubleVecType()}));
    VectorBatch *t = CreateInput(dataSize, numCols, inputTypes.GetIds(), datas);
    // TPCH 6
    FieldExpr *col0Expr = new FieldExpr(0, DoubleType());
    std::vector<Expr*> projections = {col0Expr};

    LiteralExpr *gtRight = new LiteralExpr(8766, DoubleType());
    gtRight->doubleVal = 8766;
    BinaryExpr *gtExpr = new BinaryExpr(GT, new FieldExpr(3, DoubleType()), gtRight, BooleanType());

    LiteralExpr *lt1Right = new LiteralExpr(9131, DoubleType());
    lt1Right->doubleVal = 9131;
    BinaryExpr *lt1Expr = new BinaryExpr(LT, new FieldExpr(3, DoubleType()), lt1Right, BooleanType());
    BinaryExpr *and1Expression = new BinaryExpr(AND, gtExpr, lt1Expr, BooleanType());

    LiteralExpr *lt2Right = new LiteralExpr(24, DoubleType());
    lt2Right->doubleVal = 24.0;
    BinaryExpr *lt2expr = new BinaryExpr(LT, new FieldExpr(0, DoubleType()), lt2Right, BooleanType());

    FieldExpr *data = new FieldExpr(2, DoubleType());
    LiteralExpr *lower = new LiteralExpr(0.05, DoubleType());
    lower->doubleVal = 0.05;
    LiteralExpr *upper = new LiteralExpr(0.07, DoubleType());
    upper->doubleVal = 0.07;
    std::vector<Expr *> args;
    BetweenExpr *betweenExpr = new BetweenExpr(data, lower, upper);
    BinaryExpr *and2Expression = new BinaryExpr(AND, betweenExpr, lt2expr, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(AND, and1Expression, and2Expression, BooleanType());

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, 1);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numSelectedRows = op->GetOutput(ret);
    EXPECT_EQ(numSelectedRows, 100);

    VectorHelper::FreeVecBatches(ret);

    delete[] data1;
    delete[] data2;
    delete[] data3;
    delete[] data4;
    delete op;
    delete factory;
}

TEST(FilterTest, LogicalOperators1) {
    const int32_t numCols = 6;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int32_t *col2 = new int32_t[numRows];
    int32_t *col3 = new int32_t[numRows];
    int64_t *col4 = new int64_t[numRows];
    double *col5 = new double[numRows];
    int64_t *col6 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 3 ? 1 : 0;
        col2[i] = col3[i] = i;
        col4[i] = i % 2 ? 2999999999 : 3e9;
        col5[i] = 50 + i / 10.0;
        col6[i] = i % 55;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3,
                                (int64_t) col4, (int64_t) col5, (int64_t) col6};
    // int int int long double long
    VecTypes inputTypes(std::vector<VecType>(
            {IntVecType(), IntVecType(), IntVecType(), LongVecType(), DoubleVecType(), LongVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
    const int32_t projectCount = 4;
    // projection objects:
    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col2Expr = new FieldExpr(2, IntType());
    FieldExpr *col4Expr = new FieldExpr(4, DoubleType());
    FieldExpr *col5Expr = new FieldExpr(5, LongType());
    std::vector<Expr*> projections = {col0Expr, col2Expr, col4Expr, col5Expr};

    LiteralExpr *eqRight = new LiteralExpr(3000000000, LongType());
    eqRight->longVal = 3000000000;
    BinaryExpr *eqExpr = new BinaryExpr(EQ, new FieldExpr(3, LongType()), eqRight, BooleanType());
    BinaryExpr *neqExpr = new BinaryExpr(NEQ, new FieldExpr(0, IntType()), new LiteralExpr(1, IntType()), BooleanType());
    BinaryExpr *andExpr1 = new BinaryExpr(AND, neqExpr, eqExpr, BooleanType());

    BinaryExpr *gtExpr = new BinaryExpr(GT, new FieldExpr(2, IntType()), new LiteralExpr(4800, IntType()), BooleanType());
    BinaryExpr *lteExpr = new BinaryExpr(LTE, new FieldExpr(1, IntType()), new LiteralExpr(9990, IntType()), BooleanType());
    BinaryExpr *andExpr2 = new BinaryExpr(AND, gtExpr, lteExpr, BooleanType());

    BinaryExpr *andExpr3 = new BinaryExpr(AND, andExpr2, andExpr1, BooleanType());

        LiteralExpr *ltRight = new LiteralExpr(50.8, DoubleType());
    ltRight->doubleVal = 50.8;
    BinaryExpr *ltExpr = new BinaryExpr(LT, new FieldExpr(4, DoubleType()), ltRight, BooleanType());
    BinaryExpr *andExpr4 = new BinaryExpr(AND, ltExpr, andExpr3, BooleanType());

    LiteralExpr *gteRight = new LiteralExpr(52, LongType());
    gteRight->longVal = 52;
    BinaryExpr *gteExpr = new BinaryExpr(GTE, new FieldExpr(5, LongType()), gteRight, BooleanType());
    BinaryExpr *filterExpr = new BinaryExpr(OR, gteExpr, andExpr4, BooleanType());

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 543);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter4));

    VectorHelper::FreeVecBatches(ret);

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
    const int32_t numCols = 4;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int32_t *col2 = new int32_t[numRows];
    int64_t *col3 = new int64_t[numRows];
    int64_t *col4 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 100;
        col2[i] = i % 7 == 0 ? -12 : i;
        col3[i] = i % 8 == 0 ? -i - 3e9 : i + 3e9;
        col4[i] = i % 9 - 4;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3, (int64_t) col4};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), LongVecType(), LongVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
    const int32_t projectCount = 4;

    // projections
    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col1Expr = new FieldExpr(1, IntType());
    FieldExpr *col2Expr = new FieldExpr(2, LongType());
    FieldExpr *col3Expr = new FieldExpr(3, LongType());

    std::vector<Expr*> projections = {col3Expr, col2Expr, col1Expr, col0Expr};

    LiteralExpr *lteRight = new LiteralExpr(-3000000000, LongType());
    lteRight->longVal = -3000000000;
    BinaryExpr *lteExpr = new BinaryExpr(LTE, new FieldExpr(2, LongType()), lteRight, BooleanType());
    LiteralExpr *gteRight = new LiteralExpr(-0, LongType());
    gteRight->longVal = 0;
    BinaryExpr *gteExpr = new BinaryExpr(GTE, new FieldExpr(3, LongType()), gteRight, BooleanType());
    BinaryExpr *orExpr1 = new BinaryExpr(OR, lteExpr, gteExpr, BooleanType());

    BinaryExpr *ltExpr = new BinaryExpr(LT, new FieldExpr(0, IntType()), new LiteralExpr(50, IntType()), BooleanType());
    BinaryExpr *eqExpr = new BinaryExpr(EQ, new FieldExpr(1, IntType()), new LiteralExpr(-12, IntType()), BooleanType());
    BinaryExpr *orExpr2 = new BinaryExpr(OR, ltExpr, eqExpr, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(AND, orExpr1, orExpr2, BooleanType());

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 3498);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter6));
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete op;
    delete factory;
}

TEST(FilterTest, LogicalOperators3) {
    const int32_t numCols = 2;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    double *col2 = new double[numRows];
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
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), DoubleVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
    const int32_t projectCount = 2;

    // projections
    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col1Expr = new FieldExpr(1, IntType());
    std::vector<Expr*> projections = {col1Expr, col0Expr};

    BinaryExpr *eq1Expr = new BinaryExpr(EQ, new LiteralExpr(55, IntType()), new FieldExpr(0, IntType()), BooleanType());
    BinaryExpr *eq2Expr = new BinaryExpr(EQ, new LiteralExpr(5, IntType()), new FieldExpr(0, IntType()), BooleanType());
    BinaryExpr *or1Expr = new BinaryExpr(OR, eq1Expr, eq2Expr, BooleanType());

    BinaryExpr *eq3Expr = new BinaryExpr(EQ, new FieldExpr(0, IntType()), new LiteralExpr(8, IntType()), BooleanType());
    BinaryExpr *or2Expr = new BinaryExpr(OR, or1Expr, eq3Expr, BooleanType());

    BinaryExpr *eq4Expr = new BinaryExpr(EQ, new FieldExpr(0, IntType()), new LiteralExpr(13, IntType()), BooleanType());
    BinaryExpr *or3Expr = new BinaryExpr(OR, or2Expr, eq4Expr, BooleanType());


    BinaryExpr *eq5Expr = new BinaryExpr(EQ, new FieldExpr(0, IntType()), new LiteralExpr(1, IntType()), BooleanType());
    BinaryExpr *eq6Expr = new BinaryExpr(EQ, new FieldExpr(0, IntType()), new LiteralExpr(2, IntType()), BooleanType());
    BinaryExpr *or4Expr = new BinaryExpr(OR, eq5Expr, eq6Expr, BooleanType());

    BinaryExpr *eq7Expr = new BinaryExpr(EQ, new FieldExpr(0, IntType()), new LiteralExpr(3, IntType()), BooleanType());
    BinaryExpr *or5Expr = new BinaryExpr(OR, or4Expr, eq7Expr, BooleanType());

    BinaryExpr *or6Expr = new BinaryExpr(OR, or5Expr, or3Expr, BooleanType());

    LiteralExpr *neqRight = new LiteralExpr(0, LongType());
    neqRight->longVal = 0;
    BinaryExpr *neqExpr = new BinaryExpr(NEQ, new FieldExpr(1, LongType()), neqRight, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(AND, neqExpr, or6Expr, BooleanType());

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 6);
    for (int32_t i = 0; i < 6; i++) {
        double val0 = ((DoubleVector *) ret[0]->GetVector(0))->GetValue(i);
        int32_t val1 = ((IntVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_TRUE(val0 != 0);
        EXPECT_TRUE(val1 == col1[i + 2]);
    }
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST(FilterTest, ArithmeticAdd) {
    const int32_t numCols = 1;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 5;
    }
    int64_t allData[numCols] = {(int64_t) col1};
    VecTypes inputTypes(std::vector<VecType>({IntVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;

    std::vector<Expr*> projections = {new FieldExpr(0, IntType())};

    // filter
    BinaryExpr *addExpr = new BinaryExpr(ADD, new FieldExpr(0, IntType()), new LiteralExpr(1, IntType()), IntType());
    BinaryExpr *filterExpr = new BinaryExpr(GT, addExpr, new LiteralExpr(4, IntType()), BooleanType());
    OperatorFactory *factory = new FilterAndProjectOperatorFactory( filterExpr, inputTypes, numCols, projections,
                                                                    projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 + 1 > 4);
    }
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}

TEST(FilterTest, ArithmeticSubtract) {
    const int32_t numCols = 2;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int64_t *col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 10;
        col2[i] = i;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), LongVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 2;
    std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, LongType())};

    BinaryExpr *subExpr = new BinaryExpr(SUB, new FieldExpr(0, IntType()), new LiteralExpr(5, IntType()), IntType());
    BinaryExpr *filterExpr = new BinaryExpr(LT, new LiteralExpr(0, IntType()), subExpr, BooleanType());

    OperatorFactory *factory = new FilterAndProjectOperatorFactory( filterExpr, inputTypes, numCols, projections,
                                                                    projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(0 < val0 - 5);
    }

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST(FilterTest, ArithmeticMultiply) {
    const int32_t numCols = 2;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int64_t *col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 10 + 1;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), LongVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 2;

    BinaryExpr *mul1Expr = new BinaryExpr(MUL, new FieldExpr(0, IntType()), new FieldExpr(0, IntType()), IntType());
    BinaryExpr *eqExpr = new BinaryExpr(EQ, new LiteralExpr(0, IntType()), mul1Expr, BooleanType());

    LiteralExpr *mulLeft= new LiteralExpr(2, LongType());
    mulLeft->longVal = 2;
    BinaryExpr *mul2Expr = new BinaryExpr(MUL, mulLeft, new FieldExpr(1, LongType()), LongType());
    LiteralExpr *gtLeft= new LiteralExpr(7, LongType());
    gtLeft->longVal = 7;
    BinaryExpr *gtExpr=  new BinaryExpr(GT, gtLeft, mul2Expr, BooleanType());
    BinaryExpr *filterExpr = new BinaryExpr(AND, eqExpr, gtExpr, BooleanType());

    std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, LongType())};

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, 0);
        EXPECT_TRUE(val1 * 2 < 7);
    }
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
}

TEST(FilterTest, Conditional) {
    const int32_t numCols = 3;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int32_t *col2 = new int32_t[numRows];
    int32_t *col3 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = 50;
        col3[i] = 100;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), IntVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
    const int32_t projectCount = 3;

    std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, IntType()), new FieldExpr(2, IntType())};

    // filters
    BinaryExpr *condition = new BinaryExpr(EQ, new FieldExpr(0, IntType()), new LiteralExpr(0, IntType()), BooleanType());
    BinaryExpr *texp = new BinaryExpr(ADD, new FieldExpr(1, IntType()), new LiteralExpr(5, IntType()), IntType());
    FieldExpr *fexp = new FieldExpr(2, IntType());

    IfExpr *eqLeft = new IfExpr(condition, texp, fexp);

    BinaryExpr *filterExpr = new BinaryExpr(EQ, eqLeft, new LiteralExpr(55, IntType()), BooleanType());

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 5000);

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

TEST(FilterTest, Conditional2) {
    const int32_t numCols = 3;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int32_t *col2 = new int32_t[numRows];
    int32_t *col3 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = i % 10;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), IntVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    // filters
    BinaryExpr *condition =  new BinaryExpr(EQ, new FieldExpr(0, IntType()), new LiteralExpr(0, IntType()), BooleanType());
    BinaryExpr *texp =  new BinaryExpr(LT, new FieldExpr(1, IntType()), new LiteralExpr(3, IntType()), BooleanType());
    BinaryExpr *fexp =  new BinaryExpr(EQ, new FieldExpr(1, IntType()), new LiteralExpr(4, IntType()), BooleanType());
    IfExpr *ifExpr = new IfExpr(condition, texp, fexp);

    BinaryExpr *gtExpr = new BinaryExpr(GT, new FieldExpr(2, IntType()), new LiteralExpr(3, IntType()), BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(AND, ifExpr, gtExpr, BooleanType());

    //filters
    const int32_t projectCount = 3;
        std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, IntType()), new FieldExpr(2, IntType())};

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}


TEST(FilterTest, In) {
    const int32_t numCols = 3;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int32_t *col2 = new int32_t[numRows];
    int32_t *col3 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 10;
        col2[i] = i % 5;
        col3[i] = i % 6 + 12;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), IntVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
    //filter
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, IntType()));
    args.push_back(new LiteralExpr(1, IntType()));
    args.push_back(new LiteralExpr(3, IntType()));
    args.push_back(new LiteralExpr(5, IntType()));

    InExpr *filterExpr = new InExpr(args);

    const int32_t projectCount = 3;
        std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, IntType()), new FieldExpr(2, IntType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 3000);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 == 1 || val0 == 3 || val0 == 5);
    }

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

TEST(FilterTest, Between) {
    const int32_t numCols = 3;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int32_t *col2 = new int32_t[numRows];
    int32_t *col3 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 5;
        col2[i] = i % 11;
        col3[i] = (i % 21) - 3;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), IntVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    BetweenExpr *filterExpr = new BetweenExpr(new FieldExpr(1, IntType()), new FieldExpr(0, IntType()), new FieldExpr(2, IntType()));
    const int32_t projectCount = 3;
    std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, IntType()), new FieldExpr(2, IntType())};

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4705);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        int32_t val1 = ((IntVector *) ret[0]->GetVector(1))->GetValue(i);
        int32_t val2 = ((IntVector *) ret[0]->GetVector(2))->GetValue(i);
        EXPECT_TRUE((val0 <= val1) && (val1 <= val2));
    }

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

TEST(FilterTest, NotEqualToAbs) {
    const int32_t numCols = 1;
    const int32_t numRows = 100000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i - 32435;
    }
    int64_t allData[numCols] = {(int64_t) col1};
    VecTypes inputTypes(std::vector<VecType>({IntVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    //filter
    ParserHelper ph;
    FunctionRegistry fr;
    VecTypePtr retType = IntType();
    std::string funcStr = "abs";
    std::vector<Expr *> args;
        args.push_back(new FieldExpr(0, IntType()));
    std::string funcID = ph.GetFnIdentifier(funcStr, args, retType->GetId());
    FuncExpr *absExpr = new FuncExpr(funcStr, args, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));

    BinaryExpr *filterExpr = new BinaryExpr(NEQ, absExpr, new LiteralExpr(4, IntType()), BooleanType());
    const int32_t projectCount = 1;
    std::vector<Expr*> projections = {new FieldExpr(0, IntType())};

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 99998);

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}


// Function tests
TEST(FilterTest, MathFunctionFilter1) {
    const int32_t numCols = 3;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int32_t *col2 = new int32_t[numRows];
    int32_t *col3 = new int32_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = -1;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), IntVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    //filters
    ParserHelper ph;
    FunctionRegistry fr;
    VecTypePtr retType = IntType();
    std::string funcStr = "abs";
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, IntType()));
    std::string funcID = ph.GetFnIdentifier(funcStr, args1, retType->GetId());
    FuncExpr *abs1Expr = new FuncExpr(funcStr, args1, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(2, IntType()));
    funcID = ph.GetFnIdentifier(funcStr, args2, retType->GetId());
    FuncExpr *abs2Expr = new FuncExpr(funcStr, args2, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));
    BinaryExpr *eq1Expr = new BinaryExpr(EQ, abs1Expr, abs2Expr, BooleanType());

    std::vector<Expr *> args3;
    args3.push_back(new FieldExpr(0, IntType()));
    funcID = ph.GetFnIdentifier(funcStr, args3, retType->GetId());
    FuncExpr *abs3Expr = new FuncExpr(funcStr, args3, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));

    std::vector<Expr *> args4;
    args4.push_back(new FieldExpr(1, IntType()));
    funcID = ph.GetFnIdentifier(funcStr, args4, retType->GetId());
    FuncExpr *abs4Expr = new FuncExpr(funcStr, args4, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));
    BinaryExpr *eq2Expr = new BinaryExpr(EQ, abs3Expr, abs4Expr, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(AND, eq1Expr, eq2Expr, BooleanType());

    const int32_t projectCount = 3;
        std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
                                          new FieldExpr(2, IntType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 1000);
    std::cout << "numReturned: " << numReturned << std::endl;
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        int32_t val1 = ((IntVector *) ret[0]->GetVector(1))->GetValue(i);
        int32_t val2 = ((IntVector *) ret[0]->GetVector(2))->GetValue(i);
        EXPECT_TRUE((std::abs(val0) == std::abs(val1)) && (std::abs(val1) == std::abs(val2)));
    }

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}


// For testing different types
TEST(FilterTest, MathFunctionFilter2) {
    const int32_t numCols = 3;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int64_t *col2 = new int64_t[numRows];
    int32_t *col3 = new int32_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = -1;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), LongVecType(), IntVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    // filters
    ParserHelper ph;
    FunctionRegistry fr;
    std::string castStr = "CAST";
    VecTypePtr retType = DoubleType();
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, IntType()));
    std::string funcID = ph.GetFnIdentifier(castStr, args1, retType->GetId());
    FuncExpr *cast1 = new FuncExpr(castStr, args1, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(1, LongType()));
    funcID = ph.GetFnIdentifier(castStr, args2, retType->GetId());
    FuncExpr *cast2 = new FuncExpr(castStr, args2, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));

    BinaryExpr *filterExpr = new BinaryExpr(EQ, cast1, cast2, BooleanType());

    const int32_t projectCount = 3;
        std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, LongType()),
                                          new FieldExpr(2, IntType())};

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 2000);

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

// String Filter and varcharvec testing
TEST(FilterTest, FilterString1) {
    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    vector<string *> strings;
    int64_t *col1 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 40 == 0) {
            std::string *s = new std::string("hello");
            col1[i] = (int64_t) (s->c_str());
            strings.push_back(s);
        } else {
            std::string *s = new std::string("abcdefghijklmnopqrstuvwxyz");
            col1[i] = (int64_t) (s->c_str());
            strings.push_back(s);
        }
    }
    int64_t allData[numCols] = {(int64_t) col1};
    VecTypes inputTypes(std::vector<VecType>({VarcharVecType(30)}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

        BinaryExpr *filterExpr = new BinaryExpr(EQ, new FieldExpr(0, VarCharType()),
                                                new LiteralExpr(new std::string("hello"), VarCharType()), BooleanType());
    const int32_t projectCount = 1;
    std::vector<Expr*> projections = {new FieldExpr(0, VarCharType())};

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 25);
    for (int32_t i = 0; i < numReturned; i += 1000) {
        VarcharVector *vcVec = ((VarcharVector *) ret[0]->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);

        // Truncate the resulting string
        void *charArr = &actualChar;
        auto charArrCasted = static_cast<char **>(charArr);
        string actualStr(*charArrCasted, 0, len);
    }

    for (auto &s : strings) {
        delete s;
    }

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}


TEST(FilterTest, Coalesce1) {
    const int32_t numCols = 3;
    const int32_t numRows = 1000;
    int32_t *col1 = new int32_t[numRows];
    int64_t *col2 = new int64_t[numRows];
    int32_t *col3 = new int32_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = 100;
        col2[i] = 21;
        col3[i] = -1;
    }
    int64_t allData[numCols] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), IntVecType()}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2) {
            t->GetVector(1)->SetValueNull(i);
        } else {
            t->GetVector(1)->SetValueNotNull(i);
        }
    }

    CoalesceExpr *coalesceExpr = new CoalesceExpr(new FieldExpr(1, IntType()), new FieldExpr(0, IntType()));
    BinaryExpr *filterExpr = new BinaryExpr(EQ, new LiteralExpr(21, IntType()), coalesceExpr, BooleanType());
    const int32_t projectCount = 3;
    std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
                                      new FieldExpr(2, IntType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 500);

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}

TEST(FilterTest, Coalesce2) {
    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    vector<string *> strings;
    int64_t *col1 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        std::string *s = new std::string("hello");
        col1[i] = (int64_t) (s->c_str());
        strings.push_back(s);
    }
    int64_t allData[numCols] = {(int64_t) col1};
    VecTypes inputTypes(std::vector<VecType>({VarcharVecType(30)}));
    VectorBatch *t = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2) {
            t->GetVector(0)->SetValueNull(i);
        } else {
            // Seemingly necessary so that the bitmap doesn't get default values
            t->GetVector(0)->SetValueNotNull(i);
        };
    }
        CoalesceExpr *coalesceExpr = new CoalesceExpr(new FieldExpr(0, VarCharType()),
                                                      new LiteralExpr(new std::string("bye"), VarCharType()));
BinaryExpr *filterExpr = new BinaryExpr(EQ, coalesceExpr, new LiteralExpr(new std::string("hello"), VarCharType()),
                                        BooleanType());
    const int32_t projectCount = 1;
    std::vector<Expr*> projections = {new FieldExpr(0, VarCharType())};

    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 500);

    for (auto &s : strings) {
        delete s;
    }

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}


// To run this test, ensure that externalfunctions.so has been compiled and is placed in the correct folder
// For full instructions, see the README in core/src/codegen/functions
TEST(FilterTest, DISABLED_ExternalMathFunc) {
    const int32_t NUM_COLS = 3;
    const int32_t NUM_ROWS = 1000;
    int32_t *col1 = new int32_t[NUM_ROWS];
    int32_t *col2 = new int32_t[NUM_ROWS];
    int32_t *col3 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 2;
        col2[i] = 2;
        col3[i] = 10;
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), IntVecType()}));
    VectorBatch *t = CreateInput(NUM_ROWS, NUM_COLS, inputTypes.GetIds(), allData);

    // filter
    ParserHelper ph;
    FunctionRegistry fr;
    std::string funcStr = "Add1Int32";
    VecTypePtr retType = IntType();
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, IntType()));
    std::string funcID = ph.GetFnIdentifier(funcStr, args1, retType->GetId());
    FuncExpr *add1Int1Expr = new FuncExpr(funcStr, args1, *fr.LookupFunction(funcID));
    std::vector<Expr *> args2;
    args2.push_back(add1Int1Expr);
    funcID = ph.GetFnIdentifier(funcStr, args2, retType->GetId());
    FuncExpr *eqLeft = new FuncExpr(funcStr, args2, *(fr.LookupFunction(funcID)));

    std::vector<Expr *> args3;
    args3.push_back(new FieldExpr(1, IntType()));
    funcID = ph.GetFnIdentifier(funcStr, args3, retType->GetId());
    FuncExpr *add1Int2Expr = new FuncExpr(funcStr, args3, *fr.LookupFunction(funcID));
    std::vector<Expr *> args4;
    args4.push_back(add1Int2Expr);
    funcID = ph.GetFnIdentifier(funcStr, args4, retType->GetId());
    FuncExpr *eqRight = new FuncExpr(funcStr, args4, *(fr.LookupFunction(funcID)));

    BinaryExpr *filterExpr = new BinaryExpr(EQ, eqLeft, eqRight, BooleanType());

    const int32_t PROJECT_COUNT = 3;
        std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
                                          new FieldExpr(2, IntType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, NUM_COLS, projections, PROJECT_COUNT);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 500);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        int32_t val1 = ((IntVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_TRUE(val0 + 1 == val1);
    }

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
}


// To run this test, ensure that externalfunctions.so has been compiled and is placed in the correct folder
// For full instructions, see the README in core/src/codegen/functions
TEST(FilterTest, DISABLED_ExternalStringFunc) {
    const int32_t NUM_COLS = 1;
    const int32_t NUM_ROWS = 1000;
    vector<string *> strings;
    int64_t *col1 = new int64_t[NUM_ROWS];

    // column looks like:
    // hello, bye, hello, bye, hello, bye, ...
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        if (i % 2 == 0) {
            std::string *s = new std::string("hello");
            col1[i] = (int64_t) (s->c_str());
            strings.push_back(s);
        } else {
            if (i % 4 == 1) {
                std::string *s = new std::string("bye");
                col1[i] = (int64_t) (s->c_str());
                strings.push_back(s);
            } else {
                std::string *s = new std::string("asdf");
                col1[i] = (int64_t) (s->c_str());
                strings.push_back(s);
            }
        }
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    VecTypes inputTypes(std::vector<VecType>({VarcharVecType(30)}));
    VectorBatch *t = CreateInput(NUM_ROWS, NUM_COLS, inputTypes.GetIds(), allData);

    ParserHelper ph;
    FunctionRegistry fr;
    std::string funcStr = "LengthStr";
    VecTypePtr retType = IntType();
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, VarCharType()));
    std::string funcID = ph.GetFnIdentifier(funcStr, args, retType->GetId());
    FuncExpr *eqLeft = new FuncExpr(funcStr, args, *fr.LookupFunction(funcID));
    BinaryExpr *filterExpr = new BinaryExpr(EQ, eqLeft, new LiteralExpr(5, IntType()), BooleanType());

    const int32_t PROJECT_COUNT = 1;
    std::vector<Expr*> projections = {new FieldExpr(0, VarCharType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, NUM_COLS, projections, PROJECT_COUNT);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 500);

    for (auto &s : strings) {
        delete s;
    }

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}


// To run this test, ensure that externalfunctions.so has been compiled and is placed in the correct folder
// For full instructions, see the README in core/src/codegen/functions
TEST(FilterTest, DISABLED_ExternalStringFunc2) {
    const int32_t NUM_COLS = 1;
    const int32_t NUM_ROWS = 1000;
    vector<string *> strings;
    int64_t *col1 = new int64_t[NUM_ROWS];

    // column looks like:
    // hello, bye, hello, bye, hello, bye, ...
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        if (i % 2 == 0) {
            std::string *s = new std::string("hello");
            col1[i] = (int64_t) (s->c_str());
            strings.push_back(s);
        } else {
            if (i % 4 == 1) {
                std::string *s = new std::string("bye");
                col1[i] = (int64_t) (s->c_str());
                strings.push_back(s);
            } else {
                std::string *s = new std::string("asdf");
                col1[i] = (int64_t) (s->c_str());
                strings.push_back(s);
            }
        }
    }
    int64_t allData[NUM_COLS] = {(int64_t) col1};
    VecTypes inputTypes(std::vector<VecType>({VarcharVecType(30)}));
    VectorBatch *t = CreateInput(NUM_ROWS, NUM_COLS, inputTypes.GetIds(), allData);

    // filter
    ParserHelper ph;
    FunctionRegistry fr;
    std::string funcStr = "FirstCharStr";
    VecTypePtr retType = BooleanType();
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, VarCharType()));
    std::string funcID = ph.GetFnIdentifier(funcStr, args1, retType->GetId());
    FuncExpr *eqLeft = new FuncExpr(funcStr, args1, *fr.LookupFunction(funcID));

    std::vector<Expr *> args2;
    args2.push_back(new LiteralExpr(new std::string("apple"), VarCharType()));
    funcID = ph.GetFnIdentifier(funcStr, args2, retType->GetId());
    FuncExpr *eqRight = new FuncExpr(funcStr, args2, *fr.LookupFunction(funcID));

    BinaryExpr *filterExpr = new BinaryExpr(EQ, eqLeft, eqRight, BooleanType());

    const int32_t PROJECT_COUNT = 1;
    std::vector<Expr*> projections = {new FieldExpr(0, VarCharType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, NUM_COLS, projections, PROJECT_COUNT);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 250);

    for (auto &s : strings) {
        delete s;
    }

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
}


// Testing multithreading
// Two operators running at once

void process(omniruntime::op::Operator *op, VectorBatch *t, std::vector<VectorBatch *> ret, int32_t *numReturned) {
    op->AddInput(t);
    *numReturned = op->GetOutput(ret);
    VectorHelper::FreeVecBatches(ret);
    std::cout << "numSelectedRows: " << *numReturned << std::endl;
}

#include <thread>
#include <chrono>
#include <ratio>
// For testing different types
TEST(FilterTest, Multithreading) {
    const int32_t NUM_COLS = 3;
    const int32_t NUM_ROWS = 100000;
    int32_t *col1 = new int32_t[NUM_ROWS];
    int64_t *col2 = new int64_t[NUM_ROWS];
    int32_t *col3 = new int32_t[NUM_ROWS];
    for (int32_t i = 0; i < NUM_ROWS; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = -1;
    }

    int64_t allData[NUM_COLS] = {(int64_t) col1, (int64_t) col2, (int64_t) col3};
    VecTypes inputTypes(std::vector<VecType>({IntVecType(), LongVecType(), IntVecType()}));
    VecTypes inputTypes2(std::vector<VecType>({IntVecType(), LongVecType(), IntVecType()}));
    VectorBatch *t = CreateInput(NUM_ROWS, NUM_COLS, inputTypes.GetIds(), allData);
    VectorBatch *t2 = CreateInput(NUM_ROWS, NUM_COLS, inputTypes2.GetIds(), allData);

    std::vector<VectorBatch *> ret;
    std::vector<VectorBatch *> ret2;
    int32_t *numReturned = new int32_t;
    int32_t *numReturned2 = new int32_t;

    // find wall clock time
    auto start = std::chrono::high_resolution_clock::now();

    // filters
    ParserHelper ph;
    FunctionRegistry fr;
    std::string castStr = "CAST";
    std::string absStr = "abs";
    VecTypePtr retType = DoubleType();
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, IntType()));
    std::string funcID = ph.GetFnIdentifier(castStr, args1, retType->GetId());
    FuncExpr *cast1Expr = new FuncExpr(castStr, args1, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));
    std::vector<Expr *> args2;
    args2.push_back(cast1Expr);
    funcID = ph.GetFnIdentifier(absStr, args2, retType->GetId());
    FuncExpr *eqLeft = new FuncExpr(absStr, args2, make_unique<VecType>(*retType), *(fr.LookupFunction(funcID)));

    std::vector<Expr *> args3;
    args3.push_back(new FieldExpr(1, LongType()));
    funcID = ph.GetFnIdentifier(castStr, args3, retType->GetId());
    FuncExpr *cast2Expr = new FuncExpr(castStr, args3, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));
    std::vector<Expr *> args4;
    args4.push_back(cast2Expr);
    funcID = ph.GetFnIdentifier(absStr, args4, retType->GetId());
    FuncExpr *eqRight = new FuncExpr(absStr, args4, make_unique<VecType>(*retType), *(fr.LookupFunction(funcID)));

    BinaryExpr *filterExpr1 = new BinaryExpr(EQ, eqLeft, eqRight, BooleanType());

    const int32_t PROJECT_COUNT = 3;
        std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, LongType()),
                                          new FieldExpr(2, IntType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr1, inputTypes, NUM_COLS, projections, PROJECT_COUNT);
    omniruntime::op::Operator *op = factory->CreateOperator();
    std::thread thread1(process, op, t, ret, numReturned);

    // filter2
    LiteralExpr *eqRight2 = new LiteralExpr(4, LongType());
    eqRight2->longVal = 4;
    BinaryExpr *filterExpr2 = new BinaryExpr(EQ, new FieldExpr(1, LongType()), eqRight2, BooleanType());
    std::vector<Expr*> projections2 = {new FieldExpr(0, IntType()), new FieldExpr(1, LongType()),
                                       new FieldExpr(2, IntType())};

    OperatorFactory *factory2 = new FilterAndProjectOperatorFactory(filterExpr2, inputTypes2, NUM_COLS, projections2, 3);
    omniruntime::op::Operator *op2 = factory2->CreateOperator();
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

    VectorHelper::FreeVecBatches(ret);
    VectorHelper::FreeVecBatches(ret2);

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
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    IntVector *col2 = new IntVector(vecAllocator, numRows);
    IntVector *col3 = new IntVector(vecAllocator, numRows);
    int32_t ids[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *dictionaryVector = new DictionaryVector(col3, ids, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
        col3->SetValue(i, (i % 21) - 3);
    }

    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), IntVecType()}));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->NewVectors(vecAllocator, inputTypes.Get());
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryVector);

        BetweenExpr *filterExpr = new BetweenExpr(new FieldExpr(1, IntType()), new FieldExpr(0, IntType()),
                                                  new FieldExpr(2, IntType()));
    const int32_t projectCount = 3;
    std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, LongType()), new FieldExpr(2, IntType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorBatch *copiedBatch = DuplicateVectorBatch(batch);
    op->AddInput(copiedBatch);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 7);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i));
        int32_t val1 = ((IntVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, col2->GetValue(i));
        int32_t val2 = ((IntVector *) ret[0]->GetVector(2))->GetValue(i);
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
    const int32_t numRows = 3;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    VarcharVector *col2 = new VarcharVector(vecAllocator, 1024, numRows);
    int32_t ids[] = {0, 1, 2};
    DictionaryVector *dictionaryVector = new DictionaryVector(col2, ids, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i * 3);
        std::string tmp = "test";
        col2->SetValue(i, reinterpret_cast<const uint8_t *>(tmp.c_str()), tmp.length());
    }

    VecTypes inputTypes(std::vector<VecType>({IntVecType(), VarcharVecType(50)}));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->NewVectors(VectorAllocatorFactory::GetGlobalAllocator(), inputTypes.Get());
    batch->SetVector(0, col1);
    batch->SetVector(1, dictionaryVector);

    BinaryExpr *filterExpr = new BinaryExpr(LT, new FieldExpr(0, IntType()), new LiteralExpr(6, IntType()), BooleanType());
    const int32_t projectCount = 2;
    std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, VarCharType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorBatch *copiedBatch = DuplicateVectorBatch(batch);
    op->AddInput(copiedBatch);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i));
        uint8_t *data = nullptr;
        int32_t len = ((VarcharVector *) ret[0]->GetVector(1))->GetValue(i, &data);
        std::string result(data, data + len);
        data = nullptr;
        len = dictionaryVector->GetVarchar(i, &data);
        std::string expected(data, data + len);
        EXPECT_EQ(result, expected);
    }

    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col2;
    delete op;
    delete factory;
}

TEST(FilterTest, TestFilterDictionaryVecNested) {
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    IntVector *col2 = new IntVector(vecAllocator, numRows);
    IntVector *col3 = new IntVector(vecAllocator, 3);
    int32_t data[] = {4, 5, 6};
    col3->SetValues(0, data, 3);
    int32_t ids[] = {1, 2};
    DictionaryVector *dictionaryVector = new DictionaryVector(col3, ids, 2);
    int32_t nestedIds[] = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
    DictionaryVector *dictionaryNested = new DictionaryVector(dictionaryVector, nestedIds, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
    }

    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), IntVecType()}));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->NewVectors(vecAllocator, inputTypes.Get());
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryNested);

        BetweenExpr *filterExpr = new BetweenExpr(new FieldExpr(1, IntType()), new FieldExpr(0, IntType()),
                                                  new FieldExpr(2, IntType()));
    const int32_t projectCount = 3;
    std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
                                      new FieldExpr(2, IntType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorBatch *copiedBatch = DuplicateVectorBatch(batch);
    op->AddInput(copiedBatch);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 6);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i));
        int32_t val1 = ((IntVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, col2->GetValue(i));
        int32_t val2 = ((IntVector *) ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, dictionaryNested->GetInt(i));
    }

    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col3;
    delete dictionaryVector;
    delete op;
    delete factory;
}

TEST(FilterTest, DecimalFilterBinaryTest) {
    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    int64_t *data1 = new int64_t[numRows * 2];
    int64_t *data2 = new int64_t[numRows * 2];
    for (int64_t i = 0; i < numRows; i++) {
        data1[2 * i] = (i + 1) * 1000;
        data1[2 * i + 1] = 0;
        data2[2 * i] = (i + 1) * 1;
        data2[2 * i + 1] = 0;
    }

    int64_t allData[numCols] = {(int64_t) data1};
    std::vector<VecType> vecOfTypes = {VecType(OMNI_VEC_TYPE_DECIMAL128)};
    VecTypes inputTypes(vecOfTypes);
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;
        std::vector<Expr*> projections = {new FieldExpr(0, Decimal128Type(38, 0))};
    LiteralExpr *lteRight = new LiteralExpr(500000, LongType());
    lteRight->longVal = 500000;
    BinaryExpr *filterExpr = new BinaryExpr(LTE, new FieldExpr(0, Decimal128Type(38, 0)), lteRight, BooleanType());
    OperatorFactory *factory = new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections,
                                                                   projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter7));
    EXPECT_EQ(numReturned, 500);


    allData[0] = (int64_t) data2;
    VectorBatch *in2 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(in2);
    numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[1], numReturned, Filter7));
    EXPECT_EQ(numReturned, 1000);

    VectorHelper::FreeVecBatches(ret);

    delete[] data1;
    delete[] data2;
    delete op;
    delete factory;
}

TEST(FilterTest, DecimalFilterAbsTest) {
    const int32_t numCols = 3;
    const int32_t numRows = 1000;
    int64_t *data1 = new int64_t[numRows * 2];
    int64_t *data2 = new int64_t[numRows * 2];
    int64_t *data3 = new int64_t[numRows * 2];
    for (int64_t i = 0; i < numRows; i++) {
        data1[2 * i] = (i + 1) * 1;
        data1[2 * i + 1] = -1000;
        data2[2 * i] = (i + 1) * 1;
        data2[2 * i + 1] = -1000;
        data3[2 * i] = (i + 1) * 1;
        data3[2 * i + 1] = -1000;
    }
    int64_t allData[numCols] = {(int64_t) data1, (int64_t) data2, (int64_t) data3};
    std::vector<VecType> vecOfTypes = {VecType(OMNI_VEC_TYPE_DECIMAL128), VecType(OMNI_VEC_TYPE_DECIMAL128),
                                       VecType(OMNI_VEC_TYPE_DECIMAL128)};
    VecTypes inputTypes(vecOfTypes);
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 3;
    std::vector<Expr*> projections = {new FieldExpr(0, Decimal128Type(38, 0)),
                                      new FieldExpr(1, Decimal128Type(38, 0)), new FieldExpr(2, Decimal128Type(38, 0))};

    // filters
    ParserHelper ph;
    FunctionRegistry fr;
    std::string absStr = "abs";
    VecTypePtr retType =  Decimal128Type(38, 0);
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, Decimal128Type(38, 0)));
    std::string funcID = ph.GetFnIdentifier(absStr, args1, retType->GetId());
    FuncExpr *absExpr1 = new FuncExpr(absStr, args1, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(2, Decimal128Type(38, 0)));
    funcID = ph.GetFnIdentifier(absStr, args2, retType->GetId());
    FuncExpr *absExpr2 = new FuncExpr(absStr, args2, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));

    BinaryExpr *eqExpr1 = new BinaryExpr(EQ, absExpr1, absExpr2,  Decimal128Type(38, 0));

    std::vector<Expr *> args3;
    args3.push_back(new FieldExpr(1, Decimal128Type(38, 0)));
    funcID = ph.GetFnIdentifier(absStr, args3, retType->GetId());
    FuncExpr *absExpr3 = new FuncExpr(absStr, args3, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));

    std::vector<Expr *> args4;
    args4.push_back(new FieldExpr(2, Decimal128Type(38, 0)));
    funcID = ph.GetFnIdentifier(absStr, args4, retType->GetId());
    FuncExpr *absExpr4 = new FuncExpr(absStr, args4, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));

    BinaryExpr *eqExpr2 = new BinaryExpr(EQ, absExpr3, absExpr4,  Decimal128Type(38, 0));

    BinaryExpr *filterExpr = new BinaryExpr(AND, eqExpr1, eqExpr2, BooleanType());

    OperatorFactory *factory = new FilterAndProjectOperatorFactory(
            filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 1000);


    delete[] data1;
    delete[] data2;
    delete[] data3;
    delete op;
    delete factory;
}

TEST(FilterTest, FilterStringWithNull) {
    const int32_t numCols = 1;
    const int32_t numRows = 2;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    VarcharVector *col0 = new VarcharVector(vecAllocator, 1024, numRows);
    std::string str = "hello";
    col0->SetValue(0, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    col0->SetValueNull(1);

    VecTypes inputTypes(std::vector<VecType>({VarcharVecType(1000)}));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->NewVectors(vecAllocator, inputTypes.Get());
    batch->SetVector(0, col0);

    BinaryExpr *filterExpr = new BinaryExpr(EQ, new FieldExpr(0, VarCharType()),
                                            new LiteralExpr(new std::string("hello"), VarCharType()), BooleanType());
    const int32_t projectCount = 1;
    std::vector<Expr*> projections = {new FieldExpr(0, VarCharType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(batch);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 1);

    for (int32_t i = 0; i < numReturned; i++) {
        VarcharVector *vcVec = ((VarcharVector *) ret[0]->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);
        std::string actualStr(actualChar, actualChar + len);
        EXPECT_EQ(actualStr, "hello");
    }

    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
}

TEST(FilterTest, TestFilterSlicedDictionaryVec) {
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    IntVector *col2 = new IntVector(vecAllocator, numRows);
    IntVector *col3 = new IntVector(vecAllocator, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
        col3->SetValue(i, (i % 21) - 3);
    }
    int32_t ids[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *dictionaryVector = new DictionaryVector(col3, ids, numRows);
    delete col3;
    auto slicedCol1 = col1->Slice(5, 5);
    auto slicedCol2 = col2->Slice(5, 5);
    auto slicedCol3 = dictionaryVector->Slice(5, 5);
    delete col1;
    delete col2;
    delete dictionaryVector;

    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), IntVecType()}));
    VectorBatch *intput = new VectorBatch(numCols, slicedCol1->GetSize());
    intput->NewVectors(vecAllocator, inputTypes.Get());
    intput->SetVector(0, slicedCol1);
    intput->SetVector(1, slicedCol2);
    intput->SetVector(2, slicedCol3);

    BetweenExpr *filterExpr = new BetweenExpr(new FieldExpr(1, IntType()), new FieldExpr(0, IntType()),
                                              new FieldExpr(2, IntType()));
    const int32_t projectCount = 3;
    std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
                                      new FieldExpr(2, IntType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorBatch *copiedBatch = DuplicateVectorBatch(intput);
    op->AddInput(copiedBatch);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, slicedCol1->GetValue(i));
        int32_t val1 = ((IntVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, slicedCol2->GetValue(i));
        int32_t val2 = ((IntVector *) ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, slicedCol3->GetInt(i));
    }

    VectorHelper::FreeVecBatch(intput);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
}

TEST(FilterTest, TestFilterSlicedDictionaryVecWithNull) {
    const int32_t numCols = 3;
    int32_t inputTypeIds[] = {1, 1, 1};

    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    IntVector *col2 = new IntVector(vecAllocator, numRows);
    IntVector *col3 = new IntVector(vecAllocator, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            col3->SetValueNull(i);
        } else {
            col3->SetValue(i, (i % 21) - 3);
        }
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
    }
    int32_t ids[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *dictionaryVector = new DictionaryVector(col3, ids, numRows);
    delete col3;
    auto slicedCol1 = col1->Slice(4, 6);
    auto slicedCol2 = col2->Slice(4, 6);
    auto slicedCol3 = dictionaryVector->Slice(4, 6);
    delete col1;
    delete col2;
    delete dictionaryVector;

    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType(), IntVecType()}));
    VectorBatch *intput = new VectorBatch(numCols, slicedCol1->GetSize());
    intput->NewVectors(vecAllocator, inputTypes.Get());
    intput->SetVector(0, slicedCol1);
    intput->SetVector(1, slicedCol2);
    intput->SetVector(2, slicedCol3);

    BinaryExpr *filterExpr = new BinaryExpr(EQ, new FieldExpr(2, IntType()), new LiteralExpr(6, IntType()), BooleanType());
    const int32_t projectCount = 3;
    std::vector<Expr*> projections = {new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
                                      new FieldExpr(2, IntType())};
    OperatorFactory *factory =
            new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorBatch *copiedBatch = DuplicateVectorBatch(intput);
    op->AddInput(copiedBatch);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *) ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, slicedCol1->GetValue(i + 2));
        int32_t val1 = ((IntVector *) ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, slicedCol2->GetValue(i + 2));
        int32_t val2 = ((IntVector *) ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, slicedCol3->GetInt(i + 2));
    }

    VectorHelper::FreeVecBatch(intput);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
}

TEST(FilterTest, SimpleFilter) {
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    auto col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t allData[numCols] = {(int64_t) col1};
    VecTypes inputTypes(std::vector<VecType>({IntVecType()}));
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

        BinaryExpr *filterExpr = new BinaryExpr(LT, new FieldExpr(0, IntType()), new LiteralExpr(2000, IntType()),
                                                BooleanType());
    auto filter = new SimpleFilter(*filterExpr);
    bool initialized = filter->Initialize();
    EXPECT_TRUE(initialized);

    ExecutionContext context;
    auto vector = (IntVector *) in1->GetVector(0);
    int64_t values[1];
    bool isNulls[1];
    for (int i = 0; i < numRows; i++) {
        values[0] = VectorHelper::GetValuesAddr(vector) + i * sizeof(int32_t);
        isNulls[0] = vector->IsValueNull(i);
        bool result = filter->Evaluate(values, isNulls, nullptr, (int64_t) (&context));
        if (i < 2000) {
            EXPECT_TRUE(result);
        } else {
            EXPECT_FALSE(result);
        }
    }
    delete filter;
    delete[] col1;
}

TEST(FilterTest, SimpleFilterWithNulls) {
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    auto col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t allData[numCols] = {(int64_t) col1};
    VecTypes inputTypes(std::vector<VecType>({IntVecType()}));
    VectorBatch *in1 = CreateInput(numRows, numCols, inputTypes.GetIds(), allData);

    BinaryExpr *filterExpr = new BinaryExpr(LT, new FieldExpr(0, IntType()), new LiteralExpr(2000, IntType()), BooleanType());

    auto filter = new SimpleFilter(*filterExpr);
    bool initialized = filter->Initialize();
    EXPECT_TRUE(initialized);

    // set first 500 elements to null
    auto vector = (IntVector *) in1->GetVector(0);
    for (int i = 0; i < 500; i++) {
        vector->SetValueNull(i);
    }

    ExecutionContext context;
    int64_t values[1];
    bool isNulls[1];
    for (int i = 0; i < numRows; i++) {
        values[0] = VectorHelper::GetValuesAddr(vector) + i * sizeof(int32_t);
        isNulls[0] = vector->IsValueNull(i);
        bool result = filter->Evaluate(values, isNulls, nullptr, (int64_t) (&context));
        if (i >= 500 && i < 2000) {
            EXPECT_TRUE(result);
        } else {
            EXPECT_FALSE(result);
        }
    }
    delete filter;
    delete[] col1;
}

TEST(FilterTest, SimpleFilterIntWithNulls) {
    const int32_t numRows = 10;
    int32_t data0[numRows] = {19, 14, 7, 19, 1, 20, 10, 13, 20, 16};
    int32_t data1[numRows] = {20, 16, 13, 4, 20, 4, 22, 19, 8, 7};

    VecTypes inputTypes(std::vector<VecType>({IntVecType(), IntVecType()}));
    auto vecBatch = CreateVectorBatch(inputTypes, numRows, data0, data1);

    BinaryExpr *filterExpr = new BinaryExpr(EQ, new FieldExpr(0, IntType()), new FieldExpr(1, IntType()), BooleanType());
    auto filter = new SimpleFilter(*filterExpr);
    bool initialized = filter->Initialize();
    EXPECT_TRUE(initialized);

    for (int i = 0; i < numRows; i++) {
        if (i % 5 == 4) {
            vecBatch->GetVector(0)->SetValueNull(i);
            vecBatch->GetVector(1)->SetValueNull(i);
        }
    }

    ExecutionContext context;
    int64_t values[2];
    bool isNulls[2];
    auto vector0 = vecBatch->GetVector(0);
    auto vector1 = vecBatch->GetVector(1);
    for (int i = 0; i < numRows; i++) {
        values[0] = reinterpret_cast<int64_t>(((int32_t *) vector0->GetValues()) + i);
        isNulls[0] = vector0->IsValueNull(i);
        values[1] = reinterpret_cast<int64_t>(((int32_t *) vector1->GetValues()) + i);
        isNulls[1] = vector1->IsValueNull(i);
        bool result = filter->Evaluate(values, isNulls, nullptr, (int64_t) (&context));
        EXPECT_FALSE(result);
    }
    delete filter;
}

TEST(FilterTest, SimpleFilterCharWithNulls) {
    const int32_t numRows = 9;
    std::string data0[numRows] = {"35709", "35709", "35709", "31904", "", "", "35709", "35709", ""};
    std::string data1[numRows] = {"31904", "35709", "31904", "31904", "31904", "35709", "35709", "31904", "35709"};
    auto vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    auto vec0 = new VarcharVector(vecAllocator, 5 * numRows, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        if (data0[i].compare("") == 0) {
            vec0->SetValueNull(i);
        } else {
            vec0->SetValue(i, (uint8_t *) (data0[i].c_str()), data0[i].length());
        }
    }
    auto vec1 = CreateVarcharVector(VarcharVecType(5), data1, numRows);
    auto vecBatch = new VectorBatch(2, numRows);
    vecBatch->SetVector(0, vec0);
    vecBatch->SetVector(1, vec1);

    VecTypes inputTypes(std::vector<VecType>({VarcharVecType(5), VarcharVecType(5)}));
    //filter expression object
    ParserHelper ph;
    FunctionRegistry fr;
    std::string funcStr = "substr";
    VecTypePtr retType = VarCharType();
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, VarCharType()));
    args1.push_back(new LiteralExpr(1, IntType()));
    args1.push_back(new LiteralExpr(5, IntType()));
    std::string funcID = ph.GetFnIdentifier(funcStr, args1, retType->GetId());
    FuncExpr *substrExpr1 = new FuncExpr(funcStr, args1, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(1, VarCharType()));
    args2.push_back(new LiteralExpr(1, IntType()));
    args2.push_back(new LiteralExpr(5, IntType()));
    funcID = ph.GetFnIdentifier(funcStr, args2, retType->GetId());
    FuncExpr *substrExpr2 = new FuncExpr(funcStr, args2, make_unique<VecType>(*retType), *fr.LookupFunction(funcID));
    BinaryExpr *filterExpr = new BinaryExpr(NEQ, substrExpr1, substrExpr2, BooleanType());

    auto filter = new SimpleFilter(*filterExpr);
    bool initialized = filter->Initialize();
    EXPECT_TRUE(initialized);

    ExecutionContext context;
    int64_t values[2];
    bool isNulls[2];
    int32_t lengths[2];
    auto vector0 = vecBatch->GetVector(0);
    auto vector1 = vecBatch->GetVector(1);
    for (int i = 0; i < numRows; i++) {
        isNulls[0] = vector0->IsValueNull(i);
        isNulls[1] = vector1->IsValueNull(i);
        values[0] = VectorHelper::GetValuePtrAndLength(vector0, i, lengths + 0);
        values[1] = VectorHelper::GetValuePtrAndLength(vector1, i, lengths + 1);

        bool result = filter->Evaluate(values, isNulls, lengths, (int64_t) (&context));
        if (i == 0 || i == 2 || i == 7) {
            EXPECT_TRUE(result);
        } else {
            EXPECT_FALSE(result);
        }
    }
    delete filter;
}