/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include <iostream>
#include <vector>
#include <chrono>
#include "gtest/gtest.h"
#include "operator/filter/filter_and_project.h"
#include "../util/test_util.h"

namespace FilterTest {
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace std;
using namespace TestUtil;

namespace FilterTest {
bool CheckOutput(VectorBatch *t, const int32_t numRows, bool (*filter)(VectorBatch *, int32_t))
{
    for (int32_t i = 0; i < numRows; i++) {
        if (!filter(t, i)) {
            return false;
        }
    }
    return true;
}

// Expects 1 column of type int32
bool Filter1(VectorBatch *t, int32_t index)
{
    int n = 4;
    return ((IntVector *)t->GetVector(0))->GetValue(index) <= n;
}

// Expects 2 columns of type int32, int64
bool Filter2(VectorBatch *t, int32_t index)
{
    int32_t val1 = ((IntVector *)t->GetVector(0))->GetValue(index);
    int64_t val2 = ((LongVector *)t->GetVector(1))->GetValue(index);
    // true if both values are negative
    return val1 < 0 && val2 < 0;
}

// Expects 3 columns of type int32, int64, double
bool Filter3(VectorBatch *t, int32_t index)
{
    int n1 = 0;
    int n2 = 1;
    int n3 = 2;
    int32_t val1 = ((IntVector *)t->GetVector(n1))->GetValue(index);
    int64_t val2 = ((LongVector *)t->GetVector(n2))->GetValue(index);
    double val3 = ((DoubleVector *)t->GetVector(n3))->GetValue(index);
    // first val is multiple of 3, second val = 3 billion, third val >= 0.4.
    return val1 % 3 == 0 && val2 == static_cast<int64_t>(3e9) && val3 >= 0.4;
}

bool Filter4(VectorBatch *t, int32_t index)
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

bool Filter5(VectorBatch *t, int32_t index)
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

bool Filter6(VectorBatch *t, int32_t index)
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
bool Filter7(VectorBatch *t, int32_t index)
{
    int32_t n = 500000;
    Decimal128 val = ((Decimal128Vector *)t->GetVector(0))->GetValue(index);
    return Decimal128(val.HighBits(), val.LowBits()) <= n;
}

TEST(FilterTest, LessThan)
{
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LessThan");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, col1);

    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()) };
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, IntType()),
        new LiteralExpr(2000, IntType()), BooleanType());
    OverflowConfig *overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val < 2000);
    }
    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, LessThanWihtoutParsing)
{
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LessThanWihtoutParsing");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, col1);

    const int32_t projectCount = 1;
    FieldExpr *column = new FieldExpr(0, IntType());
    FieldExpr *left = new FieldExpr(0, IntType());
    LiteralExpr *right = new LiteralExpr(2000, IntType());
    BinaryExpr *LTExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, left, right, BooleanType());
    std::vector<Expr *> projections = { column };
    OverflowConfig *overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(LTExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();


    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val < 2000);
    }
    Expr::DeleteExprs({ LTExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, GreaterThan)
{
    const int32_t numCols = 2;
    const int32_t numRows = 5000;
    int32_t *col1 = new int32_t[numRows];
    int64_t *col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 25;
        col2[i] = 3e9;
    }
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_GreaterThan");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, col1, col2);

    const int32_t projectCount = 2;
    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col1Expr = new FieldExpr(1, LongType());
    std::vector<Expr *> projections = { col0Expr, col1Expr };

    FieldExpr *gtLeft = new FieldExpr(0, IntType());
    LiteralExpr *gtRight = new LiteralExpr(20, IntType());
    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft, gtRight, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(gtExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 800);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_TRUE(val0 > 20);
        EXPECT_EQ(val1, 3e9L);
    }
    Expr::DeleteExprs({ gtExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, EqualTo)
{
    const int32_t numCols = 3;
    const int32_t numRows = 5000;
    int32_t *col1 = new int32_t[numRows];
    double *col2 = new double[numRows];
    int64_t *col3 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = col3[i] = i % 100;
    }
    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_EqualTo");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, col1, col3, col2);

    const int32_t projectCount = 2;

    FieldExpr *col1Expr = new FieldExpr(1, LongType());
    FieldExpr *col2Expr = new FieldExpr(2, DoubleType());
    std::vector<Expr *> projections = { col2Expr, col1Expr };

    FieldExpr *eqLeft = new FieldExpr(2, DoubleType());
    LiteralExpr *eqRight = new LiteralExpr(50.0, DoubleType());

    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(eqExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 50);
    for (int32_t i = 0; i < numReturned; i++) {
        double val0 = ((DoubleVector *)ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, 50);
        EXPECT_EQ(val0, val1);
    }
    Expr::DeleteExprs({ eqExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, GreaterThanOrEqualTo)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_GreaterThanOrEqualTo");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, col1, col2);

    const int32_t projectCount = 1;

    FieldExpr *col1Expr = new FieldExpr(1, IntType());
    std::vector<Expr *> projections = { col1Expr };

    FieldExpr *gteLeft = new FieldExpr(1, IntType());
    LiteralExpr *gteRight = new LiteralExpr(30, IntType());
    BinaryExpr *gteExpr = new BinaryExpr(omniruntime::expressions::Operator::GTE, gteLeft, gteRight, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(gteExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 834);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 >= 30);
    }
    Expr::DeleteExprs({ gteExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, NotEqualTo)
{
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    double *col1 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ DoubleType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_NotEqualTo");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, col1);

    const int32_t projectCount = 1;
    FieldExpr *col0Expr = new FieldExpr(0, DoubleType());
    std::vector<Expr *> projections = { col0Expr };

    FieldExpr *neqLeft = new FieldExpr(0, DoubleType());
    LiteralExpr *neqRight = new LiteralExpr(0, DoubleType());

    BinaryExpr *neqExpr = new BinaryExpr(omniruntime::expressions::Operator::NEQ, neqLeft, neqRight, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(neqExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4999);
    double cnt = 1;
    for (int32_t i = 0; i < numReturned; i++) {
        double val0 = ((DoubleVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, cnt++);
    }
    Expr::DeleteExprs({ neqExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, AllPass)
{
    const int32_t numCols = 1;
    const int32_t numRows = 20000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = 9348;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_AllPass");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, col1);

    const int32_t projectCount = 1;

    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    std::vector<Expr *> projections = { col0Expr };

    FieldExpr *eqLeft = new FieldExpr(0, IntType());
    LiteralExpr *eqRight = new LiteralExpr(9348, IntType());
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(eqExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 20000);

    Expr::DeleteExprs({ eqExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, MultipleInputs)
{
    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    int32_t *data1 = new int32_t[numRows];
    int32_t *data2 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        data1[i] = i % 10;
        data2[i] = i % 6 + 1;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_MultipleInputs");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, data1);

    const int32_t projectCount = 1;

    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    std::vector<Expr *> projections = { col0Expr };

    FieldExpr *lteLeft = new FieldExpr(0, IntType());
    LiteralExpr *lteRight = new LiteralExpr(4, IntType());
    BinaryExpr *lteExpr = new BinaryExpr(omniruntime::expressions::Operator::LTE, lteLeft, lteRight, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(lteExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter1));
    EXPECT_EQ(numReturned, 500);

    VectorBatch *in2 = CreateVectorBatch(inputTypes, numRows, data2);
    op->AddInput(in2);
    numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[1], numReturned, Filter1));
    EXPECT_EQ(numReturned, 668);

    Expr::DeleteExprs({ lteExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] data1;
    delete[] data2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, NegativeValues)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_NegativeValues");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, data1, data2);

    const int32_t projectCount = 2;

    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col1Expr = new FieldExpr(1, LongType());
    std::vector<Expr *> projections = { col0Expr, col1Expr };

    // create the filter expression object
    FieldExpr *lte1Left = new FieldExpr(0, IntType());
    LiteralExpr *lte1Right = new LiteralExpr(-1, IntType());
    BinaryExpr *lte1Expr = new BinaryExpr(omniruntime::expressions::Operator::LTE, lte1Left, lte1Right, BooleanType());

    FieldExpr *lte2Left = new FieldExpr(1, LongType());
    LiteralExpr *lte2Right = new LiteralExpr(-1L, LongType());
    BinaryExpr *lte2Expr = new BinaryExpr(omniruntime::expressions::Operator::LTE, lte2Left, lte2Right, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, lte1Expr, lte2Expr, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter2));
    // Both values are negative for every multiple of 35.
    EXPECT_EQ(numReturned, 286);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] data1;
    delete[] data2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, AllTypes)
{
    const int32_t numCols = 3;
    const int32_t numRows = 1000;
    int32_t *data1 = new int32_t[numRows];
    int64_t *data2 = new int64_t[numRows];
    double *data3 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        data1[i] = i % 3;
        data2[i] = (i % 2 != 0) ? 3e9 : 0;
        data3[i] = i % 10 / 10.0;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_AllTypes");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, data1, data2, data3);

    const int32_t projectCount = 3;
    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col1Expr = new FieldExpr(1, LongType());
    FieldExpr *col2Expr = new FieldExpr(2, DoubleType());
    std::vector<Expr *> projections = { col0Expr, col1Expr, col2Expr };

    // create the filter expression object
    FieldExpr *eq2Left = new FieldExpr(1, LongType());
    LiteralExpr *eq2Right = new LiteralExpr(3000000000L, LongType());
    BinaryExpr *eq2Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eq2Left, eq2Right, BooleanType());

    FieldExpr *gteLeft = new FieldExpr(2, DoubleType());
    LiteralExpr *gteRight = new LiteralExpr(0.4, DoubleType());
    BinaryExpr *gteExpr = new BinaryExpr(omniruntime::expressions::Operator::GTE, gteLeft, gteRight, BooleanType());

    BinaryExpr *innerAndExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, eq2Expr, gteExpr, BooleanType());

    FieldExpr *eq1Left = new FieldExpr(0, IntType());
    LiteralExpr *eq1Right = new LiteralExpr(0, IntType());
    BinaryExpr *eq1Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eq1Left, eq1Right, BooleanType());

    Expr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, eq1Expr, innerAndExpr, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter3));
    EXPECT_EQ(numReturned, 100);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] data1;
    delete[] data2;
    delete[] data3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, Compile)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ DoubleType(), IntType(), DoubleType(), DoubleType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Compile");
    VectorBatch *t = CreateVectorBatch(inputTypes, dataSize, data1, data2, data3, data4);

    // TPCH 6
    FieldExpr *col0Expr = new FieldExpr(0, DoubleType());
    std::vector<Expr *> projections = { col0Expr };

    LiteralExpr *gtRight = new LiteralExpr(8766.0, DoubleType());
    BinaryExpr *gtExpr =
        new BinaryExpr(omniruntime::expressions::Operator::GT, new FieldExpr(3, DoubleType()), gtRight, BooleanType());

    LiteralExpr *lt1Right = new LiteralExpr(9131.0, DoubleType());
    BinaryExpr *lt1Expr =
        new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(3, DoubleType()), lt1Right, BooleanType());
    BinaryExpr *and1Expression =
        new BinaryExpr(omniruntime::expressions::Operator::AND, gtExpr, lt1Expr, BooleanType());

    LiteralExpr *lt2Right = new LiteralExpr(24.0, DoubleType());
    BinaryExpr *lt2expr =
        new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, DoubleType()), lt2Right, BooleanType());

    FieldExpr *data = new FieldExpr(2, DoubleType());
    LiteralExpr *lower = new LiteralExpr(0.05, DoubleType());
    LiteralExpr *upper = new LiteralExpr(0.07, DoubleType());
    std::vector<Expr *> args;
    BetweenExpr *betweenExpr = new BetweenExpr(data, lower, upper);
    BinaryExpr *and2Expression =
        new BinaryExpr(omniruntime::expressions::Operator::AND, betweenExpr, lt2expr, BooleanType());

    BinaryExpr *filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::AND, and1Expression, and2Expression, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, 1, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numSelectedRows = op->GetOutput(ret);
    EXPECT_EQ(numSelectedRows, 100);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] data1;
    delete[] data2;
    delete[] data3;
    delete[] data4;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, LogicalOperators1)
{
    const int32_t numCols = 6;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int32_t *col2 = new int32_t[numRows];
    int32_t *col3 = new int32_t[numRows];
    int64_t *col4 = new int64_t[numRows];
    double *col5 = new double[numRows];
    int64_t *col6 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 3 != 0 ? 1 : 0;
        col2[i] = col3[i] = i;
        col4[i] = i % 2 != 0 ? 2999999999 : 3e9;
        col5[i] = 50 + i / 10.0;
        col6[i] = i % 55;
    }

    // int int int long double long
    DataTypes inputTypes(
        std::vector<DataTypePtr>({ IntType(), IntType(), IntType(), LongType(), DoubleType(), LongType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LogicalOperators1");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3, col4, col5, col6);
    const int32_t projectCount = 4;
    // projection objects:
    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col2Expr = new FieldExpr(2, IntType());
    FieldExpr *col4Expr = new FieldExpr(4, DoubleType());
    FieldExpr *col5Expr = new FieldExpr(5, LongType());
    std::vector<Expr *> projections = { col0Expr, col2Expr, col4Expr, col5Expr };

    LiteralExpr *eqRight = new LiteralExpr(3000000000L, LongType());
    BinaryExpr *eqExpr =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(3, LongType()), eqRight, BooleanType());
    BinaryExpr *neqExpr = new BinaryExpr(omniruntime::expressions::Operator::NEQ, new FieldExpr(0, IntType()),
        new LiteralExpr(1, IntType()), BooleanType());
    BinaryExpr *andExpr1 = new BinaryExpr(omniruntime::expressions::Operator::AND, neqExpr, eqExpr, BooleanType());

    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, new FieldExpr(2, IntType()),
        new LiteralExpr(4800, IntType()), BooleanType());
    BinaryExpr *lteExpr = new BinaryExpr(omniruntime::expressions::Operator::LTE, new FieldExpr(1, IntType()),
        new LiteralExpr(9990, IntType()), BooleanType());
    BinaryExpr *andExpr2 = new BinaryExpr(omniruntime::expressions::Operator::AND, gtExpr, lteExpr, BooleanType());

    BinaryExpr *andExpr3 = new BinaryExpr(omniruntime::expressions::Operator::AND, andExpr2, andExpr1, BooleanType());

    LiteralExpr *ltRight = new LiteralExpr(50.8, DoubleType());
    BinaryExpr *ltExpr =
        new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(4, DoubleType()), ltRight, BooleanType());
    BinaryExpr *andExpr4 = new BinaryExpr(omniruntime::expressions::Operator::AND, ltExpr, andExpr3, BooleanType());

    LiteralExpr *gteRight = new LiteralExpr(52L, LongType());
    BinaryExpr *gteExpr =
        new BinaryExpr(omniruntime::expressions::Operator::GTE, new FieldExpr(5, LongType()), gteRight, BooleanType());
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::OR, gteExpr, andExpr4, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 543);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter4));

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete[] col5;
    delete[] col6;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, LogicalOperators2)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), LongType(), LongType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LogicalOperators2");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3, col4);
    const int32_t projectCount = 4;

    // projections
    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col1Expr = new FieldExpr(1, IntType());
    FieldExpr *col2Expr = new FieldExpr(2, LongType());
    FieldExpr *col3Expr = new FieldExpr(3, LongType());

    std::vector<Expr *> projections = { col3Expr, col2Expr, col1Expr, col0Expr };

    LiteralExpr *lteRight = new LiteralExpr(-3000000000L, LongType());
    BinaryExpr *lteExpr =
        new BinaryExpr(omniruntime::expressions::Operator::LTE, new FieldExpr(2, LongType()), lteRight, BooleanType());
    LiteralExpr *gteRight = new LiteralExpr(0L, LongType());
    BinaryExpr *gteExpr =
        new BinaryExpr(omniruntime::expressions::Operator::GTE, new FieldExpr(3, LongType()), gteRight, BooleanType());
    BinaryExpr *orExpr1 = new BinaryExpr(omniruntime::expressions::Operator::OR, lteExpr, gteExpr, BooleanType());

    BinaryExpr *ltExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, IntType()),
        new LiteralExpr(50, IntType()), BooleanType());
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(1, IntType()),
        new LiteralExpr(-12, IntType()), BooleanType());
    BinaryExpr *orExpr2 = new BinaryExpr(omniruntime::expressions::Operator::OR, ltExpr, eqExpr, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, orExpr1, orExpr2, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 3498);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter6));

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, LogicalOperators3)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), DoubleType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LogicalOperators3");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);
    const int32_t projectCount = 2;

    // projections
    FieldExpr *col0Expr = new FieldExpr(0, IntType());
    FieldExpr *col1Expr = new FieldExpr(1, IntType());
    std::vector<Expr *> projections = { col1Expr, col0Expr };

    BinaryExpr *eq1Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new LiteralExpr(55, IntType()),
        new FieldExpr(0, IntType()), BooleanType());
    BinaryExpr *eq2Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new LiteralExpr(5, IntType()),
        new FieldExpr(0, IntType()), BooleanType());
    BinaryExpr *or1Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, eq1Expr, eq2Expr, BooleanType());

    BinaryExpr *eq3Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, IntType()),
        new LiteralExpr(8, IntType()), BooleanType());
    BinaryExpr *or2Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, or1Expr, eq3Expr, BooleanType());

    BinaryExpr *eq4Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, IntType()),
        new LiteralExpr(13, IntType()), BooleanType());
    BinaryExpr *or3Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, or2Expr, eq4Expr, BooleanType());


    BinaryExpr *eq5Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, IntType()),
        new LiteralExpr(1, IntType()), BooleanType());
    BinaryExpr *eq6Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, IntType()),
        new LiteralExpr(2, IntType()), BooleanType());
    BinaryExpr *or4Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, eq5Expr, eq6Expr, BooleanType());

    BinaryExpr *eq7Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, IntType()),
        new LiteralExpr(3, IntType()), BooleanType());
    BinaryExpr *or5Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, or4Expr, eq7Expr, BooleanType());

    BinaryExpr *or6Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, or5Expr, or3Expr, BooleanType());

    LiteralExpr *neqRight = new LiteralExpr(0L, LongType());
    BinaryExpr *neqExpr =
        new BinaryExpr(omniruntime::expressions::Operator::NEQ, new FieldExpr(1, LongType()), neqRight, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, neqExpr, or6Expr, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 6);
    for (int32_t i = 0; i < 6; i++) {
        double val0 = ((DoubleVector *)ret[0]->GetVector(0))->GetValue(i);
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_TRUE(val0 != 0);
        EXPECT_TRUE(val1 == col1[i + 2]);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, ArithmeticAdd)
{
    const int32_t numCols = 1;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 5;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_ArithmeticAdd");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);

    const int32_t projectCount = 1;

    std::vector<Expr *> projections = { new FieldExpr(0, IntType()) };

    // filter
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, new FieldExpr(0, IntType()),
        new LiteralExpr(1, IntType()), IntType());
    BinaryExpr *filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::GT, addExpr, new LiteralExpr(4, IntType()), BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 + 1 > 4);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, ArithmeticSubtract)
{
    const int32_t numCols = 2;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int64_t *col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 10;
        col2[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_ArithmeticSubtract");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);

    const int32_t projectCount = 2;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, LongType()) };

    BinaryExpr *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, new FieldExpr(0, IntType()),
        new LiteralExpr(5, IntType()), IntType());
    BinaryExpr *filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::LT, new LiteralExpr(0, IntType()), subExpr, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 - 5 > 0);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, ArithmeticMultiply)
{
    const int32_t numCols = 2;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    int64_t *col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 10 + 1;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), LongType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_ArithmeticMultiply");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);

    const int32_t projectCount = 2;

    BinaryExpr *mul1Expr = new BinaryExpr(omniruntime::expressions::Operator::MUL, new FieldExpr(0, IntType()),
        new FieldExpr(0, IntType()), IntType());
    BinaryExpr *eqExpr =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, new LiteralExpr(0, IntType()), mul1Expr, BooleanType());

    LiteralExpr *mulLeft = new LiteralExpr(2L, LongType());
    BinaryExpr *mul2Expr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, new FieldExpr(1, LongType()), LongType());
    LiteralExpr *gtLeft = new LiteralExpr(7L, LongType());
    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft, mul2Expr, BooleanType());
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, eqExpr, gtExpr, BooleanType());

    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, LongType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, 0);
        EXPECT_TRUE(val1 * 2 < 7);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, Conditional)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Conditional");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);
    const int32_t projectCount = 3;

    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
        new FieldExpr(2, IntType()) };

    // filters
    BinaryExpr *condition = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, IntType()),
        new LiteralExpr(0, IntType()), BooleanType());
    BinaryExpr *texp = new BinaryExpr(omniruntime::expressions::Operator::ADD, new FieldExpr(1, IntType()),
        new LiteralExpr(5, IntType()), IntType());
    FieldExpr *fexp = new FieldExpr(2, IntType());

    IfExpr *eqLeft = new IfExpr(condition, texp, fexp);

    BinaryExpr *filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, new LiteralExpr(55, IntType()), BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 5000);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, Conditional2)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Conditional2");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);

    // filters
    BinaryExpr *condition = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, IntType()),
        new LiteralExpr(0, IntType()), BooleanType());
    BinaryExpr *texp = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(1, IntType()),
        new LiteralExpr(3, IntType()), BooleanType());
    BinaryExpr *fexp = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(1, IntType()),
        new LiteralExpr(4, IntType()), BooleanType());
    IfExpr *ifExpr = new IfExpr(condition, texp, fexp);

    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, new FieldExpr(2, IntType()),
        new LiteralExpr(3, IntType()), BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, ifExpr, gtExpr, BooleanType());

    // filters
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
        new FieldExpr(2, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}


TEST(FilterTest, In)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_In");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);
    // filter
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, IntType()));
    args.push_back(new LiteralExpr(1, IntType()));
    args.push_back(new LiteralExpr(3, IntType()));
    args.push_back(new LiteralExpr(5, IntType()));

    InExpr *filterExpr = new InExpr(args);

    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
        new FieldExpr(2, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 3000);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 == 1 || val0 == 3 || val0 == 5);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, testLongIn)
{
    const int32_t numCols = 3;
    const int32_t numRows = 10000;
    int64_t *col1 = new int64_t[numRows];
    int64_t *col2 = new int64_t[numRows];
    int64_t *col3 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 10;
        col2[i] = i % 5;
        col3[i] = i % 6 + 12;
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
                                    reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypePtr>({ LongType(), LongType(), LongType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_In");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    // filter
    std::vector<Expr *> args;
    int64_t target1 = 1;
    int64_t target2 = 3;
    int64_t target3 = 5;
    args.push_back(new FieldExpr(0, LongType()));
    args.push_back(new LiteralExpr(target1, LongType()));
    args.push_back(new LiteralExpr(target2, LongType()));
    args.push_back(new LiteralExpr(target3, LongType()));

    InExpr *filterExpr = new InExpr(args);

    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, LongType()), new FieldExpr(1, LongType()),
        new FieldExpr(2, LongType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 3000);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((LongVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 == target1 || val0 == target2 || val0 == target3);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
}

TEST(FilterTest, testDoubleIn)
{
    const int32_t numCols = 3;
    const int32_t numRows = 10000;
    double *col1 = new double[numRows];
    double *col2 = new double[numRows];
    double *col3 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 10;
        col2[i] = i % 5;
        col3[i] = i % 6 + 12;
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
                                    reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypePtr>({ DoubleType(), DoubleType(), DoubleType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_In");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    // filter
    std::vector<Expr *> args;
    double target1 = 1.0;
    double target2 = 3.0;
    double target3 = 5.0;
    args.push_back(new FieldExpr(0, DoubleType()));
    args.push_back(new LiteralExpr(target1, DoubleType()));
    args.push_back(new LiteralExpr(target2, DoubleType()));
    args.push_back(new LiteralExpr(target3, DoubleType()));

    InExpr *filterExpr = new InExpr(args);

    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, DoubleType()), new FieldExpr(1, DoubleType()),
        new FieldExpr(2, DoubleType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 3000);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((DoubleVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val0 == target1 || val0 == target2 || val0 == target3);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
}

TEST(FilterTest, testStringIn1)
{
    const int32_t numCols = 1;
    const int32_t numRows = 10;
    vector<string> strings;
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 3 == 0) {
            strings.emplace_back("hello");
        } else {
            strings.emplace_back("hi");
        }
    }
    vector<bool> nulls;
    for (int32_t i = 0; i < numRows; i++) {
        nulls.emplace_back(false);
    }
    DataTypes inputTypes(std::vector<DataTypePtr>({ VarcharType(10) }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_StringIn1");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *t = CreateVectorBatch(numRows, cols);
    // filter
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, VarcharType()));
    args.push_back(new LiteralExpr(new std::string("hello"), VarcharType()));
    args.push_back(new LiteralExpr(new std::string("bye"), VarcharType()));
    args.push_back(new LiteralExpr(new std::string("okay"), VarcharType()));

    InExpr *filterExpr = new InExpr(args);

    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, VarcharType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);

    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
}

TEST(FilterTest, testStringIn2)
{
    const int32_t numCols = 1;
    const int32_t numRows = 10000;
    vector<string> strings;
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            strings.emplace_back("hello");
        } else {
            strings.emplace_back("hi");
        }
    }
    vector<bool> nulls;
    for (int32_t i = 0; i < numRows; i++) {
        nulls.emplace_back(false);
    }
    DataTypes inputTypes(std::vector<DataTypePtr>({ CharType(10) }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_StringIn2");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *t = CreateVectorBatch(numRows, cols);
    // filter
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, CharType()));
    args.push_back(new LiteralExpr(new std::string("hello"), CharType()));
    args.push_back(new LiteralExpr(new std::string("bye"), CharType()));
    args.push_back(new LiteralExpr(new std::string("okay"), CharType()));

    InExpr *filterExpr = new InExpr(args);

    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, CharType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 5000);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);

    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
}

TEST(FilterTest, testDecimal128In)
{
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(data1)};
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_decimal128In");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    // filter
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, Decimal128Type(38, 0)));
    args.push_back(new LiteralExpr(new std::string("1000"), Decimal128Type(38, 0)));
    args.push_back(new LiteralExpr(new std::string("2000"), Decimal128Type(38, 0)));
    args.push_back(new LiteralExpr(new std::string("555555"), Decimal128Type(38, 0)));


    InExpr *filterExpr = new InExpr(args);

    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, Decimal128Type(38, 0)) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);

    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
}

TEST(FilterTest, Between)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Between");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);

    BetweenExpr *filterExpr =
        new BetweenExpr(new FieldExpr(1, IntType()), new FieldExpr(0, IntType()), new FieldExpr(2, IntType()));
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
        new FieldExpr(2, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4705);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        int32_t val2 = ((IntVector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_TRUE((val0 <= val1) && (val1 <= val2));
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, NotEqualToAbs)
{
    const int32_t numCols = 1;
    const int32_t numRows = 100000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i - 32435;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_NotEqualToAbs");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);

    // filter
    DataTypePtr retType = IntType();
    std::string funcStr = "abs";
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, IntType()));
    auto absExpr = GetFuncExpr(funcStr, args, IntType());

    auto filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::NEQ, absExpr, new LiteralExpr(4, IntType()), BooleanType());
    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 99998);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}


// Function tests
TEST(FilterTest, MathFunctionFilter1)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_MathFunctionFilter1");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);

    // filters
    DataTypePtr retType = IntType();
    std::string funcStr = "abs";
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, IntType()));
    auto abs1Expr = GetFuncExpr(funcStr, args1, IntType());

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(2, IntType()));
    auto abs2Expr = GetFuncExpr(funcStr, args2, IntType());
    auto eq1Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, abs1Expr, abs2Expr, BooleanType());

    std::vector<Expr *> args3;
    args3.push_back(new FieldExpr(0, IntType()));
    auto abs3Expr = GetFuncExpr(funcStr, args3, IntType());

    std::vector<Expr *> args4;
    args4.push_back(new FieldExpr(1, IntType()));
    auto abs4Expr = GetFuncExpr(funcStr, args4, IntType());
    auto eq2Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, abs3Expr, abs4Expr, BooleanType());

    auto filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, eq1Expr, eq2Expr, BooleanType());

    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
        new FieldExpr(2, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 1000);
    std::cout << "numReturned: " << numReturned << std::endl;
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        int32_t val2 = ((IntVector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_TRUE((std::abs(val0) == std::abs(val1)) && (std::abs(val1) == std::abs(val2)));
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}


// For testing different types
TEST(FilterTest, MathFunctionFilter2)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), LongType(), IntType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_MathFunctionFilter2");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);

    // filters
    std::string castStr = "CAST";
    DataTypePtr retType = DoubleType();
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, IntType()));
    auto cast1 = GetFuncExpr(castStr, args1, DoubleType());

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(1, LongType()));
    auto cast2 = GetFuncExpr(castStr, args2, DoubleType());

    auto filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, cast1, cast2, BooleanType());

    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, LongType()),
        new FieldExpr(2, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2000);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

// String Filter and varcharvec testing
TEST(FilterTest, FilterString1)
{
    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    vector<string> strings;
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 40 == 0) {
            strings.emplace_back("hello");
        } else {
            strings.emplace_back("abcdefghijklmnopqrstuvwxyz");
        }
    }
    vector<bool> nulls;
    for (int32_t i = 0; i < numRows; i++) {
        nulls.emplace_back(false);
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ VarcharType(30) }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_FilterString1");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *t = CreateVectorBatch(numRows, cols);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, VarcharType()),
        new LiteralExpr(new std::string("hello"), VarcharType()), BooleanType());
    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, VarcharType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 25);
    for (int32_t i = 0; i < numReturned; i++) {
        VarcharVector *vcVec = ((VarcharVector *)ret[0]->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);

        // Truncate the resulting string
        void *charArr = &actualChar;
        auto charArrCasted = static_cast<char **>(charArr);
        string actualStr(*charArrCasted, 0, len);
        ASSERT_STREQ(actualStr.c_str(), "hello");
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}


TEST(FilterTest, Coalesce1)
{
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Coalesce1");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 != 0) {
            t->GetVector(1)->SetValueNull(i);
        } else {
            t->GetVector(1)->SetValueNotNull(i);
        }
    }

    CoalesceExpr *coalesceExpr = new CoalesceExpr(new FieldExpr(1, IntType()), new FieldExpr(0, IntType()));
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new LiteralExpr(21, IntType()),
        coalesceExpr, BooleanType());
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
        new FieldExpr(2, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 500);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, Coalesce2)
{
    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    vector<string> strings;
    for (int32_t i = 0; i < numRows; i++) {
        strings.emplace_back("hello");
    }
    vector<bool> nulls;
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 != 0) {
            nulls.emplace_back(true);
        } else {
            nulls.emplace_back(false);
        }
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ VarcharType(30) }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Coalesce2");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *t = CreateVectorBatch(numRows, cols);

    CoalesceExpr *coalesceExpr =
        new CoalesceExpr(new FieldExpr(0, VarcharType()), new LiteralExpr(new std::string("bye"), VarcharType()));
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, coalesceExpr,
        new LiteralExpr(new std::string("hello"), VarcharType()), BooleanType());
    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, VarcharType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 500);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);

    VectorHelper::FreeVecBatches(ret);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, Coalesce3)
{
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

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(data1)};
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Coalesce3");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, Decimal128Type(38, 0)) };
    auto v1 = new LiteralExpr(new std::string("500000"), Decimal128Type(38, 0));
    v1->isNull = false;
    auto v2 = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 3));
    auto coalesce = new CoalesceExpr(v1, v2);
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LTE,
        new FieldExpr(0, Decimal128Type(38, 0)), coalesce, BooleanType());
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter7));
    EXPECT_EQ(numReturned, 500);


    allData[0] = reinterpret_cast<int64_t>(data2);
    VectorBatch *in2 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(in2);
    numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[1], numReturned, Filter7));
    EXPECT_EQ(numReturned, 1000);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] data1;
    delete[] data2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
}

TEST(FilterTest, Coalesce4)
{
    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    vector<string> strings;
    for (int32_t i = 0; i < numRows; i++) {
        strings.emplace_back("hello");
    }
    vector<bool> nulls;
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 != 0) {
            nulls.emplace_back(true);
        } else {
            nulls.emplace_back(false);
        }
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ CharType(5) }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Coalesce4");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *t = CreateVectorBatch(numRows, cols);

    CoalesceExpr *coalesceExpr =
        new CoalesceExpr(new FieldExpr(0, CharType()), new LiteralExpr(new std::string("world"), CharType()));
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, coalesceExpr,
        new LiteralExpr(new std::string("hello"), CharType()), BooleanType());
    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, CharType()) };

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 500);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);

    VectorHelper::FreeVecBatches(ret);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
}


TEST(FilterTest, Coalesce5)
{
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
                                        reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypePtr>({ LongType(), LongType(), LongType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Coalesce5");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 != 0) {
            t->GetVector(1)->SetValueNull(i);
        } else {
            t->GetVector(1)->SetValueNotNull(i);
        }
    }
    int64_t targetValue = 21;
    CoalesceExpr *coalesceExpr = new CoalesceExpr(new FieldExpr(1, LongType()), new FieldExpr(0, LongType()));
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ,
        new LiteralExpr(targetValue, LongType()), coalesceExpr, BooleanType());
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, LongType()), new FieldExpr(1, LongType()),
        new FieldExpr(2, LongType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 500);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
}

TEST(FilterTest, Coalesce6)
{
    const int32_t numCols = 3;
    const int32_t numRows = 1000;
    double *col1 = new double[numRows];
    double *col2 = new double[numRows];
    double *col3 = new double[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = 100.0;
        col2[i] = 21.0;
        col3[i] = -1.0;
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
                                        reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypePtr>({ DoubleType(), DoubleType(), DoubleType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Coalesce6");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 != 0) {
            t->GetVector(1)->SetValueNull(i);
        } else {
            t->GetVector(1)->SetValueNotNull(i);
        }
    }
    CoalesceExpr *coalesceExpr = new CoalesceExpr(new FieldExpr(1, DoubleType()), new FieldExpr(0, DoubleType()));
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new LiteralExpr(21.0, DoubleType()),
        coalesceExpr, BooleanType());
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, DoubleType()), new FieldExpr(1, DoubleType()),
        new FieldExpr(2, DoubleType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 500);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
}


TEST(FilterTest, ExternalMathFunc)
{
    const int32_t numCols = 2;
    const int32_t numRows = 1000;
    int32_t *col1 = new int32_t[numRows];
    int32_t *col2 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
        col2[i] = i + 2;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_ExternalMathFunc");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);
    // filter
    std::string funcStr = "Increment";
    DataTypePtr retType = IntType();
    auto col0 = new FieldExpr(0, IntType());
    auto add1Int1Expr = GetFuncExpr(funcStr, vector<Expr *> { col0 }, IntType());
    auto eqLeft = GetFuncExpr(funcStr, vector<Expr *> { add1Int1Expr }, IntType());
    auto eqRight = new FieldExpr(1, IntType());

    auto filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());

    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory = new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections,
        projections.size(), overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, numRows);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}


TEST(FilterTest, ExternalStringFunc)
{
    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    vector<string> strings;
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            strings.emplace_back("hello");
        } else {
            if (i % 4 == 1) {
                strings.emplace_back("bye");
            } else {
                strings.emplace_back("asdf");
            }
        }
    }
    vector<bool> nulls;
    for (int32_t i = 0; i < numRows; i++) {
        nulls.emplace_back(false);
    }

    // column looks like:
    // hello, bye, hello, bye, hello, bye, ...
    DataTypes inputTypes(std::vector<DataTypePtr>({ VarcharType(30) }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_ExternalStringFunc");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *t = CreateVectorBatch(numRows, cols);

    std::string funcStr = "stringLength";
    DataTypePtr retType = IntType();
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, VarcharType()));
    auto eqLeft = GetFuncExpr(funcStr, args, IntType());
    auto filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, new LiteralExpr(5, IntType()), BooleanType());

    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, VarcharType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 500);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

// Testing multithreading
// Two operators running at once

void process(omniruntime::op::Operator *op, VectorBatch *t, std::vector<VectorBatch *> ret, int32_t *numReturned)
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
TEST(FilterTest, Multithreading)
{
    const int32_t numCols = 3;
    const int32_t numRows = 100000;
    int32_t *col1 = new int32_t[numRows];
    int64_t *col2 = new int64_t[numRows];
    int32_t *col3 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = -1;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), LongType(), IntType() }));
    DataTypes inputTypes2(std::vector<DataTypePtr>({ IntType(), LongType(), IntType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Multithreading");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);
    VectorBatch *t2 = CreateVectorBatch(inputTypes2, numRows, col1, col2, col3);

    std::vector<VectorBatch *> ret;
    std::vector<VectorBatch *> ret2;
    int32_t *numReturned = new int32_t;
    int32_t *numReturned2 = new int32_t;

    // find wall clock time
    auto start = std::chrono::high_resolution_clock::now();

    // filters
    std::string castStr = "CAST";
    std::string absStr = "abs";
    DataTypePtr retType = DoubleType();
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, IntType()));
    auto cast1Expr = GetFuncExpr(castStr, args1, DoubleType());
    std::vector<Expr *> args2;
    args2.push_back(cast1Expr);
    auto eqLeft = GetFuncExpr(absStr, args2, DoubleType());

    std::vector<Expr *> args3;
    args3.push_back(new FieldExpr(1, LongType()));
    auto cast2Expr = GetFuncExpr(castStr, args3, DoubleType());
    std::vector<Expr *> args4;
    args4.push_back(cast2Expr);
    auto eqRight = GetFuncExpr(absStr, args4, DoubleType());

    auto filterExpr1 = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());

    const int32_t projectCount = 3;
    std::vector<Expr *> projections1 = { new FieldExpr(0, IntType()), new FieldExpr(1, LongType()),
        new FieldExpr(2, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory = new FilterAndProjectOperatorFactory(filterExpr1, inputTypes, numCols, projections1,
        projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    std::thread thread1(process, op, t, ret, numReturned);

    // filter2
    auto eqRight2 = new LiteralExpr(4L, LongType());
    auto filterExpr2 =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(1, LongType()), eqRight2, BooleanType());
    std::vector<Expr *> projections2 = { new FieldExpr(0, IntType()), new FieldExpr(1, LongType()),
        new FieldExpr(2, IntType()) };

    OperatorFactory *factory2 =
        new FilterAndProjectOperatorFactory(filterExpr2, inputTypes2, numCols, projections2, 3, overflowConfig);
    omniruntime::op::Operator *op2 = factory2->CreateOperator();
    std::thread thread2(process, op2, t2, ret2, numReturned2);

    thread2.join();
    thread1.join();
    EXPECT_EQ(*numReturned, 20000);
    EXPECT_EQ(*numReturned2, 20000);

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Total time for multithreading test: ";
    std::cout << std::chrono::duration<double, std::milli>(end - start).count() << std::endl;

    Expr::DeleteExprs({ filterExpr1 });
    Expr::DeleteExprs(projections1);
    Expr::DeleteExprs({ filterExpr2 });
    Expr::DeleteExprs(projections2);
    VectorHelper::FreeVecBatches(ret);
    VectorHelper::FreeVecBatches(ret2);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    delete op2;
    DeleteOperatorFactory(factory);
    delete factory2;
    delete numReturned;
    delete numReturned2;
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, TestFilterDictionaryVec)
{
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_TestFilterDictionaryVec");
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryVector);

    BetweenExpr *filterExpr =
        new BetweenExpr(new FieldExpr(1, IntType()), new FieldExpr(0, IntType()), new FieldExpr(2, IntType()));
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, LongType()),
        new FieldExpr(2, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorBatch *copiedBatch = DuplicateVectorBatch(batch);
    op->AddInput(copiedBatch);
    std::vector<VectorBatch *> ret;
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

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);
    delete col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(FilterTest, TestFilterDictionaryVarchar)
{
    const int32_t numCols = 2;
    const int32_t numRows = 3;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_TestFilterDictionaryVarchar");
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    VarcharVector *col2 = new VarcharVector(vecAllocator, 1024, numRows);
    int32_t ids[] = {0, 1, 2};
    DictionaryVector *dictionaryVector = new DictionaryVector(col2, ids, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i * 3);
        std::string tmp = "test";
        col2->SetValue(i, reinterpret_cast<const uint8_t *>(tmp.c_str()), tmp.length());
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), VarcharType(50) }));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->SetVector(0, col1);
    batch->SetVector(1, dictionaryVector);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, IntType()),
        new LiteralExpr(6, IntType()), BooleanType());
    const int32_t projectCount = 2;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, VarcharType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorBatch *copiedBatch = DuplicateVectorBatch(batch);
    op->AddInput(copiedBatch);
    std::vector<VectorBatch *> ret;
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

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);
    delete col2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(FilterTest, TestFilterDictionaryVecNested)
{
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_TestFilterDictionaryVecNested");
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryNested);

    BetweenExpr *filterExpr =
        new BetweenExpr(new FieldExpr(1, IntType()), new FieldExpr(0, IntType()), new FieldExpr(2, IntType()));
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
        new FieldExpr(2, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorBatch *copiedBatch = DuplicateVectorBatch(batch);
    op->AddInput(copiedBatch);
    std::vector<VectorBatch *> ret;
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

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);
    delete col3;
    delete dictionaryVector;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(FilterTest, DecimalFilterBinaryTest)
{
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

    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_DecimalFilterBinaryTest");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, data1);

    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, Decimal128Type(38, 0)) };
    LiteralExpr *lteRight = new LiteralExpr(new std::string("500000"), Decimal128Type(38, 0));
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LTE,
        new FieldExpr(0, Decimal128Type(38, 0)), lteRight, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter7));
    EXPECT_EQ(numReturned, 500);

    VectorBatch *in2 = CreateVectorBatch(inputTypes, numRows, data2);
    op->AddInput(in2);
    numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[1], numReturned, Filter7));
    EXPECT_EQ(numReturned, 1000);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] data1;
    delete[] data2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, DecimalFilterAbsTest)
{
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

    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type(), Decimal128Type(), Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_DecimalFilterAbsTest");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, data1, data2, data3);

    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, Decimal128Type(38, 0)),
        new FieldExpr(1, Decimal128Type(38, 0)), new FieldExpr(2, Decimal128Type(38, 0)) };

    // filters
    std::string absStr = "abs";
    DataTypePtr retType = Decimal128Type(38, 0);
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, Decimal128Type(38, 0)));
    auto absExpr1 = GetFuncExpr(absStr, args1, Decimal128Type(38, 0));

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(2, Decimal128Type(38, 0)));
    auto absExpr2 = GetFuncExpr(absStr, args2, Decimal128Type(38, 0));

    BinaryExpr *eqExpr1 = new BinaryExpr(omniruntime::expressions::Operator::EQ, absExpr1, absExpr2, BooleanType());

    std::vector<Expr *> args3;
    args3.push_back(new FieldExpr(1, Decimal128Type(38, 0)));
    auto absExpr3 = GetFuncExpr(absStr, args3, Decimal128Type(38, 0));

    std::vector<Expr *> args4;
    args4.push_back(new FieldExpr(2, Decimal128Type(38, 0)));
    auto absExpr4 = GetFuncExpr(absStr, args4, Decimal128Type(38, 0));

    BinaryExpr *eqExpr2 = new BinaryExpr(omniruntime::expressions::Operator::EQ, absExpr3, absExpr4, BooleanType());
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, eqExpr1, eqExpr2, BooleanType());
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();

    auto start = std::chrono::system_clock::now();
    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    auto end = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "BenchmarkDecimalColumn round - elapsed: " << elapsed.count() << " ms\n";
    EXPECT_EQ(numReturned, 1000);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    delete[] data1;
    delete[] data2;
    delete[] data3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, FilterStringWithNull)
{
    const int32_t numCols = 1;
    const int32_t numRows = 2;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_FilterStringWithNull");
    VarcharVector *col0 = new VarcharVector(vecAllocator, 1024, numRows);
    std::string str = "hello";
    col0->SetValue(0, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    col0->SetValueNull(1);

    DataTypes inputTypes(std::vector<DataTypePtr>({ VarcharType(1000) }));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->SetVector(0, col0);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, VarcharType()),
        new LiteralExpr(new std::string("hello"), VarcharType()), BooleanType());
    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, VarcharType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(batch);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 1);

    for (int32_t i = 0; i < numReturned; i++) {
        VarcharVector *vcVec = ((VarcharVector *)ret[0]->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);
        std::string actualStr(actualChar, actualChar + len);
        EXPECT_EQ(actualStr, "hello");
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatches(ret);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(FilterTest, TestFilterSlicedDictionaryVec)
{
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_TestFilterSlicedDictionaryVec");
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    VectorBatch *intput = new VectorBatch(numCols, slicedCol1->GetSize());
    intput->SetVector(0, slicedCol1);
    intput->SetVector(1, slicedCol2);
    intput->SetVector(2, slicedCol3);

    BetweenExpr *filterExpr =
        new BetweenExpr(new FieldExpr(1, IntType()), new FieldExpr(0, IntType()), new FieldExpr(2, IntType()));
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
        new FieldExpr(2, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorBatch *copiedBatch = DuplicateVectorBatch(intput);
    op->AddInput(copiedBatch);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 2);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, slicedCol1->GetValue(i));
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, slicedCol2->GetValue(i));
        int32_t val2 = ((IntVector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, slicedCol3->GetInt(i));
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatch(intput);
    VectorHelper::FreeVecBatches(ret);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(FilterTest, TestFilterSlicedDictionaryVecWithNull)
{
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_TestFilterSlicedDictionaryVecWithNull");
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

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType() }));
    VectorBatch *intput = new VectorBatch(numCols, slicedCol1->GetSize());
    intput->SetVector(0, slicedCol1);
    intput->SetVector(1, slicedCol2);
    intput->SetVector(2, slicedCol3);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(2, IntType()),
        new LiteralExpr(6, IntType()), BooleanType());
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, IntType()), new FieldExpr(1, IntType()),
        new FieldExpr(2, IntType()) };
    auto overflowConfig = new OverflowConfig();
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount, overflowConfig);
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorBatch *copiedBatch = DuplicateVectorBatch(intput);
    op->AddInput(copiedBatch);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 4);
    for (int i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, slicedCol1->GetValue(i + 2));
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, slicedCol2->GetValue(i + 2));
        int32_t val2 = ((IntVector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, slicedCol3->GetInt(i + 2));
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    VectorHelper::FreeVecBatch(intput);
    VectorHelper::FreeVecBatches(ret);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(FilterTest, SimpleFilter)
{
    const int32_t numRows = 5000;
    auto col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_SimpleFilter");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, col1);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, IntType()),
        new LiteralExpr(2000, IntType()), BooleanType());
    auto overflowConfig = new OverflowConfig();
    auto filter = new SimpleFilter(*filterExpr);
    bool initialized = filter->Initialize(overflowConfig);
    EXPECT_TRUE(initialized);

    ExecutionContext context;
    auto vector = (IntVector *)in1->GetVector(0);
    int64_t values[1];
    bool isNulls[1];
    for (int i = 0; i < numRows; i++) {
        values[0] = VectorHelper::GetValuesAddr(vector) + i * sizeof(int32_t);
        isNulls[0] = vector->IsValueNull(i);
        bool result = filter->Evaluate(values, isNulls, nullptr, (int64_t)(&context));
        if (i < 2000) {
            EXPECT_TRUE(result);
        } else {
            EXPECT_FALSE(result);
        }
    }
    Expr::DeleteExprs({ filterExpr });
    VectorHelper::FreeVecBatch(in1);
    delete filter;
    delete[] col1;
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, SimpleFilterWithNulls)
{
    const int32_t numRows = 5000;
    auto col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_SimpleFilterWithNulls");
    VectorBatch *in1 = CreateVectorBatch(inputTypes, numRows, col1);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, IntType()),
        new LiteralExpr(2000, IntType()), BooleanType());
    auto overflowConfig = new OverflowConfig();
    auto filter = new SimpleFilter(*filterExpr);
    bool initialized = filter->Initialize(overflowConfig);
    EXPECT_TRUE(initialized);

    // set first 500 elements to null
    auto vector = (IntVector *)in1->GetVector(0);
    for (int i = 0; i < 500; i++) {
        vector->SetValueNull(i);
    }

    ExecutionContext context;
    int64_t values[1];
    bool isNulls[1];
    for (int i = 0; i < numRows; i++) {
        values[0] = VectorHelper::GetValuesAddr(vector) + i * sizeof(int32_t);
        isNulls[0] = vector->IsValueNull(i);
        bool result = filter->Evaluate(values, isNulls, nullptr, (int64_t)(&context));
        if (i >= 500 && i < 2000) {
            EXPECT_TRUE(result);
        } else {
            EXPECT_FALSE(result);
        }
    }
    Expr::DeleteExprs({ filterExpr });
    VectorHelper::FreeVecBatch(in1);
    delete filter;
    delete[] col1;
    delete vectorAllocator;
    delete overflowConfig;
}

TEST(FilterTest, SimpleFilterIntWithNulls)
{
    const int32_t numRows = 10;
    int32_t data0[numRows] = {19, 14, 7, 19, 1, 20, 10, 13, 20, 16};
    int32_t data1[numRows] = {20, 16, 13, 4, 20, 4, 22, 19, 8, 7};

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType() }));
    auto vecBatch = CreateVectorBatch(inputTypes, numRows, data0, data1);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, IntType()),
        new FieldExpr(1, IntType()), BooleanType());
    auto overflowConfig = new OverflowConfig();
    auto filter = new SimpleFilter(*filterExpr);
    bool initialized = filter->Initialize(overflowConfig);
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
        values[0] = reinterpret_cast<int64_t>(((int32_t *)vector0->GetValues()) + i);
        isNulls[0] = vector0->IsValueNull(i);
        values[1] = reinterpret_cast<int64_t>(((int32_t *)vector1->GetValues()) + i);
        isNulls[1] = vector1->IsValueNull(i);
        bool result = filter->Evaluate(values, isNulls, nullptr, (int64_t)(&context));
        EXPECT_FALSE(result);
    }
    Expr::DeleteExprs({ filterExpr });
    VectorHelper::FreeVecBatch(vecBatch);
    delete filter;
    delete overflowConfig;
}

TEST(FilterTest, SimpleFilterCharWithNulls)
{
    const int32_t numRows = 9;
    std::string data0[numRows] = {"35709", "35709", "35709", "31904", "", "", "35709", "35709", ""};
    std::string data1[numRows] = {"31904", "35709", "31904", "31904", "31904", "35709", "35709", "31904", "35709"};
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_SimpleFilterCharWithNulls");
    auto vec0 = new VarcharVector(vecAllocator, 5 * numRows, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        if (data0[i].compare("") == 0) {
            vec0->SetValueNull(i);
        } else {
            vec0->SetValue(i, (uint8_t *)(data0[i].c_str()), data0[i].length());
        }
    }
    VarcharDataType type(5);
    auto vec1 = CreateVarcharVector(type, data1, numRows);
    auto vecBatch = new VectorBatch(2, numRows);
    vecBatch->SetVector(0, vec0);
    vecBatch->SetVector(1, vec1);

    DataTypes inputTypes(std::vector<DataTypePtr>({ VarcharType(5), VarcharType(5) }));
    // filter expression object
    std::string funcStr = "substr";
    DataTypePtr retType = VarcharType();
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, VarcharType()));
    args1.push_back(new LiteralExpr(1, IntType()));
    args1.push_back(new LiteralExpr(5, IntType()));
    auto substrExpr1 = GetFuncExpr(funcStr, args1, VarcharType());

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(1, VarcharType()));
    args2.push_back(new LiteralExpr(1, IntType()));
    args2.push_back(new LiteralExpr(5, IntType()));
    auto substrExpr2 = GetFuncExpr(funcStr, args2, VarcharType());
    auto filterExpr = new BinaryExpr(omniruntime::expressions::Operator::NEQ, substrExpr1, substrExpr2, BooleanType());
    auto overflowConfig = new OverflowConfig();
    auto filter = new SimpleFilter(*filterExpr);
    bool initialized = filter->Initialize(overflowConfig);
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

        bool result = filter->Evaluate(values, isNulls, lengths, (int64_t)(&context));
        if (i == 0 || i == 2 || i == 7) {
            EXPECT_TRUE(result);
        } else {
            EXPECT_FALSE(result);
        }
    }
    Expr::DeleteExprs({ filterExpr });
    delete filter;
    VectorHelper::FreeVecBatch(vecBatch);
    delete vecAllocator;
    delete overflowConfig;
}
}
}