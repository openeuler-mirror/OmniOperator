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
VectorBatch *CreateInput(VectorAllocator *vectorAllocator, const int32_t numRows, const int32_t numCols,
    const int32_t *inputTypeIds, int64_t *allData)
{
    auto *vecBatch = new VectorBatch(numCols, numRows);
    vector<omniruntime::type::DataTypePtr> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    vecBatch->NewVectors(vectorAllocator, inputTypes);
    for (int i = 0; i < numCols; ++i) {
        switch (inputTypeIds[i]) {
            case OMNI_INT:
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], numRows);
                break;
            case OMNI_LONG:
                ((LongVector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *)allData[i], numRows);
                break;
            case OMNI_DOUBLE:
                ((DoubleVector *)vecBatch->GetVector(i))->SetValues(0, (double *)allData[i], numRows);
                break;
            case OMNI_SHORT:
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], numRows);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR: {
                for (int j = 0; j < numRows; ++j) {
                    int64_t addr = reinterpret_cast<int64_t *>(allData[i])[j];
                    std::string s(reinterpret_cast<char *>(addr));
                    ((VarcharVector *)vecBatch->GetVector(i))
                        ->SetValue(j, reinterpret_cast<const uint8_t *>(s.c_str()), s.length());
                }
                break;
            }
            case OMNI_DECIMAL128:
                ((Decimal128Vector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *)allData[i], numRows);
                break;
            default: {
                LogError("No such data type %d", inputTypeIds[i]);
                break;
            }
        }
    }
    return vecBatch;
}

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

    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType() }));
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LessThan");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()) };
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(2000, new IntDataType()), new BooleanDataType());
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
}

TEST(FilterTest, LessThanWihtoutParsing)
{
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType() }));
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LessThanWihtoutParsing");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;
    FieldExpr *column = new FieldExpr(0, new IntDataType());
    FieldExpr *left = new FieldExpr(0, new IntDataType());
    LiteralExpr *right = new LiteralExpr(2000, new IntDataType());
    BinaryExpr *LTExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, left, right, new BooleanDataType());

    std::vector<Expr *> projections = { column };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(LTExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new LongDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_GreaterThan");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 2;
    FieldExpr *col0Expr = new FieldExpr(0, new IntDataType());
    FieldExpr *col1Expr = new FieldExpr(1, new LongDataType());
    std::vector<Expr *> projections = { col0Expr, col1Expr };

    FieldExpr *gtLeft = new FieldExpr(0, new IntDataType());
    LiteralExpr *gtRight = new LiteralExpr(20, new IntDataType());
    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft, gtRight, new BooleanDataType());

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(gtExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col3),
        reinterpret_cast<int64_t>(col2)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new LongDataType(), new DoubleDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_EqualTo");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 2;

    FieldExpr *col1Expr = new FieldExpr(1, new LongDataType());
    FieldExpr *col2Expr = new FieldExpr(2, new DoubleDataType());
    std::vector<Expr *> projections = { col2Expr, col1Expr };

    FieldExpr *eqLeft = new FieldExpr(2, new DoubleDataType());
    LiteralExpr *eqRight = new LiteralExpr(50.0, new DoubleDataType());

    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, new BooleanDataType());

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(eqExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_GreaterThanOrEqualTo");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;

    FieldExpr *col1Expr = new FieldExpr(1, new IntDataType());
    std::vector<Expr *> projections = { col1Expr };

    FieldExpr *gteLeft = new FieldExpr(1, new IntDataType());
    LiteralExpr *gteRight = new LiteralExpr(30, new IntDataType());
    BinaryExpr *gteExpr = new BinaryExpr(omniruntime::expressions::Operator::GTE, gteLeft, gteRight, new BooleanDataType());

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(gteExpr, inputTypes, numCols, projections, projectCount);
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
}

TEST(FilterTest, NotEqualTo)
{
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    double *col1 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new DoubleDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_NotEqualTo");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;
    FieldExpr *col0Expr = new FieldExpr(0, new DoubleDataType());
    std::vector<Expr *> projections = { col0Expr };

    FieldExpr *neqLeft = new FieldExpr(0, new DoubleDataType());
    LiteralExpr *neqRight = new LiteralExpr(0, new DoubleDataType());

    BinaryExpr *neqExpr = new BinaryExpr(omniruntime::expressions::Operator::NEQ, neqLeft, neqRight, new BooleanDataType());

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(neqExpr, inputTypes, numCols, projections, projectCount);
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
}

TEST(FilterTest, AllPass)
{
    const int32_t numCols = 1;
    const int32_t numRows = 20000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = 9348;
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_AllPass");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;

    FieldExpr *col0Expr = new FieldExpr(0, new IntDataType());
    std::vector<Expr *> projections = { col0Expr };

    FieldExpr *eqLeft = new FieldExpr(0, new IntDataType());
    LiteralExpr *eqRight = new LiteralExpr(9348, new IntDataType());
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, new BooleanDataType());
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(eqExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(data1)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_MultipleInputs");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;

    FieldExpr *col0Expr = new FieldExpr(0, new IntDataType());
    std::vector<Expr *> projections = { col0Expr };

    FieldExpr *lteLeft = new FieldExpr(0, new IntDataType());
    LiteralExpr *lteRight = new LiteralExpr(4, new IntDataType());
    BinaryExpr *lteExpr = new BinaryExpr(omniruntime::expressions::Operator::LTE, lteLeft, lteRight, new BooleanDataType());

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(lteExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(in1);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_TRUE(CheckOutput(ret[0], numReturned, Filter1));
    EXPECT_EQ(numReturned, 500);

    allData[0] = reinterpret_cast<int64_t>(data2);
    VectorBatch *in2 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(data1), reinterpret_cast<int64_t>(data2)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new LongDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_NegativeValues");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 2;

    FieldExpr *col0Expr = new FieldExpr(0, new IntDataType());
    FieldExpr *col1Expr = new FieldExpr(1, new LongDataType());
    std::vector<Expr *> projections = { col0Expr, col1Expr };

    // create the filter expression object
    FieldExpr *lte1Left = new FieldExpr(0, new IntDataType());
    LiteralExpr *lte1Right = new LiteralExpr(-1, new IntDataType());
    BinaryExpr *lte1Expr = new BinaryExpr(omniruntime::expressions::Operator::LTE, lte1Left, lte1Right, new BooleanDataType());

    FieldExpr *lte2Left = new FieldExpr(1, new LongDataType());
    LiteralExpr *lte2Right = new LiteralExpr(-1L, new LongDataType());
    BinaryExpr *lte2Expr = new BinaryExpr(omniruntime::expressions::Operator::LTE, lte2Left, lte2Right, new BooleanDataType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, lte1Expr, lte2Expr, new BooleanDataType());

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(data1), reinterpret_cast<int64_t>(data2),
        reinterpret_cast<int64_t>(data3)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new LongDataType(), new DoubleDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_AllTypes");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 3;
    FieldExpr *col0Expr = new FieldExpr(0, new IntDataType());
    FieldExpr *col1Expr = new FieldExpr(1, new LongDataType());
    FieldExpr *col2Expr = new FieldExpr(2, new DoubleDataType());
    std::vector<Expr *> projections = { col0Expr, col1Expr, col2Expr };

    // create the filter expression object
    FieldExpr *eq2Left = new FieldExpr(1, new LongDataType());
    LiteralExpr *eq2Right = new LiteralExpr(3000000000L, new LongDataType());
    BinaryExpr *eq2Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eq2Left, eq2Right, new BooleanDataType());

    FieldExpr *gteLeft = new FieldExpr(2, new DoubleDataType());
    LiteralExpr *gteRight = new LiteralExpr(0.4, new DoubleDataType());
    BinaryExpr *gteExpr = new BinaryExpr(omniruntime::expressions::Operator::GTE, gteLeft, gteRight, new BooleanDataType());

    BinaryExpr *innerAndExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, eq2Expr, gteExpr, new BooleanDataType());

    FieldExpr *eq1Left = new FieldExpr(0, new IntDataType());
    LiteralExpr *eq1Right = new LiteralExpr(0, new IntDataType());
    BinaryExpr *eq1Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eq1Left, eq1Right, new BooleanDataType());

    Expr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, eq1Expr, innerAndExpr, new BooleanDataType());
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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

    int64_t datas[4] = {reinterpret_cast<int64_t>(data1), reinterpret_cast<int64_t>(data2),
        reinterpret_cast<int64_t>(data3), reinterpret_cast<int64_t>(data4)};
    DataTypes inputTypes(
        std::vector<DataTypeRawPtr>({ new DoubleDataType(), new IntDataType(), new DoubleDataType(), new DoubleDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Compile");
    VectorBatch *t = CreateInput(vectorAllocator, dataSize, numCols, inputTypes.GetIds(), datas);
    // TPCH 6
    FieldExpr *col0Expr = new FieldExpr(0, new DoubleDataType());
    std::vector<Expr *> projections = { col0Expr };

    LiteralExpr *gtRight = new LiteralExpr(8766.0, new DoubleDataType());
    BinaryExpr *gtExpr =
        new BinaryExpr(omniruntime::expressions::Operator::GT, new FieldExpr(3, new DoubleDataType()), gtRight, new BooleanDataType());

    LiteralExpr *lt1Right = new LiteralExpr(9131.0, new DoubleDataType());
    BinaryExpr *lt1Expr =
        new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(3, new DoubleDataType()), lt1Right, new BooleanDataType());
    BinaryExpr *and1Expression =
        new BinaryExpr(omniruntime::expressions::Operator::AND, gtExpr, lt1Expr, new BooleanDataType());

    LiteralExpr *lt2Right = new LiteralExpr(24.0, new DoubleDataType());
    BinaryExpr *lt2expr =
        new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, new DoubleDataType()), lt2Right, new BooleanDataType());

    FieldExpr *data = new FieldExpr(2, new DoubleDataType());
    LiteralExpr *lower = new LiteralExpr(0.05, new DoubleDataType());
    LiteralExpr *upper = new LiteralExpr(0.07, new DoubleDataType());
    std::vector<Expr *> args;
    BetweenExpr *betweenExpr = new BetweenExpr(data, lower, upper);
    BinaryExpr *and2Expression =
        new BinaryExpr(omniruntime::expressions::Operator::AND, betweenExpr, lt2expr, new BooleanDataType());

    BinaryExpr *filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::AND, and1Expression, and2Expression, new BooleanDataType());

    OperatorFactory *factory = new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, 1);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3), reinterpret_cast<int64_t>(col4), reinterpret_cast<int64_t>(col5),
        reinterpret_cast<int64_t>(col6)};
    // int int int long double long
    DataTypes inputTypes(std::vector<DataTypeRawPtr>(
        { new IntDataType(), new IntDataType(), new IntDataType(), new LongDataType(), new DoubleDataType(), new LongDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LogicalOperators1");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    const int32_t projectCount = 4;
    // projection objects:
    FieldExpr *col0Expr = new FieldExpr(0, new IntDataType());
    FieldExpr *col2Expr = new FieldExpr(2, new IntDataType());
    FieldExpr *col4Expr = new FieldExpr(4, new DoubleDataType());
    FieldExpr *col5Expr = new FieldExpr(5, new LongDataType());
    std::vector<Expr *> projections = { col0Expr, col2Expr, col4Expr, col5Expr };

    LiteralExpr *eqRight = new LiteralExpr(3000000000L, new LongDataType());
    BinaryExpr *eqExpr =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(3, new LongDataType()), eqRight, new BooleanDataType());
    BinaryExpr *neqExpr = new BinaryExpr(omniruntime::expressions::Operator::NEQ, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(1, new IntDataType()), new BooleanDataType());
    BinaryExpr *andExpr1 = new BinaryExpr(omniruntime::expressions::Operator::AND, neqExpr, eqExpr, new BooleanDataType());

    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, new FieldExpr(2, new IntDataType()),
        new LiteralExpr(4800, new IntDataType()), new BooleanDataType());
    BinaryExpr *lteExpr = new BinaryExpr(omniruntime::expressions::Operator::LTE, new FieldExpr(1, new IntDataType()),
        new LiteralExpr(9990, new IntDataType()), new BooleanDataType());
    BinaryExpr *andExpr2 = new BinaryExpr(omniruntime::expressions::Operator::AND, gtExpr, lteExpr, new BooleanDataType());

    BinaryExpr *andExpr3 = new BinaryExpr(omniruntime::expressions::Operator::AND, andExpr2, andExpr1, new BooleanDataType());

    LiteralExpr *ltRight = new LiteralExpr(50.8, new DoubleDataType());
    BinaryExpr *ltExpr =
        new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(4, new DoubleDataType()), ltRight, new BooleanDataType());
    BinaryExpr *andExpr4 = new BinaryExpr(omniruntime::expressions::Operator::AND, ltExpr, andExpr3, new BooleanDataType());

    LiteralExpr *gteRight = new LiteralExpr(52L, new LongDataType());
    BinaryExpr *gteExpr =
        new BinaryExpr(omniruntime::expressions::Operator::GTE, new FieldExpr(5, new LongDataType()), gteRight, new BooleanDataType());
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::OR, gteExpr, andExpr4, new BooleanDataType());

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3), reinterpret_cast<int64_t>(col4)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType(), new LongDataType(), new LongDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LogicalOperators2");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    const int32_t projectCount = 4;

    // projections
    FieldExpr *col0Expr = new FieldExpr(0, new IntDataType());
    FieldExpr *col1Expr = new FieldExpr(1, new IntDataType());
    FieldExpr *col2Expr = new FieldExpr(2, new LongDataType());
    FieldExpr *col3Expr = new FieldExpr(3, new LongDataType());

    std::vector<Expr *> projections = { col3Expr, col2Expr, col1Expr, col0Expr };

    LiteralExpr *lteRight = new LiteralExpr(-3000000000L, new LongDataType());
    BinaryExpr *lteExpr =
        new BinaryExpr(omniruntime::expressions::Operator::LTE, new FieldExpr(2, new LongDataType()), lteRight, new BooleanDataType());
    LiteralExpr *gteRight = new LiteralExpr(0L, new LongDataType());
    BinaryExpr *gteExpr =
        new BinaryExpr(omniruntime::expressions::Operator::GTE, new FieldExpr(3, new LongDataType()), gteRight, new BooleanDataType());
    BinaryExpr *orExpr1 = new BinaryExpr(omniruntime::expressions::Operator::OR, lteExpr, gteExpr, new BooleanDataType());

    BinaryExpr *ltExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(50, new IntDataType()), new BooleanDataType());
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(1, new IntDataType()),
        new LiteralExpr(-12, new IntDataType()), new BooleanDataType());
    BinaryExpr *orExpr2 = new BinaryExpr(omniruntime::expressions::Operator::OR, ltExpr, eqExpr, new BooleanDataType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, orExpr1, orExpr2, new BooleanDataType());

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new DoubleDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LogicalOperators3");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    const int32_t projectCount = 2;

    // projections
    FieldExpr *col0Expr = new FieldExpr(0, new IntDataType());
    FieldExpr *col1Expr = new FieldExpr(1, new IntDataType());
    std::vector<Expr *> projections = { col1Expr, col0Expr };

    BinaryExpr *eq1Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new LiteralExpr(55, new IntDataType()),
        new FieldExpr(0, new IntDataType()), new BooleanDataType());
    BinaryExpr *eq2Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new LiteralExpr(5, new IntDataType()),
        new FieldExpr(0, new IntDataType()), new BooleanDataType());
    BinaryExpr *or1Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, eq1Expr, eq2Expr, new BooleanDataType());

    BinaryExpr *eq3Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(8, new IntDataType()), new BooleanDataType());
    BinaryExpr *or2Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, or1Expr, eq3Expr, new BooleanDataType());

    BinaryExpr *eq4Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(13, new IntDataType()), new BooleanDataType());
    BinaryExpr *or3Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, or2Expr, eq4Expr, new BooleanDataType());


    BinaryExpr *eq5Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(1, new IntDataType()), new BooleanDataType());
    BinaryExpr *eq6Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(2, new IntDataType()), new BooleanDataType());
    BinaryExpr *or4Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, eq5Expr, eq6Expr, new BooleanDataType());

    BinaryExpr *eq7Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(3, new IntDataType()), new BooleanDataType());
    BinaryExpr *or5Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, or4Expr, eq7Expr, new BooleanDataType());

    BinaryExpr *or6Expr = new BinaryExpr(omniruntime::expressions::Operator::OR, or5Expr, or3Expr, new BooleanDataType());

    LiteralExpr *neqRight = new LiteralExpr(0L, new LongDataType());
    BinaryExpr *neqExpr =
        new BinaryExpr(omniruntime::expressions::Operator::NEQ, new FieldExpr(1, new LongDataType()), neqRight, new BooleanDataType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, neqExpr, or6Expr, new BooleanDataType());

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
}

TEST(FilterTest, ArithmeticAdd)
{
    const int32_t numCols = 1;
    const int32_t numRows = 10000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 5;
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_ArithmeticAdd");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;

    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()) };

    // filter
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(1, new IntDataType()), new IntDataType());
    BinaryExpr *filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::GT, addExpr, new LiteralExpr(4, new IntDataType()), new BooleanDataType());
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new LongDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_ArithmeticSubtract");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 2;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new LongDataType()) };

    BinaryExpr *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(5, new IntDataType()), new IntDataType());
    BinaryExpr *filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::LT, new LiteralExpr(0, new IntDataType()), subExpr, new BooleanDataType());

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new LongDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_ArithmeticMultiply");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 2;

    BinaryExpr *mul1Expr = new BinaryExpr(omniruntime::expressions::Operator::MUL, new FieldExpr(0, new IntDataType()),
        new FieldExpr(0, new IntDataType()), new IntDataType());
    BinaryExpr *eqExpr =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, new LiteralExpr(0, new IntDataType()), mul1Expr, new BooleanDataType());

    LiteralExpr *mulLeft = new LiteralExpr(2L, new LongDataType());
    BinaryExpr *mul2Expr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, new FieldExpr(1, new LongDataType()), new LongDataType());
    LiteralExpr *gtLeft = new LiteralExpr(7L, new LongDataType());
    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft, mul2Expr, new BooleanDataType());
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, eqExpr, gtExpr, new BooleanDataType());

    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new LongDataType()) };

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType(), new IntDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Conditional");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    const int32_t projectCount = 3;

    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new IntDataType()),
        new FieldExpr(2, new IntDataType()) };

    // filters
    BinaryExpr *condition = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(0, new IntDataType()), new BooleanDataType());
    BinaryExpr *texp = new BinaryExpr(omniruntime::expressions::Operator::ADD, new FieldExpr(1, new IntDataType()),
        new LiteralExpr(5, new IntDataType()), new IntDataType());
    FieldExpr *fexp = new FieldExpr(2, new IntDataType());

    IfExpr *eqLeft = new IfExpr(condition, texp, fexp);

    BinaryExpr *filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, new LiteralExpr(55, new IntDataType()), new BooleanDataType());

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
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType(), new IntDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Conditional2");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    // filters
    BinaryExpr *condition = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(0, new IntDataType()), new BooleanDataType());
    BinaryExpr *texp = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(1, new IntDataType()),
        new LiteralExpr(3, new IntDataType()), new BooleanDataType());
    BinaryExpr *fexp = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(1, new IntDataType()),
        new LiteralExpr(4, new IntDataType()), new BooleanDataType());
    IfExpr *ifExpr = new IfExpr(condition, texp, fexp);

    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, new FieldExpr(2, new IntDataType()),
        new LiteralExpr(3, new IntDataType()), new BooleanDataType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, ifExpr, gtExpr, new BooleanDataType());

    // filters
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new IntDataType()),
        new FieldExpr(2, new IntDataType()) };

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType(), new IntDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_In");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    // filter
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, new IntDataType()));
    args.push_back(new LiteralExpr(1, new IntDataType()));
    args.push_back(new LiteralExpr(3, new IntDataType()));
    args.push_back(new LiteralExpr(5, new IntDataType()));

    InExpr *filterExpr = new InExpr(args);

    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new IntDataType()),
        new FieldExpr(2, new IntDataType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType(), new IntDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Between");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    BetweenExpr *filterExpr =
        new BetweenExpr(new FieldExpr(1, new IntDataType()), new FieldExpr(0, new IntDataType()), new FieldExpr(2, new IntDataType()));
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new IntDataType()),
        new FieldExpr(2, new IntDataType()) };

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
}

TEST(FilterTest, NotEqualToAbs)
{
    const int32_t numCols = 1;
    const int32_t numRows = 100000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i - 32435;
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_NotEqualToAbs");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    // filter
    DataTypeRawPtr retType = new IntDataType();
    std::string funcStr = "abs";
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, new IntDataType()));
    auto absExpr = GetFuncExpr(funcStr, args, new IntDataType());

    auto filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::NEQ, absExpr, new LiteralExpr(4, new IntDataType()), new BooleanDataType());
    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()) };

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType(), new IntDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_MathFunctionFilter1");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    // filters
    DataTypeRawPtr retType = new IntDataType();
    std::string funcStr = "abs";
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, new IntDataType()));
    auto abs1Expr = GetFuncExpr(funcStr, args1, new IntDataType());

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(2, new IntDataType()));
    auto abs2Expr = GetFuncExpr(funcStr, args2, new IntDataType());
    auto eq1Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, abs1Expr, abs2Expr, new BooleanDataType());

    std::vector<Expr *> args3;
    args3.push_back(new FieldExpr(0, new IntDataType()));
    auto abs3Expr = GetFuncExpr(funcStr, args3, new IntDataType());

    std::vector<Expr *> args4;
    args4.push_back(new FieldExpr(1, new IntDataType()));
    auto abs4Expr = GetFuncExpr(funcStr, args4, new IntDataType());
    auto eq2Expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, abs3Expr, abs4Expr, new BooleanDataType());

    auto filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, eq1Expr, eq2Expr, new BooleanDataType());

    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new IntDataType()),
        new FieldExpr(2, new IntDataType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new LongDataType(), new IntDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_MathFunctionFilter2");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    // filters
    std::string castStr = "CAST";
    DataTypeRawPtr retType = new DoubleDataType();
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, new IntDataType()));
    auto cast1 = GetFuncExpr(castStr, args1, new DoubleDataType());

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(1, new LongDataType()));
    auto cast2 = GetFuncExpr(castStr, args2, new DoubleDataType());

    auto filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, cast1, cast2, new BooleanDataType());

    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new LongDataType()),
        new FieldExpr(2, new IntDataType()) };

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
}

// String Filter and varcharvec testing
TEST(FilterTest, FilterString1)
{
    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    vector<string *> strings;
    int64_t *col1 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 40 == 0) {
            std::string *s = new std::string("hello");
            col1[i] = reinterpret_cast<int64_t>(s->c_str());
            strings.push_back(s);
        } else {
            std::string *s = new std::string("abcdefghijklmnopqrstuvwxyz");
            col1[i] = reinterpret_cast<int64_t>(s->c_str());
            strings.push_back(s);
        }
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new VarcharDataType(30) }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_FilterString1");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, new VarcharDataType()),
        new LiteralExpr(new std::string("hello"), new VarcharDataType()), new BooleanDataType());
    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, new VarcharDataType()) };

    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    EXPECT_EQ(numReturned, 25);
    for (int32_t i = 0; i < numReturned; i += 1000) {
        VarcharVector *vcVec = ((VarcharVector *)ret[0]->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);

        // Truncate the resulting string
        void *charArr = &actualChar;
        auto charArrCasted = static_cast<char **>(charArr);
        string actualStr(*charArrCasted, 0, len);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    for (auto &s : strings) {
        delete s;
    }
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType(), new IntDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Coalesce1");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 != 0) {
            t->GetVector(1)->SetValueNull(i);
        } else {
            t->GetVector(1)->SetValueNotNull(i);
        }
    }

    CoalesceExpr *coalesceExpr = new CoalesceExpr(new FieldExpr(1, new IntDataType()), new FieldExpr(0, new IntDataType()));
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new LiteralExpr(21, new IntDataType()),
        coalesceExpr, new BooleanDataType());
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new IntDataType()),
        new FieldExpr(2, new IntDataType()) };
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

    DataTypes inputTypes(std::vector<DataType>({ VarcharDataType(30) }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Coalesce2");
    std::vector<Vector *> cols = {CreateVarcharVector(strings, nulls)};
    auto *t = CreateVectorBatch(numRows, cols);

    CoalesceExpr *coalesceExpr =
        new CoalesceExpr(new FieldExpr(0, new VarcharDataType()), new LiteralExpr(new std::string("bye"), new VarcharDataType()));
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, coalesceExpr,
        new LiteralExpr(new std::string("hello"), new VarcharDataType()), new BooleanDataType());
    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, new VarcharDataType()) };

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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_ExternalMathFunc");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    // filter
    std::string funcStr = "Increment";
    DataTypeRawPtr retType = new IntDataType();
    auto col0 = new FieldExpr(0, new IntDataType());
    auto add1Int1Expr = GetFuncExpr(funcStr, vector<Expr *> { col0 }, new IntDataType());
    auto eqLeft = GetFuncExpr(funcStr, vector<Expr *> { add1Int1Expr }, new IntDataType());
    auto eqRight = new FieldExpr(1, new IntDataType());

    auto filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, new BooleanDataType());

    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new IntDataType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projections.size());
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
}


TEST(FilterTest, ExternalStringFunc)
{
    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    vector<string *> strings;
    int64_t *col1 = new int64_t[numRows];

    // column looks like:
    // hello, bye, hello, bye, hello, bye, ...
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            std::string *s = new std::string("hello");
            col1[i] = reinterpret_cast<int64_t>(s->c_str());
            strings.push_back(s);
        } else {
            if (i % 4 == 1) {
                std::string *s = new std::string("bye");
                col1[i] = reinterpret_cast<int64_t>(s->c_str());
                strings.push_back(s);
            } else {
                std::string *s = new std::string("asdf");
                col1[i] = reinterpret_cast<int64_t>(s->c_str());
                strings.push_back(s);
            }
        }
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new VarcharDataType(30) }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_ExternalStringFunc");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    std::string funcStr = "length";
    DataTypeRawPtr retType = new IntDataType();
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, new VarcharDataType()));
    auto eqLeft = GetFuncExpr(funcStr, args, new IntDataType());
    auto filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, new LiteralExpr(5, new IntDataType()), new BooleanDataType());

    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, new VarcharDataType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 500);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(projections);
    for (auto &s : strings) {
        delete s;
    }
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vectorAllocator;
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

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
                                reinterpret_cast<int64_t>(col3)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new LongDataType(), new IntDataType() }));
    DataTypes inputTypes2(std::vector<DataTypeRawPtr>({ new IntDataType(), new LongDataType(), new IntDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Multithreading");
    VectorBatch *t = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    VectorBatch *t2 = CreateInput(vectorAllocator, numRows, numCols, inputTypes2.GetIds(), allData);

    std::vector<VectorBatch *> ret;
    std::vector<VectorBatch *> ret2;
    int32_t *numReturned = new int32_t;
    int32_t *numReturned2 = new int32_t;

    // find wall clock time
    auto start = std::chrono::high_resolution_clock::now();

    // filters
    std::string castStr = "CAST";
    std::string absStr = "abs";
    DataTypeRawPtr retType = new DoubleDataType();
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, new IntDataType()));
    auto cast1Expr = GetFuncExpr(castStr, args1, new DoubleDataType());
    std::vector<Expr *> args2;
    args2.push_back(cast1Expr);
    auto eqLeft = GetFuncExpr(absStr, args2, new DoubleDataType());

    std::vector<Expr *> args3;
    args3.push_back(new FieldExpr(1, new LongDataType()));
    auto cast2Expr = GetFuncExpr(castStr, args3, new DoubleDataType());
    std::vector<Expr *> args4;
    args4.push_back(cast2Expr);
    auto eqRight = GetFuncExpr(absStr, args4, new DoubleDataType());

    auto filterExpr1 = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, new BooleanDataType());

    const int32_t projectCount = 3;
    std::vector<Expr *> projections1 = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new LongDataType()),
        new FieldExpr(2, new IntDataType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr1, inputTypes, numCols, projections1, projectCount);
    omniruntime::op::Operator *op = factory->CreateOperator();
    std::thread thread1(process, op, t, ret, numReturned);

    // filter2
    auto eqRight2 = new LiteralExpr(4L, new LongDataType());
    auto filterExpr2 =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(1, new LongDataType()), eqRight2, new BooleanDataType());
    std::vector<Expr *> projections2 = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new LongDataType()),
        new FieldExpr(2, new IntDataType()) };

    OperatorFactory *factory2 = new FilterAndProjectOperatorFactory(filterExpr2, inputTypes2, numCols, projections2, 3);
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

    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType(), new IntDataType() }));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryVector);

    BetweenExpr *filterExpr =
        new BetweenExpr(new FieldExpr(1, new IntDataType()), new FieldExpr(0, new IntDataType()), new FieldExpr(2, new IntDataType()));
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new LongDataType()),
        new FieldExpr(2, new IntDataType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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

    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new VarcharDataType(50) }));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->SetVector(0, col1);
    batch->SetVector(1, dictionaryVector);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(6, new IntDataType()), new BooleanDataType());
    const int32_t projectCount = 2;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new VarcharDataType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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

    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType(), new IntDataType() }));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryNested);

    BetweenExpr *filterExpr =
        new BetweenExpr(new FieldExpr(1, new IntDataType()), new FieldExpr(0, new IntDataType()), new FieldExpr(2, new IntDataType()));
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new IntDataType()),
        new FieldExpr(2, new IntDataType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(data1)};
    std::vector<DataTypeRawPtr> vecOfTypes = { new Decimal128DataType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_DecimalFilterBinaryTest");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, new Decimal128DataType(38, 0)) };
    LiteralExpr *lteRight = new LiteralExpr(new std::string("500000"), new Decimal128DataType(38, 0));
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LTE,
        new FieldExpr(0, new Decimal128DataType(38, 0)), lteRight, new BooleanDataType());
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
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(data1), reinterpret_cast<int64_t>(data2),
        reinterpret_cast<int64_t>(data3)};
    std::vector<DataTypeRawPtr> vecOfTypes = { new Decimal128DataType(), new Decimal128DataType(), new Decimal128DataType()};
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_DecimalFilterAbsTest");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, new Decimal128DataType(38, 0)),
        new FieldExpr(1, new Decimal128DataType(38, 0)), new FieldExpr(2, new Decimal128DataType(38, 0)) };

    // filters
    std::string absStr = "abs";
    DataTypeRawPtr retType = new Decimal128DataType(38, 0);
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, new Decimal128DataType(38, 0)));
    auto absExpr1 = GetFuncExpr(absStr, args1, new Decimal128DataType(38, 0));

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(2, new Decimal128DataType(38, 0)));
    auto absExpr2 = GetFuncExpr(absStr, args2, new Decimal128DataType(38, 0));

    BinaryExpr *eqExpr1 = new BinaryExpr(omniruntime::expressions::Operator::EQ, absExpr1, absExpr2, new BooleanDataType());

    std::vector<Expr *> args3;
    args3.push_back(new FieldExpr(1, new Decimal128DataType(38, 0)));
    auto absExpr3 = GetFuncExpr(absStr, args3, new Decimal128DataType(38, 0));

    std::vector<Expr *> args4;
    args4.push_back(new FieldExpr(2, new Decimal128DataType(38, 0)));
    auto absExpr4 = GetFuncExpr(absStr, args4, new Decimal128DataType(38, 0));

    BinaryExpr *eqExpr2 = new BinaryExpr(omniruntime::expressions::Operator::EQ, absExpr3, absExpr4, new BooleanDataType());
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, eqExpr1, eqExpr2, new BooleanDataType());
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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

    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new VarcharDataType(1000) }));
    VectorBatch *batch = new VectorBatch(numCols, numRows);
    batch->SetVector(0, col0);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, new VarcharDataType()),
        new LiteralExpr(new std::string("hello"), new VarcharDataType()), new BooleanDataType());
    const int32_t projectCount = 1;
    std::vector<Expr *> projections = { new FieldExpr(0, new VarcharDataType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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

    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType(), new IntDataType() }));
    VectorBatch *intput = new VectorBatch(numCols, slicedCol1->GetSize());
    intput->SetVector(0, slicedCol1);
    intput->SetVector(1, slicedCol2);
    intput->SetVector(2, slicedCol3);

    BetweenExpr *filterExpr =
        new BetweenExpr(new FieldExpr(1, new IntDataType()), new FieldExpr(0, new IntDataType()), new FieldExpr(2, new IntDataType()));
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new IntDataType()),
        new FieldExpr(2, new IntDataType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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

    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType(), new IntDataType() }));
    VectorBatch *intput = new VectorBatch(numCols, slicedCol1->GetSize());
    intput->SetVector(0, slicedCol1);
    intput->SetVector(1, slicedCol2);
    intput->SetVector(2, slicedCol3);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(2, new IntDataType()),
        new LiteralExpr(6, new IntDataType()), new BooleanDataType());
    const int32_t projectCount = 3;
    std::vector<Expr *> projections = { new FieldExpr(0, new IntDataType()), new FieldExpr(1, new IntDataType()),
        new FieldExpr(2, new IntDataType()) };
    OperatorFactory *factory =
        new FilterAndProjectOperatorFactory(filterExpr, inputTypes, numCols, projections, projectCount);
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
}

TEST(FilterTest, SimpleFilter)
{
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    auto col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType() }));
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_SimpleFilter");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(2000, new IntDataType()), new BooleanDataType());
    auto filter = new SimpleFilter(*filterExpr);
    bool initialized = filter->Initialize();
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
}

TEST(FilterTest, SimpleFilterWithNulls)
{
    const int32_t numCols = 1;
    const int32_t numRows = 5000;
    auto col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType() }));
    VectorAllocator *vectorAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_SimpleFilterWithNulls");
    VectorBatch *in1 = CreateInput(vectorAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, new IntDataType()),
        new LiteralExpr(2000, new IntDataType()), new BooleanDataType());

    auto filter = new SimpleFilter(*filterExpr);
    bool initialized = filter->Initialize();
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
}

TEST(FilterTest, SimpleFilterIntWithNulls)
{
    const int32_t numRows = 10;
    int32_t data0[numRows] = {19, 14, 7, 19, 1, 20, 10, 13, 20, 16};
    int32_t data1[numRows] = {20, 16, 13, 4, 20, 4, 22, 19, 8, 7};

    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new IntDataType(), new IntDataType() }));
    auto vecBatch = CreateVectorBatch(inputTypes, numRows, data0, data1);

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, new IntDataType()),
        new FieldExpr(1, new IntDataType()), new BooleanDataType());
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
    auto vec1 = CreateVarcharVector(new VarcharDataType(5), data1, numRows);
    auto vecBatch = new VectorBatch(2, numRows);
    vecBatch->SetVector(0, vec0);
    vecBatch->SetVector(1, vec1);

    DataTypes inputTypes(std::vector<DataTypeRawPtr>({ new VarcharDataType(5), new VarcharDataType(5) }));
    // filter expression object
    std::string funcStr = "substr";
    DataTypeRawPtr retType = new VarcharDataType();
    std::vector<Expr *> args1;
    args1.push_back(new FieldExpr(0, new VarcharDataType()));
    args1.push_back(new LiteralExpr(1, new IntDataType()));
    args1.push_back(new LiteralExpr(5, new IntDataType()));
    auto substrExpr1 = GetFuncExpr(funcStr, args1, new VarcharDataType());

    std::vector<Expr *> args2;
    args2.push_back(new FieldExpr(1, new VarcharDataType()));
    args2.push_back(new LiteralExpr(1, new IntDataType()));
    args2.push_back(new LiteralExpr(5, new IntDataType()));
    auto substrExpr2 = GetFuncExpr(funcStr, args2, new VarcharDataType());
    auto filterExpr = new BinaryExpr(omniruntime::expressions::Operator::NEQ, substrExpr1, substrExpr2, new BooleanDataType());

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
}
}
}