/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */

#include <string>
#include <vector>
#include <chrono>
#include "gtest/gtest.h"
#include "operator/projection/projection.h"
#include "../util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace std;
using namespace TestUtil;

namespace ProjectionTest {
const int32_t INDEX_FACTOR = 2;
VectorBatch *CreateInput(VectorAllocator *vectorAllocator, const int32_t numRows, const int32_t numCols,
    const int32_t *inputTypeIds, int64_t *allData)
{
    auto *vecBatch = new VectorBatch(numCols, numRows);
    vector<DataType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    vecBatch->NewVectors(vectorAllocator, inputTypes);
    for (int i = 0; i < numCols; ++i) {
        switch (inputTypeIds[i]) {
            case OMNI_BOOLEAN:
                ((BooleanVector *)vecBatch->GetVector(i))->SetValues(0, (bool *)allData[i], numRows);
                break;
            case OMNI_INT:
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], numRows);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                ((LongVector *)vecBatch->GetVector(i))->SetValues(0, (int64_t *)allData[i], numRows);
                break;
            case OMNI_DOUBLE:
                ((DoubleVector *)vecBatch->GetVector(i))->SetValues(0, (double *)allData[i], numRows);
                break;
            case OMNI_SHORT:
                ((IntVector *)vecBatch->GetVector(i))->SetValues(0, (int32_t *)allData[i], numRows);
                break;
            case OMNI_CHAR:
            case OMNI_VARCHAR: {
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

int32_t *MakeInts(const int32_t size, const int32_t start = 0)
{
    auto *arr = new int32_t[size];
    int32_t idx = 0;
    for (int32_t i = start; i < start + size; i++) {
        arr[idx++] = i;
    }
    return arr;
}

int64_t *MakeDecimals(const int32_t size, const int32_t start = 0)
{
    auto *arr = new int64_t[size * 2];
    int32_t idx = 0;
    for (int64_t i = start; i < start + size; i++) {
        if (i >= 0) {
            arr[INDEX_FACTOR * idx] = i;
            arr[INDEX_FACTOR * idx + 1] = 0;
        } else {
            arr[INDEX_FACTOR * idx] = i * -1;
            arr[INDEX_FACTOR * idx + 1] = -1;
        }
        idx++;
    }
    return arr;
}

int64_t *MakeLongs(const int32_t size, const int64_t start = 0)
{
    auto *arr = new int64_t[size];
    int32_t idx = 0;
    for (int64_t i = start; i < start + size; i++) {
        arr[idx++] = i;
    }
    return arr;
}

double *MakeDoubles(const int32_t size, const double start = 0)
{
    auto *arr = new double[size];
    int32_t idx = 0;
    for (double i = start; i < start + size; i++) {
        arr[idx++] = i;
    }
    return arr;
}

TEST(ProjectionTest, Cast)
{
    const int32_t numRows = 1000;
    int64_t *col1 = MakeLongs(numRows);
    int32_t *col2 = MakeInts(numRows);
    const int32_t numCols = 2;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG), DataType(OMNI_INT) };
    auto data1 = new FieldExpr(0, LongType());
    std::string castStr = "CAST";
    std::vector<Expr *> args1;
    args1.push_back(data1);
    auto castExpr1 = GetFuncExpr(castStr, args1, IntType());

    auto data2 = new FieldExpr(1, IntType());
    std::vector<Expr *> args2;
    args2.push_back(data2);
    auto castExpr2 = GetFuncExpr(castStr, args2, LongType());

    std::vector<Expr *> exprs = { castExpr1, castExpr2 };

    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2)};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Cast");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
        t->GetVector(1)->SetValueNotNull(i);
    }

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, i);
        EXPECT_EQ(val1, i);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, CastDouble)
{
    const int32_t numRows = 1000;
    double *col1 = MakeDoubles(numRows);
    double *col2 = MakeDoubles(numRows);
    const int32_t numCols = 2;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_DOUBLE), DataType(OMNI_DOUBLE) };

    auto data1 = new FieldExpr(0, DoubleType());
    std::string castStr = "CAST";
    std::vector<Expr *> args1;
    args1.push_back(data1);
    auto castExpr1 = GetFuncExpr(castStr, args1, IntType());

    auto data2 = new FieldExpr(1, DoubleType());
    std::vector<Expr *> args2;
    args2.push_back(data2);
    auto castExpr2 = GetFuncExpr(castStr, args2, LongType());

    std::vector<Expr *> exprs = { castExpr1, castExpr2 };

    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2)};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastDouble");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
        t->GetVector(1)->SetValueNotNull(i);
    }

    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, i);
        EXPECT_EQ(val1, i);
    }

    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, CastInt64ToDecimal128)
{
    const int32_t numRows = 1000;
    int64_t *col1 = MakeLongs(numRows);
    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG) };
    auto data1 = new FieldExpr(0, LongType());
    std::string castStr = "CAST";
    std::vector<Expr *> args1;
    args1.push_back(data1);
    auto castExpr = GetFuncExpr(castStr, args1, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { castExpr };

    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastInt64ToDecimal128");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
    }

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, MakeDecimal64ToDiffScale)
{
    const int32_t numRows = 1000;
    int64_t *col1 = MakeLongs(numRows);
    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(Decimal64DataType(7, 2)) };
    auto data1 = new FieldExpr(0, Decimal64Type(7, 2));
    LiteralExpr *precision1 = new LiteralExpr(7, IntType());
    LiteralExpr *scale1 = new LiteralExpr(2, IntType());
    LiteralExpr *new_precision1 = new LiteralExpr(7, IntType());
    LiteralExpr *new_scale1 = new LiteralExpr(4, IntType());
    auto data2 = new FieldExpr(0, Decimal64Type(7, 2));
    LiteralExpr *precision2 = new LiteralExpr(7, IntType());
    LiteralExpr *scale2 = new LiteralExpr(2, IntType());
    LiteralExpr *new_precision2 = new LiteralExpr(7, IntType());
    LiteralExpr *new_scale2 = new LiteralExpr(0, IntType());

    std::string MakeStr = "MakeDecimal";
    std::vector<Expr *> args1 { data1, precision1, scale1, new_precision1, new_scale1 };
    std::vector<Expr *> args2 { data2, precision2, scale2, new_precision2, new_scale2 };
    auto makeExpr1 = GetFuncExpr(MakeStr, args1, Decimal64Type(7, 4));
    auto makeExpr2 = GetFuncExpr(MakeStr, args2, Decimal64Type(7, 0));
    std::vector<Expr *> exprs = { makeExpr1, makeExpr2 };

    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, exprs.size(), inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("MakeDecimal64ToDiffScale");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
    }

    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        long val0 = ((LongVector *)ret[0]->GetVector(0))->GetValue(i);
        long val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, i * 100);
        EXPECT_EQ(val1, i / 100);
    }

    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, MakeDecimal128ToDiffScale)
{
    const int32_t numRows = 1000;
    int64_t *col1 = MakeDecimals(numRows);
    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(Decimal128DataType(38, 2)) };
    auto data1 = new FieldExpr(0, Decimal128Type(38, 2));
    LiteralExpr *precision1 = new LiteralExpr(38, IntType());
    LiteralExpr *scale1 = new LiteralExpr(2, IntType());
    LiteralExpr *new_precision1 = new LiteralExpr(38, IntType());
    LiteralExpr *new_scale1 = new LiteralExpr(4, IntType());
    auto data2 = new FieldExpr(0, Decimal128Type(7, 2));
    LiteralExpr *precision2 = new LiteralExpr(38, IntType());
    LiteralExpr *scale2 = new LiteralExpr(2, IntType());
    LiteralExpr *new_precision2 = new LiteralExpr(38, IntType());
    LiteralExpr *new_scale2 = new LiteralExpr(0, IntType());

    std::string MakeStr = "MakeDecimal";
    std::vector<Expr *> args1 { data1, precision1, scale1, new_precision1, new_scale1 };
    std::vector<Expr *> args2 { data2, precision2, scale2, new_precision2, new_scale2 };
    auto makeExpr1 = GetFuncExpr(MakeStr, args1, Decimal128Type(38, 4));
    auto makeExpr2 = GetFuncExpr(MakeStr, args2, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { makeExpr1, makeExpr2 };

    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, exprs.size(), inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("MakeDecimal128ToDiffScale");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
    }

    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        Decimal128 val1 = ((Decimal128Vector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val1.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i * 100);
        EXPECT_EQ(val1.LowBits(), round((double)i / 100));
    }

    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, MakeDecimal64To128WithDiffScale)
{
    const int32_t numRows = 1000;
    int64_t *col1 = MakeLongs(numRows);
    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(Decimal64DataType(7, 2)) };
    auto data1 = new FieldExpr(0, Decimal64Type(7, 2));
    LiteralExpr *precision1 = new LiteralExpr(7, IntType());
    LiteralExpr *scale1 = new LiteralExpr(2, IntType());
    LiteralExpr *new_precision1 = new LiteralExpr(38, IntType());
    LiteralExpr *new_scale1 = new LiteralExpr(2, IntType());


    auto data2 = new FieldExpr(0, Decimal64Type(7, 2));
    LiteralExpr *precision2 = new LiteralExpr(7, IntType());
    LiteralExpr *scale2 = new LiteralExpr(2, IntType());
    LiteralExpr *new_precision2 = new LiteralExpr(38, IntType());
    LiteralExpr *new_scale2 = new LiteralExpr(4, IntType());

    auto data3 = new FieldExpr(0, Decimal64Type(7, 2));
    LiteralExpr *precision3 = new LiteralExpr(7, IntType());
    LiteralExpr *scale3 = new LiteralExpr(2, IntType());
    LiteralExpr *new_precision3 = new LiteralExpr(38, IntType());
    LiteralExpr *new_scale3 = new LiteralExpr(0, IntType());

    std::string MakeStr = "MakeDecimal";
    std::vector<Expr *> args1 { data1, precision1, scale1, new_precision1, new_scale1 };
    std::vector<Expr *> args2 { data2, precision2, scale2, new_precision2, new_scale2 };
    std::vector<Expr *> args3 { data3, precision3, scale3, new_precision3, new_scale3 };
    auto makeExpr1 = GetFuncExpr(MakeStr, args1, Decimal128Type(38, 2));
    auto makeExpr2 = GetFuncExpr(MakeStr, args2, Decimal128Type(38, 4));
    auto makeExpr3 = GetFuncExpr(MakeStr, args3, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { makeExpr1, makeExpr2, makeExpr3 };

    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, exprs.size(), inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("MakeDecimal64To128WithDiffScale");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
    }

    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        Decimal128 val1 = ((Decimal128Vector *)ret[0]->GetVector(1))->GetValue(i);
        Decimal128 val2 = ((Decimal128Vector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val1.HighBits(), 0);
        EXPECT_EQ(val2.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i);
        EXPECT_EQ(val1.LowBits(), i * 100);
        EXPECT_EQ(val2.LowBits(), round((double)i / 100));
    }

    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, Simple)
{
    const int32_t numRows = 1000;
    int32_t *col = MakeInts(numRows);
    const int32_t numCols = 1;
    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(5, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());
    std::vector<Expr *> exprs = { addExpr };
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT) };
    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col)};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Simple");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            t->GetVector(0)->SetValueNotNull(i);
        } else {
            t->GetVector(0)->SetValueNull(i);
        }
    }

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        if (i % 2 == 0) {
            EXPECT_EQ(val0, i + 5);
            EXPECT_FALSE(t->GetVector(0)->IsValueNull(i));
        } else {
            EXPECT_EQ(val0, 0);
            EXPECT_TRUE(t->GetVector(0)->IsValueNull(i));
        }
    }
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, AbsWithNullValues)
{
    const int32_t numRows = 1000;
    int32_t *col1 = MakeInts(numRows, -5);
    int64_t *col2 = MakeLongs(numRows, -5);
    int64_t *col3 = MakeLongs(numRows, -5);
    const int32_t numCols = 3;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_LONG), DataType(OMNI_DECIMAL64) };
    auto data1 = new FieldExpr(0, IntType());
    std::string funcStr = "abs";
    std::vector<Expr *> args1;
    args1.push_back(data1);
    auto absExpr1 = GetFuncExpr(funcStr, args1, IntType());

    auto data2 = new FieldExpr(1, LongType());
    std::vector<Expr *> args2;
    args2.push_back(data2);
    auto absExpr2 = GetFuncExpr(funcStr, args2, LongType());

    auto data3 = new FieldExpr(2, Decimal64Type(7, 2));
    std::vector<Expr *> args3;
    args3.push_back(data3);
    auto absExpr3 = GetFuncExpr(funcStr, args3, Decimal64Type(7, 2));

    std::vector<Expr *> exprs = { absExpr1, absExpr2, absExpr3 };


    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, exprs.size(), inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
                                reinterpret_cast<int64_t>(col3)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_AbsWithNullValues");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    for (int i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            t->GetVector(0)->SetValueNull(i);
            t->GetVector(1)->SetValueNull(i);
            t->GetVector(2)->SetValueNull(i);
        }
    }

    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        if (i % 2 == 0) {
            EXPECT_TRUE(ret[0]->GetVector(0)->IsValueNull(i));
            EXPECT_TRUE(ret[0]->GetVector(1)->IsValueNull(i));
            EXPECT_TRUE(ret[0]->GetVector(2)->IsValueNull(i));
        } else {
            EXPECT_FALSE(ret[0]->GetVector(0)->IsValueNull(i));
            EXPECT_FALSE(ret[0]->GetVector(1)->IsValueNull(i));
            EXPECT_FALSE(ret[0]->GetVector(2)->IsValueNull(i));
            int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
            int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
            int64_t val2 = ((LongVector *)ret[0]->GetVector(2))->GetValue(i);
            EXPECT_EQ(val0, abs(i - 5));
            EXPECT_EQ(val1, abs(i - 5));
            EXPECT_EQ(val2, abs(i - 5));
        }
    }

    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, Negatives)
{
    const int32_t numRows = 1000;
    int32_t *col = MakeInts(numRows, -5);
    const int32_t numCols = 1;
    FieldExpr *subLeft = new FieldExpr(0, IntType());
    LiteralExpr *subRight = new LiteralExpr(500, IntType());
    BinaryExpr *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, IntType());
    std::vector<Expr *> exprs = { subExpr };
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT) };
    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col)};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Negatives");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i - 505);
    }
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, Longs)
{
    const int32_t numRows = 10000;
    int64_t *col = MakeLongs(numRows, -5000);
    const int32_t numCols = 1;
    FieldExpr *mulLeft = new FieldExpr(0, LongType());
    LiteralExpr *mulRight = new LiteralExpr(5000000L, LongType());
    BinaryExpr *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, LongType());
    std::vector<Expr *> exprs = { mulExpr };
    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG) };
    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col)};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Longs");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, numRows);
    for (int32_t i = 0; i < 1; i++) {
        int64_t val0 = ((LongVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, static_cast<int64_t>(i - 5000) * 5000000);
    }
    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, Doubles)
{
    const int32_t numRows = 10000;
    double *col = MakeDoubles(numRows, -5000.5);
    const int32_t numCols = 1;

    FieldExpr *divLeft = new FieldExpr(0, DoubleType());
    LiteralExpr *divRight = new LiteralExpr(2.0, DoubleType());
    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, divLeft, divRight, DoubleType());
    std::vector<Expr *> exprs = { divExpr };
    std::vector<DataType> vecOfTypes = { DataType(OMNI_DOUBLE) };
    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col)};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Doubles");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, numRows);
    for (int32_t i = 0; i < 1; i++) {
        double val0 = ((DoubleVector *)ret[0]->GetVector(0))->GetValue(i);
        double expected = (i - 5000.5) / 2;
        EXPECT_TRUE(val0 > expected - 0.1 && val0 < expected + 0.1);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, MultipleColumns)
{
    const int32_t numRows = 1000;
    int32_t *col1 = MakeInts(numRows);
    int32_t *col2 = MakeInts(numRows, -100);
    int64_t *col3 = MakeLongs(numRows, -10);
    const int32_t numProject = 2;
    FieldExpr *subLeft = new FieldExpr(0, IntType());
    LiteralExpr *subRight = new LiteralExpr(10, IntType());
    BinaryExpr *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, IntType());

    FieldExpr *addLeft = new FieldExpr(2, LongType());
    LiteralExpr *addRight = new LiteralExpr(1L, LongType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, LongType());

    std::vector<Expr *> exprs = { subExpr, addExpr };

    const int32_t numCols = 3;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_INT), DataType(OMNI_LONG) };
    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3)};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_MultipleColumns");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i - 10);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, i - 9);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, BenchmarkMultipleColumns)
{
    const int32_t numRows = 1000;
    int32_t *col1 = MakeInts(numRows);
    int32_t *col2 = MakeInts(numRows, -100);
    int64_t *col3 = MakeLongs(numRows, -10);
    int64_t *col4 = new int64_t[numRows];

    vector<string *> strings;
    for (int32_t i = 0; i < numRows; i++) {
        std::string *s;
        if (i % 2 == 0) {
            s = new std::string("hello");
        } else if (i % 3 == 0) {
            s = new std::string("world");
        } else {
            s = new std::string("!!!!!");
        }
        col4[i] = reinterpret_cast<int64_t>(s->c_str());
        strings.push_back(s);
    }

    const int32_t numProject = 2;
    FieldExpr *subLeft = new FieldExpr(0, IntType());
    LiteralExpr *subRight = new LiteralExpr(10, IntType());
    BinaryExpr *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, IntType());

    FieldExpr *addLeft = new FieldExpr(2, LongType());
    LiteralExpr *addRight = new LiteralExpr(1L, LongType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, LongType());

    std::vector<Expr *> exprs = { subExpr, addExpr };
    const int32_t numCols = 4;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_INT), DataType(OMNI_LONG),
        VarcharDataType(1000) };
    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3), reinterpret_cast<int64_t>(col4)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_BenchmarkMultipleColumns");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    std::cout << "[BenchmarkMultipleColumns Project without varchar]\n\n";
    for (int i = 0; i < 10; i++) {
        auto start = std::chrono::system_clock::now();
        auto copy = DuplicateVectorBatch(t);
        op->AddInput(copy);
        vector<VectorBatch *> ret;
        int32_t numReturned = op->GetOutput(ret);
        EXPECT_EQ(numReturned, numRows);
        VectorHelper::FreeVecBatches(ret);

        auto end = std::chrono::system_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cout << "BenchmarkMultipleColumns round " << i << " elapsed " << elapsed.count() << " ms\n";
    }

    delete op;
    delete factory;

    std::cout << "\n\n\n[BenchmarkMultipleColumns Project with varchar]\n\n";
    FieldExpr *subLeft2 = new FieldExpr(0, IntType());
    LiteralExpr *subRight2 = new LiteralExpr(10, IntType());
    BinaryExpr *subExpr2 = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft2, subRight2, IntType());

    FieldExpr *substData = new FieldExpr(3, VarcharType());
    LiteralExpr *substrIndex = new LiteralExpr(1, IntType());
    LiteralExpr *substrLen = new LiteralExpr(3, IntType());
    std::string funcStr = "substr";
    DataTypePtr retType = VarcharType();
    std::vector<Expr *> args;
    args.push_back(substData);
    args.push_back(substrIndex);
    args.push_back(substrLen);
    auto substrExpr = GetFuncExpr(funcStr, args, VarcharType());

    std::vector<Expr *> exprs2 = { subExpr2, substrExpr };

    factory = new ProjectionOperatorFactory(exprs2, numProject, inputTypes, numCols);
    op = factory->CreateOperator();

    for (int i = 0; i < 10; i++) {
        auto start = std::chrono::system_clock::now();

        auto copy = DuplicateVectorBatch(t);
        op->AddInput(copy);
        vector<VectorBatch *> ret;
        int32_t numReturned = op->GetOutput(ret);
        EXPECT_EQ(numReturned, numRows);
        VectorHelper::FreeVecBatches(ret);

        auto end = std::chrono::system_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cout << "BenchmarkMultipleColumns round " << i << " elapsed " << elapsed.count() << " ms\n";
    }

    VectorHelper::FreeVecBatch(t);

    delete[] col1;
    delete[] col2;
    delete[] col3;
    for (int32_t i = 0; i < numRows; i++) {
        delete strings[i];
    }
    delete[] col4;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, DependOtherColumn)
{
    const int32_t numRows = 1000;
    int32_t *col1 = MakeInts(numRows);
    int32_t *col2 = MakeInts(numRows, -100);
    int64_t *col3 = MakeLongs(numRows);
    const int32_t numProject = 2;
    FieldExpr *mulLeft = new FieldExpr(0, IntType());
    FieldExpr *mulRight = new FieldExpr(1, IntType());
    BinaryExpr *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, IntType());

    FieldExpr *conditionLeft = new FieldExpr(0, IntType());
    LiteralExpr *conditionRight = new LiteralExpr(500, IntType());
    BinaryExpr *condition =
        new BinaryExpr(omniruntime::expressions::Operator::LT, conditionLeft, conditionRight, BooleanType());

    LiteralExpr *texp = new LiteralExpr(4000000000L, LongType());

    FieldExpr *fexp = new FieldExpr(2, LongType());

    IfExpr *ifExpr = new IfExpr(condition, texp, fexp);

    std::vector<Expr *> exprs = { mulExpr, ifExpr };
    const int32_t numCols = 3;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_INT), DataType(OMNI_LONG) };
    DataTypes inputTypes(vecOfTypes);
    auto factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2),
        reinterpret_cast<int64_t>(col3)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DependOtherColumn");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i * (i - 100));
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, (int64_t)(i < 500 ? 4000000000 : i));
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;
    delete[] col3;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, ProjectString1)
{
    vector<string *> strings;

    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_VARCHAR) };
    DataTypes inputTypes(vecOfTypes);

    const int32_t numRows = 100;
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
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ProjectString1");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);


    const int32_t numProject = 2;

    FieldExpr *substrData = new FieldExpr(0, VarcharType());
    LiteralExpr *substrIndex = new LiteralExpr(1, IntType());
    LiteralExpr *substrLen = new LiteralExpr(3, IntType());
    std::string funcStr = "substr";
    DataTypePtr retType = VarcharType();
    std::vector<Expr *> args;
    args.push_back(substrData);
    args.push_back(substrIndex);
    args.push_back(substrLen);
    auto substrExpr = GetFuncExpr(funcStr, args, VarcharType());

    FieldExpr *col0 = new FieldExpr(0, VarcharType());
    std::vector<Expr *> exprs = { substrExpr, col0 };

    auto *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    for (int32_t i = 0; i < numReturned; i += 20) {
        VarcharVector *vcVec = ((VarcharVector *)ret[0]->GetVector(0));

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

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, DictionaryVecTest)
{
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DictionaryVecTest");
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

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {1, 1, 1};
    vector<DataType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes dataTypes(inputTypes);
    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryVector);

    const int32_t numProject = 3;
    FieldExpr *addLeft1 = new FieldExpr(0, IntType());
    LiteralExpr *addRight1 = new LiteralExpr(1, IntType());
    BinaryExpr *addExpr1 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft1, addRight1, IntType());

    FieldExpr *addLeft2 = new FieldExpr(1, IntType());
    LiteralExpr *addRight2 = new LiteralExpr(2, IntType());
    BinaryExpr *addExpr2 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft2, addRight2, IntType());

    FieldExpr *addLeft3 = new FieldExpr(2, IntType());
    LiteralExpr *addRight3 = new LiteralExpr(10, IntType());
    BinaryExpr *addExpr3 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft3, addRight3, IntType());

    std::vector<Expr *> exprs = { addExpr1, addExpr2, addExpr3 };

    auto *factory = new ProjectionOperatorFactory(exprs, numProject, dataTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto copy = DuplicateVectorBatch(batch);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i) + 1);
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, col2->GetValue(i) + 2);
        int32_t val2 = ((IntVector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, dictionaryVector->GetInt(i) + 10);
    }
    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col3;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, DictionaryVecDoubleTest)
{
    const int32_t numCols = 1;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DictionaryVecDoubleTest");
    DoubleVector *col1 = new DoubleVector(vecAllocator, numRows);

    int32_t ids1[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *doubleDicVector = new DictionaryVector(col1, ids1, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, (i % 21) - 3);
    }

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {3};
    vector<DataType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes dataTypes(inputTypes);

    batch->SetVector(0, doubleDicVector);

    const int32_t numProject = 1;
    LiteralExpr *addRight = new LiteralExpr(10.0, DoubleType());
    std::vector<Expr *> exprs = { new BinaryExpr(omniruntime::expressions::Operator::ADD,
        new FieldExpr(0, DoubleType()), addRight, DoubleType()) };
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, dataTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto copy = DuplicateVectorBatch(batch);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        double val0 = ((DoubleVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, doubleDicVector->GetDouble(i) + 10);
    }
    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col1;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, DictionaryVecVarcharTest)
{
    const int32_t numCols = 1;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DictionaryVecVarcharTest");
    VarcharVector *col1 = new VarcharVector(vecAllocator, 5 * numRows, numRows);

    int32_t ids1[] = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
    DictionaryVector *varCharDicVector = new DictionaryVector(col1, ids1, numRows);
    string str1 = "hello";
    string str2 = "world";
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            col1->SetValue(i, reinterpret_cast<const uint8_t *>(str1.c_str()), str1.length());
        } else {
            col1->SetValue(i, reinterpret_cast<const uint8_t *>(str2.c_str()), str2.length());
        }
    }

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {15};
    vector<DataType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes dataTypes(inputTypes);

    batch->SetVector(0, varCharDicVector);

    const int32_t numProject = 1;
    std::string funcStr = "substr";
    DataTypePtr retType = VarcharType();

    vector<Expr *> args;
    args.push_back(new FieldExpr(0, VarcharType()));
    args.push_back(new LiteralExpr(1, IntType()));
    args.push_back(new LiteralExpr(3, IntType()));
    auto substrExpr = GetFuncExpr(funcStr, args, VarcharType());
    std::vector<Expr *> exprs = { substrExpr };
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, dataTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto copy = DuplicateVectorBatch(batch);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        VarcharVector *vcVec = ((VarcharVector *)ret[0]->GetVector(0));
        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);
        std::string actualStr(actualChar, actualChar + len);

        if (i % 2 == 0) {
            EXPECT_EQ(actualStr, "hel");
        } else {
            EXPECT_EQ(actualStr, "wor");
        }
    }
    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col1;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, DictionaryVecDecimal128Test)
{
    const int32_t numCols = 1;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DictionaryVecDecimal128Test");
    Decimal128Vector *col1 = new Decimal128Vector(vecAllocator, numRows);

    int32_t ids1[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *decimal128DicVector = new DictionaryVector(col1, ids1, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 decimal128(0, i);
        col1->SetValue(i, decimal128);
    }

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {7};
    vector<DataType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes dataTypes(inputTypes);

    batch->SetVector(0, decimal128DicVector);

    const int32_t numProject = 1;
    LiteralExpr *addRight = new LiteralExpr(new std::string("20"), Decimal128Type(38, 0));
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD,
        new FieldExpr(0, Decimal128Type(38, 0)), addRight, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { addExpr };
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, dataTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto copy = DuplicateVectorBatch(batch);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), decimal128DicVector->GetDecimal128(i).HighBits());
        EXPECT_EQ(val0.LowBits(), decimal128DicVector->GetDecimal128(i).LowBits() + 20);
    }

    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col1;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, DictionaryVecNestedTest)
{
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DictionaryVecNestedTest");
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    IntVector *col2 = new IntVector(vecAllocator, numRows);
    IntVector *col3 = new IntVector(vecAllocator, 3);
    int32_t data[] = {4, 5, 6};
    col3->SetValues(0, data, 3);
    int32_t ids[] = {1, 2};
    auto *dictionaryVector = new DictionaryVector(col3, ids, 2);
    int32_t nestedIds[] = {0, 1, 0, 1, 0, 1, 0, 1, 0, 1};
    auto *dictionaryNested = new DictionaryVector(dictionaryVector, nestedIds, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
    }

    VectorBatch *batch = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {1, 1, 1};
    vector<DataType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes dataTypes(inputTypes);

    batch->SetVector(0, col1);
    batch->SetVector(1, col2);
    batch->SetVector(2, dictionaryNested);

    const int32_t numProjs = 3;
    FieldExpr *addLeft1 = new FieldExpr(0, IntType());
    LiteralExpr *addRight1 = new LiteralExpr(1, IntType());
    BinaryExpr *addExpr1 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft1, addRight1, IntType());

    FieldExpr *addLeft2 = new FieldExpr(1, IntType());
    LiteralExpr *addRight2 = new LiteralExpr(2, IntType());
    BinaryExpr *addExpr2 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft2, addRight2, IntType());

    FieldExpr *addLeft3 = new FieldExpr(2, IntType());
    LiteralExpr *addRight3 = new LiteralExpr(10, IntType());
    BinaryExpr *addExpr3 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft3, addRight3, IntType());

    std::vector<Expr *> exprs = { addExpr1, addExpr2, addExpr3 };

    auto *factory = new ProjectionOperatorFactory(exprs, numProjs, dataTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto copy = DuplicateVectorBatch(batch);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i) + 1);
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, col2->GetValue(i) + 2);
        int32_t val2 = ((IntVector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, dictionaryNested->GetInt(i) + 10);
    }

    VectorHelper::FreeVecBatch(batch);
    VectorHelper::FreeVecBatches(ret);

    delete col3;
    delete dictionaryVector;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, Decimal128Arithmetic)
{
    const int32_t numRows = 10;
    int64_t *col1 = MakeDecimals(numRows);
    const int32_t numProject = 1;
    FieldExpr *addLeft = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *addRight = new LiteralExpr(new std::string("20"), Decimal128Type(38, 0));
    BinaryExpr *addExpr =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, Decimal128Type(38, 0));

    std::vector<Expr *> exprs = { addExpr };
    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128) };
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Decimal128Arithmetic");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int64_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i + 20);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, DISABLED_Decimal128Arithmetic2)
{
    // currently fails
    const int32_t numRows = 10;
    int64_t *col0 = MakeDecimals(numRows, -5);
    int64_t *col1 = MakeDecimals(numRows, -7);
    const int32_t numCols = 2;

    FieldExpr *subLeft0 = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *subRight0 = new LiteralExpr(new string("1"), Decimal128Type(38, 0));
    BinaryExpr *subExpr0 =
        new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft0, subRight0, Decimal128Type(38, 0));

    FieldExpr *subLeft1 = new FieldExpr(1, Decimal128Type(38, 0));
    LiteralExpr *subRight1 = new LiteralExpr(-1, IntType());
    BinaryExpr *subExpr1 =
        new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft1, subRight1, Decimal128Type(38, 0));

    std::vector<Expr *> exprs = { subExpr0, subExpr1 };
    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128), DataType(OMNI_DECIMAL128) };
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0), reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DISABLED_Decimal128Arithmetic2");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int64_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        Decimal128 val1 = ((Decimal128Vector *)ret[0]->GetVector(1))->GetValue(i);
        Decimal128 old0 = ((Decimal128Vector *)t->GetVector(0))->GetValue(i);
        Decimal128 old1 = ((Decimal128Vector *)t->GetVector(1))->GetValue(i);
        if (i <= 5) {
            EXPECT_EQ(val0.HighBits(), -1);
            EXPECT_EQ(val0.LowBits(), old0.LowBits() + 1);
            EXPECT_EQ(val1.HighBits(), -1);
            EXPECT_EQ(val1.LowBits(), old1.LowBits() - 1);
        } else {
            EXPECT_EQ(val0.HighBits(), 0);
            EXPECT_EQ(val0.LowBits(), old0.LowBits() - 1);
            EXPECT_EQ(val1.HighBits(), 0);
            EXPECT_EQ(val1.LowBits(), old1.LowBits() + 1);
        }
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col0;
    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, DISABLED_Decimal128Arithmetic3)
{
    // currently fails
    const int32_t numRows = 10;
    int64_t *col0 = MakeDecimals(numRows, -5);
    int64_t *col1 = MakeDecimals(numRows, -7);
    const int32_t numCols = 2;

    FieldExpr *addLeft0 = new FieldExpr(0, Decimal128Type(38, 1));
    LiteralExpr *addRight0 = new LiteralExpr(new string("-1"), Decimal128Type(38, 0));
    BinaryExpr *addExpr0 =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft0, addRight0, Decimal128Type(38, 0));

    FieldExpr *addLeft1 = new FieldExpr(1, Decimal128Type(38, 0));
    LiteralExpr *addRight1 = new LiteralExpr(1, IntType());
    BinaryExpr *addExpr1 =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft1, addRight1, Decimal128Type(38, 0));

    std::vector<Expr *> exprs = { addExpr0, addExpr1 };
    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128), DataType(OMNI_DECIMAL128) };
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0), reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DISABLED_Decimal128Arithmetic3");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int64_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        Decimal128 val1 = ((Decimal128Vector *)ret[0]->GetVector(1))->GetValue(i);
        Decimal128 old0 = ((Decimal128Vector *)t->GetVector(0))->GetValue(i);
        Decimal128 old1 = ((Decimal128Vector *)t->GetVector(1))->GetValue(i);
        cout << old0.LowBits() << endl;
        cout << old1.LowBits() << endl;
        if (i <= 5) {
            EXPECT_EQ(val0.HighBits(), -1);
            EXPECT_EQ(val0.LowBits(), old0.LowBits() + 1);
            EXPECT_EQ(val1.HighBits(), -1);
            EXPECT_EQ(val1.LowBits(), old1.LowBits() - 1);
        } else {
            EXPECT_EQ(val0.HighBits(), 0);
            EXPECT_EQ(val0.LowBits(), old0.LowBits() - 1);
            EXPECT_EQ(val1.HighBits(), 0);
            EXPECT_EQ(val1.LowBits(), old1.LowBits() + 1);
        }
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col0;
    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, Decimal128Multiply)
{
    const int32_t numRows = 10;
    int64_t *col1 = MakeDecimals(numRows);
    const int32_t numProject = 1;
    FieldExpr *mulLeft = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *mulRight = new LiteralExpr(new std::string("3"), Decimal128Type(38, 0));
    BinaryExpr *mulExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, Decimal128Type(38, 0));

    std::vector<Expr *> exprs = { mulExpr };
    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128) };
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Decimal128Multiply");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int64_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i * 3);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, Decimal128Divide)
{
    const int32_t numRows = 10;
    int64_t *col1 = MakeDecimals(numRows);
    const int32_t numProject = 1;
    LiteralExpr *divRight = new LiteralExpr(new std::string("20"), Decimal128Type(38, 0));
    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV,
        new FieldExpr(0, Decimal128Type(38, 0)), divRight, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { divExpr };

    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128) };
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Decimal128Divide");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int64_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i / 20);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, MultipleDecimal128Columns)
{
    const int32_t numRows = 100;
    int64_t *col1 = MakeDecimals(numRows);
    int64_t *col2 = MakeDecimals(numRows, 100);
    const int32_t numProject = 2;
    FieldExpr *addLeft = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *addRight = new LiteralExpr(new std::string("50"), Decimal128Type(38, 0));
    BinaryExpr *addExpr =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, Decimal128Type(38, 0));

    FieldExpr *mulLeft = new FieldExpr(1, Decimal128Type(38, 0));
    LiteralExpr *mulRight = new LiteralExpr(new std::string("20"), Decimal128Type(38, 0));
    BinaryExpr *mulExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, Decimal128Type(38, 0));

    std::vector<Expr *> exprs = { addExpr, mulExpr };

    const int32_t numCols = 2;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128), DataType(OMNI_DECIMAL128) };
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_MultipleDecimal128Columns");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i + 50);
    }
    int idx = 0;
    for (int32_t i = 100; i < 100 + 100; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(1))->GetValue(idx);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i * 20);
        idx++;
    }


    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete[] col2;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, StringSubstr)
{
    vector<string *> strings;

    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_VARCHAR) };
    DataTypes inputTypes(vecOfTypes);

    const int32_t numRows = 100;
    int64_t *col1 = new int64_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            std::string *s = new std::string("helloasdf");
            col1[i] = reinterpret_cast<int64_t>(s->c_str());
            strings.push_back(s);
        } else {
            std::string *s = new std::string("Bonjour");
            col1[i] = reinterpret_cast<int64_t>(s->c_str());
            strings.push_back(s);
        }
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_StringSubstr");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);


    const int32_t numProject = 2;
    FieldExpr *substrData = new FieldExpr(0, VarcharType());
    LiteralExpr *substrIndex = new LiteralExpr(1, IntType());
    LiteralExpr *substrLen = new LiteralExpr(5, IntType());
    std::string substrStr = "substr";
    DataTypePtr retType = VarcharType();
    std::vector<Expr *> args;
    args.push_back(substrData);
    args.push_back(substrIndex);
    args.push_back(substrLen);
    auto substrExpr = GetFuncExpr(substrStr, args, VarcharType());

    std::vector<Expr *> concatArgs;
    std::string concatStr = "concat";
    concatArgs.push_back(substrExpr);
    concatArgs.push_back(new LiteralExpr(new std::string(" world"), VarcharType()));
    auto concatExpr = GetFuncExpr(concatStr, concatArgs, VarcharType());

    auto col0 = new FieldExpr(0, VarcharType());
    std::vector<Expr *> exprs = { concatExpr, col0 };
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    string expected1 = "hello world";
    string expected2 = "Bonjo world";
    for (int32_t i = 0; i < numReturned; i += 20) {
        VarcharVector *vcVec = ((VarcharVector *)ret[0]->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);

        string actualStr(reinterpret_cast<char *>(actualChar), 0, len);
        if (i % 2 == 0) {
            EXPECT_EQ(actualStr, expected1);
        } else {
            EXPECT_EQ(actualStr, expected2);
        }
        std::cout << "string " << i << ": '" << actualStr << "' has length " << len << std::endl;
    }


    for (auto &s : strings) {
        delete s;
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, SlicedDictionaryVecTest)
{
    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_SlicedDictionaryVecTest");
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

    VectorBatch *input = new VectorBatch(numCols, slicedCol1->GetSize());
    int32_t inputTypeIds[numCols] = {1, 1, 1};
    vector<DataType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes inputDataTypes(inputTypes);

    input->SetVector(0, slicedCol1);
    input->SetVector(1, slicedCol2);
    input->SetVector(2, slicedCol3);

    const int32_t numProject = 3;

    FieldExpr *addLeft1 = new FieldExpr(0, IntType());
    LiteralExpr *addRight1 = new LiteralExpr(1, IntType());
    BinaryExpr *addExpr1 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft1, addRight1, IntType());

    FieldExpr *addLeft2 = new FieldExpr(1, IntType());
    LiteralExpr *addRight2 = new LiteralExpr(2, IntType());
    BinaryExpr *addExpr2 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft2, addRight2, IntType());

    FieldExpr *addLeft3 = new FieldExpr(2, IntType());
    LiteralExpr *addRight3 = new LiteralExpr(10, IntType());
    BinaryExpr *addExpr3 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft3, addRight3, IntType());

    std::vector<Expr *> exprs = { addExpr1, addExpr2, addExpr3 };
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, inputDataTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto copy = DuplicateVectorBatch(input);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, slicedCol1->GetValue(i) + 1);
        int32_t val1 = ((IntVector *)ret[0]->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, slicedCol2->GetValue(i) + 2);
        int32_t val2 = ((IntVector *)ret[0]->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, slicedCol3->GetInt(i) + 10);
    }
    VectorHelper::FreeVecBatch(input);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, SlicedDictionaryVecWithNullTest)
{
    const int32_t numCols = 1;
    const int32_t numRows = 10;

    auto vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_SlicedDictionaryVecWithNullTest");
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            col1->SetValueNull(i);
        } else {
            col1->SetValue(i, i % 5);
        }
    }
    int32_t ids[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    auto dictionaryVector = new DictionaryVector(col1, ids, numRows);
    auto slicedCol1 = dictionaryVector->Slice(5, 5);
    delete col1;
    delete dictionaryVector;

    VectorBatch *input = new VectorBatch(numCols, slicedCol1->GetSize());
    int32_t inputTypeIds[numCols] = {1};
    vector<DataType> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);

    input->SetVector(0, slicedCol1);
    DataTypes inputVecTypes(inputTypes);
    const int32_t numProject = 1;

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    FieldExpr *addRight = new FieldExpr(0, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    std::vector<Expr *> exprs = { addExpr };

    auto *factory = new ProjectionOperatorFactory(exprs, numProject, inputVecTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto copy = DuplicateVectorBatch(input);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    auto retVec = (IntVector *)(ret[0]->GetVector(0));
    for (int32_t i = 0; i < numReturned; i++) {
        if (i == 0) {
            EXPECT_TRUE(retVec->IsValueNull(i));
        } else {
            int64_t val0 = retVec->GetValue(i);
            EXPECT_EQ(val0, ((DictionaryVector *)slicedCol1)->GetInt(i) * 2);
        }
    }
    VectorHelper::FreeVecBatch(input);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, Tpcds96)
{
    const int32_t numRows = 21;
    int64_t *col0 = new int64_t[21];
    int64_t *col1 = new int64_t[21];
    int64_t *col2 = new int64_t[21];
    int64_t *col3 = new int64_t[21];
    vector<long> v0 = { 48, 9, 20, 16, 5, 20, 58, 12, 4, 71, 2, 13, 1, 1, 3, 80, 22, 22, 23, 14, 23 };
    vector<long> v1 = {
        139650736612376, 102, 71, 23, 13, 122, 46, 2, 52, 51, 39, 3, 63, 28, 38, 43, 52, 16, 72, 54, 2
    };
    vector<long> v2 = { 33, 17, 14, 0, 49, 21, 8, 56, 16, 2, 18, 47, 0, 15, 27, 58, 0, 4, 5, 3, 23 };
    vector<long> v3(21, 0);

    for (int i = 0; i < numRows; i++) {
        col0[i] = v0[i];
        col1[i] = v1[i];
        col2[i] = v2[i];
        col3[i] = v3[i];
    }

    const int32_t numCols = 4;
    FieldExpr *addCol0 = new FieldExpr(0, LongType());
    FieldExpr *addCol1 = new FieldExpr(1, LongType());
    FieldExpr *addCol2_0 = new FieldExpr(2, LongType());
    FieldExpr *addCol2_1 = new FieldExpr(2, LongType());
    BinaryExpr *addExpr1 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addCol0, addCol1, LongType());
    BinaryExpr *addExpr2 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addExpr1, addCol2_0, LongType());
    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, addCol2_1, addExpr2, LongType());
    FieldExpr *expectRes = new FieldExpr(3, LongType());
    std::vector<Expr *> exprs = { divExpr, expectRes };

    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG), DataType(OMNI_LONG), DataType(OMNI_LONG),
        DataType(OMNI_LONG) };
    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, exprs.size(), inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0), reinterpret_cast<int64_t>(col1),
        reinterpret_cast<int64_t>(col2), reinterpret_cast<int64_t>(col3)};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Tpcds96");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);

        if (i == 0) {
            t->GetVector(1)->SetValueNull(i);
            t->GetVector(2)->SetValueNotNull(i);
            t->GetVector(3)->SetValueNull(i);
        } else if (i == 3 || i == 12 || i == 16) {
            t->GetVector(1)->SetValueNotNull(i);
            t->GetVector(2)->SetValueNull(i);
            t->GetVector(3)->SetValueNull(i);
        } else {
            t->GetVector(1)->SetValueNotNull(i);
            t->GetVector(2)->SetValueNotNull(i);
            t->GetVector(3)->SetValueNotNull(i);
        }
    }

    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numRows, numReturned);
    for (int32_t i = 0; i < numReturned; i++) {
        int64_t val0 = ((LongVector *)ret[0]->GetVector(0))->GetValue(i);
        bool b0 = ((LongVector *)ret[0]->GetVector(0))->IsValueNull(i);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        bool b1 = ((LongVector *)ret[0]->GetVector(1))->IsValueNull(i);
        EXPECT_EQ(val0, val1);
        EXPECT_EQ(b0, b1);
    }

    VectorHelper::FreeVecBatches(ret);

    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, Round)
{
    const int32_t numRows = 1000;
    int32_t *col0 = MakeInts(numRows, -5);
    int64_t *col1 = MakeLongs(numRows, -5);
    double *col2 = MakeDoubles(numRows, -5.123);
    double *col3 = MakeDoubles(numRows, -5.123);
    double *col4 = MakeDoubles(numRows, -1001.456);
    // Make NaN and inf values
    auto *col5 = new double[numRows];
    double start = 1;
    int32_t idx = 0;
    for (double i = start; i < start + numRows; i++) {
        if (idx % 2 == 0) {
            // NaN
            col5[idx++] = std::sqrt(-i);
        } else {
            // inf
            col5[idx++] = i / 0.0;
        }
    }
    const int32_t numCols = 6;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT),    DataType(OMNI_LONG),   DataType(OMNI_DOUBLE),
        DataType(OMNI_DOUBLE), DataType(OMNI_DOUBLE), DataType(OMNI_DOUBLE) };
    std::string funcStr = "round";

    // rounded to tenths. since int type, no decimals, so should be same as input
    auto data0 = new FieldExpr(0, IntType());
    std::vector<Expr *> args0;
    args0.push_back(data0);
    int32_t data0Decimal = 1;
    args0.push_back(new LiteralExpr(data0Decimal, IntType()));
    auto roundExpr0 = GetFuncExpr(funcStr, args0, IntType());

    // rounded to tens
    auto data1 = new FieldExpr(1, LongType());
    std::vector<Expr *> args1;
    args1.push_back(data1);
    int32_t data1Decimal = -1;
    args1.push_back(new LiteralExpr(data1Decimal, IntType()));
    auto roundExpr1 = GetFuncExpr(funcStr, args1, LongType());
    double data1Pow = pow(10, data1Decimal);

    // rounded to ones. equivalent to round()
    auto data2 = new FieldExpr(2, DoubleType());
    std::vector<Expr *> args2;
    args2.push_back(data2);
    int32_t data2Decimal = 0;
    args2.push_back(new LiteralExpr(data2Decimal, IntType()));
    auto roundExpr2 = GetFuncExpr(funcStr, args2, DoubleType());

    // rounded to hundredths
    auto data3 = new FieldExpr(3, DoubleType());
    std::vector<Expr *> args3;
    args3.push_back(data3);
    int32_t data3Decimal = 2;
    args3.push_back(new LiteralExpr(data3Decimal, IntType()));
    auto roundExpr3 = GetFuncExpr(funcStr, args3, DoubleType());
    double data3Pow = pow(10, data3Decimal);

    // rounded to hundredths (all negatives)
    auto data4 = new FieldExpr(4, DoubleType());
    std::vector<Expr *> args4;
    args4.push_back(data4);
    int32_t data4Decimal = 2;
    args4.push_back(new LiteralExpr(data4Decimal, IntType()));
    auto roundExpr4 = GetFuncExpr(funcStr, args4, DoubleType());
    double data4Pow = pow(10, data4Decimal);

    // invalid values
    auto data5 = new FieldExpr(5, DoubleType());
    std::vector<Expr *> args5;
    args5.push_back(data5);
    int32_t data5Decimal = 2;
    args5.push_back(new LiteralExpr(data5Decimal, IntType()));
    auto roundExpr5 = GetFuncExpr(funcStr, args5, DoubleType());

    std::vector<Expr *> exprs = { roundExpr0, roundExpr1, roundExpr2, roundExpr3, roundExpr4, roundExpr5 };

    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, numCols, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col0), reinterpret_cast<int64_t>(col1),
        reinterpret_cast<int64_t>(col2), reinterpret_cast<int64_t>(col3), reinterpret_cast<int64_t>(col4),
        reinterpret_cast<int64_t>(col5)};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Round");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)ret[0]->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret[0]->GetVector(1))->GetValue(i);
        double val2 = ((DoubleVector *)ret[0]->GetVector(2))->GetValue(i);
        double val3 = ((DoubleVector *)ret[0]->GetVector(3))->GetValue(i);
        double val4 = ((DoubleVector *)ret[0]->GetVector(4))->GetValue(i);
        double val5 = ((DoubleVector *)ret[0]->GetVector(5))->GetValue(i);

        int32_t old0 = ((IntVector *)t->GetVector(0))->GetValue(i);
        int64_t old1 = ((LongVector *)t->GetVector(1))->GetValue(i);
        double old2 = ((DoubleVector *)t->GetVector(2))->GetValue(i);
        double old3 = ((DoubleVector *)t->GetVector(3))->GetValue(i);
        double old4 = ((DoubleVector *)t->GetVector(4))->GetValue(i);
        double old5 = ((DoubleVector *)t->GetVector(5))->GetValue(i);

        EXPECT_EQ(val0, round(old0));
        EXPECT_EQ(val0, old0);
        EXPECT_EQ(val1, (round(old1 * data1Pow) / data1Pow));
        EXPECT_EQ(val2, round(old2));
        EXPECT_EQ(val3, (round(old3 * data3Pow) / data3Pow));
        EXPECT_EQ(val4, -(round(-old4 * data4Pow) / data4Pow));
        if (i % 2 == 0) {
            // cannot compare NaN values
            EXPECT_TRUE(isnan(val5) && isnan(old5));
        } else {
            EXPECT_EQ(val5, old5);
        }
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete[] col5;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, ConcatStrAndChar)
{
    vector<string *> strings;

    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_CHAR) };
    DataTypes inputTypes(vecOfTypes);

    const int32_t numRows = 1;
    int64_t *col1 = new int64_t[numRows];

    std::string *s = new std::string("AAAA");
    col1[0] = reinterpret_cast<int64_t>(s->c_str());
    strings.push_back(s);

    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ConcatStrAndChar");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    const int32_t numProject = 2;
    std::vector<Expr *> concatArgs1, concatArgs2;
    std::string concatStr = "concat";
    concatArgs1.push_back(new LiteralExpr(new std::string("store"), VarcharType()));
    concatArgs1.push_back(new FieldExpr(0, CharType(16)));
    auto concatStrChar = GetFuncExpr(concatStr, concatArgs1, CharType(21));

    concatArgs2.push_back(new FieldExpr(0, CharType(16)));
    concatArgs2.push_back(new LiteralExpr(new std::string("store"), VarcharType()));
    auto concatCharStr = GetFuncExpr(concatStr, concatArgs2, CharType(21));

    std::vector<Expr *> exprs = { concatStrChar, concatCharStr };

    auto *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);

    string expected1 = "storeAAAA            ";
    string expected2 = "AAAA            store";
    for (int32_t i = 0; i < numReturned; i++) {
        VarcharVector *vcVec1 = ((VarcharVector *)ret[0]->GetVector(0));
        uint8_t *actualChar1 = nullptr;
        int len1 = vcVec1->GetValue(i, &actualChar1);
        string actualStr1(reinterpret_cast<char *>(actualChar1), 0, len1);
        EXPECT_EQ(actualStr1, expected1);

        VarcharVector *vcVec2 = ((VarcharVector *)ret[0]->GetVector(1));
        uint8_t *actualChar2 = nullptr;
        int len2 = vcVec2->GetValue(i, &actualChar2);
        string actualStr2(reinterpret_cast<char *>(actualChar2), 0, len2);
        EXPECT_EQ(actualStr2, expected2);
    }

    for (auto &s : strings) {
        delete s;
    }
    VectorHelper::FreeVecBatches(ret);
    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, varcharExpand)
{
    vector<string *> strings;

    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_VARCHAR) };
    DataTypes inputTypes(vecOfTypes);

    const int32_t numRows = 100;
    int64_t *col1 = new int64_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            std::string *s = new std::string("helloasdf");
            col1[i] = reinterpret_cast<int64_t>(s->c_str());
            strings.push_back(s);
        } else {
            std::string *s = new std::string("Bonjour");
            col1[i] = reinterpret_cast<int64_t>(s->c_str());
            strings.push_back(s);
        }
    }
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_varcharExpand");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);


    const int32_t numProject = 2;
    FieldExpr *substrData = new FieldExpr(0, VarcharType());
    LiteralExpr *substrIndex = new LiteralExpr(1, IntType());
    LiteralExpr *substrLen = new LiteralExpr(5, IntType());

    std::string substrStr = "substr";
    DataTypePtr retType = VarcharType();
    std::vector<Expr *> args;
    args.push_back(substrData);
    args.push_back(substrIndex);
    args.push_back(substrLen);
    auto substrExpr = GetFuncExpr(substrStr, args, VarcharType());
    std::string baseStr(" world");
    int32_t avgStrLen = 200;
    int32_t strLen = 0;
    for (int i = 0; i < 1000000; i++) {
        baseStr.append(std::to_string(i));
        strLen = baseStr.length();
        if (strLen > avgStrLen) {
            break;
        }
    }
    std::vector<Expr *> concatArgs;
    std::string concatStr = "concat";
    concatArgs.push_back(substrExpr);
    concatArgs.push_back(new LiteralExpr(new std::string(baseStr), VarcharType()));
    auto concatExpr = GetFuncExpr(concatStr, concatArgs, VarcharType());

    FieldExpr *col0 = new FieldExpr(0, VarcharType());
    std::vector<Expr *> exprs = { concatExpr, col0 };
    auto *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    std::vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, numRows);

    EXPECT_GT(ret[0]->GetVector(0)->GetCapacityInBytes(), avgStrLen * numReturned);
    string expected1 = "hello" + baseStr;
    string expected2 = "Bonjo" + baseStr;
    for (int32_t i = 0; i < numReturned; i++) {
        VarcharVector *vcVec = ((VarcharVector *)ret[0]->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);

        string actualStr(reinterpret_cast<char *>(actualChar), 0, len);
        if (i % 2 == 0) {
            EXPECT_EQ(actualStr, expected1);
        } else {
            EXPECT_EQ(actualStr, expected2);
        }
    }

    for (auto &s : strings) {
        delete s;
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, testDivDecimal128)
{
    const int32_t numRows = 1;
    const int32_t numProject = 1;
    auto addLeft = new LiteralExpr(new std::string("10357"), Decimal128Type(5, 2));
    auto addRight = new LiteralExpr(new std::string("95942"), Decimal128Type(5, 2));

    BinaryExpr *divExpr =
        new BinaryExpr(omniruntime::expressions::Operator::DIV, addLeft, addRight, Decimal128Type(38, 2));

    std::vector<Expr *> exprs = { divExpr };
    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_testDivDecimal");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 1);
    Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0.LowBits(), 11);
    EXPECT_EQ(val0.HighBits(), 0);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
    delete vecAllocator;
}


TEST(ProjectionTest, testAddDecimal128)
{
    const int32_t numRows = 1;
    const int32_t numProject = 1;
    auto addLeft = new LiteralExpr(new std::string("478193"), Decimal128Type(6, 3));
    auto addRight = new LiteralExpr(new std::string("54356783"), Decimal128Type(8, 5));

    BinaryExpr *addExpr =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, Decimal128Type(9, 5));

    std::vector<Expr *> exprs = { addExpr };
    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {};
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_testAddDecimal128");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 1);
    Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0.LowBits(), 102176083);
    EXPECT_EQ(val0.HighBits(), 0);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, testDecimal128Between)
{
    const int32_t numRows = 1;
    const int32_t numProject = 1;
    auto value = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 0));
    auto lowerBound = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 3));
    auto upperBound = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 1));

    auto expr = new BetweenExpr(value, lowerBound, upperBound);

    std::vector<Expr *> exprs = { expr };
    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_testDecimal128Between");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 1);
    bool val0 = ((BooleanVector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_FALSE(val0);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
    delete vecAllocator;
}


TEST(ProjectionTest, testDecimal128In)
{
    const int32_t numRows = 1;
    const int32_t numProject = 1;
    auto arg0 = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 0));
    auto arg1 = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 3));
    auto arg2 = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 0));

    std::vector<Expr *> args = { arg0, arg1, arg2 };

    auto expr = new InExpr(args);

    std::vector<Expr *> exprs = { expr };
    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {};
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_testDecimal128In");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 1);
    bool val0 = ((BooleanVector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_TRUE(val0);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
    delete vecAllocator;
}


TEST(ProjectionTest, testDecimal128Comprehensive)
{
    const int32_t numRows = 1;
    const int32_t numProject = 1;
    auto condition = new LiteralExpr(true, BooleanType());
    auto v1 = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 1));
    v1->isNull = true;
    auto v2 = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 3));
    auto coalesce = new CoalesceExpr(v1, v2);

    auto falseExpr = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 0));
    auto ifExpr = new IfExpr(condition, coalesce, falseExpr);

    auto right = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 2));

    auto expr = new BinaryExpr(omniruntime::expressions::Operator::GT, ifExpr, right, BooleanType());


    std::vector<Expr *> exprs = { expr };
    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_trstDecimalComprehensive");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, 1);

    bool val0 = ((BooleanVector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_FALSE(val0);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectTest, TestAndExprWithNull)
{
    const int32_t numRows = 9;
    bool col1[numRows] = {true, true, true, false, false, false, true, false, false};
    bool col2[numRows] = {true, false, true, true, false, false, true, false, false};

    const int32_t numCols = 2;
    auto andLeft = new FieldExpr(0, BooleanType());
    auto andRight = new FieldExpr(1, BooleanType());
    BinaryExpr *andExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, andLeft, andRight, BooleanType());
    std::vector<Expr *> exprs = { andExpr };
    std::vector<DataType> vecOfTypes = { DataType(OMNI_BOOLEAN), DataType(OMNI_BOOLEAN) };
    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, 1, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2) };
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_TestAndExprWithNull");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    t->GetVector(0)->SetValueNull(6);
    t->GetVector(0)->SetValueNull(7);
    t->GetVector(0)->SetValueNull(8);

    t->GetVector(1)->SetValueNull(2);
    t->GetVector(1)->SetValueNull(5);
    t->GetVector(1)->SetValueNull(8);

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        bool val = ((BooleanVector *)ret[0]->GetVector(0))->GetValue(i);
        bool isValNull = ((BooleanVector *)ret[0]->GetVector(0))->IsValueNull(i);
        if (i == 0) {
            EXPECT_TRUE(val);
            EXPECT_FALSE(isValNull);
        } else if (i == 2 || i == 6 || i == 8) {
            EXPECT_TRUE(isValNull);
        } else {
            EXPECT_FALSE(val);
            EXPECT_FALSE(isValNull);
        }
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectTest, TestOrExprWithNull)
{
    const int32_t numRows = 9;
    bool col1[numRows] = {true, true, true, false, false, false, true, false, false};
    bool col2[numRows] = {true, false, true, true, false, false, true, false, false};

    const int32_t numCols = 2;
    auto orLeft = new FieldExpr(0, BooleanType());
    auto orRight = new FieldExpr(1, BooleanType());
    BinaryExpr *orExpr = new BinaryExpr(omniruntime::expressions::Operator::OR, orLeft, orRight, BooleanType());
    std::vector<Expr *> exprs = { orExpr };
    std::vector<DataType> vecOfTypes = { DataType(OMNI_BOOLEAN), DataType(OMNI_BOOLEAN) };
    DataTypes inputTypes(vecOfTypes);
    auto *factory = new ProjectionOperatorFactory(exprs, 1, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1), reinterpret_cast<int64_t>(col2) };
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_TestOrExprWithNull");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    t->GetVector(0)->SetValueNull(6);
    t->GetVector(0)->SetValueNull(7);
    t->GetVector(0)->SetValueNull(8);

    t->GetVector(1)->SetValueNull(2);
    t->GetVector(1)->SetValueNull(5);
    t->GetVector(1)->SetValueNull(8);

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int32_t i = 0; i < numReturned; i++) {
        bool val = ((BooleanVector *)ret[0]->GetVector(0))->GetValue(i);
        bool isValNull = ((BooleanVector *)ret[0]->GetVector(0))->IsValueNull(i);
        if (i == 4) {
            EXPECT_FALSE(val);
            EXPECT_FALSE(isValNull);
        } else if (i == 5 || i == 7 || i == 8) {
            EXPECT_TRUE(isValNull);
        } else {
            EXPECT_TRUE(val);
            EXPECT_FALSE(isValNull);
        }
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
    delete vecAllocator;
}


TEST(ProjectionTest, testSubDecimal64)
{
    const int32_t numRows = 1;
    const int32_t numProject = 1;
    auto subLeft = new LiteralExpr(4321563L, Decimal64Type(7, 3));
    auto subRight = new LiteralExpr(123468L, Decimal64Type(6, 4));

    BinaryExpr *subExpr =
        new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, Decimal64Type(8, 4));

    std::vector<Expr *> exprs = { subExpr };
    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testSubDecimal64");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    int64_t val0 = ((LongVector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 43092162);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
}

TEST(ProjectionTest, testMulDecimal64)
{
    const int32_t numRows = 1;
    const int32_t numProject = 1;
    auto mulLeft = new LiteralExpr(100L, Decimal64Type(7, 2));
    auto mulRight = new LiteralExpr(100L, Decimal64Type(7, 2));

    BinaryExpr *mulExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, Decimal64Type(7, 2));

    std::vector<Expr *> exprs = { mulExpr };
    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testMulDecimal64");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    int64_t val0 = ((LongVector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 100L);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
}

TEST(ProjectionTest, testDivDecimal64)
{
    const int32_t numRows = 1;
    const int32_t numProject = 1;
    auto addLeft = new LiteralExpr(1225L, Decimal64Type(4, 2));
    auto addRight = new LiteralExpr(125L, Decimal64Type(3, 2));

    BinaryExpr *divExpr =
        new BinaryExpr(omniruntime::expressions::Operator::DIV, addLeft, addRight, Decimal64Type(2, 1));

    std::vector<Expr *> exprs = { divExpr };
    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDivDecimal64");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    int64_t val0 = ((LongVector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 98L);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
}

TEST(ProjectionTest, DISABLED_testDecimal64ArithOutputDecimal128)
{
    const int32_t numRows = 1;
    auto addLeft = new LiteralExpr(-999999999999999999L, Decimal64Type(18, 0));
    auto addRight = new LiteralExpr(2L, Decimal64Type(18, 0));
    BinaryExpr *addExpr =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, Decimal128Type(19, 0));
    auto subLeft = new LiteralExpr(-999999999999999999L, Decimal64Type(18, 0));
    auto subRight = new LiteralExpr(2L, Decimal64Type(18, 0));

    BinaryExpr *subExpr =
        new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, Decimal128Type(19, 0));
    std::vector<Expr *> exprs = { addExpr, subExpr };

    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, exprs.size(), inputTypes, numCols);

    // Not supporting promote decimal64 to decimal128 for now
    EXPECT_FALSE(factory->IsSupported());
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDecimal64ArithOutputDecimal128");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    EXPECT_EQ(numReturned, numRows);
    Decimal128 val0 = ((Decimal128Vector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0.HighBits(), 1L << 63);
    EXPECT_EQ(val0.LowBits(), 999999999999999997L);
    Decimal128 val1 = ((Decimal128Vector *)ret[0]->GetVector(1))->GetValue(0);
    EXPECT_EQ(val1.HighBits(), 1L << 63);
    EXPECT_EQ(val1.LowBits(), 1000000000000000001);

    VectorHelper::FreeVecBatches(ret);
    delete op;
    delete factory;
}

TEST(ProjectionTest, testDecimal64In)
{
    const int32_t numRows = 1;
    const int32_t numProject = 1;
    auto arg0 = new LiteralExpr(65781L, Decimal64Type(5, 4));
    auto arg1 = new LiteralExpr(120945L, Decimal64Type(6, 2));
    auto arg2 = new LiteralExpr(65781L, Decimal64Type(5, 3));
    auto arg3 = new LiteralExpr(65781L, Decimal64Type(5, 4));

    std::vector<Expr *> args = { arg0, arg1, arg2, arg3 };

    auto expr = new InExpr(args);

    std::vector<Expr *> exprs = { expr };
    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDecimal64In");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    bool val0 = ((BooleanVector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_TRUE(val0);
    VectorHelper::FreeVecBatches(ret);
    delete op;
    delete factory;
}

TEST(ProjectionTest, testDecimal64Between)
{
    const int32_t numRows = 1;
    const int32_t numProject = 1;
    auto value = new LiteralExpr(76582L, Decimal64Type(5, 2));
    auto lowerBound = new LiteralExpr(87230L, Decimal64Type(5, 4));
    auto upperBound = new LiteralExpr(876903L, Decimal64Type(6, 1));

    auto expr = new BetweenExpr(value, lowerBound, upperBound);

    std::vector<Expr *> exprs = { expr };
    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {};
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDecimal64Between");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    bool val0 = ((BooleanVector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_TRUE(val0);
    VectorHelper::FreeVecBatches(ret);

    delete op;
    delete factory;
}

TEST(ProjectionTest, testDecimal64Comprehensive)
{
    const int32_t numRows = 1;
    const int32_t numProject = 1;
    auto condition = new LiteralExpr(true, BooleanType());
    auto v1 = new LiteralExpr(1234L, Decimal64Type(4, 1));
    v1->isNull = true;
    auto v2 = new LiteralExpr(1234L, Decimal64Type(4, 3));
    auto coalesce = new CoalesceExpr(v1, v2);

    auto falseExpr = new LiteralExpr(1234L, Decimal64Type(4, 0));
    auto ifExpr = new IfExpr(condition, coalesce, falseExpr);

    auto subLeft = new LiteralExpr(1234L, Decimal64Type(4, 2));
    auto subRight = new LiteralExpr(101L, Decimal64Type(3, 2));
    auto right = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, Decimal64Type(4, 2));
    auto expr = new BinaryExpr(omniruntime::expressions::Operator::GT, ifExpr, right, BooleanType());

    std::vector<Expr *> exprs = { expr };
    const int32_t numCols = 0;
    std::vector<DataType> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, numProject, inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();

    int64_t allData[numCols] = {};
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDecimal64Comprehensive");
    VectorBatch *input = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);
    op->AddInput(input);
    vector<VectorBatch *> ret;
    op->GetOutput(ret);

    bool val0 = ((BooleanVector *)ret[0]->GetVector(0))->GetValue(0);
    EXPECT_FALSE(val0);
    VectorHelper::FreeVecBatches(ret);
    delete op;
    delete factory;
}

TEST(ProjectionTest, Decimal64ColDivide)
{
    const int32_t numRows = 1000;
    int64_t *col1 = MakeLongs(numRows, -500);
    LiteralExpr *divRight = new LiteralExpr(92122L, Decimal64Type(8, 4));
    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, new FieldExpr(0, Decimal64Type(8, 4)),
        divRight, Decimal64Type(8, 4));
    std::vector<Expr *> exprs = { divExpr };

    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL64) };
    DataTypes inputTypes(vecOfTypes);
    ProjectionOperatorFactory *factory = new ProjectionOperatorFactory(exprs, exprs.size(), inputTypes, numCols);
    omniruntime::op::Operator *op = factory->CreateOperator();
    int64_t allData[numCols] = {reinterpret_cast<int64_t>(col1)};
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Decimal64ColDivide");
    VectorBatch *t = CreateInput(vecAllocator, numRows, numCols, inputTypes.GetIds(), allData);

    op->AddInput(t);
    vector<VectorBatch *> ret;
    int32_t numReturned = op->GetOutput(ret);
    for (int64_t i = 0; i < numReturned; i++) {
        int64_t val0 = ((LongVector *)ret[0]->GetVector(0))->GetValue(i);
        int64_t expect = round(double(col1[i] * 10000) / 92122);
        EXPECT_EQ(val0, expect);
    }

    VectorHelper::FreeVecBatches(ret);

    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
}

Expr *GetConcatFuncExpr(DataTypePtr dataType0, DataTypePtr dataType1, DataTypePtr returnType)
{
    std::string concatStr = "concat";
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(1, std::move(dataType1)));
    args.push_back(new FieldExpr(0, std::move(dataType0)));
    auto concatExpr = GetFuncExpr(concatStr, args, std::move(returnType));
    return concatExpr;
}

VectorBatch *CreateInputVecBatchForConcat(const std::vector<DataType> &inputTypes, VectorAllocator *vecAllocator)
{
    const int32_t rowCount = 8;
    const std::string firstName = "John";
    const std::string lastName = "Rebecca";
    const std::string fullFirstName = "-John-John-John-John";
    const std::string fullLastName = "Rebecca-Rebecca-Rebecca-Rebeca";
    const std::string empty = "";

    auto vec0 = new VarcharVector(vecAllocator, rowCount * inputTypes[0].GetWidth(), rowCount);
    vec0->SetValueNull(0);
    vec0->SetValue(1, reinterpret_cast<const unsigned char *>(firstName.c_str()), firstName.length());
    vec0->SetValueNull(2);
    vec0->SetValue(3, reinterpret_cast<const unsigned char *>(empty.c_str()), empty.length());
    vec0->SetValue(4, reinterpret_cast<const unsigned char *>(firstName.c_str()), firstName.length());
    vec0->SetValue(5, reinterpret_cast<const unsigned char *>(empty.c_str()), empty.length());
    vec0->SetValue(6, reinterpret_cast<const unsigned char *>(firstName.c_str()), firstName.length());
    vec0->SetValue(7, reinterpret_cast<const unsigned char *>(fullFirstName.c_str()), fullFirstName.length());

    auto vec1 = new VarcharVector(vecAllocator, rowCount * inputTypes[1].GetWidth(), rowCount);
    vec1->SetValueNull(0);
    vec1->SetValueNull(1);
    vec1->SetValue(2, reinterpret_cast<const unsigned char *>(lastName.c_str()), lastName.length());
    vec1->SetValue(3, reinterpret_cast<const unsigned char *>(empty.c_str()), empty.length());
    vec1->SetValue(4, reinterpret_cast<const unsigned char *>(empty.c_str()), empty.length());
    vec1->SetValue(5, reinterpret_cast<const unsigned char *>(lastName.c_str()), lastName.length());
    vec1->SetValue(6, reinterpret_cast<const unsigned char *>(lastName.c_str()), lastName.length());
    vec1->SetValue(7, reinterpret_cast<const unsigned char *>(fullLastName.c_str()), fullLastName.length());

    auto input = new VectorBatch(inputTypes.size(), rowCount);
    input->SetVector(0, vec0);
    input->SetVector(1, vec1);
    return input;
}

VectorBatch *CreateExpectVecBatchForConcat(const DataType &expectDataType, VectorAllocator *vecAllocator,
    const std::vector<std::string> &expectDatas)
{
    int32_t rowCount = expectDatas.size();
    auto expectVec = new VarcharVector(vecAllocator, rowCount * expectDataType.GetWidth(), rowCount);
    for (int32_t i = 0; i < rowCount; i++) {
        if (expectDatas[i] == "NULL") {
            expectVec->SetValueNull(i);
        } else {
            expectVec->SetValue(i, reinterpret_cast<const unsigned char *>(expectDatas[i].c_str()),
                expectDatas[i].length());
        }
    }

    auto expectVecBatch = new VectorBatch(1, rowCount);
    expectVecBatch->SetVector(0, expectVec);
    return expectVecBatch;
}

TEST(ProjectionTest, ConcatStrCharTest)
{
    auto concatExpr = GetConcatFuncExpr(CharType(20), VarcharType(30), CharType(100));
    std::vector<Expr *> exprs = { concatExpr };
    std::vector<DataType> vecOfTypes = { CharDataType(20), VarcharDataType(30) };
    DataTypes inputTypes(vecOfTypes);
    auto factory = new ProjectionOperatorFactory(exprs, exprs.size(), inputTypes, inputTypes.GetSize());

    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ConcatStrCharTest");
    auto input = CreateInputVecBatchForConcat(vecOfTypes, vecAllocator);

    auto op = factory->CreateOperator();
    op->AddInput(input);
    std::vector<VectorBatch *> ret;
    op->GetOutput(ret);

    std::vector<std::string> expectedDatas = { "NULL",
        "NULL",
        "NULL",
        "                    ",
        "John                ",
        "Rebecca                    ",
        "RebeccaJohn                ",
        "Rebecca-Rebecca-Rebecca-Rebeca-John-John-John-John" };
    auto expect = CreateExpectVecBatchForConcat(CharDataType(100), vecAllocator, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(ret[0], expect));

    VectorHelper::FreeVecBatches(ret);
    VectorHelper::FreeVecBatch(expect);
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, ConcatCharStrTest)
{
    auto concatExpr = GetConcatFuncExpr(VarcharType(20), CharType(30), CharType(100));
    std::vector<Expr *> exprs = { concatExpr };
    std::vector<DataType> vecOfTypes = { VarcharDataType(20), CharDataType(30) };
    DataTypes inputTypes(vecOfTypes);
    auto factory = new ProjectionOperatorFactory(exprs, exprs.size(), inputTypes, inputTypes.GetSize());

    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ConcatCharStrTest");
    auto input = CreateInputVecBatchForConcat(vecOfTypes, vecAllocator);

    auto op = factory->CreateOperator();
    op->AddInput(input);
    std::vector<VectorBatch *> ret;
    op->GetOutput(ret);

    std::vector<std::string> expectedDatas = { "NULL",
        "NULL",
        "NULL",
        "                              ",
        "                              John",
        "Rebecca                       ",
        "Rebecca                       John",
        "Rebecca-Rebecca-Rebecca-Rebeca-John-John-John-John" };
    auto expect = CreateExpectVecBatchForConcat(CharDataType(100), vecAllocator, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(ret[0], expect));

    VectorHelper::FreeVecBatches(ret);
    VectorHelper::FreeVecBatch(expect);
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, ConcatStrStrTest)
{
    auto concatExpr = GetConcatFuncExpr(VarcharType(20), VarcharType(30), VarcharType(100));
    std::vector<Expr *> exprs = { concatExpr };
    std::vector<DataType> vecOfTypes = { VarcharDataType(20), VarcharDataType(30) };
    DataTypes inputTypes(vecOfTypes);
    auto factory = new ProjectionOperatorFactory(exprs, exprs.size(), inputTypes, inputTypes.GetSize());

    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ConcatStrStrTest");
    auto input = CreateInputVecBatchForConcat(vecOfTypes, vecAllocator);

    auto op = factory->CreateOperator();
    op->AddInput(input);
    std::vector<VectorBatch *> ret;
    op->GetOutput(ret);

    std::vector<std::string> expectedDatas = { "NULL",        "NULL",
        "NULL",        "",
        "John",        "Rebecca",
        "RebeccaJohn", "Rebecca-Rebecca-Rebecca-Rebeca-John-John-John-John" };
    auto expect = CreateExpectVecBatchForConcat(VarcharDataType(100), vecAllocator, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(ret[0], expect));

    VectorHelper::FreeVecBatches(ret);
    VectorHelper::FreeVecBatch(expect);
    delete op;
    delete factory;
    delete vecAllocator;
}

TEST(ProjectionTest, ConcatCharCharTest)
{
    auto concatExpr = GetConcatFuncExpr(CharType(20), CharType(30), CharType(51));
    std::vector<Expr *> exprs = { concatExpr };
    std::vector<DataType> vecOfTypes = { CharDataType(20), CharDataType(30) };
    DataTypes inputTypes(vecOfTypes);
    auto factory = new ProjectionOperatorFactory(exprs, exprs.size(), inputTypes, inputTypes.GetSize());

    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ConcatCharCharTest");
    auto input = CreateInputVecBatchForConcat(vecOfTypes, vecAllocator);

    auto op = factory->CreateOperator();
    op->AddInput(input);
    std::vector<VectorBatch *> ret;
    op->GetOutput(ret);

    std::vector<std::string> expectedDatas = { "NULL",
        "NULL",
        "NULL",
        "                                                  ",
        "                              John                ",
        "Rebecca                                           ",
        "Rebecca                       John                ",
        "Rebecca-Rebecca-Rebecca-Rebeca-John-John-John-John" };
    auto expect = CreateExpectVecBatchForConcat(CharDataType(51), vecAllocator, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(ret[0], expect));

    VectorHelper::FreeVecBatches(ret);
    VectorHelper::FreeVecBatch(expect);
    delete op;
    delete factory;
    delete vecAllocator;
}
}