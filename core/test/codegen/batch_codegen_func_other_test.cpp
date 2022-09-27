/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: funcExpr and other batch codegen test
 */
#include "gtest/gtest.h"

#include <string>
#include <vector>
#include "operator/filter/filter_and_project.h"
#include "operator/projection/projection.h"
#include "../util/test_util.h"
#include "util/config_util.h"
#include "codegen_util.h"

using namespace std;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace TestUtil;
using namespace CodegenUtil;


TEST(BatchCodeGenTest, CastDouble)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
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
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 1000;
    const int32_t numCols = 2;
    double *col1 = MakeDoubles(numRows);
    double *col2 = MakeDoubles(numRows);
    std::vector<DataTypePtr> vecOfTypes = { DoubleType(), DoubleType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastDouble");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    for (int i = 0; i < numRows; ++i) {
        int32_t val0 = ((IntVector *)ret->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, i);
        EXPECT_EQ(val1, i);
    }

    Expr::DeleteExprs(exprs);
    delete[] col1;
    delete[] col2;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, ProjectSparkConfig)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    std::string castStr = "CAST";
    std::vector<Expr *> argLeft { new FieldExpr(0, Decimal64Type(7, 0)) };
    FuncExpr *subLeft = GetFuncExpr(castStr, argLeft, Decimal64Type(8, 0));
    std::vector<Expr *> argRight { new FieldExpr(0, Decimal64Type(7, 0)) };
    FuncExpr *subRight = GetFuncExpr(castStr, argRight, Decimal64Type(8, 0));
    auto *addExprs = new BinaryExpr(omniruntime::expressions::Operator::ADD, subLeft, subRight, Decimal64Type(8, 0));

    std::vector<Expr *> exprs = { addExprs };
    Filter *filter = nullptr;

    auto overflowConfig = new OverflowConfig(omniruntime::op::OVERFLOW_CONFIG_NULL);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), overflowConfig);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 1;
    int64_t *col = new int64_t[1];
    col[0] = 123;
    const int32_t numCols = 1;
    std::vector<DataTypePtr> vecOfTypes = { LongType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastDoubleToString");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    int64_t val0 = ((LongVector *)ret->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 246);

    Expr::DeleteExprs(exprs);
    delete overflowConfig;
    delete[] col;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, CastDecimal128ToString)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    auto data = new FieldExpr(0, Decimal128Type(38, 0));
    std::string castStr = "CAST";
    std::vector<Expr *> args;
    args.push_back(data);
    auto castExpr = GetFuncExpr(castStr, args, VarcharType(5));

    std::vector<Expr *> exprs = { castExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 3;
    auto *col = MakeDecimals(numRows, 10);
    const int32_t numCols = 1;
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastDecimal128ToString");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    VarcharVector *vcVec = ((VarcharVector *)ret->GetVector(0));
    for (int32_t i = 0; i < numRows; i++) {
        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);
        string actualStr(reinterpret_cast<char *>(actualChar), len);
        EXPECT_EQ(actualStr, to_string(col[i * 2]));
    }

    Expr::DeleteExprs(exprs);
    delete[] col;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, AllType)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
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

    auto *projExpr1 = new FieldExpr(0, IntType());
    auto *projExpr2 = new FieldExpr(1, LongType());
    auto *projExpr3 = new FieldExpr(2, DoubleType());
    std::vector<Expr *> exprs = { projExpr1, projExpr2, projExpr3 };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

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
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_AllType");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, data1, data2, data3);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(numSelectedRows, 100);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] data1;
    delete[] data2;
    delete[] data3;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Round)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    std::string funcStr = "round";

    auto data0 = new FieldExpr(0, IntType());
    std::vector<Expr *> args0;
    args0.push_back(data0);
    int32_t data0Decimal = 1;
    args0.push_back(new LiteralExpr(data0Decimal, IntType()));
    auto roundExpr0 = GetFuncExpr(funcStr, args0, IntType());

    auto data1 = new FieldExpr(1, LongType());
    std::vector<Expr *> args1;
    args1.push_back(data1);
    int32_t data1Decimal = -1;
    args1.push_back(new LiteralExpr(data1Decimal, IntType()));
    auto roundExpr1 = GetFuncExpr(funcStr, args1, LongType());
    double data1Pow = pow(10, data1Decimal);

    auto data2 = new FieldExpr(2, DoubleType());
    std::vector<Expr *> args2;
    args2.push_back(data2);
    int32_t data2Decimal = 0;
    args2.push_back(new LiteralExpr(data2Decimal, IntType()));
    auto roundExpr2 = GetFuncExpr(funcStr, args2, DoubleType());

    auto data3 = new FieldExpr(3, DoubleType());
    std::vector<Expr *> args3;
    args3.push_back(data3);
    int32_t data3Decimal = 2;
    args3.push_back(new LiteralExpr(data3Decimal, IntType()));
    auto roundExpr3 = GetFuncExpr(funcStr, args3, DoubleType());
    double data3Pow = pow(10, data3Decimal);

    auto data4 = new FieldExpr(4, DoubleType());
    std::vector<Expr *> args4;
    args4.push_back(data4);
    int32_t data4Decimal = 2;
    args4.push_back(new LiteralExpr(data4Decimal, IntType()));
    auto roundExpr4 = GetFuncExpr(funcStr, args4, DoubleType());
    double data4Pow = pow(10, data4Decimal);

    auto data5 = new FieldExpr(5, DoubleType());
    std::vector<Expr *> args5;
    args5.push_back(data5);
    int32_t data5Decimal = 2;
    args5.push_back(new LiteralExpr(data5Decimal, IntType()));
    auto roundExpr5 = GetFuncExpr(funcStr, args5, DoubleType());

    std::vector<Expr *> exprs = { roundExpr0, roundExpr1, roundExpr2, roundExpr3, roundExpr4, roundExpr5 };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 1000;
    int32_t *col0 = MakeInts(numRows, -5);
    int64_t *col1 = MakeLongs(numRows, -5);
    double *col2 = MakeDoubles(numRows, -5.123);
    double *col3 = MakeDoubles(numRows, -5.123);
    double *col4 = MakeDoubles(numRows, -1001.456);
    auto *col5 = new double[numRows];
    double start = 1;
    int32_t idx = 0;
    for (double i = start; i < start + numRows; i++) {
        if (idx % 2 == 0) {
            col5[idx++] = std::sqrt(-i);
        } else {
            col5[idx++] = i / 0.0;
        }
    }
    const int32_t numCols = 6;

    std::vector<DataTypePtr> vecOfTypes = { IntType(),    LongType(),   DoubleType(),
        DoubleType(), DoubleType(), DoubleType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Round");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col0, col1, col2, col3, col4, col5);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int32_t i = 0; i < numRows; i++) {
        int32_t val0 = ((IntVector *)ret->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)ret->GetVector(1))->GetValue(i);
        double val2 = ((DoubleVector *)ret->GetVector(2))->GetValue(i);
        double val3 = ((DoubleVector *)ret->GetVector(3))->GetValue(i);
        double val4 = ((DoubleVector *)ret->GetVector(4))->GetValue(i);
        double val5 = ((DoubleVector *)ret->GetVector(5))->GetValue(i);

        int32_t old0 = ((IntVector *)vecBatch->GetVector(0))->GetValue(i);
        int64_t old1 = ((LongVector *)vecBatch->GetVector(1))->GetValue(i);
        double old2 = ((DoubleVector *)vecBatch->GetVector(2))->GetValue(i);
        double old3 = ((DoubleVector *)vecBatch->GetVector(3))->GetValue(i);
        double old4 = ((DoubleVector *)vecBatch->GetVector(4))->GetValue(i);
        double old5 = ((DoubleVector *)vecBatch->GetVector(5))->GetValue(i);

        EXPECT_EQ(val0, round(old0));
        EXPECT_EQ(val0, old0);
        EXPECT_EQ(val1, (round(old1 * data1Pow) / data1Pow));
        EXPECT_EQ(val2, round(old2));
        EXPECT_EQ(val3, (round(old3 * data3Pow) / data3Pow));
        EXPECT_EQ(val4, -(round(-old4 * data4Pow) / data4Pow));
        if (i % 2 == 0) {
            EXPECT_TRUE(isnan(val5) && isnan(old5));
        } else {
            EXPECT_EQ(val5, old5);
        }
    }

    Expr::DeleteExprs(exprs);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete[] col5;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, MultipleColumns)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    FieldExpr *subLeft = new FieldExpr(0, IntType());
    LiteralExpr *subRight = new LiteralExpr(10, IntType());
    BinaryExpr *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, IntType());

    FieldExpr *addLeft = new FieldExpr(2, LongType());
    LiteralExpr *addRight = new LiteralExpr(1L, LongType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, LongType());

    std::vector<Expr *> exprs = { subExpr, addExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 1000;
    int32_t *col1 = MakeInts(numRows);
    int32_t *col2 = MakeInts(numRows, -100);
    int64_t *col3 = MakeLongs(numRows, -10);
    const int32_t numCols = 3;
    std::vector<DataTypePtr> vecOfTypes = { IntType(), IntType(), LongType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_MultipleColumns");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    for (int32_t i = 0; i < numRows; i++) {
        int32_t val0 = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i - 10);
        int64_t val1 = ((LongVector *)ret->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, i - 9);
    }

    Expr::DeleteExprs(exprs);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, DictionaryVecDouble)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    LiteralExpr *addRight = new LiteralExpr(10.0, DoubleType());
    std::vector<Expr *> exprs = { new BinaryExpr(omniruntime::expressions::Operator::ADD,
        new FieldExpr(0, DoubleType()), addRight, DoubleType()) };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DictionaryVecDouble");
    DoubleVector *col1 = new DoubleVector(vecAllocator, numRows);
    int32_t ids1[] = { 3, 4, 5, 6, 7, 8, 9, 9, 9, 9 };
    DictionaryVector *doubleDicVector = new DictionaryVector(col1, ids1, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, (i % 21) - 3);
    }
    VectorBatch *vecBatch = new VectorBatch(numCols, numRows);
    vecBatch->SetVector(0, doubleDicVector);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int32_t i = 0; i < numRows; i++) {
        double val0 = ((DoubleVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, doubleDicVector->GetDouble(i) + 10);
    }

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, DictionaryVecDecimal128)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    LiteralExpr *addRight = new LiteralExpr(new std::string("20"), Decimal128Type(38, 0));
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD,
        new FieldExpr(0, Decimal128Type(38, 0)), addRight, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { addExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DictionaryVecDecimal128");
    Decimal128Vector *col1 = new Decimal128Vector(vecAllocator, numRows);
    int32_t ids1[] = { 3, 4, 5, 6, 7, 8, 9, 9, 9, 9 };
    DictionaryVector *decimal128DicVector = new DictionaryVector(col1, ids1, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 decimal128(0, i);
        col1->SetValue(i, decimal128);
    }
    VectorBatch *vecBatch = new VectorBatch(numCols, numRows);
    vecBatch->SetVector(0, decimal128DicVector);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), decimal128DicVector->GetDecimal128(i).HighBits());
        EXPECT_EQ(val0.LowBits(), decimal128DicVector->GetDecimal128(i).LowBits() + 20);
    }

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, DictionaryVecDecimal64)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    LiteralExpr *addRight = new LiteralExpr(20, Decimal64Type(8, 1));
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, new FieldExpr(0, Decimal64Type(8, 0)),
        addRight, Decimal64Type(8, 1));
    std::vector<Expr *> exprs = { addExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DictionaryVecDecimal128Test");
    LongVector *col1 = new LongVector(vecAllocator, numRows);
    int32_t ids1[] = { 3, 4, 5, 6, 7, 8, 9, 9, 9, 9 };
    DictionaryVector *longVector = new DictionaryVector(col1, ids1, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        int64_t decimal64 = i;
        col1->SetValue(i, decimal64);
    }
    VectorBatch *vecBatch = new VectorBatch(numCols, numRows);
    vecBatch->SetVector(0, longVector);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    for (int32_t i = 0; i < numRows; i++) {
        int64_t val0 = ((LongVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0 / 10, longVector->GetLong(i));
    }

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, DictionaryVecBoolean)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    LiteralExpr *andRight = new LiteralExpr(true, BooleanType());
    BinaryExpr *andExpr = new BinaryExpr(omniruntime::expressions::Operator::OR, new FieldExpr(0, BooleanType()),
        andRight, BooleanType());
    std::vector<Expr *> exprs = { andExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int32_t numRows = 6;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DictionaryVecBoolean");
    BooleanVector *col1 = new BooleanVector(vecAllocator, numRows);
    int32_t ids1[] = { 1, 3, 3, 4, 4, 4 };
    DictionaryVector *booleanVector = new DictionaryVector(col1, ids1, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            col1->SetValue(i, false);
        } else {
            col1->SetValue(i, true);
        }
    }
    VectorBatch *vecBatch = new VectorBatch(numCols, numRows);
    vecBatch->SetVector(0, booleanVector);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    for (int32_t i = 0; i < numRows; i++) {
        bool val0 = ((BooleanVector *)ret->GetVector(0))->GetValue(i);
        if (i >= 3) {
            EXPECT_FALSE(val0 && booleanVector->GetBoolean(i));
        } else {
            EXPECT_TRUE(val0 && booleanVector->GetBoolean(i));
        }
    }

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, DictionaryVarchar)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(0, IntType()),
        new LiteralExpr(6, IntType()), BooleanType());

    auto *projExpr = new FieldExpr(0, IntType());
    auto *projExpr1 = new FieldExpr(1, VarcharType());
    std::vector<Expr *> exprs = { projExpr, projExpr1 };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 2;
    const int32_t numRows = 3;
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_DictionaryVarchar");
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    VarcharVector *col2 = new VarcharVector(vecAllocator, 1024, numRows);
    int32_t ids[] = { 0, 1, 2 };
    DictionaryVector *dictionaryVector = new DictionaryVector(col2, ids, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i * 3);
        std::string tmp = "test";
        col2->SetValue(i, reinterpret_cast<const uint8_t *>(tmp.c_str()), tmp.length());
    }

    VectorBatch *vecBatch = new VectorBatch(numCols, numRows);
    vecBatch->SetVector(0, col1);
    vecBatch->SetVector(1, dictionaryVector);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    EXPECT_EQ(numSelectedRows, 2);

    for (int i = 0; i < numSelectedRows; i++) {
        int32_t val0 = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i));
        uint8_t *data = nullptr;
        int32_t len = ((VarcharVector *)ret->GetVector(1))->GetValue(i, &data);
        std::string result(data, data + len);
        data = nullptr;
        len = dictionaryVector->GetVarchar(i, &data);
        std::string expected(data, data + len);
        EXPECT_EQ(result, expected);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete col2;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, DictionaryVec)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    BetweenExpr *filterExpr =
        new BetweenExpr(new FieldExpr(1, IntType()), new FieldExpr(0, IntType()), new FieldExpr(2, IntType()));

    auto *projExpr = new FieldExpr(0, IntType());
    auto *projExpr1 = new FieldExpr(1, LongType());
    auto *projExpr2 = new FieldExpr(2, IntType());
    std::vector<Expr *> exprs = { projExpr, projExpr1, projExpr2 };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 3;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_DictionaryVec");
    IntVector *col1 = new IntVector(vecAllocator, numRows);
    IntVector *col2 = new IntVector(vecAllocator, numRows);
    IntVector *col3 = new IntVector(vecAllocator, numRows);
    int32_t ids[] = { 3, 4, 5, 6, 7, 8, 9, 9, 9, 9 };
    DictionaryVector *dictionaryVector = new DictionaryVector(col3, ids, numRows);

    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, i % 5);
        col2->SetValue(i, i % 11);
        col3->SetValue(i, (i % 21) - 3);
    }

    VectorBatch *vecBatch = new VectorBatch(numCols, numRows);
    vecBatch->SetVector(0, col1);
    vecBatch->SetVector(1, col2);
    vecBatch->SetVector(2, dictionaryVector);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    EXPECT_EQ(numSelectedRows, 7);

    for (int i = 0; i < numSelectedRows; i++) {
        int32_t val0 = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i));
        int32_t val1 = ((IntVector *)ret->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, col2->GetValue(i));
        int32_t val2 = ((IntVector *)ret->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, dictionaryVector->GetInt(i));
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete col3;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, AddDecimalReturnNull)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    FieldExpr *right1 = new FieldExpr(0, Decimal128Type(38, 0));
    FieldExpr *left1 = new FieldExpr(0, Decimal128Type(38, 0));
    BinaryExpr *addExpr1 =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, right1, left1, Decimal128Type(38, 0));
    FieldExpr *right2 = new FieldExpr(1, Decimal64Type(18, 0));
    FieldExpr *left2 = new FieldExpr(1, Decimal64Type(18, 0));
    BinaryExpr *addExpr2 =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, right2, left2, Decimal128Type(38, 0));
    FieldExpr *right3 = new FieldExpr(1, Decimal64Type(18, 0));
    FieldExpr *left3 = new FieldExpr(0, Decimal128Type(38, 0));
    BinaryExpr *addExpr3 =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, right3, left3, Decimal128Type(38, 0));
    FieldExpr *right4 = new FieldExpr(0, Decimal128Type(38, 0));
    FieldExpr *left4 = new FieldExpr(1, Decimal64Type(18, 0));
    BinaryExpr *addExpr4 =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, right4, left4, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { addExpr1, addExpr2, addExpr3, addExpr4 };
    Filter *filter = nullptr;

    auto overflowConfig = new OverflowConfig(omniruntime::op::OVERFLOW_CONFIG_NULL);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), overflowConfig);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 1;
    const int32_t numCols = 2;
    int64_t *col1 = new int64_t[2] { -1, INT64_MAX };
    int64_t *col2 = new int64_t[1] { INT64_MAX };
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type(), LongType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_AddDecimalReturnNull");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    for (int32_t i = 0; i < numRows; i++) {
        bool isNull = (ret->GetVector(0))->IsValueNull(i);
        EXPECT_TRUE(isNull);
        isNull = (ret->GetVector(1))->IsValueNull(i);
        EXPECT_FALSE(isNull);
        isNull = (ret->GetVector(2))->IsValueNull(i);
        EXPECT_TRUE(isNull);
        isNull = (ret->GetVector(3))->IsValueNull(i);
        EXPECT_TRUE(isNull);
    }

    Expr::DeleteExprs(exprs);
    delete overflowConfig;
    delete[] col1;
    delete[] col2;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, IsNullExpr)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    FieldExpr *expr = new FieldExpr(0, IntType());
    IsNullExpr *isNullExpr = new IsNullExpr(expr);

    std::vector<Expr *> exprs = { isNullExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 4;
    int32_t col1[numRows] = { 1, 2, 3, 4 };
    const int32_t numCols = 1;
    std::vector<DataTypePtr> vecOfTypes = { IntType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_IsNullExpr");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    for (int32_t i = 0; i < 4; i++) {
        vecBatch->GetVector(0)->SetValueNull(i);
    }

    int32_t numSelectedRows = numCols;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int32_t i = 0; i < numRows; i++) {
        bool isValNull = ((BooleanVector *)ret->GetVector(0))->IsValueNull(i);
        EXPECT_FALSE(isValNull);
    }
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, UnaryExpr)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    FieldExpr *expr = new FieldExpr(0, IntType());
    IsNullExpr *isNullExpr = new IsNullExpr(expr);
    auto notExpr = new UnaryExpr(omniruntime::expressions::Operator::NOT, isNullExpr, BooleanType());
    std::vector<Expr *> exprs = { notExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 4;
    int32_t col1[numRows] = { 1, 2, 3, 4 };
    const int32_t numCols = 1;
    std::vector<DataTypePtr> vecOfTypes = { IntType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_UnaryExpr");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    for (int32_t i = 0; i < 4; i++) {
        vecBatch->GetVector(0)->SetValueNull(i);
    }

    int32_t numSelectedRows = numCols;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    for (int32_t i = 0; i < numRows; i++) {
        bool val = ((BooleanVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_FALSE(val);
    }

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}
