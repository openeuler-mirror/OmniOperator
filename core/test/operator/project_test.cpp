/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */

#include <string>
#include <vector>
#include <chrono>
#include "gtest/gtest.h"
#include "operator/projection/projection.h"
#include "operator/filter/bloom_filter.h"
#include "util/test_util.h"
#include "util/config_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace std;
using namespace TestUtil;

namespace ProjectionTest {
TEST(ProjectionTest, Cast)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int64_t *col1 = MakeLongs(numRows);
    int32_t *col2 = MakeInts(numRows);
    std::vector<DataTypePtr> vecOfTypes = { LongType(), IntType() };
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
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Cast");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);

    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
        t->GetVector(1)->SetValueNotNull(i);
    }

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int32_t val0 = ((IntVector *)outputVecBatch->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)outputVecBatch->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, i);
        EXPECT_EQ(val1, i);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    delete[] col2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, CastDouble)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    double *col1 = MakeDoubles(numRows);
    double *col2 = MakeDoubles(numRows);
    std::vector<DataTypePtr> vecOfTypes = { DoubleType(), DoubleType() };

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
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastDouble");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);

    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
        t->GetVector(1)->SetValueNotNull(i);
    }

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int32_t val0 = ((IntVector *)outputVecBatch->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)outputVecBatch->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, i);
        EXPECT_EQ(val1, i);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    delete[] col2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, CastInt64ToDecimal128)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int64_t *col1 = MakeLongs(numRows);
    std::vector<DataTypePtr> vecOfTypes = { LongType() };
    auto data1 = new FieldExpr(0, LongType());
    std::string castStr = "CAST";
    std::vector<Expr *> args1;
    args1.push_back(data1);
    auto castExpr = GetFuncExpr(castStr, args1, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { castExpr };

    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastInt64ToDecimal128");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);

    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
    }

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, MakeDecimal64ToDiffScale)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int64_t *col1 = MakeLongs(numRows);
    std::vector<DataTypePtr> vecOfTypes = { Decimal64Type(7, 2) };
    auto data1 = new FieldExpr(0, Decimal64Type(7, 2));
    auto data2 = new FieldExpr(0, Decimal64Type(7, 2));

    std::string castStr = "CAST";
    auto makeExpr1 = GetFuncExpr(castStr, { data1 }, Decimal64Type(7, 4));
    auto makeExpr2 = GetFuncExpr(castStr, { data2 }, Decimal64Type(7, 0));
    std::vector<Expr *> exprs = { makeExpr1, makeExpr2 };

    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("MakeDecimal64ToDiffScale");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);
    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
    }

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        long val0 = ((LongVector *)outputVecBatch->GetVector(0))->GetValue(i);
        long val1 = ((LongVector *)outputVecBatch->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0, i * 100);
        if (i % 100 >= 50) {
            i = i + 50;
        }
        EXPECT_EQ(val1, i / 100);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, MakeDecimal128ToDiffScale)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int64_t *col1 = MakeDecimals(numRows);
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type(38, 2) };
    auto data1 = new FieldExpr(0, Decimal128Type(38, 2));
    auto data2 = new FieldExpr(0, Decimal128Type(7, 2));

    std::string castStr = "CAST";
    auto makeExpr1 = GetFuncExpr(castStr, { data1 }, Decimal128Type(38, 4));
    auto makeExpr2 = GetFuncExpr(castStr, { data2 }, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { makeExpr1, makeExpr2 };

    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Decimal128ToDiffScale");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);
    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
    }

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(i);
        Decimal128 val1 = ((Decimal128Vector *)outputVecBatch->GetVector(1))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val1.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i * 100);
        EXPECT_EQ(val1.LowBits(), round((double)i / 100));
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, MakeDecimal64To128WithDiffScale)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int64_t *col1 = MakeLongs(numRows);
    std::vector<DataTypePtr> vecOfTypes = { Decimal64Type(7, 2) };
    auto data1 = new FieldExpr(0, Decimal64Type(7, 2));
    auto data2 = new FieldExpr(0, Decimal64Type(7, 2));
    auto data3 = new FieldExpr(0, Decimal64Type(7, 2));
    std::string castStr = "CAST";
    auto makeExpr1 = GetFuncExpr(castStr, { data1 }, Decimal128Type(38, 2));
    auto makeExpr2 = GetFuncExpr(castStr, { data2 }, Decimal128Type(38, 4));
    auto makeExpr3 = GetFuncExpr(castStr, { data3 }, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { makeExpr1, makeExpr2, makeExpr3 };

    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("MakeDecimal64To128WithDiffScale");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);
    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
    }

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(i);
        Decimal128 val1 = ((Decimal128Vector *)outputVecBatch->GetVector(1))->GetValue(i);
        Decimal128 val2 = ((Decimal128Vector *)outputVecBatch->GetVector(2))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val1.HighBits(), 0);
        EXPECT_EQ(val2.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i);
        EXPECT_EQ(val1.LowBits(), i * 100);
        EXPECT_EQ(val2.LowBits(), round((double)i / 100));
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Simple)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int32_t *col = MakeInts(numRows);
    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(5, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());
    std::vector<Expr *> exprs = { addExpr };
    std::vector<DataTypePtr> vecOfTypes = { IntType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Simple");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col);

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            t->GetVector(0)->SetValueNotNull(i);
        } else {
            t->GetVector(0)->SetValueNull(i);
        }
    }

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int32_t val = ((IntVector *)outputVecBatch->GetVector(0))->GetValue(i);
        bool isNull = ((IntVector *)outputVecBatch->GetVector(0))->IsValueNull(i);
        if (i % 2 == 0) {
            EXPECT_EQ(val, i + 5);
            EXPECT_FALSE(isNull);
        } else {
            EXPECT_TRUE(isNull);
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, AbsWithNullValues)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int32_t *col1 = MakeInts(numRows, -5);
    int64_t *col2 = MakeLongs(numRows, -5);
    int64_t *col3 = MakeLongs(numRows, -5);
    std::vector<DataTypePtr> vecOfTypes = { IntType(), LongType(), Decimal64Type() };
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
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_AbsWithNullValues");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);
    for (int i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            t->GetVector(0)->SetValueNull(i);
            t->GetVector(1)->SetValueNull(i);
            t->GetVector(2)->SetValueNull(i);
        }
    }

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            EXPECT_TRUE(outputVecBatch->GetVector(0)->IsValueNull(i));
            EXPECT_TRUE(outputVecBatch->GetVector(1)->IsValueNull(i));
            EXPECT_TRUE(outputVecBatch->GetVector(2)->IsValueNull(i));
        } else {
            EXPECT_FALSE(outputVecBatch->GetVector(0)->IsValueNull(i));
            EXPECT_FALSE(outputVecBatch->GetVector(1)->IsValueNull(i));
            EXPECT_FALSE(outputVecBatch->GetVector(2)->IsValueNull(i));
            int32_t val0 = ((IntVector *)outputVecBatch->GetVector(0))->GetValue(i);
            int64_t val1 = ((LongVector *)outputVecBatch->GetVector(1))->GetValue(i);
            int64_t val2 = ((LongVector *)outputVecBatch->GetVector(2))->GetValue(i);
            EXPECT_EQ(val0, abs(i - 5));
            EXPECT_EQ(val1, abs(i - 5));
            EXPECT_EQ(val2, abs(i - 5));
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Negatives)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int32_t *col = MakeInts(numRows, -5);
    FieldExpr *subLeft = new FieldExpr(0, IntType());
    LiteralExpr *subRight = new LiteralExpr(500, IntType());
    BinaryExpr *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, IntType());
    std::vector<Expr *> exprs = { subExpr };
    std::vector<DataTypePtr> vecOfTypes = { IntType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Negatives");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int32_t val0 = ((IntVector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i - 505);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Longs)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 10000;
    int64_t *col = MakeLongs(numRows, -5000);
    FieldExpr *mulLeft = new FieldExpr(0, LongType());
    LiteralExpr *mulRight = new LiteralExpr(5000000L, LongType());
    BinaryExpr *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, LongType());
    std::vector<Expr *> exprs = { mulExpr };
    std::vector<DataTypePtr> vecOfTypes = { LongType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Longs");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int64_t val0 = ((LongVector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, static_cast<int64_t>(i - 5000) * 5000000);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Doubles)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 10000;
    double *col = MakeDoubles(numRows, -5000.5);

    FieldExpr *divLeft = new FieldExpr(0, DoubleType());
    LiteralExpr *divRight = new LiteralExpr(2.0, DoubleType());
    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, divLeft, divRight, DoubleType());
    std::vector<Expr *> exprs = { divExpr };
    std::vector<DataTypePtr> vecOfTypes = { DoubleType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Doubles");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        double val0 = ((DoubleVector *)outputVecBatch->GetVector(0))->GetValue(i);
        double expected = (i - 5000.5) / 2;
        EXPECT_TRUE(val0 > expected - 0.1 && val0 < expected + 0.1);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Doubles_DivideByZero)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    int32_t numRows = 5;
    double *col1 = MakeDoubles(numRows, -4.0);
    double *col2 = MakeDoubles(numRows, -3.0);

    FieldExpr *divLeft = new FieldExpr(0, DoubleType());
    FieldExpr *divRight = new FieldExpr(1, DoubleType());
    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, divLeft, divRight, DoubleType());
    std::vector<Expr *> exprs = { divExpr };
    std::vector<DataTypePtr> vecOfTypes = { DoubleType(), DoubleType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Doubles");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    EXPECT_TRUE(isinf(((DoubleVector *)outputVecBatch->GetVector(0))->GetValue(3)));

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testModDoubles)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 10000;
    double *col1 = MakeDoubles(numRows, -5000.5);
    double *col2 = MakeDoubles(numRows, -124.45);

    FieldExpr *modLeft = new FieldExpr(0, DoubleType());
    FieldExpr *modRight = new FieldExpr(1, DoubleType());
    BinaryExpr *modExpr = new BinaryExpr(omniruntime::expressions::Operator::MOD, modLeft, modRight, DoubleType());
    std::vector<Expr *> exprs = { modExpr };
    std::vector<DataTypePtr> vecOfTypes = { DoubleType(), DoubleType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_testModDoubles");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);
    t->GetVector(0)->SetValueNull(5);
    t->GetVector(0)->SetValueNull(8000);
    t->GetVector(1)->SetValueNull(2456);
    t->GetVector(1)->SetValueNull(8000);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        if (i == 5 || i == 2456 || i == 8000) {
            EXPECT_TRUE(outputVecBatch->GetVector(0)->IsValueNull(i));
        } else {
            double val0 = ((DoubleVector *)outputVecBatch->GetVector(0))->GetValue(i);
            double expected = std::fmod((i - 5000.5), (i - 124.45));
            EXPECT_DOUBLE_EQ(val0, expected);
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    delete[] col2;
    delete op;
    delete factory;
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testModDoubles2)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 10000;
    double *col = MakeDoubles(numRows, -1273.37);

    FieldExpr *modLeft = new FieldExpr(0, DoubleType());
    LiteralExpr *modRight = new LiteralExpr(-45.8, DoubleType());
    BinaryExpr *modExpr = new BinaryExpr(omniruntime::expressions::Operator::MOD, modLeft, modRight, DoubleType());
    std::vector<Expr *> exprs = { modExpr };
    std::vector<DataTypePtr> vecOfTypes = { DoubleType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_testModDoubles2");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col);
    t->GetVector(0)->SetValueNull(0);
    t->GetVector(0)->SetValueNull(9999);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        if (i == 0 || i == 9999) {
            EXPECT_TRUE(outputVecBatch->GetVector(0)->IsValueNull(i));
        } else {
            double val0 = ((DoubleVector *)outputVecBatch->GetVector(0))->GetValue(i);
            double expected = std::fmod((i - 1273.37), -45.8);
            EXPECT_DOUBLE_EQ(val0, expected);
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col;
    delete op;
    delete factory;
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, DoublesModulusByZero)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    int32_t numRows = 10;
    double *col1 = MakeDoubles(numRows, -4.0);

    FieldExpr *divLeft = new FieldExpr(0, DoubleType());
    LiteralExpr *divRight = new LiteralExpr(0, DoubleType());
    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::MOD, divLeft, divRight, DoubleType());
    std::vector<Expr *> exprs = { divExpr };
    std::vector<DataTypePtr> vecOfTypes = { DoubleType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Doubles");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        EXPECT_TRUE(-isnan(((DoubleVector *)outputVecBatch->GetVector(0))->GetValue(i)));
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    delete op;
    delete factory;
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, MultipleColumns)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int32_t *col1 = MakeInts(numRows);
    int32_t *col2 = MakeInts(numRows, -100);
    int64_t *col3 = MakeLongs(numRows, -10);
    FieldExpr *subLeft = new FieldExpr(0, IntType());
    LiteralExpr *subRight = new LiteralExpr(10, IntType());
    BinaryExpr *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, IntType());

    FieldExpr *addLeft = new FieldExpr(2, LongType());
    LiteralExpr *addRight = new LiteralExpr(1L, LongType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, LongType());

    std::vector<Expr *> exprs = { subExpr, addExpr };

    std::vector<DataTypePtr> vecOfTypes = { IntType(), IntType(), LongType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_MultipleColumns");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int32_t val0 = ((IntVector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i - 10);
        int64_t val1 = ((LongVector *)outputVecBatch->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, i - 9);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, BenchmarkMultipleColumns)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int32_t *col1 = MakeInts(numRows);
    int32_t *col2 = MakeInts(numRows, -100);
    int64_t *col3 = MakeLongs(numRows, -10);
    string *col4 = new string[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            col4[i] = "hello";
        } else if (i % 3 == 0) {
            col4[i] = "world";
        } else {
            col4[i] = "!!!!!";
        }
    }

    FieldExpr *subLeft = new FieldExpr(0, IntType());
    LiteralExpr *subRight = new LiteralExpr(10, IntType());
    BinaryExpr *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, IntType());

    FieldExpr *addLeft = new FieldExpr(2, LongType());
    LiteralExpr *addRight = new LiteralExpr(1L, LongType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, LongType());

    std::vector<Expr *> exprs = { subExpr, addExpr };
    std::vector<DataTypePtr> vecOfTypes = { IntType(), IntType(), LongType(), VarcharType(1000) };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_BenchmarkMultipleColumns");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3, col4);

    std::cout << "[BenchmarkMultipleColumns Project without varchar]\n\n";
    for (int i = 0; i < 10; i++) {
        auto start = std::chrono::system_clock::now();
        auto copy = DuplicateVectorBatch(t);
        op->AddInput(copy);
        VectorBatch *outputVecBatch = nullptr;
        op->GetOutput(&outputVecBatch);
        VectorHelper::FreeVecBatch(outputVecBatch);

        auto end = std::chrono::system_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cout << "BenchmarkMultipleColumns round " << i << " elapsed " << elapsed.count() << " ms\n";
    }

    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);

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

    auto exprEvaluator2 = std::make_shared<ExpressionEvaluator>(exprs2, inputTypes, overflowConfig);
    factory = new ProjectionOperatorFactory(move(exprEvaluator2));
    op = factory->CreateOperator();

    for (int i = 0; i < 10; i++) {
        auto start = std::chrono::system_clock::now();
        auto copy = DuplicateVectorBatch(t);
        op->AddInput(copy);
        VectorBatch *outputVecBatch = nullptr;
        op->GetOutput(&outputVecBatch);
        VectorHelper::FreeVecBatch(outputVecBatch);

        auto end = std::chrono::system_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cout << "BenchmarkMultipleColumns round " << i << " elapsed " << elapsed.count() << " ms\n";
    }

    VectorHelper::FreeVecBatch(t);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, DependOtherColumn)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int32_t *col1 = MakeInts(numRows);
    int32_t *col2 = MakeInts(numRows, -100);
    int64_t *col3 = MakeLongs(numRows, 0);
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
    std::vector<DataTypePtr> vecOfTypes = { IntType(), IntType(), LongType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DependOtherColumn");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int32_t val0 = ((IntVector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i * (i - 100));
        int64_t val1 = ((LongVector *)outputVecBatch->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, (int64_t)(i < 500 ? 4000000000 : i));
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ProjectString1)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    vector<string> strings;
    std::vector<DataTypePtr> vecOfTypes = { VarcharType() };
    DataTypes inputTypes(vecOfTypes);
    const int32_t numRows = 100;
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

    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ProjectString1");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *t = CreateVectorBatch(numRows, cols);

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
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    VarcharVector *vcVec = ((VarcharVector *)outputVecBatch->GetVector(0));
    uint8_t *actualChar = nullptr;
    int len;
    for (int32_t i = 0; i < numRows; i++) {
        len = vcVec->GetValue(i, &actualChar);
        // Truncate the resulting string
        void *charArr = &actualChar;
        auto charArrCasted = static_cast<char **>(charArr);
        string actualStr(*charArrCasted, len);
        if (i % 40 == 0) {
            EXPECT_STREQ(actualStr.c_str(), "hel");
        } else {
            EXPECT_STREQ(actualStr.c_str(), "abc");
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, DictionaryVecTest)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
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

    VectorBatch *t = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {1, 1, 1};
    vector<DataTypePtr> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes dataTypes(inputTypes);
    t->SetVector(0, col1);
    t->SetVector(1, col2);
    t->SetVector(2, dictionaryVector);

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
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, dataTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int32_t val0 = ((IntVector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i) + 1);
        int32_t val1 = ((IntVector *)outputVecBatch->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, col2->GetValue(i) + 2);
        int32_t val2 = ((IntVector *)outputVecBatch->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, dictionaryVector->GetInt(i) + 10);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, DictionaryVecDoubleTest)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numCols = 1;
    const int32_t numRows = 10;
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DictionaryVecDoubleTest");
    DoubleVector *col1 = new DoubleVector(vecAllocator, numRows);
    int32_t ids1[] = {3, 4, 5, 6, 7, 8, 9, 9, 9, 9};
    DictionaryVector *doubleDicVector = new DictionaryVector(col1, ids1, numRows);
    for (int32_t i = 0; i < numRows; i++) {
        col1->SetValue(i, (i % 21) - 3);
    }
    VectorBatch *t = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {3};
    vector<DataTypePtr> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes dataTypes(inputTypes);
    t->SetVector(0, doubleDicVector);

    LiteralExpr *addRight = new LiteralExpr(10.0, DoubleType());
    std::vector<Expr *> exprs = { new BinaryExpr(omniruntime::expressions::Operator::ADD,
        new FieldExpr(0, DoubleType()), addRight, DoubleType()) };
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, dataTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        double val0 = ((DoubleVector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, doubleDicVector->GetDouble(i) + 10);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, DictionaryVecVarcharTest)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
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

    VectorBatch *t = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {15};
    vector<DataTypePtr> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes dataTypes(inputTypes);
    t->SetVector(0, varCharDicVector);

    std::string funcStr = "substr";
    DataTypePtr retType = VarcharType();

    vector<Expr *> args;
    args.push_back(new FieldExpr(0, VarcharType()));
    args.push_back(new LiteralExpr(1, IntType()));
    args.push_back(new LiteralExpr(3, IntType()));
    auto substrExpr = GetFuncExpr(funcStr, args, VarcharType());
    std::vector<Expr *> exprs = { substrExpr };
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, dataTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        VarcharVector *vcVec = ((VarcharVector *)outputVecBatch->GetVector(0));
        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);
        std::string actualStr(actualChar, actualChar + len);

        if (i % 2 == 0) {
            EXPECT_EQ(actualStr, "hel");
        } else {
            EXPECT_EQ(actualStr, "wor");
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, DictionaryVecDecimal128Test)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
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
    VectorBatch *t = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {7};
    vector<DataTypePtr> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes dataTypes(inputTypes);
    t->SetVector(0, decimal128DicVector);

    LiteralExpr *addRight = new LiteralExpr(new std::string("20"), Decimal128Type(38, 0));
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD,
        new FieldExpr(0, Decimal128Type(38, 0)), addRight, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { addExpr };
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, dataTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), decimal128DicVector->GetDecimal128(i).HighBits());
        EXPECT_EQ(val0.LowBits(), decimal128DicVector->GetDecimal128(i).LowBits() + 20);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, DictionaryVecNestedTest)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
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
    VectorBatch *t = new VectorBatch(numCols, numRows);
    int32_t inputTypeIds[numCols] = {1, 1, 1};
    vector<DataTypePtr> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes dataTypes(inputTypes);
    t->SetVector(0, col1);
    t->SetVector(1, col2);
    t->SetVector(2, dictionaryNested);

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
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, dataTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int32_t val0 = ((IntVector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col1->GetValue(i) + 1);
        int32_t val1 = ((IntVector *)outputVecBatch->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, col2->GetValue(i) + 2);
        int32_t val2 = ((IntVector *)outputVecBatch->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, dictionaryNested->GetInt(i) + 10);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete col3;
    delete dictionaryVector;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Decimal128Arithmetic)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 10;
    int64_t *col1 = MakeDecimals(numRows);
    FieldExpr *addLeft = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *addRight = new LiteralExpr(new std::string("20"), Decimal128Type(38, 0));
    BinaryExpr *addExpr =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, Decimal128Type(38, 0));

    std::vector<Expr *> exprs = { addExpr };
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Decimal128Arithmetic");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int64_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i + 20);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Decimal128Arithmetic2)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 10;
    int64_t *col0 = MakeDecimals(numRows, -5);
    int64_t *col1 = MakeDecimals(numRows, -7);

    FieldExpr *subLeft0 = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *subRight0 = new LiteralExpr(new string("1"), Decimal128Type(38, 0));
    BinaryExpr *subExpr0 =
        new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft0, subRight0, Decimal128Type(38, 0));

    FieldExpr *subLeft1 = new FieldExpr(1, Decimal128Type(38, 0));
    LiteralExpr *subRight1 = new LiteralExpr(-1L, Decimal64Type(10, 0));
    BinaryExpr *subExpr1 =
        new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft1, subRight1, Decimal128Type(38, 0));

    std::vector<Expr *> exprs = { subExpr0, subExpr1 };
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type(), Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DISABLED_Decimal128Arithmetic2");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col0, col1);
    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(i);
        Decimal128 val1 = ((Decimal128Vector *)outputVecBatch->GetVector(1))->GetValue(i);
        Decimal128 old0 = ((Decimal128Vector *)t->GetVector(0))->GetValue(i);
        Decimal128 old1 = ((Decimal128Vector *)t->GetVector(1))->GetValue(i);
        if (i <= 5) {
            EXPECT_EQ(val0.HighBits(), 1LL << 63);
            EXPECT_EQ(val0.LowBits(), old0.LowBits() + 1);
        } else {
            EXPECT_EQ(val0.HighBits(), 0);
            EXPECT_EQ(val0.LowBits(), old0.LowBits() - 1);
        }
        if (i <= 5) {
            EXPECT_EQ(val1.HighBits(), 1LL << 63);
            EXPECT_EQ(val1.LowBits(), old1.LowBits() - 1);
        } else if (i == 6) {
            EXPECT_EQ(val1.HighBits(), 0);
            EXPECT_EQ(val1.LowBits(), old1.LowBits() - 1);
        } else {
            EXPECT_EQ(val1.HighBits(), 0);
            EXPECT_EQ(val1.LowBits(), old1.LowBits() + 1);
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(t);
    delete[] col0;
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Decimal128Arithmetic3)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 10;
    int64_t *col0 = MakeDecimals(numRows, -5);
    int64_t *col1 = MakeDecimals(numRows, -7);

    FieldExpr *addLeft0 = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *addRight0 = new LiteralExpr(new string("-1"), Decimal128Type(38, 0));
    BinaryExpr *addExpr0 =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft0, addRight0, Decimal128Type(38, 0));

    FieldExpr *addLeft1 = new FieldExpr(1, Decimal128Type(38, 0));
    LiteralExpr *addRight1 = new LiteralExpr(1L, Decimal64Type(10, 0));
    BinaryExpr *addExpr1 =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft1, addRight1, Decimal128Type(38, 0));

    std::vector<Expr *> exprs = { addExpr0, addExpr1 };
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type(), Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DISABLED_Decimal128Arithmetic3");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col0, col1);

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(i);
        Decimal128 val1 = ((Decimal128Vector *)outputVecBatch->GetVector(1))->GetValue(i);
        Decimal128 old0 = ((Decimal128Vector *)t->GetVector(0))->GetValue(i);
        Decimal128 old1 = ((Decimal128Vector *)t->GetVector(1))->GetValue(i);

        if (i <= 5) {
            EXPECT_EQ(val0.HighBits(), 1LL << 63);
            EXPECT_EQ(val0.LowBits(), old0.LowBits() + 1);
        } else {
            EXPECT_EQ(val0.HighBits(), 0);
            EXPECT_EQ(val0.LowBits(), old0.LowBits() - 1);
        }

        if (i <= 5) {
            EXPECT_EQ(val1.HighBits(), 1LL << 63);
            EXPECT_EQ(val1.LowBits(), old1.LowBits() - 1);
        } else if (i == 6) {
            EXPECT_EQ(val1.HighBits(), 0);
            EXPECT_EQ(val1.LowBits(), old1.LowBits() - 1);
        } else {
            EXPECT_EQ(val1.HighBits(), 0);
            EXPECT_EQ(val1.LowBits(), old1.LowBits() + 1);
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(t);
    delete[] col0;
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Decimal128Multiply)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 10;
    int64_t *col1 = MakeDecimals(numRows);
    FieldExpr *mulLeft = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *mulRight = new LiteralExpr(new std::string("3"), Decimal128Type(38, 0));
    BinaryExpr *mulExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, Decimal128Type(38, 0));

    std::vector<Expr *> exprs = { mulExpr };
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Decimal128Multiply");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int64_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i * 3);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Decimal128Divide)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 10;
    int64_t *col1 = MakeDecimals(numRows);
    LiteralExpr *divRight = new LiteralExpr(new std::string("20"), Decimal128Type(38, 0));
    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV,
        new FieldExpr(0, Decimal128Type(38, 0)), divRight, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { divExpr };

    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Decimal128Divide");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int64_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i / 20);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, MultipleDecimal128Columns)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 100;
    int64_t *col1 = MakeDecimals(numRows);
    int64_t *col2 = MakeDecimals(numRows, 100);
    FieldExpr *addLeft = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *addRight = new LiteralExpr(new std::string("50"), Decimal128Type(38, 0));
    BinaryExpr *addExpr =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, Decimal128Type(38, 0));

    FieldExpr *mulLeft = new FieldExpr(1, Decimal128Type(38, 0));
    LiteralExpr *mulRight = new LiteralExpr(new std::string("20"), Decimal128Type(38, 0));
    BinaryExpr *mulExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, Decimal128Type(38, 0));

    std::vector<Expr *> exprs = { addExpr, mulExpr };

    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type(), Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_MultipleDecimal128Columns");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i + 50);
    }
    int idx = 0;
    for (int32_t i = 100; i < 100 + 100; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(1))->GetValue(idx);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i * 20);
        idx++;
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    delete[] col2;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, StringSubstr)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    vector<string> strings;

    std::vector<DataTypePtr> vecOfTypes = { VarcharType() };
    DataTypes inputTypes(vecOfTypes);

    const int32_t numRows = 100;
    int64_t *col1 = new int64_t[numRows];

    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            strings.emplace_back("helloasdf");
        } else {
            strings.emplace_back("Bonjour");
        }
    }
    vector<bool> nulls;
    for (int32_t i = 0; i < numRows; i++) {
        nulls.emplace_back(false);
    }

    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_StringSubstr");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *t = CreateVectorBatch(numRows, cols);

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
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    string expected1 = "hello world";
    string expected2 = "Bonjo world";
    VarcharVector *vcVec = ((VarcharVector *)outputVecBatch->GetVector(0));
    uint8_t *actualChar = nullptr;
    for (int32_t i = 0; i < numRows; i++) {
        int len = vcVec->GetValue(i, &actualChar);
        string actualStr(reinterpret_cast<char *>(actualChar), 0, len);
        if (i % 2 == 0) {
            EXPECT_EQ(actualStr, expected1);
        } else {
            EXPECT_EQ(actualStr, expected2);
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, SlicedDictionaryVecTest)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
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

    VectorBatch *t = new VectorBatch(numCols, slicedCol1->GetSize());
    int32_t inputTypeIds[numCols] = {1, 1, 1};
    vector<DataTypePtr> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);
    DataTypes inputDataTypes(inputTypes);

    t->SetVector(0, slicedCol1);
    t->SetVector(1, slicedCol2);
    t->SetVector(2, slicedCol3);

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
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputDataTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    int numReturned = op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numReturned; i++) {
        int32_t val0 = ((IntVector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, slicedCol1->GetValue(i) + 1);
        int32_t val1 = ((IntVector *)outputVecBatch->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, slicedCol2->GetValue(i) + 2);
        int32_t val2 = ((IntVector *)outputVecBatch->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, slicedCol3->GetInt(i) + 10);
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, SlicedDictionaryVecWithNullTest)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
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

    VectorBatch *t = new VectorBatch(numCols, slicedCol1->GetSize());
    int32_t inputTypeIds[numCols] = {1};
    vector<DataTypePtr> inputTypes;
    ToVectorTypes(inputTypeIds, numCols, inputTypes);

    t->SetVector(0, slicedCol1);
    DataTypes inputVecTypes(inputTypes);

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    FieldExpr *addRight = new FieldExpr(0, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());
    std::vector<Expr *> exprs = { addExpr };
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputVecTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    int numReturned = op->GetOutput(&outputVecBatch);
    auto retVec = (IntVector *)(outputVecBatch->GetVector(0));
    for (int32_t i = 0; i < numReturned; i++) {
        if (i == 0) {
            EXPECT_TRUE(retVec->IsValueNull(i));
        } else {
            int64_t val0 = retVec->GetValue(i);
            EXPECT_EQ(val0, ((DictionaryVector *)slicedCol1)->GetInt(i) * 2);
        }
    }

    VectorHelper::FreeVecBatch(t);
    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Tpcds96)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
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

    FieldExpr *addCol0 = new FieldExpr(0, LongType());
    FieldExpr *addCol1 = new FieldExpr(1, LongType());
    FieldExpr *addCol2_0 = new FieldExpr(2, LongType());
    FieldExpr *addCol2_1 = new FieldExpr(2, LongType());
    BinaryExpr *addExpr1 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addCol0, addCol1, LongType());
    BinaryExpr *addExpr2 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addExpr1, addCol2_0, LongType());
    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, addCol2_1, addExpr2, LongType());
    FieldExpr *expectRes = new FieldExpr(3, LongType());
    std::vector<Expr *> exprs = { divExpr, expectRes };

    std::vector<DataTypePtr> vecOfTypes = { LongType(), LongType(), LongType(), LongType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Tpcds96");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col0, col1, col2, col3);

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
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int64_t val0 = ((LongVector *)outputVecBatch->GetVector(0))->GetValue(i);
        bool b0 = ((LongVector *)outputVecBatch->GetVector(0))->IsValueNull(i);
        int64_t val1 = ((LongVector *)outputVecBatch->GetVector(1))->GetValue(i);
        bool b1 = ((LongVector *)outputVecBatch->GetVector(1))->IsValueNull(i);
        EXPECT_EQ(b0, b1);
        if (!b0) {
            EXPECT_EQ(val0, val1);
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Round)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
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
    std::vector<DataTypePtr> vecOfTypes = { IntType(),    LongType(),   DoubleType(),
        DoubleType(), DoubleType(), DoubleType() };
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
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Round");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col0, col1, col2, col3, col4, col5);

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int32_t val0 = ((IntVector *)outputVecBatch->GetVector(0))->GetValue(i);
        int64_t val1 = ((LongVector *)outputVecBatch->GetVector(1))->GetValue(i);
        double val2 = ((DoubleVector *)outputVecBatch->GetVector(2))->GetValue(i);
        double val3 = ((DoubleVector *)outputVecBatch->GetVector(3))->GetValue(i);
        double val4 = ((DoubleVector *)outputVecBatch->GetVector(4))->GetValue(i);
        double val5 = ((DoubleVector *)outputVecBatch->GetVector(5))->GetValue(i);

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
    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col0;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete[] col5;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ConcatStrAndChar)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    std::vector<DataTypePtr> vecOfTypes = { CharType() };
    DataTypes inputTypes(vecOfTypes);
    const int32_t numRows = 1;
    vector<string> strings;
    for (int32_t i = 0; i < numRows; i++) {
        strings.emplace_back("AAAA");
    }
    vector<bool> nulls;
    for (int32_t i = 0; i < numRows; i++) {
        nulls.emplace_back(false);
    }

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ConcatStrAndChar");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *t = CreateVectorBatch(numRows, cols);

    std::vector<Expr *> concatArgs1, concatArgs2;
    std::string concatStr = "concat";
    concatArgs1.push_back(new LiteralExpr(new std::string("store"), VarcharType()));
    concatArgs1.push_back(new FieldExpr(0, CharType(16)));
    auto concatStrChar = GetFuncExpr(concatStr, concatArgs1, CharType(21));

    concatArgs2.push_back(new FieldExpr(0, CharType(16)));
    concatArgs2.push_back(new LiteralExpr(new std::string("store"), VarcharType()));
    auto concatCharStr = GetFuncExpr(concatStr, concatArgs2, CharType(21));
    std::vector<Expr *> exprs = { concatStrChar, concatCharStr };
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    string expected1 = "storeAAAA";
    string expected2 = "AAAA            store";
    for (int32_t i = 0; i < numRows; i++) {
        VarcharVector *vcVec1 = ((VarcharVector *)outputVecBatch->GetVector(0));
        uint8_t *actualChar1 = nullptr;
        int len1 = vcVec1->GetValue(i, &actualChar1);
        string actualStr1(reinterpret_cast<char *>(actualChar1), len1);
        EXPECT_EQ(actualStr1, expected1);

        VarcharVector *vcVec2 = ((VarcharVector *)outputVecBatch->GetVector(1));
        uint8_t *actualChar2 = nullptr;
        int len2 = vcVec2->GetValue(i, &actualChar2);
        string actualStr2(reinterpret_cast<char *>(actualChar2), len2);
        EXPECT_EQ(actualStr2, expected2);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, varcharExpand)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    std::vector<DataTypePtr> vecOfTypes = { VarcharType() };
    DataTypes inputTypes(vecOfTypes);
    const int32_t numRows = 100;
    vector<string> strings;
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            strings.emplace_back("helloasdf");
        } else {
            strings.emplace_back("Bonjour");
        }
    }
    vector<bool> nulls;
    for (int32_t i = 0; i < numRows; i++) {
        nulls.emplace_back(false);
    }

    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_varcharExpand");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *t = CreateVectorBatch(numRows, cols);

    FieldExpr *substrData = new FieldExpr(0, VarcharType());
    LiteralExpr *substrIndex = new LiteralExpr(1, IntType());
    LiteralExpr *substrLen = new LiteralExpr(5, IntType());

    std::string substrStr = "substr";
    DataTypePtr retType = VarcharType();
    std::vector<Expr *> args{ substrData, substrIndex, substrLen };
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
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    EXPECT_GT(outputVecBatch->GetVector(0)->GetCapacityInBytes(), avgStrLen * numRows);
    string expected1 = "hello" + baseStr;
    string expected2 = "Bonjo" + baseStr;
    for (int32_t i = 0; i < numRows; i++) {
        VarcharVector *vcVec = ((VarcharVector *)outputVecBatch->GetVector(0));

        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);

        string actualStr(reinterpret_cast<char *>(actualChar), len);
        if (i % 2 == 0) {
            EXPECT_EQ(actualStr, expected1);
        } else {
            EXPECT_EQ(actualStr, expected2);
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testDivDecimal128)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto addLeft = new LiteralExpr(new std::string("10357"), Decimal128Type(5, 2));
    auto addRight = new LiteralExpr(new std::string("95942"), Decimal128Type(5, 2));

    BinaryExpr *divExpr =
        new BinaryExpr(omniruntime::expressions::Operator::DIV, addLeft, addRight, Decimal128Type(38, 2));

    std::vector<Expr *> exprs = { divExpr };
    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_testDivDecimal");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0.LowBits(), 11);
    EXPECT_EQ(val0.HighBits(), 0);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}


TEST(ProjectionTest, testAddDecimal128)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto addLeft = new LiteralExpr(new std::string("478193"), Decimal128Type(6, 3));
    auto addRight = new LiteralExpr(new std::string("54356783"), Decimal128Type(8, 5));

    BinaryExpr *addExpr =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, Decimal128Type(9, 5));

    std::vector<Expr *> exprs = { addExpr };
    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_testAddDecimal128");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0.LowBits(), 102176083);
    EXPECT_EQ(val0.HighBits(), 0);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testDecimal128Between)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto value = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 0));
    auto lowerBound = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 3));
    auto upperBound = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 1));

    auto expr = new BetweenExpr(value, lowerBound, upperBound);

    std::vector<Expr *> exprs = { expr };
    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_testDecimal128Between");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    bool val0 = ((BooleanVector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_FALSE(val0);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testDecimal128In)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto arg0 = new LiteralExpr(new std::string("1234"), Decimal128Type(6, 0));
    auto arg1 = new LiteralExpr(new std::string("12345"), Decimal128Type(6, 0));
    auto arg2 = new LiteralExpr(new std::string("123456"), Decimal128Type(6, 0));
    std::vector<Expr *> args = { arg0, arg1, arg2 };
    auto expr = new InExpr(args);
    std::vector<Expr *> exprs = { expr };

    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_testDecimal128In");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    bool val0 = ((BooleanVector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_FALSE(val0);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testDecimal128Comprehensive)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto condition = new LiteralExpr(true, BooleanType());
    auto v1 = new LiteralExpr(new std::string("123400"), Decimal128Type(7, 3));
    v1->isNull = true;
    auto v2 = new LiteralExpr(new std::string("1234"), Decimal128Type(7, 3));
    auto coalesce = new CoalesceExpr(v1, v2);

    auto falseExpr = new LiteralExpr(new std::string("1234000"), Decimal128Type(7, 3));
    auto ifExpr = new IfExpr(condition, coalesce, falseExpr);
    auto right = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 2));
    auto expr = new BinaryExpr(omniruntime::expressions::Operator::GT, ifExpr, right, BooleanType());

    std::vector<Expr *> exprs = { expr };
    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_testDecimalComprehensive");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    bool val0 = ((BooleanVector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_FALSE(val0);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, TestAndExprWithNull)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 9;
    bool col1[numRows] = { true, true, true, false, false, false, true, false, false };
    bool col2[numRows] = { true, false, true, true, false, false, true, false, false };

    auto andLeft = new FieldExpr(0, BooleanType());
    auto andRight = new FieldExpr(1, BooleanType());
    auto andLeft1 = new FieldExpr(0, BooleanType());
    auto andRight1 = new FieldExpr(1, BooleanType());
    auto andLeft2 = new FieldExpr(0, BooleanType());
    auto andRight2 = new FieldExpr(1, BooleanType());
    BinaryExpr *andExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, andLeft, andRight, BooleanType());
    BinaryExpr *andExpr1 = new BinaryExpr(omniruntime::expressions::Operator::AND, andLeft1, andRight1, BooleanType());
    BinaryExpr *andExpr2 = new BinaryExpr(omniruntime::expressions::Operator::AND, andLeft2, andRight2, BooleanType());
    auto isNullExpr = new IsNullExpr(andExpr1);
    auto isNullExpr1 = new IsNullExpr(andExpr2);
    auto notExpr = new UnaryExpr(omniruntime::expressions::Operator::NOT, isNullExpr1, BooleanType());
    std::vector<Expr *> exprs = { andExpr, isNullExpr, notExpr };

    std::vector<DataTypePtr> vecOfTypes = { BooleanType(), BooleanType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_TestAndExprWithNull");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);

    t->GetVector(0)->SetValueNull(6);
    t->GetVector(0)->SetValueNull(7);
    t->GetVector(0)->SetValueNull(8);

    t->GetVector(1)->SetValueNull(2);
    t->GetVector(1)->SetValueNull(5);
    t->GetVector(1)->SetValueNull(8);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        bool val = ((BooleanVector *)outputVecBatch->GetVector(0))->GetValue(i);
        bool isValNull = ((BooleanVector *)outputVecBatch->GetVector(0))->IsValueNull(i);
        bool val1 = ((BooleanVector *)outputVecBatch->GetVector(1))->GetValue(i);
        bool val2 = ((BooleanVector *)outputVecBatch->GetVector(2))->GetValue(i);
        if (i == 0) {
            EXPECT_TRUE(val);
            EXPECT_FALSE(isValNull);
            EXPECT_FALSE(val1);
            EXPECT_TRUE(val2);
        } else if (i == 2 || i == 6 || i == 8) {
            EXPECT_TRUE(isValNull);
            EXPECT_TRUE(val1);
            EXPECT_FALSE(val2);
        } else {
            EXPECT_FALSE(val);
            EXPECT_FALSE(isValNull);
            EXPECT_FALSE(val1);
            EXPECT_TRUE(val2);
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, TestOrExprWithNull)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 9;
    bool col1[numRows] = { true, true, true, false, false, false, true, false, false };
    bool col2[numRows] = { true, false, true, true, false, false, true, false, false };

    auto orLeft = new FieldExpr(0, BooleanType());
    auto orRight = new FieldExpr(1, BooleanType());
    BinaryExpr *orExpr = new BinaryExpr(omniruntime::expressions::Operator::OR, orLeft, orRight, BooleanType());
    std::vector<Expr *> exprs = { orExpr };
    std::vector<DataTypePtr> vecOfTypes = { BooleanType(), BooleanType() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_TestOrExprWithNull");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);

    t->GetVector(0)->SetValueNull(6);
    t->GetVector(0)->SetValueNull(7);
    t->GetVector(0)->SetValueNull(8);

    t->GetVector(1)->SetValueNull(2);
    t->GetVector(1)->SetValueNull(5);
    t->GetVector(1)->SetValueNull(8);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        bool val = ((BooleanVector *)outputVecBatch->GetVector(0))->GetValue(i);
        bool isValNull = ((BooleanVector *)outputVecBatch->GetVector(0))->IsValueNull(i);
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

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testSubDecimal64)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto subLeft = new LiteralExpr(4321563L, Decimal64Type(7, 3));
    auto subRight = new LiteralExpr(123468L, Decimal64Type(6, 4));

    BinaryExpr *subExpr =
        new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, Decimal64Type(8, 4));

    std::vector<Expr *> exprs = { subExpr };
    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testSubDecimal64");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    int64_t val0 = ((LongVector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 43092162);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testMulDecimal64)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto mulLeft = new LiteralExpr(100L, Decimal64Type(7, 2));
    auto mulRight = new LiteralExpr(100L, Decimal64Type(7, 2));

    BinaryExpr *mulExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, Decimal64Type(7, 4));

    std::vector<Expr *> exprs = { mulExpr };
    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testMulDecimal64");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    int64_t val0 = ((LongVector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 10000L);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testDivDecimal64)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto left = new LiteralExpr(1225L, Decimal64Type(4, 2));
    auto right = new LiteralExpr(125L, Decimal64Type(3, 2));

    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, left, right, Decimal64Type(2, 1));

    std::vector<Expr *> exprs = { divExpr };
    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDivDecimal64");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    int64_t val0 = ((LongVector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 98L);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testModDecimal64)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto left = new LiteralExpr(12250L, Decimal64Type(5, 3));
    auto right = new LiteralExpr(125L, Decimal64Type(3, 2));

    BinaryExpr *modExpr = new BinaryExpr(omniruntime::expressions::Operator::MOD, left, right, Decimal64Type(4, 3));

    std::vector<Expr *> exprs = { modExpr };
    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testModDecimal64");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    int64_t val0 = ((LongVector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 1000L);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testDecimal64ArithOutputDecimal128)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
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

    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDecimal64ArithOutputDecimal128");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0.HighBits(), 1L << 63);
    EXPECT_EQ(val0.LowBits(), 999999999999999997L);
    Decimal128 val1 = ((Decimal128Vector *)outputVecBatch->GetVector(1))->GetValue(0);
    EXPECT_EQ(val1.HighBits(), 1L << 63);
    EXPECT_EQ(val1.LowBits(), 1000000000000000001);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testDecimal64In)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto arg0 = new LiteralExpr(65781L, Decimal64Type(6, 2));
    auto arg1 = new LiteralExpr(120945L, Decimal64Type(6, 2));
    auto arg2 = new LiteralExpr(65781L, Decimal64Type(6, 2));
    auto arg3 = new LiteralExpr(65781L, Decimal64Type(6, 2));
    std::vector<Expr *> args = { arg0, arg1, arg2, arg3 };
    auto expr = new InExpr(args);
    std::vector<Expr *> exprs = { expr };

    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDecimal64In");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    bool val0 = ((BooleanVector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_TRUE(val0);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testDecimal64Between)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto value = new LiteralExpr(76582L, Decimal64Type(5, 2));
    auto lowerBound = new LiteralExpr(87230L, Decimal64Type(5, 4));
    auto upperBound = new LiteralExpr(876903L, Decimal64Type(6, 1));
    auto expr = new BetweenExpr(value, lowerBound, upperBound);
    std::vector<Expr *> exprs = { expr };

    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDecimal64Between");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    bool val0 = ((BooleanVector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_TRUE(val0);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, testDecimal64Comprehensive)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto condition = new LiteralExpr(true, BooleanType());
    auto v1 = new LiteralExpr(123400L, Decimal64Type(7, 3));
    v1->isNull = true;
    auto v2 = new LiteralExpr(1234L, Decimal64Type(7, 3));
    auto coalesce = new CoalesceExpr(v1, v2);

    auto falseExpr = new LiteralExpr(1234000L, Decimal64Type(7, 3));
    auto ifExpr = new IfExpr(condition, coalesce, falseExpr);

    auto subLeft = new LiteralExpr(12340L, Decimal64Type(7, 3));
    auto subRight = new LiteralExpr(1010L, Decimal64Type(7, 3));
    auto right = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, Decimal64Type(7, 3));
    auto expr = new BinaryExpr(omniruntime::expressions::Operator::GT, ifExpr, right, BooleanType());

    std::vector<Expr *> exprs = { expr };
    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDecimal64Comprehensive");
    VectorBatch *input = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    bool val0 = ((BooleanVector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_FALSE(val0);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, Decimal64ColDivide)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1000;
    int64_t *col1 = MakeLongs(numRows, -500);
    LiteralExpr *divRight = new LiteralExpr(92122L, Decimal64Type(8, 4));
    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, new FieldExpr(0, Decimal64Type(8, 4)),
        divRight, Decimal64Type(8, 4));
    std::vector<Expr *> exprs = { divExpr };

    std::vector<DataTypePtr> vecOfTypes = { Decimal64Type() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Decimal64ColDivide");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int64_t i = 0; i < numRows; i++) {
        int64_t val0 = ((LongVector *)outputVecBatch->GetVector(0))->GetValue(i);
        int64_t expect = round(double(col1[i] * 10000) / 92122);
        EXPECT_EQ(val0, expect);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
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

VectorBatch *CreateInputVecBatchForConcat(const std::vector<DataTypePtr> &inputTypes, VectorAllocator *vecAllocator)
{
    const int32_t rowCount = 8;
    const std::string firstName = "John";
    const std::string lastName = "Rebecca";
    const std::string fullFirstName = "-John-John-John-John";
    const std::string fullLastName = "Rebecca-Rebecca-Rebecca-Rebeca";
    const std::string empty = "";

    auto vec0 = new VarcharVector(vecAllocator,
        rowCount * static_cast<VarcharDataType *>(inputTypes[0].get())->GetWidth(), rowCount);
    vec0->SetValueNull(0);
    vec0->SetValue(1, reinterpret_cast<const unsigned char *>(firstName.c_str()), firstName.length());
    vec0->SetValueNull(2);
    vec0->SetValue(3, reinterpret_cast<const unsigned char *>(empty.c_str()), empty.length());
    vec0->SetValue(4, reinterpret_cast<const unsigned char *>(firstName.c_str()), firstName.length());
    vec0->SetValue(5, reinterpret_cast<const unsigned char *>(empty.c_str()), empty.length());
    vec0->SetValue(6, reinterpret_cast<const unsigned char *>(firstName.c_str()), firstName.length());
    vec0->SetValue(7, reinterpret_cast<const unsigned char *>(fullFirstName.c_str()), fullFirstName.length());

    auto vec1 = new VarcharVector(vecAllocator,
        rowCount * static_cast<VarcharDataType *>(inputTypes[1].get())->GetWidth(), rowCount);
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
    auto expectVec = new VarcharVector(vecAllocator,
        rowCount * static_cast<const VarcharDataType &>(expectDataType).GetWidth(), rowCount);
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
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    auto concatExpr = GetConcatFuncExpr(CharType(20), VarcharType(30), CharType(100));
    std::vector<Expr *> exprs = { concatExpr };
    std::vector<DataTypePtr> vecOfTypes = { CharType(20), VarcharType(30) };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();

    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ConcatStrCharTest");
    auto input = CreateInputVecBatchForConcat(vecOfTypes, vecAllocator);

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    std::vector<std::string> expectedDatas = { "NULL",        "NULL",
        "NULL",        "",
        "John",        "Rebecca",
        "RebeccaJohn", "Rebecca-Rebecca-Rebecca-Rebeca-John-John-John-John" };
    auto expect = CreateExpectVecBatchForConcat(CharDataType(100), vecAllocator, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete op;
    delete factory;
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ConcatCharStrTest)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    auto concatExpr = GetConcatFuncExpr(VarcharType(20), CharType(30), CharType(100));
    std::vector<Expr *> exprs = { concatExpr };
    std::vector<DataTypePtr> vecOfTypes = { VarcharType(20), CharType(30) };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();

    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ConcatCharStrTest");
    auto input = CreateInputVecBatchForConcat(vecOfTypes, vecAllocator);

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    std::vector<std::string> expectedDatas = { "NULL",
        "NULL",
        "NULL",
        "",
        "                              John",
        "Rebecca",
        "Rebecca                       John",
        "Rebecca-Rebecca-Rebecca-Rebeca-John-John-John-John" };
    auto expect = CreateExpectVecBatchForConcat(CharDataType(100), vecAllocator, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete op;
    delete factory;
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ConcatStrStrTest)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    auto concatExpr = GetConcatFuncExpr(VarcharType(20), VarcharType(30), VarcharType(100));
    std::vector<Expr *> exprs = { concatExpr };
    std::vector<DataTypePtr> vecOfTypes = { VarcharType(20), VarcharType(30) };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();

    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ConcatStrStrTest");
    auto input = CreateInputVecBatchForConcat(vecOfTypes, vecAllocator);

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    std::vector<std::string> expectedDatas = { "NULL",        "NULL",
        "NULL",        "",
        "John",        "Rebecca",
        "RebeccaJohn", "Rebecca-Rebecca-Rebecca-Rebeca-John-John-John-John" };
    auto expect = CreateExpectVecBatchForConcat(VarcharDataType(100), vecAllocator, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete op;
    delete factory;
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ConcatCharCharTest)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    auto concatExpr = GetConcatFuncExpr(CharType(20), CharType(30), CharType(51));
    std::vector<Expr *> exprs = { concatExpr };
    std::vector<DataTypePtr> vecOfTypes = { CharType(20), CharType(30) };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();

    auto vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_ConcatCharCharTest");
    auto input = CreateInputVecBatchForConcat(vecOfTypes, vecAllocator);

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    std::vector<std::string> expectedDatas = { "NULL",
        "NULL",
        "NULL",
        "",
        "                              John",
        "Rebecca",
        "Rebecca                       John",
        "Rebecca-Rebecca-Rebecca-Rebeca-John-John-John-John" };
    auto expect = CreateExpectVecBatchForConcat(CharDataType(51), vecAllocator, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete op;
    delete factory;
    delete vecAllocator;
    delete overflowConfig;
}

Expr *GetStringFuncExpr(std::vector<DataTypePtr> inputTypes, DataTypePtr returnType, string funcStr)
{
    std::vector<Expr *> args;
    for (int32_t i = 0; i < (int32_t)inputTypes.size(); i++) {
        args.push_back(new FieldExpr(i, std::move(inputTypes.at(i))));
    }

    auto stringFuncExpr = GetFuncExpr(funcStr, args, std::move(returnType));
    return stringFuncExpr;
}

TEST(ProjectionTest, ReplaceStrWithRep)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    vector<DataTypePtr> vecTypes = { VarcharType(20), VarcharType(10), VarcharType(10) };
    string replaceFuncStr = "replace";
    auto replaceFuncExpr = GetStringFuncExpr(vecTypes, VarcharType(100), replaceFuncStr);
    vector<Expr *> exprs = { replaceFuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    string str[] = { "varchar100", "varchar200", "varchar300" };
    string search[] = { "char1", "char2", "char3" };
    string replace[] = { "opera", "*#", "VARCHAR" };
    auto input = CreateVectorBatch(inputTypes, 3, str, search, replace);

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { VarcharType(100) };
    string expectedDatas[] = { "varopera00", "var*#00", "varVARCHAR00" };

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, ReplaceStrWithoutRep)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    vector<DataTypePtr> vecTypes = { VarcharType(20), VarcharType(10) };
    string replaceFuncStr = "replace";
    auto replaceFuncExpr = GetStringFuncExpr(vecTypes, VarcharType(100), replaceFuncStr);
    vector<Expr *> exprs = { replaceFuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    string str[] = { "varchar100", "varchar200", "varchar300" };
    string search[] = { "char1", "char2", "char3" };
    auto input = CreateVectorBatch(inputTypes, 3, str, search);

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { VarcharType(100) };
    string expectedDatas[] = { "var00", "var00", "var00" };

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, LowerStr)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    vector<DataTypePtr> vecTypes = { VarcharType(20) };
    string lowerFuncStr = "lower";
    auto lowerFuncExpr = GetStringFuncExpr(vecTypes, VarcharType(20), lowerFuncStr);
    vector<Expr *> exprs = { lowerFuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    string str[] = { "VARchar100", "Char200", "var**VAR" };
    auto input = CreateVectorBatch(inputTypes, 3, str);

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { VarcharType(20) };
    string expectedDatas[] = { "varchar100", "char200", "var**var" };

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, LowerChar)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    vector<DataTypePtr> vecTypes = { CharType(20) };
    string lowerFuncStr = "lower";
    auto lowerFuncExpr = GetStringFuncExpr(vecTypes, CharType(20), lowerFuncStr);
    vector<Expr *> exprs = { lowerFuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    string str[] = { "VARchar100", "Char200", "var**VAR" };
    auto input = CreateVectorBatch(inputTypes, 3, str);

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { CharType(20) };
    string expectedDatas[] = { "varchar100", "char200", "var**var" };

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, LengthChar)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    vector<DataTypePtr> vecTypes = { CharType(20) };
    string lenFuncStr = "length";
    auto lenFuncExpr = GetStringFuncExpr(vecTypes, LongType(), lenFuncStr);
    vector<Expr *> exprs = { lenFuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    string str[] = { "VARchar100", "Char200", "var**VAR" };
    auto input = CreateVectorBatch(inputTypes, 3, str);

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { LongType() };
    int64_t expectedDatas[] = { 20, 20, 20 };

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, LengthStr)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    vector<DataTypePtr> vecTypes = { VarcharType(20) };
    string lenFuncStr = "length";
    auto lenFuncExpr = GetStringFuncExpr(vecTypes, LongType(), lenFuncStr);
    vector<Expr *> exprs = { lenFuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    string str[] = { "VARchar100", "Char200", "var**VAR" };
    auto input = CreateVectorBatch(inputTypes, 3, str);

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { LongType() };
    int64_t expectedDatas[] = { 10, 7, 8 };

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, XxH64Int)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    std::vector<DataTypePtr> vecTypes = { IntType() };
    auto col = new FieldExpr(0, IntType());
    auto seed = new LiteralExpr(42L, LongType());
    std::vector<Expr *> hashArgs;
    hashArgs.push_back(col);
    hashArgs.push_back(seed);
    string xxH64FuncStr = "xxhash64";
    auto xxH64FuncExpr = GetFuncExpr(xxH64FuncStr, hashArgs, LongType());
    vector<Expr *> exprs = { xxH64FuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto factory = new ProjectionOperatorFactory(move(exprEvaluator));

    int32_t value[] = {1, 2, 3};
    auto input = CreateVectorBatch(inputTypes, 3, value);

    auto op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { LongType() };
    int64_t expectedDatas[] = { -6698625589789238999, 8420071140774656230, 6258084186791473711 };

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, XxH64Long)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    std::vector<DataTypePtr> vecTypes = { LongType() };
    auto col = new FieldExpr(0, LongType());
    auto seed = new LiteralExpr(42L, LongType());
    std::vector<Expr *> hashArgs;
    hashArgs.push_back(col);
    hashArgs.push_back(seed);
    string xxH64FuncStr = "xxhash64";
    auto xxH64FuncExpr = GetFuncExpr(xxH64FuncStr, hashArgs, LongType());
    vector<Expr *> exprs = { xxH64FuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto factory = new ProjectionOperatorFactory(move(exprEvaluator));

    int64_t value[] = {1L, 2L, 3L};
    auto input = CreateVectorBatch(inputTypes, 3, value);

    auto op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { LongType() };
    int64_t expectedDatas[] = { -7001672635703045582, -3341702809300393011, 3188756510806108107 };

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, XxH64Double)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    std::vector<DataTypePtr> vecTypes = { DoubleType() };
    auto col = new FieldExpr(0, DoubleType());
    auto seed = new LiteralExpr(42L, LongType());
    std::vector<Expr *> hashArgs;
    hashArgs.push_back(col);
    hashArgs.push_back(seed);
    string xxH64FuncStr = "xxhash64";
    auto xxH64FuncExpr = GetFuncExpr(xxH64FuncStr, hashArgs, LongType());
    vector<Expr *> exprs = { xxH64FuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto factory = new ProjectionOperatorFactory(move(exprEvaluator));

    double value[] = {12.34, 56.78, 90.12};
    auto input = CreateVectorBatch(inputTypes, 3, value);

    auto op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { LongType() };
    int64_t expectedDatas[] = { -8572681829277995901, -8568427336103557548, -5412156501924286366};

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, XxH64Boolean)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    std::vector<DataTypePtr> vecTypes = { BooleanType() };
    auto col = new FieldExpr(0, BooleanType());
    auto seed = new LiteralExpr(42L, LongType());
    std::vector<Expr *> hashArgs;
    hashArgs.push_back(col);
    hashArgs.push_back(seed);
    string xxH64FuncStr = "xxhash64";
    auto xxH64FuncExpr = GetFuncExpr(xxH64FuncStr, hashArgs, LongType());
    vector<Expr *> exprs = { xxH64FuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto factory = new ProjectionOperatorFactory(move(exprEvaluator));

    bool value[] = {false, true, false};
    auto input = CreateVectorBatch(inputTypes, 3, value);

    auto op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { LongType() };
    int64_t expectedDatas[] = { 3614696996920510707, -6698625589789238999, 3614696996920510707 };

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, XxH64Str)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    std::vector<DataTypePtr> vecTypes = { VarcharType(50) };
    auto col = new FieldExpr(0, VarcharType(50));
    auto seed = new LiteralExpr(42L, LongType());
    std::vector<Expr *> hashArgs;
    hashArgs.push_back(col);
    hashArgs.push_back(seed);
    string xxH64FuncStr = "xxhash64";
    auto xxH64FuncExpr = GetFuncExpr(xxH64FuncStr, hashArgs, LongType());
    vector<Expr *> exprs = { xxH64FuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto factory = new ProjectionOperatorFactory(move(exprEvaluator));

    string str[] = { "hello world", "abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ", "china" };
    auto input = CreateVectorBatch(inputTypes, 3, str);

    auto op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { LongType() };
    int64_t expectedDatas[] = { 7620854247404556961, -8961370173016112133, 1148854020565811068 };

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, XxH64Decimal128)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    std::vector<DataTypePtr> vecTypes = { Decimal128Type(38, 16) };
    auto col = new FieldExpr(0, Decimal128Type(38, 16));
    auto seed = new LiteralExpr(42L, LongType());
    std::vector<Expr *> hashArgs;
    hashArgs.push_back(col);
    hashArgs.push_back(seed);
    string xxH64FuncStr = "xxhash64";
    auto xxH64FuncExpr = GetFuncExpr(xxH64FuncStr, hashArgs, LongType());
    vector<Expr *> exprs = { xxH64FuncExpr };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto factory = new ProjectionOperatorFactory(move(exprEvaluator));

    Decimal128 value[] = { Decimal128("1111111111111111111.1000000000000000"),
                               Decimal128("8888888888888888888.8000000000000000"),
                               Decimal128("9999999999999999999.9000000000000000")};
    auto input = CreateVectorBatch(inputTypes, 3, value);

    auto op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { LongType() };
    int64_t expectedDatas[] = { 6365211361990375607, 2903581947191644088, 6893535756916194581};

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
}

TEST(ProjectionTest, MightContain)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);

    int32_t versionJava = 1;
    int32_t hashFuncNum = 6;
    int32_t numWords = 1048576; // 1MBytes

    int64_t byteLength = numWords * sizeof(int64_t) + sizeof(versionJava) + sizeof(hashFuncNum) + sizeof(numWords);
    auto *in = new int8_t[byteLength]{ 0 };
    (reinterpret_cast<int32_t *>(in))[0] = 1;
    (reinterpret_cast<int32_t *>(in))[1] = hashFuncNum;
    (reinterpret_cast<int32_t *>(in))[2] = numWords;
    auto *bloomFilter = new BloomFilter(in, versionJava);
    for (int64_t i = 1; i < 100; i += 2) {
        bloomFilter->PutLong(i);
    }

    auto bloomFilterAddr = new LiteralExpr(reinterpret_cast<int64_t>(bloomFilter), LongType());
    auto hashValue = new FieldExpr(0, LongType());
    std::vector<Expr *> hashArgs;
    hashArgs.push_back(bloomFilterAddr);
    hashArgs.push_back(hashValue);
    string mightContainFuncStr = "might_contain";
    auto mightContainFuncExpr = GetFuncExpr(mightContainFuncStr, hashArgs, BooleanType());
    vector<Expr *> exprs = { mightContainFuncExpr };
    std::vector<DataTypePtr> vecTypes = { LongType() };
    DataTypes inputTypes(vecTypes);
    auto overflowConfig = new OverflowConfig();

    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto factory = new ProjectionOperatorFactory(move(exprEvaluator));

    int64_t value[] = { 11L, 30L, 55L};
    auto input = CreateVectorBatch(inputTypes, 3, value);

    auto op = factory->CreateOperator();
    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    vector<DataTypePtr> expectedTypes = { BooleanType() };
    bool expectedDatas[] = { true, false, true};

    auto expect = CreateVectorBatch(DataTypes(expectedTypes), 3, expectedDatas);
    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expect));

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expect);
    delete overflowConfig;
    delete op;
    delete factory;
    delete bloomFilter;
    delete[] in;
}

TEST(ProjectionTest, testDecimal128NegativeLiteral)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    auto literalLeft = new LiteralExpr(new std::string("0"), Decimal128Type(38, 16));
    auto literalRight =
        new LiteralExpr(new std::string("-99999999999999999999999999999999999999"), Decimal128Type(38, 16));

    BinaryExpr *subExpr =
        new BinaryExpr(omniruntime::expressions::Operator::SUB, literalLeft, literalRight, Decimal128Type(38, 16));

    std::vector<Expr *> exprs = { subExpr };
    std::vector<DataTypePtr> vecOfTypes = {};
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDecimal128NegativeLiteral");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0.LowBits(), 687399551400673279);
    EXPECT_EQ(val0.HighBits(), 5421010862427522170);

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete op;
    delete factory;
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ProjectCastIntToString)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 3;
    auto *col = new int[3]{ 123, 312, 456 };
    std::vector<DataTypePtr> vecOfTypes = { IntType() };

    auto data = new FieldExpr(0, IntType());
    std::string castStr = "CAST";
    std::vector<Expr *> args;
    args.push_back(data);
    auto castExpr = GetFuncExpr(castStr, args, VarcharType(4));

    std::vector<Expr *> exprs = { castExpr };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastDouble");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col);

    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
    }

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    VarcharVector *vcVec = ((VarcharVector *)outputVecBatch->GetVector(0));
    for (int32_t i = 0; i < numRows; i++) {
        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);
        string actualStr(reinterpret_cast<char *>(actualChar), len);
        EXPECT_EQ(actualStr, to_string(col[i]));
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(t);
    delete[] col;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ProjectCastDecimal128ToString)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 3;
    auto *col = MakeDecimals(numRows, 10);
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };

    auto data = new FieldExpr(0, Decimal128Type(38, 0));
    std::string castStr = "CAST";
    std::vector<Expr *> args;
    args.push_back(data);
    auto castExpr = GetFuncExpr(castStr, args, VarcharType(5));

    std::vector<Expr *> exprs = { castExpr };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastDouble");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col);

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    VarcharVector *vcVec = ((VarcharVector *)outputVecBatch->GetVector(0));
    for (int32_t i = 0; i < numRows; i++) {
        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);
        string actualStr(reinterpret_cast<char *>(actualChar), len);
        EXPECT_EQ(actualStr, to_string(col[i * 2]));
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(t);
    delete[] col;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ProjectCastDoubleToString)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 3;
    auto *col = MakeDoubles(numRows, 10);
    std::vector<DataTypePtr> vecOfTypes = { DoubleType() };

    auto data = new FieldExpr(0, DoubleType());
    std::string castStr = "CAST";
    std::vector<Expr *> args;
    args.push_back(data);
    auto castExpr = GetFuncExpr(castStr, args, VarcharType(5));

    std::vector<Expr *> exprs = { castExpr };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig(omniruntime::op::OVERFLOW_CONFIG_NULL);
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastDouble");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col);

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    string res[] = { "10.0", "11.0", "12.0" };
    VarcharVector *vcVec = ((VarcharVector *)outputVecBatch->GetVector(0));
    for (int32_t i = 0; i < numRows; i++) {
        uint8_t *actualChar = nullptr;
        int len = vcVec->GetValue(i, &actualChar);
        string actualStr(reinterpret_cast<char *>(actualChar), len);
        EXPECT_EQ(actualStr, res[i]);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(t);
    delete[] col;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ProjectCastStringToString)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    vector<string> strings;
    for (int32_t i = 0; i < numRows; i++) {
        strings.emplace_back("CastStringToString");
    }
    vector<bool> nulls;
    for (int32_t i = 0; i < numRows; i++) {
        nulls.emplace_back(false);
    }

    std::string castStr = "CAST";
    auto data1 = new FieldExpr(0, VarcharType(18));
    std::vector<Expr *> args1;
    args1.push_back(data1);
    auto castExpr1 = GetFuncExpr(castStr, args1, VarcharType(1024));

    auto data2 = new FieldExpr(0, VarcharType(18));
    std::vector<Expr *> args2;
    args2.push_back(data2);
    auto castExpr2 = GetFuncExpr(castStr, args2, VarcharType(10));
    std::vector<Expr *> exprs = { castExpr1, castExpr2 };

    std::vector<DataTypePtr> vecOfTypes = { VarcharType(18) };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastStringToString");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *t = CreateVectorBatch(numRows, cols);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    string expected1 = "CastStringToString";
    string expected2 = "CastString";

    VarcharVector *vcVec1 = ((VarcharVector *)outputVecBatch->GetVector(0));
    uint8_t *actualChar1 = nullptr;
    int len1 = vcVec1->GetValue(0, &actualChar1);
    string actualStr1(reinterpret_cast<char *>(actualChar1), len1);
    EXPECT_EQ(len1, 18);
    EXPECT_EQ(actualStr1, expected1);

    VarcharVector *vcVec2 = ((VarcharVector *)outputVecBatch->GetVector(1));
    uint8_t *actualChar2 = nullptr;
    int len2 = vcVec2->GetValue(0, &actualChar2);
    string actualStr2(reinterpret_cast<char *>(actualChar2), len2);
    EXPECT_EQ(len2, 10);
    EXPECT_EQ(actualStr2, expected2);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ProjectCastStrStrZh)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;

    std::string castStr = "CAST";
    std::vector<Expr *> args1 = { new FieldExpr(0, VarcharType(18)) };
    auto castExpr1 = GetFuncExpr(castStr, args1, VarcharType(1024));

    std::vector<Expr *> args2 = { new FieldExpr(0, VarcharType(18)) };
    auto castExpr2 = GetFuncExpr(castStr, args2, VarcharType(7));
    std::vector<Expr *> exprs = { castExpr1, castExpr2 };

    std::vector<DataTypePtr> vecOfTypes = { VarcharType(18) };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    string *col0 = new string[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col0[i] = "cast乌s斯侧后解";
    }
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastStrStrZh");
    VectorBatch *input = CreateVectorBatch(inputTypes, numRows, col0);

    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    string *expected1 = new string[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        expected1[i] = "cast乌s斯侧后解";
    }
    string *expected2 = new string[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        expected2[i] = "cast乌s斯";
    }

    DataTypes outputTypes({ VarcharType(1024), VarcharType(7) });
    VectorBatch *expectedRet = CreateVectorBatch(outputTypes, numRows, expected1, expected2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectedRet));

    delete[] col0;
    delete[] expected1;
    delete[] expected2;
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectedRet);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ProjectCastStrStrWithOverflowConfig)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;

    std::string castStr = "CAST";
    std::vector<Expr *> args1 = { new FieldExpr(0, VarcharType(18)) };
    auto castExpr1 = GetFuncExpr(castStr, args1, VarcharType(1024));

    std::vector<Expr *> args2 = { new FieldExpr(0, VarcharType(18)) };
    auto castExpr2 = GetFuncExpr(castStr, args2, VarcharType(7));
    std::vector<Expr *> exprs = { castExpr1, castExpr2 };

    std::vector<DataTypePtr> vecOfTypes = { VarcharType(18) };
    DataTypes inputTypes(vecOfTypes);

    auto overflowConfig = new OverflowConfig(omniruntime::op::OVERFLOW_CONFIG_NULL);
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();

    string *col0 = new string[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col0[i] = "cast乌s斯侧后解";
    }
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastStrStrZh");
    VectorBatch *input = CreateVectorBatch(inputTypes, numRows, col0);

    op->AddInput(input);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);

    string *expected1 = new string[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        expected1[i] = "cast乌s斯侧后解";
    }
    string *expected2 = new string[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        expected2[i] = "cast乌s斯";
    }

    DataTypes outputTypes({ VarcharType(1024), VarcharType(7) });
    VectorBatch *expectedRet = CreateVectorBatch(outputTypes, numRows, expected1, expected2);

    EXPECT_TRUE(VecBatchMatch(outputVecBatch, expectedRet));

    delete[] col0;
    delete[] expected1;
    delete[] expected2;
    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(expectedRet);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ProjectCastIntToDecimal)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 10;
    auto *col1 = MakeInts(numRows);
    std::vector<DataTypePtr> vecOfTypes = { IntType() };

    auto data = new FieldExpr(0, IntType());
    std::string castStr = "CAST";
    std::vector<Expr *> args;
    args.push_back(data);
    auto castExpr = GetFuncExpr(castStr, args, Decimal128Type(38, 0));

    std::vector<Expr *> exprs = { castExpr };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastDouble");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1);

    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    delete[] col1;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ProjectCastIntToDecimal64)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 3;
    auto *col = new int[3]{ 123, 312, 456 };
    std::vector<DataTypePtr> vecOfTypes = { IntType() };

    auto data = new FieldExpr(0, IntType());
    std::string castStr = "CAST";
    std::vector<Expr *> args;
    args.push_back(data);
    auto castExpr = GetFuncExpr(castStr, args, Decimal64Type(18, 2));

    std::vector<Expr *> exprs = { castExpr };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_CastDouble");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col);

    for (int32_t i = 0; i < numRows; i++) {
        t->GetVector(0)->SetValueNotNull(i);
    }

    auto copy = DuplicateVectorBatch(t);
    op->AddInput(copy);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        int64_t val0 = ((LongVector *)outputVecBatch->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, col[i] * 100);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    VectorHelper::FreeVecBatch(t);
    delete[] col;
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ProjectSparkConfig)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    std::string castStr = "CAST";
    std::vector<Expr *> argLeft{ new FieldExpr(0, Decimal64Type(7, 0)) };
    FuncExpr *subLeft = GetFuncExpr(castStr, argLeft, Decimal64Type(8, 0));
    int64_t *col = new int64_t[1];
    col[0] = 123;
    std::vector<Expr *> argRight{ new FieldExpr(0, Decimal64Type(7, 0)) };
    FuncExpr *subRight = GetFuncExpr(castStr, argRight, Decimal64Type(8, 0));
    auto *addExprs = new BinaryExpr(omniruntime::expressions::Operator::ADD, subLeft, subRight, Decimal64Type(8, 0));

    std::vector<Expr *> exprs = { addExprs };
    std::vector<DataTypePtr> vecOfTypes = { Decimal64Type() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig(OVERFLOW_CONFIG_NULL);
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDecimal128NegativeLiteral");
    VectorBatch *t = CreateVectorBatch(inputTypes, 1, col);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    int64_t val0 = ((LongVector *)outputVecBatch->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 246);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete[] col;
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, ProjectMulDecimal64)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    int64_t *col = new int64_t[1];
    col[0] = 999999;
    FieldExpr *mulLeft = new FieldExpr(0, Decimal64Type(7, 0));
    FieldExpr *mulRight = new FieldExpr(0, Decimal64Type(7, 0));
    auto *mulExprs1 = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, Decimal64Type(7, 0));
    FieldExpr *mulRight2 = new FieldExpr(0, Decimal64Type(7, 0));
    auto *mulExprs2 =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, mulExprs1, mulRight2, Decimal64Type(7, 0));

    std::vector<Expr *> exprs = { mulExprs2 };
    std::vector<DataTypePtr> vecOfTypes = { Decimal64Type() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig(OVERFLOW_CONFIG_NULL);
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("testDecimal128NegativeLiteral");
    VectorBatch *t = CreateVectorBatch(inputTypes, 1, col);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    bool isNull = (outputVecBatch->GetVector(0))->IsValueNull(0);
    EXPECT_EQ(isNull, true);

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete[] col;
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, AddDecimal)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 2;
    int64_t *col1 = new int64_t[4]{ 1, 0, 2, 0 };
    int64_t *col2 = new int64_t[2]{ 1, 2 };
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

    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type(), Decimal64Type() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig();
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_BatchAddDecimal128");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        for (int32_t j = 0; i < numRows; i++) {
            Decimal128 val0 = ((Decimal128Vector *)outputVecBatch->GetVector(j))->GetValue(i);
            EXPECT_EQ(val0.HighBits(), 0);
            EXPECT_EQ(val0.LowBits(), (i + 1) * 2);
        }
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete[] col1;
    delete[] col2;
    delete vecAllocator;
    delete overflowConfig;
}

TEST(ProjectionTest, AddDecimalReturnNull)
{
    ConfigUtil::SetEnableBatchExprEvaluate(false);
    const int32_t numRows = 1;
    int64_t *col1 = new int64_t[2]{ -1, INT64_MAX };
    int64_t *col2 = new int64_t[1]{ INT64_MAX };
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

    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type(), Decimal64Type() };
    DataTypes inputTypes(vecOfTypes);
    auto overflowConfig = new OverflowConfig(OVERFLOW_CONFIG_NULL);
    auto exprEvaluator = std::make_shared<ExpressionEvaluator>(exprs, inputTypes, overflowConfig);
    auto *factory = new ProjectionOperatorFactory(move(exprEvaluator));
    omniruntime::op::Operator *op = factory->CreateOperator();
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_BatchAddDecimal128");
    VectorBatch *t = CreateVectorBatch(inputTypes, numRows, col1, col2);
    op->AddInput(t);
    VectorBatch *outputVecBatch = nullptr;
    op->GetOutput(&outputVecBatch);
    for (int32_t i = 0; i < numRows; i++) {
        bool isNull = (outputVecBatch->GetVector(0))->IsValueNull(i);
        EXPECT_TRUE(isNull);
        isNull = (outputVecBatch->GetVector(1))->IsValueNull(i);
        EXPECT_FALSE(isNull);
        isNull = (outputVecBatch->GetVector(2))->IsValueNull(i);
        EXPECT_TRUE(isNull);
        isNull = (outputVecBatch->GetVector(3))->IsValueNull(i);
        EXPECT_TRUE(isNull);
    }

    VectorHelper::FreeVecBatch(outputVecBatch);
    omniruntime::op::Operator::DeleteOperator(op);
    DeleteOperatorFactory(factory);
    delete[] col1;
    delete[] col2;
    delete vecAllocator;
    delete overflowConfig;
}
}