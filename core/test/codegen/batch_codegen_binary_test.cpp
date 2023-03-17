/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: binaryExpr batch codegen test
 */
#include "gtest/gtest.h"

#include <string>
#include <vector>
#include "operator/filter/filter_and_project.h"
#include "operator/projection/projection.h"
#include "util/test_util.h"
#include "util/config_util.h"
#include "codegen_util.h"

using namespace std;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace TestUtil;
using namespace CodegenUtil;

TEST(BatchCodeGenTest, IntCompare1)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto *lessThanLeft = new FieldExpr(0, IntType());
    auto *lessThanRight = new LiteralExpr(50, IntType());
    auto *lessThanExpr =
        new BinaryExpr(omniruntime::expressions::Operator::LT, lessThanLeft, lessThanRight, BooleanType());

    auto *greatThanOrEqLeft = new FieldExpr(0, IntType());
    auto *greatThanOrEqRight = new LiteralExpr(25, IntType());
    auto *greatThanOrEqExpr =
        new BinaryExpr(omniruntime::expressions::Operator::GTE, greatThanOrEqLeft, greatThanOrEqRight, BooleanType());

    BinaryExpr *filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::AND, lessThanExpr, greatThanOrEqExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, IntType());
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int numRows = 100;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_IntCompare1");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(filter, projections, numCols, vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(25, numSelectedRows);
    for (int i = 0; i < numSelectedRows; ++i) {
        int32_t val = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val < 50 && val >= 25);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, IntCompare2)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto *gtLeft = new FieldExpr(0, IntType());
    auto *gtRight = new LiteralExpr(50, IntType());
    auto *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft, gtRight, BooleanType());

    auto *lteLeft = new FieldExpr(0, IntType());
    auto *lteRight = new LiteralExpr(25, IntType());
    auto *lteEqExpr = new BinaryExpr(omniruntime::expressions::Operator::LTE, lteLeft, lteRight, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::OR, gtExpr, lteEqExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, IntType());
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int numRows = 100;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType() }));
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_IntCompare2");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(filter, projections, numCols, vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(75, numSelectedRows);

    for (int i = 0; i < numSelectedRows; ++i) {
        int32_t val = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val > 50 || val <= 25);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, LongCompare1)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto *greatThanOrEqLeft = new FieldExpr(0, LongType());
    auto *greatThanOrEqRight = new LiteralExpr(50L, LongType());
    auto *greatThanOrEqExpr =
        new BinaryExpr(omniruntime::expressions::Operator::GTE, greatThanOrEqLeft, greatThanOrEqRight, BooleanType());

    auto *lessThanOrEqLeft = new FieldExpr(0, LongType());
    auto *lessThanOrEqRight = new LiteralExpr(25L, LongType());
    auto *lessThanOrEqExpr =
        new BinaryExpr(omniruntime::expressions::Operator::LTE, lessThanOrEqLeft, lessThanOrEqRight, BooleanType());

    BinaryExpr *filterExpr =
        new BinaryExpr(omniruntime::expressions::Operator::OR, greatThanOrEqExpr, lessThanOrEqExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, LongType());
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int numRows = 100;
    int64_t *col1 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ LongType() }));
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LongCompare1");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(filter, projections, numCols, vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(76, numSelectedRows);

    for (int i = 0; i < numSelectedRows; ++i) {
        int32_t val = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val >= 50 || val <= 25);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, LongCompare2)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto *eqLeft = new FieldExpr(0, LongType());
    auto *eqRight = new LiteralExpr(9L, LongType());
    auto *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());

    auto *ltLeft = new FieldExpr(0, LongType());
    auto *ltRight = new LiteralExpr(10L, LongType());
    auto *ltExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, ltLeft, ltRight, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, ltExpr, eqExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, LongType());
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int numRows = 100;
    int64_t *col1 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ LongType() }));
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LongCompare2");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(filter, projections, numCols, vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(1, numSelectedRows);

    for (int i = 0; i < numSelectedRows; ++i) {
        int32_t val = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val == 9);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, DoubleCompare1)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto *eqLeft = new FieldExpr(0.0, DoubleType());
    auto *eqRight = new LiteralExpr(50.0, DoubleType());
    auto *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());

    auto *ltLeft = new FieldExpr(0.0, DoubleType());
    auto *ltRight = new LiteralExpr(25.0, DoubleType());
    auto *ltExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, ltLeft, ltRight, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::OR, eqExpr, ltExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, DoubleType());
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int numRows = 100;
    double *col1 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ DoubleType() }));
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_DoubleCompare1");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(filter, projections, numCols, vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(26, numSelectedRows);

    for (int i = 0; i < numSelectedRows; ++i) {
        double val = ((DoubleVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val == 50.0 || val < 25.0);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, DoubleCompare2)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto *gtLeft = new FieldExpr(0.0, DoubleType());
    auto *gtRight = new LiteralExpr(50.0, DoubleType());
    auto *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft, gtRight, BooleanType());

    auto *lteLeft = new FieldExpr(0.0, DoubleType());
    auto *lteRight = new LiteralExpr(25.0, DoubleType());
    auto *lteExpr = new BinaryExpr(omniruntime::expressions::Operator::LTE, lteLeft, lteRight, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::OR, gtExpr, lteExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, DoubleType());
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int numRows = 100;
    double *col1 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ DoubleType() }));
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_DoubleCompare2");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(filter, projections, numCols, vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(75, numSelectedRows);

    for (int i = 0; i < numSelectedRows; ++i) {
        double val = ((DoubleVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_TRUE(val > 50.0 || val <= 25.0);
    }

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, StringCompare)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, VarcharType()),
        new LiteralExpr(new std::string("hello"), VarcharType()), BooleanType());

    auto *projExpr = new FieldExpr(0, VarcharType());
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    vector<string> strings;
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 40 == 0) {
            strings.emplace_back("hello");
        } else {
            strings.emplace_back("abcdefghijklmhjs");
        }
    }
    vector<bool> nulls;
    for (int32_t i = 0; i < numRows; i++) {
        nulls.emplace_back(false);
    }

    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_StringCompare");
    auto *vecBatch = CreateVectorBatch(numRows, cols);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(filter, projections, numCols, vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(25, numSelectedRows);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal64Compare1)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    FieldExpr *lteLeft = new FieldExpr(0, Decimal64Type(8, 0));
    LiteralExpr *lteRight = new LiteralExpr(3, Decimal64Type(8, 1));
    BinaryExpr *lteExpr = new BinaryExpr(omniruntime::expressions::Operator::LTE, lteLeft, lteRight, BooleanType());

    FieldExpr *gteLeft = new FieldExpr(0, Decimal64Type(8, 0));
    LiteralExpr *gteRight = new LiteralExpr(3, Decimal64Type(8, 1));
    BinaryExpr *gteExpr = new BinaryExpr(omniruntime::expressions::Operator::GTE, gteLeft, gteRight, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, gteExpr, lteExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, Decimal64Type(8, 0));
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int32_t numRows = 10;
    int64_t *data1 = MakeLongs(numRows);

    std::vector<DataTypePtr> vecOfTypes = { LongType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal64Compare1");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, data1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(filter, projections, numCols, vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(1, numSelectedRows);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] data1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal64Compare2)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    FieldExpr *eqLeft = new FieldExpr(0, Decimal64Type(8, 0));
    LiteralExpr *eqRight = new LiteralExpr(0, Decimal64Type(8, 1));
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());

    FieldExpr *ltLeft = new FieldExpr(0, Decimal64Type(8, 0));
    LiteralExpr *ltRight = new LiteralExpr(0, Decimal64Type(8, 1));
    BinaryExpr *ltExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, ltLeft, ltRight, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::OR, eqExpr, ltExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, Decimal64Type(8, 0));
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int32_t numRows = 10;
    int64_t *data1 = MakeLongs(numRows, -5);

    std::vector<DataTypePtr> vecOfTypes = { LongType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal64Compare2");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, data1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(filter, projections, numCols, vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(6, numSelectedRows);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] data1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal128Compare1)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    LiteralExpr *lteRight = new LiteralExpr(new std::string("500000"), Decimal128Type(38, 1));
    FieldExpr *lteLetf = new FieldExpr(0, Decimal128Type(38, 0));
    BinaryExpr *lteExpr = new BinaryExpr(omniruntime::expressions::Operator::LTE, lteLetf, lteRight, BooleanType());

    LiteralExpr *gteRight = new LiteralExpr(new std::string("500000"), Decimal128Type(38, 1));
    FieldExpr *gteLetf = new FieldExpr(0, Decimal128Type(38, 0));
    BinaryExpr *gteExpr = new BinaryExpr(omniruntime::expressions::Operator::GTE, gteLetf, gteRight, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, lteExpr, gteExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int32_t numRows = 1000;
    int64_t *data1 = new int64_t[numRows * 2];
    for (int32_t i = 0; i < numRows; i++) {
        data1[2 * i] = (i + 1) * 1000;
        data1[2 * i + 1] = 0;
    }

    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal128Compare1");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, data1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(filter, projections, numCols, vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(1, numSelectedRows);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] data1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal128Compare2)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    LiteralExpr *ltRight = new LiteralExpr(new std::string("50000"), Decimal128Type(38, 1));
    FieldExpr *ltLetf = new FieldExpr(0, Decimal128Type(38, 0));
    BinaryExpr *ltExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, ltLetf, ltRight, BooleanType());

    LiteralExpr *eqRight = new LiteralExpr(new std::string("50000"), Decimal128Type(38, 1));
    FieldExpr *eqLetf = new FieldExpr(0, Decimal128Type(38, 0));
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLetf, eqRight, BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::OR, ltExpr, eqExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 1;
    const int32_t numRows = 10;
    int64_t *data1 = new int64_t[numRows * 2];
    for (int32_t i = 0; i < numRows; i++) {
        data1[2 * i] = (i + 1) * 1000;
        data1[2 * i + 1] = 0;
    }

    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal128Compare2");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, data1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(filter, projections, numCols, vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(5, numSelectedRows);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] data1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, IntArith)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto *addLeft = new FieldExpr(0, IntType());
    auto *addRight = new LiteralExpr(1, IntType());
    auto *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    auto *subLeft = new FieldExpr(1, IntType());
    auto *subRight = new LiteralExpr(1, IntType());
    auto *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, IntType());

    auto *mulLeft = new FieldExpr(2, IntType());
    auto *mulRight = new LiteralExpr(2, IntType());
    auto *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, IntType());

    auto *divLeft = new FieldExpr(3, IntType());
    auto *divRight = new LiteralExpr(2, IntType());
    auto *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, divLeft, divRight, IntType());

    std::vector<Expr *> exprs = { addExpr, subExpr, mulExpr, divExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }
    const int32_t numCols = 4;
    const int32_t numRows = 10;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i - 1;
    }
    int32_t *col2 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = i + 1;
    }
    int32_t *col3 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col3[i] = i;
    }
    int32_t *col4 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col4[i] = i * 2;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ IntType(), IntType(), IntType(), IntType() }));
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_IntArith");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2, col3, col4);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int32_t i = 0; i < numRows; i++) {
        int32_t val0 = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i);
        int32_t val1 = ((IntVector *)ret->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, i);
        int32_t val2 = ((IntVector *)ret->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, i * 2);
        int32_t val3 = ((IntVector *)ret->GetVector(3))->GetValue(i);
        EXPECT_EQ(val3, i);
    }
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, LongArith)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto *addLeft = new FieldExpr(0, LongType());
    auto *addRight = new LiteralExpr(1L, LongType());
    auto *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, LongType());

    auto *subLeft = new FieldExpr(1L, LongType());
    auto *subRight = new LiteralExpr(1L, LongType());
    auto *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, LongType());

    auto *mulLeft = new FieldExpr(2L, LongType());
    auto *mulRight = new LiteralExpr(2L, LongType());
    auto *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, LongType());

    auto *divLeft = new FieldExpr(3L, LongType());
    auto *divRight = new LiteralExpr(2L, LongType());
    auto *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, divLeft, divRight, LongType());

    std::vector<Expr *> exprs = { addExpr, subExpr, mulExpr, divExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }
    const int32_t numCols = 4;
    const int32_t numRows = 10;
    int64_t *col1 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i - 1;
    }
    int64_t *col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = i + 1;
    }
    int64_t *col3 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col3[i] = i;
    }
    int64_t *col4 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col4[i] = i * 2;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ LongType(), LongType(), LongType(), LongType() }));
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_IntArith");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2, col3, col4);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int32_t i = 0; i < numRows; i++) {
        int64_t val0 = ((LongVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i);
        int64_t val1 = ((LongVector *)ret->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, i);
        int64_t val2 = ((LongVector *)ret->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, i * 2);
        int64_t val3 = ((LongVector *)ret->GetVector(3))->GetValue(i);
        EXPECT_EQ(val3, i);
    }
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, DoubleArith)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto *addLeft = new FieldExpr(0, DoubleType());
    auto *addRight = new LiteralExpr(1.0, DoubleType());
    auto *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, DoubleType());

    auto *subLeft = new FieldExpr(1.0, DoubleType());
    auto *subRight = new LiteralExpr(1.0, DoubleType());
    auto *subExpr = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, DoubleType());

    auto *mulLeft = new FieldExpr(2.0, DoubleType());
    auto *mulRight = new LiteralExpr(2.0, DoubleType());
    auto *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, DoubleType());

    auto *divLeft = new FieldExpr(3.0, DoubleType());
    auto *divRight = new LiteralExpr(2.0, DoubleType());
    auto *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, divLeft, divRight, DoubleType());

    std::vector<Expr *> exprs = { addExpr, subExpr, mulExpr, divExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }
    const int32_t numCols = 4;
    const int32_t numRows = 10;
    double *col1 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i - 1.0;
    }
    double *col2 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = i + 1.0;
    }
    double *col3 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col3[i] = i;
    }
    double *col4 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col4[i] = i * 2.0;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ DoubleType(), DoubleType(), DoubleType(), DoubleType() }));
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_IntArith");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2, col3, col4);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int32_t i = 0; i < numRows; i++) {
        double val0 = ((DoubleVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, i);
        double val1 = ((DoubleVector *)ret->GetVector(1))->GetValue(i);
        EXPECT_EQ(val1, i);
        double val2 = ((DoubleVector *)ret->GetVector(2))->GetValue(i);
        EXPECT_EQ(val2, i * 2.0);
        double val3 = ((DoubleVector *)ret->GetVector(3))->GetValue(i);
        EXPECT_EQ(val3, i);
    }
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete[] col4;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal64Arith1)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto subLeft = new FieldExpr(0, Decimal64Type(7, 3));
    auto subRight = new LiteralExpr(123468L, Decimal64Type(6, 4));
    BinaryExpr *subExpr =
        new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, Decimal64Type(8, 4));
    std::vector<Expr *> exprs = { subExpr };
    Filter *filter = nullptr;
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 1;
    const int32_t numCols = 1;
    auto col1 = new int64_t[numRows];
    col1[0] = 4321563;
    std::vector<DataTypePtr> vecOfTypes = { Decimal64Type(7, 3) };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal64Arith1");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    int64_t val0 = ((LongVector *)ret->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 43092162);

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal64Arith2)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto mulLeft = new FieldExpr(0, Decimal64Type(7, 2));
    auto mulRight = new LiteralExpr(100L, Decimal64Type(7, 2));
    BinaryExpr *mulExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, Decimal64Type(7, 4));

    std::vector<Expr *> exprs = { mulExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 1;
    const int32_t numCols = 1;
    auto col1 = new int64_t[numRows];
    col1[0] = 100;
    std::vector<DataTypePtr> vecOfTypes = { Decimal64Type(7, 2) };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal64Arith2");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    int64_t val0 = ((LongVector *)ret->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 10000L);

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal64Arith3)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto left = new FieldExpr(0, Decimal64Type(4, 2));
    auto right = new LiteralExpr(125L, Decimal64Type(3, 2));

    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV, left, right, Decimal64Type(2, 1));

    std::vector<Expr *> exprs = { divExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 1;
    const int32_t numCols = 1;
    auto col1 = new int64_t[numRows];
    col1[0] = 1225;
    std::vector<DataTypePtr> vecOfTypes = { Decimal64Type(4, 2) };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal64Arith3");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    int64_t val0 = ((LongVector *)ret->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 98L);

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal64Arith4)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto left = new FieldExpr(0, Decimal64Type(5, 3));
    auto right = new LiteralExpr(125L, Decimal64Type(3, 2));

    BinaryExpr *modExpr = new BinaryExpr(omniruntime::expressions::Operator::MOD, left, right, Decimal64Type(4, 3));

    std::vector<Expr *> exprs = { modExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 1;
    const int32_t numCols = 1;
    auto col1 = new int64_t[numRows];
    col1[0] = 12250;
    std::vector<DataTypePtr> vecOfTypes = { Decimal64Type(5, 3) };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal64Arith4");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    int64_t val0 = ((LongVector *)ret->GetVector(0))->GetValue(0);
    EXPECT_EQ(val0, 1000L);

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal128Arith1)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    FieldExpr *addLeft = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *addRight = new LiteralExpr(new std::string("20"), Decimal128Type(38, 1));
    BinaryExpr *addExpr =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, Decimal128Type(38, 1));

    std::vector<Expr *> exprs = { addExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }
    const int32_t numRows = 10;
    int64_t *col1 = MakeDecimals(numRows);
    const int32_t numCols = 1;
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal128Arith1");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i * 10 + 20);
    }
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal128Arith2)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    FieldExpr *subLeft = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *subRight = new LiteralExpr(new string("1"), Decimal128Type(38, 1));
    BinaryExpr *subExpr =
        new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, Decimal128Type(38, 1));

    std::vector<Expr *> exprs = { subExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }
    const int32_t numRows = 10;
    int64_t *col0 = MakeDecimals(numRows, -5);
    const int32_t numCols = 1;
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal128Arith2");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col0);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret->GetVector(0))->GetValue(i);
        Decimal128 old0 = ((Decimal128Vector *)vecBatch->GetVector(0))->GetValue(i);
        if (i <= 5) {
            EXPECT_EQ(val0.HighBits(), 1LL << 63);
            EXPECT_EQ(val0.LowBits() - 1, old0.LowBits() * 10);
        } else {
            EXPECT_EQ(val0.HighBits(), 0);
            EXPECT_EQ(val0.LowBits() - 1, old0.LowBits() * 10 - 2);
        }
    }
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col0;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal128Arith3)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    FieldExpr *mulLeft = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *mulRight = new LiteralExpr(new std::string("3"), Decimal128Type(38, 1));
    BinaryExpr *mulExpr =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, Decimal128Type(38, 1));

    std::vector<Expr *> exprs = { mulExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }
    const int32_t numRows = 10;
    int64_t *col1 = MakeDecimals(numRows);
    const int32_t numCols = 1;
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal128Arith3");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i * 3);
    }
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal128Arith4)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    LiteralExpr *divRight = new LiteralExpr(new std::string("20"), Decimal128Type(38, 0));
    BinaryExpr *divExpr = new BinaryExpr(omniruntime::expressions::Operator::DIV,
        new FieldExpr(0, Decimal128Type(38, 0)), divRight, Decimal128Type(38, 0));
    std::vector<Expr *> exprs = { divExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }
    const int32_t numRows = 10;
    int64_t *col1 = MakeDecimals(numRows);
    const int32_t numCols = 1;
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal128Arith4");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int32_t i = 0; i < numRows; i++) {
        Decimal128 val0 = ((Decimal128Vector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0.HighBits(), 0);
        EXPECT_EQ(val0.LowBits(), i / 20);
    }
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}
