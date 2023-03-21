/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: ifExpr SwitchExpr batch codegen test
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

TEST(BatchCodeGenTest, IntIf)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
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

    auto *projExpr = new FieldExpr(0, IntType());
    auto *projExpr1 = new FieldExpr(1, IntType());
    auto *projExpr2 = new FieldExpr(2, IntType());
    std::vector<Expr *> exprs = { projExpr, projExpr1, projExpr2 };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

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
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_IntIf");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(numSelectedRows, 2000);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, LongIf)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    BinaryExpr *condition = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, LongType()),
        new LiteralExpr(0, LongType()), BooleanType());
    BinaryExpr *texp = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(1, LongType()),
        new LiteralExpr(3L, LongType()), BooleanType());
    BinaryExpr *fexp = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(1, LongType()),
        new LiteralExpr(4L, LongType()), BooleanType());
    IfExpr *ifExpr = new IfExpr(condition, texp, fexp);

    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, new FieldExpr(2, IntType()),
        new LiteralExpr(3L, LongType()), BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, ifExpr, gtExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, LongType());
    auto *projExpr1 = new FieldExpr(1, LongType());
    auto *projExpr2 = new FieldExpr(2, LongType());
    std::vector<Expr *> exprs = { projExpr, projExpr1, projExpr2 };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 3;
    const int32_t numRows = 10000;
    int64_t *col1 = new int64_t[numRows];
    int64_t *col2 = new int64_t[numRows];
    int64_t *col3 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = i % 10;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ LongType(), LongType(), LongType() }));
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_LongIf");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(numSelectedRows, 1500);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, DoubleIf)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    BinaryExpr *condition = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(0, DoubleType()),
        new LiteralExpr(0, DoubleType()), BooleanType());
    BinaryExpr *texp = new BinaryExpr(omniruntime::expressions::Operator::LT, new FieldExpr(1, DoubleType()),
        new LiteralExpr(3.0, DoubleType()), BooleanType());
    BinaryExpr *fexp = new BinaryExpr(omniruntime::expressions::Operator::EQ, new FieldExpr(1, DoubleType()),
        new LiteralExpr(4.0, DoubleType()), BooleanType());
    IfExpr *ifExpr = new IfExpr(condition, texp, fexp);

    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, new FieldExpr(2, DoubleType()),
        new LiteralExpr(3.0, DoubleType()), BooleanType());

    BinaryExpr *filterExpr = new BinaryExpr(omniruntime::expressions::Operator::AND, ifExpr, gtExpr, BooleanType());

    auto *projExpr = new FieldExpr(0, DoubleType());
    auto *projExpr1 = new FieldExpr(1, DoubleType());
    auto *projExpr2 = new FieldExpr(2, DoubleType());
    std::vector<Expr *> exprs = { projExpr, projExpr1, projExpr2 };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 3;
    const int32_t numRows = 10000;
    double *col1 = new double[numRows];
    double *col2 = new double[numRows];
    double *col3 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
        col2[i] = i % 5;
        col3[i] = i % 10;
    }

    DataTypes inputTypes(std::vector<DataTypePtr>({ DoubleType(), DoubleType(), DoubleType() }));
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_DoubleIf");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2, col3);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(numSelectedRows, 2000);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete[] col2;
    delete[] col3;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, StringIf)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, VarcharType()));
    args.push_back(new LiteralExpr(new std::string("hello"), VarcharType()));
    args.push_back(new LiteralExpr(new std::string("bye"), VarcharType()));
    args.push_back(new LiteralExpr(new std::string("okay"), VarcharType()));

    InExpr *filterExpr = new InExpr(args);

    auto *projExpr = new FieldExpr(0, VarcharType());
    std::vector<Expr *> exprs = { projExpr };

    auto filter = make_unique<Filter>(*filterExpr, nullptr);
    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), true, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }
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
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_AllType");
    std::vector<Vector *> cols = { CreateVarcharVector(strings, nulls) };
    auto *vecBatch = CreateVectorBatch(numRows, cols);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, nullptr);
    EXPECT_EQ(numSelectedRows, 4);

    Expr::DeleteExprs({ filterExpr });
    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal64If)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto condition = new LiteralExpr(true, BooleanType());
    auto v1 = new LiteralExpr(123400L, Decimal64Type(7, 3));
    v1->isNull = true;
    auto v2 = new FieldExpr(0, Decimal64Type(7, 3));
    auto coalesce = new CoalesceExpr(v1, v2);

    auto falseExpr = new LiteralExpr(1234000L, Decimal64Type(7, 3));
    auto ifExpr = new IfExpr(condition, coalesce, falseExpr);

    auto subLeft = new LiteralExpr(12340L, Decimal64Type(7, 3));
    auto subRight = new LiteralExpr(1010L, Decimal64Type(7, 3));
    auto right = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, Decimal64Type(7, 3));
    auto expr = new BinaryExpr(omniruntime::expressions::Operator::GT, ifExpr, right, BooleanType());

    std::vector<Expr *> exprs = { expr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numRows = 1;
    const int32_t numCols = 1;
    auto col1 = new int64_t[numRows];
    col1[0] = 1234;
    std::vector<DataTypePtr> vecOfTypes = { Decimal64Type(7, 3) };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal64If");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    bool val0 = ((BooleanVector *)ret->GetVector(0))->GetValue(0);
    EXPECT_FALSE(val0);

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal128If)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    auto condition = new LiteralExpr(true, BooleanType());
    auto v1 = new LiteralExpr(new std::string("123400"), Decimal128Type(7, 2));
    v1->isNull = true;
    auto v2 = new FieldExpr(0, Decimal128Type(7, 2));
    auto coalesce = new CoalesceExpr(v1, v2);

    auto falseExpr = new LiteralExpr(new std::string("1234000"), Decimal128Type(7, 2));
    auto ifExpr = new IfExpr(condition, coalesce, falseExpr);
    auto right = new LiteralExpr(new std::string("1234"), Decimal128Type(4, 2));
    auto expr = new BinaryExpr(omniruntime::expressions::Operator::GT, ifExpr, right, BooleanType());

    std::vector<Expr *> exprs = { expr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }
    const int32_t numRows = 1;
    const int32_t numCols = 1;
    auto col1 = new int64_t[2];
    col1[0] = 1234;
    col1[1] = 0;
    std::vector<DataTypePtr> vecOfTypes = { Decimal128Type(7, 2) };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("filter_Decimal128If");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1);

    int32_t numSelectedRows = 0;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);
    bool val0 = ((BooleanVector *)ret->GetVector(0))->GetValue(0);
    EXPECT_FALSE(val0);

    Expr::DeleteExprs(exprs);
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete[] col1;
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, IntSwitch)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    FieldExpr *gtLeft = new FieldExpr(1, LongType());
    LiteralExpr *gtRight = new LiteralExpr(3000000000L, LongType());
    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft, gtRight, BooleanType());

    FieldExpr *gtLeft1 = new FieldExpr(1, LongType());
    LiteralExpr *gtRight1 = new LiteralExpr(3000000001L, LongType());
    BinaryExpr *gtExpr1 = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft1, gtRight1, BooleanType());

    FieldExpr *gtLeft2 = new FieldExpr(1, LongType());
    LiteralExpr *gtRight2 = new LiteralExpr(3000000002L, LongType());
    BinaryExpr *gtExpr2 = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft2, gtRight2, BooleanType());

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(10, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    FieldExpr *addLeft1 = new FieldExpr(0, IntType());
    LiteralExpr *addRight1 = new LiteralExpr(10, IntType());
    BinaryExpr *addExpr1 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft1, addRight1, IntType());

    FieldExpr *addLeft2 = new FieldExpr(0, IntType());
    LiteralExpr *addRight2 = new LiteralExpr(10, IntType());
    auto *addExpr2 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft2, addRight2, IntType());

    FieldExpr *mulLeft = new FieldExpr(0, IntType());
    LiteralExpr *mulRight = new LiteralExpr(-1, IntType());
    BinaryExpr *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, IntType());

    std::vector<std::pair<Expr *, Expr *>> whenClause;
    std::pair<Expr *, Expr *> when1;
    std::pair<Expr *, Expr *> when2;
    std::pair<Expr *, Expr *> when3;
    when1.first = gtExpr;
    when1.second = addExpr;
    when2.first = gtExpr1;
    when2.second = addExpr1;
    when3.first = gtExpr2;
    when3.second = addExpr2;
    whenClause.push_back(when1);
    whenClause.push_back(when2);
    whenClause.push_back(when3);

    SwitchExpr *switchExpr = new SwitchExpr(whenClause, mulExpr);

    std::vector<Expr *> exprs = { switchExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 2;
    const int32_t numRows = 10;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t *col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = (i % 2 != 0) ? 4000000000 : 12;
    }

    std::vector<DataTypePtr> vecOfTypes = { IntType(), LongType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_IntSwitch");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int i = 0; i < numRows; ++i) {
        int32_t val0 = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, (i % 2 == 0) ? -i : i + 10);
    }

    Expr::DeleteExprs(exprs);
    delete[] col1;
    delete[] col2;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, DoubleSwitch)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    FieldExpr *gtLeft = new FieldExpr(1, DoubleType());
    LiteralExpr *gtRight = new LiteralExpr(100.0, DoubleType());
    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, gtLeft, gtRight, BooleanType());

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(1, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    FieldExpr *mulLeft = new FieldExpr(0, IntType());
    LiteralExpr *mulRight = new LiteralExpr(-1, IntType());
    BinaryExpr *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, IntType());

    std::vector<std::pair<Expr *, Expr *>> whenClause;
    std::pair<Expr *, Expr *> when;
    when.first = gtExpr;
    when.second = addExpr;
    whenClause.push_back(when);

    SwitchExpr *switchExpr = new SwitchExpr(whenClause, mulExpr);

    std::vector<Expr *> exprs = { switchExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 2;
    const int32_t numRows = 10;

    double *col2 = new double[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = i % 2 == 0 ? 1.0 : 100.0;
    }

    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    std::vector<DataTypePtr> vecOfTypes = { IntType(), DoubleType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_DoubleSwitch");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int i = 0; i < numRows; ++i) {
        int32_t val0 = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, (i % 2 == 0) ? -i : 1 + i);
    }

    Expr::DeleteExprs(exprs);
    delete[] col1;
    delete[] col2;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, StringSwitch)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    FieldExpr *eqLeft = new FieldExpr(1, VarcharType());
    LiteralExpr *eqRight = new LiteralExpr(new std::string("hello"), VarcharType());
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(1, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    FieldExpr *mulLeft = new FieldExpr(0, IntType());
    LiteralExpr *mulRight = new LiteralExpr(-1, IntType());
    BinaryExpr *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, IntType());

    std::vector<std::pair<Expr *, Expr *>> whenClause;
    std::pair<Expr *, Expr *> when;
    when.first = eqExpr;
    when.second = addExpr;
    whenClause.push_back(when);

    SwitchExpr *switchExpr = new SwitchExpr(whenClause, mulExpr);

    std::vector<Expr *> exprs = { switchExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 2;
    const int32_t numRows = 10;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    string *col2 = new string[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            col2[i] = "hello";
        } else {
            col2[i] = "hi";
        }
    }

    std::vector<DataTypePtr> vecOfTypes = { IntType(), VarcharType(10) };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_StringSwitch");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int i = 0; i < numRows; ++i) {
        int32_t val0 = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, (i % 2 == 0) ? 1 + i : -i);
    }

    Expr::DeleteExprs(exprs);
    delete[] col1;
    delete[] col2;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal128Switch)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    FieldExpr *eqLeft = new FieldExpr(1, Decimal128Type(38, 0));
    LiteralExpr *eqRight = new LiteralExpr(new std::string("100"), Decimal128Type(38, 0));
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(1, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    FieldExpr *mulLeft = new FieldExpr(0, IntType());
    LiteralExpr *mulRight = new LiteralExpr(-1, IntType());
    BinaryExpr *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, IntType());

    std::vector<std::pair<Expr *, Expr *>> whenClause;
    std::pair<Expr *, Expr *> when;
    when.first = eqExpr;
    when.second = addExpr;
    whenClause.push_back(when);

    SwitchExpr *switchExpr = new SwitchExpr(whenClause, mulExpr);

    std::vector<Expr *> exprs = { switchExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 2;
    const int32_t numRows = 10;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    int64_t *col2 = new int64_t[numRows * 2];
    for (int32_t i = 0; i < numRows; i++) {
        if (i % 2 == 0) {
            col2[2 * i] = 100;
            col2[2 * i + 1] = 0;
        } else {
            col2[2 * i] = 0;
            col2[2 * i + 1] = 0;
        }
    }

    std::vector<DataTypePtr> vecOfTypes = { IntType(), Decimal128Type(38, 0) };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Decimal128Switch");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int i = 0; i < numRows; ++i) {
        int32_t val0 = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, (i % 2 == 0) ? 1 + i : -i);
    }

    Expr::DeleteExprs(exprs);
    delete[] col1;
    delete[] col2;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}

TEST(BatchCodeGenTest, Decimal64Switch)
{
    ConfigUtil::SetEnableBatchExprEvaluate(true);
    FieldExpr *eqLeft = new FieldExpr(1, Decimal64Type(8, 0));
    LiteralExpr *eqRight = new LiteralExpr(100L, Decimal64Type(8, 0));
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(1, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    FieldExpr *mulLeft = new FieldExpr(0, IntType());
    LiteralExpr *mulRight = new LiteralExpr(-1, IntType());
    BinaryExpr *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, IntType());

    std::vector<std::pair<Expr *, Expr *>> whenClause;
    std::pair<Expr *, Expr *> when;
    when.first = eqExpr;
    when.second = addExpr;
    whenClause.push_back(when);

    SwitchExpr *switchExpr = new SwitchExpr(whenClause, mulExpr);

    std::vector<Expr *> exprs = { switchExpr };
    Filter *filter = nullptr;

    std::vector<std::unique_ptr<Projection>> projections;
    for (uint32_t i = 0; i < exprs.size(); i++) {
        auto projection = make_unique<Projection>(*(exprs[i]), false, exprs[i]->GetReturnType(), nullptr);
        projections.push_back(move(projection));
    }

    const int32_t numCols = 2;
    const int32_t numRows = 10;
    int64_t *col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = i % 2 == 0 ? 100 : i;
    }

    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    std::vector<DataTypePtr> vecOfTypes = { IntType(), LongType() };
    DataTypes inputTypes(vecOfTypes);
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("project_Decimal64Switch");
    VectorBatch *vecBatch = CreateVectorBatch(inputTypes, numRows, col1, col2);

    int32_t numSelectedRows = numRows;
    auto ret = FilterAndProject(reinterpret_cast<unique_ptr<omniruntime::op::Filter> &>(filter), projections, numCols,
        vecBatch, numSelectedRows, vecAllocator);

    for (int i = 0; i < numRows; ++i) {
        int32_t val0 = ((IntVector *)ret->GetVector(0))->GetValue(i);
        EXPECT_EQ(val0, (i % 2 == 0) ? i + 1 : -i);
    }

    Expr::DeleteExprs(exprs);
    delete[] col1;
    delete[] col2;
    VectorHelper::FreeVecBatch(vecBatch);
    VectorHelper::FreeVecBatch(ret);
    delete vecAllocator;
    ConfigUtil::SetEnableBatchExprEvaluate(false);
}
