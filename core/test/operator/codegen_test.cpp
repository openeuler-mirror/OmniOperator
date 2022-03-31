/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "gtest/gtest.h"

#include <iostream>
#include <string>
#include <vector>
#include "expression/jsonparser/jsonparser.h"
#include "codegen/expression_codegen.h"
#include "codegen/filter_codegen.h"
#include "codegen/projection_codegen.h"
#include "operator/filter/filter_and_project.h"
#include "codegen/functions/murmur3_hash.h"
#include "../util/test_util.h"

using omniruntime::op::RowFilter;
using omniruntime::op::RowFilterFunc;
using omniruntime::op::RowProjection;
using omniruntime::op::RowProjFunc;
using namespace std;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace omniruntime::codegen;

const string defaultTestFunctionName = "test-function";

using FilterFunc = int32_t (*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
using ProjectFunc = int32_t (*)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *,
    int32_t *, int64_t, int64_t *);

// Filter is basically just a projection that must return a boolean.
// The logic for filtering out rows from final output is handled in C++ when
// processing each row individually, so to LLVM it is exactly the same as a projection.
// Check CodeGenTest.RowFilter for a dedicated test using the RowFilter class instead.
TEST(CodeGenTest, SimpleFilter)
{
    // create expression objects
    FieldExpr *lessThanLeft = new FieldExpr(0, IntType());
    LiteralExpr *lessThanRight = new LiteralExpr(50, IntType());
    BinaryExpr *lessThanExpr =
        new BinaryExpr(omniruntime::expressions::Operator::LT, lessThanLeft, lessThanRight, BooleanType());

    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT) };
    DataTypes types(vecOfTypes);
    const int numRows = 100;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    int64_t *table = new int64_t[numCols];
    table[0] = reinterpret_cast<int64_t>(col1);

    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }

    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows];
    }

    RowProjection rowProjection(*lessThanExpr);
    RowProjFunc func = rowProjection.Create();
    EXPECT_EQ(rowProjection.GetReturnType().GetId(), OMNI_BOOLEAN);

    int32_t *dataLength = new int32_t(0);
    int64_t dictionaries[numCols] = {};
    bool isNull;

    auto context = new ExecutionContext();
    for (int32_t i = 0; i < 50; i++) {
        bool res = *((bool *)func(table, (int64_t *)bitmap, (int64_t *)offsets, i, dataLength,
            reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_TRUE(res);
    }
    context->GetArena()->Reset();
    for (int32_t i = 50; i < 100; i++) {
        bool res = *((bool *)func(table, (int64_t *)bitmap, (int64_t *)offsets, i, dataLength,
            reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_FALSE(res);
    }
    context->GetArena()->Reset();
    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] col1;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
    delete dataLength;
    delete context;
}
// Simple project example using individual row processing.
TEST(CodeGenTest, SimpleProject)
{
    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(50, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT) };
    DataTypes types(vecOfTypes);

    const int numRows = 100;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    int64_t *table = new int64_t[numCols];
    table[0] = reinterpret_cast<int64_t>(col1);

    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }

    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows];
    }

    RowProjection rowProjection(*addExpr);
    RowProjFunc func = rowProjection.Create();
    EXPECT_EQ(rowProjection.GetReturnType().GetId(), OMNI_INT);
    bool isNull = false;
    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    int64_t dictionaries[numCols] = {};
    auto context = new ExecutionContext();
    for (int32_t i = 0; i < 100; i++) {
        int32_t res = *((int32_t *)func(table, (int64_t *)bitmap, (int64_t *)offsets, i, dataLength,
            reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_EQ(res, i + 50);
    }
    context->GetArena()->Reset();
    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] col1;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
    delete[] dataLength;
    delete context;
}

// Same logic as Single Project Test below except using switch expression to replace if expression
// A more complicated test for individual row projection
// when gtExpr then addExpr
// else mulExpr
TEST(CodeGenTest, SingleCaseSwitch)
{
    FieldExpr *gtLeft = new FieldExpr(1, LongType());
    LiteralExpr *gtRight = new LiteralExpr(3000000000L, LongType());
    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft, gtRight, BooleanType());

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(10, IntType());
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

    const int32_t numCols = 2;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);

    const int numRows = 10;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t *col2 = new int64_t[numRows];
    for (uint32_t i = 0; i < numRows; i++) {
        col2[i] = (i % 2 != 0) ? 4000000000 : 12;
    }

    int64_t *table = new int64_t[numCols];
    table[0] = reinterpret_cast<int64_t>(col1);
    table[1] = reinterpret_cast<int64_t>(col2);

    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }

    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows];
    }

    RowProjection rowProjection(*switchExpr);

    RowProjFunc func = rowProjection.Create();
    EXPECT_EQ(rowProjection.GetReturnType().GetId(), OMNI_INT);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    int64_t dictionaries[numCols] = {};
    auto context = new ExecutionContext();
    bool isNull = false;
    for (int32_t i = 0; i < numRows; i++) {
        int32_t res = *((int32_t *)func(table, (int64_t *)bitmap, (int64_t *)offsets, i, dataLength,
            reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_EQ(res, i % 2 ? i + 10 : -i);
    }
    context->GetArena()->Reset();
    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] col1;
    delete[] col2;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
    delete[] dataLength;
    delete context;
}

// whenClause with 2 cases
// when gtExpr then addExpr
// when ltExpr then addExpr
// else mulExpr
TEST(CodeGenTest, DoubleCaseSwitch)
{
    FieldExpr *gtLeft = new FieldExpr(1, LongType());
    LiteralExpr *gtRight = new LiteralExpr(3000000000L, LongType());
    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft, gtRight, BooleanType());

    FieldExpr *ltLeft = new FieldExpr(1, LongType());
    LiteralExpr *ltRight = new LiteralExpr(3000000000L, LongType());
    BinaryExpr *ltExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, ltLeft, ltRight, BooleanType());

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(10, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    FieldExpr *addLeft1 = new FieldExpr(0, IntType());
    LiteralExpr *addRight1 = new LiteralExpr(10, IntType());
    BinaryExpr *addExpr1 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft1, addRight1, IntType());

    FieldExpr *mulLeft = new FieldExpr(0, IntType());
    LiteralExpr *mulRight = new LiteralExpr(-1, IntType());
    BinaryExpr *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, IntType());

    std::vector<std::pair<Expr *, Expr *>> whenClause;
    std::pair<Expr *, Expr *> when1;
    std::pair<Expr *, Expr *> when2;
    when1.first = gtExpr;
    when1.second = addExpr;
    when2.first = ltExpr;
    when2.second = addExpr1;
    whenClause.push_back(when1);
    whenClause.push_back(when2);


    SwitchExpr *switchExpr = new SwitchExpr(whenClause, mulExpr);

    const int32_t numCols = 2;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);

    const int numRows = 10;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t *col2 = new int64_t[numRows];
    for (uint32_t i = 0; i < numRows; i++) {
        col2[i] = (i % 2 != 0) ? 4000000000 : 12;
    }

    int64_t *table = new int64_t[numCols];
    table[0] = reinterpret_cast<int64_t>(col1);
    table[1] = reinterpret_cast<int64_t>(col2);

    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }

    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows];
    }

    RowProjection rowProjection(*switchExpr);

    RowProjFunc func = rowProjection.Create();
    EXPECT_EQ(rowProjection.GetReturnType().GetId(), OMNI_INT);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    int64_t dictionaries[numCols] = {};
    auto context = new ExecutionContext();
    bool isNull = false;
    for (int32_t i = 0; i < numRows; i++) {
        int32_t res = *((int32_t *)func(table, (int64_t *)bitmap, (int64_t *)offsets, i, dataLength,
            reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_EQ(res, i + 10);
    }
    context->GetArena()->Reset();
    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] col1;
    delete[] col2;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
    delete[] dataLength;
    delete context;
}

// whenClause with 3 cases
// when gtExpr then addExpr
// when gtExpr1 then addExpr1
// when gtExpr2 then addExpr2
// else mulExpr
TEST(CodeGenTest, ThreeCaseSwitch)
{
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
    BinaryExpr *addExpr2 = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft2, addRight2, IntType());

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

    const int32_t numCols = 2;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);

    const int numRows = 10;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t *col2 = new int64_t[numRows];
    for (uint32_t i = 0; i < numRows; i++) {
        col2[i] = (i % 2 != 0) ? 4000000000 : 12;
    }

    int64_t *table = new int64_t[numCols];
    table[0] = reinterpret_cast<int64_t>(col1);
    table[1] = reinterpret_cast<int64_t>(col2);

    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }

    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows];
    }

    RowProjection rowProjection(*switchExpr);

    RowProjFunc func = rowProjection.Create();
    EXPECT_EQ(rowProjection.GetReturnType().GetId(), OMNI_INT);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    int64_t dictionaries[numCols] = {};
    auto context = new ExecutionContext();
    bool isNull = false;
    for (int32_t i = 0; i < numRows; i++) {
        int32_t res = *((int32_t *)func(table, (int64_t *)bitmap, (int64_t *)offsets, i, dataLength,
            reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_EQ(res, (i % 2 != 0) ? i + 10 : -i);
    }
    context->GetArena()->Reset();
    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] col1;
    delete[] col2;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
    delete[] dataLength;
    delete context;
}

TEST(CodeGenTest, SwitchElseNull)
{
    FieldExpr *gtLeft = new FieldExpr(1, LongType());
    LiteralExpr *gtRight = new LiteralExpr(3000000000L, LongType());
    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft, gtRight, BooleanType());

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(10, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    DataTypePtr destType = make_unique<DataType>(OMNI_INT);
    LiteralExpr *nullExpr = new LiteralExpr();
    nullExpr->isNull = true;
    nullExpr->dataType = std::move(destType);

    std::vector<std::pair<Expr *, Expr *>> whenClause;
    std::pair<Expr *, Expr *> when;
    when.first = gtExpr;
    when.second = addExpr;
    whenClause.push_back(when);


    SwitchExpr *switchExpr = new SwitchExpr(whenClause, nullExpr);

    const int32_t numCols = 2;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);

    const int numRows = 10;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t *col2 = new int64_t[numRows];
    for (uint32_t i = 0; i < numRows; i++) {
        col2[i] = (i % 2 != 0) ? 4000000000 : 12;
    }

    int64_t *table = new int64_t[numCols];
    table[0] = reinterpret_cast<int64_t>(col1);
    table[1] = reinterpret_cast<int64_t>(col2);

    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }

    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows];
    }

    RowProjection rowProjection(*switchExpr);

    RowProjFunc func = rowProjection.Create();
    EXPECT_EQ(rowProjection.GetReturnType().GetId(), OMNI_INT);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    int64_t dictionaries[numCols] = {};
    auto context = new ExecutionContext();
    bool isNull = false;
    for (int32_t i = 0; i < numRows; i++) {
        int32_t res = *((int32_t *)func(table, (int64_t *)bitmap, (int64_t *)offsets, i, dataLength,
            reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_EQ(res, i % 2 ? i + 10 : 0);
    }
    context->GetArena()->Reset();
    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] col1;
    delete[] col2;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
    delete[] dataLength;
    delete context;
}

TEST(CodeGenTest, SingleProject)
{
    FieldExpr *gtLeft = new FieldExpr(1, LongType());
    LiteralExpr *gtRight = new LiteralExpr(3000000000L, LongType());
    BinaryExpr *gtExpr = new BinaryExpr(omniruntime::expressions::Operator::GT, gtLeft, gtRight, BooleanType());

    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(10, IntType());
    BinaryExpr *addExpr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, IntType());

    FieldExpr *mulLeft = new FieldExpr(0, IntType());
    LiteralExpr *mulRight = new LiteralExpr(-1, IntType());
    BinaryExpr *mulExpr = new BinaryExpr(omniruntime::expressions::Operator::MUL, mulLeft, mulRight, IntType());

    IfExpr *ifExpr = new IfExpr(gtExpr, addExpr, mulExpr);

    const int32_t numCols = 2;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);

    const int numRows = 1000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t *col2 = new int64_t[numRows];
    for (uint32_t i = 0; i < numRows; i++) {
        col2[i] = (i % 2 != 0) ? 4000000000 : 12;
    }

    int64_t *table = new int64_t[numCols];
    table[0] = reinterpret_cast<int64_t>(col1);
    table[1] = reinterpret_cast<int64_t>(col2);

    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }

    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows];
    }

    RowProjection rowProjection(*ifExpr);
    RowProjFunc func = rowProjection.Create();
    EXPECT_EQ(rowProjection.GetReturnType().GetId(), OMNI_INT);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    int64_t dictionaries[numCols] = {};
    auto context = new ExecutionContext();
    bool isNull = false;
    for (int32_t i = 0; i < numRows; i++) {
        int32_t res = *((int32_t *)func(table, (int64_t *)bitmap, (int64_t *)offsets, i, dataLength,
            reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_EQ(res, i % 2 ? i + 10 : -i);
    }
    context->GetArena()->Reset();
    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] col1;
    delete[] col2;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
    delete[] dataLength;
    delete context;
}

// Test the short circuit functionality in the case that the projection is a column index.
TEST(CodeGenTest, ShortCircuitProject)
{
    FieldExpr *colExpr1 = new FieldExpr(1, LongType());

    const int32_t numCols = 2;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);

    const int numRows = 1000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t *col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = i % 10;
    }

    int64_t *table = new int64_t[numCols];
    table[0] = reinterpret_cast<int64_t>(col1);
    table[1] = reinterpret_cast<int64_t>(col2);

    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }

    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows];
    }

    RowProjection rowProjection(*colExpr1);
    RowProjFunc func = rowProjection.Create();
    EXPECT_TRUE(rowProjection.IsColumnProjection());
    EXPECT_EQ(rowProjection.GetIndexIfColumnProjection(), 1);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    int64_t dictionaries[numCols] = {};
    auto context = new ExecutionContext();
    bool isNull = false;
    for (int32_t i = 0; i < numRows; i++) {
        isNull = false;
        int32_t res = *((int32_t *)func(table, (int64_t *)bitmap, (int64_t *)offsets, i, dataLength,
            reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_EQ(res, i % 10);
    }
    context->GetArena()->Reset();
    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] col1;
    delete[] col2;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
    delete[] dataLength;
    delete context;
}

// Test the row filter
TEST(CodeGenTest, RowFilter)
{
    FieldExpr *equalLeft = new FieldExpr(0, IntType());
    LiteralExpr *equalRight = new LiteralExpr(0, IntType());
    BinaryExpr *equalExpr =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, equalLeft, equalRight, BooleanType());

    const int32_t numCols = 1;
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT) };
    DataTypes DataTypes(vecOfTypes);

    const int numRows = 1000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
    }
    int64_t *table = new int64_t[numCols];
    table[0] = reinterpret_cast<int64_t>(col1);

    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }

    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows];
    }
    int64_t dictionaryVectors[numCols] = {};
    auto filter = new RowFilter(*equalExpr);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    auto context = new ExecutionContext();
    for (int32_t i = 0; i < numRows; i++) {
        bool res = filterFunc(table, (int64_t *)bitmap, (int64_t *)offsets, i, reinterpret_cast<int64_t>(context),
            dictionaryVectors);
        EXPECT_EQ(res, i % 2 == 0);
    }
    context->GetArena()->Reset();
    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] col1;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
    delete filter;
    delete context;
}

TEST(CodeGenTest, RowFilterString)
{
    std::vector<DataType> vecOfTypes = { DataType(OMNI_VARCHAR), DataType(OMNI_VARCHAR) };
    DataTypes dataTypes(vecOfTypes);
    const int32_t numCols = 2;
    const int32_t numRows = 1;

    string s1[1];
    string s2[1];

    s1[0] = "hello world";
    s2[0] = "world hello";
    int64_t *vals = new int64_t[2];
    vals[0] = reinterpret_cast<int64_t>(s1->c_str());
    vals[1] = reinterpret_cast<int64_t>(s2->c_str());
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }
    int64_t dictionaryVectors[numCols] = {};

    auto context = new ExecutionContext();
    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows + 1];
        offsets[col][0] = 0;
        offsets[col][1] = 11;
    }
    auto substrCol = new FieldExpr(0, VarcharType());
    auto substrIndex = new LiteralExpr(1, IntType());
    auto substrLen = new LiteralExpr(5, IntType());
    std::vector<Expr *> args;
    args.push_back(substrCol);
    args.push_back(substrIndex);
    args.push_back(substrLen);
    DataTypePtr retType = VarcharType();
    std::string funcStr = "substr";

    auto substrExpr = GetFuncExpr(funcStr, args, VarcharType());
    auto helloExpr = new LiteralExpr(new std::string("hello"), VarcharType(6));
    auto eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, substrExpr, helloExpr, BooleanType());

    auto filter = new RowFilter(*eqExpr);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);

    bool res = filterFunc(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, reinterpret_cast<int64_t>(context),
        dictionaryVectors);
    EXPECT_EQ(res, true);
    delete filter;

    auto substrCol2 = new FieldExpr(1, VarcharType());
    auto *substrIndex2 = new LiteralExpr(1, IntType());
    auto substrLen2 = new LiteralExpr(5, IntType());
    std::vector<Expr *> args2;
    args2.push_back(substrCol2);
    args2.push_back(substrIndex2);
    args2.push_back(substrLen2);
    auto substrExpr2 = GetFuncExpr(funcStr, args2, VarcharType());

    auto helloExpr2 = new LiteralExpr(new std::string("hello"), VarcharType(6));

    auto eqExpr2 = new BinaryExpr(omniruntime::expressions::Operator::EQ, substrExpr2, helloExpr2, BooleanType());
    filter = new RowFilter(*eqExpr2);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);

    res = filterFunc(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, reinterpret_cast<int64_t>(context),
        dictionaryVectors);
    EXPECT_EQ(res, false);

    delete filter;

    auto substrCol3 = new FieldExpr(0, VarcharType());
    auto substrIndex3 = new LiteralExpr(1, IntType());
    auto substrLen3 = new LiteralExpr(5, IntType());
    std::vector<Expr *> args3;
    args3.push_back(substrCol3);
    args3.push_back(substrIndex3);
    args3.push_back(substrLen3);
    auto substrExpr3 = GetFuncExpr(funcStr, args3, VarcharType());

    auto substrCol4 = new FieldExpr(1, VarcharType());
    auto substrIndex4 = new LiteralExpr(7, IntType());
    auto substrLen4 = new LiteralExpr(11, IntType());
    std::vector<Expr *> args4;
    args4.push_back(substrCol4);
    args4.push_back(substrIndex4);
    args4.push_back(substrLen4);
    auto substrExpr4 = GetFuncExpr(funcStr, args4, VarcharType());

    auto eqExpr3 = new BinaryExpr(omniruntime::expressions::Operator::EQ, substrExpr3, substrExpr4, BooleanType());
    filter = new RowFilter(*eqExpr3);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);

    res = filterFunc(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, reinterpret_cast<int64_t>(context),
        dictionaryVectors);
    EXPECT_EQ(res, true);
    context->GetArena()->Reset();
    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] selected;
    delete[] vals;
    delete[] bitmap;
    delete[] offsets;
    delete filter;
    delete context;
}

TEST(CodeGenTest, Operators1)
{
    // create expression objects
    FieldExpr *addLeft = new FieldExpr(0, IntType());
    LiteralExpr *addRight = new LiteralExpr(2, IntType());

    BinaryExpr *gteExpr = new BinaryExpr(omniruntime::expressions::Operator::GTE, addLeft, addRight, BooleanType());

    FieldExpr *ltLeft = new FieldExpr(1, IntType());
    LiteralExpr *ltRight = new LiteralExpr(4, IntType());
    BinaryExpr *ltExpr = new BinaryExpr(omniruntime::expressions::Operator::LT, ltLeft, ltRight, BooleanType());

    FieldExpr *eqLeft = new FieldExpr(2, IntType());
    LiteralExpr *eqRight = new LiteralExpr(2, IntType());
    BinaryExpr *eqExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, eqLeft, eqRight, BooleanType());

    BinaryExpr *andLeft = new BinaryExpr(omniruntime::expressions::Operator::AND, ltExpr, eqExpr, BooleanType());

    BinaryExpr *expr = new BinaryExpr(omniruntime::expressions::Operator::AND, gteExpr, andLeft, BooleanType());


    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_INT), DataType(OMNI_INT) };
    DataTypes types(vecOfTypes);

    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {2};
    int32_t v2[1] = {3};
    int32_t v3[1] = {2};
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }

    int64_t dictionaries[3] = {};
    auto context = new ExecutionContext();
    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);

    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    // number of rows that passed filter
    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);

    v1[0] = 2;
    v2[0] = 4;
    v3[0] = 2;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    for (int i = 0; i < 3; i++) {
        delete[] offsets[i];
    }

    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    codegen.reset();
    delete expr;
    delete context;
}

TEST(CodeGenTest, MathFunctions1)
{
    // create the expression objects
    FieldExpr *col01 = new FieldExpr(0, IntType());
    FieldExpr *col2 = new FieldExpr(2, IntType());
    DataTypePtr retType = IntType();
    std::string funcStr = "abs";

    std::vector<Expr *> args1;
    args1.push_back(col01);
    auto abs1 = GetFuncExpr(funcStr, args1, IntType());
    std::vector<Expr *> args2;
    args2.push_back(col2);
    auto abs2 = GetFuncExpr(funcStr, args2, IntType());
    auto eq1 = new BinaryExpr(omniruntime::expressions::Operator::EQ, abs1, abs2, BooleanType());

    auto col02 = new FieldExpr(0, IntType());
    auto col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> args3;
    std::vector<Expr *> args4;
    args3.push_back(col02);
    auto abs3 = GetFuncExpr(funcStr, args3, IntType());
    args4.push_back(col1);
    auto abs4 = GetFuncExpr(funcStr, args4, IntType());
    auto eq2 = new BinaryExpr(omniruntime::expressions::Operator::EQ, abs3, abs4, BooleanType());
    auto expr = new BinaryExpr(omniruntime::expressions::Operator::AND, eq1, eq2, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_INT), DataType(OMNI_INT) };
    DataTypes types(vecOfTypes);

    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {-123};
    int32_t v2[1] = {123};
    int32_t v3[1] = {123};
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }
    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);

    int64_t dictionaries[3] = {};
    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);

    context->GetArena()->Reset();

    EXPECT_EQ(result, 1);
    std::cout << "result: " << result << std::endl;

    v1[0] = 10000;
    v2[0] = 10000;
    v3[0] = -10001;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);

    context->GetArena()->Reset();
    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    codegen.reset();
    delete expr;
    delete context;
}

TEST(CodeGenTest, MathFunctions2)
{
    // create expression
    FieldExpr *valueExpr = new FieldExpr(1, IntType());
    FieldExpr *lowerExpr = new FieldExpr(0, IntType());
    FieldExpr *upperExpr = new FieldExpr(2, IntType());
    BetweenExpr *expr = new BetweenExpr(valueExpr, lowerExpr, upperExpr);

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_INT), DataType(OMNI_INT) };
    DataTypes types(vecOfTypes);

    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {1001};
    int32_t v2[1] = {1001};
    int32_t v3[1] = {1001};
    int64_t *vals = new int64_t[3];

    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);

    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = 12356;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    codegen.reset();
    delete expr;
    delete context;
}

TEST(CodeGenTest, MathFunctions3)
{
    // create expression objects
    FieldExpr *col01 = new FieldExpr(0, IntType());
    LiteralExpr *data01 = new LiteralExpr(100, IntType());
    BinaryExpr *condition = new BinaryExpr(omniruntime::expressions::Operator::GT, col01, data01, BooleanType());

    FieldExpr *col02 = new FieldExpr(0, IntType());
    LiteralExpr *data02 = new LiteralExpr(200, IntType());
    BinaryExpr *texp = new BinaryExpr(omniruntime::expressions::Operator::GT, col02, data02, BooleanType());

    FieldExpr *col03 = new FieldExpr(0, IntType());
    LiteralExpr *data03 = new LiteralExpr(0, IntType());
    BinaryExpr *fexp = new BinaryExpr(omniruntime::expressions::Operator::LT, col03, data03, BooleanType());
    IfExpr *expr = new IfExpr(condition, texp, fexp);

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_INT), DataType(OMNI_INT) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {1001};
    int32_t v2[1] = {0};
    int32_t v3[1] = {21};
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    v1[0] = -12;
    v2[0] = 1245;
    v3[0] = 123;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = -12222;
    v2[0] = -12312;
    v3[0] = 42;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    codegen.reset();
    delete expr;
    delete context;
}

TEST(CodeGenTest, MathFunctions4)
{
    // create expression objects
    FieldExpr *col = new FieldExpr(0, IntType());
    std::vector<Expr *> args;
    args.push_back(col);
    for (int32_t i = 1; i < 6; i++) {
        args.push_back(new LiteralExpr(i, IntType()));
    }
    InExpr *expr = new InExpr(args);

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_INT), DataType(OMNI_INT) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {1};
    int32_t v2[1] = {0};
    int32_t v3[1] = {21};
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = 3;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = 5;
    v2[0] = 1245;
    v3[0] = 123;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = 0;
    v2[0] = -12312;
    v3[0] = 42;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    v1[0] = 123;
    v2[0] = -43;
    v3[0] = 542;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    codegen.reset();
    delete expr;
    delete context;
}

// For testing different types
TEST(CodeGenTest, CastNumbers1)
{
    std::string castStr = "CAST";
    std::string absStr = "abs";
    DataTypePtr retType = DoubleType();
    // create expression objects
    auto col0 = new FieldExpr(0, IntType());
    std::vector<Expr *> args01;
    args01.push_back(col0);
    auto cast0 = GetFuncExpr(castStr, args01, DoubleType());
    std::vector<Expr *> args02;
    args02.push_back(cast0);
    auto abs0 = GetFuncExpr(absStr, args02, DoubleType());

    auto col1 = new FieldExpr(1, IntType());
    std::vector<Expr *> args11;
    args11.push_back(col1);
    auto cast1 = GetFuncExpr(castStr, args11, DoubleType());
    std::vector<Expr *> args12;
    args12.push_back(cast1);
    auto abs1 = GetFuncExpr(absStr, args12, DoubleType());

    auto expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, abs0, abs1, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_LONG), DataType(OMNI_DOUBLE) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {10000};
    int64_t v2[1] = {10000};
    double v3[1] = {12.34};
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = 2000000000;
    v2[0] = 3000000000;
    v3[0] = -234;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    v1[0] = -1000000;
    v2[0] = -1000000;
    v3[0] = 133.324234;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    codegen.reset();
    delete expr;
    delete context;
}

TEST(CodeGenTest, CastNumbers2)
{
    // create expression objects
    std::string castStr = "CAST";
    auto col1 = new FieldExpr(1, LongType());
    DataTypePtr retType = DoubleType();
    std::vector<Expr *> args;
    args.push_back(col1);
    auto cast = GetFuncExpr(castStr, args, DoubleType());
    auto col2 = new FieldExpr(2, DoubleType());
    auto expr = new BinaryExpr(omniruntime::expressions::Operator::GT, cast, col2, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_LONG), DataType(OMNI_DOUBLE) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {324233};
    int64_t v2[1] = {12};
    double v3[1] = {12.34};
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    v1[0] = 2000000000;
    v2[0] = -233;
    v3[0] = -234.2142;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = -1000000;
    v2[0] = 12;
    v3[0] = 12;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    codegen.reset();
    delete expr;
    delete context;
}

TEST(CodeGenTest, Like)
{
    // create expression objects
    std::string funcStr = "LIKE";
    DataTypePtr retType = BooleanType();
    auto col = new FieldExpr(2, VarcharType());
    auto data = new LiteralExpr(new std::string(".*hello.*world.*"), VarcharType(17));
    std::vector<Expr *> args;
    args.push_back(col);
    args.push_back(data);
    auto expr = GetFuncExpr(funcStr, args, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_VARCHAR), DataType(OMNI_VARCHAR) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[1];
    string s2[1];

    int32_t v1[1] = {8766};
    s1[0] = "asdf";
    s2[0] = "asjd fehellojdsl kfjworlddslk  jf ";
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    offsets[0] = new int32_t[1];
    offsets[1] = new int32_t[2];
    offsets[1][0] = 0;
    offsets[1][1] = s1[0].length();
    offsets[2] = new int32_t[2];
    offsets[2][0] = 0;
    offsets[2][1] = s2[0].length();

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = {8766};
    s1[0] = "asdf";
    s2[0] = "asjd fehell ojdsl kfjwo rld dslk  jf ";
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());

    offsets[2][1] = s2->length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}


TEST(CodeGenTest, DateCast)
{
    // create expression objects
    std::string funcStr = "CAST";
    auto col2 = new FieldExpr(2, VarcharType());
    std::vector<Expr *> args;
    args.push_back(col2);
    DataTypePtr retType = IntType();
    auto cast = GetFuncExpr(funcStr, args, IntType());
    auto col0 = new FieldExpr(0, IntType());
    auto expr = new BinaryExpr(omniruntime::expressions::Operator::GT, cast, col0, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_VARCHAR), DataType(OMNI_VARCHAR) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[1];
    string s2[1];

    int32_t v1[1] = {8766};
    s1[0] = "asdf";
    s2[0] = "1994-01-01";
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    offsets[0] = new int32_t[1];
    offsets[1] = new int32_t[2];
    offsets[1][0] = 0;
    offsets[1][1] = s1[0].length();
    offsets[2] = new int32_t[2];
    offsets[2][0] = 0;
    offsets[2][1] = s2[0].length();

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "1996-01-02";
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "1993-11-12";
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, SubstrIn)
{
    // create the expression objects
    auto col2 = new FieldExpr(2, VarcharType());
    auto substrIndex = new LiteralExpr(1, IntType());
    auto substrLen = new LiteralExpr(2, IntType());
    DataTypePtr retType = VarcharType();
    std::string funcStr = "substr";
    std::vector<Expr *> substrArgs;
    substrArgs.push_back(col2);
    substrArgs.push_back(substrIndex);
    substrArgs.push_back(substrLen);
    auto substrExpr = GetFuncExpr(funcStr, substrArgs, VarcharType());

    std::vector<Expr *> args;
    args.push_back(substrExpr);
    args.push_back(new LiteralExpr(new std::string("12"), VarcharType(3)));
    args.push_back(new LiteralExpr(new std::string("21"), VarcharType(3)));
    args.push_back(new LiteralExpr(new std::string("13"), VarcharType(3)));
    args.push_back(new LiteralExpr(new std::string("31"), VarcharType(3)));
    args.push_back(new LiteralExpr(new std::string("34"), VarcharType(3)));
    args.push_back(new LiteralExpr(new std::string("43"), VarcharType(3)));

    InExpr *expr = new InExpr(args);

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_VARCHAR), DataType(OMNI_VARCHAR) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[1];
    string s2[1];
    int32_t v1[1] = {8766};
    s1[0] = "asdf";
    s2[0] = "2134121";

    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    offsets[0] = new int32_t[1];
    offsets[1] = new int32_t[2];
    offsets[1][0] = 0;
    offsets[1][1] = s1[0].length();
    offsets[2] = new int32_t[2];
    offsets[2][0] = 0;
    offsets[2][1] = s2[0].length();

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);

    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "233425";
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "424321";
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, ConcatStr)
{
    std::string funcStr = "concat";
    auto col1 = new FieldExpr(1, VarcharType());
    auto col2 = new FieldExpr(2, VarcharType());
    std::vector<Expr *> concatArgs;
    concatArgs.push_back(col1);
    concatArgs.push_back(col2);
    DataTypePtr retType = VarcharType();
    auto concatExpr = GetFuncExpr(funcStr, concatArgs, VarcharType());

    auto helloWorldExpr = new LiteralExpr(new std::string("helloworld"), VarcharType(11));
    auto expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, concatExpr, helloWorldExpr, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_VARCHAR), DataType(OMNI_VARCHAR) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[1];
    string s2[1];
    int32_t v1[1] = {8766};
    s1[0] = "hello";
    s2[0] = "world";
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    offsets[0] = new int32_t[1];
    offsets[1] = new int32_t[2];
    offsets[1][0] = 0;
    offsets[1][1] = s1[0].length();
    offsets[2] = new int32_t[2];
    offsets[2][0] = 0;
    offsets[2][1] = s2[0].length();

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);

    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = {8766};
    s1[0] = "hello";
    s2[0] = "world ";
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    v1[0] = {8766};
    s1[0] = "hello ";
    s2[0] = "world";
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, ConcatChars)
{
    // create expression objects
    auto *col1 = new FieldExpr(1, CharType(30));
    auto commaExpr = new LiteralExpr(new std::string(","), CharType(2));
    std::string funcStr = "concat";
    std::vector<Expr *> innerArgs;
    innerArgs.push_back(col1);
    innerArgs.push_back(commaExpr);
    auto innerConcat = GetFuncExpr(funcStr, innerArgs, CharType(32));

    auto col2 = new FieldExpr(2, CharType(20));
    std::vector<Expr *> outerArgs;
    outerArgs.push_back(innerConcat);
    outerArgs.push_back(col2);
    auto outerConcat = GetFuncExpr(funcStr, outerArgs, CharType(52));

    auto helloExpr = new LiteralExpr(new std::string("hello                         , world"), CharType(52));
    auto expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, outerConcat, helloExpr, BooleanType());

    auto charTypeA = new CharDataType(30);
    auto charTypeB = new CharDataType(20);
    std::vector<DataType> vecOfTypes = { IntDataType(), DataType(*charTypeA), DataType(*charTypeB) };
    DataTypes types(vecOfTypes);
    Parser parser {};
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[1];
    string s2[1];
    int32_t v1[1] = {8766};
    s1[0] = "hello";
    s2[0] = "world";
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());
    int32_t *selected = new int32_t[1];
    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    offsets[0] = new int32_t[1];
    offsets[1] = new int32_t[2];
    offsets[1][0] = 0;
    offsets[1][1] = s1[0].length();
    offsets[2] = new int32_t[2];
    offsets[2][0] = 0;
    offsets[2][1] = s2[0].length();

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);

    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = {8766};
    s1[0] = "hello";
    s2[0] = "world ";
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    v1[0] = {8766};
    s1[0] = "hello ";
    s2[0] = "world";
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete charTypeA;
    delete charTypeB;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, ToUpper)
{
    std::string funcStr = "upper";
    auto *col1 = new FieldExpr(1, VarcharType());
    std::vector<Expr *> toUpperArgs;
    toUpperArgs.push_back(col1);
    DataTypePtr retType = VarcharType();
    auto toUpperExpr = GetFuncExpr(funcStr, toUpperArgs, VarcharType());

    auto upperTestExpr = new LiteralExpr(
        new std::string("[\\]^_ABCDEFGHIJKLMNOPQRSTUVWXYZ{|} THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG."),
        VarcharType(80));
    auto expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, toUpperExpr, upperTestExpr, BooleanType());

    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[] = {"[\\]^_abcdefghijklmnopqrstuvwxyz{|} The quick brown fox jumps over the lazy dog."};
    int32_t v1[] = {8766};
    int64_t *vals = new int64_t[2];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[2];
    for (int i = 0; i < 2; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[2];
    offsets[0] = new int32_t[1];
    offsets[1] = new int32_t[2];
    offsets[0][0] = 0;
    offsets[1][0] = 0;
    offsets[1][1] = s1[0].length();

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[2] = {};

    auto context = new ExecutionContext();

    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);

    EXPECT_EQ(result, 1);

    context->GetArena()->Reset();

    for (int i = 0; i < 2; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, ToUpperChar)
{
    std::string funcStr = "upper";
    auto *col1 = new FieldExpr(1, CharType(80));
    std::vector<Expr *> toUpperCharArgs;
    toUpperCharArgs.push_back(col1);
    auto toUpperCharExpr = GetFuncExpr(funcStr, toUpperCharArgs, CharType(80));

    auto upperCharTestExpr = new LiteralExpr(
        new std::string("[\\]^_ABCDEFGHIJKLMNOPQRSTUVWXYZ{|} THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG."),
        CharType(80));
    auto expr =
        new BinaryExpr(omniruntime::expressions::Operator::EQ, toUpperCharExpr, upperCharTestExpr, BooleanType());

    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[] = {"[\\]^_abcdefghijklmnopqrstuvwxyz{|} The quick brown fox jumps over the lazy dog."};
    int32_t v1[] = {8766};
    int64_t *vals = new int64_t[2];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[2];
    for (int i = 0; i < 2; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[2];
    offsets[0] = new int32_t[1];
    offsets[1] = new int32_t[2];
    offsets[0][0] = 0;
    offsets[1][0] = 0;
    offsets[1][1] = s1[0].length();

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[2] = {};

    auto context = new ExecutionContext();

    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);

    EXPECT_EQ(result, 1);

    context->GetArena()->Reset();

    for (int i = 0; i < 2; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, StringWithOps)
{
    // create expression objects
    FieldExpr *col21 = new FieldExpr(2, VarcharType());
    LiteralExpr *sunday = new LiteralExpr(new std::string("Sunday"), VarcharType(7));
    BinaryExpr *eqExpr1 = new BinaryExpr(omniruntime::expressions::Operator::EQ, col21, sunday, BooleanType());

    FieldExpr *col22 = new FieldExpr(2, VarcharType());
    LiteralExpr *saturday = new LiteralExpr(new std::string("Saturday"), VarcharType(9));
    BinaryExpr *eqExpr2 = new BinaryExpr(omniruntime::expressions::Operator::EQ, col22, saturday, BooleanType());

    BinaryExpr *expr = new BinaryExpr(omniruntime::expressions::Operator::OR, eqExpr1, eqExpr2, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_VARCHAR), DataType(OMNI_VARCHAR) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[1];
    string s2[1];

    int32_t v1[1] = {8766};
    s1[0] = "asdf";
    s2[0] = "Saturday";

    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());
    int32_t *selected = new int32_t[1];


    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    offsets[0] = new int32_t[1];
    offsets[1] = new int32_t[2];
    offsets[1][0] = 0;
    offsets[1][1] = s1[0].length();
    offsets[2] = new int32_t[2];
    offsets[2][0] = 0;
    offsets[2][1] = s2[0].length();

    auto codegen = FilterCodeGen::Create("stringTest1", *expr);
    int64_t dictionaries[3] = {};
    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "Sunday";
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();
    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "Monday";
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(s1->c_str());
    vals[2] = reinterpret_cast<int64_t>(s2->c_str());
    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();
    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, Coalesce)
{
    // create expression objects
    FieldExpr *value1 = new FieldExpr(0, LongType());
    LiteralExpr *value2 = new LiteralExpr(0, LongType());
    value2->dataType = LongType();
    CoalesceExpr *coalesceExpr = new CoalesceExpr(value1, value2);

    LiteralExpr *right = new LiteralExpr(123L, LongType());
    right->dataType = LongType();
    BinaryExpr *expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, coalesceExpr, right, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG), DataType(OMNI_LONG), DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int64_t v1[1] = {123};
    int64_t v2[1] = {234};
    int64_t v3[1] = {345};
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    bitmap[0][0] = true;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, ProjectionCoalesce)
{
    // create expression objects
    FieldExpr *value1 = new FieldExpr(0, LongType());
    LiteralExpr *value2 = new LiteralExpr(100L, LongType());
    value2->dataType = LongType();
    CoalesceExpr *coalesceExpr = new CoalesceExpr(value1, value2);

    LiteralExpr *right = new LiteralExpr(100L, LongType());
    right->dataType = LongType();
    BinaryExpr *expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, coalesceExpr, right, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t c[3] = {100, 200, 300};
    int64_t *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(c);
    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[0][i] = false;
    }
    auto **offsets = new int32_t *[1];
    for (int col = 0; col < 1; col++) {
        offsets[col] = new int32_t[3];
    }
    bool newNullValues[3];
    int newLengths[3];

    auto codegen = ProjectionCodeGen::Create(defaultTestFunctionName, *expr, false);
    int64_t dictionaryVectors[1] = {};

    bool oVec[3];
    auto context = new ExecutionContext();
    auto func = (ProjectFunc)(intptr_t)codegen->GetFunction();

    int32_t r = func(vals, 3, (int64_t)oVec, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues,
        newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_NE(r, 0);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);
    EXPECT_EQ(oVec[0], true);
    EXPECT_EQ(oVec[1], false);
    EXPECT_EQ(oVec[2], false);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        bitmap[0][0] = true;
    }
    r = func(vals, 3, (int64_t)oVec, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths,
        reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_NE(r, 0);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);
    EXPECT_EQ(oVec[0], true);
    EXPECT_EQ(oVec[1], false);
    EXPECT_EQ(oVec[2], false);
    context->GetArena()->Reset();

    for (int i = 0; i < 1; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, ProjectionIsNull)
{
    FieldExpr *col0 = new FieldExpr(0, LongType());
    IsNullExpr *expr = new IsNullExpr(col0);

    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t c[3] = {100, 200, 300};
    int64_t *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(c);
    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[0][i] = false;
    }
    auto **offsets = new int32_t *[1];
    for (int col = 0; col < 1; col++) {
        offsets[col] = new int32_t[3];
    }
    bool newNullValues[3];
    int newLengths[3];

    auto codegen = ProjectionCodeGen::Create(defaultTestFunctionName, *expr, false);
    int64_t dictionaryVectors[1] = {};

    bool oVec[3];
    auto context = new ExecutionContext();
    auto func = (ProjectFunc)(intptr_t)codegen->GetFunction();

    int32_t r = func(vals, 3, (int64_t)oVec, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues,
        newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_NE(r, 0);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);
    EXPECT_EQ(oVec[0], false);
    EXPECT_EQ(oVec[1], false);
    EXPECT_EQ(oVec[2], false);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        bitmap[0][i] = true;
    }
    r = func(vals, 3, (int64_t)oVec, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths,
        reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_NE(r, 0);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);
    EXPECT_EQ(oVec[0], true);
    EXPECT_EQ(oVec[1], true);
    EXPECT_EQ(oVec[2], true);
    context->GetArena()->Reset();

    for (int i = 0; i < 1; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, IsNull)
{
    FieldExpr *col0 = new FieldExpr(0, LongType());
    IsNullExpr *expr = new IsNullExpr(col0);

    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t v1[1] = {123};
    auto *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(v1);
    auto *selected = new int32_t[1];

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;

    auto **offsets = new int32_t *[1];
    for (int col = 0; col < 1; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, false);
    context->GetArena()->Reset();

    bitmap[0][0] = true;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, true);
    context->GetArena()->Reset();

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] offsets[0];
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, IsNotNull)
{
    FieldExpr *col0 = new FieldExpr(0, LongType());
    IsNullExpr *isNull = new IsNullExpr(col0);
    UnaryExpr *expr = new UnaryExpr(omniruntime::expressions::Operator::NOT, isNull, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t v1[1] = {123};
    auto *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(v1);
    auto *selected = new int32_t[1];

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;

    auto **offsets = new int32_t *[1];
    for (int col = 0; col < 1; col++) {
        offsets[col] = new int32_t[1];
    }
    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, true);
    context->GetArena()->Reset();

    bitmap[0][0] = true;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, false);
    context->GetArena()->Reset();

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] offsets[0];
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, DecimalOperators1)
{
    // create expression objects
    FieldExpr *addLeft = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *addRight = new LiteralExpr(new string("5"), Decimal128Type(38, 0));
    BinaryExpr *addExpr =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, Decimal128Type(38, 0));

    LiteralExpr *equalRight = new LiteralExpr(new string("15"), Decimal128Type(38, 0));
    BinaryExpr *expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, addExpr, equalRight, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    // creating decimal
    int64_t c[4] = { 10, 0, 9, 0 };
    int64_t *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(c);

    int32_t *selected = new int32_t[2];

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[2];
    for (int i = 0; i < 2; i++) {
        bitmap[0][i] = false;
    }

    auto **offsets = new int32_t *[1];
    for (int col = 0; col < 1; col++) {
        offsets[col] = new int32_t[2];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    for (int i = 0; i < 1; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, DecimalOperators2)
{
    string unparsed = "BETWEEN:4(#0, #1, #2)";
    FieldExpr *col0 = new FieldExpr(0, Decimal128Type(38, 0));
    FieldExpr *col1 = new FieldExpr(1, Decimal128Type(38, 0));
    FieldExpr *col2 = new FieldExpr(2, Decimal128Type(38, 0));

    BetweenExpr *expr = new BetweenExpr(col0, col1, col2);

    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128), DataType(OMNI_DECIMAL128),
        DataType(OMNI_DECIMAL128) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    // creating decimal
    int64_t c1[2] = {4000, 0};

    int64_t d1[2] = {5000, 0};

    int64_t e1[2] = {15000, 0};

    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(c1);
    vals[1] = reinterpret_cast<int64_t>(d1);
    vals[2] = reinterpret_cast<int64_t>(e1);

    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};
    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, DecimalOperators3)
{
    // create expression objects
    FieldExpr *col01 = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *data01 = new LiteralExpr(new string("100"), Decimal128Type(38, 0));
    BinaryExpr *condition = new BinaryExpr(omniruntime::expressions::Operator::GT, col01, data01, BooleanType());

    FieldExpr *col02 = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *data02 = new LiteralExpr(new string("200"), Decimal128Type(38, 0));
    BinaryExpr *texp = new BinaryExpr(omniruntime::expressions::Operator::GT, col02, data02, BooleanType());

    FieldExpr *col03 = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *data03 = new LiteralExpr(new string("0"), Decimal128Type(38, 0));
    BinaryExpr *fexp = new BinaryExpr(omniruntime::expressions::Operator::LT, col03, data03, BooleanType());

    IfExpr *expr = new IfExpr(condition, texp, fexp);

    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128), DataType(OMNI_DECIMAL128),
        DataType(OMNI_DECIMAL128) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    // creating decimal
    int64_t c1[2] = {-12222, -1};

    int64_t d1[2] = {-12312, -1};

    int64_t e1[2] = {42, 0};

    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(c1);
    vals[1] = reinterpret_cast<int64_t>(d1);
    vals[2] = reinterpret_cast<int64_t>(e1);

    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, DISABLED_DecimalNegate)
{
    // currently fails
    FieldExpr *col0 = new FieldExpr(0, Decimal128Type(38, 0));
    FieldExpr *col1 = new FieldExpr(1, Decimal128Type(38, 0));
    LiteralExpr *mulRight0 = new LiteralExpr(new string("-1"), Decimal128Type(38, 0));

    LiteralExpr *mulRight1 = new LiteralExpr(new string("-1"), Decimal128Type(38, 0));

    BinaryExpr *mulExpr0 =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, col0, mulRight0, Decimal128Type(38, 0));
    BinaryExpr *mulExpr1 =
        new BinaryExpr(omniruntime::expressions::Operator::MUL, col1, mulRight1, Decimal128Type(38, 0));

    FieldExpr *col2 = new FieldExpr(2, Decimal128Type(38, 0));
    FieldExpr *col3 = new FieldExpr(3, Decimal128Type(38, 0));

    BinaryExpr *gteExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, mulExpr0, col2, BooleanType());
    BinaryExpr *lteExpr = new BinaryExpr(omniruntime::expressions::Operator::EQ, mulExpr1, col3, BooleanType());

    BinaryExpr *expr = new BinaryExpr(omniruntime::expressions::Operator::AND, lteExpr, gteExpr, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    // creating decimal
    int64_t c1[2] = {3, 0};
    int64_t d1[2] = {4, 0};
    int64_t e1[2] = {3, -1};
    int64_t f1[2] = {4, -1};

    int64_t *vals = new int64_t[4];
    vals[0] = reinterpret_cast<int64_t>(c1);
    vals[1] = reinterpret_cast<int64_t>(d1);
    vals[2] = reinterpret_cast<int64_t>(e1);
    vals[3] = reinterpret_cast<int64_t>(f1);

    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[4];
    for (int i = 0; i < 4; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[4];
    for (int col = 0; col < 4; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[4] = {};
    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);

    for (int i = 0; i < 4; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, DISABLED_Decimal128AbsAndCompare)
{
    // currently fails
    FieldExpr *col0 = new FieldExpr(0, Decimal128Type(38, 0));
    FieldExpr *col1 = new FieldExpr(1, Decimal128Type(38, 0));
    std::string absFuncStr = "abs";
    std::vector<Expr *> absArgs;
    absArgs.push_back(col0);
    auto absExpr = GetFuncExpr(absFuncStr, absArgs, Decimal128Type(38, 0));

    std::string compFuncStr = "Decimal128Compare";
    std::vector<Expr *> compArgs;
    compArgs.push_back(absExpr);
    compArgs.push_back(col1);
    auto compExpr = GetFuncExpr(compFuncStr, compArgs, IntType());

    LiteralExpr *eqRight = new LiteralExpr(0, IntType());

    BinaryExpr *expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, compExpr, eqRight, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128), DataType(OMNI_DECIMAL128) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    // creating decimal
    int64_t c1[2] = {3, -1};
    int64_t d1[2] = {3, 0};

    int64_t *vals = new int64_t[2];
    vals[0] = reinterpret_cast<int64_t>(c1);
    vals[1] = reinterpret_cast<int64_t>(d1);

    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[2];
    for (int i = 0; i < 2; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[2];
    for (int col = 0; col < 2; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[2] = {};
    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);

    for (int i = 0; i < 2; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, ProjectionSubtractNulls)
{
    // create expression objects
    FieldExpr *subLeft = new FieldExpr(0, LongType());
    LiteralExpr *subRight = new LiteralExpr(100L, LongType());

    BinaryExpr *expr = new BinaryExpr(omniruntime::expressions::Operator::SUB, subLeft, subRight, LongType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t c[3] = {10, 20, 30};

    int64_t *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(c);

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[0][i] = false;
    }
    auto **offsets = new int32_t *[1];
    for (int col = 0; col < 1; col++) {
        offsets[col] = new int32_t[3];
    }
    bool newNullValues[3];
    int newLengths[3];

    auto codegen = ProjectionCodeGen::Create(defaultTestFunctionName, *expr, false);
    int64_t dictionaryVectors[1] = {};

    vector<int64_t> oVec(6);
    auto ov = oVec.data();
    void *vecVals = &ov;
    auto cvecVals = static_cast<int64_t *>(vecVals);
    auto context = new ExecutionContext();
    auto func = (ProjectFunc)(intptr_t)codegen->GetFunction();

    int32_t r = func(vals, 3, *cvecVals, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues,
        newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_NE(r, 0);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);
    EXPECT_EQ(oVec[0], -90);
    EXPECT_EQ(oVec[1], -80);
    EXPECT_EQ(oVec[2], -70);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        bitmap[0][i] = true;
    }
    r = func(vals, 3, *cvecVals, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths,
        reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(newNullValues[0], true);
    EXPECT_EQ(newNullValues[1], true);
    EXPECT_EQ(newNullValues[2], true);
    EXPECT_EQ(oVec[0], 0);
    EXPECT_EQ(oVec[1], 0);
    EXPECT_EQ(oVec[2], 0);
    context->GetArena()->Reset();
    for (int i = 0; i < 1; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, ProjectionCodeGen)
{
    // create expression objects
    FieldExpr *addLeft = new FieldExpr(0, Decimal128Type(38, 0));
    LiteralExpr *addRight = new LiteralExpr(new string("100"), Decimal128Type(38, 0));

    BinaryExpr *expr =
        new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, Decimal128Type(38, 0));

    std::vector<DataType> vecOfTypes = { DataType(OMNI_DECIMAL128) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    // creating decimal
    int64_t c1[6] = {10, 0, 20, 0, 30, 0};

    int64_t *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(c1);

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[0][i] = false;
    }
    auto **offsets = new int32_t *[1];
    for (int col = 0; col < 1; col++) {
        offsets[col] = new int32_t[3];
    }
    bool newNullValues[3];
    int newLengths[3];

    auto codegen = ProjectionCodeGen::Create(defaultTestFunctionName, *expr, false);
    int64_t dictionaryVectors[1] = {};

    vector<int64_t> oVec(6);
    auto ov = oVec.data();
    void *vecVals = &ov;
    auto cvecVals = static_cast<int64_t *>(vecVals);
    auto context = new ExecutionContext();
    auto func = (ProjectFunc)(intptr_t)codegen->GetFunction();

    int32_t r = func(vals, 3, *cvecVals, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues,
        newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_NE(r, 0);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);

    EXPECT_EQ(oVec.at(0), 110);
    EXPECT_EQ(oVec.at(1), 0);
    EXPECT_EQ(oVec.at(2), 120);
    EXPECT_EQ(oVec.at(3), 0);
    EXPECT_EQ(oVec.at(4), 130);
    EXPECT_EQ(oVec.at(5), 0);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        bitmap[0][i] = true;
    }
    r = func(vals, 3, *cvecVals, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths,
        reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(newNullValues[0], true);
    EXPECT_EQ(newNullValues[1], true);
    EXPECT_EQ(newNullValues[2], true);

    EXPECT_EQ(oVec.at(0), 0);
    EXPECT_EQ(oVec.at(1), 0);
    EXPECT_EQ(oVec.at(2), 0);
    EXPECT_EQ(oVec.at(3), 0);
    EXPECT_EQ(oVec.at(4), 0);
    EXPECT_EQ(oVec.at(5), 0);

    context->GetArena()->Reset();
    for (int i = 0; i < 1; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }
    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete expr;
    codegen.reset();
    delete context;
}

TEST(CodeGenTest, TestRowProjectLong)
{
    int64_t values[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    auto *vector = CreateVector<LongVector>(values, 10);
    auto slicedVector = vector->Slice(4, 6);

    // create expression objects
    FieldExpr *addLeft = new FieldExpr(0, LongType());
    LiteralExpr *addRight = new LiteralExpr(100L, LongType());

    BinaryExpr *expr = new BinaryExpr(omniruntime::expressions::Operator::ADD, addLeft, addRight, LongType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);
    RowProjection rowProjection(*expr);
    RowProjFunc func = rowProjection.Create();

    int32_t positionOffset = slicedVector->GetPositionOffset();
    int64_t *valueAddress = (int64_t *)(slicedVector->GetValues()) + positionOffset;
    bool *nullAddress = (bool *)(slicedVector->GetValueNulls()) + positionOffset;
    int32_t *offsetAddress = (int32_t *)(slicedVector->GetValueOffsets()) + positionOffset;

    int64_t valuesAddr[1] = {reinterpret_cast<int64_t>(valueAddress)};
    int64_t nullsAddr[1] = {reinterpret_cast<int64_t>(nullAddress)};
    int64_t offsetsAddr[1] = {reinterpret_cast<int64_t>(offsetAddress)};
    bool isNull;
    int32_t length;
    int64_t dictVecAddr[1] = {0};
    auto context = new ExecutionContext();
    for (int32_t i = 0; i < slicedVector->GetSize(); i++) {
        isNull = false;
        void *valuePtr = func(valuesAddr, nullsAddr, offsetsAddr, i, &length, reinterpret_cast<int64_t>(context),
            dictVecAddr, &isNull);
        int64_t value = *((int64_t *)valuePtr);
        int64_t inputValue = slicedVector->GetValue(i);
        EXPECT_EQ(value, inputValue + 100);
    }
    context->GetArena()->Reset();

    delete slicedVector;
    delete vector;
    delete context;
}

TEST(CodeGenTest, TestRowProjectVarchar)
{
    omniruntime::type::VarcharDataType type(10);
    std::string values[2] = {"hello", "world"};
    omniruntime::vec::VarcharVector *vector = CreateVarcharVector(type, values, 2);
    auto slicedVector = vector->Slice(1, 1);

    // create expression objects
    std::string funcStr = "substr";
    auto substrData = new FieldExpr(0, VarcharType());
    auto substrIndex = new LiteralExpr(1, IntType());
    auto substrLen = new LiteralExpr(5, IntType());
    DataTypePtr retType = VarcharType();
    std::vector<Expr *> args;
    args.push_back(substrData);
    args.push_back(substrIndex);
    args.push_back(substrLen);
    auto expr = GetFuncExpr(funcStr, args, VarcharType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_VARCHAR) };
    DataTypes types(vecOfTypes);
    RowProjection rowProjection(*expr);
    RowProjFunc func = rowProjection.Create();

    int32_t positionOffset = slicedVector->GetPositionOffset();
    uint8_t *valueAddress = (uint8_t *)(slicedVector->GetValues());
    bool *nullAddress = (bool *)(slicedVector->GetValueNulls()) + positionOffset;
    int32_t *offsetAddress = (int32_t *)(slicedVector->GetValueOffsets()) + positionOffset;

    int64_t valuesAddr[1] = {reinterpret_cast<int64_t>(valueAddress)};
    int64_t valueNulls[1] = {reinterpret_cast<int64_t>(nullAddress)};
    int64_t valueOffsets[1] = {reinterpret_cast<int64_t>(offsetAddress)};
    int32_t length;
    int64_t dictionaries[1] = {};
    bool isNull;
    auto context = new ExecutionContext();
    for (int32_t i = 0; i < slicedVector->GetSize(); i++) {
        isNull = false;
        void *valuePtr = func(valuesAddr, valueNulls, valueOffsets, i, &length, reinterpret_cast<int64_t>(context),
            dictionaries, &isNull);
        uint8_t *value = *reinterpret_cast<uint8_t **>(reinterpret_cast<uintptr_t>(valuePtr));
        std::string result(value, value + length);

        uint8_t *expectValue = nullptr;
        int32_t expectLen = slicedVector->GetValue(i, &expectValue);
        std::string expectResult(expectValue, expectValue + expectLen);
        EXPECT_EQ(result, expectResult.substr(0, 5));
    }
    context->GetArena()->Reset();

    delete slicedVector;
    delete vector;
    delete context;
}

TEST(CodeGenTest, CastNumbers3)
{
    // create expression objects
    std::vector<Expr *> args;
    FieldExpr *col1 = new FieldExpr(1, DoubleType());
    FieldExpr *col2 = new FieldExpr(2, DoubleType());
    BinaryExpr *expr = new BinaryExpr(omniruntime::expressions::Operator::LT, col1, col2, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT), DataType(OMNI_DOUBLE), DataType(OMNI_DOUBLE) };
    DataTypes types(vecOfTypes);
    Parser parser {};
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {10000};
    double v2[1] = {9.01};
    double v3[1] = {12.34};
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }
    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->GetArena()->Reset();

    v1[0] = 2000000000;
    v2[0] = -100.22;
    v3[0] = -234.2142;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    v1[0] = -1000000;
    v2[0] = 12;
    v3[0] = 12;
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }

    delete[] offsets;
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    codegen.reset();
    delete expr;
    delete context;
}

TEST(CodeGenTest, Substr)
{
    std::vector<DataType> vecOfTypes = { DataType(OMNI_VARCHAR), DataType(OMNI_VARCHAR) };
    DataTypes types(vecOfTypes);
    const int32_t numCols = 2;
    const int32_t numRows = 1;

    string s1[1];
    string s2[1];
    s1[0] = "SUBSTR Function";
    s2[0] = "Function SUBSTR";

    int64_t *vals = new int64_t[2];
    vals[0] = reinterpret_cast<int64_t>(s1->c_str());
    vals[1] = reinterpret_cast<int64_t>(s2->c_str());

    int32_t *selected = new int32_t[1];
    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }

    auto context = new ExecutionContext();
    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows + 1];
        offsets[col][0] = 0;
        offsets[col][1] = 15;
    }

    // create expression objects
    auto substrData1 = new FieldExpr(0, VarcharType());
    auto substrIndex1 = new LiteralExpr(-5, IntType());
    auto substrLen1 = new LiteralExpr(5, IntType());
    std::string funcStr = "substr";
    DataTypePtr retType = VarcharType();
    std::vector<Expr *> args1;
    args1.push_back(substrData1);
    args1.push_back(substrIndex1);
    args1.push_back(substrLen1);
    auto substrExp1 = GetFuncExpr(funcStr, args1, VarcharType());

    auto ctionExpr = new LiteralExpr(new std::string("ction"), VarcharType(6));
    auto expr1 = new BinaryExpr(omniruntime::expressions::Operator::EQ, substrExp1, ctionExpr, BooleanType());

    int64_t dictionaries[numCols] = {};

    auto filter = new RowFilter(*expr1);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    bool res =
        filterFunc(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(res, true);
    delete filter;

    // create expression objects
    auto substrData2 = new FieldExpr(1, VarcharType());
    auto substrIndex2 = new LiteralExpr(-5, IntType());
    std::vector<Expr *> args2;
    args2.push_back(substrData2);
    args2.push_back(substrIndex2);
    auto substrExp2 = GetFuncExpr(funcStr, args2, VarcharType());

    auto ubsterExpr = new LiteralExpr(new std::string("UBSTR"), VarcharType(6));
    auto expr2 = new BinaryExpr(omniruntime::expressions::Operator::EQ, substrExp2, ubsterExpr, BooleanType());

    filter = new RowFilter(*expr2);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    res = filterFunc(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(res, true);
    delete filter;

    // create expression objects
    auto substrData3 = new FieldExpr(0, VarcharType());
    auto substrIndex3 = new LiteralExpr(4, IntType());
    std::vector<Expr *> args3;
    args3.push_back(substrData3);
    args3.push_back(substrIndex3);
    auto substrExp3 = GetFuncExpr(funcStr, args3, VarcharType());

    auto STRExpr = new LiteralExpr(new std::string("STR Function"), VarcharType(13));
    auto expr3 = new BinaryExpr(omniruntime::expressions::Operator::EQ, substrExp3, STRExpr, BooleanType());

    filter = new RowFilter(*expr3);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    res = filterFunc(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(res, true);
    context->GetArena()->Reset();

    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }

    delete[] selected;
    delete[] vals;
    delete[] bitmap;
    delete[] offsets;
    delete filter;
    delete context;
}

TEST(CodeGenTest, Mm3HashInt)
{
    // create expression objects
    auto col0 = new FieldExpr(0, IntType());
    auto data = new LiteralExpr(42, IntType());
    std::string funcStr = "mm3hash";
    DataTypePtr retType = IntType();
    std::vector<Expr *> hashArgs;
    hashArgs.push_back(col0);
    hashArgs.push_back(data);
    auto mhash = GetFuncExpr(funcStr, hashArgs, IntType());

    auto equalRight = new LiteralExpr(723455942, IntType());

    auto expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, mhash, equalRight, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int32_t v1[1] = {-2147483648};
    auto *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(v1);
    auto *selected = new int32_t[1];

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;
    auto **offsets = new int32_t *[1];
    offsets[0] = new int32_t[1];
    offsets[0][0] = 0;

    auto codegen = FilterCodeGen::Create("mm3hashTest", *expr);

    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    bool result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, true);
    context->GetArena()->Reset();

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] offsets[0];
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    delete context;
    delete expr;
    codegen.reset();
}

TEST(CodeGenTest, Mm3HashLong)
{
    std::string funcStr = "mm3hash";
    DataTypePtr retType = IntType();
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, LongType()));
    args.push_back(new LiteralExpr(42, IntType()));
    auto expr = GetFuncExpr(funcStr, args, IntType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);

    int64_t v1[1] = {-2147483648};
    auto *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(v1);

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;
    auto **offsets = new int32_t *[1];
    offsets[0] = new int32_t[1];
    offsets[0][0] = 0;

    RowProjection rowProjection(*expr);
    RowProjFunc func = rowProjection.Create();
    EXPECT_EQ(rowProjection.GetReturnType().GetId(), OMNI_INT);
    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    bool isNull = false;
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();

    int32_t res = *((int32_t *)func(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, dataLength,
        reinterpret_cast<int64_t>(context), dictionaries, &isNull));
    int32_t expectedRes = Mm3Int64(v1[0], false, 42);
    EXPECT_EQ(res, expectedRes);
    context->GetArena()->Reset();

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] offsets[0];
    delete[] offsets;
    delete[] vals;
    delete[] dataLength;
    delete context;
}

TEST(CodeGenTest, Mm3HashDouble)
{
    std::string funcStr = "mm3hash";
    DataTypePtr retType = IntType();
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, DoubleType()));
    args.push_back(new LiteralExpr(42, IntType()));
    auto expr = GetFuncExpr(funcStr, args, IntType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_DOUBLE) };
    DataTypes types(vecOfTypes);

    double v1[1] = {123.456};
    auto *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(v1);

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;
    auto **offsets = new int32_t *[1];
    offsets[0] = new int32_t[1];
    offsets[0][0] = 0;

    RowProjection rowProjection(*expr);
    RowProjFunc func = rowProjection.Create();
    EXPECT_EQ(rowProjection.GetReturnType().GetId(), OMNI_INT);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    bool isNull = false;
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();

    int32_t res = *((int32_t *)func(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, dataLength,
        reinterpret_cast<int64_t>(context), dictionaries, &isNull));
    int32_t expectedRes = Mm3Double(v1[0], false, 42);
    EXPECT_EQ(res, expectedRes);
    context->GetArena()->Reset();

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] offsets[0];
    delete[] offsets;
    delete[] vals;
    delete[] dataLength;
    delete context;
}

TEST(CodeGenTest, Mm3HashString)
{
    std::string funcStr = "mm3hash";
    DataTypePtr retType = IntType();
    std::vector<Expr *> args;
    args.push_back(new FieldExpr(0, VarcharType()));
    args.push_back(new LiteralExpr(42, IntType()));
    auto expr = GetFuncExpr(funcStr, args, IntType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_VARCHAR) };
    DataTypes types(vecOfTypes);

    std::string v1 = "hello world";
    auto *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(v1.c_str());

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;
    auto **offsets = new int32_t *[1];
    offsets[0] = new int32_t[2];
    offsets[0][0] = 0;
    offsets[0][1] = v1.size();

    RowProjection codegen(*expr);
    RowProjFunc func = codegen.Create();
    EXPECT_EQ(codegen.GetReturnType().GetId(), OMNI_INT);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    bool isNull = false;
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();

    int32_t res = *((int32_t *)func(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, dataLength,
        reinterpret_cast<int64_t>(context), dictionaries, &isNull));
    int32_t expectedRes = Mm3String(v1.c_str(), v1.size(), false, 42);
    EXPECT_EQ(res, expectedRes);
    context->GetArena()->Reset();

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] offsets[0];
    delete[] offsets;
    delete[] vals;
    delete[] dataLength;
    delete context;
}

TEST(CodeGenTest, Pmod)
{
    // create expression objects
    FieldExpr *col0 = new FieldExpr(0, IntType());
    LiteralExpr *data1 = new LiteralExpr(42, IntType());

    std::string hashStr = "mm3hash";
    DataTypePtr retType = IntType();
    std::vector<Expr *> hashArgs;
    hashArgs.push_back(col0);
    hashArgs.push_back(data1);
    auto mhash = GetFuncExpr(hashStr, hashArgs, IntType());

    std::string pmodStr = "pmod";
    std::vector<Expr *> pmodArgs;
    pmodArgs.push_back(mhash);
    auto pmodData1 = new LiteralExpr(42, IntType());
    pmodArgs.push_back(pmodData1);
    auto pmod = GetFuncExpr(pmodStr, pmodArgs, IntType());

    auto equalRight = new LiteralExpr(20, IntType());

    auto expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, pmod, equalRight, BooleanType());
    std::vector<DataType> vecOfTypes = { DataType(OMNI_INT) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int32_t v1[1] = {-2147483648};
    auto *vals = new int64_t[1];
    vals[0] = reinterpret_cast<int64_t>(v1);
    auto *selected = new int32_t[1];

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;
    auto **offsets = new int32_t *[1];
    offsets[0] = new int32_t[1];
    offsets[0][0] = 0;


    string testName = "pmodTest";
    auto codegen = FilterCodeGen::Create(testName, *expr);
    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();

    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(
        intptr_t)codegen->GetFunction();
    bool result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, true);
    context->GetArena()->Reset();

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] offsets[0];
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    delete context;
    delete expr;
}

TEST(CodeGenTest, SubstrWithChars)
{
    std::vector<DataType> vecOfTypes = { DataType(OMNI_CHAR), DataType(OMNI_CHAR) };
    DataTypes types(vecOfTypes);
    const int32_t numCols = 2;
    const int32_t numRows = 1;

    string s1[1];
    string s2[1];
    s1[0] = "SUBSTR Function";
    s2[0] = "Function SUBSTR";

    int64_t *vals = new int64_t[2];
    vals[0] = reinterpret_cast<int64_t>(s1->c_str());
    vals[1] = reinterpret_cast<int64_t>(s2->c_str());

    int32_t *selected = new int32_t[1];
    bool **bitmap = new bool *[numCols];
    for (int col = 0; col < numCols; col++) {
        bitmap[col] = new bool[numRows];
        for (int i = 0; i < numRows; i++) {
            bitmap[col][i] = false;
        }
    }

    auto context = new ExecutionContext();
    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows + 1];
        offsets[col][0] = 0;
        offsets[col][1] = 15;
    }

    // create expression objects
    std::string funcStr = "substr";
    DataTypePtr retType = CharType(10);
    FieldExpr *substrData1 = new FieldExpr(0, CharType(10));
    LiteralExpr *substrIndex1 = new LiteralExpr(1, IntType());
    LiteralExpr *substrLen1 = new LiteralExpr(5, IntType());
    std::vector<Expr *> args1;
    args1.push_back(substrData1);
    args1.push_back(substrIndex1);
    args1.push_back(substrLen1);
    auto substrExpr1 = GetFuncExpr(funcStr, args1, CharType(10));

    FieldExpr *substrData2 = new FieldExpr(1, CharType(10));
    LiteralExpr *substrIndex2 = new LiteralExpr(1, IntType());
    LiteralExpr *substrLen2 = new LiteralExpr(5, IntType());
    std::vector<Expr *> args2;
    args2.push_back(substrData2);
    args2.push_back(substrIndex2);
    args2.push_back(substrLen2);
    auto substrExpr2 = GetFuncExpr(funcStr, args2, CharType(10));

    auto expr = new BinaryExpr(omniruntime::expressions::Operator::NEQ, substrExpr1, substrExpr2, BooleanType());

    int64_t dictionaries[numCols] = {};
    auto filter = new RowFilter(*expr);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    bool res =
        filterFunc(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(res, true);

    context->GetArena()->Reset();

    for (int i = 0; i < numCols; i++) {
        delete[] bitmap[i];
        delete[] offsets[i];
    }

    delete[] selected;
    delete[] vals;
    delete filter;
    delete[] bitmap;
    delete[] offsets;
    delete context;
}

TEST(CodeGenTest, CombineHash)
{
    FieldExpr *col0 = new FieldExpr(0, LongType());
    FieldExpr *col1 = new FieldExpr(1, LongType());
    std::string funcStr = "combine_hash";
    DataTypePtr retType = LongType();
    std::vector<Expr *> args;
    args.push_back(col0);
    args.push_back(col1);
    auto combineHash = GetFuncExpr(funcStr, args, LongType());

    FieldExpr *col2 = new FieldExpr(2, LongType());
    BinaryExpr *expr = new BinaryExpr(omniruntime::expressions::Operator::EQ, combineHash, col2, BooleanType());

    std::vector<DataType> vecOfTypes = { DataType(OMNI_LONG), DataType(OMNI_LONG), DataType(OMNI_LONG) };
    DataTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t v1[1] = {12};
    int64_t v2[1] = {123};
    int64_t v3[1] = {495};
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);

    auto *selected = new int32_t[1];
    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }
    int32_t **offsets = new int32_t *[3];
    for (int i = 0; i < 3; i++) {
        offsets[i] = new int32_t[1];
        offsets[i][0] = false;
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};
    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    bool result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, true);
    context->GetArena()->Reset();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;

    for (int i = 0; i < 3; i++) {
        delete[] offsets[i];
    }
    delete[] offsets;

    delete[] vals;
    delete[] selected;
    delete context;
    delete expr;
    codegen.reset();
}
TEST(CodeGenTest, JSONFunc)
{
    std::string unparsed = R"(
        {
        "exprType": "BINARY",
        "returnType": 4,
        "operator": "EQUAL",
        "left": {
            "exprType": "BINARY",
            "returnType": 2,
            "operator": "ADD",
            "left": {
                "exprType": "LITERAL",
                "dataType": 2,
                "isNull": false,
                "value": 122
            },
            "right": {
                "exprType": "FUNC",
                "returnType": 2,
                "function_name": "abs",
                "arguments": [
                    {
                        "exprType": "FIELD_REFERENCE",
                        "dataType": 2,
                        "colVal": 0
                    }
                ]
            }
        },
        "right": {
            "exprType": "LITERAL",
            "dataType": 2,
            "isNull": false,
            "value": 123
        }
    }
    )";

    nlohmann::json unparsedJSON = nlohmann::json::parse(unparsed);
    std::cout << "parsed into JSON" << std::endl;

    Expr *expr = JSONParser::ParseJSON(unparsedJSON);
    ExprPrinter printer;
    expr->Accept(printer);
    cout << endl;

    int64_t v1[1] = {1};
    int64_t v2[1] = {234};
    int64_t v3[1] = {345};
    int64_t *vals = new int64_t[3];
    vals[0] = reinterpret_cast<int64_t>(v1);
    vals[1] = reinterpret_cast<int64_t>(v2);
    vals[2] = reinterpret_cast<int64_t>(v3);
    int32_t *selected = new int32_t[1];

    bool **bitmap = new bool *[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = new bool[1];
        bitmap[i][0] = false;
    }

    auto **offsets = new int32_t *[3];
    for (int col = 0; col < 3; col++) {
        offsets[col] = new int32_t[1];
    }

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[3] = {};
    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets),
        reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);

    v1[0] = -1;
    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 1);

    v1[0] = 2;
    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context),
        dictionaries);
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    codegen.reset();
}
