/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "gtest/gtest.h"

#include <iostream>
#include <string>
#include <vector>

#include <src/common/jsonparser/jsonparser.h>
#include "../../src/codegen/expression_codegen.h"
#include "../../src/codegen/filter_codegen.h"
#include "../../src/codegen/projection_codegen.h"
#include "../../src/operator/filter/filter_and_project.h"
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

const string defaultTestFunctionName = "test-function";

typedef int32_t (*FilterFunc)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
typedef int32_t (*ProjectFunc)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *, int32_t *, int64_t, int64_t *);

// Filter is basically just a projection that must return a boolean.
// The logic for filtering out rows from final output is handled in C++ when
// processing each row individually, so to LLVM it is exactly the same as a projection.
// Check CodeGenTest.RowFilter for a dedicated test using the RowFilter class instead.
TEST(CodeGenTest, SimpleFilter)
{
    // create expression objects
    DataExpr *lessThanLeft = new DataExpr(0, INT32D);
    DataExpr *lessThanRight = new DataExpr(50);
    BinaryExpr *lessThanExpr = new BinaryExpr(LT, lessThanLeft, lessThanRight, BOOLD);

    const int32_t numCols = 1;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);
    const int numRows = 100;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    int64_t *table = new int64_t[numCols];
    table[0] = (int64_t)col1;

    const int32_t entries = numRows * numCols;

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
    EXPECT_EQ(rowProjection.GetReturnType(), BOOLD);

    int32_t *dataLength = new int32_t(0);
    int64_t dictionaries[numCols] = {};
    bool isNull;

    auto context = new ExecutionContext();
    for (int32_t i = 0; i < 50; i++) {
        bool res = *((bool *)func(table, (int64_t*) bitmap, (int64_t*) offsets, i, dataLength, reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_TRUE(res);
    }
    context->getArena()->Reset();
    for (int32_t i = 50; i < 100; i++) {
        bool res = *((bool *)func(table, (int64_t*) bitmap, (int64_t*) offsets, i, dataLength, reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_FALSE(res);
    }
    context->getArena()->Reset();
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
    DataExpr *addLeft = new DataExpr(0, INT32D);
    DataExpr *addRight = new DataExpr(50);
    BinaryExpr *addExpr = new BinaryExpr(ADD, addLeft, addRight, INT32D);

    const int32_t numCols = 1;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);

    const int numRows = 100;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }

    int64_t *table = new int64_t[numCols];
    table[0] = (int64_t)col1;

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
    EXPECT_EQ(rowProjection.GetReturnType(), INT32D);
    bool isNull = false;
    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    int64_t dictionaries[numCols] = {};
    auto context = new ExecutionContext();
    for (int32_t i = 0; i < 100; i++) {
        int32_t res = *((int32_t *)func(table, (int64_t*) bitmap, (int64_t*) offsets, i, dataLength, reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_EQ(res, i + 50);
    }
    context->getArena()->Reset();
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
// A more complicated test for individual row projection
TEST(CodeGenTest, SingleProject)
{
    DataExpr *gtLeft = new DataExpr(1, INT64D);
    DataExpr *gtRight = new DataExpr(3000000000);
    gtRight->longVal = 3000000000;
    BinaryExpr *gtExpr = new BinaryExpr(GT, gtLeft, gtRight, BOOLD);

    DataExpr *addLeft = new DataExpr(0, INT32D);
    DataExpr *addRight = new DataExpr(10);
    BinaryExpr *addExpr = new BinaryExpr(ADD, addLeft, addRight, INT32D);

    DataExpr *mulLeft = new DataExpr(0, INT32D);
    DataExpr *mulRight = new DataExpr(-1);
    BinaryExpr *mulExpr = new BinaryExpr(MUL, mulLeft, mulRight, INT32D);

    IfExpr *ifExpr = new IfExpr(gtExpr, addExpr, mulExpr);

    const int32_t numCols = 2;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);

    const int numRows = 1000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i;
    }
    int64_t *col2 = new int64_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col2[i] = i % 2 ? 4000000000 : 12;
    }

    int64_t *table = new int64_t[numCols];
    table[0] = (int64_t)col1;
    table[1] = (int64_t)col2;

    const int32_t entries = numRows * numCols;

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
    EXPECT_EQ(rowProjection.GetReturnType(), INT32D);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    int64_t dictionaries[numCols] = {};
    auto context = new ExecutionContext();
    bool isNull = false;
    for (int32_t i = 0; i < numRows; i++) {
        int32_t res = *((int32_t *)func(table, (int64_t*) bitmap, (int64_t*) offsets, i, dataLength, reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_EQ(res, i % 2 ? i + 10 : -i);
    }
    context->getArena()->Reset();
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
    DataExpr *colExpr1 = new DataExpr(1, INT64D);

    const int32_t numCols = 2;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);

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
    table[0] = (int64_t)col1;
    table[1] = (int64_t)col2;

    const int32_t entries = numRows * numCols;
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
        int32_t res = *((int32_t *)func(table, (int64_t*) bitmap, (int64_t*) offsets, i, dataLength, reinterpret_cast<int64_t>(context), dictionaries, &isNull));
        EXPECT_EQ(res, i % 10);
    }
    context->getArena()->Reset();
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
    DataExpr *equalLeft = new DataExpr(0, INT32D);
    DataExpr *equalRight = new DataExpr(0);
    BinaryExpr *equalExpr = new BinaryExpr(EQ, equalLeft, equalRight, BOOLD);

    DataType types[1] = {DataType::INT32D};
    const int32_t numCols = 1;
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT) };
    VecTypes vecTypes(vecOfTypes);

    const int numRows = 1000;
    int32_t *col1 = new int32_t[numRows];
    for (int32_t i = 0; i < numRows; i++) {
        col1[i] = i % 2;
    }
    int64_t *table = new int64_t[numCols];
    table[0] = (int64_t)col1;

    const int32_t entries = numRows * numCols;
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
    vector<DataType> typeVec = vector<DataType>(types, types + 2);
    auto filter = new RowFilter(*equalExpr);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    auto context = new ExecutionContext();
    for (int32_t i = 0; i < numRows; i++) {
        bool res = filterFunc(table, (int64_t*) bitmap, (int64_t*) offsets, i, reinterpret_cast<int64_t>(context), dictionaryVectors);
        EXPECT_EQ(res, i % 2 == 0);
    }
    context->getArena()->Reset();
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

TEST (CodeGenTest, RowFilterString) {
    DataType types[2] = {DataType::VARCHARD, DataType::VARCHARD};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes vecTypes(vecOfTypes);
    const int32_t numCols = 2;
    const int32_t numRows = 1;

    string s1[1];
    string s2[1];

    s1[0] = "hello world";
    s2[0] = "world hello";
    int64_t *vals = new int64_t[2];
    vals[0] = (int64_t) s1->c_str();
    vals[1] = (int64_t) s2->c_str();
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

    vector<DataType> typeVec = vector<DataType>(types, types + 2);

    DataExpr *substrCol = new DataExpr(0, VARCHARD);
    DataExpr *substrIndex = new DataExpr(1);
    DataExpr *substrLen = new DataExpr(5);
    std::vector<Expr*> args;
    args.push_back(substrCol);
    args.push_back(substrIndex);
    args.push_back(substrLen);
    FuncExpr *substrExpr = new FuncExpr("substr", args, VARCHARD);
    DataExpr *helloExpr = new DataExpr(new std::string("hello"));

    BinaryExpr *eqExpr = new BinaryExpr(EQ, substrExpr, helloExpr, BOOLD);

    auto filter = new RowFilter(*eqExpr);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);

    bool res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(res, true);
    delete filter;

    DataExpr *substrCol2 = new DataExpr(1, VARCHARD);
    DataExpr *substrIndex2 = new DataExpr(1);
    DataExpr *substrLen2 = new DataExpr(5);
    std::vector<Expr*> args2;
    args2.push_back(substrCol2);
    args2.push_back(substrIndex2);
    args2.push_back(substrLen2);
    FuncExpr *substrExpr2 = new FuncExpr("substr", args2, VARCHARD);

    DataExpr *helloExpr2 = new DataExpr(new std::string("hello"));

    BinaryExpr *eqExpr2 = new BinaryExpr(EQ, substrExpr2, helloExpr2, BOOLD);
    filter = new RowFilter(*eqExpr2);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);

    res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(res, false);

    delete filter;
    DataExpr *substrCol3 = new DataExpr(0, VARCHARD);
    DataExpr *substrIndex3 = new DataExpr(1);
    DataExpr *substrLen3 = new DataExpr(5);
    std::vector<Expr*> args3;
    args3.push_back(substrCol3);
    args3.push_back(substrIndex3);
    args3.push_back(substrLen3);
    FuncExpr *substrExpr3 = new FuncExpr("substr", args3, VARCHARD);

    DataExpr *substrCol4 = new DataExpr(1, VARCHARD);
    DataExpr *substrIndex4 = new DataExpr(7);
    DataExpr *substrLen4 = new DataExpr(11);
    std::vector<Expr*> args4;
    args4.push_back(substrCol4);
    args4.push_back(substrIndex4);
    args4.push_back(substrLen4);
    FuncExpr *substrExpr4 = new FuncExpr("substr", args4, VARCHARD);

    BinaryExpr *eqExpr3 = new BinaryExpr(EQ, substrExpr3, substrExpr4, BOOLD);
    filter = new RowFilter(*eqExpr3);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);

    res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(res, true);
    context->getArena()->Reset();
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
    DataExpr *addLeft = new DataExpr(0, INT32D);
    DataExpr *addRight = new DataExpr(2);
    BinaryExpr *addExpr = new BinaryExpr(ADD, addLeft, addRight, INT32D);
    DataExpr *gteRight = new DataExpr(4);

    BinaryExpr *gteExpr = new BinaryExpr(GTE, addLeft, addRight, BOOLD);

    DataExpr *ltLeft = new DataExpr(1, INT32D);
    DataExpr *ltRight = new DataExpr(4);
    BinaryExpr *ltExpr = new BinaryExpr(LT, ltLeft, ltRight, BOOLD);

    DataExpr *eqLeft = new DataExpr(2, INT32D);
    DataExpr *eqRight = new DataExpr(2);
    BinaryExpr *eqExpr = new BinaryExpr(EQ, eqLeft, eqRight, BOOLD);

    BinaryExpr *andLeft = new BinaryExpr(AND, ltExpr, eqExpr, BOOLD);

    BinaryExpr *expr = new BinaryExpr(AND, gteExpr, andLeft, BOOLD);


    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);

    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {2};
    int32_t v2[1] = {3};
    int32_t v3[1] = {2};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;
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
    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);

    v1[0] = 2;
    v2[0] = 4;
    v3[0] = 2;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
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
    DataExpr *col01 = new DataExpr(0, INT32D);
    DataExpr *col2 = new DataExpr(2, INT32D);
    std::vector<Expr *> args1;
    std::vector<Expr *> args2;
    args1.push_back(col01);
    FuncExpr *abs1 = new FuncExpr("abs", args1, INT32D);
    args2.push_back(col2);
    FuncExpr *abs2 = new FuncExpr("abs", args2, INT32D);
    BinaryExpr *eq1 = new BinaryExpr(EQ, abs1, abs2, BOOLD);

    DataExpr *col02 = new DataExpr(0, INT32D);
    DataExpr *col1 = new DataExpr(1, INT32D);
    std::vector<Expr *> args3;
    std::vector<Expr *> args4;
    args3.push_back(col02);
    FuncExpr *abs3 = new FuncExpr("abs", args3, INT32D);
    args4.push_back(col1);
    FuncExpr *abs4 = new FuncExpr("abs", args4, INT32D);
    BinaryExpr *eq2 = new BinaryExpr(EQ, abs3, abs4, BOOLD);
    BinaryExpr *expr = new BinaryExpr(AND, eq1, eq2, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);

    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {-123};
    int32_t v2[1] = {123};
    int32_t v3[1] = {123};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);

    context->getArena()->Reset();

    EXPECT_EQ(result, 1);
    std::cout << "result: " << result << std::endl;

    v1[0] = 10000;
    v2[0] = 10000;
    v3[0] = -10001;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);

    context->getArena()->Reset();
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
    //create expression
    DataExpr *valueExpr = new DataExpr(1, INT32D);
    DataExpr *lowerExpr = new DataExpr(0, INT32D);
    DataExpr *upperExpr = new DataExpr(2, INT32D);
    BetweenExpr *expr = new BetweenExpr(valueExpr, lowerExpr, upperExpr);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);

    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {1001};
    int32_t v2[1] = {1001};
    int32_t v3[1] = {1001};
    int64_t *vals = new int64_t[3];

    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);

    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = 12356;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

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
    //create expression objects
    DataExpr *col01 = new DataExpr(0, INT32D);
    DataExpr *data01 = new DataExpr(100);
    BinaryExpr *condition = new BinaryExpr(GT, col01, data01, BOOLD);

    DataExpr *col02 = new DataExpr(0, INT32D);
    DataExpr *data02 = new DataExpr(200);
    BinaryExpr *texp = new BinaryExpr(GT, col02, data02, BOOLD);

    DataExpr *col03 = new DataExpr(0, INT32D);
    DataExpr *data03 = new DataExpr(0);
    BinaryExpr *fexp = new BinaryExpr(LT, col03, data03, BOOLD);

    IfExpr *expr = new IfExpr(condition, texp, fexp);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {1001};
    int32_t v2[1] = {0};
    int32_t v3[1] = {21};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

    v1[0] = -12;
    v2[0] = 1245;
    v3[0] = 123;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = -12222;
    v2[0] = -12312;
    v3[0] = 42;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

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
    DataExpr *col = new DataExpr(0, INT32D);
    std::vector<Expr *> args;
    args.push_back(col);
    for (int32_t i = 1; i<6; i++){
        args.push_back(new DataExpr(i));
    }
    InExpr *expr = new InExpr(args);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {1};
    int32_t v2[1] = {0};
    int32_t v3[1] = {21};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = 3;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = 5;
    v2[0] = 1245;
    v3[0] = 123;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = 0;
    v2[0] = -12312;
    v3[0] = 42;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

    v1[0] = 123;
    v2[0] = -43;
    v3[0] = 542;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
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
    // create expression objects
    DataExpr *col0 = new DataExpr(0, INT32D);
    std::vector<Expr *> args01;
    args01.push_back(col0);
    FuncExpr *cast0 = new FuncExpr("CAST", args01, DOUBLED);
    std::vector<Expr *> args02;
    args02.push_back(cast0);
    FuncExpr *abs0 = new FuncExpr("abs", args02, DOUBLED);

    DataExpr *col1 = new DataExpr(1, INT32D);
    std::vector<Expr *> args11;
    args11.push_back(col1);
    FuncExpr *cast1 = new FuncExpr("CAST", args11, DOUBLED);
    std::vector<Expr *> args12;
    args12.push_back(cast1);
    FuncExpr *abs1 = new FuncExpr("abs", args12, DOUBLED);

    BinaryExpr *expr = new BinaryExpr(EQ, abs0, abs1, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_DOUBLE) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {10000};
    int64_t v2[1] = {10000};
    double v3[1] = {12.34};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = 2000000000;
    v2[0] = 3000000000;
    v3[0] = -234;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

    v1[0] = -1000000;
    v2[0] = -1000000;
    v3[0] = 133.324234;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

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
    //create expression objects
    DataExpr *col1 = new DataExpr(1, INT64D);
    std::vector<Expr *> args;
    args.push_back(col1);
    FuncExpr *cast = new FuncExpr("CAST", args, DOUBLED);
    DataExpr *col2 = new DataExpr(2, DOUBLED);
    BinaryExpr *expr = new BinaryExpr(GT, cast, col2, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_DOUBLE) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {324233};
    int64_t v2[1] = {12};
    double v3[1] = {12.34};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

    v1[0] = 2000000000;
    v2[0] = -233;
    v3[0] = -234.2142;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = -1000000;
    v2[0] = 12;
    v3[0] = 12;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

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
    //create expression objects
    DataExpr *col = new DataExpr(2, VARCHARD);
    DataExpr *data = new DataExpr(new std::string(".*hello.*world.*"));
    std::vector<Expr *> args;
    args.push_back(col);
    args.push_back(data);
    FuncExpr *expr = new FuncExpr("LIKE", args, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);;
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[1];
    string s2[1];

    int32_t v1[1] = {8766};
    s1[0] = "asdf";
    s2[0] = "asjd fehellojdsl kfjworlddslk  jf ";
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = {8766};
    s1[0] = "asdf";
    s2[0] = "asjd fehell ojdsl kfjwo rld dslk  jf ";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[2][1] = s2->length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

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
    DataExpr *col2 = new DataExpr(2, VARCHARD);
    std::vector<Expr *> args;
    args.push_back(col2);
    FuncExpr *cast = new FuncExpr("CAST", args, INT32D);
    DataExpr *col0 = new DataExpr(0, INT32D);
    BinaryExpr *expr = new BinaryExpr(GT, cast, col0, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[1];
    string s2[1];

    int32_t v1[1] = {8766};
    s1[0] = "asdf";
    s2[0] = "1994-01-01";
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "1996-01-02";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "1993-11-12";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

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
    //create the expression objects
    DataExpr *col2 = new DataExpr(2, VARCHARD);
    DataExpr *substrIndex = new DataExpr(1);
    DataExpr *substrLen = new DataExpr(2);
    std::vector<Expr*> substrArgs;
    substrArgs.push_back(col2);
    substrArgs.push_back(substrIndex);
    substrArgs.push_back(substrLen);
    FuncExpr *substrExpr = new FuncExpr("substr", substrArgs, VARCHARD);

    std::vector<Expr *> args;
    args.push_back(substrExpr);
    args.push_back(new DataExpr(new std::string("12")));
    args.push_back(new DataExpr(new std::string("21")));
    args.push_back(new DataExpr(new std::string("13")));
    args.push_back(new DataExpr(new std::string("31")));
    args.push_back(new DataExpr(new std::string("34")));
    args.push_back(new DataExpr(new std::string("43")));

    InExpr *expr = new InExpr(args);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[1];
    string s2[1];

    int32_t v1[1] = {8766};
    s1[0] = "asdf";

    s2[0] = "2134121";

    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);

    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "233425";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "424321";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

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
    DataExpr *col1 = new DataExpr(1, VARCHARD);
    DataExpr *col2 = new DataExpr(2, VARCHARD);
    std::vector<Expr*> concatArgs;
    concatArgs.push_back(col1);
    concatArgs.push_back(col2);
    FuncExpr *concatExpr = new FuncExpr("concat", concatArgs, VARCHARD);

    DataExpr *helloWorldExpr = new DataExpr(new std::string("helloworld"));
    BinaryExpr *expr = new BinaryExpr(EQ, concatExpr, helloWorldExpr, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[1];
    string s2[1];

    int32_t v1[1] = {8766};
    s1[0] = "hello";
    s2[0] = "world";
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);

    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = {8766};
    s1[0] = "hello";
    s2[0] = "world ";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

    v1[0] = {8766};
    s1[0] = "hello ";
    s2[0] = "world";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

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
    DataExpr *col1 = new DataExpr(1, CHARD);
    col1->width = 30;
    DataExpr *commaExpr = new DataExpr(new std::string(","));
    std::vector<Expr *> innerArgs;
    innerArgs.push_back(col1);
    innerArgs.push_back(commaExpr);
    FuncExpr *innerConcat = new FuncExpr("concat", innerArgs, CHARD);
    innerConcat->width = 32;

    DataExpr *col2 = new DataExpr(2, CHARD);
    col2->width = 20;
    std::vector<Expr *> outerArgs;
    outerArgs.push_back(innerConcat);
    outerArgs.push_back(col2);
    FuncExpr *outerConcat = new FuncExpr("concat", outerArgs, CHARD);
    outerConcat->width = 52;

    DataExpr *helloExpr = new DataExpr(new std::string("hello                         , world"));
    BinaryExpr *expr = new BinaryExpr(EQ, outerConcat, helloExpr, BOOLD);

    auto charTypeA = new CharVecType(30);
    auto charTypeB = new CharVecType(20);
    std::vector<VecType> vecOfTypes = { IntVecType(), VecType(*charTypeA), VecType(*charTypeB)};
    VecTypes types(vecOfTypes);
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
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);

    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = {8766};
    s1[0] = "hello";
    s2[0] = "world ";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

    v1[0] = {8766};
    s1[0] = "hello ";
    s2[0] = "world";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

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

TEST(CodeGenTest, StringWithOps)
{
    //create expression objects
    DataExpr *col21 = new DataExpr(2, VARCHARD);
    DataExpr *sunday = new DataExpr(new std::string("Sunday"));
    BinaryExpr *eqExpr1 = new BinaryExpr(EQ, col21, sunday, BOOLD);

    DataExpr *col22 = new DataExpr(2, VARCHARD);
    DataExpr *saturday = new DataExpr(new std::string("Saturday"));
    BinaryExpr *eqExpr2 = new BinaryExpr(EQ, col22, saturday, BOOLD);

    BinaryExpr *expr = new BinaryExpr(OR, eqExpr1, eqExpr2, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    string s1[1];
    string s2[1];

    int32_t v1[1] = {8766};
    s1[0] = "asdf";
    s2[0] = "Saturday";

    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "Sunday";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();
    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "Monday";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();
    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();
    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

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
    DataExpr *value1 = new DataExpr(0, INT64D);
    DataExpr *value2 = new DataExpr(0);
    value2->longVal = 0;
    value2->dataType = INT64D;
    CoalesceExpr *coalesceExpr = new CoalesceExpr(value1, value2);

    DataExpr *right = new DataExpr(123);
    right->longVal = 123;
    right->dataType = INT64D;
    BinaryExpr *expr = new BinaryExpr(EQ, coalesceExpr, right, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int64_t v1[1] = {123};
    int64_t v2[1] = {234};
    int64_t v3[1] = {345};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    bitmap[0][0] = true;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

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
    DataExpr *value1 = new DataExpr(0, INT64D);
    DataExpr *value2 = new DataExpr(100);
    value2->longVal = 100;
    value2->dataType = INT64D;
    CoalesceExpr *coalesceExpr = new CoalesceExpr(value1, value2);

    DataExpr *right = new DataExpr(100);
    right->longVal = 100;
    right->dataType = INT64D;
    BinaryExpr *expr = new BinaryExpr(EQ, coalesceExpr, right, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t c[3] = {100, 200, 300};
    int64_t *vals = new int64_t[1];
    vals[0] = (int64_t)c;
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

    int32_t r = func(vals, 3, (int64_t)oVec, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);
    EXPECT_EQ(oVec[0], true);
    EXPECT_EQ(oVec[1], false);
    EXPECT_EQ(oVec[2], false);
    context->getArena()->Reset();

    for (int i = 0; i < 3; i++) {
        bitmap[0][0] = true;
    }
    r = func(vals, 3, (int64_t)oVec, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);
    EXPECT_EQ(oVec[0], true);
    EXPECT_EQ(oVec[1], false);
    EXPECT_EQ(oVec[2], false);
    context->getArena()->Reset();

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
    DataExpr *col0 = new DataExpr(0, INT64D);
    IsNullExpr *expr = new IsNullExpr(col0);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t c[3] = {100, 200, 300};
    int64_t *vals = new int64_t[1];
    vals[0] = (int64_t)c;
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

    int32_t r = func(vals, 3, (int64_t)oVec, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);
    EXPECT_EQ(oVec[0], false);
    EXPECT_EQ(oVec[1], false);
    EXPECT_EQ(oVec[2], false);
    context->getArena()->Reset();

    for (int i = 0; i < 3; i++) {
        bitmap[0][i] = true;
    }
    r = func(vals, 3, (int64_t)oVec, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);
    EXPECT_EQ(oVec[0], true);
    EXPECT_EQ(oVec[1], true);
    EXPECT_EQ(oVec[2], true);
    context->getArena()->Reset();

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
    DataExpr *col0 = new DataExpr(0, INT64D);
    IsNullExpr *expr = new IsNullExpr(col0);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t v1[1] = {123};
    auto *vals = new int64_t[1];
    vals[0] = (int64_t)v1;
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, false);
    context->getArena()->Reset();

    bitmap[0][0] = true;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, true);
    context->getArena()->Reset();

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
    DataExpr *col0 = new DataExpr(0, INT64D);
    IsNullExpr *isNull = new IsNullExpr(col0);
    UnaryExpr *expr = new UnaryExpr(NOT, isNull, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t v1[1] = {123};
    auto *vals = new int64_t[1];
    vals[0] = (int64_t)v1;
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, true);
    context->getArena()->Reset();

    bitmap[0][0] = true;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, false);
    context->getArena()->Reset();

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
    DataExpr *addLeft = new DataExpr(0, DECIMAL128D);
    DataExpr *addRight = new DataExpr(5);
    addRight->doubleVal = 5;
    addRight->longVal = 5;
    BinaryExpr *addExpr = new BinaryExpr(ADD, addLeft, addRight, DECIMAL128D);

    DataExpr *equalRight = new DataExpr(15);
    equalRight->doubleVal = 15;
    equalRight->longVal = 15;

    BinaryExpr *expr = new BinaryExpr(EQ, addExpr, equalRight, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    // creating decimal
    int64_t c1[4] = {10, 0, 9, 0};

    int64_t c[2] = {(int64_t) c1, (int64_t) (c1 + 2)};

    int64_t *vals = new int64_t[1];
    vals[0] = (int64_t)c;

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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

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
    DataExpr *col0 = new DataExpr(0, DECIMAL128D);
    DataExpr *col1 = new DataExpr(1, DECIMAL128D);
    DataExpr *col2 = new DataExpr(2, DECIMAL128D);

    BetweenExpr *expr = new BetweenExpr(col0, col1, col2);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DECIMAL128), VecType(OMNI_VEC_TYPE_DECIMAL128), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    // creating decimal
    int64_t c1[2] = {4000, 0};
    int64_t c[1] = {(int64_t) c1};

    int64_t d1[2] = {5000, 0};
    int64_t d[1] = {(int64_t) d1};

    int64_t e1[2] = {15000, 0};
    int64_t e[1] = {(int64_t) e1};

    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)c;
    vals[1] = (int64_t)d;
    vals[2] = (int64_t)e;

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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
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
    DataExpr *col01 = new DataExpr(0, DECIMAL128D);
    DataExpr *data01 = new DataExpr(100);
    data01->longVal = 100;
    data01->doubleVal = 100;
    BinaryExpr *condition = new BinaryExpr(GT, col01, data01, BOOLD);

    DataExpr *col02 = new DataExpr(0, DECIMAL128D);
    DataExpr *data02 = new DataExpr(200);
    data02->longVal = 200;
    data02->doubleVal = 200;
    BinaryExpr *texp = new BinaryExpr(GT, col02, data02, BOOLD);

    DataExpr *col03 = new DataExpr(0, DECIMAL128D);
    DataExpr *data03 = new DataExpr(0);
    data03->longVal = 0;
    data03->doubleVal = 0;
    BinaryExpr *fexp = new BinaryExpr(LT, col03, data03, BOOLD);

    IfExpr *expr = new IfExpr(condition, texp, fexp);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DECIMAL128), VecType(OMNI_VEC_TYPE_DECIMAL128), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    // creating decimal
    int64_t c1[2] = {-12222, -1};
    int64_t c[1] = {(int64_t) c1};

    int64_t d1[2] = {-12312, -1};
    int64_t d[1] = {(int64_t) d1};

    int64_t e1[2] = {42, 0};
    int64_t e[1] = {(int64_t) e1};

    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)c;
    vals[1] = (int64_t)d;
    vals[2] = (int64_t)e;

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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

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

TEST(CodeGenTest, ProjectionSubtractNulls)
{
    // create expression objects
    DataExpr *subLeft = new DataExpr(0, INT64D);
    DataExpr *subRight = new DataExpr(100);
    subRight->longVal = 100;

    BinaryExpr *expr = new BinaryExpr(SUB, subLeft, subRight, INT64D);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t c[3] = {10, 20, 30};

    int64_t *vals = new int64_t[1];
    vals[0] = (int64_t)c;

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

    vector<int64_t> oVec(3);
    auto ov = oVec.data();
    void *vecVals = &ov;
    auto cvecVals = static_cast<int64_t *>(vecVals);
    auto context = new ExecutionContext();
    auto func = (ProjectFunc)(intptr_t)codegen->GetFunction();

    int32_t r = func(vals, 3, *cvecVals, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);
    EXPECT_EQ(oVec[0], -90);
    EXPECT_EQ(oVec[1], -80);
    EXPECT_EQ(oVec[2], -70);
    context->getArena()->Reset();

    for (int i = 0; i < 3; i++) {
        bitmap[0][i] = true;
    }
    r = func(vals, 3, *cvecVals, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(newNullValues[0], true);
    EXPECT_EQ(newNullValues[1], true);
    EXPECT_EQ(newNullValues[2], true);
    EXPECT_EQ(oVec[0], 0);
    EXPECT_EQ(oVec[1], 0);
    EXPECT_EQ(oVec[2], 0);
    context->getArena()->Reset();
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
    DataExpr *addLeft = new DataExpr(0, DECIMAL128D);
    DataExpr *addRight = new DataExpr(100);
    addRight->longVal = 100;
    addRight->doubleVal = 100;

    BinaryExpr *expr = new BinaryExpr(ADD, addLeft, addRight, DECIMAL128D);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    // creating decimal
    int64_t c1[6] = {10, 0, 20, 0, 30, 0};
    int64_t c[3] = {(int64_t) c1, (int64_t) (c1 + 2), (int64_t) (c1 + 4)};

    int64_t *vals = new int64_t[1];
    vals[0] = (int64_t)c;

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

    vector<int64_t> oVec(3);
    auto ov = oVec.data();
    void *vecVals = &ov;
    auto cvecVals = static_cast<int64_t *>(vecVals);
    auto context = new ExecutionContext();
    auto func = (ProjectFunc)(intptr_t)codegen->GetFunction();

    int32_t r = func(vals, 3, *cvecVals, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    int64_t *result = reinterpret_cast<int64_t *>(oVec[0]);
    EXPECT_EQ(newNullValues[0], false);
    EXPECT_EQ(newNullValues[1], false);
    EXPECT_EQ(newNullValues[2], false);

    EXPECT_EQ(*result, 110);
    EXPECT_EQ(*(result + 1), 0);

    result = reinterpret_cast<int64_t *>(oVec[1]);
    EXPECT_EQ(*(result), 120);
    EXPECT_EQ(*(result + 1), 0);

    result = reinterpret_cast<int64_t *>(oVec[2]);
    EXPECT_EQ(*(result), 130);
    EXPECT_EQ(*(result + 1), 0);
    context->getArena()->Reset();

    for (int i = 0; i < 3; i++) {
        bitmap[0][i] = true;
    }
    r = func(vals, 3, *cvecVals, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(newNullValues[0], true);
    EXPECT_EQ(newNullValues[1], true);
    EXPECT_EQ(newNullValues[2], true);

    result = reinterpret_cast<int64_t *>(oVec[0]);
    EXPECT_EQ(*result, 0);
    EXPECT_EQ(*(result + 1), 0);
    result = reinterpret_cast<int64_t *>(oVec[1]);
    EXPECT_EQ(*(result), 0);
    EXPECT_EQ(*(result + 1), 0);

    result = reinterpret_cast<int64_t *>(oVec[2]);
    EXPECT_EQ(*(result), 0);
    EXPECT_EQ(*(result + 1), 0);

    context->getArena()->Reset();
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
    auto * vector = CreateVector<LongVector>(values, 10);
    auto slicedVector = vector->Slice(4, 6);

    // create expression objects
    DataExpr *addLeft = new DataExpr(0, INT64D);
    DataExpr *addRight = new DataExpr(100);
    addRight->longVal = 100;

    BinaryExpr *expr = new BinaryExpr(ADD, addLeft, addRight, INT64D);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    RowProjection rowProjection(*expr);
    RowProjFunc func = rowProjection.Create();

    int32_t positionOffset = slicedVector->GetPositionOffset();
    int64_t *valueAddress = (int64_t *)(slicedVector->GetValues()) + positionOffset;
    bool *nullAddress = (bool *)(slicedVector->GetValueNulls()) + positionOffset;
    int32_t *offsetAddress = (int32_t *)(slicedVector->GetValueOffsets()) + positionOffset;

    int64_t valuesAddr[1] = {(int64_t)(valueAddress)};
    int64_t nullsAddr[1] = {(int64_t)(nullAddress)};
    int64_t offsetsAddr[1] = {(int64_t)(offsetAddress)};
    bool isNull;
    int32_t length;
    int64_t dictVecAddr[1] = {0};
    auto context = new ExecutionContext();
    for (int32_t i = 0; i < slicedVector->GetSize(); i++) {
        isNull = false;
        void *valuePtr = func(valuesAddr, nullsAddr, offsetsAddr, i, &length, reinterpret_cast<int64_t>(context), dictVecAddr, &isNull);
        int64_t value = *((int64_t *)valuePtr);
        int64_t inputValue = slicedVector->GetValue(i);
        EXPECT_EQ(value, inputValue + 100);
    }
    context->getArena()->Reset();

    delete slicedVector;
    delete vector;
    delete context;
}

TEST(CodeGenTest, TestRowProjectVarchar)
{
    omniruntime::vec::VarcharVecType type(10);
    std::string values[2] = {"hello", "world"};
    omniruntime::vec::VarcharVector *vector = CreateVarcharVector(type, values, 2);
    auto slicedVector = vector->Slice(1, 1);

    // create expression objects
    DataExpr *substrData = new DataExpr(0, VARCHARD);
    DataExpr *substrIndex = new DataExpr(1);
    DataExpr *substrLen = new DataExpr(5);
    std::vector<Expr *> args;
    args.push_back(substrData);
    args.push_back(substrIndex);
    args.push_back(substrLen);
    FuncExpr *expr = new FuncExpr("substr", args, VARCHARD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    RowProjection rowProjection(*expr);
    RowProjFunc func = rowProjection.Create();

    int32_t positionOffset = slicedVector->GetPositionOffset();
    uint8_t *valueAddress = (uint8_t *)(slicedVector->GetValues());
    bool *nullAddress = (bool *)(slicedVector->GetValueNulls()) + positionOffset;
    int32_t *offsetAddress = (int32_t *)(slicedVector->GetValueOffsets()) + positionOffset;

    int64_t valuesAddr[1] = {(int64_t)(valueAddress)};
    int64_t valueNulls[1] = {(int64_t)(nullAddress)};
    int64_t valueOffsets[1] = {(int64_t)(offsetAddress)};
    int32_t length;
    int64_t dictionaries[1] = {};
    bool isNull;
    auto context = new ExecutionContext();
    for (int32_t i = 0; i < slicedVector->GetSize(); i++) {
        isNull = false;
        void *valuePtr = func(valuesAddr, valueNulls, valueOffsets, i, &length, reinterpret_cast<int64_t>(context), dictionaries, &isNull);
        uint8_t *value = *reinterpret_cast<uint8_t **>(reinterpret_cast<uintptr_t>(valuePtr));
        std::string result(value, value + length);

        uint8_t *expectValue = nullptr;
        int32_t expectLen = slicedVector->GetValue(i, &expectValue);
        std::string expectResult(expectValue, expectValue + expectLen);
        EXPECT_EQ(result, expectResult.substr(0, 5));
    }
    context->getArena()->Reset();

    delete slicedVector;
    delete vector;
    delete context;
}

TEST(CodeGenTest, CastNumbers3)
{
    // create expression objects
    std::vector<Expr *> args;
    DataExpr *col1 =  new DataExpr(1, DOUBLED);

    DataExpr *col2 = new DataExpr(2, DOUBLED);
    BinaryExpr *expr = new BinaryExpr(LT, col1, col2, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DOUBLE) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);
    cout << endl;

    int32_t v1[1] = {10000};
    double v2[1] = {9.01};
    double v3[1] = {12.34};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);
    context->getArena()->Reset();

    v1[0] = 2000000000;
    v2[0] = -100.22;
    v3[0] = -234.2142;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

    v1[0] = -1000000;
    v2[0] = 12;
    v3[0] = 12;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 0);
    context->getArena()->Reset();

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

TEST (CodeGenTest, Substr) {
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    const int32_t numCols = 2;
    const int32_t numRows = 1;

    string s1[1];
    string s2[1];
    s1[0] = "SUBSTR Function";
    s2[0] = "Function SUBSTR";

    int64_t *vals = new int64_t[2];
    vals[0] = (int64_t) s1->c_str();
    vals[1] = (int64_t) s2->c_str();

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
    DataExpr *substrData1 = new DataExpr(0, VARCHARD);
    DataExpr *substrIndex1 = new DataExpr(-5 );
    DataExpr *substrLen1 = new DataExpr(5 );
    std::vector<Expr *> args1;
    args1.push_back(substrData1);
    args1.push_back(substrIndex1);
    args1.push_back(substrLen1);
    FuncExpr *substrExp1 = new FuncExpr("substr", args1, VARCHARD);

    DataExpr *ctionExpr= new DataExpr(new std::string("ction"));
    BinaryExpr *expr1 = new BinaryExpr(EQ, substrExp1, ctionExpr, BOOLD);

    int64_t dictionaries[numCols] = {};

    auto filter = new RowFilter(*expr1);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    bool res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0, reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(res, true);
    delete filter;

    // create expression objects
    DataExpr *substrData2 = new DataExpr(1, VARCHARD);
    DataExpr *substrIndex2 = new DataExpr(-5);
    std::vector<Expr *> args2;
    args2.push_back(substrData2);
    args2.push_back(substrIndex2);;
    FuncExpr *substrExp2 = new FuncExpr("substr", args2, VARCHARD);

    DataExpr *ubsterExpr= new DataExpr(new std::string("UBSTR"));
    BinaryExpr *expr2 = new BinaryExpr(EQ, substrExp2, ubsterExpr, BOOLD);

    filter = new RowFilter(*expr2);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0, reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(res, true);
    delete filter;

    // create expression objects
    DataExpr *substrData3 = new DataExpr(0, VARCHARD);
    DataExpr *substrIndex3 = new DataExpr(4);
    std::vector<Expr *> args3;
    args3.push_back(substrData3);
    args3.push_back(substrIndex3);;
    FuncExpr *substrExp3 = new FuncExpr("substr", args3, VARCHARD);

    DataExpr *STRExpr= new DataExpr(new std::string("STR Function"));
    BinaryExpr *expr3 = new BinaryExpr(EQ, substrExp3, STRExpr, BOOLD);

    filter = new RowFilter(*expr3);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0, reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(res, true);
    context->getArena()->Reset();

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
    DataExpr *col0 = new DataExpr(0, INT32D);
    DataExpr *data = new DataExpr(42);
    std::vector<Expr *> hashArgs;
    hashArgs.push_back(col0);
    hashArgs.push_back(data);
    FuncExpr *mhash = new FuncExpr("mm3hash", hashArgs, INT32D);

    DataExpr *equalRight = new DataExpr(723455942);
    equalRight->longVal = 723455942;

    BinaryExpr *expr = new BinaryExpr(EQ, mhash, equalRight, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int32_t v1[1] = {-2147483648};
    auto *vals = new int64_t[1];
    vals[0] = (int64_t)v1;
    auto *selected = new int32_t[1];

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;
    auto **offsets = new int32_t *[1];
    offsets[0] = new int32_t[1];
    offsets[0][0] = 0;

    auto codegen = FilterCodeGen::Create(defaultTestFunctionName, *expr);
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();
    auto func = (FilterFunc)(intptr_t)codegen->GetFunction();

    bool result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, true);
    context->getArena()->Reset();

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
    std::vector<Expr *> args;
    args.push_back(new DataExpr(0, INT64D));
    args.push_back(new DataExpr(42));
    FuncExpr *expr = new FuncExpr("mm3hash", args, INT32D);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);

    int64_t v1[1] = {-2147483648};
    auto *vals = new int64_t[1];
    vals[0] = (int64_t)v1;

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;
    auto **offsets = new int32_t *[1];
    offsets[0] = new int32_t[1];
    offsets[0][0] = 0;

    RowProjection rowProjection(*expr);
    RowProjFunc func = rowProjection.Create();
    EXPECT_EQ(rowProjection.GetReturnType(), INT32D);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    bool isNull = false;
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();

    int32_t res = *((int32_t *)func(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, dataLength,
        reinterpret_cast<int64_t>(context), dictionaries, &isNull));
    int32_t expected_res = Mm3Int64(v1[0], false, 42);
    EXPECT_EQ(res, expected_res);
    context->getArena()->Reset();

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
    std::vector<Expr *> args;
    args.push_back(new DataExpr(0, DOUBLED));
    args.push_back(new DataExpr(42));
    FuncExpr *expr = new FuncExpr("mm3hash", args, INT32D);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DOUBLE) };
    VecTypes types(vecOfTypes);

    double v1[1] = {123.456};
    auto *vals = new int64_t[1];
    vals[0] = (int64_t)v1;

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;
    auto **offsets = new int32_t *[1];
    offsets[0] = new int32_t[1];
    offsets[0][0] = 0;

    RowProjection rowProjection(*expr);
    RowProjFunc func = rowProjection.Create();
    EXPECT_EQ(rowProjection.GetReturnType(), INT32D);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    bool isNull = false;
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();

    int32_t res = *((int32_t *)func(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, dataLength,
        reinterpret_cast<int64_t>(context), dictionaries, &isNull));
    int32_t expected_res = Mm3Double(v1[0], false, 42);
    EXPECT_EQ(res, expected_res);
    context->getArena()->Reset();

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
    std::vector<Expr *> args;
    args.push_back(new DataExpr(0, VARCHARD));
    args.push_back(new DataExpr(42));
    FuncExpr *expr = new FuncExpr("mm3hash", args, INT32D);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);

    std::string v1 = "hello world";
    auto *vals = new int64_t[1];
    vals[0] = (int64_t)v1.c_str();

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;
    auto **offsets = new int32_t *[1];
    offsets[0] = new int32_t[2];
    offsets[0][0] = 0;
    offsets[0][1] = v1.size();

    RowProjection codegen(*expr);
    RowProjFunc func = codegen.Create();
    EXPECT_EQ(codegen.GetReturnType(), INT32D);

    int32_t *dataLength = new int32_t[1];
    dataLength[0] = 0;
    bool isNull = false;
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();

    int32_t res = *((int32_t *)func(vals, (int64_t *)bitmap, (int64_t *)offsets, 0, dataLength,
        reinterpret_cast<int64_t>(context), dictionaries, &isNull));
    int32_t expected_res = Mm3String(v1.c_str(), v1.size(), false, 42);
    EXPECT_EQ(res, expected_res);
    context->getArena()->Reset();

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] offsets[0];
    delete[] offsets;
    delete[] vals;
    delete[] dataLength;
    delete context;
}

TEST(CodeGenTest, SubstrWithChars)
{
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_CHAR), VecType(OMNI_VEC_TYPE_CHAR) };
    VecTypes types(vecOfTypes);
    const int32_t numCols = 2;
    const int32_t numRows = 1;

    string s1[1];
    string s2[1];
    s1[0] = "SUBSTR Function";
    s2[0] = "Function SUBSTR";

    int64_t *vals = new int64_t[2];
    vals[0] = (int64_t) s1->c_str();
    vals[1] = (int64_t) s2->c_str();

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
    DataExpr *substrData1 = new DataExpr(0, CHARD);
    substrData1->width = 0;
    DataExpr *substrIndex1 = new DataExpr(1);
    DataExpr *substrLen1 = new DataExpr(5);
    std::vector<Expr *> args1;
    args1.push_back(substrData1);
    args1.push_back(substrIndex1);
    args1.push_back(substrLen1);
    FuncExpr *substrExpr1 = new FuncExpr("substr", args1, CHARD);
    substrExpr1->width = 10;

    DataExpr *substrData2 = new DataExpr(1, CHARD);
    substrData2->width = 0;
    DataExpr *substrIndex2 = new DataExpr(1);
    DataExpr *substrLen2 = new DataExpr(5);
    std::vector<Expr *> args2;
    args2.push_back(substrData2);
    args2.push_back(substrIndex2);
    args2.push_back(substrLen2);
    FuncExpr *substrExpr2 = new FuncExpr("substr", args2, CHARD);
    substrExpr2->width = 10;

    BinaryExpr *expr = new BinaryExpr(NEQ, substrExpr1, substrExpr2, BOOLD);

    int64_t dictionaries[numCols] = {};
    auto filter = new RowFilter(*expr);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    bool res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0, reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(res, true);

    context->getArena()->Reset();

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
    DataExpr *col0 = new DataExpr(0, INT64D);
    DataExpr *col1 = new DataExpr(1, INT64D);
    std::vector<Expr*> args;
    args.push_back(col0);
    args.push_back(col1);
    FuncExpr *combineHash = new FuncExpr("combine_hash", args, INT32D);

    DataExpr *col2 = new DataExpr(2, INT64D);
    BinaryExpr *expr = new BinaryExpr(EQ, combineHash, col2, BOOLD);

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t v1[1] = {12};
    int64_t v2[1] = {123};
    int64_t v3[1] = {495};
    int64_t* vals = new int64_t[3];
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

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

    bool result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, true);
    context->getArena()->Reset();

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
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;
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

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);

    v1[0] = -1;
    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, 1);

    v1[0] = 2;
    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
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
