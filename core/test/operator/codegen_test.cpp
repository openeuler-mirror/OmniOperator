/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "gtest/gtest.h"

#include <iostream>
#include <string>
#include <vector>

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

// Filter is basically just a projection that must return a boolean.
// The logic for filtering out rows from final output is handled in C++ when
// processing each row individually, so to LLVM it is exactly the same as a projection.
// Check CodeGenTest.RowFilter for a dedicated test using the RowFilter class instead.
TEST(CodeGenTest, SimpleFilter)
{
    string unparsed = "$operator$LESS_THAN:4(#0, 50)";

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

    RowProjection lc(unparsed, types);
    RowProjFunc func = lc.Create();
    EXPECT_EQ(lc.GetReturnType(), BOOLD);

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
    }    delete[] col1;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
    delete dataLength;
    delete context;
}
// Simple project example using individual row processing.
TEST(CodeGenTest, SimpleProject)
{
    string unparsed = "ADD:1(#0, 50)";

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

    RowProjection lc(unparsed, types);
    RowProjFunc func = lc.Create();
    EXPECT_EQ(lc.GetReturnType(), INT32D);
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
    string unparsed = "IF:1($operator$GREATER_THAN:4(#1, 3000000000), ADD:1(#0, 10), MULTIPLY:1(#0, -1))";

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

    RowProjection lc(unparsed, types);
    RowProjFunc func = lc.Create();
    EXPECT_EQ(lc.GetReturnType(), INT32D);

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
    string unparsed = "#1";

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

    RowProjection lc(unparsed, types);
    RowProjFunc func = lc.Create();
    EXPECT_TRUE(lc.IsColumnProjection());
    EXPECT_EQ(lc.GetIndexIfColumnProjection(), 1);

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
    string unparsed = "$operator$EQUAL:4(#0, 0)";
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
    auto filter = new RowFilter(unparsed, vecTypes);
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

    string testname = "stringTest1";
    vector<DataType> typeVec = vector<DataType>(types, types + 2);

    std::string expr = "$operator$EQUAL:4(substr:15(#0, 1, 5), 'hello')";
    auto filter = new RowFilter(expr, vecTypes);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);

    bool res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(res, true);
    delete filter;

    expr = "$operator$EQUAL:4(substr:15(#1, 1, 5), 'hello')";
    filter = new RowFilter(expr, vecTypes);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);

    res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0, reinterpret_cast<int64_t>(context), dictionaryVectors);
    EXPECT_EQ(res, false);

    delete filter;
    expr = "$operator$EQUAL:4(substr:15(#0, 1, 5), substr:15(#1, 7, 11))";
    filter = new RowFilter(expr, vecTypes);
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
    string unparsed = "AND:4($operator$GREATER_THAN_OR_EQUAL:4(ADD:1(#0, 2), 4), AND:4($operator$LESS_THAN:4(#1, 4), "
                      "$operator$EQUAL:4(#2, 2)))";


    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);

    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed,types, 3);
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

    string testname = "simpleTest1";
    auto *lc = new FilterCodeGen(testname, *expr);


    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete expr;
    delete context;
}

TEST(CodeGenTest, MathFunctions1)
{
    string unparsed = "AND:4($operator$EQUAL:4(abs:1(#0), abs:1(#2)), $operator$EQUAL:4(abs:1(#0), abs:1(#1)))";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);

    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "simpleTest2";
    auto *lc = new FilterCodeGen(testname, *expr);

    int64_t dictionaries[3] = {};
    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete expr;
    delete context;
}


TEST(CodeGenTest, MathFunctions2)
{
    string unparsed = "BETWEEN:4(#1, #0, #2)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);

    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "simpleTest2";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete expr;
    delete context;
}


TEST(CodeGenTest, MathFunctions3)
{
    string unparsed = "IF:4($operator$GREATER_THAN:4(#0, 100), $operator$GREATER_THAN:4(#0, 200), "
                      "$operator$LESS_THAN:4(#0, 0))";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "simpleTest2";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete expr;
    delete context;
}


TEST(CodeGenTest, MathFunctions4)
{
    string unparsed = "IN:4(#0, 1, 2, 3, 4, 5)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "simpleTest2";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete expr;
    delete context;
}

// For testing different types
TEST(CodeGenTest, CastNumbers1)
{
    string unparsed = "$operator$EQUAL:4(abs:3(CAST:3(#0)), abs:3(CAST:3(#1)))";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_DOUBLE) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "simpleTest3";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete expr;
    delete context;
}

TEST(CodeGenTest, CastNumbers2)
{
    string unparsed = "$operator$GREATER_THAN:4(CAST:3(#1), #2)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_DOUBLE) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "simpleTest3";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete expr;
    delete context;
}


TEST(CodeGenTest, Like)
{
    string unparsed = "LIKE:4(#2, '%hello%world%')";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "stringTest1";
    auto lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}


TEST(CodeGenTest, DateCast)
{
    string unparsed = "$operator$GREATER_THAN:4(CAST:1(#2), #0)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "stringTest1";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, SubstrIn)
{
    string unparsed = "IN:4(substr:15(#2, 1, 2), '12', '21', '13', '31', '34', '43')";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "stringTest1";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, ConcatStr)
{
    string unparsed = "$operator$EQUAL:4(concat:15(#1, #2), 'helloworld')";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "stringTest1";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, ConcatChars)
{
    string unparsed = "$operator$EQUAL:4(concat:16[52](concat:16[32](#1, ','), #2), 'hello                         , world')";
    auto charTypeA = new CharVecType(30);
    auto charTypeB = new CharVecType(20);
    std::vector<VecType> vecOfTypes = { IntVecType(), VecType(*charTypeA), VecType(*charTypeB)};
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "stringTestConcat";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete expr;
    delete lc;
    delete context;
}

TEST(CodeGenTest, StringWithOps)
{
    string unparsed = "OR:4($operator$EQUAL:4(#2, 'Sunday'), $operator$EQUAL:4(#2, 'Saturday'))";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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


    string testname = "stringTest1";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};
    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, Coalesce)
{
    string unparsed = "$operator$EQUAL:4(COALESCE:2(#0, 0), 123)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "coalesceTest1";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, ProjectionCoalesce)
{
    string unparsed = "$operator$EQUAL:4(COALESCE:2(#0, 100), 100)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "CoalesceProjectTest";
    auto *lc = new ProjectionCodeGen(testname, *expr, false);
    int64_t dictionaryVectors[1] = {};

    bool oVec[3];
    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *, int32_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *, int32_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();int32_t r = func(vals, 3, (int64_t)oVec, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
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
    delete lc;
    delete context;
}

TEST(CodeGenTest, ProjectionIsNull)
{
//    string unparsed = "$operator$EQUAL:boolean(COALESCE:long(#0, 100), 100)";
    string unparsed = "IS_NULL:boolean(#0)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "IsNullProjectTest";
    auto *lc = new ProjectionCodeGen(testname, *expr, false);
    int64_t dictionaryVectors[1] = {};

    bool oVec[3];
    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *, int32_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *, int32_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();int32_t r = func(vals, 3, (int64_t)oVec, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths, reinterpret_cast<int64_t>(context), dictionaryVectors);
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
    delete lc;
    delete context;
}

TEST(CodeGenTest, IsNull)
{
    string unparsed = "IS_NULL:4(#0)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 1);
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

    string testName = "isNullTest";
    auto *lc = new FilterCodeGen(testName, *expr);
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, IsNotNull)
{
    string unparsed = "IS_NOT_NULL:4(#0)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 1);
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
    string testName = "isNotNullTest";
    auto *lc = new FilterCodeGen(testName, *expr);
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, DecimalOperators1)
{
    string unparsed = "$operator$EQUAL:4(ADD:7(#0, 5), 15)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 1);
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

    string testname = "DecimalTest1";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, DecimalOperators2)
{
    string unparsed = "BETWEEN:4(#0, #1, #2)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DECIMAL128), VecType(OMNI_VEC_TYPE_DECIMAL128), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "DecimalBetweenTest";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};
    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, DecimalOperators3)
{
    string unparsed = "IF:4($operator$GREATER_THAN:4(#0, 100), $operator$GREATER_THAN:4(#0, 200), "
                      "$operator$LESS_THAN:4(#0, 0))";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DECIMAL128), VecType(OMNI_VEC_TYPE_DECIMAL128), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "DecimalBetweenTest";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, ProjectionSubtractNulls)
{
    string unparsed = "$operator$SUBTRACT:2(#0, 100)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "SubtractNullProjectTest";
    auto *lc = new ProjectionCodeGen(testname, *expr, false);
    int64_t dictionaryVectors[1] = {};

    vector<int64_t> oVec(3);
    auto ov = oVec.data();
    void *vecVals = &ov;
    auto cvecVals = static_cast<int64_t *>(vecVals);
    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *, int32_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *, int32_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, ProjectionCodeGen)
{
    string unparsed = "$operator$ADD:7(#0, 100)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "DecimalProjectTest";
    auto *lc = new ProjectionCodeGen(testname, *expr, false);
    int64_t dictionaryVectors[1] = {};

    vector<int64_t> oVec(3);
    auto ov = oVec.data();
    void *vecVals = &ov;
    auto cvecVals = static_cast<int64_t *>(vecVals);
    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *, int32_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *, int32_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
    delete context;
}

TEST(CodeGenTest, TestRowProjectLong)
{
    int64_t values[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    omniruntime::vec::LongVector * vector = CreateVector<LongVector>(values, 10);
    auto slicedVector = vector->Slice(4, 6);

    std::string expr = "$operator$ADD:2(#0, 100)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    RowProjection rowProjection(expr, types);
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

    std::string expr = "substr:15(#0, 1, 5)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes types(vecOfTypes);
    RowProjection rowProjection(expr, types);
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
    string unparsed = "$operator$LESS_THAN:4(CAST:3(#1), #2)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DOUBLE) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testname = "castTest3";
    auto *lc = new FilterCodeGen(testname, *expr);
    int64_t dictionaries[3] = {};

    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();

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
    delete lc;
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

    string testname = "stringTest";
    std::string expr = "$operator$EQUAL:4(substr:15(#0, -5, 5), 'ction')";
    int64_t dictionaries[numCols] = {};

    auto filter = new RowFilter(expr, types);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    bool res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0, reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(res, true);
    delete filter;

    expr = "$operator$EQUAL:4(substr:15(#1, -5), 'UBSTR')";

    filter = new RowFilter(expr, types);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create();
    EXPECT_FALSE(filter == nullptr);
    res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0, reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(res, true);
    delete filter;

    expr = "$operator$EQUAL:4(substr:15(#0, 4), 'STR Function')";

    filter = new RowFilter(expr, types);
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

TEST(CodeGenTest, Mm3Hash)
{
    string unparsed = "$operator$EQUAL:4(mm3hash:1(#0, 42), 723455942)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 1);
    ExprPrinter printExprTree;
    expr->Accept(printExprTree);

    int64_t v1[1] = {-2147483648};
    auto *vals = new int64_t[1];
    vals[0] = (int64_t)v1;
    auto *selected = new int32_t[1];

    bool **bitmap = new bool *[1];
    bitmap[0] = new bool[1];
    bitmap[0][0] = false;
    auto **offsets = new int32_t *[1];
    offsets[0] = new int32_t[1];
    offsets[0][0] = 0;


    string testName = "mm3hashTest";
    auto *lc = new FilterCodeGen(testName, *expr);
    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    int64_t dictionaries[1] = {};

    auto context = new ExecutionContext();

    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();
    bool result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, true);
    context->getArena()->Reset();

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}


TEST (CodeGenTest, SubstrWithChars) {
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

    string testname = "stringTest";
    std::string expr = "$operator$NOT_EQUAL:4(substr:16[10](#0, 1, 5), substr:16[10](#1, 1, 5))";
    int64_t dictionaries[numCols] = {};

    auto filter = new RowFilter(expr, types);
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
    string unparsed = "$operator$EQUAL:4(combine_hash:1(#0, #1), #2)";

    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_LONG), VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes types(vecOfTypes);
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, types, 3);
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

    string testName = "CombineHashTest";
    auto *lc = new FilterCodeGen(testName, *expr);
    int64_t dictionaries[3] = {};
    auto context = new ExecutionContext();

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *, int64_t, int64_t *))(intptr_t)lc->GetFunction();
    bool result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets), reinterpret_cast<int64_t>(context), dictionaries);
    EXPECT_EQ(result, true);
    context->getArena()->Reset();

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}