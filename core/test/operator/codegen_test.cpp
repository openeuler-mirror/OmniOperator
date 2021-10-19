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
#include "../../src/codegen/func_registry.h"
#include "../../src/operator/filter/filter_and_project.h"

using omniruntime::op::RowFilter;
using omniruntime::op::RowFilterFunc;
using omniruntime::op::RowProjection;
using omniruntime::op::RowProjFunc;
using namespace std;
using namespace omniruntime::expressions;

// Filter is basically just a projection that must return a boolean.
// The logic for filtering out rows from final output is handled in C++ when
// processing each row individually, so to LLVM it is exactly the same as a projection.
// Check CodeGenTest.RowFilter for a dedicated test using the RowFilter class instead.
TEST(CodeGenTest, SimpleFilter)
{
    string unparsed = "$operator$LESS_THAN:boolean(#0, 50)";

    const int32_t numCols = 1;
    DataType types[numCols] = {DataType::INT32D};

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

    std::vector<DataType> vecTypes = std::vector<DataType>(types, types + numCols);

    RowProjection lc(unparsed, vecTypes);
    RowProjFunc func = lc.Create(vecTypes);
    EXPECT_EQ(lc.GetReturnType(), BOOLD);

    bool *nullResult = new bool(false);

    for (int32_t i = 0; i < 50; i++) {
        bool res = *((bool *)func(table, (int64_t*) bitmap, (int64_t*) offsets, i, (int64_t*) nullResult));
        EXPECT_TRUE(res);
    }
    for (int32_t i = 50; i < 100; i++) {
        bool res = *((bool *)func(table, (int64_t*) bitmap, (int64_t*) offsets, i, (int64_t*) nullResult));
        EXPECT_FALSE(res);
    }

    delete[] col1;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
}
// Simple project example using individual row processing.
TEST(CodeGenTest, SimpleProject)
{
    string unparsed = "ADD:int(#0, 50)";

    const int32_t numCols = 1;
    DataType types[numCols] = {DataType::INT32D};

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

    std::vector<DataType> vecTypes = std::vector<DataType>(types, types + numCols);

    RowProjection lc(unparsed, vecTypes);
    RowProjFunc func = lc.Create(vecTypes);
    EXPECT_EQ(lc.GetReturnType(), INT32D);

    bool *nullResult = new bool(false);

    for (int32_t i = 0; i < 100; i++) {
        int32_t res = *((int32_t *)func(table, (int64_t*) bitmap, (int64_t*) offsets, i, (int64_t*) nullResult));
        EXPECT_EQ(res, i + 50);
    }

    delete[] col1;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
}
// A more complicated test for individual row projection
TEST(CodeGenTest, SingleProject)
{
    string unparsed = "IF:int($operator$GREATER_THAN:boolean(#1, 3000000000), ADD:int(#0, 10), MULTIPLY:int(#0, -1))";

    const int32_t numCols = 2;
    DataType types[numCols] = {DataType::INT32D, DataType::INT64D};

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

    std::vector<DataType> vecTypes = std::vector<DataType>(types, types + numCols);

    RowProjection lc(unparsed, vecTypes);
    RowProjFunc func = lc.Create(vecTypes);
    EXPECT_EQ(lc.GetReturnType(), INT32D);
    
    bool *nullResult = new bool(false);

    for (int32_t i = 0; i < numRows; i++) {
        int32_t res = *((int32_t *)func(table, (int64_t*) bitmap, (int64_t*) offsets, i, (int64_t*) nullResult));
        EXPECT_EQ(res, i % 2 ? i + 10 : -i);
    }

    delete[] col1;
    delete[] col2;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
}

// Test the short circuit functionality in the case that the projection is a column index.
TEST(CodeGenTest, ShortCircuitProject)
{
    string unparsed = "#1";

    const int32_t numCols = 2;
    DataType types[numCols] = {DataType::INT32D, DataType::INT64D};

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
    std::vector<DataType> vecTypes = std::vector<DataType>(types, types + numCols);

    RowProjection lc(unparsed, vecTypes);
    RowProjFunc func = lc.Create(vecTypes);

    EXPECT_TRUE(lc.IsColumnProjection());
    EXPECT_EQ(lc.GetIndexIfColumnProjection(), 1);

    bool *nullResult = new bool[1];
    nullResult[0] = false;

    for (int32_t i = 0; i < numRows; i++) {
        int32_t res = *((int32_t *)func(table, (int64_t*) bitmap, (int64_t*) offsets, i, (int64_t*) nullResult));
        EXPECT_EQ(res, i % 10);
    }

    delete[] col1;
    delete[] col2;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
}

// Test the row filter
TEST(CodeGenTest, RowFilter)
{
    string unparsed = "$operator$EQUAL:boolean(#0, 0)";

    const int32_t numCols = 1;
    DataType types[numCols] = {DataType::INT32D};

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
    std::vector<DataType> vecTypes = std::vector<DataType>(types, types + numCols);

    auto filter = new RowFilter(unparsed, vecTypes);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create(vecTypes);
    EXPECT_FALSE(filter == nullptr);

    for (int32_t i = 0; i < numRows; i++) {
        bool res = filterFunc(table, (int64_t*) bitmap, (int64_t*) offsets, i);
        EXPECT_EQ(res, i % 2 == 0);
    }

    delete[] col1;
    delete[] table;
    delete[] bitmap;
    delete[] offsets;
}

TEST (CodeGenTest, RowFilterString) {
    DataType types[2] = {DataType::STRINGD, DataType::STRINGD};
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

    auto **offsets = new int32_t *[numCols];
    for (int col = 0; col < numCols; col++) {
        offsets[col] = new int32_t[numRows + 1];
        offsets[col][0] = 0;
        offsets[col][1] = 11;
    }

    string testname = "stringTest1";
    vector<DataType> typeVec = vector<DataType>(types, types + 2);

    std::string expr = "$operator$EQUAL:boolean(substr:varchar(#0, 1, 5), 'hello')";
    auto filter = new RowFilter(expr, typeVec);
    EXPECT_FALSE(filter == nullptr);
    auto filterFunc = filter->Create(typeVec);
    EXPECT_FALSE(filter == nullptr);

    bool res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0);
    EXPECT_EQ(res, true);
    delete filter;

    expr = "$operator$EQUAL:boolean(substr:varchar(#1, 1, 5), 'hello')";
    filter = new RowFilter(expr, typeVec);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create(typeVec);
    EXPECT_FALSE(filter == nullptr);

    res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0);
    EXPECT_EQ(res, false);

    delete filter;
    expr = "$operator$EQUAL:boolean(substr:varchar(#0, 1, 5), substr:varchar(#1, 7, 11))";
    filter = new RowFilter(expr, typeVec);
    EXPECT_FALSE(filter == nullptr);
    filterFunc = filter->Create(typeVec);
    EXPECT_FALSE(filter == nullptr);

    res = filterFunc(vals, (int64_t*) bitmap, (int64_t*) offsets, 0);;
    EXPECT_EQ(res, true);

    delete[] vals;
    delete[] bitmap;
    delete[] offsets;
}

TEST(CodeGenTest, Operators1)
{
    string unparsed = "AND:boolean($operator$GREATER_THAN_OR_EQUAL:boolean(ADD:int(#0, 2), 4), "
        "AND:boolean($operator$LESS_THAN:boolean(#1, 4), $operator$EQUAL:boolean(#2, 2)))";

    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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


    string testname = "simpleTest1";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *))(intptr_t)lc->GetFunction();

    // number of rows that passed filter
    int32_t result = func(vals, 1, selected, (int64_t *)((int64_t *)(bitmap)));
    EXPECT_EQ(result, 1);

    v1[0] = 2;
    v2[0] = 4;
    v3[0] = 2;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, ((int64_t *)(bitmap)));
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}

TEST(CodeGenTest, MathFunctions1)
{
    string unparsed = "AND:boolean($operator$EQUAL:boolean(abs:int(#0), abs:int(#2)), "
        "$operator$EQUAL:boolean(abs:int(#0), abs:int(#1)))";

    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));

    EXPECT_EQ(result, 1);
    std::cout << "result: " << result << std::endl;

    v1[0] = 10000;
    v2[0] = 10000;
    v3[0] = -10001;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}


TEST(CodeGenTest, MathFunctions2)
{
    string unparsed = "BETWEEN:boolean(#1, #0, #2)";

    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));

    EXPECT_EQ(result, 1);

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = 12356;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}


TEST(CodeGenTest, MathFunctions3)
{
    string unparsed = "IF:boolean($operator$GREATER_THAN:boolean(#0, 100), $operator$GREATER_THAN:boolean(#0, 200), "
        "$operator$LESS_THAN:boolean(#0, 0))";

    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    v1[0] = -12;
    v2[0] = 1245;
    v3[0] = 123;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    v1[0] = -12222;
    v2[0] = -12312;
    v3[0] = 42;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}


TEST(CodeGenTest, MathFunctions4)
{
    string unparsed = "IN:boolean(#0, 1, 2, 3, 4, 5)";

    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    v1[0] = 3;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    v1[0] = 5;
    v2[0] = 1245;
    v3[0] = 123;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    v1[0] = 0;
    v2[0] = -12312;
    v3[0] = 42;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    v1[0] = 123;
    v2[0] = -43;
    v3[0] = 542;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}

// For testing different types
TEST(CodeGenTest, CastNumbers1)
{
    string unparsed = "$operator$EQUAL:boolean(abs:double(CAST:double(#0)), abs:double(CAST:double(#1)))";

    DataType types[3] = {DataType::INT32D, DataType::INT64D, DataType::DOUBLED};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    v1[0] = 2000000000;
    v2[0] = 3000000000;
    v3[0] = -234;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    v1[0] = -1000000;
    v2[0] = -1000000;
    v3[0] = 133.324234;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}

TEST(CodeGenTest, CastNumbers2)
{
    string unparsed = "$operator$GREATER_THAN:boolean(CAST:double(#1), #2)";

    DataType types[3] = {DataType::INT32D, DataType::INT64D, DataType::DOUBLED};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    auto *lc = new FilterCodeGen(testname, *expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    v1[0] = 2000000000;
    v2[0] = -233;
    v3[0] = -234.2142;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    v1[0] = -1000000;
    v2[0] = 12;
    v3[0] = 12;
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)v2;
    vals[2] = (int64_t)v3;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}


TEST(CodeGenTest, Like)
{
    string unparsed = "LIKE:boolean(#2, '%hello%world%')";

    DataType types[3] = {DataType::INT32D, DataType::STRINGD, DataType::STRINGD};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);


    v1[0] = {8766};
    s1[0] = "asdf";
    s2[0] = "asjd fehell ojdsl kfjwo rld dslk  jf ";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[2][1] = s2->length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] offsets;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}


TEST(CodeGenTest, DateCast)
{
    string unparsed = "$operator$GREATER_THAN:boolean(CAST:int(#2), #0)";


    DataType types[3] = {DataType::INT32D, DataType::STRINGD, DataType::STRINGD};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);


    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "1996-01-02";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "1993-11-12";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, SubstrIn)
{
    string unparsed = "IN:boolean(substr:varchar(#2, 1, 2), '12', '21', '13', '31', '34', '43')";

    DataType types[3] = {DataType::INT32D, DataType::STRINGD, DataType::STRINGD};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));

    EXPECT_EQ(result, 1);
    FreeStrings();


    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "233425";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);
    FreeStrings();

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "424321";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);
    FreeStrings();

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, ConcatStr)
{
    string unparsed = "$operator$EQUAL:boolean(concat:varchar(#1, #2), 'helloworld')";

    DataType types[3] = {DataType::INT32D, DataType::STRINGD, DataType::STRINGD};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));

    EXPECT_EQ(result, 1);
    FreeStrings();


    v1[0] = {8766};
    s1[0] = "hello";
    s2[0] = "world ";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);
    FreeStrings();


    v1[0] = {8766};
    s1[0] = "hello ";
    s2[0] = "world";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);
    FreeStrings();


    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, StringWithOps)
{
    string unparsed = "OR:boolean($operator$EQUAL:boolean(#2, 'Sunday'), $operator$EQUAL:boolean(#2, 'Saturday'))";


    DataType types[3] = {DataType::INT32D, DataType::STRINGD, DataType::STRINGD};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "Sunday";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();

    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();
    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    v1[0] = {8766};
    s1[0] = "j";
    s2[0] = "Monday";
    vals[0] = (int64_t)v1;
    vals[1] = (int64_t)s1->c_str();
    vals[2] = (int64_t)s2->c_str();
    offsets[1][1] = s1[0].length();
    offsets[2][1] = s2[0].length();
    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, Coalesce)
{
    string unparsed = "$operator$EQUAL:boolean(COALESCE:long(#0, 0), 123)";

    DataType types[3] = {DataType::INT64D, DataType::INT64D, DataType::INT64D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);


    bitmap[0][0] = true;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    for (int i = 0; i < 3; i++) {
        delete[] bitmap[i];
    }
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, IsNull)
{
    string unparsed = "IS_NULL:boolean(#0)";

    DataType types[1] = {DataType::INT64D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 1);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 1);
    auto *lc = new FilterCodeGen(testName, *expr, typeVec);

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, false);

    bitmap[0][0] = true;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, true);

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, IsNotNull)
{
    string unparsed = "IS_NOT_NULL:boolean(#0)";

    DataType types[1] = {DataType::INT64D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 1);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 1);
    auto *lc = new FilterCodeGen(testName, *expr, typeVec);

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, true);

    bitmap[0][0] = true;

    result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, false);

    delete[] bitmap[0];
    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, DecimalOperators1)
{
    string unparsed = "$operator$EQUAL:boolean(ADD:decimal(#0, 5), 15)";

    DataType types[1] = {DataType::DECIMAL128D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 1);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 1);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 2, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, DecimalOperators2)
{
    string unparsed = "BETWEEN:boolean(#0, #1, #2)";

    DataType types[3] = {DataType::DECIMAL128D, DataType::DECIMAL128D, DataType::DECIMAL128D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 0);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, DecimalOperators3)
{
    string unparsed = "IF:boolean($operator$GREATER_THAN:boolean(#0, 100), $operator$GREATER_THAN:boolean(#0, 200), "
        "$operator$LESS_THAN:boolean(#0, 0))";

    DataType types[3] = {DataType::DECIMAL128D, DataType::DECIMAL128D, DataType::DECIMAL128D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, *expr, typeVec);

    int32_t (*func)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *);
    func = (int32_t(*)(int64_t *, int32_t, int32_t *, int64_t *, int64_t *))(intptr_t)lc->GetFunction();

    int32_t result = func(vals, 1, selected, (int64_t *)(bitmap), (int64_t *)(offsets));
    EXPECT_EQ(result, 1);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, ProjectionCodeGen)
{
    string unparsed = "$operator$ADD:decimal(#0, 100)";

    DataType types[1] = {DataType::DECIMAL128D};
    Parser parser {};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
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
    vector<DataType> typeVec = vector<DataType>(types, types + 1);
    ProjectionCodeGen *lc = new ProjectionCodeGen(testname, *expr, typeVec, false);

    vector<int64_t> oVec(3);
    auto ov = oVec.data();
    void *vecVals = &ov;
    auto cvecVals = static_cast<int64_t *>(vecVals);
    int32_t (*func)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *, int32_t *);
    func = (int32_t(*)(int64_t *, int32_t, int64_t, int32_t *, int32_t, int64_t *, int64_t *, bool *, int32_t *))(intptr_t)lc->GetFunction();

    int32_t r = func(vals, 3, *cvecVals, nullptr, 3, (int64_t *)(bitmap), (int64_t *)(offsets), newNullValues, newLengths);
    int64_t *result = reinterpret_cast<int64_t *>(oVec[0]);
    EXPECT_EQ(*result, 110);
    EXPECT_EQ(*(result + 1), 0);

    result = reinterpret_cast<int64_t *>(oVec[1]);
    EXPECT_EQ(*(result), 120);
    EXPECT_EQ(*(result + 1), 0);

    result = reinterpret_cast<int64_t *>(oVec[2]);
    EXPECT_EQ(*(result), 130);
    EXPECT_EQ(*(result + 1), 0);

    delete[] bitmap;
    delete[] vals;
    delete expr;
    delete lc;
}
