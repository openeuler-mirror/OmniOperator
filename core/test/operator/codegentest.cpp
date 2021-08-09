/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "gtest/gtest.h"

#include <iostream>
#include <string>
#include <cstring>
#include <memory>
#include <vector>
#include <cassert>
#include <ctime>
#include <regex>

#include "../../src/codegen/llvm_codegen.h"
#include "../../src/codegen/filter_codegen.h"
#include "../../src/codegen/projection_codegen.h"
#include "../../src/codegen/func_registry.h"


#include "llvm/ExecutionEngine/Orc/LLJIT.h"

#include "llvm/ADT/APInt.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Target/TargetMachine.h"


using namespace std;
TEST(CodeGenTest, Operators1) {
    string unparsed = "AND($operator$GREATER_THAN_OR_EQUAL(ADD(#0, 2), 4), AND($operator$LESS_THAN(#1, 4), $operator$EQUAL(#2, 2)))";

    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;

    int32_t v1[1] = {2};
    int32_t v2[1] = {3};
    int32_t v3[1] = {2};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "simpleTest1";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    // number of rows that passed Filter
    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = 2;
    v2[0] = 4;
    v3[0] = 2;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}

TEST(CodeGenTest, MathFunctions1) {
    string unparsed = "AND($operator$EQUAL(abs(#0), abs(#2)), $operator$EQUAL(abs(#0), abs(#1)))";

    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;

    int32_t v1[1] = {-123};
    int32_t v2[1] = {123};
    int32_t v3[1] = {123};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "simpleTest2";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = 10000;
    v2[0] = 10000;
    v3[0] = -10001;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}


TEST(CodeGenTest, MathFunctions2) {
    string unparsed = "BETWEEN(#1, #0, #2)";

    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;

    int32_t v1[1] = {1001};
    int32_t v2[1] = {1001};
    int32_t v3[1] = {1001};
    int64_t *vals = new int64_t[3];
    //    vals[0] = reinterpret_cast<int64_t>(v1);
    //    vals[1] = reinterpret_cast<int64_t>(v2);
    //    vals[2] = reinterpret_cast<int64_t>(v3);
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "simpleTest2";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = 12356;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}


TEST(CodeGenTest, MathFunctions3) {
    string unparsed = "IF($operator$GREATER_THAN(#0, 100), $operator$GREATER_THAN(#0, 200), $operator$LESS_THAN(#0, 0))";

    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;

    int32_t v1[1] = {1001};
    int32_t v2[1] = {0};
    int32_t v3[1] = {21};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "simpleTest2";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = 100;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    v1[0] = -12;
    v2[0] = 1245;
    v3[0] = 123;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = -12222;
    v2[0] = -12312;
    v3[0] = 42;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}


TEST(CodeGenTest, MathFunctions4) {
    string unparsed = "IN(#0, 1, 2, 3, 4, 5)";

    DataType types[3] = {DataType::INT32D, DataType::INT32D, DataType::INT32D};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;

    int32_t v1[1] = {1};
    int32_t v2[1] = {0};
    int32_t v3[1] = {21};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "simpleTest2";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = 3;
    v2[0] = 1245;
    v3[0] = -12356;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = 5;
    v2[0] = 1245;
    v3[0] = 123;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = 0;
    v2[0] = -12312;
    v3[0] = 42;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    v1[0] = 123;
    v2[0] = -43;
    v3[0] = 542;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}

// For testing different types
TEST(CodeGenTest, CastNumbers1) {
    string unparsed = "$operator$EQUAL(abs(CAST(#0)), abs(CAST(#1)))";


    DataType types[3] = {DataType::INT32D, DataType::INT64D, DataType::DOUBLED};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;


    int32_t v1[1] = {10000};
    int64_t v2[1] = {10000};
    double v3[1] = {12.34};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "simpleTest3";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = 2000000000;
    v2[0] = 3000000000;
    v3[0] = -234;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    v1[0] = -1000000;
    v2[0] = -1000000;
    v3[0] = 133.324234;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}

TEST(CodeGenTest, CastNumbers2) {
    string unparsed = "$operator$GREATER_THAN(CAST(#1), #2)";

    DataType types[3] = {DataType::INT32D, DataType::INT64D, DataType::DOUBLED};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;


    int32_t v1[1] = {324233};
    int64_t v2[1] = {12};
    double v3[1] = {12.34};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "simpleTest3";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    v1[0] = 2000000000;
    v2[0] = -233;
    v3[0] = -234.2142;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = -1000000;
    v2[0] = 12;
    v3[0] = 12;
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete lc;
    delete expr;
}


TEST(CodeGenTest, Like) {
    string unparsed = "LIKE(#2, '%hello%world%')";


    DataType types[3] = {DataType::INT32D, DataType::STRINGD, DataType::STRINGD};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;

    string s1;
    string s2;

    int32_t v1[1] = {8766};
    s1 = "asdf";
    int64_t v2[1] = {(int64_t) (s1.c_str())};
    s2 = "asjd fehellojdsl kfjworlddslk  jf ";
    int64_t v3[1] = {(int64_t) (s2.c_str())};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "stringTest1";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);


    v1[0] = {8766};
    s1 = "asdf";
    v2[0] = {(int64_t) (s1.c_str())};
    s2 = "asjd fehell ojdsl kfjwo rld dslk  jf ";
    v3[0] = {(int64_t) (s2.c_str())};
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}


TEST(CodeGenTest, DateCast) {
    string unparsed = "$operator$GREATER_THAN(CAST(#2), #0)";


    DataType types[3] = {DataType::INT32D, DataType::STRINGD, DataType::STRINGD};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;


    string s1;
    string s2;

    int32_t v1[1] = {8766};
    s1 = "asdf";
    int64_t v2[1] = {(int64_t) (s1.c_str())};
    s2 = "1994-01-01";
    int64_t v3[1] = {(int64_t) (s2.c_str())};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "stringTest1";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);


    v1[0] = {8766};
    s1 = "j";
    v2[0] = {(int64_t) (s1.c_str())};
    s2 = "1996-01-02";
    v3[0] = {(int64_t) (s2.c_str())};
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = {8766};
    s1 = "j";
    v2[0] = {(int64_t) (s1.c_str())};
    s2 = "1993-11-12";
    v3[0] = {(int64_t) (s2.c_str())};
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, SubstrIn) {
    string unparsed = "IN(substr(#2, 0, 2), '12', '21', '13', '31', '34', '43')";


    DataType types[3] = {DataType::INT32D, DataType::STRINGD, DataType::STRINGD};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;


    string s1;
    string s2;

    int32_t v1[1] = {8766};
    s1 = "asdf";
    int64_t v2[1] = {(int64_t) (s1.c_str())};
    s2 = "2134124";
    int64_t v3[1] = {(int64_t) (s2.c_str())};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "stringTest1";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);
    FreeStrings();


    v1[0] = {8766};
    s1 = "j";
    v2[0] = {(int64_t) (s1.c_str())};
    s2 = "233425";
    v3[0] = {(int64_t) (s2.c_str())};
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);
    FreeStrings();

    v1[0] = {8766};
    s1 = "j";
    v2[0] = {(int64_t) (s1.c_str())};
    s2 = "424321";
    v3[0] = {(int64_t) (s2.c_str())};
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);
    FreeStrings();

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, ConcatStr) {
    string unparsed = "$operator$EQUAL(concat(#1, #2), 'helloworld')";


    DataType types[3] = {DataType::INT32D, DataType::STRINGD, DataType::STRINGD};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;


    string s1;
    string s2;

    int32_t v1[1] = {8766};
    s1 = "hello";
    int64_t v2[1] = {(int64_t) (s1.c_str())};
    s2 = "world";
    int64_t v3[1] = {(int64_t) (s2.c_str())};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }

    string testname = "stringTest1";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);

    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);
    FreeStrings();


    v1[0] = {8766};
    s1 = "hello";
    v2[0] = {(int64_t) (s1.c_str())};
    s2 = "world ";
    v3[0] = {(int64_t) (s2.c_str())};
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);
    FreeStrings();


    v1[0] = {8766};
    s1 = "hello ";
    v2[0] = {(int64_t) (s1.c_str())};
    s2 = "world";
    v3[0] = {(int64_t) (s2.c_str())};
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);
    FreeStrings();


    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, StringWithOps) {
    string unparsed = "OR($operator$EQUAL(#2, 'Sunday'), $operator$EQUAL(#2, 'Saturday'))";


    DataType types[3] = {DataType::INT32D, DataType::STRINGD, DataType::STRINGD};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;

    string s1;
    string s2;

    int32_t v1[1] = {8766};
    s1 = "asdf";
    int64_t v2[1] = {(int64_t) (s1.c_str())};
    s2 = "Saturday";
    int64_t v3[1] = {(int64_t) (s2.c_str())};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "stringTest1";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);


    v1[0] = {8766};
    s1 = "j";
    v2[0] = {(int64_t) (s1.c_str())};
    s2 = "Sunday";
    v3[0] = {(int64_t) (s2.c_str())};
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);

    v1[0] = {8766};
    s1 = "j";
    v2[0] = {(int64_t) (s1.c_str())};
    s2 = "Monday";
    v3[0] = {(int64_t) (s2.c_str())};
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}

TEST(CodeGenTest, Coalesce) {
    string unparsed = "$operator$EQUAL(COALESCE(#0, 0), 123)";


    DataType types[3] = {DataType::INT64D, DataType::INT64D, DataType::INT64D};
    Parser parser{};
    Expr *expr = parser.ParseRowExpression(unparsed, reinterpret_cast<int *>(types), 3);
    expr->PrintExprTree();
    cout << endl;

    int64_t v1[1] = {123};
    int64_t v2[1] = {234};
    int64_t v3[1] = {345};
    int64_t *vals = new int64_t[3];
    vals[0] = (int64_t) v1;
    vals[1] = (int64_t) v2;
    vals[2] = (int64_t) v3;
    int32_t *selected = new int32_t[1];

    bool *bitmap = new bool[3];
    for (int i = 0; i < 3; i++) {
        bitmap[i] = false;
    }


    string testname = "coalesceTest1";
    vector<DataType> typeVec = vector<DataType>(types, types + 3);
    FilterCodeGen *lc = new FilterCodeGen(testname, expr, typeVec);


    int32_t (*func)(int64_t *, int32_t, int32_t *, bool *);
    func = (int32_t (*)(int64_t *, int32_t, int32_t *, bool *)) (intptr_t) lc->GetFunction();

    int32_t result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 1);


    bitmap[0] = true;

    result = func(vals, 1, selected, bitmap);
    EXPECT_EQ(result, 0);

    delete[] bitmap;
    delete[] vals;
    delete[] selected;
    delete expr;
    delete lc;
}
