/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include "gtest/gtest.h"
#include "../util/test_util.h"
#include "../../src/operator/projection/projection.h"
#include <string>
#include <vector>

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace std;
namespace project_test {
void testCmpBinaryExpressions(std::vector<Expr *> result, omniruntime::expressions::Operator op,
    const int PROJECT_COUNT, bool isBoolResult = false)
{
    std::vector<VecTypeId> dataTypes = { OMNI_VEC_TYPE_INT,        OMNI_VEC_TYPE_LONG,    OMNI_VEC_TYPE_DOUBLE,
        OMNI_VEC_TYPE_DECIMAL128, OMNI_VEC_TYPE_VARCHAR, OMNI_VEC_TYPE_CHAR };
    for (int i = 0; i < PROJECT_COUNT; i++) {
        BinaryExpr *binaryExpr = static_cast<BinaryExpr *>(result.at(i));
        if (isBoolResult) {
            EXPECT_EQ(binaryExpr->GetReturnTypeId(), OMNI_VEC_TYPE_BOOLEAN);
        } else {
            EXPECT_EQ(binaryExpr->GetReturnTypeId(), dataTypes[i]);
        }
        EXPECT_EQ(binaryExpr->op, op);
        FieldExpr *left = static_cast<FieldExpr *>(binaryExpr->left);
        EXPECT_EQ(left->GetType(), FIELD_E);
        EXPECT_EQ(left->colVal, i);

        LiteralExpr *right = static_cast<LiteralExpr *>(binaryExpr->right);
        EXPECT_EQ(right->GetType(), LITERAL_E);
        if (i == 3) {
            EXPECT_EQ(right->GetReturnTypeId(), OMNI_VEC_TYPE_DECIMAL128);
        } else if (i == 5) {
            EXPECT_EQ(right->GetReturnTypeId(), OMNI_VEC_TYPE_CHAR);
        } else {
            EXPECT_EQ(right->GetReturnTypeId(), dataTypes[i]);
        }
        if (i == 0)
            EXPECT_EQ(right->intVal, i);
        else if (i == 1 || i == 3)
            EXPECT_EQ(right->longVal, i); // parser sets longVal for OMNI_VEC_TYPE_LONG and OMNI_VEC_TYPE_DECIMAL128
        else if (i == 2)
            EXPECT_EQ(right->doubleVal, i);
        else if (i == 4 || i == 5)
            ASSERT_STREQ(right->stringVal->c_str(), "hello");

        if (i == 5) {
            EXPECT_EQ(right->dataType->GetWidth(), 6);
        }
    }
}

void testArithmeticBinaryExpressions(std::vector<Expr *> result, omniruntime::expressions::Operator op,
    const int PROJECT_COUNT)
{
    std::vector<VecTypeId> dataTypes = { OMNI_VEC_TYPE_INT, OMNI_VEC_TYPE_LONG, OMNI_VEC_TYPE_DOUBLE,
        OMNI_VEC_TYPE_DECIMAL128 };
    for (int i = 0; i < PROJECT_COUNT; i++) {
        BinaryExpr *binaryExpr = static_cast<BinaryExpr *>(result.at(i));
        EXPECT_EQ(binaryExpr->GetReturnTypeId(), dataTypes[i]);
        EXPECT_EQ(binaryExpr->op, op);
        FieldExpr *left = static_cast<FieldExpr *>(binaryExpr->left);
        EXPECT_EQ(left->GetType(), FIELD_E);
        EXPECT_EQ(left->colVal, i);

        LiteralExpr *right = static_cast<LiteralExpr *>(binaryExpr->right);
        EXPECT_EQ(right->GetType(), LITERAL_E);
        if (i == 3) {
            EXPECT_EQ(right->GetReturnTypeId(), OMNI_VEC_TYPE_DECIMAL128);
        } else {
            EXPECT_EQ(right->GetReturnTypeId(), dataTypes[i]);
        }
        if (i == 0)
            EXPECT_EQ(right->intVal, i);
        else if (i == 1 || i == 3)
            EXPECT_EQ(right->longVal, i); // parser sets longVal for OMNI_VEC_TYPE_LONG and OMNI_VEC_TYPE_DECIMAL128
        else if (i == 2)
            EXPECT_EQ(right->doubleVal, i);
    }
}

// Test Not expression
TEST(ParseTest, parseNotOperation)
{
    const int PROJECT_COUNT = 1;
    int vecTypeCount = 1;
    string expr = "$operator$NOT:4(#0)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_BOOLEAN) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_BOOLEAN);
    UnaryExpr *notExpr = static_cast<UnaryExpr *>(result);
    EXPECT_EQ(notExpr->op, NOT);
    FieldExpr *colExpr = static_cast<FieldExpr *>(notExpr->exp);
    EXPECT_EQ(colExpr->GetType(), FIELD_E);
    EXPECT_EQ(colExpr->colVal, 0);
}

// Test Arithmetic Binary Operations
TEST(ParseTest, parseAddOperation)
{
    const int PROJECT_COUNT = 4;
    int vecTypeCount = 4;
    string exprs[PROJECT_COUNT] = {"$operator$ADD:1(#0, 0:1)", "$operator$ADD:2(#1, 1:2)",
                                       "$operator$ADD:3(#2, 2:3)", "$operator$ADD:7(#3, 3:7)"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
        VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    testArithmeticBinaryExpressions(result, ADD, PROJECT_COUNT);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

TEST(ParseTest, parseSubtractOperation)
{
    const int PROJECT_COUNT = 4;
    int vecTypeCount = 4;
    string exprs[PROJECT_COUNT] = {"$operator$SUBTRACT:1(#0, 0:1)", "$operator$SUBTRACT:2(#1, 1:2)",
                                       "$operator$SUBTRACT:3(#2, 2:3)", "$operator$SUBTRACT:7(#3, 3:7)"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
        VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    testArithmeticBinaryExpressions(result, SUB, PROJECT_COUNT);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

TEST(ParseTest, parseMultiplyOperation)
{
    const int PROJECT_COUNT = 4;
    int vecTypeCount = 4;
    string exprs[PROJECT_COUNT] = {"$operator$MULTIPLY:1(#0, 0:1)", "$operator$MULTIPLY:2(#1, 1:2)",
                                       "$operator$MULTIPLY:3(#2, 2:3)", "$operator$MULTIPLY:7(#3, 3:7)"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
        VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    testArithmeticBinaryExpressions(result, MUL, PROJECT_COUNT);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

TEST(ParseTest, parseDivideOperation)
{
    const int PROJECT_COUNT = 4;
    int vecTypeCount = 4;
    string exprs[PROJECT_COUNT] = {"$operator$DIVIDE:1(#0, 0:1)", "$operator$DIVIDE:2(#1, 1:2)",
                                       "$operator$DIVIDE:3(#2, 2:3)", "$operator$DIVIDE:7(#3, 3:7)"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
        VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    testArithmeticBinaryExpressions(result, DIV, PROJECT_COUNT);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

TEST(ParseTest, parseModulusOperation)
{
    const int PROJECT_COUNT = 4;
    int vecTypeCount = 4;
    string exprs[PROJECT_COUNT] = {"$operator$MODULUS:1(#0, 0:1)", "$operator$MODULUS:2(#1, 1:2)",
                                       "$operator$MODULUS:3(#2, 2:3)", "$operator$MODULUS:7(#3, 3:7)"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
        VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    testArithmeticBinaryExpressions(result, MOD, PROJECT_COUNT);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

// Test Compare Binary Operations
TEST(ParseTest, parseLTOperation)
{
    const int PROJECT_COUNT = 4;
    int vecTypeCount = 4;
    string exprs[PROJECT_COUNT] = {"$operator$LESS_THAN:4(#0, 0:1)", "$operator$LESS_THAN:4(#1, 1:2)",
                                       "$operator$LESS_THAN:4(#2, 2:3)", "$operator$LESS_THAN:4(#3, 3:7)"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
        VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    testCmpBinaryExpressions(result, LT, PROJECT_COUNT, true);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

TEST(ParseTest, parseLTEOperation)
{
    const int PROJECT_COUNT = 4;
    int vecTypeCount = 4;
    string exprs[PROJECT_COUNT] = {"$operator$LESS_THAN_OR_EQUAL:4(#0, 0:1)",
                                       "$operator$LESS_THAN_OR_EQUAL:4(#1, 1:2)",
                                       "$operator$LESS_THAN_OR_EQUAL:4(#2, 2:3)",
                                       "$operator$LESS_THAN_OR_EQUAL:4(#3, 3:7)"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
        VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    testCmpBinaryExpressions(result, LTE, PROJECT_COUNT, true);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

TEST(ParseTest, parseGTOperation)
{
    const int PROJECT_COUNT = 4;
    int vecTypeCount = 4;
    string exprs[PROJECT_COUNT] = {"$operator$GREATER_THAN:4(#0, 0:1)", "$operator$GREATER_THAN:4(#1, 1:2)",
                                       "$operator$GREATER_THAN:4(#2, 2:3)", "$operator$GREATER_THAN:4(#3, 3:7)"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
        VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    testCmpBinaryExpressions(result, GT, PROJECT_COUNT, true);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

TEST(ParseTest, parseGTEOperation)
{
    const int PROJECT_COUNT = 4;
    int vecTypeCount = 4;
    string exprs[PROJECT_COUNT] = {"$operator$GREATER_THAN_OR_EQUAL:4(#0, 0:1)",
                                       "$operator$GREATER_THAN_OR_EQUAL:4(#1, 1:2)",
                                       "$operator$GREATER_THAN_OR_EQUAL:4(#2, 2:3)",
                                       "$operator$GREATER_THAN_OR_EQUAL:4(#3, 3:7)"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
        VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    testCmpBinaryExpressions(result, GTE, PROJECT_COUNT, true);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

TEST(ParseTest, parseEQOperation)
{
    const int PROJECT_COUNT = 6;
    int vecTypeCount = 6;
    string exprs[PROJECT_COUNT] = {"$operator$EQUAL:4(#0, 0:1)", "$operator$EQUAL:4(#1, 1:2)",
                                       "$operator$EQUAL:4(#2, 2:3)", "$operator$EQUAL:4(#3, 3:7)",
                                       "$operator$EQUAL:4(#4, 'hello':15)", "$operator$EQUAL:4(#5, 'hello':16[6])"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT),     VecType(OMNI_VEC_TYPE_LONG),
        VecType(OMNI_VEC_TYPE_DOUBLE),  VecType(OMNI_VEC_TYPE_DECIMAL128),
        VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_CHAR) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    testCmpBinaryExpressions(result, EQ, PROJECT_COUNT, true);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

TEST(ParseTest, parseNEQOperation)
{
    const int PROJECT_COUNT = 6;
    int vecTypeCount = 6;
    string exprs[PROJECT_COUNT] = {"$operator$NOT_EQUAL:4(#0, 0:1)", "$operator$NOT_EQUAL:4(#1, 1:2)",
                                       "$operator$NOT_EQUAL:4(#2, 2:3)", "$operator$NOT_EQUAL:4(#3, 3:7)",
                                       "$operator$NOT_EQUAL:4(#4, 'hello':15)",
                                       "$operator$NOT_EQUAL:4(#5, 'hello':16[6])"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT),     VecType(OMNI_VEC_TYPE_LONG),
        VecType(OMNI_VEC_TYPE_DOUBLE),  VecType(OMNI_VEC_TYPE_DECIMAL128),
        VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_CHAR) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    testCmpBinaryExpressions(result, NEQ, PROJECT_COUNT, true);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

// Test Logic Operations
TEST(ParseTest, parseLogicOperations)
{
    const int PROJECT_COUNT = 2;
    int vecTypeCount = 2;
    string exprs[PROJECT_COUNT] = {"$operator$AND:4(#0, true:4)", "$operator$OR:4(#1, false:4)"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_BOOLEAN), VecType(OMNI_VEC_TYPE_BOOLEAN) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes);
    EXPECT_EQ(result.size(), PROJECT_COUNT);
    for (int i = 0; i < PROJECT_COUNT; i++) {
        BinaryExpr *logExpr = static_cast<BinaryExpr *>(result.at(i));
        EXPECT_EQ(logExpr->GetReturnTypeId(), OMNI_VEC_TYPE_BOOLEAN);
        FieldExpr *logLeft = static_cast<FieldExpr *>(logExpr->left);
        LiteralExpr *logRight = static_cast<LiteralExpr *>(logExpr->right);
        EXPECT_EQ(logLeft->GetType(), FIELD_E);
        EXPECT_EQ(logLeft->colVal, i);
        EXPECT_EQ(logRight->GetType(), LITERAL_E);
        if (i == 0) {
            EXPECT_TRUE(logRight->boolVal);
            EXPECT_EQ(logExpr->op, AND);
        }
        if (i == 1) {
            EXPECT_FALSE(logRight->boolVal);
            EXPECT_EQ(logExpr->op, OR);
        }
    }

    for (int i = 0; i < PROJECT_COUNT; i++) {
        delete result.at(i);
    }
}

// Test Between expression
TEST(ParseTest, parseBetweenOperation)
{
    int vecTypeCount = 3;
    string expr = "$operator$BETWEEN:4(#1, #0, #2)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_DOUBLE),
        VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_BOOLEAN);
    BetweenExpr *betweenExpr = static_cast<BetweenExpr *>(result);
    FieldExpr *valueExpr = static_cast<FieldExpr *>(betweenExpr->value);
    FieldExpr *lowerExpr = static_cast<FieldExpr *>(betweenExpr->lowerBound);
    FieldExpr *upperExpr = static_cast<FieldExpr *>(betweenExpr->upperBound);

    EXPECT_EQ(valueExpr->GetType(), FIELD_E);
    EXPECT_EQ(valueExpr->colVal, 1);
    EXPECT_EQ(valueExpr->GetReturnTypeId(), OMNI_VEC_TYPE_DOUBLE);

    EXPECT_EQ(lowerExpr->GetType(), FIELD_E);
    EXPECT_EQ(lowerExpr->colVal, 0);
    EXPECT_EQ(lowerExpr->GetReturnTypeId(), OMNI_VEC_TYPE_INT);

    EXPECT_EQ(upperExpr->GetType(), FIELD_E);
    EXPECT_EQ(upperExpr->colVal, 2);
    EXPECT_EQ(upperExpr->GetReturnTypeId(), OMNI_VEC_TYPE_LONG);
}

// Test In expression
TEST(ParseTest, parseInOperation)
{
    int vecTypeCount = 3;
    string expr = "$operator$IN:4(#1, #0, #2)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_DOUBLE),
        VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_BOOLEAN);
    InExpr *inExpr = static_cast<InExpr *>(result);
    auto args = inExpr->arguments;
    FieldExpr *arg0 = static_cast<FieldExpr *>(args[0]);
    FieldExpr *arg1 = static_cast<FieldExpr *>(args[1]);
    FieldExpr *arg2 = static_cast<FieldExpr *>(args[2]);

    EXPECT_EQ(arg0->GetType(), FIELD_E);
    EXPECT_EQ(arg0->colVal, 1);
    EXPECT_EQ(arg0->GetReturnTypeId(), OMNI_VEC_TYPE_DOUBLE);

    EXPECT_EQ(arg1->GetType(), FIELD_E);
    EXPECT_EQ(arg1->colVal, 0);
    EXPECT_EQ(arg1->GetReturnTypeId(), OMNI_VEC_TYPE_INT);

    EXPECT_EQ(arg2->GetType(), FIELD_E);
    EXPECT_EQ(arg2->colVal, 2);
    EXPECT_EQ(arg2->GetReturnTypeId(), OMNI_VEC_TYPE_LONG);
}

// Test coalesce expression
TEST(ParseTest, parseCoalesceOperation)
{
    int vecTypeCount = 3;
    string expr = "COALESCE:1(#1, #0)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_INT);
    CoalesceExpr *coalesceExpr = static_cast<CoalesceExpr *>(result);
    FieldExpr *value1 = static_cast<FieldExpr *>(coalesceExpr->value1);
    FieldExpr *value2 = static_cast<FieldExpr *>(coalesceExpr->value2);

    EXPECT_EQ(value1->GetType(), FIELD_E);
    EXPECT_EQ(value1->colVal, 1);
    EXPECT_EQ(value1->GetReturnTypeId(), OMNI_VEC_TYPE_INT);

    EXPECT_EQ(value2->GetType(), FIELD_E);
    EXPECT_EQ(value2->colVal, 0);
    EXPECT_EQ(value2->GetReturnTypeId(), OMNI_VEC_TYPE_INT);
}

// Test If expression
TEST(ParseTest, parseIfOperation)
{
    int vecTypeCount = 1;
    string expr = "IF:15(true:4, #0, 'hello':15)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_VARCHAR);
    IfExpr *ifExpr = static_cast<IfExpr *>(result);
    LiteralExpr *condition = static_cast<LiteralExpr *>(ifExpr->condition);
    FieldExpr *tExpr = static_cast<FieldExpr *>(ifExpr->trueExpr);
    LiteralExpr *fExpr = static_cast<LiteralExpr *>(ifExpr->falseExpr);

    EXPECT_EQ(condition->GetReturnTypeId(), OMNI_VEC_TYPE_BOOLEAN);
    EXPECT_EQ(condition->GetType(), LITERAL_E);
    EXPECT_EQ(condition->boolVal, true);

    EXPECT_EQ(tExpr->GetType(), FIELD_E);
    EXPECT_EQ(tExpr->colVal, 0);
    EXPECT_EQ(tExpr->GetReturnTypeId(), OMNI_VEC_TYPE_VARCHAR);

    EXPECT_EQ(fExpr->GetType(), LITERAL_E);
    EXPECT_STREQ(fExpr->stringVal->c_str(), "hello");
}

// Test IsNull expression
TEST(ParseTest, parseIsNullOperation)
{
    int vecTypeCount = 1;
    string expr = "IS_NULL:4(#0)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_CHAR) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_BOOLEAN);
    IsNullExpr *isNullExpr = static_cast<IsNullExpr *>(result);
    FieldExpr *value = static_cast<FieldExpr *>(isNullExpr->value);

    EXPECT_EQ(value->GetType(), FIELD_E);
    EXPECT_EQ(value->colVal, 0);
    EXPECT_EQ(value->GetReturnTypeId(), OMNI_VEC_TYPE_CHAR);
}

// Test IsNotNull expression
TEST(ParseTest, parseIsNotNullOperation)
{
    int vecTypeCount = 1;
    string expr = "IS_NOT_NULL:4(#0)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_CHAR) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_BOOLEAN);
    UnaryExpr *unaryExpr = static_cast<UnaryExpr *>(result);
    EXPECT_EQ(unaryExpr->op, NOT);
    IsNullExpr *isNullExpr = static_cast<IsNullExpr *>(unaryExpr->exp);
    FieldExpr *value = static_cast<FieldExpr *>(isNullExpr->value);

    EXPECT_EQ(value->GetType(), FIELD_E);
    EXPECT_EQ(value->colVal, 0);
    EXPECT_EQ(value->GetReturnTypeId(), OMNI_VEC_TYPE_CHAR);
}

// Test Cast expression
TEST(ParseTest, parseCastOperation)
{
    int vecTypeCount = 1;
    string expr = "CAST:2(#0)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_LONG);
    FuncExpr *castExpr = static_cast<FuncExpr *>(result);
    EXPECT_STREQ(castExpr->funcName.c_str(), "CAST");
    auto args = castExpr->arguments;
    FieldExpr *arg0 = static_cast<FieldExpr *>(args[0]);

    EXPECT_EQ(arg0->GetType(), FIELD_E);
    EXPECT_EQ(arg0->colVal, 0);
    EXPECT_EQ(arg0->GetReturnTypeId(), OMNI_VEC_TYPE_INT);
}

// Test abs expression
TEST(ParseTest, parseAbsOperation)
{
    int vecTypeCount = 1;
    string expr = "abs:3(#0)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DOUBLE) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_DOUBLE);
    FuncExpr *absExpr = static_cast<FuncExpr *>(result);
    EXPECT_STREQ(absExpr->funcName.c_str(), "abs");
    auto args = absExpr->arguments;
    FieldExpr *arg0 = static_cast<FieldExpr *>(args[0]);

    EXPECT_EQ(arg0->GetType(), FIELD_E);
    EXPECT_EQ(arg0->colVal, 0);
    EXPECT_EQ(arg0->GetReturnTypeId(), OMNI_VEC_TYPE_DOUBLE);
}

// test substr
TEST(ParseTest, parseSubstrOperation)
{
    int vecTypeCount = 1;
    string expr = "substr:15(#0, 1:1, 6:1)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_VARCHAR);
    FuncExpr *absExpr = static_cast<FuncExpr *>(result);
    EXPECT_STREQ(absExpr->funcName.c_str(), "substr");
    auto args = absExpr->arguments;
    FieldExpr *value = static_cast<FieldExpr *>(args[0]);
    LiteralExpr *index = static_cast<LiteralExpr *>(args[1]);
    LiteralExpr *length = static_cast<LiteralExpr *>(args[2]);

    EXPECT_EQ(value->GetType(), FIELD_E);
    EXPECT_EQ(value->colVal, 0);
    EXPECT_EQ(value->GetReturnTypeId(), OMNI_VEC_TYPE_VARCHAR);

    EXPECT_EQ(index->GetType(), LITERAL_E);
    EXPECT_EQ(index->intVal, 1);
    EXPECT_EQ(index->GetReturnTypeId(), OMNI_VEC_TYPE_INT);

    EXPECT_EQ(length->GetType(), LITERAL_E);
    EXPECT_EQ(length->intVal, 6);
    EXPECT_EQ(length->GetReturnTypeId(), OMNI_VEC_TYPE_INT);
}

// test concat
TEST(ParseTest, parseConcatOperation)
{
    int vecTypeCount = 1;
    string expr = "concat:15(#0, 'hello':15)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_VARCHAR);
    FuncExpr *absExpr = static_cast<FuncExpr *>(result);
    EXPECT_STREQ(absExpr->funcName.c_str(), "concat");
    auto args = absExpr->arguments;
    FieldExpr *value1 = static_cast<FieldExpr *>(args[0]);
    LiteralExpr *value2 = static_cast<LiteralExpr *>(args[1]);

    EXPECT_EQ(value1->GetType(), FIELD_E);
    EXPECT_EQ(value1->colVal, 0);
    EXPECT_EQ(value1->GetReturnTypeId(), OMNI_VEC_TYPE_VARCHAR);

    EXPECT_EQ(value2->GetType(), LITERAL_E);
    EXPECT_STREQ(value2->stringVal->c_str(), "hello");
    EXPECT_EQ(value2->GetReturnTypeId(), OMNI_VEC_TYPE_VARCHAR);
}

// Test Like
TEST(ParseTest, parseLikeOperation)
{
    int vecTypeCount = 1;
    string expr = "LIKE:4(#0, '%hello':15)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_BOOLEAN);
    FuncExpr *absExpr = static_cast<FuncExpr *>(result);
    EXPECT_STREQ(absExpr->funcName.c_str(), "LIKE");
    auto args = absExpr->arguments;
    FieldExpr *value = static_cast<FieldExpr *>(args[0]);
    LiteralExpr *index = static_cast<LiteralExpr *>(args[1]);

    EXPECT_EQ(value->GetType(), FIELD_E);
    EXPECT_EQ(value->colVal, 0);
    EXPECT_EQ(value->GetReturnTypeId(), OMNI_VEC_TYPE_VARCHAR);

    EXPECT_EQ(index->GetType(), LITERAL_E);
    EXPECT_STREQ(index->stringVal->c_str(), ".*hello");
    EXPECT_EQ(index->GetReturnTypeId(), OMNI_VEC_TYPE_VARCHAR);
}

// Test mm3hash
TEST(ParseTest, parseMM3HashOperation)
{
    int vecTypeCount = 1;
    string expr = "mm3hash:1(#0, 2:1)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_INT);
    FuncExpr *absExpr = static_cast<FuncExpr *>(result);
    EXPECT_STREQ(absExpr->funcName.c_str(), "mm3hash");
    auto args = absExpr->arguments;
    FieldExpr *value = static_cast<FieldExpr *>(args[0]);
    LiteralExpr *index = static_cast<LiteralExpr *>(args[1]);

    EXPECT_EQ(value->GetType(), FIELD_E);
    EXPECT_EQ(value->colVal, 0);
    EXPECT_EQ(value->GetReturnTypeId(), OMNI_VEC_TYPE_VARCHAR);

    EXPECT_EQ(index->GetType(), LITERAL_E);
    EXPECT_EQ(index->intVal, 2);
    EXPECT_EQ(index->GetReturnTypeId(), OMNI_VEC_TYPE_INT);
}

// Test combine hash
TEST(ParseTest, parseCombineHashOperation)
{
    int vecTypeCount = 1;
    string expr = "combine_hash:2(#0, 2:2)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_LONG);
    FuncExpr *absExpr = static_cast<FuncExpr *>(result);
    EXPECT_STREQ(absExpr->funcName.c_str(), "combine_hash");
    auto args = absExpr->arguments;
    FieldExpr *value = static_cast<FieldExpr *>(args[0]);
    LiteralExpr *index = static_cast<LiteralExpr *>(args[1]);

    EXPECT_EQ(value->GetType(), FIELD_E);
    EXPECT_EQ(value->colVal, 0);
    EXPECT_EQ(value->GetReturnTypeId(), OMNI_VEC_TYPE_LONG);

    EXPECT_EQ(index->GetType(), LITERAL_E);
    EXPECT_EQ(index->longVal, 2);
    EXPECT_EQ(index->GetReturnTypeId(), OMNI_VEC_TYPE_LONG);
}

// Test pmod hash
TEST(ParseTest, parsePmodOperation)
{
    int vecTypeCount = 1;
    string expr = "pmod:1(#0, 5:1)";
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result->GetReturnTypeId(), OMNI_VEC_TYPE_INT);
    FuncExpr *absExpr = static_cast<FuncExpr *>(result);
    EXPECT_STREQ(absExpr->funcName.c_str(), "pmod");
    auto args = absExpr->arguments;
    FieldExpr *value = static_cast<FieldExpr *>(args[0]);
    LiteralExpr *index = static_cast<LiteralExpr *>(args[1]);

    EXPECT_EQ(value->GetType(), FIELD_E);
    EXPECT_EQ(value->colVal, 0);
    EXPECT_EQ(value->GetReturnTypeId(), OMNI_VEC_TYPE_INT);

    EXPECT_EQ(index->GetType(), LITERAL_E);
    EXPECT_EQ(index->intVal, 5);
    EXPECT_EQ(index->GetReturnTypeId(), OMNI_VEC_TYPE_INT);
}

// Test null expr
TEST(ParseTest, parseNullExprOperation)
{
    int vecTypeCount = 0;
    string expr = "abs:1(2)";
    std::vector<VecType> vecOfTypes = {};
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    EXPECT_EQ(result, nullptr);
}

// test if column is null
TEST(ParseTest, parseNullStringOperation)
{
    int vecTypeCount = 0;
    string expr = "null:0";
    std::vector<VecType> vecOfTypes = {};
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
    FieldExpr *nullExpr = static_cast<FieldExpr *>(result);
    EXPECT_TRUE(nullExpr->isNull);
}

TEST(ParseTest, ParseExpressions)
{
    string exprs[2] = {"$operator$SUBTRACT:1(#0, 10:1)", "$operator$ADD:2(#1, 1:2)"};
    std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG) };
    VecTypes inputTypes(vecOfTypes);
    Parser parser;
    std::vector<Expr *> result = parser.ParseExpressions(exprs, 2, inputTypes);
    // check substract expression
    EXPECT_EQ(result.at(0)->GetType(), BINARY_E);
    BinaryExpr *subtractExpr = static_cast<BinaryExpr *>(result.at(0));
    EXPECT_EQ(subtractExpr->op, SUB);
    FieldExpr *subtractLeft = static_cast<FieldExpr *>(subtractExpr->left);
    LiteralExpr *subtractRight = static_cast<LiteralExpr *>(subtractExpr->right);
    EXPECT_EQ(subtractLeft->GetType(), FIELD_E);
    EXPECT_EQ(subtractLeft->colVal, 0);
    EXPECT_EQ(subtractRight->GetType(), LITERAL_E);
    EXPECT_EQ(subtractRight->intVal, 10);

    // check add expression
    EXPECT_EQ(result.at(1)->GetType(), BINARY_E);
    BinaryExpr *addExpr = static_cast<BinaryExpr *>(result.at(1));
    EXPECT_EQ(addExpr->op, ADD);
    FieldExpr *addLeft = static_cast<FieldExpr *>(addExpr->left);
    LiteralExpr *addRight = static_cast<LiteralExpr *>(addExpr->right);
    EXPECT_EQ(addLeft->GetType(), FIELD_E);
    EXPECT_EQ(addLeft->colVal, 1);
    EXPECT_EQ(addRight->GetType(), LITERAL_E);
    EXPECT_EQ(addRight->longVal, 1);
}
}