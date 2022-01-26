/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: ...
 */
#include "gtest/gtest.h"
#include "../util/test_util.h"
#include "../../src/operator/projection/projection.h"
#include "../../src/vector/vector_helper.h"
#include <string>
#include <vector>
#include <chrono>

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace std;
namespace project_test {

    void testCmpBinaryExpressions(std::vector<Expr *> result, omniruntime::expressions::Operator op,
                                  const int PROJECT_COUNT, bool isBoolResult = false)
    {
        std::vector<DataType> dataTypes = {INT32D, INT64D, DOUBLED , DECIMAL128D, VARCHARD, CHARD};
        for (int i = 0; i < PROJECT_COUNT; i++) {
            BinaryExpr *binaryExpr = static_cast<BinaryExpr *>(result.at(i));
            if (isBoolResult) {
                EXPECT_EQ(binaryExpr->dataType, BOOLD);
            } else {
                EXPECT_EQ(binaryExpr->dataType, dataTypes[i]);
            }
            EXPECT_EQ(binaryExpr->op, op);
            DataExpr *left = static_cast<DataExpr *>(binaryExpr->left);
            EXPECT_EQ(left->isColumn, true);
            EXPECT_EQ(left->colVal, i);

            DataExpr *right = static_cast<DataExpr *>(binaryExpr->right);
            EXPECT_EQ(right->isColumn, false);
            if (i == 3) {
                EXPECT_EQ(right->dataType, INT64D);
            } else if (i == 5) {
                EXPECT_EQ(right->dataType, VARCHARD);
            } else {
                EXPECT_EQ(right->dataType, dataTypes[i]);
            }
            if (i == 0) EXPECT_EQ(right->intVal, i);
            else if (i == 1 || i == 3) EXPECT_EQ(right->longVal, i); // parser sets longVal for INT64D and DECIMAL128D
            else if (i == 2) EXPECT_EQ(right->doubleVal, i);
            else if (i == 4 || i == 5) ASSERT_STREQ(right->stringVal->c_str(), "hello");

            if (i == 5) {
                EXPECT_EQ(right->width, 6);
            }
        }
    }

    void testArithmeticBinaryExpressions(std::vector<Expr *> result, omniruntime::expressions::Operator op,
                                                const int PROJECT_COUNT)
                                                {
        std::vector<DataType> dataTypes = {INT32D, INT64D, DOUBLED , DECIMAL128D};
        for (int i = 0; i < PROJECT_COUNT; i++) {
            BinaryExpr *binaryExpr = static_cast<BinaryExpr *>(result.at(i));
            EXPECT_EQ(binaryExpr->dataType, dataTypes[i]);
            EXPECT_EQ(binaryExpr->op, op);
            DataExpr *left = static_cast<DataExpr *>(binaryExpr->left);
            EXPECT_EQ(left->isColumn, true);
            EXPECT_EQ(left->colVal, i);

            DataExpr *right = static_cast<DataExpr *>(binaryExpr->right);
            EXPECT_EQ(right->isColumn, false);
            if (i == 3) {
                EXPECT_EQ(right->dataType, INT64D);
            } else {
                EXPECT_EQ(right->dataType, dataTypes[i]);
            }
            if (i == 0) EXPECT_EQ(right->intVal, i);
            else if (i == 1 || i == 3) EXPECT_EQ(right->longVal, i); // parser sets longVal for INT64D and DECIMAL128D
            else if (i == 2) EXPECT_EQ(right->doubleVal, i);
        }
}


    // Test Not expression
    TEST (ParseTest, parseNotOperation) {
        const int PROJECT_COUNT = 1;
        int vecTypeCount = 1;
        string expr = "$operator$NOT:4(#0)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_BOOLEAN)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, BOOLD);
        UnaryExpr *notExpr = static_cast<UnaryExpr *>(result);
        EXPECT_EQ(notExpr->op, NOT);
        DataExpr *colExpr = static_cast<DataExpr *>(notExpr->exp);
        EXPECT_EQ(colExpr->isColumn, true);
        EXPECT_EQ(colExpr->colVal, 0);
        }

    // Test Arithmetic Binary Operations
    TEST (ParseTest, parseAddOperation) {
        const int PROJECT_COUNT = 4;
        int vecTypeCount = 4;
        string exprs[PROJECT_COUNT] = {"$operator$ADD:1(#0, 0:1)", "$operator$ADD:2(#1, 1:2)",
                                       "$operator$ADD:3(#2, 2:3)", "$operator$ADD:7(#3, 3:7)"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                            VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        testArithmeticBinaryExpressions(result, ADD, PROJECT_COUNT);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    TEST (ParseTest, parseSubtractOperation) {
        const int PROJECT_COUNT = 4;
        int vecTypeCount = 4;
        string exprs[PROJECT_COUNT] = {"$operator$SUBTRACT:1(#0, 0:1)", "$operator$SUBTRACT:2(#1, 1:2)",
                                       "$operator$SUBTRACT:3(#2, 2:3)", "$operator$SUBTRACT:7(#3, 3:7)"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                            VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        testArithmeticBinaryExpressions(result, SUB, PROJECT_COUNT);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    TEST (ParseTest, parseMultiplyOperation) {
        const int PROJECT_COUNT = 4;
        int vecTypeCount = 4;
        string exprs[PROJECT_COUNT] = {"$operator$MULTIPLY:1(#0, 0:1)", "$operator$MULTIPLY:2(#1, 1:2)",
                                       "$operator$MULTIPLY:3(#2, 2:3)", "$operator$MULTIPLY:7(#3, 3:7)"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                            VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        testArithmeticBinaryExpressions(result, MUL, PROJECT_COUNT);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    TEST (ParseTest, parseDivideOperation) {
        const int PROJECT_COUNT = 4;
        int vecTypeCount = 4;
        string exprs[PROJECT_COUNT] = {"$operator$DIVIDE:1(#0, 0:1)", "$operator$DIVIDE:2(#1, 1:2)",
                                       "$operator$DIVIDE:3(#2, 2:3)", "$operator$DIVIDE:7(#3, 3:7)"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                            VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        testArithmeticBinaryExpressions(result, DIV, PROJECT_COUNT);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    TEST (ParseTest, parseModulusOperation) {
        const int PROJECT_COUNT = 4;
        int vecTypeCount = 4;
        string exprs[PROJECT_COUNT] = {"$operator$MODULUS:1(#0, 0:1)", "$operator$MODULUS:2(#1, 1:2)",
                                       "$operator$MODULUS:3(#2, 2:3)", "$operator$MODULUS:7(#3, 3:7)"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                            VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        testArithmeticBinaryExpressions(result, MOD, PROJECT_COUNT);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    // Test Compare Binary Operations
    TEST (ParseTest, parseLTOperation) {
        const int PROJECT_COUNT = 4;
        int vecTypeCount = 4;
        string exprs[PROJECT_COUNT] = {"$operator$LESS_THAN:4(#0, 0:1)", "$operator$LESS_THAN:4(#1, 1:2)",
                                       "$operator$LESS_THAN:4(#2, 2:3)", "$operator$LESS_THAN:4(#3, 3:7)"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                            VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        testCmpBinaryExpressions(result, LT, PROJECT_COUNT, true);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    TEST (ParseTest, parseLTEOperation) {
        const int PROJECT_COUNT = 4;
        int vecTypeCount = 4;
        string exprs[PROJECT_COUNT] = {"$operator$LESS_THAN_OR_EQUAL:4(#0, 0:1)",
                                       "$operator$LESS_THAN_OR_EQUAL:4(#1, 1:2)",
                                       "$operator$LESS_THAN_OR_EQUAL:4(#2, 2:3)",
                                       "$operator$LESS_THAN_OR_EQUAL:4(#3, 3:7)"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                            VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        testCmpBinaryExpressions(result, LTE, PROJECT_COUNT, true);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    TEST (ParseTest, parseGTOperation) {
        const int PROJECT_COUNT = 4;
        int vecTypeCount = 4;
        string exprs[PROJECT_COUNT] = {"$operator$GREATER_THAN:4(#0, 0:1)", "$operator$GREATER_THAN:4(#1, 1:2)",
                                       "$operator$GREATER_THAN:4(#2, 2:3)", "$operator$GREATER_THAN:4(#3, 3:7)"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                            VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        testCmpBinaryExpressions(result, GT, PROJECT_COUNT, true);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    TEST (ParseTest, parseGTEOperation) {
        const int PROJECT_COUNT = 4;
        int vecTypeCount = 4;
        string exprs[PROJECT_COUNT] = {"$operator$GREATER_THAN_OR_EQUAL:4(#0, 0:1)",
                                       "$operator$GREATER_THAN_OR_EQUAL:4(#1, 1:2)",
                                       "$operator$GREATER_THAN_OR_EQUAL:4(#2, 2:3)",
                                       "$operator$GREATER_THAN_OR_EQUAL:4(#3, 3:7)"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                            VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        testCmpBinaryExpressions(result, GTE, PROJECT_COUNT, true);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    TEST (ParseTest, parseEQOperation) {
        const int PROJECT_COUNT = 6;
        int vecTypeCount = 6;
        string exprs[PROJECT_COUNT] = {"$operator$EQUAL:4(#0, 0:1)", "$operator$EQUAL:4(#1, 1:2)",
                                       "$operator$EQUAL:4(#2, 2:3)", "$operator$EQUAL:4(#3, 3:7)",
                                       "$operator$EQUAL:4(#4, 'hello':15)", "$operator$EQUAL:4(#5, 'hello':16[5])"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                            VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128),
                                            VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_CHAR)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        testCmpBinaryExpressions(result, EQ, PROJECT_COUNT, true);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    TEST (ParseTest, parseNEQOperation) {
        const int PROJECT_COUNT = 6;
        int vecTypeCount = 6;
        string exprs[PROJECT_COUNT] = {"$operator$NOT_EQUAL:4(#0, 0:1)", "$operator$NOT_EQUAL:4(#1, 1:2)",
                                       "$operator$NOT_EQUAL:4(#2, 2:3)", "$operator$NOT_EQUAL:4(#3, 3:7)",
                                       "$operator$NOT_EQUAL:4(#4, 'hello':15)",
                                       "$operator$NOT_EQUAL:4(#5, 'hello':16[5])"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG),
                                            VecType(OMNI_VEC_TYPE_DOUBLE), VecType(OMNI_VEC_TYPE_DECIMAL128),
                                            VecType(OMNI_VEC_TYPE_VARCHAR), VecType(OMNI_VEC_TYPE_CHAR)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        testCmpBinaryExpressions(result, NEQ, PROJECT_COUNT, true);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    // Test Logic Operations
    TEST (ParseTest, parseLogicOperations) {
        const int PROJECT_COUNT = 2;
        int vecTypeCount = 2;
        string exprs[PROJECT_COUNT] = {"$operator$AND:4(#0, true:4)", "$operator$OR:4(#1, false:4)"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_BOOLEAN), VecType(OMNI_VEC_TYPE_BOOLEAN)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, PROJECT_COUNT, inputTypes, vecTypeCount);
        EXPECT_EQ(result.size(), PROJECT_COUNT);
        for (int i = 0; i < PROJECT_COUNT; i++) {
            BinaryExpr *logExpr = static_cast<BinaryExpr *>(result.at(i));
            EXPECT_EQ(logExpr->dataType, BOOLD);
            DataExpr *logLeft = static_cast<DataExpr *>(logExpr->left);
            DataExpr *logRight = static_cast<DataExpr *>(logExpr->right);
            EXPECT_TRUE(logLeft->isColumn);
            EXPECT_EQ(logLeft->colVal, i);
            EXPECT_FALSE(logRight->isColumn);
            if (i == 0) {
                EXPECT_TRUE(logRight->boolVal);
                EXPECT_EQ(logExpr->op, AND);
            }
            if (i == 1){
                EXPECT_FALSE(logRight->boolVal);
                EXPECT_EQ(logExpr->op, OR);
            }

        }





        for (int i = 0; i < PROJECT_COUNT; i++) {
            delete result.at(i);
        }
    }

    // Test Between expression
    TEST (ParseTest, parseBetweenOperation) {
        int vecTypeCount = 3;
        string expr = "$operator$BETWEEN:4(#1, #0, #2)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_DOUBLE),
                                            VecType(OMNI_VEC_TYPE_LONG)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, BOOLD);
        BetweenExpr *betweenExpr = static_cast<BetweenExpr *>(result);
        DataExpr *valueExpr = static_cast<DataExpr *>(betweenExpr->value);
        DataExpr *lowerExpr = static_cast<DataExpr *>(betweenExpr->lowerBound);
        DataExpr *upperExpr = static_cast<DataExpr *>(betweenExpr->upperBound);

        EXPECT_TRUE(valueExpr->isColumn);
        EXPECT_EQ(valueExpr->colVal, 1);
        EXPECT_EQ(valueExpr->dataType, DOUBLED);

        EXPECT_TRUE(lowerExpr->isColumn);
        EXPECT_EQ(lowerExpr->colVal, 0);
        EXPECT_EQ(lowerExpr->dataType, INT32D);

        EXPECT_TRUE(upperExpr->isColumn);
        EXPECT_EQ(upperExpr->colVal, 2);
        EXPECT_EQ(upperExpr->dataType, INT64D);
    }

    // Test In expression
    TEST (ParseTest, parseInOperation) {
        int vecTypeCount = 3;
        string expr = "$operator$IN:4(#1, #0, #2)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_DOUBLE),
                                            VecType(OMNI_VEC_TYPE_LONG)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, BOOLD);
        InExpr *inExpr = static_cast<InExpr *>(result);
        auto args = inExpr->arguments;
        DataExpr *arg0 = static_cast<DataExpr *>(args[0]);
        DataExpr *arg1 = static_cast<DataExpr *>(args[1]);
        DataExpr *arg2 = static_cast<DataExpr *>(args[2]);

        EXPECT_TRUE(arg0->isColumn);
        EXPECT_EQ(arg0->colVal, 1);
        EXPECT_EQ(arg0->dataType, DOUBLED);

        EXPECT_TRUE(arg1->isColumn);
        EXPECT_EQ(arg1->colVal, 0);
        EXPECT_EQ(arg1->dataType, INT32D);

        EXPECT_TRUE(arg2->isColumn);
        EXPECT_EQ(arg2->colVal, 2);
        EXPECT_EQ(arg2->dataType, INT64D);
    }

    // Test coalesce expression
    TEST (ParseTest, parseCoalesceOperation) {
        int vecTypeCount = 3;
        string expr = "COALESCE:1(#1, #0)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_INT)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, INT32D);
        CoalesceExpr *coalesceExpr = static_cast<CoalesceExpr *>(result);
        DataExpr *value1 = static_cast<DataExpr *>(coalesceExpr->value1);
        DataExpr *value2 = static_cast<DataExpr *>(coalesceExpr->value2);

        EXPECT_TRUE(value1->isColumn);
        EXPECT_EQ(value1->colVal, 1);
        EXPECT_EQ(value1->dataType, INT32D);

        EXPECT_TRUE(value2->isColumn);
        EXPECT_EQ(value2->colVal, 0);
        EXPECT_EQ(value2->dataType, INT32D);
    }

    // Test If expression
    TEST (ParseTest, parseIfOperation) {
        int vecTypeCount = 1;
        string expr = "IF:15(true:4, #0, 'hello':15)";
        std::vector<VecType> vecOfTypes = {VecType(OMNI_VEC_TYPE_VARCHAR)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, VARCHARD);
        IfExpr *ifExpr = static_cast<IfExpr *>(result);
        DataExpr *condition = static_cast<DataExpr *>(ifExpr->condition);
        DataExpr *tExpr = static_cast<DataExpr *>(ifExpr->trueExpr);
        DataExpr *fExpr = static_cast<DataExpr *>(ifExpr->falseExpr);

        EXPECT_FALSE(condition->isColumn);
        EXPECT_EQ(condition->dataType, BOOLD);
        EXPECT_EQ(condition->boolVal, true);

        EXPECT_TRUE(tExpr->isColumn);
        EXPECT_EQ(tExpr->colVal, 0);
        EXPECT_EQ(tExpr->dataType, VARCHARD);

        EXPECT_FALSE(fExpr->isColumn);
        EXPECT_STREQ(fExpr->stringVal->c_str(), "hello");
    }

    // Test IsNull expression
    TEST (ParseTest, parseIsNullOperation) {
        int vecTypeCount = 1;
        string expr = "IS_NULL:4(#0)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_CHAR)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, BOOLD);
        IsNullExpr *isNullExpr = static_cast<IsNullExpr *>(result);
        DataExpr *value = static_cast<DataExpr *>(isNullExpr->value);

        EXPECT_TRUE(value->isColumn);
        EXPECT_EQ(value->colVal, 0);
        EXPECT_EQ(value->dataType, CHARD);

    }

    // Test IsNotNull expression
    TEST (ParseTest, parseIsNotNullOperation) {
        int vecTypeCount = 1;
        string expr = "IS_NOT_NULL:4(#0)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_CHAR)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, BOOLD);
        UnaryExpr *unaryExpr = static_cast<UnaryExpr *>(result);
        EXPECT_EQ(unaryExpr->op, NOT);
        IsNullExpr *isNullExpr = static_cast<IsNullExpr *>(unaryExpr->exp);
        DataExpr *value = static_cast<DataExpr *>(isNullExpr->value);

        EXPECT_TRUE(value->isColumn);
        EXPECT_EQ(value->colVal, 0);
        EXPECT_EQ(value->dataType, CHARD);

    }

    // Test Cast expression
    TEST (ParseTest, parseCastOperation) {
        int vecTypeCount = 1;
        string expr = "CAST:2(#0)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, INT64D);
        FuncExpr *castExpr = static_cast<FuncExpr *>(result);
        EXPECT_STREQ(castExpr->funcName.c_str(), "CAST");
        auto args = castExpr->arguments;
        DataExpr *arg0 = static_cast<DataExpr *>(args[0]);

        EXPECT_TRUE(arg0->isColumn);
        EXPECT_EQ(arg0->colVal, 0);
        EXPECT_EQ(arg0->dataType, INT32D);
    }

    // Test abs expression
    TEST (ParseTest, parseAbsOperation) {
        int vecTypeCount = 1;
        string expr = "abs:3(#0)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_DOUBLE)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, DOUBLED);
        FuncExpr *absExpr = static_cast<FuncExpr *>(result);
        EXPECT_STREQ(absExpr->funcName.c_str(), "abs");
        auto args = absExpr->arguments;
        DataExpr *arg0 = static_cast<DataExpr *>(args[0]);

        EXPECT_TRUE(arg0->isColumn);
        EXPECT_EQ(arg0->colVal, 0);
        EXPECT_EQ(arg0->dataType, DOUBLED);

    }

    // test substr
    TEST (ParseTest, parseSubstrOperation) {
        int vecTypeCount = 1;
        string expr = "substr:15(#0, 1:1, 6:1)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, VARCHARD);
        FuncExpr *absExpr = static_cast<FuncExpr *>(result);
        EXPECT_STREQ(absExpr->funcName.c_str(), "substr");
        auto args = absExpr->arguments;
        DataExpr *value = static_cast<DataExpr *>(args[0]);
        DataExpr *index = static_cast<DataExpr *>(args[1]);
        DataExpr *length = static_cast<DataExpr *>(args[2]);

        EXPECT_TRUE(value->isColumn);
        EXPECT_EQ(value->colVal, 0);
        EXPECT_EQ(value->dataType, VARCHARD);

        EXPECT_FALSE(index->isColumn);
        EXPECT_EQ(index->intVal, 1);
        EXPECT_EQ(index->dataType, INT32D);

        EXPECT_FALSE(length->isColumn);
        EXPECT_EQ(length->intVal, 6);
        EXPECT_EQ(length->dataType, INT32D);
    }

    // test concat
    TEST (ParseTest, parseConcatOperation) {
        int vecTypeCount = 1;
        string expr = "concat:15(#0, 'hello':15)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, VARCHARD);
        FuncExpr *absExpr = static_cast<FuncExpr *>(result);
        EXPECT_STREQ(absExpr->funcName.c_str(), "concat");
        auto args = absExpr->arguments;
        DataExpr *value1 = static_cast<DataExpr *>(args[0]);
        DataExpr *value2 = static_cast<DataExpr *>(args[1]);

        EXPECT_TRUE(value1->isColumn);
        EXPECT_EQ(value1->colVal, 0);
        EXPECT_EQ(value1->dataType, VARCHARD);

        EXPECT_FALSE(value2->isColumn);
        EXPECT_STREQ(value2->stringVal->c_str(), "hello");
        EXPECT_EQ(value2->dataType, VARCHARD);
    }

    // Test Like
    TEST (ParseTest, parseLikeOperation) {
        int vecTypeCount = 1;
        string expr = "LIKE:15(#0, '%hello':15)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, VARCHARD);
        FuncExpr *absExpr = static_cast<FuncExpr *>(result);
        EXPECT_STREQ(absExpr->funcName.c_str(), "LIKE");
        auto args = absExpr->arguments;
        DataExpr *value = static_cast<DataExpr *>(args[0]);
        DataExpr *index = static_cast<DataExpr *>(args[1]);

        EXPECT_TRUE(value->isColumn);
        EXPECT_EQ(value->colVal, 0);
        EXPECT_EQ(value->dataType, VARCHARD);

        EXPECT_FALSE(index->isColumn);
        EXPECT_STREQ(index->stringVal->c_str(), ".*hello");
        EXPECT_EQ(index->dataType, VARCHARD);
    }

    // Test mm3hash
    TEST (ParseTest, parseMM3HashOperation) {
        int vecTypeCount = 1;
        string expr = "mm3hash:1(#0, 2:1)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_VARCHAR)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, INT32D);
        FuncExpr *absExpr = static_cast<FuncExpr *>(result);
        EXPECT_STREQ(absExpr->funcName.c_str(), "mm3hash");
        auto args = absExpr->arguments;
        DataExpr *value = static_cast<DataExpr *>(args[0]);
        DataExpr *index = static_cast<DataExpr *>(args[1]);

        EXPECT_TRUE(value->isColumn);
        EXPECT_EQ(value->colVal, 0);
        EXPECT_EQ(value->dataType, VARCHARD);

        EXPECT_FALSE(index->isColumn);
        EXPECT_EQ(index->intVal, 2);
        EXPECT_EQ(index->dataType, INT32D);
    }

    // Test combine hash
    TEST (ParseTest, parseCombineHashOperation) {
        int vecTypeCount = 1;
        string expr = "combine_hash:2(#0, 2:2)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_LONG)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, INT64D);
        FuncExpr *absExpr = static_cast<FuncExpr *>(result);
        EXPECT_STREQ(absExpr->funcName.c_str(), "combine_hash");
        auto args = absExpr->arguments;
        DataExpr *value = static_cast<DataExpr *>(args[0]);
        DataExpr *index = static_cast<DataExpr *>(args[1]);

        EXPECT_TRUE(value->isColumn);
        EXPECT_EQ(value->colVal, 0);
        EXPECT_EQ(value->dataType, INT64D);

        EXPECT_FALSE(index->isColumn);
        EXPECT_EQ(index->longVal, 2);
        EXPECT_EQ(index->dataType, INT64D);
    }

    // Test pmod hash
    TEST (ParseTest, parsePmodOperation) {
        int vecTypeCount = 1;
        string expr = "pmod:1(#0, 5:1)";
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result->dataType, INT32D);
        FuncExpr *absExpr = static_cast<FuncExpr *>(result);
        EXPECT_STREQ(absExpr->funcName.c_str(), "pmod");
        auto args = absExpr->arguments;
        DataExpr *value = static_cast<DataExpr *>(args[0]);
        DataExpr *index = static_cast<DataExpr *>(args[1]);

        EXPECT_TRUE(value->isColumn);
        EXPECT_EQ(value->colVal, 0);
        EXPECT_EQ(value->dataType, INT32D);

        EXPECT_FALSE(index->isColumn);
        EXPECT_EQ(index->intVal, 5);
        EXPECT_EQ(index->dataType, INT32D);
    }

    // Test null expr
    TEST (ParseTest, parseNullExprOperation) {
        int vecTypeCount = 0;
        string expr = "abs:1(2)";
        std::vector<VecType> vecOfTypes = {};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        EXPECT_EQ(result, nullptr);
    }

    // test if column is null
    TEST (ParseTest, parseNullStringOperation) {
        int vecTypeCount = 0;
        string expr = "null:0";
        std::vector<VecType> vecOfTypes = {};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        Expr *result = parser.ParseRowExpression(expr, inputTypes, vecTypeCount);
        DataExpr *nullExpr = static_cast<DataExpr *>(result);
        EXPECT_TRUE(nullExpr->isNull);
    }

    TEST (ParseTest, ParseExpressions) {
        string exprs[2] = {"$operator$SUBTRACT:1(#0, 10:1)", "$operator$ADD:2(#1, 1:2)"};
        std::vector<VecType> vecOfTypes = { VecType(OMNI_VEC_TYPE_INT), VecType(OMNI_VEC_TYPE_LONG)};
        VecTypes inputTypes(vecOfTypes);
        Parser parser;
        std::vector<Expr *> result = parser.ParseExpressions(exprs, 2, inputTypes, 2);
        // check substract expression
        EXPECT_EQ(result.at(0)->GetType(), BINARY_E);
        BinaryExpr *subtractExpr = static_cast<BinaryExpr *>(result.at(0));
        EXPECT_EQ(subtractExpr->op, SUB);
        EXPECT_EQ(subtractExpr->left->GetType(), DATA_E);
        DataExpr *subtractLeft = static_cast<DataExpr *>(subtractExpr->left);
        DataExpr *subtractRight = static_cast<DataExpr *>(subtractExpr->right);
        EXPECT_EQ(subtractLeft->isColumn, true);
        EXPECT_EQ(subtractLeft->colVal, 0);
        EXPECT_EQ(subtractRight->isColumn, false);
        EXPECT_EQ(subtractRight->intVal, 10);

        // check add expression
        EXPECT_EQ(result.at(1)->GetType(), BINARY_E);
        BinaryExpr *addExpr = static_cast<BinaryExpr *>(result.at(1));
        EXPECT_EQ(addExpr->op, ADD);
        EXPECT_EQ(addExpr->left->GetType(), DATA_E);
        DataExpr *addLeft = static_cast<DataExpr *>(addExpr->left);
        DataExpr *addRight = static_cast<DataExpr *>(addExpr->right);
        EXPECT_EQ(addLeft->isColumn, true);
        EXPECT_EQ(addLeft->colVal, 1);
        EXPECT_EQ(addRight->isColumn, false);
        EXPECT_EQ(addRight->longVal, 1);
    }


}