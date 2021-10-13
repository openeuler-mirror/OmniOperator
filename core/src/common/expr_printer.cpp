/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: print expression tree methods
 */
#include "expr_printer.h"
#include <cstdio>
#include <iostream>
#include <string>
#include <algorithm>
#include "../util/debug.h"

using namespace omniruntime::expressions;
using namespace std;

void ExprPrinter::Visit(BinaryExpr &e)
{
    BinaryExpr *expr = &e;
    switch (expr->op) {
        // Comparison
        case EQ:
            printf("Cmp:%s(EQ, ", DataTypeString(expr->dataType).c_str());
            break;
        case NEQ:
            printf("Cmp:%s(NEQ, ", DataTypeString(expr->dataType).c_str());
            break;
        case LT:
            printf("Cmp:%s(LT, ", DataTypeString(expr->dataType).c_str());
            break;
        case LTE:
            printf("Cmp:%s(LTE, ", DataTypeString(expr->dataType).c_str());
            break;
        case GT:
            printf("Cmp:%s(GT, ", DataTypeString(expr->dataType).c_str());
            break;
        case GTE:
            printf("Cmp:%s(GTE, ", DataTypeString(expr->dataType).c_str());
            break;

            // Logical
        case AND:
            printf("Bin:%s(AND, ", DataTypeString(expr->dataType).c_str());
            break;
        case OR:
            printf("Bin:%s(OR, ", DataTypeString(expr->dataType).c_str());
            break;

            // Arithmetic
        case ADD:
            printf("Arith:%s(ADD, ", DataTypeString(expr->dataType).c_str());
            break;
        case SUB:
            printf("Arith:%s(SUB, ", DataTypeString(expr->dataType).c_str());
            break;
        case MUL:
            printf("Arith:%s(MUL, ", DataTypeString(expr->dataType).c_str());
            break;
        case DIV:
            printf("Arith:%s(DIV, ", DataTypeString(expr->dataType).c_str());
            break;
        case MOD:
            printf("Arith:%s(MOD, ", DataTypeString(expr->dataType).c_str());
            break;
        default:
            printf("invalid BinaryOperator %d(", expr->op);
    }
    (expr->left)->Accept(*this);
    printf(", ");
    (expr->right)->Accept(*this);
    printf(")");
}

void ExprPrinter::Visit(UnaryExpr &e)
{
    UnaryExpr *expr = &e;
    switch (expr->op) {
        case NOT:
            printf("Unary:%s(NOT, ", DataTypeString(expr->dataType).c_str());
            break;
        default:
            printf("invalid UnaryOperator %d(", expr->op);
            break;
    }
    (expr->exp)->Accept(*this);
    printf(")");
}

void ExprPrinter::Visit(DataExpr &e)
{
    DataExpr *expr = &e;
    const bool printWithTypes = false; // for debugging types
    if (expr->isColumn) {
        printf("#%d", expr->colVal);
    } else {
        switch (expr->dataType) {
            case BOOLD: {
                if (printWithTypes) {
                    printf("bool_");
}
                if (expr->boolVal) {
                    printf("true");
                } else {
                    printf("false");
}
                break;
            }
            case INT32D:
                if (printWithTypes) {
                    printf("i32_");
}
                printf("%d", expr->intVal);
                break;
            case INT64D:
                if (printWithTypes) {
                    printf("i64_");
}
                printf("%ld", expr->longVal);
                break;
            case DOUBLED:
                if (printWithTypes) {
                    printf("d64_");
}
                printf("%f", expr->doubleVal);
                break;
            case STRINGD:
                if (printWithTypes) {
                    printf("s_");
}
                printf("'%s'", (expr->stringVal)->c_str());
                break;
            default:
                printf("invalid DataType %d", expr->dataType);
        }
    }
}

void ExprPrinter::Visit(InExpr &e)
{
    InExpr *expr = &e;
    printf("In:%s(", DataTypeString(expr->dataType).c_str());
    for (int i = 0; i < expr->arguments.size(); i++) {
        (expr->arguments[i])->Accept(*this);
        if (i == expr->arguments.size() - 1) {
            printf(")");
        } else {
            printf(", ");
        }
    }
}

void ExprPrinter::Visit(BetweenExpr &e)
{
    BetweenExpr *expr = &e;
    printf("Between:%s(", DataTypeString(expr->dataType).c_str());
    (expr->value)->Accept(*this);
    printf(", ");
    (expr->lowerBound)->Accept(*this);
    printf(", ");
    (expr->upperBound)->Accept(*this);
    printf(")");
}

void ExprPrinter::Visit(IfExpr &e)
{
    IfExpr *expr = &e;
    printf("If:%s(", DataTypeString(expr->dataType).c_str());
    expr->condition->Accept(*this);
    printf(", ");
    expr->trueExpr->Accept(*this);
    printf(", ");
    expr->falseExpr->Accept(*this);
    printf(")");
}

void ExprPrinter::Visit(CoalesceExpr &e)
{
    CoalesceExpr *expr = &e;
    printf("Coalesce:%s(", DataTypeString(expr->dataType).c_str());
    expr->value1->Accept(*this);
    printf(", ");
    expr->value2->Accept(*this);
    printf(")");
}

void ExprPrinter::Visit(IsNullExpr &e)
{
    IsNullExpr *expr = &e;
    printf("IsNull:%s(", DataTypeString(expr->dataType).c_str());
    expr->value->Accept(*this);
    printf(")");
}

void ExprPrinter::Visit(FuncExpr &e)
{
    FuncExpr *expr = &e;
    printf("%s:%s(", expr->funcName.c_str(), DataTypeString(expr->dataType).c_str());

    for (int i = 0; i < expr->arguments.size(); i++) {
        (expr->arguments[i])->Accept(*this);
        if (i == expr->arguments.size() - 1) {
            printf(")");
        } else {
            printf(", ");
        }
    }
}
