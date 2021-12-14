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
#include <map>

using namespace omniruntime::expressions;
using namespace std;

string ExprPrinter::BinaryExprPrinterHelper(const Operator &op) const
{
    switch (op) {
        case EQ: return "Cmp:%s(EQ, ";
        case NEQ: return "Cmp:%s(NEQ, ";
        case LT: return "Cmp:%s(LT, ";
        case LTE: return "Cmp:%s(LTE, ";
        case GT: return "Cmp:%s(GT, ";
        case GTE: return "Cmp:%s(GTE, ";
        case AND: return "Bin:%s(AND, ";
        case OR: return "Bin:%s(OR, ";
        case ADD: return "Arith:%s(ADD, ";
        case SUB: return "Arith:%s(SUB, ";
        case MUL: return "Arith:%s(MUL, ";
        case DIV: return "Arith:%s(DIV, ";
        case MOD: return "Arith:%s(MOD, ";
        default: return "Invalid";
    }
}

string ExprPrinter::GenerateIndentation() const
{
    string indent = "";
    for (int i = 0; i < this->indentationDepth; i++) {
        indent.append("\t");
    }
    return indent;
}

/*
 * EXAMPLE
 *
 * Bin:bool(OR,
 *     Cmp:bool(EQ,
 *         #7,
 *         'Winter
 *     ),
 *     Cmp:bool(EQ,
 *         #2,
 *         'Summer'
 *      )
 * )
 *
 */
void ExprPrinter::Visit(BinaryExpr &e)
{
    string indent = GenerateIndentation();
    BinaryExpr *expr = &e;
    string message = BinaryExprPrinterHelper(expr->op);
    if (message == "Invalid") {
        printf(
            "invalid BinaryOperator %d(",
            expr->op);
    } else {
        printf(
            (indent + (message.append("\n"))).c_str(),
            DataTypeString(e).c_str()
            );
    }
    this->indentationDepth++;
    (expr->left)->Accept(*this);
    printf(",\n");

    (expr->right)->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

/*
 * EXAMPLE
 *
 * Unary:bool(NOT,
 *     IsNull:bool(
 *         #0
 *     )
 * )
 *
 */
void ExprPrinter::Visit(UnaryExpr &e)
{
    string indent = GenerateIndentation();
    UnaryExpr *expr = &e;
    switch (expr->op) {
        case NOT:
            printf(
                (indent + "Unary:%s(NOT,\n").c_str(),
                DataTypeString(e).c_str()
                );
            break;
        default:
            printf(
                "invalid UnaryOperator %d(",
                expr->op);
            break;
    }
    this->indentationDepth++;
    (expr->exp)->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

void ExprPrinter::Visit(DataExpr &e)
{
    string indent = GenerateIndentation();
    DataExpr *expr = &e;
    const bool printWithTypes = false; // for debugging types
    if (expr->isColumn) {
        if (expr->dataType == DataType::CHARD) {
            printf(indent.append("#%d[%d]").c_str(), expr->colVal, expr->width);
        } else {
            printf(indent.append("#%d").c_str(), expr->colVal);
        }
    } else {
        switch (expr->dataType) {
            case BOOLD:
                if (printWithTypes) {
                    printf("bool_");
                }
                expr->boolVal ? printf("%s", indent.append("true").c_str()) :
                printf("%s", indent.append("false").c_str());
                break;
            case INT32D:
                if (printWithTypes) {
                    printf("i32_");
                }
                printf(indent.append("%d:%s").c_str(), expr->intVal, DataTypeString(e).c_str());
                break;
            case INT64D:
                if (printWithTypes) {
                    printf("i64_");
                }
                printf(indent.append("%ld:%s").c_str(), expr->longVal, DataTypeString(e).c_str());
                break;
            case DOUBLED:
                if (printWithTypes) {
                    printf("d64_");
                }
                printf(indent.append("%f:%s").c_str(), expr->doubleVal, DataTypeString(e).c_str());
                break;
            case CHARD:
                if (printWithTypes) {
                    printf("s_");
                }
                printf(
                    indent.append("'%s[%d]':%s").c_str(),
                    (expr->stringVal)->c_str(), expr->width, DataTypeString(e).c_str());
                break;
            case VARCHARD:
                if (printWithTypes) {
                    printf("s_");
                }
                printf(
                    indent.append("'%s':%s").c_str(), (expr->stringVal)->c_str(), DataTypeString(e).c_str());
                break;
            default:
                printf(
                    "invalid DataType %d",
                    expr->dataType);
        }
    }
}

/*
 * EXAMPLE
 *
 * In:bool(
 *     1,
 *     2,
 *     3,
 *     4
 * )
 *
 */
void ExprPrinter::Visit(InExpr &e)
{
    string indent = GenerateIndentation();
    InExpr *expr = &e;
    printf((indent + "In:%s(\n").c_str(),
           DataTypeString(e).c_str());
    this->indentationDepth++;
    for (int i = 0; i < expr->arguments.size(); i++) {
        (expr->arguments[i])->Accept(*this);
        if (i == expr->arguments.size() - 1) {
            printf("\n%s)", indent.c_str());
        } else {
            printf(",\n");
        }
    }
    this->indentationDepth--;
}

/*
 * EXAMPLE
 *
 * Between:bool(
 *     5,
 *     -1,
 *     7,
 * )
 *
 */
void ExprPrinter::Visit(BetweenExpr &e)
{
    string indent = GenerateIndentation();
    BetweenExpr *expr = &e;
    printf(
        (indent + "Between:%s(\n").c_str(),
        DataTypeString(e).c_str()
        );
    this->indentationDepth++;
    (expr->value)->Accept(*this);
    printf(",\n");

    (expr->lowerBound)->Accept(*this);
    printf(",\n");

    (expr->upperBound)->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

/*
 * EXAMPLE
 *
 * If:bool(
 *     Cmp:bool(GT,
 *         #0,
 *         100,
 *     ),
 *     Cmp:bool(GT,
 *         #0,
 *         200
 *     ),
 *     Cmp:bool(LT,
 *         #0,
 *         0
 *     )
 * )
 *
 */
void ExprPrinter::Visit(IfExpr &e)
{
    string indent = GenerateIndentation();
    IfExpr *expr = &e;
    printf(
        (indent + "If:%s(\n").c_str(),
        DataTypeString(e).c_str()
        );
    this->indentationDepth++;
    expr->condition->Accept(*this);
    printf(",\n");

    expr->trueExpr->Accept(*this);
    printf(",\n");

    expr->falseExpr->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

/*
 * EXAMPLE
 *
 * Cmp:bool(EQ,
 *     Coalesce:int64(
 *         #0,
 *         0
 *     ),
 *     123
 * )
 *
 */
void ExprPrinter::Visit(CoalesceExpr &e)
{
    string indent = GenerateIndentation();
    CoalesceExpr *expr = &e;
    printf(
        (indent + "Coalesce:%s(\n").c_str(),
        DataTypeString(e).c_str()
        );
    this->indentationDepth++;
    expr->value1->Accept(*this);
    printf(",\n");

    expr->value2->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

/*
 * EXAMPLE
 *
 * Printed Pared Expression
 *
 * IsNull:bool(
 *     #0
 * )
 *
 */
void ExprPrinter::Visit(IsNullExpr &e)
{
    string indent = GenerateIndentation();
    IsNullExpr *expr = &e;
    printf(
        (indent + "IsNull:%s(\n").c_str(),
        DataTypeString(e).c_str()
        );
    this->indentationDepth++;
    expr->value->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

/*
 * EXAMPLE
 * concat:string(
 *     #1,
 *     #2
 * )
 *
 */
void ExprPrinter::Visit(FuncExpr &e)
{
    string indent = GenerateIndentation();
    FuncExpr *expr = &e;
    printf(
        (indent + "%s:%s(\n").c_str(),
        expr->funcName.c_str(), DataTypeString(e).c_str()
        );
    this->indentationDepth++;
    for (int i = 0; i < expr->arguments.size(); i++) {
        (expr->arguments[i])->Accept(*this);
        if (i == expr->arguments.size() - 1) {
            printf("\n%s)", indent.c_str());
        } else {
            printf(",\n");
        }
    }
    this->indentationDepth--;
}
