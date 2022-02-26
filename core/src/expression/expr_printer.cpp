/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: print expression tree methods
 */
#include "expr_printer.h"
#include <cstdio>
#include <iostream>
#include <string>
#include <algorithm>
#include <map>
#include "util/debug.h"

using namespace omniruntime::expressions;
using namespace omniruntime::type;
using namespace std;

string ExprPrinter::BinaryExprPrinterHelper(const Operator &op) const
{
    switch (op) {
        case EQ:
            return "Cmp:%s(EQ, ";
        case NEQ:
            return "Cmp:%s(NEQ, ";
        case LT:
            return "Cmp:%s(LT, ";
        case LTE:
            return "Cmp:%s(LTE, ";
        case GT:
            return "Cmp:%s(GT, ";
        case GTE:
            return "Cmp:%s(GTE, ";
        case AND:
            return "Bin:%s(AND, ";
        case OR:
            return "Bin:%s(OR, ";
        case ADD:
            return "Arith:%s(ADD, ";
        case SUB:
            return "Arith:%s(SUB, ";
        case MUL:
            return "Arith:%s(MUL, ";
        case DIV:
            return "Arith:%s(DIV, ";
        case MOD:
            return "Arith:%s(MOD, ";
        default:
            return "Invalid";
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
 * Cmp:bool(EQ,
 * #7,
 * 'Winter
 * ),
 * Cmp:bool(EQ,
 * #2,
 * 'Summer'
 * )
 * )
 *
 */
void ExprPrinter::Visit(const BinaryExpr &e)
{
    string indent = GenerateIndentation();
    string message = BinaryExprPrinterHelper(e.op);
    if (message == "Invalid") {
        printf("invalid BinaryOperator %d(", e.op);
    } else {
        printf((indent + (message.append("\n"))).c_str(), TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
    }
    this->indentationDepth++;
    (e.left)->Accept(*this);
    printf(",\n");

    (e.right)->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

/*
 * EXAMPLE
 *
 * Unary:bool(NOT,
 * IsNull:bool(
 * #0
 * )
 * )
 *
 */
void ExprPrinter::Visit(const UnaryExpr &e)
{
    string indent = GenerateIndentation();
    switch (e.op) {
        case NOT:
            printf((indent + "Unary:%s(NOT,\n").c_str(), TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
            break;
        default:
            printf("invalid UnaryOperator %d(", e.op);
            break;
    }
    this->indentationDepth++;
    (e.exp)->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

void PrintBoolVal(const LiteralExpr &e, bool printWithTypes, string &indent)
{
    if (printWithTypes) {
        printf("bool_");
    }
    e.boolVal ? printf("%s", indent.append("true").c_str()) : printf("%s", indent.append("false").c_str());
}

void PrintIntVal(const LiteralExpr &e, bool printWithTypes, string &indent)
{
    if (printWithTypes) {
        printf("i32_");
    }
    printf(indent.append("%d:%s").c_str(), e.intVal, TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
}

void PrintLongVal(const LiteralExpr &e, bool printWithTypes, string &indent)
{
    if (printWithTypes) {
        printf("i64_");
    }
    printf(indent.append("%ld:%s").c_str(), e.longVal, TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
}

void PrintDoubleVal(const LiteralExpr &e, bool printWithTypes, string &indent)
{
    if (printWithTypes) {
        printf("d64_");
    }
    printf(indent.append("%f:%s").c_str(), e.doubleVal, TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
}

void PrintCharVal(const LiteralExpr &e, bool printWithTypes, string &indent)
{
    if (printWithTypes) {
        printf("s_");
    }
    if (e.GetReturnTypeId() == OMNI_CHAR) {
        printf(indent.append("'%s[%d]':%s").c_str(), (e.stringVal)->c_str(), e.dataType->GetWidth(),
            TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
    } else {
        printf(indent.append("'%s':%s").c_str(), (e.stringVal)->c_str(),
            TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
    }
}

void PrintDecimal64Val(const LiteralExpr &e, bool printWithTypes, string &indent)
{
    if (printWithTypes) {
        printf("d64_");
    }
    printf(indent.append("%ld").c_str(), e.longVal);
}

void PrintDecimal128Val(const LiteralExpr &e, bool printWithTypes, string &indent)
{
    if (printWithTypes) {
        printf("d128_");
    }
    // FIXME: printing as int64_t for now;
    printf(indent.append("%ld").c_str(), e.longVal);
}

void ExprPrinter::Visit(const LiteralExpr &e)
{
    string indent = GenerateIndentation();
    const bool printWithTypes = false; // for debugging types
    switch (e.GetReturnTypeId()) {
        case OMNI_BOOLEAN:
            PrintBoolVal(e, printWithTypes, indent);
            break;
        case OMNI_INT:
        case OMNI_DATE32:
            PrintIntVal(e, printWithTypes, indent);
            break;
        case OMNI_LONG:
            PrintLongVal(e, printWithTypes, indent);
            break;
        case OMNI_DOUBLE:
            PrintDoubleVal(e, printWithTypes, indent);
            break;
        case OMNI_CHAR:
            PrintCharVal(e, printWithTypes, indent);
            break;
        case OMNI_VARCHAR:
            PrintCharVal(e, printWithTypes, indent);
            break;
        case OMNI_DECIMAL64:
            PrintDecimal64Val(e, printWithTypes, indent);
            break;
        case OMNI_DECIMAL128:
            PrintDecimal128Val(e, printWithTypes, indent);
            break;
        default:
            printf("invalid DataType %d", e.GetReturnTypeId());
    }
}

void ExprPrinter::Visit(const FieldExpr &e)
{
    string indent = GenerateIndentation();
    if (e.GetReturnTypeId() == OMNI_CHAR) {
        printf(indent.append("#%d[%d]").c_str(), e.colVal, e.GetReturnType().GetWidth());
    } else {
        printf(indent.append("#%d").c_str(), e.colVal);
    }
}

/*
 * EXAMPLE
 *
 * In:bool(
 * 1,
 * 2,
 * 3,
 * 4
 * )
 *
 */
void ExprPrinter::Visit(const InExpr &e)
{
    string indent = GenerateIndentation();
    printf((indent + "In:%s(\n").c_str(), TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
    this->indentationDepth++;
    for (int i = 0; i < e.arguments.size(); i++) {
        (e.arguments[i])->Accept(*this);
        if (i == e.arguments.size() - 1) {
            printf("\n%s)", indent.c_str());
        } else {
            printf(",\n");
        }
    }
    this->indentationDepth--;
}
/*
 * Example:switch:bool(
 * Cmp:bool(EQ
 * #0,
 * 100,
 * ),
 * Cmp:bool(EQ
 * #0,
 * 200,
 * ),
 * Cmp:bool(),
 *
 *
 * )
 */
void ExprPrinter::Visit(const SwitchExpr &e)
{
    string indent = GenerateIndentation();
    printf((indent + "Switch:%s(\n").c_str(), TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
    this->indentationDepth++;
    for (int i = 0; i < e.whenClause.size(); i++) {
        (e.whenClause[i].first)->Accept(*this);
        printf(",\n");
        (e.whenClause[i].second)->Accept(*this);
        printf(",\n");
    }

    e.falseExpr->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}
/*
 * EXAMPLE
 *
 * Between:bool(
 * 5,
 * -1,
 * 7,
 * )
 *
 */
void ExprPrinter::Visit(const BetweenExpr &e)
{
    string indent = GenerateIndentation();
    printf((indent + "Between:%s(\n").c_str(), TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
    this->indentationDepth++;
    (e.value)->Accept(*this);
    printf(",\n");

    (e.lowerBound)->Accept(*this);
    printf(",\n");

    (e.upperBound)->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

/*
 * EXAMPLE
 *
 * If:bool(
 * Cmp:bool(GT,
 * #0,
 * 100,
 * ),
 * Cmp:bool(GT,
 * #0,
 * 200
 * ),
 * Cmp:bool(LT,
 * #0,
 * 0
 * )
 * )
 *
 */
void ExprPrinter::Visit(const IfExpr &e)
{
    string indent = GenerateIndentation();
    printf((indent + "If:%s(\n").c_str(), TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
    this->indentationDepth++;
    e.condition->Accept(*this);
    printf(",\n");

    e.trueExpr->Accept(*this);
    printf(",\n");

    e.falseExpr->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

/*
 * EXAMPLE
 *
 * Cmp:bool(EQ,
 * Coalesce:int64(
 * #0,
 * 0
 * ),
 * 123
 * )
 *
 */
void ExprPrinter::Visit(const CoalesceExpr &e)
{
    string indent = GenerateIndentation();
    printf((indent + "Coalesce:%s(\n").c_str(), TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
    this->indentationDepth++;
    e.value1->Accept(*this);
    printf(",\n");

    e.value2->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

/*
 * EXAMPLE
 *
 * Printed Pared Expression
 *
 * IsNull:bool(
 * #0
 * )
 *
 */
void ExprPrinter::Visit(const IsNullExpr &e)
{
    string indent = GenerateIndentation();
    printf((indent + "IsNull:%s(\n").c_str(), TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
    this->indentationDepth++;
    e.value->Accept(*this);
    printf("\n%s)", indent.c_str());
    this->indentationDepth--;
}

/*
 * EXAMPLE
 * concat:string(
 * #1,
 * #2
 * )
 *
 */
void ExprPrinter::Visit(const FuncExpr &e)
{
    string indent = GenerateIndentation();
    printf((indent + "%s:%s(\n").c_str(), e.funcName.c_str(), TypeUtil::TypeToString(e.GetReturnTypeId()).c_str());
    this->indentationDepth++;
    for (int i = 0; i < e.arguments.size(); i++) {
        (e.arguments[i])->Accept(*this);
        if (i == e.arguments.size() - 1) {
            printf("\n%s)", indent.c_str());
        } else {
            printf(",\n");
        }
    }
    this->indentationDepth--;
}
