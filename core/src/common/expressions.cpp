/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2027. All rights reserved.
 * Description:
 */
#include "expressions.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include <algorithm>

namespace omniruntime {
namespace expressions {


const int TYPE_INT32D = 1;
const int TYPE_INT64D = 2;
const int TYPE_DOUBLED = 3;
const int TYPE_BOOLD = 4;
const int TYPE_INT32D_2ND = 5;
const int TYPE_STRINGD = 100;
// Helper function to get DataType from jint type
// Find types in core/src/types/vector_type.h
DataType ColTypeTrans(int32_t colType)
{
    switch (colType) {
        case TYPE_INT32D:
            return DataType::INT32D;
        case TYPE_INT64D:
            return DataType::INT64D;
        case TYPE_DOUBLED:
            return DataType::DOUBLED;
        case TYPE_BOOLD :
            return DataType::BOOLD;
        // Should be short datatype (INT16D)
        case TYPE_INT32D_2ND:
            return DataType::INT32D;
        case TYPE_STRINGD:
            return DataType::STRINGD;
        default:
            return DataType::INVALIDDATAD;
    }
}

std::string dataTypeString(DataType dt)
{
    switch (dt) {
        case DataType::BOOLD: return "bool";
        case DataType::DOUBLED: return "double";
        case DataType::INT32D: return "int32";
        case DataType::INT64D: return "int64";
        case DataType::STRINGD: return "string";
        case DataType::INVALIDDATAD: return "invalid";
    }
}


ExprType Expr::GetType()
{
    return INVALID_E;
}

DataType Expr::GetExprDataType()
{
    return dataType;
}


DataExpr::DataExpr(){}

DataExpr::~DataExpr()
{
    if (dataType == STRINGD && !isColumn) delete stringVal;
}

ExprType DataExpr::GetType()
{
    return ExprType::DATA_E;
}

// Helper constructors for different data types
DataExpr::DataExpr(bool val)
{
    isColumn = false;
    dataType = BOOLD;
    boolVal = val;
}
DataExpr::DataExpr(int32_t val)
{
    isColumn = false;
    dataType = INT32D;
    intVal = val;
}
DataExpr::DataExpr(int64_t val)
{
    isColumn = false;
    dataType = INT64D;
    longVal = val;
}
DataExpr::DataExpr(double val)
{
    isColumn = false;
    dataType = DOUBLED;
    doubleVal = val;
}
DataExpr::DataExpr(std::string* val)
{
    isColumn = false;
    dataType = STRINGD;
    stringVal = val;
}
DataExpr::DataExpr(int32_t colIdx, DataType dt)
{
    isColumn = true;
    // use magic numbers from jni wrapper
    dataType = dt; // has already been translated
    colVal = colIdx;
}


BinaryExpr::BinaryExpr()
{
    dataType = BOOLD;
}

BinaryExpr::BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr)
{
    op = bop;
    left = leftExpr;
    right = rightExpr;
    // use the more encompassing DataType
    dataType = std::max(leftExpr->GetExprDataType(), rightExpr->GetExprDataType());
}

BinaryExpr::BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr, DataType dt)
{
    op = bop;
    left = leftExpr;
    right = rightExpr;
    dataType = dt;
}

BinaryExpr::~BinaryExpr()
{
    delete left;
    delete right;
}

ExprType BinaryExpr::GetType()
{
    return ExprType::BINARY_E;
}


UnaryExpr::UnaryExpr()
{
    dataType = BOOLD;
}

UnaryExpr::UnaryExpr(Operator uop, Expr *expr)
{
    op = uop;
    exp = expr;
}

UnaryExpr::UnaryExpr(Operator uop, Expr *expr, DataType dt)
{
    op = uop;
    exp = expr;
    dataType = dt;
}

UnaryExpr::~UnaryExpr()
{
    delete exp;
}

ExprType UnaryExpr::GetType()
{
    return ExprType::UNARY_E;
}


InExpr::InExpr()
{
    dataType = BOOLD;
}

InExpr::~InExpr()
{
    for (Expr* exp : arguments) {
        delete exp;
    }
}

InExpr::InExpr(std::vector<Expr*> args)
{
    dataType = BOOLD;
    arguments = args;
}

ExprType InExpr::GetType()
{
    return ExprType::IN_E;
}


BetweenExpr::BetweenExpr()
{
    dataType = BOOLD;
}

BetweenExpr::~BetweenExpr()
{
    delete value;
    delete lowerBound;
    delete upperBound;
}

BetweenExpr::BetweenExpr(Expr* val, Expr* lowBound, Expr* upBound)
{
    dataType = BOOLD;
    value = val;
    lowerBound = lowBound;
    upperBound = upBound;
}

ExprType BetweenExpr::GetType()
{
    return ExprType::BETWEEN_E;
}


IfExpr::IfExpr() {
}

IfExpr::~IfExpr()
{
    delete condition;
    delete trueExpr;
    delete falseExpr;
}

IfExpr::IfExpr(Expr* cond, Expr* texp, Expr* fexp)
{
    dataType = texp->dataType;
    condition = cond;
    trueExpr = texp;
    falseExpr = fexp;
}

ExprType IfExpr::GetType()
{
    return ExprType::IF_E;
}


CoalesceExpr::CoalesceExpr() {
}

CoalesceExpr::~CoalesceExpr()
{
    delete value1;
    delete value2;
}

CoalesceExpr::CoalesceExpr(Expr* val1, Expr* val2)
{
    dataType = val1->dataType;
    value1 = val1;
    value2 = val2;
}

ExprType CoalesceExpr::GetType()
{
    return ExprType::COALESCE_E;
}


FuncExpr::FuncExpr() {}

FuncExpr::~FuncExpr()
{
    for (Expr* exp : arguments) {
        delete exp;
    }
}

FuncExpr::FuncExpr(std::string fnName, std::vector<Expr*> args)
{
    funcName = fnName;
    arguments = args;
}

FuncExpr::FuncExpr(std::string fnName, std::vector<Expr*> args, DataType dt)
{
    funcName = fnName;
    arguments = args;
    dataType = dt;
}

ExprType FuncExpr::GetType()
{
    return ExprType::FUNC_E;
}



// debugging Expr tree
void Expr::PrintExprTree() {
}

void BinaryExpr::PrintExprTree()
{
    switch (op) {
        // Comparison
        case EQ:
            printf("Cmp(EQ, ");
            break;
        case NEQ:
            printf("Cmp(NEQ, ");
            break;
        case LT:
            printf("Cmp(LT, ");
            break;
        case LTE:
            printf("Cmp(LTE, ");
            break;
        case GT:
            printf("Cmp(GT, ");
            break;
        case GTE:
            printf("Cmp(GTE, ");
            break;

        // Logical
        case AND:
            printf("Bin(AND, ");
            break;
        case OR:
            printf("Bin(OR, ");
            break;

        // Arithmetic
        case ADD:
            printf("Arith(ADD, ");
            break;
        case SUB:
            printf("Arith(SUB, ");
            break;
        case MUL:
            printf("Arith(MUL, ");
            break;
        case DIV:
            printf("Arith(DIV, ");
            break;
        case MOD:
            printf("Arith(MOD, ");
            break;
        default:
            printf("invalid BinaryOperator %d(", op);
            break;
    }
    left->PrintExprTree();
    printf(", ");
    right->PrintExprTree();
    printf(")");
}

void UnaryExpr::PrintExprTree()
{
    switch (op) {
        case NOT:
            printf("Unary(NOT, ");
            break;
        default:
            printf("invalid UnaryOperator %d(", op);
            break;
    }
    exp->PrintExprTree();
    printf(")");
}

void DataExpr::PrintExprTree()
{
    const bool printWithTypes = false; // for debugging types
#ifdef DEBUG
    std::cout << "Data" + dataTypeString(dataType) + "(";
#endif
    if (isColumn) {
        printf("#%d", colVal);
    } else {
        switch (dataType) {
            case BOOLD: {
                if (printWithTypes) printf("bool_");
                if (boolVal) printf("true");
                else printf("false");
                break;
            }
            case INT32D:
                if (printWithTypes) printf("i32_");
                printf("%d", intVal);
                break;
            case INT64D:
                if (printWithTypes) printf("i64_");
                printf("%ld", longVal);
                break;
            case DOUBLED:
                if (printWithTypes) printf("d64_");
                printf("%f", doubleVal);
                break;
            case STRINGD:
                if (printWithTypes) printf("s_");
                printf("'%s'", stringVal->c_str());
                break;
            default:
                printf("invalid DataType %d", dataType);
        }
    }
#ifdef DEBUG
    printf(")");
#endif
}

void InExpr::PrintExprTree()
{
    printf("In(");
    for (int i = 0; i < arguments.size(); i++) {
        arguments[i]->PrintExprTree();
        if (i == arguments.size() - 1) printf(")");
        else printf(", ");
    }
}

void BetweenExpr::PrintExprTree()
{
    printf("Between(");
    value->PrintExprTree();
    printf(", ");
    lowerBound->PrintExprTree();
    printf(", ");
    upperBound->PrintExprTree();
    printf(")");
}

void IfExpr::PrintExprTree()
{
    printf("If(");
    condition->PrintExprTree();
    printf(", ");
    trueExpr->PrintExprTree();
    printf(", ");
    falseExpr->PrintExprTree();
    printf(")");
}

void CoalesceExpr::PrintExprTree()
{
    printf("Coalesce(");
    value1->PrintExprTree();
    printf(", ");
    value2->PrintExprTree();
    printf(")");
}

void FuncExpr::PrintExprTree()
{
    printf("%s(", funcName.c_str());

    for (int i = 0; i < arguments.size(); i++) {
        arguments[i]->PrintExprTree();
        if (i == arguments.size() - 1) printf(")");
        else printf(", ");
    }
}
}
}