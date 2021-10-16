/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
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
const int TYPE_DECIMAL64D = 6;
const int TYPE_DECIMAL128D = 7;
const int TYPE_DATE32D = 8;
const int TYPE_DATE64D = 9;
const int TYPE_TIME32D = 10;
const int TYPE_TIME64D = 11;
const int TYPE_TIMESTAMPD = 12;
const int TYPE_STRINGD = 15;

// Helper function to get DataType from jint type
// Find types in core/src/types/vector_type.h
DataType ColTypeTrans(int32_t colType)
{
    switch (colType) {
        case TYPE_INT32D:
        case TYPE_DATE32D:
            return DataType::INT32D;
        case TYPE_INT64D:
        case TYPE_DECIMAL64D:
            return DataType::INT64D;
        case TYPE_DECIMAL128D:
            return DataType::DECIMAL128D;
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

std::string DataTypeString(DataType dt)
{
    switch (dt) {
        case DataType::BOOLD: return "bool";
        case DataType::DOUBLED: return "double";
        case DataType::INT32D: return "int32";
        case DataType::INT64D: return "int64";
        case DataType::STRINGD: return "string";
        case DataType::DECIMAL64D: return "decimal64";
        case DataType::DECIMAL128D: return "decimal128";
        case DataType::INT32PTRD: return "int32ptr";
        case DataType::INT8PTRD: return "int8ptr";
        case DataType::VOIDD: return "void";
        case DataType::INVALIDDATAD: return "invalid";
    }
}

// Helper function to get DataType from a string representing the type
DataType StringToDataType(std::string dt)
{
    // Strip spaces
    dt.erase(remove(dt.begin(), dt.end(), ' '), dt.end());

    if (dt == "INT32") {
        return DataType::INT32D;
    }
    if (dt == "INT64") {
        return DataType::INT64D;
    }
    if (dt == "DOUBLE") {
        return DataType::DOUBLED;
    }
    if (dt == "BOOL") {
        return DataType::BOOLD;
    }
    if (dt == "STRING") {
        return DataType::STRINGD;
    }
    if (dt == "DECIMAL128") {
        return DataType::DECIMAL128D;
    }
    return DataType::INVALIDDATAD;
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
    if (dataType == STRINGD && !isColumn) {
        delete stringVal;
    }
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
DataExpr::DataExpr(int64_t &val)
{
    isColumn = false;
    dataType = DECIMAL128D;
    dec128Val = &val;
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


IfExpr::IfExpr() : condition(), trueExpr(), falseExpr() {
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


CoalesceExpr::CoalesceExpr() : value1(), value2() {
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

IsNullExpr::IsNullExpr() : value() {
}

IsNullExpr::~IsNullExpr()
{
    delete value;
}

IsNullExpr::IsNullExpr(Expr* value)
{
    dataType = BOOLD;
    this->value = value;
}

ExprType IsNullExpr::GetType()
{
    return ExprType::IS_NULL_E;
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
}
}
