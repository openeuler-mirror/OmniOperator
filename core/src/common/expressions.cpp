/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "expressions.h"
#include <iostream>
#include <string>
#include <algorithm>
#include "../vector/vector_type.h"

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
const int TYPE_CHAR = 16;

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
            return DataType::VARCHARD;
        case TYPE_CHAR:
            return DataType::CHARD;
        default:
            return DataType::INVALIDDATAD;
    }
}

std::string DataTypeString(const Expr &expr)
{
    DataType dt = expr.dataType;
    int32_t width = expr.width;
    switch (dt) {
        case DataType::BOOLD: return "bool";
        case DataType::DOUBLED: return "double";
        case DataType::INT32D: return "int32";
        case DataType::INT64D: return "int64";
        case DataType::VARCHARD: return "string";
        case DataType::CHARD: return "char(" + std::to_string(width) + ")";
        case DataType::DECIMAL64D: return "decimal64";
        case DataType::DECIMAL128D: return "decimal128";
        case DataType::INT32PTRD: return "int32ptr";
        case DataType::INT8PTRD: return "int8ptr";
        case DataType::VOIDD: return "void";
        case DataType::INVALIDDATAD: return "invalid";
        default: return "";
    }
}

bool IsStringDataType(DataType type)
{
    return type == DataType::CHARD || type == DataType::VARCHARD;
}

bool IsNullLiteral(const std::string& value)
{
    const std::string loweredNullValue = "null";
    if (value.size() != loweredNullValue.size()) {
        return false;
    }
    for (int i = 0; i < loweredNullValue.size(); i++) {
        if (tolower(value[i]) != loweredNullValue[i]) {
            return false;
        }
    }
    return true;
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
        return DataType::VARCHARD;
    }
    if (dt == "DECIMAL128") {
        return DataType::DECIMAL128D;
    }
    return DataType::INVALIDDATAD;
}

// Helper function to get DataType from the enum ordinal value of the type
DataType OrdinalToDataType(const int32_t& dt)
{
    if (dt < INT32_MAX) {
        if (omniruntime::vec::OMNI_VEC_TYPE_DATE32 == dt) {
            return DataType::INT32D;
        }
        if (DECIMAL64D == dt) {
            return INT64D;
        }
        if (omniruntime::vec::OMNI_VEC_TYPE_SHORT == dt ||
            (omniruntime::vec::OMNI_VEC_TYPE_DATE64 <= dt &&
            omniruntime::vec::OMNI_VEC_TYPE_INTERVAL_DAY_TIME >= dt)) {
            LogWarn("Unsupported return type: %u", static_cast<omniruntime::vec::VecTypeId>(dt));
        }

        return static_cast<DataType>(dt);
    }
    std::cout << "Unsupported return type: " << dt << std::endl;
    return INVALIDDATAD;
}

// Helper function to get Operator enum from string representing operator
Operator StringToOperator(std::string opStr)
{
    auto opItr = OPERATOR_FROM_STRING.find(opStr);
    if (opItr != OPERATOR_FROM_STRING.end()) {
        return opItr->second;
    }
    return INVALIDOP;
}

ExprType Expr::GetType() const
{
    return INVALID_E;
}

DataType Expr::GetExprDataType() const
{
    return dataType;
}


DataExpr::DataExpr() {}

DataExpr::~DataExpr()
{
    if (dataType == VARCHARD && !isColumn) {
        delete stringVal;
    }
}

ExprType DataExpr::GetType() const
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
    dataType = VARCHARD;
    stringVal = val;
    width = val->length() + 1;
}
DataExpr::DataExpr(int64_t *val)
{
    isColumn = false;
    dataType = DECIMAL128D;
    dec128Val = val;
}
DataExpr::DataExpr(int32_t colIdx, DataType dt)
{
    isColumn = true;
    dataType = dt;
    colVal = colIdx;
}

DataExpr::DataExpr(int32_t colIdx, DataType dt, int32_t width)
{
    isColumn = true;
    dataType = dt;
    colVal = colIdx;
    this->width = width;
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

ExprType BinaryExpr::GetType() const
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

ExprType UnaryExpr::GetType() const
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

ExprType InExpr::GetType() const
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

ExprType BetweenExpr::GetType() const
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

ExprType IfExpr::GetType() const
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

ExprType CoalesceExpr::GetType() const
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

ExprType IsNullExpr::GetType() const
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

FuncExpr::FuncExpr(std::string fnName, std::vector<Expr*> args, DataType dt, int32_t width)
{
    funcName = fnName;
    arguments = args;
    dataType = dt;
    this->width = width;
}

ExprType FuncExpr::GetType() const
{
    return ExprType::FUNC_E;
}
}
}
