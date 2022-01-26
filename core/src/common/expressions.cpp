/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "expressions.h"
#include <string>
#include <algorithm>
#include "../vector/vector_type.h"

namespace omniruntime {
namespace expressions {
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

FuncExpr::FuncExpr(std::string fnName, std::vector<Expr*> args, DataType dt, Function &function)
{
    funcName = fnName;
    arguments = args;
    dataType = dt;
    this->function = &function;
}

FuncExpr::FuncExpr(std::string fnName, std::vector<Expr*> args, Function &function)
{
    funcName = fnName;
    arguments = args;
    this->function = &function;
}

FuncExpr::FuncExpr(std::string fnName, std::vector<Expr*> args, DataType dt, int32_t width, Function &function)
{
    funcName = fnName;
    arguments = args;
    dataType = dt;
    this->width = width;
    this->function = &function;
}

ExprType FuncExpr::GetType() const
{
    return ExprType::FUNC_E;
}
}
}
