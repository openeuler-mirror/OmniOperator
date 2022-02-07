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

VecType &Expr::GetReturnType() const
{
    return *dataType;
}

VecTypeId Expr::GetReturnTypeId() const
{
    return dataType->GetId();
}


DataExpr::DataExpr() {}

DataExpr::~DataExpr()
{
    if (dataType->GetId() == VecTypeId::OMNI_VEC_TYPE_VARCHAR && !isColumn) {
        delete stringVal;
    }
}

ExprType DataExpr::GetType() const
{
    return ExprType::DATA_E;
}

// Helper constructors for different data types
DataExpr::DataExpr(bool val, VecTypePtr dt)
{
    isColumn = false;
    boolVal = val;
    dataType = std::move(dt);
}
DataExpr::DataExpr(int32_t val, VecTypePtr dt)
{
    isColumn = false;
    intVal = val;
    dataType = std::move(dt);
}
DataExpr::DataExpr(int64_t val, VecTypePtr dt)
{
    isColumn = false;
    longVal = val;
    dataType = std::move(dt);
}
DataExpr::DataExpr(double val, VecTypePtr dt)
{
    isColumn = false;
    doubleVal = val;
    dataType = std::move(dt);
}
DataExpr::DataExpr(std::string* val, VecTypePtr dt)
{
    isColumn = false;
    stringVal = val;
    dataType = std::move(dt);
}
DataExpr::DataExpr(int64_t *val, VecTypePtr dt)
{
    isColumn = false;
    dec128Val = val;
    dataType = std::move(dt);
}
DataExpr::DataExpr(int32_t colIdx, VecTypePtr colType, bool isCol)
{
    isColumn = isCol;
    dataType = std::move(colType);
    colVal = colIdx;
}

BinaryExpr::BinaryExpr()
{
    dataType = std::make_unique<VecType>(OMNI_VEC_TYPE_BOOLEAN);
}

BinaryExpr::BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr)
{
    op = bop;
    left = leftExpr;
    right = rightExpr;
    // use the more encompassing DataType
    dataType = std::make_unique<VecType>(leftExpr->GetReturnTypeId() <= rightExpr->GetReturnTypeId()
            ? rightExpr->GetReturnType() : leftExpr->GetReturnType());
}

BinaryExpr::BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr, VecTypePtr dt)
{
    op = bop;
    left = leftExpr;
    right = rightExpr;
    dataType = std::move(dt);
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
    dataType = std::make_unique<VecType>(VecTypeId::OMNI_VEC_TYPE_BOOLEAN);
}

UnaryExpr::UnaryExpr(Operator uop, Expr *expr)
{
    op = uop;
    exp = expr;
}

UnaryExpr::UnaryExpr(Operator uop, Expr *expr, VecTypePtr dt)
{
    op = uop;
    exp = expr;
    dataType = std::move(dt);
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
    dataType = std::make_unique<VecType>(VecTypeId::OMNI_VEC_TYPE_BOOLEAN);
}

InExpr::~InExpr()
{
    for (Expr* exp : arguments) {
        delete exp;
    }
}

InExpr::InExpr(std::vector<Expr*> args)
{
    dataType = std::make_unique<VecType>(VecTypeId::OMNI_VEC_TYPE_BOOLEAN);
    arguments = args;
}

ExprType InExpr::GetType() const
{
    return ExprType::IN_E;
}


BetweenExpr::BetweenExpr()
{
    dataType = std::make_unique<VecType>(VecTypeId::OMNI_VEC_TYPE_BOOLEAN);
}

BetweenExpr::~BetweenExpr()
{
    delete value;
    delete lowerBound;
    delete upperBound;
}

BetweenExpr::BetweenExpr(Expr* val, Expr* lowBound, Expr* upBound)
{
    dataType = std::make_unique<VecType>(VecTypeId::OMNI_VEC_TYPE_BOOLEAN);
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
    dataType = std::make_unique<VecType>(texp->GetReturnType());
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
    dataType = std::make_unique<VecType>(val1->GetReturnType());
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
    dataType = std::make_unique<VecType>(VecTypeId::OMNI_VEC_TYPE_BOOLEAN);
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

FuncExpr::FuncExpr(std::string fnName, std::vector<Expr*> args, VecTypePtr dt)
{
    funcName = fnName;
    arguments = args;
    dataType = std::move(dt);
}

FuncExpr::FuncExpr(std::string fnName, std::vector<Expr*> args, VecTypePtr dt, Function &function)
{
    funcName = fnName;
    arguments = args;
    dataType = std::move(dt);
    this->function = &function;
}

FuncExpr::FuncExpr(std::string fnName, std::vector<Expr*> args, Function &function)
{
    funcName = fnName;
    arguments = args;
    this->function = &function;
}

ExprType FuncExpr::GetType() const
{
    return ExprType::FUNC_E;
}
}
}
