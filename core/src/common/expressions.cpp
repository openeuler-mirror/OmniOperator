/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "expressions.h"
#include <string>
#include <algorithm>
#include <utility>
#include "../vector/vector_type.h"
#include "../codegen/func_registry.h"
#include "../util/type_util.h"

using namespace omniruntime::vec;

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

// Literal Expression methods
LiteralExpr::LiteralExpr() {}

LiteralExpr::~LiteralExpr()
{
    if (dataType->GetId() == VecTypeId::OMNI_VEC_TYPE_VARCHAR) {
        delete stringVal;
    }
}

ExprType LiteralExpr::GetType() const
{
    return ExprType::LITERAL_E;
}
// Helper constructors for different data types
LiteralExpr::LiteralExpr(bool val, VecTypePtr dt)
{
    dataType = std::move(dt);
    boolVal = val;
}
LiteralExpr::LiteralExpr(int32_t val, VecTypePtr dt)
{
    dataType = std::move(dt);
    intVal = val;
}
LiteralExpr::LiteralExpr(int64_t val, VecTypePtr dt)
{
    dataType = std::move(dt);
    longVal = val;
}
LiteralExpr::LiteralExpr(double val, VecTypePtr dt)
{
    dataType = std::move(dt);
    doubleVal = val;
}
LiteralExpr::LiteralExpr(std::string *val, VecTypePtr dt)
{
    dataType = std::move(dt);
    stringVal = val;
}
LiteralExpr::LiteralExpr(int64_t *val, VecTypePtr dt)
{
    dataType = std::move(dt);
    dec128Val = val;
}

// FieldExpr
FieldExpr::FieldExpr() {}

FieldExpr::~FieldExpr() {}

ExprType FieldExpr::GetType() const
{
    return ExprType::FIELD_E;
}

// Helper constructors
FieldExpr::FieldExpr(int32_t colIdx, VecTypePtr colType)
{
    dataType = std::move(colType);
    colVal = colIdx;
}

BinaryExpr::BinaryExpr()
{
    dataType = BooleanType();
}

BinaryExpr::BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr)
{
    op = bop;
    left = leftExpr;
    right = rightExpr;
    // use the more encompassing DataType
    if (leftExpr->GetReturnTypeId() <= rightExpr->GetReturnTypeId()) {
        dataType = std::make_unique<VecType>(rightExpr->GetReturnType());
    } else {
        dataType = std::make_unique<VecType>(leftExpr->GetReturnType());
    }
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
    dataType = BooleanType();
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
    dataType = BooleanType();
}

InExpr::~InExpr()
{
    for (Expr* exp : arguments) {
        delete exp;
    }
}

InExpr::InExpr(std::vector<Expr*> args)
{
    dataType = BooleanType();
    arguments = std::move(args);
}

ExprType InExpr::GetType() const
{
    return ExprType::IN_E;
}


BetweenExpr::BetweenExpr()
{
    dataType = BooleanType();
}

BetweenExpr::~BetweenExpr()
{
    delete value;
    delete lowerBound;
    delete upperBound;
}

BetweenExpr::BetweenExpr(Expr* val, Expr* lowBound, Expr* upBound)
{
    dataType = BooleanType();
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
    dataType = BooleanType();
    this->value = value;
}

ExprType IsNullExpr::GetType() const
{
    return ExprType::IS_NULL_E;
}

FuncExpr::FuncExpr() = default;

FuncExpr::~FuncExpr()
{
    for (Expr* exp : arguments) {
        delete exp;
    }
}

FuncExpr::FuncExpr(std::string fnName, std::vector<Expr*> args, VecTypePtr returnType)
{
    funcName = fnName;
    arguments = std::move(args);
    dataType = std::move(returnType);

    std::vector<VecTypeId> argTypes(arguments.size());
    std::transform(arguments.begin(), arguments.end(), argTypes.begin(),
        [](Expr *expr) -> VecTypeId {return expr->GetReturnTypeId();});
    auto signature = FunctionSignature(funcName, argTypes, dataType->GetId());
    this->function = omniruntime::FunctionRegistry::LookupFunction(&signature);
}

FuncExpr::FuncExpr(std::string fnName, std::vector<Expr*> args, VecTypePtr returnType, const Function *function)
{
    funcName = fnName;
    arguments = std::move(args);
    dataType = std::move(returnType);
    this->function = function;
}

ExprType FuncExpr::GetType() const
{
    return ExprType::FUNC_E;
}
}
}
