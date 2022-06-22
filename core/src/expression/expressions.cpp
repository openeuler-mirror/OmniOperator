/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#include "expressions.h"
#include <string>
#include <algorithm>
#include <utility>
#include "type/data_type.h"
#include "codegen/func_registry.h"
#include "util/type_util.h"

using namespace std;
using namespace omniruntime::type;

namespace omniruntime {
namespace expressions {
bool IsNullLiteral(const std::string &value)
{
    const std::string loweredNullValue = "null";
    if (value.size() != loweredNullValue.size()) {
        return false;
    }
    for (uint32_t i = 0; i < loweredNullValue.size(); i++) {
        if (tolower(value[i]) != loweredNullValue[i]) {
            return false;
        }
    }
    return true;
}

bool IsComparisonOperator(Operator op)
{
    return op == Operator::GT || op == Operator::GTE || op == Operator::LT || op == Operator::LTE ||
        op == Operator::EQ || op == Operator::NEQ;
}

bool IsLogicalOperator(Operator op)
{
    return op == Operator::AND || op == Operator::OR || op == Operator::NOT;
}

Operator StringToOperator(const std::string &opStr)
{
    auto opItr = OPERATOR_FROM_STRING.find(opStr);
    if (opItr != OPERATOR_FROM_STRING.end()) {
        return opItr->second;
    }
    return Operator::INVALIDOP;
}

ExprType Expr::GetType() const
{
    return ExprType::INVALID_E;
}

DataTypeRawPtr Expr::GetReturnType() const {
    return dataType;
}

DataTypeId Expr::GetReturnTypeId() const
{
    return dataType->GetId();
}

void Expr::DeleteExprs(const std::vector<Expr *> &exprs)
{
    for (Expr *exp : exprs) {
        delete exp;
    }
}

// Literal Expression methods
LiteralExpr::LiteralExpr() = default;

LiteralExpr::~LiteralExpr()
{
    delete stringVal;
}

ExprType LiteralExpr::GetType() const
{
    return ExprType::LITERAL_E;
}

// Helper constructors for different data types
LiteralExpr::LiteralExpr(bool val, DataTypeRawPtr dt)
{
    dataType = dt;
    boolVal = val;
}

LiteralExpr::LiteralExpr(int32_t val, DataTypeRawPtr dt)
{
    dataType = dt;
    intVal = val;
}

LiteralExpr::LiteralExpr(int64_t val, DataTypeRawPtr dt)
{
    dataType = dt;
    longVal = val;
}

LiteralExpr::LiteralExpr(double val, DataTypeRawPtr dt)
{
    dataType = dt;
    doubleVal = val;
}

LiteralExpr::LiteralExpr(std::string *val, DataTypeRawPtr dt)
{
    dataType = dt;
    stringVal = val;
}

// FieldExpr
FieldExpr::FieldExpr() = default;

FieldExpr::~FieldExpr() = default;

ExprType FieldExpr::GetType() const
{
    return ExprType::FIELD_E;
}

// Helper constructors
FieldExpr::FieldExpr(int32_t colIdx, DataTypeRawPtr colType)
{
    dataType = colType;
    colVal = colIdx;
}

BinaryExpr::BinaryExpr()
{
    dataType = new BooleanDataType();
}

BinaryExpr::BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr, DataTypeRawPtr dt)
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
    dataType = new BooleanDataType();
}

UnaryExpr::UnaryExpr(Operator logOp, Expr *bodyexp) : op(logOp), exp(bodyexp) {}

UnaryExpr::UnaryExpr(Operator uop, Expr *expr, DataTypeRawPtr dt) : op(uop), exp(expr)
{
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
    dataType = new BooleanDataType();
}

InExpr::~InExpr()
{
    DeleteExprs(arguments);
}

InExpr::InExpr(std::vector<Expr *> args)
{
    dataType = new BooleanDataType();
    arguments = std::move(args);
}

ExprType InExpr::GetType() const
{
    return ExprType::IN_E;
}

BetweenExpr::BetweenExpr()
{
    dataType = new BooleanDataType();
}

BetweenExpr::~BetweenExpr()
{
    delete value;
    delete lowerBound;
    delete upperBound;
}

BetweenExpr::BetweenExpr(Expr *val, Expr *lowBound, Expr *upBound)
{
    dataType = new BooleanDataType();
    value = val;
    lowerBound = lowBound;
    upperBound = upBound;
}

ExprType BetweenExpr::GetType() const
{
    return ExprType::BETWEEN_E;
}

SwitchExpr::SwitchExpr() : whenClause(), falseExpr() {}

SwitchExpr::~SwitchExpr()
{
    for (std::pair<Expr *, Expr *> &vec : whenClause) {
        delete vec.first;
        delete vec.second;
    }
    delete falseExpr;
}

SwitchExpr::SwitchExpr(const std::vector<std::pair<Expr *, Expr *>> &whens, Expr *fexp)
{
    dataType = fexp->GetReturnType();
    whenClause = whens;
    falseExpr = fexp;
}

ExprType SwitchExpr::GetType() const
{
    return ExprType::SWITCH_E;
}

IfExpr::IfExpr() : condition(), trueExpr(), falseExpr() {}

IfExpr::~IfExpr()
{
    delete condition;
    delete trueExpr;
    delete falseExpr;
}

IfExpr::IfExpr(Expr *cond, Expr *texp, Expr *fexp)
{
    dataType = texp->GetReturnType();
    condition = cond;
    trueExpr = texp;
    falseExpr = fexp;
}

ExprType IfExpr::GetType() const
{
    return ExprType::IF_E;
}

CoalesceExpr::CoalesceExpr() : value1(), value2() {}

CoalesceExpr::~CoalesceExpr()
{
    delete value1;
    delete value2;
}

CoalesceExpr::CoalesceExpr(Expr *val1, Expr *val2)
{
    dataType = val1->GetReturnType();
    value1 = val1;
    value2 = val2;
}

ExprType CoalesceExpr::GetType() const
{
    return ExprType::COALESCE_E;
}

IsNullExpr::IsNullExpr() : value() {}

IsNullExpr::~IsNullExpr()
{
    delete value;
}

IsNullExpr::IsNullExpr(Expr *value)
{
    dataType = new BooleanDataType();

    this->value = value;
}

ExprType IsNullExpr::GetType() const
{
    return ExprType::IS_NULL_E;
}

FuncExpr::FuncExpr() : function(nullptr) {}

FuncExpr::~FuncExpr()
{
    DeleteExprs(arguments);
}

FuncExpr::FuncExpr(const std::string &fnName, const std::vector<Expr *> &args, DataTypeRawPtr returnType)
    : funcName(fnName), arguments(args)
{
    dataType = returnType;

    std::vector<DataTypeId> argTypes(arguments.size());
    std::transform(arguments.begin(), arguments.end(), argTypes.begin(),
        [](Expr *expr) -> DataTypeId { return expr->GetReturnTypeId(); });
    auto signature = FunctionSignature(funcName, argTypes, dataType->GetId());
    this->function = omniruntime::FunctionRegistry::LookupFunction(&signature);
}

FuncExpr::FuncExpr(const std::string &fnName, const std::vector<Expr *> &args, DataTypeRawPtr returnType,
    const Function *function)
    : funcName(std::move(fnName)), arguments(std::move(args)), function(function)
{
    dataType = returnType;
}

ExprType FuncExpr::GetType() const
{
    return ExprType::FUNC_E;
}
}
}
