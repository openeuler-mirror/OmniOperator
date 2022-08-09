/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#ifndef __EXPRESSIONS_H__
#define __EXPRESSIONS_H__

#include <vector>
#include <string>
#include <map>
#include <codegen/function.h>
#include "type/decimal128.h"
#include "type/data_type.h"

class ExprVisitor;

namespace omniruntime {
namespace expressions {
using namespace type;
// place holder context class here
class Context {};

enum class Operator {
    // Comparison
    EQ,
    NEQ,
    LT,
    LTE,
    GT,
    GTE,
    // Logical
    AND,
    OR,
    NOT,
    // Arithmetic
    ADD,
    SUB,
    MUL,
    DIV,
    MOD,
    INVALIDOP
};

enum class OperatorType {
    COMPARISON,
    LOGICAL,
    ARITHMETIC,
    INVALIDOPTTYPE
};

enum class ExprType {
    LITERAL_E,
    FIELD_E,
    BINARY_E,
    UNARY_E,
    IN_E,
    BETWEEN_E,
    IF_E,
    SWITCH_E,
    COALESCE_E,
    IS_NULL_E,
    FUNC_E,
    INVALID_E,
};

const std::map<std::string, Operator> OPERATOR_FROM_STRING = {
    { "EQUAL", Operator::EQ },
    { "LESS_THAN", Operator::LT },
    { "LESS_THAN_OR_EQUAL", Operator::LTE },
    { "GREATER_THAN_OR_EQUAL", Operator::GTE },
    { "GREATER_THAN", Operator::GT },
    { "NOT_EQUAL", Operator::NEQ },
    { "AND", Operator::AND },
    { "OR", Operator::OR },
    { "NOT", Operator::NOT },
    { "not", Operator::NOT },
    { "ADD", Operator::ADD },
    { "SUBTRACT", Operator::SUB },
    { "MULTIPLY", Operator::MUL },
    { "DIVIDE", Operator::DIV },
    { "MODULUS", Operator::MOD },
};

bool IsNullLiteral(const std::string &value);
bool IsComparisonOperator(Operator op);
bool IsLogicalOperator(Operator op);
Operator StringToOperator(const std::string &opStr);

class Expr {
public:
    DataTypePtr dataType; // dataType of returned value
    DataTypePtr GetReturnType() const;
    omniruntime::type::DataTypeId GetReturnTypeId() const;
    virtual ExprType GetType() const;
    virtual ~Expr() = default;
    virtual void Accept(ExprVisitor &visitor) const = 0;
    static void DeleteExprs(const std::vector<Expr *> &exprs);
};

class LiteralExpr : public Expr {
public:
    bool isNull = false;
    bool boolVal = false;
    int32_t intVal = 0;
    int64_t longVal = 0;
    double doubleVal = 0;
    std::string *stringVal = nullptr;

    LiteralExpr();
    ~LiteralExpr() override;
    explicit LiteralExpr(bool val, DataTypePtr colType);
    explicit LiteralExpr(int32_t val, DataTypePtr colType);
    explicit LiteralExpr(int64_t val, DataTypePtr colType);
    explicit LiteralExpr(double val, DataTypePtr colType);
    explicit LiteralExpr(std::string *val, DataTypePtr colType);
    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class FieldExpr : public Expr {
public:
    bool isNull = false;
    int32_t colVal = 0;

    FieldExpr();
    ~FieldExpr() override;
    FieldExpr(int32_t colIdx, DataTypePtr colType);
    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class UnaryExpr : public Expr {
public:
    Operator op = Operator::EQ;
    Expr *exp = nullptr;

    UnaryExpr();
    ~UnaryExpr() override;
    UnaryExpr(Operator logOp, Expr *bodyexp);
    UnaryExpr(Operator uop, Expr *expr, DataTypePtr dt);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class BinaryExpr : public Expr {
public:
    Operator op = Operator::EQ;
    Expr *left = nullptr;
    Expr *right = nullptr;

    BinaryExpr();
    ~BinaryExpr() override;
    BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr, DataTypePtr dt);
    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class InExpr : public Expr {
public:
    // first element of arguments is the value to be compared to every other argument
    std::vector<Expr *> arguments;

    InExpr();
    ~InExpr() override;
    explicit InExpr(std::vector<Expr *> args);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class BetweenExpr : public Expr {
public:
    Expr *value = nullptr;
    Expr *lowerBound = nullptr;
    Expr *upperBound = nullptr;

    BetweenExpr();
    ~BetweenExpr() override;
    BetweenExpr(Expr *val, Expr *lowBound, Expr *upBound);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class SwitchExpr : public Expr {
public:
    std::vector<std::pair<Expr *, Expr *>> whenClause;
    Expr *falseExpr = nullptr;
    SwitchExpr();
    ~SwitchExpr() override;
    SwitchExpr(const std::vector<std::pair<Expr *, Expr *>> &whens, Expr *fexp);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class IfExpr : public Expr {
public:
    Expr *condition = nullptr;
    Expr *trueExpr = nullptr;
    Expr *falseExpr = nullptr;

    IfExpr();
    ~IfExpr() override;
    IfExpr(Expr *cond, Expr *texp, Expr *fexp);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class CoalesceExpr : public Expr {
public:
    Expr *value1 = nullptr;
    Expr *value2 = nullptr;

    CoalesceExpr();
    ~CoalesceExpr() override;
    CoalesceExpr(Expr *val1, Expr *val2);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class IsNullExpr : public Expr {
public:
    Expr *value = nullptr;
    IsNullExpr();
    ~IsNullExpr() override;
    explicit IsNullExpr(Expr *value);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class FuncExpr : public Expr {
public:
    std::string funcName;
    std::vector<Expr *> arguments;
    const omniruntime::Function *function;

    FuncExpr();
    ~FuncExpr() override;
    FuncExpr(const std::string &fnName, const std::vector<Expr *> &args, DataTypePtr returnType);
    FuncExpr(const std::string &fnName, const std::vector<Expr *> &args, DataTypePtr returnType,
        const omniruntime::Function *function);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};
}
}
#endif
