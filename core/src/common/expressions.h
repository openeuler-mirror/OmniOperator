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
#include "datatype.h"
#include "../vector/type/decimal128.h"

class ExprVisitor;

namespace omniruntime {
namespace expressions {

// place holder context class here
class Context {

};


enum Operator {
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


enum OperatorType {
    COMPARISON,
    LOGICAL,
    ARITHMETIC,
    INVALIDOPTTYPE
};



enum ExprType {
    DATA_E,
    BINARY_E,
    UNARY_E,
    IN_E,
    BETWEEN_E,
    IF_E,
    COALESCE_E,
    IS_NULL_E,
    FUNC_E,
    INVALID_E
};

const std::map<std::string, Operator> OPERATOR_FROM_STRING = {
    {"EQUAL", Operator::EQ},
    {"LESS_THAN", Operator::LT},
    {"LESS_THAN_OR_EQUAL", Operator::LTE},
    {"GREATER_THAN_OR_EQUAL", Operator::GTE},
    {"GREATER_THAN", Operator::GT},
    {"NOT_EQUAL", Operator::NEQ},
    {"AND", Operator::AND},
    {"OR", Operator::OR},
    {"NOT", Operator::NOT},
    {"not", Operator::NOT},
    {"ADD", Operator::ADD},
    {"SUBTRACT", Operator::SUB},
    {"MULTIPLY", Operator::MUL},
    {"DIVIDE", Operator::DIV},
    {"MODULUS", Operator::MOD},
};

bool IsNullLiteral(const std::string& value);
Operator StringToOperator(std::string opStr);


class Expr {
public:
        // TODO:: wrap width with the type
        int32_t width = INT32_MAX;
        DataType dataType; // dataType of returned value
        DataType GetExprDataType() const;
        virtual ExprType GetType() const;
        virtual ~Expr() = default;
        virtual void Accept(ExprVisitor &visitor) const = 0;
};


class DataExpr : public Expr {
public:
    bool isColumn = false;
    bool isNull = false;
    bool boolVal = false;
    int32_t intVal = 0;
    int64_t longVal = 0;
    double doubleVal = 0;
    std::string* stringVal = nullptr;
    int32_t colVal = 0;
    int64_t* dec128Val = nullptr;

    DataExpr();
    ~DataExpr() override;
    explicit DataExpr(bool val);
    explicit DataExpr(int32_t val);
    explicit DataExpr(int64_t val);
    explicit DataExpr(double val);
    explicit DataExpr(std::string* val);
    explicit DataExpr(int64_t* val);
    DataExpr(int32_t colIdx, DataType colType);
    DataExpr(int32_t colIdx, DataType colType, int32_t width);
    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class UnaryExpr : public Expr {
public:
    Operator op = EQ;
    Expr *exp = nullptr;

    UnaryExpr();
    ~UnaryExpr() override;
    UnaryExpr(Operator logOp, Expr *bodyexp);
    UnaryExpr(Operator uop, Expr *expr, DataType dt);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};


class BinaryExpr : public Expr {
public:
    Operator op = EQ;
    Expr *left = nullptr;
    Expr *right = nullptr;

    BinaryExpr();
    ~BinaryExpr() override;
    BinaryExpr(Operator op, Expr *leftExpr, Expr *rightExpr);
    BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr, DataType dt);
   
    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};


class InExpr : public Expr {
public:
    // first element of arguments is the value to be compared to every other argument
    std::vector<Expr*> arguments;

    InExpr();
    ~InExpr() override;
    explicit InExpr(std::vector<Expr*> args);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};


class BetweenExpr : public Expr {
public:
    Expr* value = nullptr;
    Expr* lowerBound = nullptr;
    Expr* upperBound = nullptr;

    BetweenExpr();
    ~BetweenExpr() override;
    BetweenExpr(Expr* val, Expr* lowBound, Expr* upBound);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};


class IfExpr : public Expr {
public:
    Expr* condition = nullptr;
    Expr* trueExpr = nullptr;
    Expr* falseExpr = nullptr;

    IfExpr();
    ~IfExpr() override;
    IfExpr(Expr* cond, Expr* texp, Expr* fexp);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};


class CoalesceExpr : public Expr {
public:
    Expr* value1 = nullptr;
    Expr* value2 = nullptr;

    CoalesceExpr();
    ~CoalesceExpr() override;
    CoalesceExpr(Expr* val1, Expr* val2);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class IsNullExpr : public Expr {
public:
    Expr* value = nullptr;
    IsNullExpr();
    ~IsNullExpr() override;
    explicit IsNullExpr(Expr* value);

    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};

class FuncExpr : public Expr {
public:
    std::string funcName;
    std::vector<Expr*> arguments;
    omniruntime::Function *function;

    FuncExpr();
    ~FuncExpr() override;
    FuncExpr(std::string fnName, std::vector<Expr*> args);
    FuncExpr(std::string fnName, std::vector<Expr*> args, DataType dt);
    FuncExpr(std::string fnName, std::vector<Expr*> args, DataType dt, int32_t width);
    FuncExpr(std::string fnName, std::vector<Expr*> args, DataType dt, omniruntime::Function &function);
    FuncExpr(std::string fnName, std::vector<Expr*> args, omniruntime::Function &function);
    FuncExpr(std::string fnName, std::vector<Expr*> args, DataType dt, int32_t width, omniruntime::Function &function);
    void Accept(ExprVisitor &visitor) const override;
    ExprType GetType() const override;
};
}
}
#endif
