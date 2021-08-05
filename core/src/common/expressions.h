/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#ifndef __EXPRESSIONS_H__
#define __EXPRESSIONS_H__

#include <vector>
#include <stdint.h>
#include <string>
#include <map>

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



enum DataType {
    BOOLD = 4,
    INT32D = 1,
    INT64D = 2,
    DOUBLED = 3,
    STRINGD = 15,
    INVALIDDATAD
};


enum ExprType {
    DATA_E,
    BINARY_E,
    UNARY_E,
    IN_E,
    BETWEEN_E,
    IF_E,
    COALESCE_E,
    FUNC_E,
    INVALID_E
};


class Expr {
public:
    DataType dataType; // dataType of returned value
    DataType GetExprDataType();

    virtual ExprType GetType();
    virtual ~Expr() = default;
    virtual void PrintExprTree();
};


class DataExpr : public Expr {
public:
    bool isColumn = false;
    bool boolVal = false;
    int32_t intVal = 0;
    int64_t longVal = 0;
    double doubleVal = 0;
    std::string* stringVal;
    int32_t colVal = 0;

    DataExpr();
    ~DataExpr();
    explicit DataExpr(bool val);
    explicit DataExpr(int32_t val);
    explicit DataExpr(int64_t val);
    explicit DataExpr(double val);
    explicit DataExpr(std::string* val);
    DataExpr(int32_t val, DataType colType);

    void PrintExprTree() override;
    ExprType GetType() override;
};


// Helper function to translate from jni type number to DataType
DataType ColTypeTrans(int32_t colType);

// Helper function for printing out datatypes
std::string dataTypeString(DataType dt);


class UnaryExpr : public Expr {
public:
    Operator op = EQ;
    Expr *exp = nullptr;

    UnaryExpr();
    ~UnaryExpr();
    UnaryExpr(Operator logOp, Expr *bodyexp);
    UnaryExpr(Operator uop, Expr *expr, DataType dt);

    void PrintExprTree() override;
    ExprType GetType() override;
};


class BinaryExpr : public Expr {
public:
    Operator op = EQ;
    Expr *left = nullptr;
    Expr *right = nullptr;

    BinaryExpr();
    ~BinaryExpr();
    BinaryExpr(Operator op, Expr *leftExpr, Expr *rightExpr);
    BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr, DataType dt);

    void PrintExprTree() override;
    ExprType GetType() override;
};


class InExpr : public Expr {
public:
    // first element of arguments is the value to be compared to every other argument
    std::vector<Expr*> arguments;

    InExpr();
    ~InExpr();
    InExpr(std::vector<Expr*> args);

    void PrintExprTree() override;
    ExprType GetType() override;
};


class BetweenExpr : public Expr {
public:
    Expr* value = nullptr;
    Expr* lowerBound = nullptr;
    Expr* upperBound = nullptr;

    BetweenExpr();
    ~BetweenExpr();
    BetweenExpr(Expr* val, Expr* lowBound, Expr* upBound);

    void PrintExprTree() override;
    ExprType GetType() override;
};


class IfExpr : public Expr {
public:
    Expr* condition = nullptr;
    Expr* trueExpr = nullptr;
    Expr* falseExpr = nullptr;

    IfExpr();
    ~IfExpr();
    IfExpr(Expr* cond, Expr* texp, Expr* fexp);

    void PrintExprTree() override;
    ExprType GetType() override;
};


class CoalesceExpr : public Expr {
public:
    Expr* value1 = nullptr;
    Expr* value2 = nullptr;

    CoalesceExpr();
    ~CoalesceExpr();
    CoalesceExpr(Expr* val1, Expr* val2);

    void PrintExprTree() override;
    ExprType GetType() override;
};


class FuncExpr : public Expr {
public:
    std::string funcName;
    std::vector<Expr*> arguments;

    FuncExpr();
    ~FuncExpr();
    FuncExpr(std::string fnName, std::vector<Expr*> args);
    FuncExpr(std::string fnName, std::vector<Expr*> args, DataType dt);

    void PrintExprTree() override;
    ExprType GetType() override;
};

}
}
#endif
