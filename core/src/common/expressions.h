#ifndef __EXPRESSIONS_H__
#define __EXPRESSIONS_H__

#include <vector>
#include <stdint.h>

// place holder context class here
class Context
{
    
};

enum LogicalOperator
{
    AND,
    OR, 
    NOT, 
    INVALIDBIN
};

enum ComparisionOperator
{
    EQ,
    NEQ,
    LT,
    LTE,
    GT,
    GTE, 
    BETWEEN, 
    IN,
    INVALIDCMP
};

enum ArithmeticOperator
{
    ADD,
    SUB,
    MUL,
    DIV,
    MOD, 
    INVALIDARITH
};


class Expr
{
    public:
        virtual ~Expr() = default;
        virtual void printExprTree();
};


class BinaryExpr : public Expr
{
public:
    LogicalOperator op;
    Expr *left;
    Expr *right;
    BinaryExpr();
    ~BinaryExpr();
    BinaryExpr(LogicalOperator logOp, Expr *leftExpr, Expr *rightExpr);
    void printExprTree();
};


class ComparisionExpr : public Expr
{
public:
    ComparisionOperator op;
    int32_t columnIdx;
    int32_t columnData;
    ComparisionExpr();
    ComparisionExpr(ComparisionOperator cmpOp, int colId, int colData);
    void printExprTree();
};


class BetweenExpr : public Expr
{
public:
    int columnIdx;
    int lowerBound;
    int upperBound;
    BetweenExpr();
    BetweenExpr(int colId, int lb, int ub);
    void printExprTree();
};


class UnaryExpr : public Expr
{
public:
    LogicalOperator op;
    Expr *exp;
    UnaryExpr();
    ~UnaryExpr();
    UnaryExpr(LogicalOperator logOp, Expr *bodyexp);
    void printExprTree();
};


class ArithmeticExpr : public Expr
{
public:
    ArithmeticOperator op;
    Expr *left;
    Expr *right;
    ArithmeticExpr();
    ~ArithmeticExpr();
    ArithmeticExpr(ArithmeticOperator arithOp, Expr *leftExpr, Expr *rightExpr);
    void printExprTree();
};

class InExpr : public Expr
{
public:
    int columnIdx;
    std::vector<int> arr;
    InExpr();
    InExpr(int colId, std::vector<int> vals);
    void printExprTree();
};

#endif
