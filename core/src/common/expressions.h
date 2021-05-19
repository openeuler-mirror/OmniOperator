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
    OR
};

enum ComparisionOperator
{
    EQ,
    NEQ,
    LT,
    LTE,
    GT,
    GTE
};

class Expr
{
    public:
        virtual ~Expr() = default;
};

class BinaryExpr : public Expr
{
public:
    LogicalOperator op;
    Expr left;
    Expr right;
    BinaryExpr();
    BinaryExpr(LogicalOperator logOp, Expr leftExpr, Expr rightExpr);
};

class ComparisionExpr : public Expr
{
public:
    ComparisionOperator op;
    int32_t columnIdx;
    int32_t columnData;

};
#endif