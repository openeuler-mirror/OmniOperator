#ifndef __EXPRESSIONS_H__
#define __EXPRESSIONS_H__

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
    int columnIdx;
    int columnData;

};
#endif