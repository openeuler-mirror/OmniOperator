#ifndef __EXPRESSIONS_H__
#define __EXPRESSIONS_H__

#include <vector>
#include <stdint.h>
#include <string>

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
    INVALIDCMP
};

enum FnType
{
    IN, 
    BETWEEN, 
    COALESCE, 
    SUBSTR, 
    INVALIDFN
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

enum DataType
{
    INT32D, 
    INT64D, 
    DOUBLED, 
    STRINGD, 
    COLUMND, 
    INVALIDDATAD
};

enum ExprType
{
    COMPARISION_E, 
    BINARY_E, 
    UNARY_E, 
    IN_E, 
    BETWEEN_E, 
    ARITHMETIC_E, 
    COALESCE_E, 
    SUBSTR_E, 
    INVALID_E
};


class Expr
{
    public:
        virtual ExprType getType();
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
    ExprType getType();
};


class Data
{
public:
    DataType dataType;
    int32_t intVal;
    int64_t longVal;
    double doubleVal;
    std::string stringVal;
    int32_t colVal;

    void printData(bool withTypes);
};


class ComparisionExpr : public Expr
{
public:
    ComparisionOperator op;
    int32_t columnIdx;
    Data columnData;
    
    ComparisionExpr();
    ComparisionExpr(ComparisionOperator cmpOp, int32_t colId, Data colData);
    void printExprTree();
    ExprType getType();
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
    ExprType getType();
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
    ExprType getType();
};


class BetweenExpr : public Expr
{
public:
    int32_t columnIdx;

    Data lowerBound;
    Data upperBound;

    BetweenExpr();
    BetweenExpr(int32_t colId, Data lowBound, Data upBound);
    void printExprTree();
    ExprType getType();
};


class InExpr : public Expr
{
public:
    Data column;
    std::vector<Data> arr;
    InExpr();
    InExpr(Data col, std::vector<Data> vals);
    void printExprTree();
    ExprType getType();
};


class CoalesceExpr : public Expr
{
public:
    std::vector<int> columnIds;
    CoalesceExpr();
    CoalesceExpr(std::vector<int> cols);
    void printExprTree();
    ExprType getType();
};

class SubstrExpr : public Expr
{
public:
    int columnIdx;
    int startIdx;
    int length;
    SubstrExpr();
    SubstrExpr(int colId, int startId, int len);
    void printExprTree();
    ExprType getType();
};


#endif