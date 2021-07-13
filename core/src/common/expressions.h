#ifndef __EXPRESSIONS_H__
#define __EXPRESSIONS_H__

#include <vector>
#include <stdint.h>
#include <string>
#include <map>

namespace omniruntime {
namespace expressions {

// place holder context class here
class Context
{
    
};


enum Operator
{
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



enum DataType
{
    BOOLD = 4, 
    INT32D = 1, 
    INT64D = 2, 
    DOUBLED = 3, 
    STRINGD = 100, 
    INVALIDDATAD
};


enum ExprType
{
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


class Expr
{
    public:
        DataType dataType; // dataType of returned value
        DataType getExprDataType();

        virtual ExprType getType();
        virtual ~Expr() = default;
        virtual void printExprTree();
};


class DataExpr : public Expr
{
public:
    bool isColumn;
    bool boolVal;
    int32_t intVal;
    int64_t longVal;
    double doubleVal;
    std::string* stringVal;
    int32_t colVal;

    DataExpr();
    ~DataExpr();
    DataExpr(bool val);
    DataExpr(int32_t val);
    DataExpr(int64_t val);
    DataExpr(double val);
    DataExpr(std::string* val);
    DataExpr(int32_t val, DataType colType);

    void printExprTree();
    ExprType getType();
};


// Helper function to translate from jni type number to DataType
DataType colTypeTrans(int32_t colType);

// Helper function for printing out datatypes
std::string dataTypeString(DataType dt);


class UnaryExpr : public Expr
{
public:
    Operator op;
    Expr *exp;

    UnaryExpr();
    ~UnaryExpr();
    UnaryExpr(Operator logOp, Expr *bodyexp);
    UnaryExpr(Operator uop, Expr *expr, DataType dt);

    void printExprTree();
    ExprType getType();
};


class BinaryExpr : public Expr
{
public:
    Operator op;
    Expr *left;
    Expr *right;

    BinaryExpr();
    ~BinaryExpr();
    BinaryExpr(Operator op, Expr *leftExpr, Expr *rightExpr);
    BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr, DataType dt);

    void printExprTree();
    ExprType getType();
};


class InExpr : public Expr
{
public:
    // first element of arguments is the value to be compared to every other argument
    std::vector<Expr*> arguments;

    InExpr();
    ~InExpr();
    InExpr(std::vector<Expr*> args);

    void printExprTree();
    ExprType getType();
};


class BetweenExpr : public Expr
{
public:
    Expr* value;
    Expr* lowerBound;
    Expr* upperBound;

    BetweenExpr();
    ~BetweenExpr();
    BetweenExpr(Expr* val, Expr* lowBound, Expr* upBound);

    void printExprTree();
    ExprType getType();
};


class IfExpr : public Expr
{
public:
    Expr* condition;
    Expr* trueExpr;
    Expr* falseExpr;

    IfExpr();
    ~IfExpr();
    IfExpr(Expr* cond, Expr* texp, Expr* fexp);

    void printExprTree();
    ExprType getType();
};


class CoalesceExpr : public Expr
{
public:
    Expr* value1;
    Expr* value2;

    CoalesceExpr();
    ~CoalesceExpr();
    CoalesceExpr(Expr* val1, Expr* val2);

    void printExprTree();
    ExprType getType();
};


class FuncExpr : public Expr
{
public:
    std::string funcName;
    std::vector<Expr*> arguments;

    FuncExpr();
    ~FuncExpr();
    FuncExpr(std::string fnName, std::vector<Expr*> args);
    FuncExpr(std::string fnName, std::vector<Expr*> args, DataType dt);
    
    void printExprTree();
    ExprType getType();
};

}
}
#endif
