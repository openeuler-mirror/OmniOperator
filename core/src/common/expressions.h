#ifndef __EXPRESSIONS_H__
#define __EXPRESSIONS_H__

#include <vector>
#include <stdint.h>
#include <string>
#include <map>

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

// For calling special forms and functions
enum CallType
{
    // special forms
    IN, 
    BETWEEN, 
    COALESCE, 
    IF, 
    // functions
    SUBSTR, 
    CAST, 
    ABS, 
    INVALIDCALL
};


enum DataType
{
    BOOLD, 
    INT32D, 
    INT64D, 
    DOUBLED, 
    STRINGD, 
    INVALIDDATAD
};


enum ExprType
{
    DATA_E, 
    BINARY_E, 
    UNARY_E, 
    CALL_E, // for special forms and functions
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
    int32_t intVal;
    int64_t longVal;
    double doubleVal;
    std::string stringVal;
    int32_t colVal;

    DataExpr();
    DataExpr(int32_t val);
    DataExpr(int64_t val); // might need explicit
    DataExpr(double val);
    DataExpr(std::string val);
    DataExpr(int32_t val, DataType colType);

    void printExprTree();
    ExprType getType();
};
// Helper functions to create DataExprs
// DataExpr* createDataInt32(int32_t val);
// DataExpr* createDataInt64(int64_t val);
// DataExpr* createDataDouble(double val);
// DataExpr* createDataString(std::string val);
// DataExpr* createDataColumn(int32_t colIdx);

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


class CallExpr : public Expr
{
public:
    CallType callType;
    std::vector<Expr*> arguments;

    CallExpr();
    ~CallExpr();
    CallExpr(CallType ct, std::vector<Expr*> args);
    CallExpr(CallType ct, std::vector<Expr*> args, DataType dt);
    
    void printExprTree();
    ExprType getType();
};

}

#endif