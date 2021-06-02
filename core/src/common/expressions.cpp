#include "expressions.h"
#include <stdio.h>
#include <iostream>
#include <string>


ExprType Expr::getType() {
    return INVALID_E;
}


BinaryExpr::BinaryExpr() {
}

BinaryExpr::BinaryExpr(LogicalOperator logOp, Expr *leftExpr, Expr *rightExpr){
    op = logOp;
    left = leftExpr;
    right = rightExpr;
} 

BinaryExpr::~BinaryExpr() {
    delete left;
    delete right;
}

ExprType BinaryExpr::getType() {
    return ExprType::BINARY_E;
}


ComparisionExpr::ComparisionExpr() {
}

ComparisionExpr::ComparisionExpr(ComparisionOperator cmpOp, int colId, Data colData) {
    op = cmpOp;
    columnIdx = colId;
    columnData = colData;
} 

ExprType ComparisionExpr::getType() {
    return ExprType::COMPARISION_E;
}


UnaryExpr::UnaryExpr() {
}

UnaryExpr::UnaryExpr(LogicalOperator logOp, Expr *expr){
    op = logOp;
    exp = expr;
} 

UnaryExpr::~UnaryExpr() {
    delete exp;
}

ExprType UnaryExpr::getType() {
    return ExprType::UNARY_E;
}


ArithmeticExpr::ArithmeticExpr() {
}

ArithmeticExpr::ArithmeticExpr(ArithmeticOperator aOp, Expr *leftExpr, Expr *rightExpr){
    op = aOp;
    left = leftExpr;
    right = rightExpr;
} 

ArithmeticExpr::~ArithmeticExpr() {
    delete left;
    delete right;
}

ExprType ArithmeticExpr::getType() {
    return ExprType::ARITHMETIC_E;
}


BetweenExpr::BetweenExpr() {
}

BetweenExpr::BetweenExpr(int colId, Data lowBound, Data upBound) {
    columnIdx = colId;
    lowerBound = lowBound;
    upperBound = upBound;
}

ExprType BetweenExpr::getType() {
    return ExprType::BETWEEN_E;
}


InExpr::InExpr() {
}

InExpr::InExpr(Data col, std::vector<Data> vals) {
    column = col;
    arr = vals;
}

ExprType InExpr::getType() {
    return ExprType::IN_E;
}


CoalesceExpr::CoalesceExpr() {
}

CoalesceExpr::CoalesceExpr(std::vector<int> cols) {
    columnIds = cols;
}

ExprType CoalesceExpr::getType() {
    return ExprType::COALESCE_E;
}


SubstrExpr::SubstrExpr() {
}

SubstrExpr::SubstrExpr(int colId, int startId, int len) {
    columnIdx = colId;
    startIdx = startId;
    length = len;
}

ExprType SubstrExpr::getType() {
    return ExprType::SUBSTR_E;
}


// debugging Expr tree
void Expr::printExprTree() {
}

void BinaryExpr::printExprTree() {
    switch(op) 
    {
        case AND:
            printf("Bin(AND, ");
            break;
        case OR:
            printf("Bin(OR, ");
            break;
        default:
            printf("invalid BinaryOperator %d", op);
            break;
    }
    left->printExprTree();
    printf(", ");
    right->printExprTree();
    printf(")");
}

void UnaryExpr::printExprTree() {
    switch(op) 
    {
        case NOT:
            printf("Un(NOT, ");
            break;
        default:
            printf("invalid UnaryOperator %d", op);
            break;
    }
    exp->printExprTree();
    printf(")");
}

void Data::printData(bool printWithTypes) {
    switch(dataType)
    {
        case INT32D:
            if (printWithTypes) printf("i32_");
            printf("%d", intVal);
            break;
        case INT64D:
            if (printWithTypes) printf("i64_");
            printf("%ld", longVal);
            break;
        case DOUBLED:
            if (printWithTypes) printf("d64_");
            printf("%f", doubleVal);
            break;
        case STRINGD:
            if (printWithTypes) printf("s_");
            printf("'%s'", stringVal.c_str());
            break;
        case COLUMND:
            printf("#%d", colVal);
            break;
        default:
            printf("invalid DataType %d", dataType);
    }
}


void ComparisionExpr::printExprTree() {
    switch(op) 
    {
        case EQ:
            printf("Cmp(EQ, ");
            break;
        case LT:
            printf("Cmp(LT, ");
            break;
        case GT:
            printf("Cmp(GT, ");
            break;
        case LTE:
            printf("Cmp(LTE, ");
            break;
        case GTE:
            printf("Cmp(GTE, ");
            break;
        case NEQ:
            printf("Cmp(NEQ, ");
            break;
        default:
            printf("invalid ComparisionOperator %d(", op);
            break;
    }
    printf("#%d, ", columnIdx);
    columnData.printData(false);
    printf(")");
}

void BetweenExpr::printExprTree() {
    printf("Between(#%d, ", columnIdx);
    lowerBound.printData(false);
    printf(", ");
    upperBound.printData(false);
    printf(")");
}

void ArithmeticExpr::printExprTree() {
    switch(op) {
        case ADD:
            printf("Arith(ADD, ");
            break;
        case SUB:
            printf("Arith(SUB, ");
            break;
        case MUL:
            printf("Arith(MUL, ");
            break;
        case DIV:
            printf("Arith(DIV, ");
            break;
        case MOD:
            printf("Arith(MOD, ");
            break;
        default:
            printf("Invalid arithmetic operation");
    }
    left->printExprTree();
    printf(", ");
    right->printExprTree();
    printf(")");
}

void InExpr::printExprTree() {
    printf("In(");
    column.printData(false);
    printf(", ");
    for (int i = 0; i < arr.size() - 1; i++) {
        arr[i].printData(false);
        printf(", ");
    }
    arr[arr.size() - 1].printData(false);
    printf(")");
}

void CoalesceExpr::printExprTree() {
    printf("Coalesce(");
    for (int i = 0; i < columnIds.size() - 1; i++) {
        printf("#%d, ", columnIds[i]);
    }
    printf("#%d)", columnIds[columnIds.size() - 1]);
}

void SubstrExpr::printExprTree() {
    printf("Substr(#%d, %d, %d)", columnIdx, startIdx, length);
}