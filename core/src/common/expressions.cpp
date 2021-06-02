#include "expressions.h"
#include <stdio.h>
#include <iostream>

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

ComparisionExpr::ComparisionExpr() {
}

ComparisionExpr::ComparisionExpr(ComparisionOperator cmpOp, int colId, int colData) {
    op = cmpOp;
    columnIdx = colId;
    columnData = colData;
} 

BetweenExpr::BetweenExpr() {
}

BetweenExpr::BetweenExpr(int colId, int lowBound, int highBound) {
    columnIdx = colId;
    lowerBound = lowBound;
    upperBound = highBound;
}

UnaryExpr::UnaryExpr() {
}

UnaryExpr::UnaryExpr(LogicalOperator logOp, Expr *expr) {
    op = logOp;
    exp = expr;
} 

UnaryExpr::~UnaryExpr() {
    delete exp;
}

ArithmeticExpr::ArithmeticExpr() {
}

ArithmeticExpr::ArithmeticExpr(ArithmeticOperator aOp, Expr *leftExpr, Expr *rightExpr) {
    op = aOp;
    left = leftExpr;
    right = rightExpr;
} 

ArithmeticExpr::~ArithmeticExpr() {
    delete left;
    delete right;
}

InExpr::InExpr() {
}

InExpr::InExpr(int colId, std::vector<int> vals) {
    columnIdx = colId;
    arr = vals;
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
            printf("invalid ComparisionOperator %d", op);
            break;
    }
    printf("#%d, %d)", columnIdx, columnData);
}

void BetweenExpr::printExprTree() {
    printf("Between(#%d, %d, %d)", columnIdx, lowerBound, upperBound);
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
    printf("#%d, ", columnIdx);
    for (int i = 0; i < arr.size() - 1; i++) {
        printf("%d, ", arr[i]);
    }
    printf("%d)", arr[arr.size()-1]);
}
