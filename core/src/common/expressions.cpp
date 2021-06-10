#include "expressions.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include <algorithm>

namespace expressions {

// Helper function to get DataType from jint type
DataType colTypeTrans(int32_t colType) {
    switch(colType) {
        case 1:
            return DataType::INT32D;
        case 2: 
            return DataType::INT64D;
        case 3:
            return DataType::DOUBLED;
        // not yet supported in table
        case 4 :
            return DataType::STRINGD;
    }
    return DataType::INVALIDDATAD;
}

std::string dataTypeString(DataType dt) {
    switch(dt) {
        case DataType::BOOLD: return "bool";
        case DataType::DOUBLED: return "double";
        case DataType::INT32D: return "int32";
        case DataType::INT64D: return "int64";
        case DataType::STRINGD: return "string";
        case DataType::INVALIDDATAD: return "invalid";
    }
}


ExprType Expr::getType() {
    return INVALID_E;
}

DataType Expr::getExprDataType() {
    return dataType;
}


DataExpr::DataExpr() {
}

ExprType DataExpr::getType() {
    return ExprType::DATA_E;
}

// Helper constructors for different data types
DataExpr::DataExpr(int32_t val) {
    isColumn = false;
    dataType = INT32D;
    intVal = val;
}
DataExpr::DataExpr(int64_t val) {
    isColumn = false;
    dataType = INT64D;
    longVal = val;
}
DataExpr::DataExpr(double val) {
    isColumn = false;
    dataType = DOUBLED;
    doubleVal = val;
}
DataExpr::DataExpr(std::string val) {
    isColumn = false;
    dataType = STRINGD;
    stringVal = val;
}
DataExpr::DataExpr(int32_t colIdx, DataType dt) {
    isColumn = true;
    // use magic numbers from jni wrapper
    dataType = dt; // has already been translated
    colVal = colIdx;
}


BinaryExpr::BinaryExpr() {
    dataType = BOOLD;
}

BinaryExpr::BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr) {
    op = bop;
    left = leftExpr;
    right = rightExpr;
    // use the more encompassing DataType
    dataType = std::max(leftExpr->getExprDataType(), rightExpr->getExprDataType());
} 

BinaryExpr::BinaryExpr(Operator bop, Expr *leftExpr, Expr *rightExpr, DataType dt) {
    op = bop;
    left = leftExpr;
    right = rightExpr;
    dataType = dt;
} 

BinaryExpr::~BinaryExpr() {
    delete left;
    delete right;
}

ExprType BinaryExpr::getType() {
    return ExprType::BINARY_E;
}


UnaryExpr::UnaryExpr() {
    dataType = BOOLD;
}

UnaryExpr::UnaryExpr(Operator uop, Expr *expr){
    op = uop;
    exp = expr;
} 

UnaryExpr::UnaryExpr(Operator uop, Expr *expr, DataType dt) {
    op = uop;
    exp = expr;
    dataType = dt;
} 

UnaryExpr::~UnaryExpr() {
    delete exp;
}

ExprType UnaryExpr::getType() {
    return ExprType::UNARY_E;
}


CallExpr::CallExpr() {
}

CallExpr::~CallExpr() {
    for (Expr* exp : arguments) {
        delete exp;
    }
}

CallExpr::CallExpr(CallType ct, std::vector<Expr*> args){
    callType = ct;
    arguments = args;
}

CallExpr::CallExpr(CallType ct, std::vector<Expr*> args, DataType dt){
    callType = ct;
    arguments = args;
    dataType = dt;
}

ExprType CallExpr::getType() {
    return ExprType::CALL_E;
}



// debugging Expr tree
void Expr::printExprTree() {
}

void BinaryExpr::printExprTree() {
    switch(op) 
    {
        // Comparison
        case EQ:
            printf("Cmp(EQ, ");
            break;
        case NEQ:
            printf("Cmp(NEQ, ");
            break;
        case LT:
            printf("Cmp(LT, ");
            break;
        case LTE:
            printf("Cmp(LTE, ");
            break;
        case GT:
            printf("Cmp(GT, ");
            break;
        case GTE:
            printf("Cmp(GTE, ");
            break;
        
        // Logical
        case AND:
            printf("Bin(AND, ");
            break;
        case OR:
            printf("Bin(OR, ");
            break;

        // Arithmetic
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
            printf("invalid BinaryOperator %d(", op);
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
            printf("Unary(NOT, ");
            break;
        default:
            printf("invalid UnaryOperator %d(", op);
            break;
    }
    exp->printExprTree();
    printf(")");
}

void DataExpr::printExprTree() {
    const bool printWithTypes = false; // for debugging types
    #ifdef DEBUG
    std::cout << "Data" + dataTypeString(dataType) + "(";
    #endif
    if (isColumn) {
        printf("#%d", colVal);
    }
    else {
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
            default:
                printf("invalid DataType %d", dataType);
        }
    }
    #ifdef DEBUG
    printf(")");
    #endif
}


void CallExpr::printExprTree() {
    switch(callType) {
        case CallType::ABS: {
            printf("Abs(");
            break;
        }
        case CallType::BETWEEN: {
            printf("Between(");
            break;
        }
        case CallType::CAST: {
            printf("Cast(");
            break;
        }
        case CallType::COALESCE: {
            printf("Coalesce(");
            break;
        }
        case CallType::IF: {
            printf("If(");
            break;
        }
        case CallType::IN: {
            printf("In(");
            break;
        }
        case CallType::SUBSTR: {
            printf("Substr(");
            break;
        }
        default: {
            printf("InvalidCall(");
            break;
        }
    }

    for (int i = 0; i < arguments.size(); i++) {
        arguments[i]->printExprTree();
        if (i == arguments.size() - 1) printf(")");
        else printf(", ");
    }
}

}