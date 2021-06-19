
#include "parser.h"
#include <stdio.h>
#include <iostream>
#include <cstring>
#include <stack> // std::stack
#include <algorithm>
using namespace std;

string operatorPrefix = "$operator$";
Operator opTrans(string op)
{
    // Comparison operators
    if (op == operatorPrefix + "EQUAL") return Operator::EQ;
    else if (op == operatorPrefix + "LESS_THAN" || op == "LESS_THAN") return Operator::LT;
    else if (op == operatorPrefix + "LESS_THAN_OR_EQUAL" || op == "LESS_THAN_OR_EQUAL") return Operator::LTE;
    else if (op == operatorPrefix + "GREATER_THAN_OR_EQUAL" || op == "GREATER_THAN_OR_EQUAL") return Operator::GTE;
    else if (op == operatorPrefix + "GREATER_THAN" || op == "GREATER_THAN") return Operator::GT;
    else if (op == operatorPrefix + "NOT_EQUAL" || op == "NOT_EQUAL") return Operator::NEQ;
    // Logical operators
    else if (op == "AND") return Operator::AND;
    else if (op == "OR") return Operator::OR;
    else if (op == "NOT" || op == "not") return Operator::NOT;
    // Arithmetic
    else if (op == operatorPrefix + "ADD" || op == "ADD") return Operator::ADD;
    else if (op == operatorPrefix + "SUBTRACT" || op == "SUBTRACT") return Operator::SUB;
    else if (op == operatorPrefix + "MULTIPLY" || op == "MULTIPLY") return Operator::MUL;
    else if (op == operatorPrefix + "DIVIDE" || op == "DIVIDE") return Operator::DIV;
    else if (op == operatorPrefix + "MODULUS" || op == "MODULUS") return Operator::MOD;
    else return Operator::INVALIDOP;
}


OperatorReturnType getBinaryOperatorType(string opStr) 
{
    vector<string> allCmpOps {operatorPrefix + "LESS_THAN", operatorPrefix + "LESS_THAN_OR_EQUAL", operatorPrefix + "GREATER_THAN", operatorPrefix + "GREATER_THAN_OR_EQUAL", operatorPrefix + "EQUAL", operatorPrefix + "NOT_EQUAL", 
    "LESS_THAN", "LESS_THAN_OR_EQUAL", "GREATER_THAN", "GREATER_THAN_OR_EQUAL", "EQUAL", "NOT_EQUAL"};
    vector<string> allLogOps {"AND", "OR"};
    vector<string> allArithOps {operatorPrefix + "ADD", operatorPrefix + "SUBTRACT", operatorPrefix + "MULTIPLY", operatorPrefix + "DIVIDE", operatorPrefix + "MODULUS", 
    "ADD", "SUBTRACT", "MULTIPLY", "DIVIDE", "MODULULS"};
    for (string cmpOp : allCmpOps) {
        if (opStr == cmpOp) return OperatorReturnType::COMPARISON;
    }
    for (string logOp : allLogOps) {
        if (opStr == logOp) return OperatorReturnType::LOGICAL;
    }
    for (string arithOp : allArithOps) {
        if (opStr == arithOp) return OperatorReturnType::ARITHMETIC;
    }
    return OperatorReturnType::INVALIDRETURNTYPE;
}
// For functions and special forms
CallType callTrans(string fn)
{
    if (fn == "BETWEEN") return CallType::BETWEEN;
    else if (fn == "IN") return CallType::IN;
    else if (fn == "COALESCE") return CallType::COALESCE;
    else if (fn == "IF") return CallType::IF;
    else if (fn == "substr") return CallType::SUBSTR;
    else if (fn == "CAST") return CallType::CAST;
    else if (fn == "abs") return CallType::ABS;
    else return CallType::INVALIDCALL;
}



bool isUnaryOperator(string opStr)
{
    vector<string> allUnaryOps {"NOT", "not"};
    for (string unaryOp : allUnaryOps) {
        if (opStr == unaryOp) return true;
    }
    return false;
}



Expr *Parser::parseRowExpression(string input, int32_t *inputTypes, int32_t vecCount)
{
    // remove spaces from input
    input.erase(remove(input.begin(), input.end(), ' '), input.end());

    int firstParenInd = input.find("(");
    // Check if it is just data (i.e. 123, #4, 34.4)
    if (firstParenInd == string::npos) return generateData(input, inputTypes, vecCount);

    string opStr = input.substr(0, firstParenInd);
    string exprStr = input.substr(firstParenInd + 1, input.size() - firstParenInd - 2);


    // ensure that strings and parentheses are respected
    vector<int> commaPositions; // indices of commas in exprStr
    int numCommas = 0;
    int parenCount = 0;
    bool outsideQuotes = true;
    for (int i = 0; i < exprStr.size(); i++) {
        if (exprStr[i] == ',' && parenCount == 0 && outsideQuotes) {
            commaPositions.push_back(i);
            numCommas++;
        }
        if (exprStr[i] == '\'') outsideQuotes = !outsideQuotes;
        if (exprStr[i] == '(') parenCount++;
        if (exprStr[i] == ')') parenCount--;
    }
    commaPositions.push_back(exprStr.size());

    // Place all of the arguments into a vector first
    vector<Expr*> args;
    args.push_back(parseRowExpression(exprStr.substr(0, commaPositions[0]), inputTypes, vecCount));
    for (int i = 1; i <= numCommas; i++) {
        string currVal = exprStr.substr(commaPositions[i-1]+1, commaPositions[i] - commaPositions[i-1] - 1);
        args.push_back(parseRowExpression(currVal, inputTypes, vecCount));
    }

    // BinaryExpr
    OperatorReturnType binRetType = getBinaryOperatorType(opStr);
    if (binRetType != OperatorReturnType::INVALIDRETURNTYPE && args.size() == 2) {
        if (binRetType == OperatorReturnType::COMPARISON) return new BinaryExpr(opTrans(opStr), args[0], args[1], DataType::BOOLD);
        if (binRetType == OperatorReturnType::LOGICAL) return new BinaryExpr(opTrans(opStr), args[0], args[1], DataType::BOOLD);
        // uses most lenient data type of left and right
        if (binRetType == OperatorReturnType::ARITHMETIC) return new BinaryExpr(opTrans(opStr), args[0], args[1]);
    }

    // UnaryExpr
    if (isUnaryOperator(opStr) && args.size() == 1) {
        return new UnaryExpr(opTrans(opStr), args[0], BOOLD); // only handling NOT for now
    }

    // Default to CallExpr
    CallType ct = callTrans(opStr);

    // special forms
    if (ct == CallType::IN || ct == CallType::BETWEEN) return new CallExpr(ct, args, BOOLD);
    if (ct == CallType::COALESCE) return new CallExpr(ct, args, args[0]->getExprDataType());
    if (ct == CallType::IF) return new CallExpr(ct, args, args[1]->getExprDataType()); // dataType of true branch
    // functions
    if (ct == CallType::SUBSTR) return new CallExpr(ct, args, STRINGD);
    if (ct == CallType::CAST) {
        // uses next highest DataType
        if (args[0]->getExprDataType() == BOOLD) return new CallExpr(ct, args, INT32D); 
        else if (args[0]->getExprDataType() == INT32D) return new CallExpr(ct, args, INT64D); 
        else if (args[0]->getExprDataType() == INT64D) return new CallExpr(ct, args, DOUBLED); 
        // Default to string
        else return new CallExpr(ct, args, STRINGD); 
    }
    if (ct == CallType::ABS) return new CallExpr(ct, args, args[0]->getExprDataType());
    // Default to INVALIDCALL
    return new CallExpr(ct, args, INVALIDDATAD);
}

// Helper functions for generateComparisionExpr to find the correct data type 
// Takes in an array of inputTypes from filter to determine the correct column type
DataType getDataType(string data, int32_t* inputTypes, int32_t vecCount) 
{
    // Check for '' (string)
    if (data[0] == '\'' && data[data.size() - 1] == '\'') {
        return STRINGD;
    }
    // Check for # (column)
    if (data[0] == '#') {
        for (int i = 1; i < data.size(); i++) {
            // Treat as string if the chars aren't digits
            if (!isdigit(data[i])) return STRINGD;
        }
        int colIdx = stoi(data.substr(1));
        return colTypeTrans(inputTypes[colIdx]);
    }
    // First check if int32 or int64
    bool isIntOrLong = true;
    for (int i = 0; i < data.size(); i++) {
        if ((i == 0 && data[i] == '-') || ('0' <= data[i] && data[i] <= '9')) {
            continue;
        }
        else {
            isIntOrLong = false;
            break;
        }
    }
    if (isIntOrLong) {
        long longVal = stol(data);
        if (INT32_MIN <= longVal && longVal <= INT32_MAX) return INT32D;
        return INT64D;
    }

    // Check if double
    // default to string
    bool foundDot = false;
    for (int i = 0; i < data.size(); i++) {
        if ((i == 0 && data[i] == '-') || ('0' <= data[i] && data[i] <= '9') || data[i] == '.') {
            if (data[i] == '.') {
                if (foundDot) return STRINGD; // if a second . is found
                foundDot = true;
            }
        }
        else {
            return STRINGD;
        }
    }

    return DOUBLED;
}

DataExpr* Parser::generateData(string dataStr, int32_t* inputTypes, int32_t vecCount)
{
    #ifdef DEBUG
    // std::cout << "generating data:::" << dataStr << std::endl;
    #endif
    DataType currDataType = getDataType(dataStr, inputTypes, vecCount);
    // Case with normal string format (ex. 'hello')
    if (currDataType == STRINGD && dataStr[0] == '\'' && dataStr[dataStr.size() - 1] == '\'') {
        return new DataExpr(dataStr.substr(1, dataStr.size() - 2));
    }
    // Case with column
    if (dataStr[0] == '#') {
        int colIdx = stoi(dataStr.substr(1));
        DataType dt = colTypeTrans(inputTypes[colIdx]);
        return new DataExpr(colIdx, dt);
    }

    // Case with regular data (int, long, double, string)
    switch(currDataType)
    {
        // handle boolean as int32
        case BOOLD:
            return new DataExpr(stoi(dataStr));
        case INT32D: {
            DataExpr* de = new DataExpr(stoi(dataStr));
            de->longVal = de->intVal;
            de->doubleVal = de->intVal;
            return de;
        }
        case INT64D:
            return new DataExpr(stol(dataStr));
        case DOUBLED: 
            return new DataExpr(stod(dataStr));
        case STRINGD: 
            return new DataExpr(dataStr);
        default:
            // create string data with value "Invalid data"
            return new DataExpr("Invalid data"); 
    }
}
