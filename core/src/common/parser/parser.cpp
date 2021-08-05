/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: parser function
 */
#include "parser.h"
#include <stdio.h>
#include <iostream>
#include <cstring>
#include <stack>
#include <algorithm>

using namespace std;

using namespace omniruntime::expressions;


Parser::Parser()
{
    externalFuncNames = GetAllExternalFunctionNames();
    externalFuncRetTypeMap = GetFuncReturnTypeMap();
}

Parser::~Parser() {
}

// Update with function return type every time a new function is added
DataType Parser::FuncRetTypeMap(string fnName, vector<Expr *> args)
{
    if (fnName == "CAST") {
        if (args[0]->dataType == STRINGD) return INT32D;
        else return DOUBLED;
    }
    if (fnName == "substr") return STRINGD;
    if (fnName == "concat") return STRINGD;
    if (fnName == "abs") return args[0]->dataType;
    if (fnName == "LIKE") return BOOLD;
    if (fnName == "CombineHash") return INT64D;
    if (externalFuncNames.find(fnName) != externalFuncNames.end()) {
        return externalFuncRetTypeMap[fnName];
    }
    return INVALIDDATAD;
}

// Update with conditions every time a new function is added
bool Parser::FuncDeclMatch(string fnName, vector<Expr *> args, bool checkTypes)
{
    if (fnName == "CAST" && args.size() == 1) {
        return true;
    }
    if (fnName == "substr" && (args.size() == 2 || args.size() == 3)) {
        if (!checkTypes) {
            return true;
        }

        if (args[0]->dataType == STRINGD &&
            (args[1]->dataType == INT32D || args[1]->dataType == INT64D) &&
            (args.size() == 2 || (args[2]->dataType == INT32D || args[2]->dataType == INT64D))) {
            return true;
        }
    }
    if (fnName == "concat" && args.size() == 2) {
        if (!checkTypes) {
            return true;
        }

        if (args[0]->dataType == STRINGD && args[1]->dataType == STRINGD) {
            return true;
        }
    }
    if (fnName == "abs" && args.size() == 1) {
        if (!checkTypes) {
            return true;
        }

        if (args[0]->dataType == INT32D || args[0]->dataType == INT64D || args[0]->dataType == DOUBLED) {
            return true;
        }
    }
    if (fnName == "LIKE" && args.size() == 2) {
        if (!checkTypes) {
            return true;
        }

        // Assuming that like patterns are represented with strings
        if (args[0]->dataType == STRINGD && args[0]->dataType == STRINGD) {
            return true;
        }
    }
    if (fnName == "CombineHash" && args.size() == 2) {
        if (!checkTypes) {
            return true;
        }
        if ((args[0]->dataType == INT32D || args[0]->dataType == INT64D) &&
            (args[1]->dataType == INT32D || args[1]->dataType == INT64D)) {
            return true;
        }
    }
    if (externalFuncNames.find(fnName) != externalFuncNames.end()) {
        // Don't do any other checks for now for external fucntions
        return true;
    }
    return false;
}


string operatorPrefix = "$operator$";
const int SUBSTR_LEN = 10;
// Helper function to remove operator prefix if it is there
string demangleOperator(string opStr)
{
    if (opStr.size() > SUBSTR_LEN && opStr.substr(0, SUBSTR_LEN) == operatorPrefix) {
        return opStr.substr(SUBSTR_LEN);
    }
    return opStr;
}

Operator opTrans(string op)
{
    op = demangleOperator(op);

    // Comparison operators
    if (op == "EQUAL") return Operator::EQ;
    else if (op == "LESS_THAN") return Operator::LT;
    else if (op == "LESS_THAN_OR_EQUAL") return Operator::LTE;
    else if (op == "GREATER_THAN_OR_EQUAL") return Operator::GTE;
    else if (op == "GREATER_THAN") return Operator::GT;
    else if (op == "NOT_EQUAL") return Operator::NEQ;
        // Logical operators
    else if (op == "AND") return Operator::AND;
    else if (op == "OR") return Operator::OR;
    else if (op == "NOT" || op == "not") return Operator::NOT;
        // Arithmetic
    else if (op == "ADD") return Operator::ADD;
    else if (op == "SUBTRACT") return Operator::SUB;
    else if (op == "MULTIPLY") return Operator::MUL;
    else if (op == "DIVIDE") return Operator::DIV;
    else if (op == "MODULUS") return Operator::MOD;
    else return Operator::INVALIDOP;
}

OperatorReturnType getBinaryOperatorType(string opStr)
{
    opStr = demangleOperator(opStr);

    vector<string> allCmpOps{"LESS_THAN", "LESS_THAN_OR_EQUAL", "GREATER_THAN", "GREATER_THAN_OR_EQUAL", "EQUAL",
                              "NOT_EQUAL"};
    vector<string> allLogOps{"AND", "OR"};
    vector<string> allArithOps{"ADD", "SUBTRACT", "MULTIPLY", "DIVIDE", "MODULULS"};
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

bool isUnaryOperator(string opStr)
{
    vector<string> allUnaryOps{"NOT", "not"};
    for (string unaryOp : allUnaryOps) {
        if (opStr == unaryOp) return true;
    }
    return false;
}


Expr *Parser::ParseRowExpression(string input, int32_t *inputTypes, int32_t veccount)
{
    // remove spaces from input but not from inside strings
    string newinput = "";
    bool isInString = false;
    for (int i = 0; i < input.size(); i++) {
        if (input[i] == '\'') {
            isInString = !isInString;
            newinput.push_back(input[i]);
        } else if (input[i] == ' ') {
            if (isInString) {
                newinput.push_back(input[i]);
            }
        }  else newinput.push_back(input[i]);
    }
    input = newinput;


    int firstParenInd = input.find("(");
    // Check if it is just data (i.e. 123, #4, 34.4)
    if (firstParenInd == string::npos) return GenerateData(input, inputTypes, veccount);

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
    args.push_back(ParseRowExpression(exprStr.substr(0, commaPositions[0]), inputTypes, veccount));
    for (int i = 1; i <= numCommas; i++) {
        string currVal = exprStr.substr(commaPositions[i - 1] + 1, commaPositions[i] - commaPositions[i - 1] - 1);
        args.push_back(ParseRowExpression(currVal, inputTypes, veccount));
    }

    // BinaryExpr
    OperatorReturnType binRetType = getBinaryOperatorType(opStr);
    if (binRetType != OperatorReturnType::INVALIDRETURNTYPE && args.size() == 2) {
        if (binRetType == OperatorReturnType::COMPARISON)
            return std::make_unique<BinaryExpr>(opTrans(opStr), args[0], args[1], DataType::BOOLD).release();
        if (binRetType == OperatorReturnType::LOGICAL)
            return std::make_unique<BinaryExpr>(opTrans(opStr), args[0], args[1], DataType::BOOLD).release();
        // uses most lenient data type of left and right
        if (binRetType == OperatorReturnType::ARITHMETIC) return std::make_unique<BinaryExpr>(opTrans(opStr), args[0], args[1]).release();
    }

    // UnaryExpr
    if (isUnaryOperator(opStr) && args.size() == 1) {
        return std::make_unique<UnaryExpr>(opTrans(opStr), args[0], BOOLD).release(); // only handling NOT for now
    }

    // Special form
    // Special forms are IN, BETWEEN, IF, COALESCE
    if (opStr == "BETWEEN") return std::make_unique<BetweenExpr>(args[0], args[1], args[2]).release();
    if (opStr == "IN") return std::make_unique<InExpr>(args).release();
    if (opStr == "COALESCE") return std::make_unique<CoalesceExpr>(args[0], args[1]).release();
    if (opStr == "IF") return std::make_unique<IfExpr>(args[0], args[1], args[2]).release();

    // Function
    // Check that the signature matches
    if (FuncDeclMatch(opStr, args, false)) {
        return std::make_unique<FuncExpr>(opStr, args, FuncRetTypeMap(opStr, args)).release();
    }
    // default to false
    return std::make_unique<DataExpr>(false).release();
}

// Helper functions for generateComparisionExpr to find the correct data type
// Takes in an array of inputTypes from filter to determine the correct column type
DataType getDataType(string data, int32_t *inputTypes, int32_t vecCount)
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
        return ColTypeTrans(inputTypes[colIdx]);
    }
    // First check if int32 or int64
    bool isIntOrLong = true;
    for (int i = 0; i < data.size(); i++) {
        if ((i == 0 && data[i] == '-') || ('0' <= data[i] && data[i] <= '9')) {
            continue;
        } else {
            isIntOrLong = false;
            break;
        }
    }
    if (isIntOrLong) {
        int64_t longVal = stol(data);
        if (INT32_MIN <= longVal && longVal <= INT32_MAX) return INT32D;
        return INT64D;
    }

    // Check if double
    // default to string
    bool foundDot = false;
    for (int i = 0; i < data.size(); i++) {
        if (!((i == 0 && data[i] == '-') || ('0' <= data[i] && data[i] <= '9') || data[i] == '.')) {
            return  STRINGD;
        }
        if (data[i] == '.') {
            if (foundDot) return STRINGD;
            foundDot = true;
        }
    }
    return DOUBLED;
}

// Helper function to turn all % to .* for regex wildcard matching
string *fixString(string dataStr)
{
    string *fixedStr = std::make_unique<string>("").release();
    for (int i = 0; i < dataStr.size(); i++) {
        if (dataStr[i] == '%') {
            fixedStr->push_back('.');
            fixedStr->push_back('*');
        } else fixedStr->push_back(dataStr[i]);
    }
    return fixedStr;
}

DataExpr *Parser::GenerateData(string dataStr, int32_t *inputTypes, int32_t vecCount)
{
#ifdef DEBUG
    std::cout << "generating data:::" << dataStr << std::endl;
#endif
    // Case with boolean true/false
    if (dataStr == "true") return std::make_unique<DataExpr>(true).release();
    if (dataStr == "false") return std::make_unique<DataExpr>(false).release();

    // Other cases
    DataType currDataType = getDataType(dataStr, inputTypes, vecCount);
#ifdef DEBUG
    cout << "currDataType: " << currDataType << endl;
#endif
    // Case with normal string format (ex. 'hello')
    if (currDataType == STRINGD && dataStr[0] == '\'' && dataStr[dataStr.size() - 1] == '\'') {
        return std::make_unique<DataExpr>(fixString(dataStr.substr(1, dataStr.size() - 2))).release();
    }
    // Case with column
    if (dataStr[0] == '#') {
        int colIdx = stoi(dataStr.substr(1));
        DataType dt = ColTypeTrans(inputTypes[colIdx]);
        return std::make_unique<DataExpr>(colIdx, dt).release();
    }

    // Case with regular data (int, long, double, string)
    switch (currDataType) {
        // handle boolean as int32
        case BOOLD:
            return std::make_unique<DataExpr>(stoi(dataStr)).release();
        case INT32D: {
            DataExpr *e = std::make_unique<DataExpr>(stoi(dataStr)).release();
            e->longVal = e->intVal;
            e->doubleVal = e->intVal;
            return e;
        }
        case INT64D:
            return  std::make_unique<DataExpr>(stol(dataStr)).release();
        case DOUBLED:
            return  std::make_unique<DataExpr>(stod(dataStr)).release();
        case STRINGD: {
            return std::make_unique<DataExpr>(fixString(dataStr)).release();
        }
        default:
            // create string data with value "Invalid data"
            return std::make_unique<DataExpr>("Invalid data").release();
    }
}
