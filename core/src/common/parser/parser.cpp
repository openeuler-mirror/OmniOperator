/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: parser function
 */
#include "parser.h"
#include <iostream>
#include <stack>
#include <algorithm>

using namespace std;

using namespace omniruntime::expressions;


Parser::Parser() {
}

Parser::~Parser() {
}

namespace {
    const string OPERATOR_PREFIX = "$operator$";
    const int32_t SUBSTR_LEN = 10;
    const int32_t ARG2 = 2;
}

// Helper function to remove operator prefix if it is there
string DemangleOperator(string opStr)
{
    if (opStr.size() > SUBSTR_LEN && opStr.substr(0, SUBSTR_LEN) == OPERATOR_PREFIX) {
        return opStr.substr(SUBSTR_LEN);
    }
    return opStr;
}

Operator OpTrans(string op)
{
    op = DemangleOperator(op);

    // Comparison operators
    if (op == "EQUAL") {
        return Operator::EQ;
    } else if (op == "LESS_THAN") {
        return Operator::LT;
    } else if (op == "LESS_THAN_OR_EQUAL") {
        return Operator::LTE;
    } else if (op == "GREATER_THAN_OR_EQUAL") {
        return Operator::GTE;
    } else if (op == "GREATER_THAN") {
        return Operator::GT;
    } else if (op == "NOT_EQUAL") {
        return Operator::NEQ;
        // Logical operators
    } else if (op == "AND") {
        return Operator::AND;
    } else if (op == "OR") {
        return Operator::OR;
    } else if (op == "NOT" || op == "not") {
        return Operator::NOT;
        // Arithmetic
    } else if (op == "ADD") {
        return Operator::ADD;
    } else if (op == "SUBTRACT") {
        return Operator::SUB;
    } else if (op == "MULTIPLY") {
        return Operator::MUL;
    } else if (op == "DIVIDE") {
        return Operator::DIV;
    } else if (op == "MODULUS") {
        return Operator::MOD;
    } else {
        return Operator::INVALIDOP;
}
}

OperatorReturnType GetBinaryOperatorType(string opStr)
{
    opStr = DemangleOperator(opStr);

    vector<string> allCmpOps{"LESS_THAN", "LESS_THAN_OR_EQUAL", "GREATER_THAN", "GREATER_THAN_OR_EQUAL", "EQUAL",
                              "NOT_EQUAL"};
    vector<string> allLogOps{"AND", "OR"};
    vector<string> allArithOps{"ADD", "SUBTRACT", "MULTIPLY", "DIVIDE", "MODULUS"};
    for (string cmpOp : allCmpOps) {
        if (opStr == cmpOp) {return OperatorReturnType::COMPARISON; }
    }
    for (string logOp : allLogOps) {
        if (opStr == logOp) {return OperatorReturnType::LOGICAL; }
    }
    for (string arithOp : allArithOps) {
        if (opStr == arithOp) {return OperatorReturnType::ARITHMETIC; }
    }
    return OperatorReturnType::INVALIDRETURNTYPE;
}

bool IsUnaryOperator(string opStr)
{
    vector<string> allUnaryOps{"NOT", "not"};
    for (string unaryOp : allUnaryOps) {
        if (opStr == unaryOp) {
            return true;
        }
    }
    return false;
}

string Parser::StripString(string input) const
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
        } else {
            newinput.push_back(input[i]);
        }
    }
    return newinput;
}

Expr *Parser::ParseRowExpression(const string inputStr, int32_t *inputTypes, int32_t veccount)
{
    string input = this->StripString(inputStr);
    int firstParenInd = input.find("(");
    // Check if it is just data (i.e. 123, #4, 34.4)
    if (firstParenInd == string::npos) {
        return GenerateData(input, inputTypes, veccount);
    }

    string opStr = input.substr(0, firstParenInd);
    string exprStr = input.substr(firstParenInd + 1, input.size() - firstParenInd - 1 - 1);

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
        if (exprStr[i] == '\'') {outsideQuotes = !outsideQuotes; }
        if (exprStr[i] == '(') {parenCount++; }
        if (exprStr[i] == ')') {parenCount--; }
    }
    commaPositions.push_back(exprStr.size());

    // Place all of the arguments into a vector first
    vector<Expr *> args;
    args.push_back(ParseRowExpression(exprStr.substr(0, commaPositions[0]), inputTypes, veccount));
    for (int i = 1; i <= numCommas; i++) {
        string currVal = exprStr.substr(commaPositions[i - 1] + 1, commaPositions[i] - commaPositions[i - 1] - 1);
        args.push_back(ParseRowExpression(currVal, inputTypes, veccount));
    }

    return ParseRowExpressionHelper(opStr, args);
}

Expr *Parser::ParseRowExpressionHelper(string opStr, vector<Expr *> args)
{
    // BinaryExpr
    OperatorReturnType binRetType = GetBinaryOperatorType(opStr);
    if (binRetType != OperatorReturnType::INVALIDRETURNTYPE && args.size() == ARG2) {
        if (binRetType == OperatorReturnType::COMPARISON || binRetType == OperatorReturnType::LOGICAL) {
            return std::make_unique<BinaryExpr>
                    (OpTrans(opStr), args[0], args[1], DataType::BOOLD).release();
        }
        // uses most lenient data type of left and right
        if (binRetType == OperatorReturnType::ARITHMETIC) {
            return std::make_unique<BinaryExpr>(OpTrans(opStr), args[0], args[1]).release();
        }
    }

    // UnaryExpr
    // only handling NOT for now
    if (IsUnaryOperator(opStr) && args.size() == 1) {
        return std::make_unique<UnaryExpr>(OpTrans(opStr), args[0], BOOLD).release();
    }

    // Special form
    // Special forms are IN, BETWEEN, IF, COALESCE
    if (opStr == "BETWEEN") return std::make_unique<BetweenExpr>(args[0], args[1], args[ARG2]).release();
    if (opStr == "IN") return std::make_unique<InExpr>(args).release();
    if (opStr == "COALESCE") return std::make_unique<CoalesceExpr>(args[0], args[1]).release();
    if (opStr == "IF") return std::make_unique<IfExpr>(args[0], args[1], args[ARG2]).release();

    // Function
    // Check that the signature matches
    if (ph.FuncDeclMatch(opStr, args, true)) {
        return std::make_unique<FuncExpr>(opStr, args, ph.FuncRetTypeMap(opStr, args)).release();
    }
    // default to false
    return std::make_unique<DataExpr>(false).release();
}

// Helper functions for generateComparisionExpr to find the correct data type
// Takes in an array of inputTypes from filter to determine the correct column type
DataType GetDataTypeInt(string data)
{
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
        if (INT32_MIN <= longVal && longVal <= INT32_MAX) {
            return INT32D;
        }
        return INT64D;
    }
    return INVALIDDATAD;
}

DataType GetDataTypeHelper(string data)
{
    bool foundDot = false;
    for (int i = 0; i < data.size(); i++) {
        if ((i == 0 && data[i] == '-') || ('0' <= data[i] && data[i] <= '9') || data[i] == '.') {
            if (data[i] == '.' && foundDot) {
                return STRINGD; // if a second . is found
            } else if (data[i] == '.') {
                foundDot = true;
            }
        } else {
            return STRINGD;
        }
    }
    return DOUBLED;
}

DataType GetDataType(string data, int32_t *inputTypes, int32_t vecCount)
{
    // Check for '' (string)
    if (data[0] == '\'' && data[data.size() - 1] == '\'') {
        return STRINGD;
    }
    // Check for # (column)
    if (data[0] == '#') {
        for (int i = 1; i < data.size(); i++) {
            // Treat as string if the chars aren't digits
            if (!isdigit(data[i])) {
                return STRINGD;
            }
        }
        int colIdx = stoi(data.substr(1));
        return ColTypeTrans(inputTypes[colIdx]);
    }
    DataType intType = GetDataTypeInt(data);
    if (intType != INVALIDDATAD) {
        return intType;
    }

    // Check if double
    // default to string
    return GetDataTypeHelper(data);
}

// Helper function to turn all % to .* for regex wildcard matching
string *FixString(string dataStr)
{
    string *fixedStr = std::make_unique<string>("").release();
    for (int i = 0; i < dataStr.size(); i++) {
        if (dataStr[i] == '%') {
            fixedStr->push_back('.');
            fixedStr->push_back('*');
        } else {
            fixedStr->push_back(dataStr[i]);
}
    }
    return fixedStr;
}

DataExpr *Parser::GenerateDataHelper(string dataStr, DataType currDataType) const
{
    switch (currDataType) {
        // handle boolean as int32
        case BOOLD: {
            return std::make_unique<DataExpr>(stoi(dataStr)).release();
        }
        case INT32D: {
            DataExpr *e = std::make_unique<DataExpr>(stoi(dataStr)).release();
            e->longVal = e->intVal;
            e->doubleVal = e->intVal;
            return e;
        }
        case INT64D: {
            return  std::make_unique<DataExpr>(stol(dataStr)).release();
        }
        case DOUBLED: {
            return std::make_unique<DataExpr>(stod(dataStr)).release();
        }
        case STRINGD: {
            return std::make_unique<DataExpr>(FixString(dataStr)).release();
        }
        default: {
            // create string data with value "Invalid data"
            return std::make_unique<DataExpr>("Invalid data").release();
        }
    }
}

DataExpr *Parser::GenerateData(string dataStr, int32_t inputTypes[], int32_t vecCount) const
{
#ifdef DEBUG
    std::cout << "generating data:::" << dataStr << std::endl;
#endif
    // Case with boolean true/false
    if (dataStr == "true") return std::make_unique<DataExpr>(true).release();
    if (dataStr == "false") return std::make_unique<DataExpr>(false).release();

    // Other cases
    DataType currDataType = GetDataType(dataStr, inputTypes, vecCount);
#ifdef DEBUG
    cout << "currDataType: " << currDataType << endl;
#endif
    // Case with normal string format (ex. 'hello')
    if (currDataType == STRINGD && dataStr[0] == '\'' && dataStr[dataStr.size() - 1] == '\'') {
        return std::make_unique<DataExpr>
                (FixString(dataStr.substr(1, dataStr.size() - 1 - 1))).release();
    }
    // Case with column
    if (dataStr[0] == '#') {
        int colIdx = stoi(dataStr.substr(1));
        DataType dt = ColTypeTrans(inputTypes[colIdx]);
        return std::make_unique<DataExpr>(colIdx, dt).release();
    }

    // Case with regular data (int, long, double, string)
    return GenerateDataHelper(dataStr, currDataType);
}
