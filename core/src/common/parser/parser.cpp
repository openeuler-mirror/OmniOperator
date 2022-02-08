/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: parser function
 */
#include <iostream>
#include "parser.h"

using namespace std;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;


Parser::Parser() {}

Parser::~Parser() {}

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

OperatorType GetBinaryOperatorType(string opStr)
{
    vector<string> allCmpOps { "LESS_THAN", "LESS_THAN_OR_EQUAL", "GREATER_THAN", "GREATER_THAN_OR_EQUAL",
        "EQUAL",     "NOT_EQUAL" };
    vector<string> allLogOps { "AND", "OR" };
    vector<string> allArithOps { "ADD", "SUBTRACT", "MULTIPLY", "DIVIDE", "MODULUS" };
    for (const string &cmpOp : allCmpOps) {
        if (opStr == cmpOp) {
            return OperatorType::COMPARISON;
        }
    }
    for (const string &logOp : allLogOps) {
        if (opStr == logOp) {
            return OperatorType::LOGICAL;
        }
    }
    for (const string &arithOp : allArithOps) {
        if (opStr == arithOp) {
            return OperatorType::ARITHMETIC;
        }
    }
    return OperatorType::INVALIDOPTTYPE;
}

bool IsUnaryOperator(const string &opStr)
{
    vector<string> allUnaryOps { "NOT", "not" };
    for (const string &unaryOp : allUnaryOps) {
        if (opStr == unaryOp) {
            return true;
        }
    }
    return false;
}

string Parser::StripString(const string &input)
{
    // remove spaces from input but not from inside strings
    string newInput;
    bool isInString = false;
    for (char i : input) {
        if (i == '\'') {
            isInString = !isInString;
            newInput.push_back(i);
        } else if (i == ' ') {
            if (isInString) {
                newInput.push_back(i);
            }
        } else {
            newInput.push_back(i);
        }
    }
    return newInput;
}

DataType ParseReturnType(const string &typeString)
{
    int endIdx = 2;
    int widthIdx = typeString.find('[');
    if (widthIdx != string::npos) {
        if (stoi(typeString.substr(0, endIdx)) == CHARD) {
            return CHARD;
        }
    }
    if (typeString.find_first_not_of("0123456789") == string::npos && stoi(typeString) < INT32_MAX) {
        int typeOrdinal = stoi(typeString);
        return OrdinalToDataType(typeOrdinal);
    }
    LogError("Invalid return type: %s", typeString.c_str());
    return INVALIDDATAD;
}

std::vector<omniruntime::expressions::Expr *> Parser::ParseExpressions(const string expressions[],
    int32_t numberOfExpressions, VecTypes inputTypes)
{
    std::vector<Expr *> vExprs;
    for (int32_t i = 0; i < numberOfExpressions; i++) {
        Expr *expr = ParseRowExpression(expressions[i], inputTypes, inputTypes.GetSize());
        if (expr == nullptr)
            return { nullptr };
        vExprs.push_back(expr);
    }
    return vExprs;
}

Expr *Parser::ParseRowExpression(const string &inputStr, VecTypes inputTypes, int32_t vecCount)
{
    string input = this->StripString(inputStr);
    int firstParenInd = input.find('(');
    // Check if it is just data (i.e. 123, #4, 34.4)
    if (firstParenInd == string::npos) {
        return GenerateData(input, inputTypes);
    }

    // demangled operator string
    string opStr = DemangleOperator(input.substr(0, firstParenInd));
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
        if (exprStr[i] == '\'') {
            outsideQuotes = !outsideQuotes;
        }
        if (exprStr[i] == '(') {
            parenCount++;
        }
        if (exprStr[i] == ')') {
            parenCount--;
        }
    }
    commaPositions.push_back(exprStr.size());

    // Place all of the arguments into a vector first
    vector<Expr *> args;
    auto expr = ParseRowExpression(exprStr.substr(0, commaPositions[0]), inputTypes, vecCount);
    if (expr == nullptr) {
        return nullptr;
    }
    args.push_back(expr);
    for (int i = 1; i <= numCommas; i++) {
        string currVal = exprStr.substr(commaPositions[i - 1] + 1, commaPositions[i] - commaPositions[i - 1] - 1);
        expr = ParseRowExpression(currVal, inputTypes, vecCount);
        if (expr == nullptr) {
            return nullptr;
        }
        args.push_back(expr);
    }

    return ParseRowExpressionHelper(opStr, args);
}

Expr *Parser::ParseRowExpressionHelper(string opStr, vector<Expr *> args)
{
    int typeIdx = opStr.find(':');
    int stepSize = 4;
    int32_t width = INT32_MAX;
    DataType type;
    if (typeIdx != string::npos) {
        type = ParseReturnType(opStr.substr(typeIdx + 1));
        if (type == CHARD) {
            width = stoi(opStr.substr(typeIdx + stepSize, opStr.size() - typeIdx - stepSize));
        }
        opStr = opStr.substr(0, typeIdx);
    }

    // BinaryExpr
    OperatorType binRetType = GetBinaryOperatorType(opStr);
    if (binRetType != OperatorType::INVALIDOPTTYPE && args.size() == ARG2) {
        return std::make_unique<BinaryExpr>(StringToOperator(DemangleOperator(opStr)), args[0], args[1], type)
            .release();
    }

    // UnaryExpr
    // only handling NOT for now
    if (IsUnaryOperator(opStr) && args.size() == 1) {
        return std::make_unique<UnaryExpr>(StringToOperator(DemangleOperator(opStr)), args[0], type).release();
    }

    // Special form
    // Special forms are IN, BETWEEN, IF, COALESCE
    if (opStr == "BETWEEN")
        return std::make_unique<BetweenExpr>(args[0], args[1], args[ARG2]).release();
    if (opStr == "IN")
        return std::make_unique<InExpr>(args).release();
    if (opStr == "COALESCE")
        return std::make_unique<CoalesceExpr>(args[0], args[1]).release();
    if (opStr == "IF") {
        if ((args[ARG2]->GetExprDataType() == VARCHARD || args[ARG2]->GetExprDataType() == CHARD) &&
            args[ARG2]->GetType() == ExprType::DATA_E &&
            static_cast<DataExpr *>(args[ARG2])->stringVal->compare("null") == 0) {
            return std::make_unique<IfExpr>(args[0], args[1], ParserHelper::GetDefaultValueForType(args[1]->dataType))
                .release();
        }
        return std::make_unique<IfExpr>(args[0], args[1], args[ARG2]).release();
    }
    if (opStr == "IS_NULL")
        return std::make_unique<IsNullExpr>(args[0]).release();
    if (opStr == "IS_NOT_NULL") {
        auto isNullExpr = std::make_unique<IsNullExpr>(args[0]).release();
        return std::make_unique<UnaryExpr>(Operator::NOT, isNullExpr, type).release();
    }
    // When casting to the same type, the result is the argument itself
    // Treat argument as constant DataExpr instead of returning FuncExpr
    if (opStr == "CAST" && args.size() == 1 && (type == args[0]->dataType))
        return static_cast<DataExpr *>(args[0]);

    // Function
    // Check that the signature matches
    std::string funcID = ph.GetFnIdentifier(opStr, args, type);
    if (!funcID.empty()) {
        auto function = FunctionRegistry::LookupFunction(funcID);
        if (function != nullptr)
            return make_unique<FuncExpr>(opStr, args, type, width, *function).release();
    }
#ifdef DEBUG
    cout << "operator is not supported" << endl;
#endif
    return nullptr;
}

// Helper function to turn all % to .* for regex wildcard matching
string *FixString(const string &dataStr)
{
    string *fixedStr = std::make_unique<string>("").release();
    for (char i : dataStr) {
        if (i == '%') {
            fixedStr->push_back('.');
            fixedStr->push_back('*');
        } else {
            fixedStr->push_back(i);
        }
    }
    return fixedStr;
}

DataExpr *Parser::GenerateDataHelper(const string &dataStr, DataType currDataType)
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
        // Need to handle decimals properly
        case DECIMAL128D:
        case INT64D: {
            return std::make_unique<DataExpr>(stol(dataStr)).release();
        }
        case DOUBLED: {
            return std::make_unique<DataExpr>(stod(dataStr)).release();
        }
        case CHARD:
        case VARCHARD: {
            return std::make_unique<DataExpr>(FixString(dataStr)).release();
        }
        case UNKNOWND: {
            return std::make_unique<DataExpr>(0).release();
        }
        default: {
            LogError("type %u is not supported", currDataType);
            return nullptr;
        }
    }
}

DataExpr *Parser::GenerateData(string dataStr, const VecTypes &inputTypes)
{
    // Case with column
    if (dataStr[0] == '#') {
        int colIdx = stoi(dataStr.substr(1));
        DataType dt = ColTypeTrans(inputTypes.GetIds()[colIdx]);
        return std::make_unique<DataExpr>(colIdx, dt, inputTypes.Get().at(colIdx).GetWidth()).release();
    }

    int typeIdx = dataStr.find(':');
    int stepSize = 4;
    int32_t width = INT32_MAX;
    DataType currDataType;
    if (typeIdx != string::npos) {
        currDataType = ParseReturnType(dataStr.substr(typeIdx + 1));
        if (currDataType == CHARD) {
            width = stoi(dataStr.substr(typeIdx + stepSize, dataStr.size() - typeIdx - stepSize));
        }
        dataStr = dataStr.substr(0, typeIdx);
    } else {
        LogError("Unknown constant type for expr: %s", dataStr.c_str());
        return nullptr;
    }

    // Case with boolean true/false
    if (dataStr == "true")
        return std::make_unique<DataExpr>(true).release();
    if (dataStr == "false")
        return std::make_unique<DataExpr>(false).release();

    // trim the single quotes for string values if there is any
    if (IsStringDataType(currDataType) && dataStr[0] == '\'' && dataStr[dataStr.size() - 1] == '\'') {
        dataStr = dataStr.substr(1, dataStr.size() - 1 - 1);
    }

    // case with null constants
    if (IsNullLiteral(dataStr)) {
        auto expr = ParserHelper::GetDefaultValueForType(currDataType);
        expr->isNull = true;
        return expr;
    }

    // Case with regular data (int, long, double, string ...)
    return GenerateDataHelper(dataStr, currDataType);
}
