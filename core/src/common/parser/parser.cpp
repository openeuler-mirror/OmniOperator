
#include "parser.h"
#include <stdio.h>
#include <iostream>
#include <cstring>
#include <stack> // std::stack
#include <algorithm>
#include <typeinfo>
using namespace std;

ComparisionOperator cmpTrans(string op)
{
    if (op == "$operator$EQUAL") return ComparisionOperator::EQ;
    else if (op == "$operator$LESS_THAN") return ComparisionOperator::LT;
    else if (op == "$operator$GREATER_THAN") return ComparisionOperator::GT;
    else if (op == "$operator$LESS_THAN_OR_EQUAL") return ComparisionOperator::LTE;
    else if (op == "$operator$GREATER_THAN_OR_EQUAL") return ComparisionOperator::GTE;
    else if (op == "$operator$NOT_EQUAL") return ComparisionOperator::NEQ;
    else return ComparisionOperator::INVALIDCMP;
}

LogicalOperator logTrans(string op)
{
    if (op == "AND") return LogicalOperator::AND;
    else if (op == "OR") return LogicalOperator::OR;
    else if (op == "NOT" || op == "not") return LogicalOperator::NOT;
    else return LogicalOperator::INVALIDBIN;
}

FnType fnTrans(string fn)
{
    if (fn == "BETWEEN") return FnType::BETWEEN;
    else if (fn == "IN") return FnType::IN;
    else if (fn == "COALESCE") return FnType::COALESCE;
    else if (fn == "substr") return FnType::SUBSTR;
    else if (fn == "CAST") return FnType::CAST;
    else return FnType::INVALIDFN;
}

ArithmeticOperator arithTrans(string op)
{
    if (op == "ADD") return ArithmeticOperator::ADD;
    else if (op == "SUBTRACT") return ArithmeticOperator::SUB;
    else if (op == "MULTIPLY") return ArithmeticOperator::MUL;
    else if (op == "DIVIDE") return ArithmeticOperator::DIV;
    else if (op == "MODULUS") return ArithmeticOperator::MOD;
    else return ArithmeticOperator::INVALIDARITH;
}


bool hasComparisionOperator(string opStr) 
{
    vector<string> allCmpOps {"$operator$LESS_THAN", "$operator$LESS_THAN_OR_EQUAL", "$operator$GREATER_THAN", "$operator$GREATER_THAN_OR_EQUAL", "$operator$EQUAL", "$operator$NOT_EQUAL"};
    for (string cmpOp : allCmpOps) {
        if (opStr.find(cmpOp) != string::npos) return true;
    }
    return false;
}

bool hasFunction(string fnStr)
{
    vector<string> allFns {"BETWEEN", "IN", "COALESCE", "substr", "CAST"};
    for (string fn : allFns) {
        if (fnStr.find(fn) != string::npos) return true;
    }
    return false;
}

bool hasArithmeticOperator(string opStr) 
{
    vector<string> allArithOps {"ADD", "SUBTRACT", "MULTIPLY", "DIVIDE", "MODULUS"};
    for (string arithOp : allArithOps) {
        if (opStr.find(arithOp) != string::npos) return true;
    }
    return false;
}


Expr *Parser::parseRowExpression(string input)
{
    // remove spaces from input
    input.erase(remove(input.begin(), input.end(), ' '), input.end());

    int n = input.length();
    // declaring character array
    char char_array[n + 1];
    char ch;
    // -1 if unary operator, 0 if should be left, 1 if should be right
    int shouldBeRight = 0;

    // copying the contents of the string to char_array
    strcpy(char_array, input.c_str());
    stack<Expr*> exprStack;
    int lastIdx = 0;

    // Most recent expression popped from stack; this is returned at the end
    Expr *recentExpr;

    #ifdef DEBUG
    std::cout << "parseRowExpression: " << input << std::endl;
    std::cout<< "RowExpression length: " << n << std::endl;
    std::cout << "inside parser:::" << std::endl;
    #endif
    

    bool hasCmpOrFn = hasComparisionOperator(input) || hasFunction(input);

    bool hasNoAnd = input.find("AND(") == string::npos;
    bool hasNoOr = input.find("OR(") == string::npos;
    bool hasNoNot = (input.find("NOT(") == string::npos) && (input.find("not(") == string::npos);
    bool hasNoBin = hasNoAnd && hasNoOr && hasNoNot;

    // Special case: Top level expression is an ArithmeticExpr
    if (hasNoBin && !hasCmpOrFn) 
    {
        #ifdef DEBUG
        std::cout << "Top level: ArithmeticExpr" << std::endl;
        #endif

        return generateArithmeticExpr(input);
    }


    // Special case: Top level expression is a ComparisionExpr or a FnExpr
    if (hasNoBin && hasCmpOrFn)
    {
        #ifdef DEBUG
        std::cout << "Top level: ComparisionExpr or FnExpr" << std::endl;
        #endif

        string stripped = input;
        // remove leading spaces, if any
        int firstNonspace = 0;
        while (stripped[firstNonspace] == ' ')
        {
            firstNonspace++;
        }
        stripped = stripped.substr(firstNonspace);

        int firstParen = stripped.find('(');
        string opStr = stripped.substr(0, firstParen);

        #ifdef DEBUG
        std::cout << "opStr:::"<< opStr << std::endl;
        #endif

        string rest = stripped.substr(firstParen);
        string result;
        int i = 0;
        int parenCount = 0;
        // respect parentheses of other expressions
        while(i < rest.size())
        {
            if (rest[i] == '(') parenCount++;
            if (rest[i] == ')') parenCount--;
            if (parenCount == 0) break;
            result.push_back(rest[i]);
            i++;
        }
        result = result.substr(1);


        #ifdef DEBUG
        std::cout << "result:::"<< result  << std::endl;
        #endif

        Expr* ret;
        if (hasComparisionOperator(input)) ret = generateComparisionExpr(result, firstParen+1, i, cmpTrans(opStr));
        else if (hasFunction(input)) ret = generateFnExpr(result, fnTrans(opStr));
        return ret;
    }

    // General case: Top level expression is a BinaryExpr
    #ifdef DEBUG
    std::cout << "Top level: BinaryExpr" << std::endl;
    #endif
    bool isTopLevel = true;
    for (int i = 0; i < n; i++)
    {
        ch = char_array[i];
        

        switch (ch)
        {
        case '(':
        {
            int numbOfChars = i - lastIdx;
            string opStr = input.substr(lastIdx, numbOfChars);
            // Simple logical operators
            if (isTopLevel && (opStr == "AND" || opStr == "OR"))
            {
                BinaryExpr *expr = new BinaryExpr;
                expr->op = logTrans(opStr);

                exprStack.push(expr);
                lastIdx = i + 1;

                isTopLevel = false;
            }
            else if (isTopLevel && (opStr == "NOT" || opStr == "not")) {
                UnaryExpr *expr = new UnaryExpr;
                expr->op = logTrans(opStr);

                exprStack.push(expr);
                lastIdx = i + 1;

                isTopLevel = false;
                shouldBeRight = -1;
            }
            
            else
            {
                // remove leading spaces, if any
                int firstNonspace = 0;
                while (opStr[firstNonspace] == ' ')
                {
                    firstNonspace++;
                }
                opStr = opStr.substr(firstNonspace);
                #ifdef DEBUG
                std::cout << "opStr:::" << opStr << std::endl;
                #endif
                
                Expr *parent = exprStack.top();
                exprStack.pop();

                ExprType parentType = parent->getType();
                if (parentType == UNARY_E) shouldBeRight = -1;


                // Simple logical operators
                if (opStr == "AND" || opStr == "OR")
                {
                    BinaryExpr *newExpr = new BinaryExpr;
                    newExpr->op = logTrans(opStr);

                    if (shouldBeRight == 1)
                    {
                        BinaryExpr *newparent = (BinaryExpr*) parent;
                        newparent->right = newExpr;
                        exprStack.push(newparent);

                        shouldBeRight = 0;
                    }
                    else if (shouldBeRight == 0)
                    {                        
                        BinaryExpr *newparent = (BinaryExpr*) parent;
                        newparent->left = newExpr;
                        exprStack.push(newparent);

                        shouldBeRight = 0;
                    }
                    else {
                        UnaryExpr *newparent = (UnaryExpr*) parent;
                        newparent->exp = newExpr;
                        exprStack.push(newparent);
                        
                        shouldBeRight = 0;
                    }
                    
                    exprStack.push(newExpr);
                    lastIdx = i + 1;
                }

                else if (opStr == "NOT" || opStr == "not") {
                    UnaryExpr *newExpr = new UnaryExpr;
                    newExpr->op = NOT;

                    if (shouldBeRight == 1)
                    {
                        BinaryExpr *newparent = (BinaryExpr*) parent;
                        newparent->right = newExpr;
                        exprStack.push(newparent);

                        shouldBeRight = 0;
                    }
                    else if (shouldBeRight == 0)
                    {                        
                        BinaryExpr *newparent = (BinaryExpr*) parent;
                        newparent->left = newExpr;
                        exprStack.push(newparent);

                        shouldBeRight = 0;
                    }
                    else {
                        UnaryExpr *newparent = (UnaryExpr*) parent;
                        newparent->exp = newExpr;
                        exprStack.push(newparent);
                        
                        shouldBeRight = 0;
                    }
                    
                    exprStack.push(newExpr);
                    lastIdx = i + 1;
                }


                else if (hasComparisionOperator(opStr) || hasFunction(opStr))
                {

                    #ifdef DEBUG
                    std::cout << "operator \"" << opStr << "\" found" << std::endl;
                    #endif

                    
                    ComparisionOperator cmpOp = cmpTrans(opStr);


                    ch = char_array[i];
                    int startIdx = i;
                    string result;
                    int parenCount = 0;
                    // respect parentheses of other expressions
                    while(i < input.size())
                    {
                        if (char_array[i] == '(') parenCount++;
                        if (char_array[i] == ')') parenCount--;
                        if (parenCount == 0) break;
                        result.push_back(char_array[i]);
                        i++;
                    }
                    result = result.substr(1);
    

                    #ifdef DEBUG
                    std::cout << "result:::"<< result  << std::endl;
                    #endif

                    Expr* newExpr;
                    if (hasComparisionOperator(opStr)) newExpr = this->generateComparisionExpr(result, startIdx, i, cmpTrans(opStr));
                    else newExpr = this->generateFnExpr(result, fnTrans(opStr));

                    if (shouldBeRight == 1)
                    {
                        BinaryExpr* newparent = (BinaryExpr*) parent;
                        newparent->right = newExpr;
                        exprStack.push(newparent);
                        shouldBeRight = 0;
                    }
                    else if (shouldBeRight == 0)
                    {
                        BinaryExpr* newparent = (BinaryExpr*) parent;
                        newparent->left = newExpr;
                        exprStack.push(newparent);
                        shouldBeRight = 0;
                    }
                    else
                    {
                        UnaryExpr* newparent = (UnaryExpr*) parent;
                        newparent->exp = newExpr;
                        exprStack.push(newparent);
                    }
                    lastIdx = i+1;
                    
                }
                break;
            }
            break;
        }

        case ')':
        {
            if (!exprStack.empty())
            {
                recentExpr = exprStack.top();
                exprStack.pop();
            }
            break;
        }

        case ' ':
        {
            lastIdx++;
            break;
        }
        case ',':
        {
            shouldBeRight = 1;
            lastIdx = i+1;
            break;
        }
        default:
            break;
        }
    }

    // cout << exprStack.empty() << (recentExpr == nullptr) << endl;
    return recentExpr;
}

// Helper functions for generateComparisionExpr to find the correct column type
DataType getColType(string data) 
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
        return COLUMND;
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
                if (foundDot) return STRINGD;
                foundDot = true;
            }
        }
        else {
            return STRINGD;
        }
    }

    return DOUBLED;
}

Data Parser::generateData(string dataStr)
{
    DataType currDataType = getColType(dataStr);
    Data currData;
    currData.dataType = currDataType;
    if (currDataType == STRINGD && dataStr[0] == '\'' && dataStr[dataStr.size() - 1] == '\'') {
    dataStr = dataStr.substr(1, dataStr.size() - 2);
    }
    switch(currDataType)
    {
        case INT32D: {
	    int v = stoi(dataStr);
            currData.intVal = v;
	    currData.longVal = v;
	    currData.doubleVal = v;
            break; }
        case INT64D:
            currData.longVal = stol(dataStr);
            break;
        case DOUBLED: 
            currData.doubleVal = stod(dataStr);
            break;
        case COLUMND:
            currData.colVal = stoi(dataStr.substr(1));
        case STRINGD: 
            currData.stringVal = dataStr;
            break;
        case INVALIDDATAD:
            currData.stringVal = "Invalid data";
    }
    return currData;
}

// Handles all ComparisionExprs, calls appropriate functions for between and in
Expr* Parser::generateComparisionExpr(string exprStr, int startIdx, int endIdx, ComparisionOperator cmpOp)
{
    #ifdef DEBUG
    std::cout << "exprStr:::"<< exprStr << ":::(" << startIdx << ", " << endIdx<<")"  << std::endl;
    #endif

    int numbOfChars = endIdx - startIdx;
    string cmprStr = exprStr.substr(1);
    int position = cmprStr.find_first_of(",");

    string fieldIdx = cmprStr.substr(0, position);
    string fieldData = cmprStr.substr(position + 1);
    ComparisionExpr *expr = new ComparisionExpr;
    expr->op = cmpOp;
    expr->columnIdx = stoi(fieldIdx);

    #ifdef DEBUG
    std::cout << "columnIdx:::"<< expr->columnIdx  << std::endl;
    #endif

    const auto strBegin = fieldData.find_first_not_of(" ");
    const auto strEnd = fieldData.find_last_not_of(" ");
    const auto strRange = strEnd - strBegin + 1;
    string fdata = fieldData.substr(strBegin, strRange);

    #ifdef DEBUG
    std::cout << "fdata:::" << fdata << std::endl;
    #endif
    
    expr->columnData = generateData(fdata);
    return expr;
}


Expr* Parser::generateFnExpr(string exprStr, FnType fnType)
{
    #ifdef DEBUG
    std::cout << "exprStr:::" << exprStr << std::endl;
    #endif
    // remove spaces from exprStr
    exprStr.erase(remove(exprStr.begin(), exprStr.end(), ' '), exprStr.end());

    vector<Data> arr;

    vector<int> commaPositions;
    int numCommas = 0;
    // ensure that strings and parentheses are respected
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

    // SUBSTR; ex. substr(#1, 1, 2)
    if (fnType == SUBSTR) {
        int colId = stoi(exprStr.substr(1, commaPositions[0]));
        int startId = stoi(exprStr.substr(commaPositions[0] + 1, commaPositions[1] - commaPositions[0] - 1));
        int len = stoi(exprStr.substr(commaPositions[1] + 1, commaPositions[2] - commaPositions[1] - 1));
        #ifdef DEBUG
        std::cout << "substr:::#" << colId << ", " << startId << ", " << len << std::endl;
        #endif
        return new SubstrExpr(colId, startId, len);
    }

    if (fnType == CAST) {
        Data val = generateData(exprStr);
        return new CastExpr(val);
    }



    #ifdef DEBUG
    std::cout << "number of elements in fn parameters:::" << numCommas<< std::endl;
    #endif
    
    for (int i = 1; i <= numCommas; i++) {
        string currVal = exprStr.substr(commaPositions[i-1]+1, commaPositions[i] - commaPositions[i-1] - 1);
        arr.push_back(generateData(currVal));
    }
    if (fnType == IN) {
        return new InExpr(parseRowExpression(exprStr.substr(0, commaPositions[0])), arr);
    }
    if (fnType == BETWEEN && arr.size() == 2) {
        int colId = stoi(exprStr.substr(1, commaPositions[0]));
        return new BetweenExpr(colId, arr[0], arr[1]);
    }
    // COALESCE; ex. COALESCE(#2, #3)
    if (fnType == COALESCE) {
        Data val1 = generateData(exprStr.substr(0, commaPositions[0]));
        arr.insert(arr.begin(), val1);
        return new CoalesceExpr(arr);
    }
    return new BinaryExpr(INVALIDBIN, nullptr, nullptr);
}


// TODO
Expr* Parser::generateArithmeticExpr(string exprStr)
{
    // remove spaces from exprStr
    exprStr.erase(remove(exprStr.begin(), exprStr.end(), ' '), exprStr.end());
    int numbOfChars = exprStr.size();
    #ifdef DEBUG
    std::cout << "exprStr:::"<< exprStr << "; size:::" << numbOfChars << std::endl;
    #endif

    // Number or column
    bool isNumOrCol = !hasArithmeticOperator(exprStr);
    if (isNumOrCol) {
        #ifdef DEBUG
        std::cout << "number or column found:::" << exprStr << std::endl;
        #endif
        
        return new ArithmeticExpr(generateData(exprStr));
    }

    
    // Binary arithmetic expression
    #ifdef DEBUG
    std::cout << "Binary arithmetic expression found" << std::endl;
    #endif
    // Find operator first
    ArithmeticOperator aOp;
    string inner = exprStr;
    for (int i = 0; i < numbOfChars; i++) {
        if (exprStr[i] == '(') {
            aOp = arithTrans(exprStr.substr(0, i));
            inner = exprStr.substr(i+1, numbOfChars - i - 2);
            break;
        }
    }

    #ifdef DEBUG
    std::cout << "arithOp:::" << aOp << std::endl;
    std::cout << "inner:::" << inner << std::endl;
    #endif

    int numInnerChars = inner.size();
    // Ensure that parentheses are respected
    int parenCount = 0;
    for (int i = 0; i < numInnerChars; i++) {
        if (inner[i] == '(') parenCount++;
        if (inner[i] == ')') parenCount--;
        if (inner[i] == ',' && parenCount == 0) {
            // split at comma
            string leftArith = inner.substr(0, i);
            string rightArith = inner.substr(i+1, numInnerChars - i - 1);

            #ifdef DEBUG
            std::cout << "leftArith:::" << leftArith << std::endl;
            std::cout << "rightArith:::" << rightArith << std::endl;
            #endif

            return new ArithmeticExpr(aOp, generateArithmeticExpr(leftArith), generateArithmeticExpr(rightArith));
        }
    }

    return new ArithmeticExpr(INVALIDARITH, nullptr, nullptr);
}
