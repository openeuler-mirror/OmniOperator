
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

FnType fnTrans(string op)
{
    if (op == "BETWEEN") return FnType::BETWEEN;
    else if (op == "IN") return FnType::IN;
    else if (op == "COALESCE") return FnType::COALESCE;
    else if (op == "substr") return FnType::SUBSTR;
    else return FnType::INVALIDFN;
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
    vector<string> allFns {"BETWEEN", "IN", "COALESCE", "substr"};
    for (string fn : allFns) {
        if (fnStr.find(fn) != string::npos) return true;
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

    // copying the contents of the
    // string to char arrayc
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
    

    // Special case: Top level expression is a ComparisionExpr or a FnExpr
    bool hasCmpOrFn = hasComparisionOperator(input) || hasFunction(input);
    // cout << hasCmp << endl;

    bool hasNoAnd = input.find("AND(") == string::npos;
    bool hasNoOr = input.find("OR(") == string::npos;
    bool hasNoNot = (input.find("NOT(") == string::npos) && (input.find("not(") == string::npos);

    if (hasNoAnd && hasNoOr && hasNoNot && hasCmpOrFn)
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

        string rest = stripped.substr(firstParen+1);
        string result;
        int i = 0;
        while(rest[i] != ')')
        {
            result.push_back(rest[i]);
            i++;
        }
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
                string parentType = typeid(*parent).name();
                #ifdef DEBUG
                std::cout << "parent has type " << parentType<< std::endl;
                #endif
                if (parentType.find("Unary") != string::npos) {
                    shouldBeRight = -1;
                }

                // #ifdef DEBUG
                // // only using AND, OR for now
                // std::cout << "retrieved parent " << ((parent->op == AND) ? "AND" : "OR") << " for opStr \"" << opStr << "\"" << std::endl; 
                // #endif

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


                    i = i + 1;
                    ch = char_array[i];
                    int startIdx = i;
                    string result;
                    while (ch != ')')
                    {
                        result.push_back(ch);
                        i++;
                        ch = char_array[i];
                    }

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
        // Treat as string if the column index doesn't fit in an integer
        try {
            int colVal = stoi(data.substr(1));
            return COLUMND;
        }
        catch (std::out_of_range &e) {
            return STRINGD;
        }
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
        int intVal;
        try {
            intVal = stoi(data);
            return INT32D;
        }
        catch (std::out_of_range &e) {
            return INT64D;
        }
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

// Handles all ComparisionExprs, calls appropriate functions for between and in
Expr* Parser::generateComparisionExpr(string exprStr, int startIdx, int endIdx, ComparisionOperator cmpOp)
{
    #ifdef DEBUG
    std::cout << "exprStr:::"<< exprStr << ":::(" << startIdx << ", " << endIdx<<")"  << std::endl;
    #endif

    // if (cmpOp == BETWEEN)
    // {
    //     return generateBetween(exprStr);
    // }
    // if (cmpOp == IN)
    // {
    //     return generateInExpr(exprStr);
    // }

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
    DataType fdataType = getColType(fdata);
    Data fDataO;
    fDataO.dataType = fdataType;

    #ifdef DEBUG
    std::cout << "fieldData:::"<< fdataType << ":::" << fdata << std::endl;
    #endif

    if (fdataType == STRINGD && fdata[0] == '\'' && fdata[fdata.size() - 1] == '\'') {
        fdata = fdata.substr(1, fdata.size() - 2);
    }

    switch(fdataType)
    {
        case INT32D:
            fDataO.intVal = stoi(fdata);
            break;
        case INT64D:
            fDataO.longVal = stol(fdata);
            break;
        case DOUBLED: 
            fDataO.doubleVal = stod(fdata);
            break;
        case COLUMND:
            fDataO.colVal = stoi(fdata.substr(1));
            break;
        case STRINGD: 
            fDataO.stringVal = fdata;
            break;
        case INVALIDDATAD:
            fDataO.stringVal = "Invalid data";
    }

    expr->columnData = fDataO;
    return expr;
}

// Handles BETWEEN(#x, y, z)
// BetweenExpr* Parser::generateBetween(string exprStr)
// {
//     #ifdef DEBUG
//     std::cout << "exprStr:::"<< exprStr << std::endl;
//     #endif
//     // remove spaces from exprStr
//     exprStr.erase(remove(exprStr.begin(), exprStr.end(), ' '), exprStr.end());

//     int firstComma = exprStr.find_first_of(",");
//     string fieldIdx = exprStr.substr(1, firstComma);
//     #ifdef DEBUG
//     std::cout << "fieldIdx:::"<< fieldIdx  << std::endl;
//     #endif
//     string numbers = exprStr.substr(firstComma + 1);
//     int secondComma = numbers.find_first_of(",");
//     string lowerBound = numbers.substr(0, secondComma);
//     string upperBound = numbers.substr(secondComma + 1);
//     #ifdef DEBUG
//     std::cout << "lowerBound:::"<< lowerBound  << std::endl;
//     std::cout << "upperBound:::"<< upperBound  << std::endl;
//     #endif

//     DataType lbType = getColType(lowerBound);
//     DataType ubType = getColType(upperBound);
//     if (lbType != ubType) {
//         std::cout << "Incompatible types " << lbType << " and " << ubType << std::endl;
//     }
//     if (lbType == STRING && lowerBound[0] == '\'' && lowerBound[lowerBound.size() - 1] == '\'') {
//         lowerBound = lowerBound.substr(1, lowerBound.size() - 2);
//     }
//     if (ubType == STRING && upperBound[0] == '\'' && upperBound[upperBound.size() - 1] == '\'') {
//         upperBound = upperBound.substr(1, upperBound.size() - 2);
//     }
//     // cout << lowerBound << "; " << upperBound;
//     Data lowBound;
//     Data upBound;
//     lowBound.dataType = lbType;
//     upBound.dataType = ubType;
//     switch(lbType)
//     {
//         case INT32:
//             lowBound.intVal = stoi(lowerBound);
//             upBound.intVal = stoi(upperBound);
//             break;
//         case INT64:
//             lowBound.longVal = stol(lowerBound);
//             upBound.longVal = stol(upperBound);
//             break;
//         case DOUBLE: 
//             lowBound.doubleVal = stod(lowerBound);
//             upBound.doubleVal = stod(upperBound);
//             break;
//         default: 
//             lowBound.stringVal = lowerBound;
//             upBound.stringVal = upperBound;
//             break;
//     }

//     return new BetweenExpr(stoi(fieldIdx), lowBound, upBound);

// }

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

    int colId = stoi(exprStr.substr(1, commaPositions[0]));
    
    // COALESCE; ex. COALESCE(#2, #3)
    if (fnType == COALESCE) {
        int colId2 = stoi(exprStr.substr(commaPositions[0] + 2, commaPositions[1] - commaPositions[0] - 1));
        #ifdef DEBUG
        std::cout << "coalesce columns:::" << colId << ", " << colId2 << std::endl;
        #endif
        vector<int> cols;
        cols.push_back(colId);
        cols.push_back(colId2);
        return new CoalesceExpr(cols);
    }

    // SUBSTR; ex. substr(#1, 1, 2)
    if (fnType == SUBSTR) {
        int startId = stoi(exprStr.substr(commaPositions[0] + 1, commaPositions[1] - commaPositions[0] - 1));
        int len = stoi(exprStr.substr(commaPositions[1] + 1, commaPositions[2] - commaPositions[1] - 1));
        #ifdef DEBUG
        std::cout << "substr:::#" << colId << ", " << startId << ", " << len << std::endl;
        #endif
        return new SubstrExpr(colId, startId, len);
    }



    #ifdef DEBUG
    std::cout << "number of elements in fn parameters:::" << numCommas<< std::endl;
    std::cout << "columnIdx:::" << colId << std::endl;
    #endif
    
    for (int i = 1; i <= numCommas; i++) {
        string currVal = exprStr.substr(commaPositions[i-1]+1, commaPositions[i] - commaPositions[i-1] - 1);
        DataType currDataType = getColType(currVal);
        Data currData;
        currData.dataType = currDataType;
        if (currDataType == STRINGD && currVal[0] == '\'' && currVal[currVal.size() - 1] == '\'') {
        currVal = currVal.substr(1, currVal.size() - 2);
        }
        switch(currDataType)
        {
            case INT32D:
                currData.intVal = stoi(currVal);
                break;
            case INT64D:
                currData.longVal = stol(currVal);
                break;
            case DOUBLED: 
                currData.doubleVal = stod(currVal);
                break;
            case COLUMND:
                currData.colVal = stoi(currVal.substr(1));
            case STRINGD: 
                currData.stringVal = currVal;
                break;
            case INVALIDDATAD:
                currData.stringVal = "Invalid data";
        }
        arr.push_back(currData);
    }
    if (fnType == IN) {
        Data col;
        col.dataType = COLUMND;
        col.colVal = colId;
        return new InExpr(col, arr);
    }
    if (fnType == BETWEEN && arr.size() == 2) return new BetweenExpr(colId, arr[0], arr[1]);
    return new BinaryExpr(INVALIDBIN, nullptr, nullptr);
}
