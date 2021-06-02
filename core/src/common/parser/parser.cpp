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
    if (op == "IN") return ComparisionOperator::IN;
    else if (op == "BETWEEN") return ComparisionOperator::BETWEEN;
    else if (op == "$operator$EQUAL") return ComparisionOperator::EQ;
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

bool hasComparisionOperator(string opStr) 
{
    vector<string> allCmpOps {"$operator$LESS_THAN", "$operator$LESS_THAN_OR_EQUAL", "$operator$GREATER_THAN", "$operator$GREATER_THAN_OR_EQUAL", "$operator$EQUAL", "$operator$NOT_EQUAL", "BETWEEN", "IN"};
    for (string cmpOp : allCmpOps) {
        if (opStr.find(cmpOp) != string::npos) return true;
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
    

    // Special case: Top level expression is a ComparisionExpr
    bool hasCmp = hasComparisionOperator(input);
    // cout << hasCmp << endl;

    bool hasNoAnd = input.find("AND(") == string::npos;
    bool hasNoOr = input.find("OR(") == string::npos;
    bool hasNoNot = (input.find("NOT(") == string::npos) && (input.find("not(") == string::npos);

    if (hasNoAnd && hasNoOr && hasNoNot && hasCmp)
    {
        #ifdef DEBUG
        std::cout << "Top level: ComparisionExpr" << std::endl;
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

        ComparisionOperator cmpOp = cmpTrans(opStr);

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
        Expr *ret = generateComparisionExpr(result, firstParen+1, i, cmpOp);
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


                else if (hasComparisionOperator(opStr))
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

                    Expr *cmpExpr = this->generateComparisionExpr(result, startIdx, i, cmpOp);
                    if (shouldBeRight == 1)
                    {
                        BinaryExpr* newparent = (BinaryExpr*) parent;
                        newparent->right = cmpExpr;
                        exprStack.push(newparent);
                        shouldBeRight = 0;
                    }
                    else if (shouldBeRight == 0)
                    {
                        BinaryExpr* newparent = (BinaryExpr*) parent;
                        newparent->left = cmpExpr;
                        exprStack.push(newparent);
                        shouldBeRight = 0;
                    }
                    else
                    {
                        UnaryExpr* newparent = (UnaryExpr*) parent;
                        newparent->exp = cmpExpr;
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

    return recentExpr;
}

// Handles all ComparisionExprs, calls appropriate functions for between and in
Expr* Parser::generateComparisionExpr(string exprStr, int startIdx, int endIdx, ComparisionOperator cmpOp)
{
    #ifdef DEBUG
    std::cout << "exprStr:::"<< exprStr << ":::(" << startIdx << ", " << endIdx<<")"  << std::endl;
    #endif

    if (cmpOp == BETWEEN)
    {
        return generateBetween(exprStr);
    }
    if (cmpOp == IN)
    {
        return generateInExpr(exprStr);
    }

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
    std::cout << "fieldData:::"<< fdata  << std::endl;
    #endif

    expr->columnData = stoi(fdata);
    return expr;
}

// Handles BETWEEN(#x, y, z)
BetweenExpr* Parser::generateBetween(string exprStr)
{
    #ifdef DEBUG
    std::cout << "exprStr:::"<< exprStr << std::endl;
    #endif
    // remove spaces from exprStr
    exprStr.erase(remove(exprStr.begin(), exprStr.end(), ' '), exprStr.end());

    int firstComma = exprStr.find_first_of(",");
    string fieldIdx = exprStr.substr(1, firstComma);
    #ifdef DEBUG
    std::cout << "fieldIdx:::"<< fieldIdx  << std::endl;
    #endif
    string numbers = exprStr.substr(firstComma + 1);
    int secondComma = numbers.find_first_of(",");
    string lowerBound = numbers.substr(0, secondComma);
    string upperBound = numbers.substr(secondComma + 1);
    #ifdef DEBUG
    std::cout << "lowerBound:::"<< lowerBound  << std::endl;
    std::cout << "upperBound:::"<< upperBound  << std::endl;
    #endif

    return new BetweenExpr(stoi(fieldIdx), stoi(lowerBound), stoi(upperBound));

}

InExpr* Parser::generateInExpr(string exprStr)
{
    #ifdef DEBUG
    std::cout << "exprStr:::" << exprStr << std::endl;
    #endif
    // remove spaces from exprStr
    exprStr.erase(remove(exprStr.begin(), exprStr.end(), ' '), exprStr.end());

    vector<int> arr;

    vector<int> commaPositions;
    int numCommas = 0;
    for (int i = 0; i < exprStr.size(); i++) {
        if (exprStr[i] == ',') {
            commaPositions.push_back(i);
            numCommas++;
        }
    }
    commaPositions.push_back(exprStr.size());
    

    int colId = stoi(exprStr.substr(1, commaPositions[0]));
    #ifdef DEBUG
    std::cout << "number of elements in IN array:::" << numCommas<< std::endl;
    std::cout << "columnIdx:::" << colId << std::endl;
    #endif
    
    for (int i = 1; i <= numCommas; i++) {
        string currVal = exprStr.substr(commaPositions[i-1]+1, commaPositions[i] - commaPositions[i-1] - 1);
        arr.push_back(stoi(currVal));
    }

    return new InExpr(colId, arr);
}