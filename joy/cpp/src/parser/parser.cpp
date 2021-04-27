
#include "parser.h"
#include "../common/expressions.h"
#include <stdio.h>
#include <iostream>
#include <cstring>
#include <stack> // std::stack
using namespace std;

Expr Parser::parseRowExpression(string input)
{
    std::cout << "parseRowExpression" << std::endl;
    int n = input.length();
    BinaryExpr root;
    // declaring character array
    char char_array[n + 1];
    bool shouldBeRight = false;
    cout<<n;
    // copying the contents of the
    // string to char arrayc
    strcpy(char_array, input.c_str());
    stack<BinaryExpr> exprStack;
    std::cout << "inside parser:::" << std::endl;
    int lastIdx = 0;
    for (int i = 0; i < n; i++)
    {
        char ch = char_array[i];

        switch (ch)
        {
        case '(':
        {
            int numbOfChars = i - lastIdx;
            string opStr = input.substr(lastIdx, numbOfChars);
            std::cout << "opStr:::"<< opStr  << std::endl;
            if (opStr == "AND")
            {
                BinaryExpr expr;
                expr.op = LogicalOperator::AND;
                root = expr;
                exprStack.push(expr);
                lastIdx = i + 1;
            }
            else if (opStr.find("operator") > 0)
            {
                string op = opStr.substr(10);
                std::cout << "op:::"<< op  << std::endl;
                if (op == "EQ" || op == "LT" || op == "GT" || op == "LTE" || op == "GTE")
                {
                    i = i + 1;
                    ch = char_array[i];
                    int startIdx = i;
                    while (ch != ')')
                    {
                        i++;
                        ch = char_array[i];
                    }
                    int numbOfChars = i - startIdx;
                    string result = input.substr(startIdx, numbOfChars);
                    std::cout << "result="<< result  << std::endl;
                    ComparisionExpr expr = this->generateComparisionExpr(result, startIdx, i);
                    return expr;
                }
            }
            else
            {
                int numbOfChars = i - lastIdx;
                string opStr = opStr.substr(lastIdx, numbOfChars);
                std::cout << "opStr:::"<< opStr  << std::endl;
                exprStack.pop();
                BinaryExpr parent = (BinaryExpr)exprStack.top();
                if (opStr == "AND")
                {
                    if (shouldBeRight)
                    {
                        BinaryExpr newExpr;

                        parent.right = newExpr;
                        exprStack.push(parent);
                        exprStack.push(newExpr);
                        shouldBeRight = false;
                    }
                    else
                    {
                        BinaryExpr newExpr;

                        parent.left = newExpr;
                        exprStack.push(parent);
                        exprStack.push(newExpr);
                        shouldBeRight = false;
                    }
                }
                else if (opStr.find("operator") > 0)
                {
                    string op = opStr.substr(10);
                    if (op == "EQ" || op == "LT" || op == "GT" || op == "LTE" || op == "GTE")
                    {
                        i = i + 1;
                        ch = char_array[i];
                        int startIdx = i;
                        string result;
                        while (ch != ')')
                        {
                            result = result.append("" + ch);
                            i++;
                            ch = char_array[i];
                        }
                        std::cout << "result:::"<< result  << std::endl;
                        ComparisionExpr cmpExpr = this->generateComparisionExpr(result, startIdx, i);
                        if (shouldBeRight)
                        {
                            parent.right = cmpExpr;
                            exprStack.push(parent);
                            shouldBeRight = false;
                        }
                        else
                        {
                            parent.left = cmpExpr;
                            exprStack.push(parent);
                            shouldBeRight = false;
                        }
                    }
                }
                break;
            }
        }

        case ')':
        {
            if (!exprStack.empty())
            {
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
            shouldBeRight = true;
            break;
        }
        default:
            break;
        }
    }
    return root;
}

ComparisionExpr Parser::generateComparisionExpr(string exprStr, int startIdx, int endIdx)
{
    std::cout << "exprStr:::"<< exprStr << ":::(" << startIdx << ", " << endIdx<<")"  << std::endl;
    int numbOfChars = endIdx - startIdx;
    string cmprStr = exprStr.substr(1);
    std::cout << "cmprStr:::"<< cmprStr  << std::endl;
    int position = cmprStr.find_first_of(",");

    string fieldIdx = cmprStr.substr(0, position);
    string fieldData = cmprStr.substr(position + 1);
    ComparisionExpr expr;
    expr.op = ComparisionOperator::EQ;
    expr.columnIdx = stoi(fieldIdx);
    std::cout << "columnIdx:::"<< expr.columnIdx  << std::endl;
    const auto strBegin = fieldData.find_first_not_of(" ");

    const auto strEnd = fieldData.find_last_not_of(" ");
    const auto strRange = strEnd - strBegin + 1;
    string fdata = fieldData.substr(strBegin, strRange);
    std::cout << "fieldData:::"<< fdata  << std::endl;
    expr.columnData = stoi(fdata);
    return expr;
}

int simpleTest()
{
    Parser parserObj;

    parserObj.parseRowExpression("AND(AND($operator$GT(#3, 8766), $operator$LT(#3, 9131)), AND(BETWEEN(#2, 0.05, 0.07), $operator$LT(#0, 24.0)))");
    return 0;
}