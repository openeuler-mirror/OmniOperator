#ifndef __PARSER_H__
#define __PARSER_H__

#include "../common/expressions.h"
#include <cstring>

using namespace std;

class Parser
{
public:
    Expr parseRowExpression(string input);
    ComparisionExpr generateComparisionExpr(string exprStr, int startIdx, int endIdx);
};

int simpleTest();

#endif