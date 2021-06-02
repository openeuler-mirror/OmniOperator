#ifndef __PARSER_H__
#define __PARSER_H__

#ifndef __EXPRESSION_H__
#include "../expressions.h"
#endif
#include <string>
#include <cstring>

using namespace std;

class Parser
{
public:
    Expr *parseRowExpression(string input);
    Expr *generateComparisionExpr(string exprStr, int startIdx, int endIdx, ComparisionOperator cmpOp);
    // BetweenExpr *generateBetween(string exprStr);
    // InExpr *generateInExpr(string exprStr);
    // CoalesceExpr *generateCoalesceExpr(string exprStr);
    Expr *generateFnExpr(string exprStr, FnType fnType);
};


#endif