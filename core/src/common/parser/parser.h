#ifndef __PARSER_H__
#define __PARSER_H__

#ifndef __EXPRESSION_H__
#include "../expressions.h"
#endif
#include <string>
#include <cstring>

using namespace std;
using namespace expressions;

enum OperatorReturnType {
    COMPARISON, 
    LOGICAL, 
    ARITHMETIC, 
    INVALIDRETURNTYPE
};



class Parser
{
public:
    Expr *parseRowExpression(string input, int32_t *inputTypes, int32_t veccount);
    DataExpr* generateData(string dataStr, int32_t *inputTypes, int32_t vecCount);
};


#endif