#ifndef __PARSER_H__
#define __PARSER_H__

#ifndef __EXPRESSION_H__
#include "../expressions.h"
#endif
#include "../../codegen/func_signature.h"
#include "../../codegen/functions/externalfunctions.h"
#include "../../codegen/functions/external_func_registry.h"
#include <string>
#include <cstring>
#include <set>

using namespace std;
using namespace omniruntime::expressions;

enum OperatorReturnType {
    COMPARISON, 
    LOGICAL, 
    ARITHMETIC, 
    INVALIDRETURNTYPE
};


class Parser
{
public:
    Parser();
    ~Parser();

    DataType funcRetTypeMap(string fnName, vector<Expr*> args);
    bool funcDeclMatch(string fnName, vector<Expr*> args, bool checkTypes);

    Expr *parseRowExpression(string input, int32_t *inputTypes, int32_t veccount);
    DataExpr* generateData(string dataStr, int32_t *inputTypes, int32_t vecCount);

private:
    set<string> externalFuncNames;
    map<string, DataType> externalFuncRetTypeMap;
    map<string, FunctionSignature*> funcNameToSignature;
};


#endif