/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2027. All rights reserved.
 * Description: parser function
 */
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

enum OperatorReturnType {
    COMPARISON,
    LOGICAL,
    ARITHMETIC,
    INVALIDRETURNTYPE
};


class Parser {
public:
    Parser();
    ~Parser();

    DataType FuncRetTypeMap(std::string fnName, std::vector<Expr*> args);
    bool FuncDeclMatch(std::string fnName, std::vector<Expr*> args, bool checkTypes);

    Expr *ParseRowExpression(std::string input, int32_t *inputTypes, int32_t veccount);
    DataExpr* GenerateData(std::string dataStr, int32_t *inputTypes, int32_t vecCount);

private:
    std::set<std::string> externalFuncNames;
    std::map<std::string, DataType> externalFuncRetTypeMap;
    std::map<std::string, FunctionSignature*> funcNameToSignature;
};


#endif