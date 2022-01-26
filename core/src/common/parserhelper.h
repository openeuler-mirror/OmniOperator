/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */
#ifndef __PARSERHELPER_H__
#define __PARSERHELPER_H__

#ifndef __EXPRESSION_H__
#endif
#include "codegen/external_func_registry.h"
#include <string>
#include <set>
#include <iostream>
#include <algorithm>

class ParserHelper {
public:
    ParserHelper() : efr(), externalFuncNames(efr.GetAllExternalFunctionNames()),
    externalFuncRetTypeMap(efr.GetFuncReturnTypeMap())
    {
    };
    ~ParserHelper();
    omniruntime::expressions::DataType FuncRetTypeMap(std::string fnName,
                                                      std::vector<omniruntime::expressions::Expr *> args);
    bool HasValidArguments(const std::string& fnName, std::vector<omniruntime::expressions::Expr *> args);
    static omniruntime::expressions::DataExpr *GetDefaultValueForType(omniruntime::expressions::DataType destType);
    std::string GetFnIdentifier(std::string opStr, std::vector<omniruntime::expressions::Expr *> args,
                            omniruntime::expressions::DataType ret);
private:
    ExternalFuncRegistry efr;
    std::set<std::string> externalFuncNames;
    std::map<std::string, omniruntime::expressions::DataType> externalFuncRetTypeMap;
    static bool IsIntType(omniruntime::expressions::DataType dt);
};

#endif