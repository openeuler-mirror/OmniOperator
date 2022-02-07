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
#include "util/type_util.h"

class ParserHelper {
public:
    ParserHelper() : efr(), externalFuncNames(efr.GetAllExternalFunctionNames()),
    externalFuncRetTypeMap(efr.GetFuncReturnTypeMap())
    {
    };
    ~ParserHelper();
    omniruntime::vec::VecType FuncRetTypeMap(std::string fnName,
                                                      std::vector<omniruntime::expressions::Expr *> args);
    bool HasValidArguments(const std::string& fnName, std::vector<omniruntime::expressions::Expr *> args);
    static omniruntime::expressions::DataExpr *GetDefaultValueForType(VecTypeId destTypeId);
    std::string GetFnIdentifier(std::string opStr, std::vector<omniruntime::expressions::Expr *> args,
                                VecTypeId retTypeId);
private:
    ExternalFuncRegistry efr;
    std::set<std::string> externalFuncNames;
    std::map<std::string, omniruntime::vec::VecTypeId> externalFuncRetTypeMap;
    static bool IsIntType(VecTypeId typeId);
};

#endif