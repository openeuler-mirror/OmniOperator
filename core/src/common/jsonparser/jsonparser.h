/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __JSONPARSER_H__
#define __JSONPARSER_H__

// Contains expressions.h
#include "../expressions.h"

#include <string>
#include <iostream>

#include <nlohmann/json.hpp>
#include <common/parserhelper.h>
#include <codegen/func_registry.h>

class JSONParser {
public:
    static omniruntime::expressions::Expr *ParseJSON(nlohmann::json jsonExpr);
    static std::vector<omniruntime::expressions::Expr *> ParseJSON(nlohmann::json expressions[],
                                                                   int32_t numberOfExpressions);

private:
    static omniruntime::expressions::Expr *ParseJSONFieldRef(nlohmann::json jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONLiteral(nlohmann::json jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONBinary(nlohmann::json jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONUnary(nlohmann::json jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONIn(nlohmann::json jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONBetween(nlohmann::json jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONIf(nlohmann::json jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONCoalesce(nlohmann::json jsonExpr);
    static omniruntime::expressions::Expr *ParseJsonIsNull(nlohmann::json jsonExpr);
    static omniruntime::expressions::Expr *ParseJsonIsNotNull(nlohmann::json jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONFunc(nlohmann::json jsonExpr);

    static omniruntime::expressions::OperatorType GetOperatorType(omniruntime::expressions::Operator op);

};


#endif