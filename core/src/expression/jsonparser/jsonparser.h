/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __JSONPARSER_H__
#define __JSONPARSER_H__

#include <string>
#include <iostream>

#include <nlohmann/json.hpp>
#include "expression/parserhelper.h"
#include "codegen/func_registry.h"
#include "util/type_util.h"
#include "expression/expressions.h"

class JSONParser {
public:
    static omniruntime::expressions::Expr *ParseJSON(const nlohmann::json &jsonExpr);
    static std::vector<omniruntime::expressions::Expr *> ParseJSON(nlohmann::json expressions[],
        int32_t numberOfExpressions);
    static omniruntime::expressions::Expr *ParseJSON(const std::string &expression);

private:
    static omniruntime::expressions::Expr *ParseJSONFieldRef(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONLiteral(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONBinary(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONUnary(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONIn(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONBetween(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONIf(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONCoalesce(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJsonIsNull(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJsonIsNotNull(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONFunc(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONSwitch(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJSONSwitchGeneral(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *TryGetCastedExpr(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *TryGetSpecialFunction(const nlohmann::json &jsonExpr);
    static omniruntime::expressions::Expr *ParseJsonMultiAndOr(const nlohmann::json &jsonExpr);
};

#endif