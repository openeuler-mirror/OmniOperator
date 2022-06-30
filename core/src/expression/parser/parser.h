/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: parser function
 */
#ifndef __PARSER_H__
#define __PARSER_H__

#include "expression/parserhelper.h"
#include "type/data_types.h"
#include "codegen/func_registry.h"

enum ParserFormat {
    STRING = 0,
    JSON = 1
};

class Parser {
public:
    Parser();
    ~Parser();

    omniruntime::expressions::Expr *ParseRowExpression(const std::string &input,
        omniruntime::type::DataTypes &inputTypes, int32_t vecCount);

    std::vector<omniruntime::expressions::Expr *> ParseExpressions(const std::string expressions[],
        int32_t numberOfExpressions, omniruntime::type::DataTypes &inputTypes);

    omniruntime::expressions::Expr *ParseRowExpressionHelper(std::string opStr,
        std::vector<omniruntime::expressions::Expr *> args);

    static omniruntime::expressions::FieldExpr *GenerateFieldExpr(std::string fieldStr,
        const omniruntime::type::DataTypes &vecTypePtr);
    static omniruntime::expressions::LiteralExpr *GenerateLiteralExpr(std::string literalStr);
    static omniruntime::expressions::LiteralExpr *GenerateLiteralExprHelper(const std::string &literalStr,
        omniruntime::expressions::DataTypePtr inputType);

private:
    ParserHelper ph;
    // Helper function to strip a string but keep spaces intact inside string literals
    static std::string StripString(const std::string &input);
};


#endif