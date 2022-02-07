/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: parser function
 */
#ifndef __PARSER_H__
#define __PARSER_H__

#include "../parserhelper.h"
#include "../../vector/vector_types.h"
#include "../../codegen/func_registry.h"

using namespace omniruntime::vec;

enum ParserFormat {
    STRING = 0,
    JSON = 1
};

class Parser {
public:
    Parser();
    ~Parser();

    omniruntime::expressions::Expr *ParseRowExpression(const std::string &input, VecTypes inputTypes, int32_t vecCount);

    std::vector<omniruntime::expressions::Expr *> ParseExpressions(const std::string expressions[],
        int32_t numberOfExpressions, VecTypes inputTypes);

    omniruntime::expressions::Expr *ParseRowExpressionHelper(std::string opStr,
        std::vector<omniruntime::expressions::Expr *> args);

    static omniruntime::expressions::DataExpr *GenerateData(std::string dataStr, VecTypes& inputTypes);
    static omniruntime::expressions::DataExpr *GenerateDataHelper(const std::string &dataStr,
        omniruntime::expressions::VecTypePtr inputType );
private:
    ParserHelper ph;
    // Helper function to strip a string but keep spaces intact inside string literals
    static std::string StripString(const std::string &input);
};


#endif