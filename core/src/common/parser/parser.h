/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: parser function
 */
#ifndef __PARSER_H__
#define __PARSER_H__

#include "../parserhelper.h"
#include "../../vector/vector_types.h"

using namespace omniruntime::vec;
class Parser {
public:
    Parser();
    ~Parser();

    omniruntime::expressions::Expr *ParseRowExpression(const std::string &input, VecTypes inputTypes,
        int32_t vecCount);
    omniruntime::expressions::Expr *ParseRowExpressionHelper(std::string opStr,
        std::vector<omniruntime::expressions::Expr *> args);

    static omniruntime::expressions::DataExpr *GenerateData(std::string dataStr, VecTypes inputTypes, int32_t vecCount);
    static omniruntime::expressions::DataExpr *GenerateDataHelper(const std::string &dataStr,
        omniruntime::expressions::DataType currDataType);

private:
    ParserHelper ph;

    // Helper function to strip a string but keep spaces intact inside string literals
    static std::string StripString(const std::string& input);
};


#endif