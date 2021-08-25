/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: parser function
 */
#ifndef __PARSER_H__
#define __PARSER_H__

#include "../parserhelper.h"

class Parser {
public:
    Parser();
    ~Parser();

    omniruntime::expressions::Expr *ParseRowExpression(const std::string input, int32_t inputTypes[], int32_t vecCount);
    omniruntime::expressions::Expr *ParseRowExpressionHelper(const std::string opStr,
                                                             std::vector<omniruntime::expressions::Expr *> args);

    omniruntime::expressions::DataExpr* GenerateData(std::string dataStr, int32_t *inputTypes, int32_t vecCount) const;
    omniruntime::expressions::DataExpr *GenerateDataHelper(std::string dataStr,
                                                           omniruntime::expressions::DataType currDataType) const;

private:
    ParserHelper ph;

    // Helper function to strip a string but keep spaces intact inside string literals
    std::string StripString(std::string input) const;
};


#endif