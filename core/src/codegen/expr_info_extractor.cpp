/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Extract essential information from the expression tree
 */
#include <string>

#include "expr_info_extractor.h"
#include "functions/external_func_registry.h"

using namespace omniruntime::expressions;
using namespace std;

void ExprInfoExtractor::Visit(const DataExpr &e)
{
    if (e.isColumn) {
        this->vectorIndexes.insert(e.colVal);
    }
}

void ExprInfoExtractor::Visit(const BinaryExpr &e)
{
    e.left->Accept(*this);
    e.right->Accept(*this);
}

void ExprInfoExtractor::Visit(const UnaryExpr &e)
{
    e.exp->Accept(*this);
}

void ExprInfoExtractor::Visit(const IfExpr &e)
{
    e.condition->Accept(*this);
    e.trueExpr->Accept(*this);
    e.falseExpr->Accept(*this);
}

void ExprInfoExtractor::Visit(const InExpr &e)
{
    for (auto arg : e.arguments) {
        arg->Accept(*this);
    }
}

void ExprInfoExtractor::Visit(const BetweenExpr &e)
{
    e.value->Accept(*this);
    e.lowerBound->Accept(*this);
    e.upperBound->Accept(*this);
}

void ExprInfoExtractor::Visit(const CoalesceExpr &e)
{
    e.value1->Accept(*this);
    e.value2->Accept(*this);
}

void ExprInfoExtractor::Visit(const IsNullExpr &e)
{
    e.value->Accept(*this);
}

void ExprInfoExtractor::Visit(const FuncExpr &e)
{
    std::string fn = e.funcName;
    ExternalFuncRegistry efr;
    std::set<std::string> externalFuncNames = efr.GetAllExternalFunctionNames();
    if (externalFuncNames.find(e.funcName) == externalFuncNames.end()) {
        for (auto &argument : e.arguments) {
            fn += "_" + DataTypeString(*argument);
        }
        fn += "_" + DataTypeString(e);
    }
    this->functions.insert(fn);
    // Recurse on the arguments
    for (auto arg : e.arguments) {
        arg->Accept(*this);
    }
}

std::set<std::string> ExprInfoExtractor::GetFunctions()
{
    return this->functions;
}

std::set<int32_t> ExprInfoExtractor::GetVectorIndexes()
{
    return this->vectorIndexes;
}
