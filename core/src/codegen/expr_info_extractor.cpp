/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Extract essential information from the expression tree
 */
#include <string>

#include "expr_info_extractor.h"
#include "functions/external_func_registry.h"

using namespace omniruntime::expressions;
using namespace std;

void ExprInfoExtractor::Visit(DataExpr &e)
{
    if (e.isColumn) {
        this->vectorIndexes.insert(e.colVal);
    }
}

void ExprInfoExtractor::Visit(BinaryExpr &e)
{
    e.left->Accept(*this);
    e.right->Accept(*this);
}

void ExprInfoExtractor::Visit(UnaryExpr &e)
{
    e.exp->Accept(*this);
}

void ExprInfoExtractor::Visit(IfExpr &e)
{
    e.condition->Accept(*this);
    e.trueExpr->Accept(*this);
    e.falseExpr->Accept(*this);
}

void ExprInfoExtractor::Visit(InExpr &e)
{
    for (auto arg : e.arguments) {
        arg->Accept(*this);
    }
}

void ExprInfoExtractor::Visit(BetweenExpr &e)
{
    e.value->Accept(*this);
    e.lowerBound->Accept(*this);
    e.upperBound->Accept(*this);
}

void ExprInfoExtractor::Visit(CoalesceExpr &e)
{
    e.value1->Accept(*this);
    e.value2->Accept(*this);
}

void ExprInfoExtractor::Visit(IsNullExpr &e)
{
    e.value->Accept(*this);
}

void ExprInfoExtractor::Visit(FuncExpr &e)
{
    std::string fn = e.funcName;
    ExternalFuncRegistry efr;
    std::set<std::string> externalFuncNames = efr.GetAllExternalFunctionNames();
    if (externalFuncNames.find(e.funcName) == externalFuncNames.end()) {
        for (auto &argument : e.arguments) {
            fn += "_" + DataTypeString(argument->GetExprDataType());
        }
        fn += "_" + DataTypeString(e.GetExprDataType());
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
