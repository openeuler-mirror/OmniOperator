/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Extract essential information from the expression tree
 */
#include <string>
#include "expr_info_extractor.h"

using namespace omniruntime::expressions;
using namespace std;

void ExprInfoExtractor::PopulateFunctions(const vector<omniruntime::Function>& functionsToPopulate)
{
    for (auto func : functionsToPopulate) {
        this->functions.push_back(&func);
    }
}

void ExprInfoExtractor::Visit(const DataExpr &e)
{
    PopulateFunctions(GetDecimalFunctionRegistry());
    if (e.isColumn) {
        this->vectorIndexes.insert(e.colVal);
        PopulateFunctions(GetDictionaryFunctionRegistry());
    }
}

void ExprInfoExtractor::Visit(const BinaryExpr &e)
{
    if (IsStringDataType(e.left->GetExprDataType())) {
        this->functions.push_back(&GetStringCmpFn().front());
    }
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
    if (IsStringDataType(e.arguments[0]->GetExprDataType())) {
        this->functions.push_back(&GetStringCmpFn().front());
    }
    for (auto arg : e.arguments) {
        arg->Accept(*this);
    }
}

void ExprInfoExtractor::Visit(const BetweenExpr &e)
{
    if (IsStringDataType(e.value->GetExprDataType())) {
        this->functions.push_back(&GetStringCmpFn().front());
    }
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
    this->functions.push_back(e.function);
    // Recurse on the arguments
    for (auto arg : e.arguments) {
        arg->Accept(*this);
    }
}

std::vector<omniruntime::Function*> ExprInfoExtractor::GetFunctions()
{
    return this->functions;
}

std::set<int32_t> ExprInfoExtractor::GetVectorIndexes()
{
    return this->vectorIndexes;
}
