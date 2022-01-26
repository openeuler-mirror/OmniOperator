
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Extract essential information from the expression tree
 */
#include "../common/expr_visitor.h"
#include "func_registry_string.h"
#include "func_registry_dictionary.h"
#include "func_registry_decimal.h"
#include "func_registry_context.h"
#include "function.h"

class ExprInfoExtractor : public ExprVisitor {
public:
    void Visit(const omniruntime::expressions::DataExpr &e) override;
    void Visit(const omniruntime::expressions::UnaryExpr &e) override;
    void Visit(const omniruntime::expressions::BinaryExpr &e) override;
    void Visit(const omniruntime::expressions::InExpr &e) override;
    void Visit(const omniruntime::expressions::BetweenExpr &e) override;
    void Visit(const omniruntime::expressions::IfExpr &e) override;
    void Visit(const omniruntime::expressions::CoalesceExpr &e) override;
    void Visit(const omniruntime::expressions::IsNullExpr &e) override;
    void Visit(const omniruntime::expressions::FuncExpr &e) override;
    void PopulateFunctions(const std::vector<omniruntime::Function>& functionsToPopulate);
    std::vector<omniruntime::Function*> GetFunctions();
    std::set<int32_t> GetVectorIndexes();
private:
    std::vector<omniruntime::Function*> functions;
    std::set<int32_t> vectorIndexes;
};
