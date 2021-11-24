
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Extract essential information from the expression tree
 */
#include "../common/expr_visitor.h"
#include <set>

class ExprInfoExtractor : public ExprVisitor {
public:
    void Visit(omniruntime::expressions::DataExpr &e) override;
    void Visit(omniruntime::expressions::UnaryExpr &e) override;        
    void Visit(omniruntime::expressions::BinaryExpr &e) override;
    void Visit(omniruntime::expressions::InExpr &e) override;
    void Visit(omniruntime::expressions::BetweenExpr &e) override;
    void Visit(omniruntime::expressions::IfExpr &e) override;
    void Visit(omniruntime::expressions::CoalesceExpr &e) override;
    void Visit(omniruntime::expressions::IsNullExpr &e) override;
    void Visit(omniruntime::expressions::FuncExpr &e) override;
    std::set<std::string> GetFunctions();
    std::set<int32_t> GetVectorIndexes();
private:
    // all functions used in the expression
    std::set<std::string> functions;
    std::set<int32_t> vectorIndexes;
};
