/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: project  codegen
 */
#ifndef PROJECTION_CODEGEN_H
#define PROJECTION_CODEGEN_H

#include "llvm_codegen.h"

class ProjectionCodeGen : public LLVMCodeGen {
public:
    ProjectionCodeGen(std::string name, omniruntime::expressions::Expr &expr,
                      std::vector <omniruntime::expressions::DataType> &datatypes, bool filter)
        :LLVMCodeGen(name, expr, datatypes), filter(filter) {}
    ~ProjectionCodeGen() {}
    int64_t GetFunction() override;
    bool IsFilterEnabled() const
    {
        return filter;
    }

private:
    int64_t CreateWrapper(llvm::Function &proj);
    bool filter;
};

#endif