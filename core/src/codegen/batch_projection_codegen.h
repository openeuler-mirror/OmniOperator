/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: batch projection expression codegen
 */
#ifndef OMNI_RUNTIME_BATCH_PROJECTION_CODEGEN_H
#define OMNI_RUNTIME_BATCH_PROJECTION_CODEGEN_H

#include <utility>

#include "batch_expression_codegen.h"
#include "util/type_util.h"

class BatchProjectionCodeGen : public BatchExpressionCodeGen {
public:
    static std::unique_ptr<BatchProjectionCodeGen> Create(std::string name, const omniruntime::expressions::Expr &expression,
                                                     bool filter);

    ~BatchProjectionCodeGen() override = default;

    int64_t GetFunction() override;

private:
    BatchProjectionCodeGen(std::string name, const omniruntime::expressions::Expr &expr, bool filter)
            : BatchExpressionCodeGen(std::move(name), expr), filter(filter)
    {}
    int64_t CreateWrapper(llvm::Function &proj);
    bool filter;
};
#endif //OMNI_RUNTIME_BATCH_PROJECTION_CODEGEN_H
