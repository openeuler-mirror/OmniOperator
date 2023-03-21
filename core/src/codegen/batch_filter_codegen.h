/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch filter expression codegen
 */

#ifndef OMNI_RUNTIME_BATCH_FILTER_CODEGEN_H
#define OMNI_RUNTIME_BATCH_FILTER_CODEGEN_H

#include <utility>
#include "batch_expression_codegen.h"

class BatchFilterCodeGen : public BatchExpressionCodeGen {
public:
    static std::unique_ptr<BatchFilterCodeGen> Create(std::string name,
        const omniruntime::expressions::Expr &expression, omniruntime::op::OverflowConfig *overflowConfig);

    ~BatchFilterCodeGen() override = default;

    int64_t GetFunction() override;

private:
    BatchFilterCodeGen(std::string name, const omniruntime::expressions::Expr &expression,
        omniruntime::op::OverflowConfig *overflowConfig)
        : BatchExpressionCodeGen(std::move(name), expression, overflowConfig)
    {}
    int64_t CreateBatchWrapper(llvm::Function &filter);
};

#endif // OMNI_RUNTIME_BATCH_FILTER_CODEGEN_H
