/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: batch filter expression codegen
 */

#ifndef OMNI_RUNTIME_BATCH_FILTER_CODEGEN_H
#define OMNI_RUNTIME_BATCH_FILTER_CODEGEN_H

#include <utility>
#include "batch_expression_codegen.h"

class BatchFilterCodeGen : public BatchExpressionCodeGen {
public:

    static std::unique_ptr<BatchFilterCodeGen> Create(std::string name, const omniruntime::expressions::Expr &expression);

    ~BatchFilterCodeGen() override = default;

    int64_t GetFunction() override;

private:
    BatchFilterCodeGen(std::string name, const omniruntime::expressions::Expr &expression)
            : BatchExpressionCodeGen(std::move(name), expression)
    {}
    int64_t CreateWrapper(llvm::Function &filter);
};

#endif //OMNI_RUNTIME_BATCH_FILTER_CODEGEN_H
