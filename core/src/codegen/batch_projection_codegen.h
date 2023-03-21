/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch projection expression codegen
 */
#ifndef OMNI_RUNTIME_BATCH_PROJECTION_CODEGEN_H
#define OMNI_RUNTIME_BATCH_PROJECTION_CODEGEN_H

#include <utility>

#include "batch_expression_codegen.h"

namespace omniruntime {
namespace codegen {
class BatchProjectionCodeGen : public BatchExpressionCodeGen {
public:
    BatchProjectionCodeGen(std::string name, const omniruntime::expressions::Expr &expr, bool filter,
        omniruntime::op::OverflowConfig *overflowConfig)
        : BatchExpressionCodeGen(std::move(name), expr, overflowConfig), filter(filter)
    {}

    ~BatchProjectionCodeGen() override = default;

    intptr_t GetFunction() override;

private:
    intptr_t CreateBatchWrapper(llvm::Function &projFunc);

    bool filter;
};
}
}
#endif // OMNI_RUNTIME_BATCH_PROJECTION_CODEGEN_H
