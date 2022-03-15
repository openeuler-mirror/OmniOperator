/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Jit Context Header File
 */
#ifndef OMNI_RUNTIME_JIT_CONTEXT_H
#define OMNI_RUNTIME_JIT_CONTEXT_H

#include "vector/vector_types.h"
#include "expression/expressions.h"

using JitContext = struct JitContext {
    uintptr_t func;
};

JitContext *CreateSortJitContext(omniruntime::vec::VecTypes &sourceVecTypes, int32_t *outputCols,
    int32_t outputColsCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortColsCount);

JitContext *CreateSortWithExprJitContext(omniruntime::vec::VecTypes &sourceVecTypes, int32_t *outputCols,
    int32_t outputColsCount, const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings,
    int32_t *sortNullFirsts);

JitContext *CreateHashBuilderJitContext(omniruntime::vec::VecTypes &buildVecTypes, int32_t *buildHashCols,
    int32_t buildHashColsCount, int32_t operatorCount);

JitContext *CreateLookupJoinJitContext(omniruntime::vec::VecTypes &probeVecTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, int32_t *probeHashCols, int32_t probeHashColsCount,
    omniruntime::vec::VecTypes &buildOutputVecTypes, int32_t *buildOutputCols);

JitContext *CreateHashBuilderWithExprJitContext(omniruntime::vec::VecTypes &buildVecTypes,
    const std::vector<omniruntime::expressions::Expr *> &buildHashKeys, int32_t operatorCount);

JitContext *CreateLookupJoinWithExprJitContext(omniruntime::vec::VecTypes &probeVecTypes, int32_t *probeOutputCols,
    int32_t probeOutputColsCount, const std::vector<omniruntime::expressions::Expr *> &probeHashKeys,
    omniruntime::vec::VecTypes &buildOutputVecTypes, int32_t *buildOutputCols);

JitContext *CreateTopNJitContext(omniruntime::vec::VecTypes &sourceVecTypes, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount);

JitContext *CreateTopNWithExprJitContext(omniruntime::vec::VecTypes &sourceVecTypes,
    const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings, int32_t *sortNullFirsts);

JitContext *CreateWindowJitContext(omniruntime::vec::VecTypes &sourceVecTypes, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount, int32_t *allTypes, int32_t allCount);

JitContext *CreateWindowWithExprJitContext(omniruntime::vec::VecTypes &sourceVecTypes, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount, omniruntime::vec::VecTypes &outputTypes,
    const std::vector<omniruntime::expressions::Expr *> &argumentKeys);

JitContext *CreateHashAggregationJitContext(omniruntime::vec::VecTypes &groupByVecTypes, int32_t *groupByCols,
    omniruntime::vec::VecTypes &aggVecTypes, int32_t *aggCols, int32_t *aggFuncTypes, int32_t aggFuncsCount,
    omniruntime::vec::VecTypes &outputVecTypes);

JitContext *CreateHashAggregationWithExprJitContext(omniruntime::vec::VecTypes &sourceVecTypes,
    const std::vector<omniruntime::expressions::Expr *> &groupByKeys,
    const std::vector<omniruntime::expressions::Expr *> &aggKeys, int32_t *aggFuncTypes, int32_t aggFuncsCount,
    omniruntime::vec::VecTypes &outputVecTypes);

JitContext *CreateAggregationJitContext(omniruntime::vec::VecTypes &sourceVecTypes, int32_t *aggCols,
    int32_t *aggMaskCols, int32_t *aggFuncTypes, int32_t aggFuncsCount, omniruntime::vec::VecTypes &outputVecTypes);

#endif // OMNI_RUNTIME_JIT_CONTEXT_H
