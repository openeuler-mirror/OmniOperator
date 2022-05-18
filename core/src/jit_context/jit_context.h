/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Jit Context Header File
 */
#ifndef OMNI_RUNTIME_JIT_CONTEXT_H
#define OMNI_RUNTIME_JIT_CONTEXT_H

#include <jit/jit.h>
#include "type/data_types.h"
#include "expression/expressions.h"

namespace omniruntime {
namespace op {
using JitContext = struct JitContext {
    uintptr_t func;
    omniruntime::jit::Jit *jit;
};

JitContext *CreateSortJitContext(omniruntime::type::DataTypes &sourceDataTypes, int32_t *outputCols,
    int32_t outputColsCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts,
    int32_t sortColsCount);

JitContext *CreateSortWithExprJitContext(omniruntime::type::DataTypes &sourceDataTypes, int32_t *outputCols,
    int32_t outputColsCount, const std::vector<omniruntime::expressions::Expr *> &sortKeys, int32_t *sortAscendings,
    int32_t *sortNullFirsts);

JitContext *CreateHashBuilderJitContext(omniruntime::type::DataTypes &buildDataTypes, int32_t *buildHashCols,
    int32_t buildHashColsCount);

JitContext *CreateLookupJoinJitContext(omniruntime::type::DataTypes &probeDataTypes, int32_t probeOutputColsCount,
    int32_t *probeHashCols, int32_t probeHashColsCount, omniruntime::type::DataTypes &buildOutputDataTypes,
    int32_t *buildOutputCols);

JitContext *CreateHashBuilderWithExprJitContext(omniruntime::type::DataTypes &buildDataTypes,
    const std::vector<omniruntime::expressions::Expr *> &buildHashKeys);

JitContext *CreateLookupJoinWithExprJitContext(omniruntime::type::DataTypes &probeDataTypes,
    int32_t probeOutputColsCount, const std::vector<omniruntime::expressions::Expr *> &probeHashKeys,
    omniruntime::type::DataTypes &buildOutputDataTypes, int32_t *buildOutputCols);

JitContext *CreateTopNJitContext(omniruntime::type::DataTypes &sourceDataTypes, int32_t *sortCols,
    int32_t sortColsCount);

JitContext *CreateTopNWithExprJitContext(omniruntime::type::DataTypes &sourceDataTypes,
    const std::vector<omniruntime::expressions::Expr *> &sortKeys);

JitContext *CreateWindowJitContext(omniruntime::type::DataTypes &sourceDataTypes, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount, int32_t *allTypes, int32_t allCount);

JitContext *CreateWindowWithExprJitContext(omniruntime::type::DataTypes &sourceDataTypes, int32_t *outputCols,
    int32_t outputColsCount, int32_t *partitionCols, int32_t partitionCount, int32_t *sortCols, int32_t *sortAscendings,
    int32_t *sortNullFirsts, int32_t sortColsCount, omniruntime::type::DataTypes &outputTypes,
    const std::vector<omniruntime::expressions::Expr *> &argumentKeys);

JitContext *CreateHashAggregationJitContext(omniruntime::type::DataTypes &groupByDataTypes, int32_t aggFuncsCount);

JitContext *CreateHashAggregationWithExprJitContext(const std::vector<omniruntime::expressions::Expr *> &groupByKeys,
    int32_t aggFuncsCount);

JitContext *CreateAggregationJitContext();
}
}
#endif // OMNI_RUNTIME_JIT_CONTEXT_H
