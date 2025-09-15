/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: CodegenUtil Class
 */
#ifndef OMNI_RUNTIME_CODEGEN_UTIL_H
#define OMNI_RUNTIME_CODEGEN_UTIL_H

#include <vector>
#include "gtest/gtest.h"
#include "operator/filter/filter_and_project.h"
#include "operator/projection/projection.h"

namespace CodegenUtil {
using namespace omniruntime::expressions;

void GetDataFromVecBatch(omniruntime::vec::VectorBatch &vecBatch, intptr_t valueAddrs[], intptr_t nullAddrs[],
    intptr_t offsetAddrs[], intptr_t dictionaries[], const DataTypes &types);

omniruntime::vec::VectorBatch *FilterAndProject(std::unique_ptr<omniruntime::codegen::Filter> &filter,
    std::vector<std::unique_ptr<omniruntime::codegen::Projection>> &projections, int32_t numCols,
    omniruntime::vec::VectorBatch *vecBatch, int32_t &numSelectedRows, const DataTypes &types);

std::unique_ptr<Filter> GenerateFilterAndProjections(Expr *filterExpr, std::vector<Expr *> &projExprs,
    DataTypes &inputTypes, std::vector<std::unique_ptr<Projection>> &projections, OverflowConfig *overflowConfig);
}

#endif
