/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: CodegenUtil Class
 */
#ifndef OMNI_RUNTIME_CODEGEN_UTIL_H
#define OMNI_RUNTIME_CODEGEN_UTIL_H

#include "gtest/gtest.h"
#include <vector>
#include "operator/filter/filter_and_project.h"

namespace CodegenUtil {
void GetDataFromVecBatch(omniruntime::vec::VectorBatch &vecBatch, int64_t valueAddrs[], int64_t nullAddrs[],
    int64_t offsetAddrs[], int64_t dictionaries[]);

omniruntime::vec::VectorBatch *FilterAndProject(std::unique_ptr<omniruntime::op::Filter> &filter,
    std::vector<std::unique_ptr<omniruntime::op::Projection>> &projections, int32_t numCols,
    omniruntime::vec::VectorBatch *vecBatch, int32_t &numSelectedRows, omniruntime::vec::VectorAllocator *vecAllocator);
}

#endif
