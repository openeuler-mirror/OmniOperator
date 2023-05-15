/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_UTIL_H
#define OMNI_RUNTIME_VECTOR_UTIL_H

#include "vector/vector_batch.h"
#include "vector/vector_helper.h"
#include "util/type_util.h"
#include "common.h"

namespace om_benchmark {
omniruntime::vec::VectorBatch *CreateSequenceVectorBatch(const std::vector<omniruntime::type::DataTypePtr> &types,
    int length);

omniruntime::vec::VectorBatch *CreateSequenceVectorBatchWithDictionaryVector(
    const std::vector<omniruntime::type::DataTypePtr> &types, int length);

omniruntime::vec::VectorBatch *CreateVectorBatch(uint32_t encoding,
    const std::vector<omniruntime::type::DataTypePtr> &types, std::string &prefix,
    const std::vector<std::vector<int32_t>> &values, int rowCount);

std::vector<VectorBatchSupplier> VectorBatchToVectorBatchSupplier(
    std::vector<omniruntime::vec::VectorBatch *> vectorBatches);

void SetVectorBatchRow(omniruntime::vec::VectorBatch *vb, std::vector<omniruntime::type::DataTypeId> dataTypes,
    int index, std::vector<std::string> value);
}
#endif // OMNI_RUNTIME_VECTOR_UTIL_H
