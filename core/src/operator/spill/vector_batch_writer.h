/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: vector batch writer
 */

#ifndef OMNI_RUNTIME_VECTOR_BATCH_WRITER_H
#define OMNI_RUNTIME_VECTOR_BATCH_WRITER_H

#include "util/error_code.h"
#include "spill_iterator.h"
#include "vector/vector_batch.h"
#include "spill_tracker.h"
#include "type/data_types.h"

namespace omniruntime {
namespace op {
class VectorBatchWriter {
public:
    VectorBatchWriter(SpillTracker *tracker, const omniruntime::type::DataTypes &sourceTypes);

    ~VectorBatchWriter() {}

    ErrorCode CreateTempFile(const std::string &path);

    ErrorCode WriteVecBatches(VectorBatchUnitIter &vecBatches);

    int32_t GetFd()
    {
        return fd;
    }

    uint64_t GetFileLength() const
    {
        return fileLength;
    }

private:
    ErrorCode WriteFileHeader(omniruntime::vec::VectorBatch *vectorBatch);

    ErrorCode WriteVarcharVector(omniruntime::vec::BaseVector *vector, int32_t rowCount);

    template <typename T> ErrorCode WriteVector(omniruntime::vec::BaseVector *vector, int32_t rowCount);

    ErrorCode WriteVecBatch(omniruntime::vec::VectorBatch *vectorBatch);

    uint64_t GetVecBatchSize(omniruntime::vec::VectorBatch *vectorBatch);

    int32_t fd;
    uint64_t fileLength;
    SpillTracker *tracker;
    omniruntime::type::DataTypes sourceTypes;
};
}
}
#endif // OMNI_RUNTIME_VECTOR_BATCH_WRITER_H
