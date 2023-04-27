/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: vector batch reader
 */

#ifndef OMNI_RUNTIME_VECTOR_BATCH_READER_H
#define OMNI_RUNTIME_VECTOR_BATCH_READER_H

#include "spill_iterator.h"
#include "spill_tracker.h"

namespace omniruntime {
namespace op {
class VectorBatchReader : public VectorBatchUnitIter {
public:
    VectorBatchReader(int32_t fd, uint64_t fileLength, SpillTracker *tracker)
        : fd(fd), fileLength(fileLength), tracker(tracker)
    {}

    ~VectorBatchReader() override
    {
        tracker->Free(fileLength);
        delete vectorBatchUnit;
        delete[] vecTypeIds;
    }

    ErrorCode ReadFileTailAndHead();

    bool HasNext() override;

    VectorBatchUnit *Next() override;

    int64_t GetRowCount() const
    {
        return totalRowCount;
    }

private:
    std::unique_ptr<omniruntime::vec::BaseVector> ReadVarcharVector(int32_t rowCount);

    template <typename T> std::unique_ptr<omniruntime::vec::BaseVector> ReadVector(int32_t rowCount);

    omniruntime::vec::VectorBatch *ReadVecBatch();

    int32_t fd;
    uint64_t fileLength;
    SpillTracker *tracker;
    int64_t rowOffset = 0;

    // file header
    int32_t vecCount = 0;
    int32_t *vecTypeIds = nullptr;

    // file tail
    int64_t totalRowCount = 0;

    VectorBatchUnit *vectorBatchUnit = new VectorBatchUnit();
};
}
}
#endif // OMNI_RUNTIME_VECTOR_BATCH_READER_H
