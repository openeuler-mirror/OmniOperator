/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: dynamic pages index implementations
 */
#ifndef __DYNAMIC_PAGES_INDEX_H__
#define __DYNAMIC_PAGES_INDEX_H__

#include <cstdint>
#include <vector>
#include "type/data_type.h"
#include "type/data_types.h"
#include "vector/vector_batch.h"

namespace omniruntime {
namespace op {
class DynamicPagesIndex {
public:
    explicit DynamicPagesIndex(const omniruntime::type::DataTypes &types, int32_t *computeCols,
        int32_t computeColsCount);

    ~DynamicPagesIndex();

    int32_t AddVecBatch(omniruntime::vec::VectorBatch *vecBatch);

    ALWAYS_INLINE int32_t GetTypesCount() const
    {
        return typesCount;
    }

    ALWAYS_INLINE int32_t GetPositionCount() const
    {
        return this->positionCount;
    }

    ALWAYS_INLINE bool IsDataFinish() const
    {
        return this->finishAddData;
    }

    ALWAYS_INLINE bool IsDataFinish(int32_t rowIndex) const
    {
        return this->finishAddData && (rowIndex >= this->positionCount - 1);
    }

    ALWAYS_INLINE bool IsEmptyBatch() const
    {
        return this->finishAddData && this->positionCount == 0;
    }

    ALWAYS_INLINE int64_t GetValueAddresses(int32_t rowIndex) const
    {
        return this->valueAddressesDeque[rowIndex];
    }

    ALWAYS_INLINE bool HaveNull(int32_t rowIndex) const
    {
        return this->nullsDeque[rowIndex];
    }

    ALWAYS_INLINE omniruntime::vec::Vector *GetColumn(int32_t vectorBatchIndex, int32_t columnIndex) const
    {
        return this->columnsDeque[vectorBatchIndex][columnIndex];
    }

    ALWAYS_INLINE omniruntime::vec::Vector *GetColumnFromCache(int32_t columnIndex) const
    {
        return this->cacheBatch[columnIndex];
    }

    ALWAYS_INLINE void CacheBatch(int32_t vectorBatchIndex)
    {
        if (vectorBatchIndex != cacheBatchId) {
            this->cacheBatchId = vectorBatchIndex;
            this->cacheBatch = this->columnsDeque[vectorBatchIndex];
        }
    }

    // free vecBatch until vecBatchIdx
    void FreeBeforeVecBatch(int32_t vecBatchIdx);

    // free all vecBatch
    void FreeAllRemainingVecBatch();

private:
    int32_t typesCount;
    int32_t *computeCols;
    int32_t computeColsCount;
    int32_t lastFreedVecBatchIdx = -1;

    int32_t cacheBatchId = -1;
    omniruntime::vec::Vector **cacheBatch;
    // vector  first Level：vectorBatch second Level: columnar vector
    std::vector<omniruntime::vec::Vector **> columnsDeque;
    std::vector<int64_t> valueAddressesDeque; // row
    std::vector<bool> nullsDeque;             // whether has a column whose value is null for gaven row
    int32_t positionCount;
    std::vector<bool> vecBatchFreeFlagDeque;                       // vectorBatch free flag
    std::vector<omniruntime::vec::VectorBatch *> vectorBatchDeque; // vectorBatch
    bool finishAddData;
};
}
}
#endif
