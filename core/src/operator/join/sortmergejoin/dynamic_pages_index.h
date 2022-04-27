/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: dynamic pages index implementations
 */
#ifndef __DYNAMIC_PAGES_INDEX_H__
#define __DYNAMIC_PAGES_INDEX_H__

#include <cstdint>
#include <vector>
#include <deque>
#include "type/data_type.h"
#include "type/data_types.h"
#include "vector/vector_batch.h"

namespace omniruntime {
namespace op {
class DynamicPagesIndex {
public:
    explicit DynamicPagesIndex(const omniruntime::type::DataTypes &types);
    ~DynamicPagesIndex();
    int32_t AddVecBatches(const std::vector<omniruntime::vec::VectorBatch *> &vecBatches);

    const int32_t *GetTypes() const
    {
        return dataTypeIds;
    }

    int32_t GetTypesCount() const
    {
        return typesCount;
    }

    int32_t GetPositionCount() const
    {
        return this->positionCount;
    }

    bool IsDataFinish() const
    {
        return this->finishAddData;
    }

    bool IsDataFinish(int32_t rowIndex) const
    {
        return this->finishAddData && (rowIndex >= this->positionCount - 1);
    }

    int64_t GetValueAddresses(int32_t rowIndex) const
    {
        if (rowIndex >= this->positionCount) {
            return -1;
        }
        return this->valueAddressesDeque[rowIndex];
    }

    omniruntime::vec::Vector *GetColumns(int32_t vectorBatchIndex, int32_t columnIndex) const
    {
        if (columnIndex >= typesCount) {
            return nullptr;
        }

        if (static_cast<size_t>(vectorBatchIndex) >= this->vecBatchFreeFlagDeque.size()) {
            return nullptr;
        }
        // relate vector have freed
        if (this->vecBatchFreeFlagDeque[vectorBatchIndex]) {
            return nullptr;
        }
        return this->columnsDeque[vectorBatchIndex][columnIndex];
    }

    // free vecBatch until vecBatchIdx
    void FreeBeforeVecBatch(int32_t vecBatchIdx);

    // free all vecBatch
    void FreeAllRemainingVecBatch();

private:
    const int32_t *dataTypeIds;
    int32_t typesCount;
    int32_t lastFreedVecBatchIdx = -1;

    // vector  first Level：vectorBatch second Level: columnar vector
    std::deque<std::deque<omniruntime::vec::Vector *>> columnsDeque;
    std::deque<int64_t> valueAddressesDeque; // row
    int32_t positionCount;
    std::deque<bool> vecBatchFreeFlagDeque;                       // vectorBatch free flag
    std::deque<omniruntime::vec::VectorBatch *> vectorBatchDeque; // vectorBatch
    bool finishAddData;
};
}
}
#endif
