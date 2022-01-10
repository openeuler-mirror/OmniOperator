/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: dynamic pages index implementations
 */
#ifndef __DYNAMIC_PAGES_INDEX_H__
#define __DYNAMIC_PAGES_INDEX_H__

#include <stdint.h>
#include <vector>
#include <deque>
#include "../../../vector/vector_type.h"
#include "../../../vector/vector_batch.h"
#include "../../../vector/vector_types.h"

using namespace omniruntime::vec;

class DynamicPagesIndex {
public:
    explicit DynamicPagesIndex(const VecTypes &types);
    ~DynamicPagesIndex();
    int32_t AddVecBatches(const std::vector<VectorBatch *> &vecBatches);

    const int32_t *GetTypes() const
    {
        return vecTypeIds;
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

    Vector *GetColumns(int32_t vectorBatchIndex, int32_t columnIndex) const
    {
        if (columnIndex >= typesCount) {
            return nullptr;
        }

        if (vectorBatchIndex >= this->vecBatchFreeFlagDeque.size()) {
            return nullptr;
        }
        // relate vector have freed
        if (this->vecBatchFreeFlagDeque[vectorBatchIndex]) {
            return nullptr;
        }
        return this->columnsDeque[vectorBatchIndex][columnIndex];
    }

    void FreeVecBatch(int32_t vecBatchIdx);

private:
    const VecType *vecTypes;
    const int32_t *vecTypeIds;
    int32_t typesCount;

    // vector  first Level：vectorBatch second Level: columnar vector
    std::deque<std::deque<Vector *>> columnsDeque;
    std::deque<int64_t> valueAddressesDeque; // row
    int32_t positionCount;
    std::deque<bool> vecBatchFreeFlagDeque;     // vectorBatch free flag
    std::deque<VectorBatch *> vectorBatchDeque; // vectorBatch
    bool finishAddData;
};

#endif
