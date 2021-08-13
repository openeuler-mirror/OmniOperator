/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: pages index implementations
 */
#ifndef __PAGES_INDEX_H__
#define __PAGES_INDEX_H__

#include <stdint.h>
#include <vector>
#include "../vector/vector_type.h"
#include "../vector/vector_batch.h"
#include "../vector/vector_types.h"

class PagesIndex {
public:
    explicit PagesIndex(const omniruntime::vec::VecTypes &types);
    ~PagesIndex();
    int32_t AddVecBatches(std::vector<omniruntime::vec::VectorBatch *> &vecBatches);
    void Sort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
        const int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to) const;
    void GetOutput(int32_t *outputCols, int32_t outputColsCount, omniruntime::vec::VectorBatch *outputVecBatch,
        const int32_t *sourceTypes, int32_t offset, int32_t length) const;

    const int32_t *GetTypes() const
    {
        return vecTypeIds;
    }
    int32_t GetTypesCount() const
    {
        return typesCount;
    }
    int64_t *GetValueAddresses() const
    {
        return this->valueAddresses;
    }
    int32_t GetPositionCount() const
    {
        return this->positionCount;
    }

    omniruntime::vec::Vector ***GetColumns() const
    {
        return this->columns;
    }

private:
    const omniruntime::vec::VecType *vecTypes;
    const int32_t *vecTypeIds;
    int32_t typesCount;
    omniruntime::vec::Vector ***columns; // Vector* [columnIndex][tableIndex]
    int64_t *valueAddresses;
    int32_t positionCount;
};

const int32_t SHIFT_SIZE_32 = 32;
inline int64_t EncodeSyntheticAddress(int32_t sliceIndex, int32_t sliceOffset)
{
    return (static_cast<int64_t>(sliceIndex) << SHIFT_SIZE_32) | sliceOffset;
}

inline int32_t DecodeSliceIndex(int64_t sliceAddress)
{
    return static_cast<int32_t>(sliceAddress >> SHIFT_SIZE_32);
}

inline int32_t DecodePosition(int64_t sliceAddress)
{
    return static_cast<int32_t>(sliceAddress);
}

#endif
