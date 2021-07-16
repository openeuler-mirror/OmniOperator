/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#ifndef __PAGES_INDEX_H__
#define __PAGES_INDEX_H__

#include "../vector/vector_batch.h"

#include <stdint.h>
#include <vector>

class PagesIndex {
public:
    PagesIndex(int32_t *types, int32_t typesCount);
    ~PagesIndex();
    int32_t AddVecBatches(std::vector<VectorBatch *> &vecBatches);
    void Sort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
        const int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to) const;
    void GetOutput(int32_t *outputCols, int32_t outputColsCount, VectorBatch *outputVecBatch, int32_t *sourceTypes,
        int32_t offset, int32_t length) const;

    int32_t *GetTypes() const
    {
        return types;
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

    Vector ***GetColumns() const
    {
        return this->columns;
    }

private:
    int32_t *types;
    int32_t typesCount;
    Vector ***columns; // Vector* [columnIndex][tableIndex]
    int64_t *valueAddresses;
    int32_t positionCount;
};

int64_t EncodeSyntheticAddress(int32_t sliceIndex, int32_t sliceOffset);
int32_t DecodeSliceIndex(int64_t sliceAddress);
int32_t DecodePosition(int64_t sliceAddress);

#endif
