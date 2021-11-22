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
#include "operator.h"
#include "operator_factory.h"
#include "../vector/vector_helper.h"
#include "util/operator_util.h"

using namespace omniruntime::vec;
using namespace omniruntime::op;

class PagesIndex {
public:
    explicit PagesIndex(const VecTypes &types);
    ~PagesIndex();
    int32_t AddVecBatches(std::vector<VectorBatch *> &vecBatches);
    void Sort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
        const int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to) const;
    void GetOutput(int32_t *outputCols, int32_t outputColsCount, VectorBatch *outputVecBatch,
        const int32_t *sourceTypes, int32_t offset, int32_t length, VectorAllocator *vecAllocator) const;

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

    Vector ***GetColumns() const
    {
        return this->columns;
    }

private:
    const VecType *vecTypes;
    const int32_t *vecTypeIds;
    int32_t typesCount;
    Vector ***columns; // Vector* [columnIndex][tableIndex]
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

using CompareFunc = int32_t (*)(Vector *leftVector, int32_t leftPosition, Vector *rightVector, int32_t rightPosition);

static int32_t ALWAYS_INLINE Compare(const int32_t sortAscendings, const int32_t sortNullFirsts, const int64_t *valueAddresses,
                                     Vector **columns, int32_t leftPosition, int32_t rightPosition, CompareFunc compareFunc)
{
    int64_t leftValueAddress = valueAddresses[leftPosition];
    int32_t leftColumnIndex = DecodeSliceIndex(leftValueAddress);
    int32_t leftColumnPosition = DecodePosition(leftValueAddress);
    int64_t rightValueAddress = valueAddresses[rightPosition];
    int32_t rightColumnIndex = DecodeSliceIndex(rightValueAddress);
    int32_t rightColumnPosition = DecodePosition(rightValueAddress);

    Vector *leftColumn = columns[leftColumnIndex];
    Vector *rightColumn = columns[rightColumnIndex];
    int32_t originalLeftColumnPosition, originalRightColumnPosition;
    leftColumn = VectorHelper::ExpandVectorAndIndex(leftColumn, leftColumnPosition, originalLeftColumnPosition);
    rightColumn =
            VectorHelper::ExpandVectorAndIndex(rightColumn, rightColumnPosition, originalRightColumnPosition);
    int32_t compare = OperatorUtil::CompareNull(leftColumn, originalLeftColumnPosition, rightColumn,
                                                originalRightColumnPosition, sortNullFirsts);
    if (compare == OperatorUtil::COMPARE_STATUS_OTHER) {
        // neither the left nor the right is NULL
        compare = compareFunc(leftColumn, originalLeftColumnPosition, rightColumn, originalRightColumnPosition);

        if (sortAscendings == 0) {
            compare = -compare;
        }
    }
    return compare;
}
#endif
