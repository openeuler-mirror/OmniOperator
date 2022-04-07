/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: pages index implementations
 */
#ifndef __PAGES_INDEX_H__
#define __PAGES_INDEX_H__

#include <cstdint>
#include <vector>
#include "type/data_type.h"
#include "type/data_types.h"
#include "vector/vector_batch.h"
#include "operator.h"
#include "operator_factory.h"
#include "vector/vector_helper.h"
#include "util/operator_util.h"

namespace omniruntime {
namespace op {
class PagesIndex {
public:
    explicit PagesIndex(const omniruntime::type::DataTypes &types);
    ~PagesIndex();
    int32_t AddVecBatches(std::vector<omniruntime::vec::VectorBatch *> &vecBatches);
    void Sort(const int32_t *sortCols, const int32_t *sortColTypes, const int32_t *sortAscendings,
        const int32_t *sortNullFirsts, int32_t sortColCount, int32_t from, int32_t to) const;
    void GetOutput(int32_t *outputCols, int32_t outputColsCount, omniruntime::vec::VectorBatch *outputVecBatch,
        const int32_t *sourceTypes, int32_t offset, int32_t length,
        omniruntime::vec::VectorAllocator *vecAllocator) const;

    const int32_t *GetTypes() const
    {
        return dataTypeIds;
    }
    int32_t GetTypesCount() const
    {
        return typesCount;
    }
    uint64_t *GetValueAddresses() const
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
    const omniruntime::type::DataType *dataTypes;
    const int32_t *dataTypeIds;
    uint32_t typesCount;
    omniruntime::vec::Vector ***columns; // Vector* [columnIndex][tableIndex]
    uint64_t *valueAddresses;
    uint32_t positionCount;
};

constexpr uint32_t SHIFT_SIZE_32 = 32;
inline uint64_t EncodeSyntheticAddress(uint32_t sliceIndex, uint32_t sliceOffset)
{
    return (static_cast<uint64_t>(sliceIndex) << SHIFT_SIZE_32) | sliceOffset;
}

inline uint32_t DecodeSliceIndex(uint64_t sliceAddress)
{
    return static_cast<uint32_t>(sliceAddress >> SHIFT_SIZE_32);
}

inline uint32_t DecodePosition(uint64_t sliceAddress)
{
    return static_cast<uint32_t>(sliceAddress);
}

using CompareFunc = int32_t (*)(omniruntime::vec::Vector *leftVector, int32_t leftPosition,
    omniruntime::vec::Vector *rightVector, int32_t rightPosition);

static int32_t ALWAYS_INLINE Compare(const int32_t sortAscendings, const int32_t sortNullFirsts,
    const uint64_t *valueAddresses, omniruntime::vec::Vector **columns, int32_t leftPosition, int32_t rightPosition,
    CompareFunc compareFunc)
{
    int64_t leftValueAddress = valueAddresses[leftPosition];
    uint32_t leftColumnIndex = DecodeSliceIndex(leftValueAddress);
    uint32_t leftColumnPosition = DecodePosition(leftValueAddress);
    uint64_t rightValueAddress = valueAddresses[rightPosition];
    uint32_t rightColumnIndex = DecodeSliceIndex(rightValueAddress);
    uint32_t rightColumnPosition = DecodePosition(rightValueAddress);

    omniruntime::vec::Vector *leftColumn = columns[leftColumnIndex];
    omniruntime::vec::Vector *rightColumn = columns[rightColumnIndex];
    int32_t originalLeftColumnPosition;
    int32_t originalRightColumnPosition;
    leftColumn = omniruntime::vec::VectorHelper::ExpandVectorAndIndex(leftColumn, static_cast<int32_t>(leftColumnPosition),
        originalLeftColumnPosition);
    rightColumn = omniruntime::vec::VectorHelper::ExpandVectorAndIndex(rightColumn, static_cast<int32_t>(rightColumnPosition),
        originalRightColumnPosition);
    int32_t compare = omniruntime::op::OperatorUtil::CompareNull(leftColumn, originalLeftColumnPosition, rightColumn,
        originalRightColumnPosition, sortNullFirsts);
    if (compare == omniruntime::op::OperatorUtil::COMPARE_STATUS_OTHER) {
        // neither the left nor the right is NULL
        compare = compareFunc(leftColumn, originalLeftColumnPosition, rightColumn, originalRightColumnPosition);

        if (sortAscendings == 0) {
            compare = -compare;
        }
    }
    return compare;
}
}
}
#endif
