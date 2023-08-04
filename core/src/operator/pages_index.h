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
#include "vector/vector_helper.h"
#include "util/operator_util.h"
#include "operator/memory_builder.h"

namespace omniruntime {
namespace op {
class PagesIndex : public MemoryBuilder {
public:
    explicit PagesIndex(const DataTypes &types);

    ~PagesIndex() override;

    void AddVecBatch(omniruntime::vec::VectorBatch *vecBatch);

    void Prepare();

    template <DataTypeId typeId> void PrepareInplaceSort(int32_t nullFirst)
    {
        auto vecBatchCount = static_cast<int32_t>(inputVecBatches.size());

        for (int32_t vecBatchIdx = 0; vecBatchIdx < vecBatchCount; ++vecBatchIdx) {
            VectorBatch *vecBatch = inputVecBatches[vecBatchIdx];
            auto col = vecBatch->Get(0);
            totalNullCount += col->GetNullCount();
            if (col->GetEncoding() == OMNI_DICTIONARY) {
                vecBatch->SetVector(0, VectorHelper::DecodeDictionaryVector(col));
                delete col;
            }
        }

        inplaceSortColumn = VectorHelper::CreateFlatVector<typeId>(positionCount);
        if (totalNullCount == 0) {
            // all batches have no null value
            PartitionNonNull<typeId>();
        } else {
            // support nulls first and nulls last
            PartitionNull<typeId>(nullFirst);
        }
        inputVecBatches.clear();
    }

    void Sort(const int32_t *sortCols, const int32_t *sortAscendings, const int32_t *sortNullFirsts,
        int32_t sortColCount, int32_t from, int32_t to);

    void SortInplace(const int32_t *sortCols, const int32_t *sortAscendings, const int32_t *sortNullFirsts,
        int32_t sortColCount, int32_t from, int32_t to);

    void GetOutput(int32_t *outputCols, int32_t outputColsCount, omniruntime::vec::VectorBatch *outputVecBatch,
        const int32_t *sourceTypes, int32_t offset, int32_t length) const;

    void GetOutputInplaceSort(int32_t *outputCols, int32_t outputColsCount,
        omniruntime::vec::VectorBatch *outputVecBatch, const int32_t *sourceTypes, int32_t offset,
        int32_t length) const;

    void GetSortedVecBatches(std::vector<int32_t> &outputCols,
        std::vector<omniruntime::vec::VectorBatch *> &sortedVecBatches, bool canSortInplace = false);

    void Clear();

    ALWAYS_INLINE const DataTypes &GetTypes() const
    {
        return dataTypes;
    }

    ALWAYS_INLINE int32_t GetTypesCount() const
    {
        return typesCount;
    }

    ALWAYS_INLINE uint64_t *GetValueAddresses() const
    {
        return this->valueAddresses;
    }

    ALWAYS_INLINE int64_t GetRowCount() override
    {
        return this->positionCount;
    }

    ALWAYS_INLINE omniruntime::vec::BaseVector ***GetColumns() const
    {
        return this->columns;
    }

private:
    void ColumnarSort(const int32_t *sortCols, const int32_t *sortAscendings, const int32_t *sortNullFirsts,
        int32_t sortColCount, std::vector<int64_t> &values, std::vector<uint32_t> &varcharLength, int32_t from,
        int32_t to, int32_t currentCol);

    template <DataTypeId D>
    void ColumnarSort(const int32_t *sortCols, const int32_t *sortAscendings, const int32_t *sortNullFirsts,
        int32_t sortColCount, std::vector<int64_t> &values, std::vector<uint32_t> &varcharLength, int32_t from,
        int32_t to, int32_t currentCol);

    template <DataTypeId typeId> void PartitionNonNull()
    {
        using T = typename NativeType<typeId>::type;
        auto vecBatchCount = static_cast<int32_t>(inputVecBatches.size());
        int32_t valueIndex = 0;
        for (int32_t vecBatchIdx = 0; vecBatchIdx < vecBatchCount; ++vecBatchIdx) {
            VectorBatch *vecBatch = inputVecBatches[vecBatchIdx];
            auto rowCount = vecBatch->GetRowCount();
            auto *col = reinterpret_cast<Vector<T> *>(vecBatch->Get(0));
            for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                reinterpret_cast<Vector<T> *>(inplaceSortColumn)->SetValue(valueIndex++, col->GetValue(rowIdx));
            }
            VectorHelper::FreeVecBatch(vecBatch);
        }
    }

    template <DataTypeId typeId> void PartitionNull(int32_t nullFirst)
    {
        using T = typename NativeType<typeId>::type;
        auto vecBatchCount = static_cast<int32_t>(inputVecBatches.size());
        auto values = reinterpret_cast<T *>(VectorHelper::UnsafeGetValues(inplaceSortColumn));
        auto nulls = unsafe::UnsafeBaseVector::GetNulls(inplaceSortColumn);

        // init values, nulls position
        if (nullFirst) {
            values += totalNullCount;
        } else {
            nulls += positionCount - totalNullCount;
        }

        int32_t valueIndex = 0;
        int32_t nullIndex = 0;
        for (int32_t vecBatchIdx = 0; vecBatchIdx < vecBatchCount; ++vecBatchIdx) {
            VectorBatch *vecBatch = inputVecBatches[vecBatchIdx];
            auto rowCount = static_cast<uint32_t>(vecBatch->GetRowCount());
            auto *col = reinterpret_cast<Vector<T> *>(vecBatch->Get(0));
            if (!col->HasNull()) {
                for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                    values[valueIndex++] = col->GetValue(rowIdx);
                }
            } else {
                for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                    if (col->IsNull(rowIdx)) {
                        nulls[nullIndex++] = true;
                    } else {
                        values[valueIndex++] = col->GetValue(rowIdx);
                    }
                }
            }
            VectorHelper::FreeVecBatch(vecBatch);
        }
    }

    template <DataTypeId dataTypeId>
    void SortInplace(int32_t sortAscending, int32_t sortNullFirst, int32_t from, int32_t to);

    const DataTypes dataTypes;
    uint32_t typesCount;
    omniruntime::vec::BaseVector ***columns; // Vector* [columnIndex][tableIndex]
    uint64_t *valueAddresses;
    uint32_t positionCount;
    std::vector<omniruntime::vec::VectorBatch *> inputVecBatches;
    std::vector<bool> hasDictionaries;
    std::vector<bool> hasNulls;
    int64_t totalNullCount = 0;
    omniruntime::vec::BaseVector *inplaceSortColumn = nullptr;
};

constexpr uint32_t SHIFT_SIZE_32 = 32;
static ALWAYS_INLINE uint64_t EncodeSyntheticAddress(uint32_t sliceIndex, uint32_t sliceOffset)
{
    return (static_cast<uint64_t>(sliceIndex) << SHIFT_SIZE_32) | sliceOffset;
}

static ALWAYS_INLINE uint32_t DecodeSliceIndex(uint64_t sliceAddress)
{
    return static_cast<uint32_t>(sliceAddress >> SHIFT_SIZE_32);
}

static ALWAYS_INLINE uint32_t DecodePosition(uint64_t sliceAddress)
{
    return static_cast<uint32_t>(sliceAddress);
}

template <bool columnsNullFlag, int32_t sortAscendings>
static int32_t ALWAYS_INLINE Compare(const int32_t sortNullFirsts, const uint64_t *valueAddresses,
    omniruntime::vec::BaseVector **columns, int32_t leftPosition, int32_t rightPosition,
    omniruntime::op::OperatorUtil::CompareFunc compareFunc)
{
    int64_t leftValueAddress = valueAddresses[leftPosition];
    uint32_t leftColumnIndex = DecodeSliceIndex(leftValueAddress);
    uint32_t leftColumnPosition = DecodePosition(leftValueAddress);
    uint64_t rightValueAddress = valueAddresses[rightPosition];
    uint32_t rightColumnIndex = DecodeSliceIndex(rightValueAddress);
    uint32_t rightColumnPosition = DecodePosition(rightValueAddress);

    vec::BaseVector *leftColumn = columns[leftColumnIndex];
    vec::BaseVector *rightColumn = columns[rightColumnIndex];

    int32_t compare = omniruntime::op::OperatorUtil::COMPARE_STATUS_OTHER;
    if constexpr (columnsNullFlag) {
        compare = omniruntime::op::OperatorUtil::CompareNull(leftColumn, leftColumnPosition, rightColumn,
            rightColumnPosition, sortNullFirsts);
        if (compare == omniruntime::op::OperatorUtil::COMPARE_STATUS_OTHER) {
            compare = compareFunc(leftColumn, leftColumnPosition, rightColumn, rightColumnPosition);
            if constexpr (sortAscendings == 0) {
                compare = -compare;
            }
        }
    } else {
        // neither the left nor the right is NULL
        compare = compareFunc(leftColumn, leftColumnPosition, rightColumn, rightColumnPosition);
        if constexpr (sortAscendings == 0) {
            compare = -compare;
        }
    }

    return compare;
}
}
}
#endif
