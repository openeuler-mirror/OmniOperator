/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Helpers for max_by/min_by when target column (col1) is OMNI_ARRAY, OMNI_MAP, or OMNI_ROW.
 *              These types do not store one value per row in a flat buffer; element access uses type-specific APIs.
 *
 * - ArrayVector: one element vector + offsets; GetValue(i) returns Slice of elements for row i; SetValue(i, slice) appends slice to element vector and sets offset/size.
 * - MapVector: key vector + value vector + offsets; Slice(i, 1) returns one-row MapVector; set by appending slice's keys/values to output's key/value vectors and setting offset/size.
 * - RowVector: child vectors per column; Slice(i, 1) returns one-row RowVector; Append(slice, rowIndex, 1) copies one row from slice into output at rowIndex.
 */
#ifndef OMNI_RUNTIME_COMPLEX_AGGREGATOR_UTIL_H
#define OMNI_RUNTIME_COMPLEX_AGGREGATOR_UTIL_H

#include "vector/array_vector.h"
#include "vector/map_vector.h"
#include "vector/row_vector.h"
#include "vector/vector_helper.h"
#include "type/data_type.h"

namespace omniruntime {
namespace op {

/**
 * True if the slice represents a null value (caller should not copy and should output null).
 * - slice == nullptr: row was null at source (GetComplexColSlice returned nullptr).
 * - Map/Row slice with one row and that row IsNull(0): slice represents null.
 */
inline bool IsComplexSliceNull(vec::BaseVector *slice, type::DataTypeId colTypeId)
{
    if (slice == nullptr) {
        return true;
    }
    if (colTypeId != type::OMNI_ARRAY && slice->GetSize() >= 1 && slice->IsNull(0)) {
        return true;
    }
    return false;
}

/**
 * True if this row should be skipped for aggregation (target is null; do not update state).
 * Use when we must only consider rows with non-null target (Spark semantics: only update with valid target).
 * - IsComplexSliceNull(slice): already null.
 * - Array/Map: slice has 0 size and colVector->IsNull(rowIndex) (null stored as 0-size without null bit).
 */
inline bool ShouldSkipRowTargetNull(vec::BaseVector *slice, type::DataTypeId colTypeId,
    vec::BaseVector *colVector, int32_t rowIndex)
{
    if (IsComplexSliceNull(slice, colTypeId)) {
        return true;
    }
    if (colTypeId == type::OMNI_ARRAY) {
        if (static_cast<int32_t>(slice->GetSize()) == 0 && colVector->IsNull(rowIndex)) {
            return true;
        }
    } else if (colTypeId == type::OMNI_MAP) {
        auto *mapSlice = static_cast<vec::MapVector *>(slice);
        if (mapSlice->GetSize(0) == 0 && colVector->IsNull(rowIndex)) {
            return true;
        }
    }
    return false;
}

/**
 * Get the value at rowIndex from a complex-type column as a slice (view).
 * Returned pointer is valid as long as colVector is not modified; caller does not own.
 * Returns nullptr when the row is null (so caller can treat uniformly via IsComplexSliceNull).
 *
 * - OMNI_ARRAY: ArrayVector::GetValue(rowIndex) -> BaseVector* (element Slice for that row).
 * - OMNI_MAP: MapVector::Slice(rowIndex, 1) -> MapVector* (one row).
 * - OMNI_ROW: RowVector::Slice(rowIndex, 1) -> RowVector* (one row).
 */
inline vec::BaseVector *GetComplexColSlice(vec::BaseVector *colVector, type::DataTypeId colTypeId, int32_t rowIndex)
{
    switch (colTypeId) {
        case type::OMNI_ARRAY:
            if (colVector->IsNull(rowIndex)) {
                return nullptr;
            }
            return static_cast<vec::ArrayVector *>(colVector)->GetValue(rowIndex);
        case type::OMNI_MAP:
            if (colVector->IsNull(rowIndex)) {
                return nullptr;
            }
            return static_cast<vec::MapVector *>(colVector)->Slice(rowIndex, 1);
        case type::OMNI_ROW:
            if (colVector->IsNull(rowIndex)) {
                return nullptr;
            }
            return static_cast<vec::RowVector *>(colVector)->Slice(rowIndex, 1);
        default:
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
                "GetComplexColSlice: unsupported target col type " + std::to_string(static_cast<int>(colTypeId)));
    }
}

/**
 * Copy a complex-type slice to a newly allocated vector that the caller owns.
 * Used when storing slice in aggregator state so the pointer remains valid after the source batch is released.
 * Caller must delete the returned pointer (or use ReleaseComplexSliceCopy).
 * @param colDataType optional column DataType; when colTypeId is OMNI_ARRAY and slice is empty, used to create
 *        element vector via CreateEmptyComplexVector (correct for both flat and complex element types).
 */
inline vec::BaseVector *CopyComplexSliceToOwned(vec::BaseVector *slice, type::DataTypeId colTypeId,
    type::DataType *colDataType = nullptr)
{
    switch (colTypeId) {
        case type::OMNI_ARRAY: {
            // slice is element-vector slice for one row (from ArrayVector::GetValue).
            // Return a copy of the element vector; SetComplexColValue(ArrayVector::SetValue) expects this.
            int32_t n = static_cast<int32_t>(slice->GetSize());
            if (n == 0) {
                if (colDataType != nullptr && colDataType->GetId() == type::OMNI_ARRAY) {
                    type::ArrayType *arrayType = static_cast<type::ArrayType *>(colDataType);
                    return vec::VectorHelper::CreateEmptyComplexVector(arrayType->ElementType().get(), 0);
                }
                type::DataTypePtr elemType = vec::VectorHelper::GetDataType(slice);
                return vec::VectorHelper::CreateEmptyComplexVector(elemType.get(), 0);
            }
            std::vector<int> positions(static_cast<size_t>(n));
            for (int32_t i = 0; i < n; i++) {
                positions[i] = i;
            }
            return vec::VectorHelper::CopyPositionsVector(slice, positions.data(), 0, n);
        }
        case type::OMNI_MAP: {
            int pos = 0;
            return vec::VectorHelper::CopyPositionsVector(slice, &pos, 0, 1);
        }
        case type::OMNI_ROW: {
            int pos = 0;
            return vec::VectorHelper::CopyPositionsVector(slice, &pos, 0, 1);
        }
        default:
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
                "CopyComplexSliceToOwned: unsupported target col type " + std::to_string(static_cast<int>(colTypeId)));
    }
}

/**
 * Release a complex slice copy previously returned by CopyComplexSliceToOwned.
 */
inline void ReleaseComplexSliceCopy(vec::BaseVector *ownedSlice, type::DataTypeId colTypeId)
{
    if (ownedSlice == nullptr) {
        return;
    }
    delete ownedSlice;
}

/**
 * Write one row into the output complex-type column from a slice previously obtained by GetComplexColSlice.
 * Must be called in row order (0, 1, 2, ...) for MAP so offsets remain correct.
 *
 * - OMNI_ARRAY: ArrayVector::SetValue(rowIndex, slice) appends slice elements and sets size.
 * - OMNI_MAP: Expand key/value vectors, append slice's key/value to output, then SetOffset(rowIndex), SetSize(rowIndex, sliceLen).
 * - OMNI_ROW: RowVector::Append(slice, rowIndex, 1) copies one row from slice into output at rowIndex.
 */
inline void SetComplexColValue(vec::BaseVector *outputVector, type::DataTypeId colTypeId, int32_t rowIndex,
    vec::BaseVector *slice)
{
    if (IsComplexSliceNull(slice, colTypeId)) {
        outputVector->SetNull(rowIndex);
        if (colTypeId == type::OMNI_ROW) {
            auto *outRow = static_cast<vec::RowVector *>(outputVector);
            int32_t needSize = rowIndex + 1;
            if (outRow->GetSize() < needSize) {
                outRow->Expand(needSize);
            }
            for (int32_t c = 0; c < outRow->ChildSize(); c++) {
                vec::BaseVector *child = outRow->ChildAt(c).get();
                if (child->GetSize() < needSize) {
                    child->Expand(needSize);
                }
                child->SetNull(rowIndex);
            }
        } else if (colTypeId == type::OMNI_MAP) {
            auto *outMap = static_cast<vec::MapVector *>(outputVector);
            int32_t destOffset = (rowIndex == 0) ? 0 : static_cast<int32_t>(outMap->GetOffset(rowIndex));
            outMap->SetOffset(rowIndex, destOffset);
            outMap->SetSize(rowIndex, 0);
        } else if (colTypeId == type::OMNI_ARRAY) {
            // ArrayVector::SetNull already sets size to 0; set explicitly so null is not interpreted as empty.
            static_cast<vec::ArrayVector *>(outputVector)->SetSize(rowIndex, 0);
        }
        return;
    }
    switch (colTypeId) {
        case type::OMNI_ARRAY:
            static_cast<vec::ArrayVector *>(outputVector)->SetValue(rowIndex, slice);
            break;
        case type::OMNI_MAP: {
            auto *outMap = static_cast<vec::MapVector *>(outputVector);
            auto *sliceMap = static_cast<vec::MapVector *>(slice);
            int32_t sliceLen = static_cast<int32_t>(sliceMap->GetSize(0));
            int32_t destOffset = (rowIndex == 0) ? 0 : static_cast<int32_t>(outMap->GetOffset(rowIndex));
            vec::VectorHelper::ExpandElementVector(outMap->GetKeyVector().get(), outMap->GetKeyVector()->GetTypeId(),
                destOffset + sliceLen);
            vec::VectorHelper::ExpandElementVector(outMap->GetValueVector().get(), outMap->GetValueVector()->GetTypeId(),
                destOffset + sliceLen);
            vec::VectorHelper::AppendVector(outMap->GetKeyVector().get(), destOffset, sliceMap->GetKeyVector().get(),
                sliceLen);
            vec::VectorHelper::AppendVector(outMap->GetValueVector().get(), destOffset, sliceMap->GetValueVector().get(),
                sliceLen);
            outMap->SetOffset(rowIndex, destOffset);
            outMap->SetSize(rowIndex, sliceLen);
            break;
        }
        case type::OMNI_ROW:
            static_cast<vec::RowVector *>(outputVector)->Append(slice, rowIndex, 1);
            break;
        default:
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
                "SetComplexColValue: unsupported target col type " + std::to_string(static_cast<int>(colTypeId)));
    }
}

} // namespace op
} // namespace omniruntime

#endif // OMNI_RUNTIME_COMPLEX_AGGREGATOR_UTIL_H
