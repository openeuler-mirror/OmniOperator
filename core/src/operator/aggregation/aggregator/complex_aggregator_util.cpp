/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: CompareComplexSlice for min/max on complex types (ARRAY, ROW).
 */

#include "complex_aggregator_util.h"
#include "aggregator.h"
#include "vector/vector.h"
#include "vector/array_vector.h"
#include "vector/row_vector.h"
#include "vector/map_vector.h"
#include "vector/large_string_container.h"
#include "type/decimal128.h"
#include <cstring>

namespace omniruntime {
namespace op {

using namespace vec;

// Compare two scalar elements at the same index in two vectors; returns <0, 0, >0.
// Null is considered less than non-null (Spark nulls first ordering).
static int CompareScalarElementsAt(BaseVector *vecA, BaseVector *vecB, int32_t index, type::DataTypeId elemTypeId)
{
    bool nullA = vecA->IsNull(index);
    bool nullB = vecB->IsNull(index);
    if (nullA && nullB) {
        return 0;
    }
    if (nullA) {
        return -1;
    }
    if (nullB) {
        return 1;
    }
    switch (elemTypeId) {
        case type::OMNI_BOOLEAN: {
            auto a = reinterpret_cast<Vector<int8_t> *>(vecA)->GetValue(index);
            auto b = reinterpret_cast<Vector<int8_t> *>(vecB)->GetValue(index);
            return Compare(a, b);
        }
        case type::OMNI_BYTE: {
            auto a = reinterpret_cast<Vector<int8_t> *>(vecA)->GetValue(index);
            auto b = reinterpret_cast<Vector<int8_t> *>(vecB)->GetValue(index);
            return Compare(a, b);
        }
        case type::OMNI_SHORT: {
            auto a = reinterpret_cast<Vector<int16_t> *>(vecA)->GetValue(index);
            auto b = reinterpret_cast<Vector<int16_t> *>(vecB)->GetValue(index);
            return Compare(a, b);
        }
        case type::OMNI_INT:
        case type::OMNI_DATE32:
        case type::OMNI_TIME32: {
            auto a = reinterpret_cast<Vector<int32_t> *>(vecA)->GetValue(index);
            auto b = reinterpret_cast<Vector<int32_t> *>(vecB)->GetValue(index);
            return Compare(a, b);
        }
        case type::OMNI_LONG:
        case type::OMNI_DATE64:
        case type::OMNI_TIME64:
        case type::OMNI_TIMESTAMP:
        case type::OMNI_DECIMAL64: {
            auto a = reinterpret_cast<Vector<int64_t> *>(vecA)->GetValue(index);
            auto b = reinterpret_cast<Vector<int64_t> *>(vecB)->GetValue(index);
            return Compare(a, b);
        }
        case type::OMNI_FLOAT: {
            auto a = reinterpret_cast<Vector<float> *>(vecA)->GetValue(index);
            auto b = reinterpret_cast<Vector<float> *>(vecB)->GetValue(index);
            return Compare(a, b);
        }
        case type::OMNI_DOUBLE: {
            auto a = reinterpret_cast<Vector<double> *>(vecA)->GetValue(index);
            auto b = reinterpret_cast<Vector<double> *>(vecB)->GetValue(index);
            return Compare(a, b);
        }
        case type::OMNI_DECIMAL128: {
            auto a = reinterpret_cast<Vector<type::Decimal128> *>(vecA)->GetValue(index);
            auto b = reinterpret_cast<Vector<type::Decimal128> *>(vecB)->GetValue(index);
            return Compare(a, b);
        }
        case type::OMNI_VARCHAR:
        case type::OMNI_CHAR:
        case type::OMNI_VARBINARY: {
            auto *varcharVecA = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vecA);
            auto *varcharVecB = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vecB);
            std::string_view sa = varcharVecA->GetValue(index);
            std::string_view sb = varcharVecB->GetValue(index);
            int cmp = static_cast<int>(sa.compare(sb));
            return (cmp < 0 ? -1 : (cmp > 0 ? 1 : 0));
        }
        default:
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
                "CompareComplexSlice: unsupported element type " + std::to_string(static_cast<int>(elemTypeId)));
    }
}

// Compare two array slices (element vectors). Lexicographic: element by element, then by length.
// Spark semantics: compare up to min(lenA, lenB); if equal prefix, shorter array is less.
static int CompareArraySlices(BaseVector *sliceA, BaseVector *sliceB)
{
    size_t sizeA = static_cast<size_t>(sliceA->GetSize());
    size_t sizeB = static_cast<size_t>(sliceB->GetSize());
    size_t minSize = (sizeA < sizeB) ? sizeA : sizeB;
    type::DataTypeId elemTypeId = static_cast<type::DataTypeId>(sliceA->GetTypeId());
    for (size_t i = 0; i < minSize; i++) {
        int cmp = CompareScalarElementsAt(sliceA, sliceB, static_cast<int32_t>(i), elemTypeId);
        if (cmp != 0) {
            return cmp;
        }
    }
    // Equal prefix: shorter array is less (Spark array ordering)
    if (sizeA != sizeB) {
        return (sizeA < sizeB) ? -1 : 1;
    }
    return 0;
}

// Compare two row slices (RowVectors with one row each). Field-by-field.
static int CompareRowSlices(BaseVector *sliceA, BaseVector *sliceB, type::DataType *colDataType)
{
    auto *rowA = static_cast<RowVector *>(sliceA);
    auto *rowB = static_cast<RowVector *>(sliceB);
    int32_t numFields = rowA->ChildSize();
    if (numFields != rowB->ChildSize()) {
        return (numFields < rowB->ChildSize()) ? -1 : 1;
    }
    int32_t rowIndex = 0;  // single row in slice
    for (int32_t c = 0; c < numFields; c++) {
        BaseVector *childA = rowA->ChildAt(c).get();
        BaseVector *childB = rowB->ChildAt(c).get();
        type::DataTypeId fieldTypeId = static_cast<type::DataTypeId>(childA->GetTypeId());
        if (fieldTypeId == type::OMNI_ARRAY || fieldTypeId == type::OMNI_ROW) {
            vec::BaseVector *subA = (fieldTypeId == type::OMNI_ARRAY)
                ? static_cast<vec::ArrayVector *>(childA)->GetValue(rowIndex)
                : static_cast<vec::RowVector *>(childA)->Slice(rowIndex, 1);
            vec::BaseVector *subB = (fieldTypeId == type::OMNI_ARRAY)
                ? static_cast<vec::ArrayVector *>(childB)->GetValue(rowIndex)
                : static_cast<vec::RowVector *>(childB)->Slice(rowIndex, 1);
            type::DataType *fieldDataType = (colDataType != nullptr && colDataType->GetId() == type::OMNI_ROW)
                ? static_cast<const type::RowType *>(colDataType)->Type(c).get()
                : nullptr;
            int cmp = CompareComplexSlice(subA, subB, fieldTypeId, fieldDataType);
            delete subA;
            delete subB;
            if (cmp != 0) {
                return cmp;
            }
        } else {
            int cmp = CompareScalarElementsAt(childA, childB, rowIndex, fieldTypeId);
            if (cmp != 0) {
                return cmp;
            }
        }
    }
    return 0;
}

int CompareComplexSlice(vec::BaseVector *sliceA, vec::BaseVector *sliceB, type::DataTypeId colTypeId,
    type::DataType *colDataType)
{
    (void)colDataType;
    if (sliceA == nullptr || sliceB == nullptr) {
        if (sliceA == sliceB) {
            return 0;
        }
        return (sliceA == nullptr) ? -1 : 1;
    }
    switch (colTypeId) {
        case type::OMNI_ARRAY:
            return CompareArraySlices(sliceA, sliceB);
        case type::OMNI_ROW:
            return CompareRowSlices(sliceA, sliceB, colDataType);
        case type::OMNI_MAP:
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
                "CompareComplexSlice: Map type is not orderable in Spark; min/max on map not supported");
        default:
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
                "CompareComplexSlice: unsupported col type " + std::to_string(static_cast<int>(colTypeId)));
    }
}

} // namespace op
} // namespace omniruntime
