/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#include "vector_analyzer.h"

namespace omniruntime {
namespace op {

static constexpr uint32_t ARRAY_THRESHOLD = 8;

template <typename T>
bool VectorAnalyzer::CheckArrayMap(const T* values, const size_t length)
{
    int64_t min = groupbyColsRange.min;
    int64_t max = groupbyColsRange.max;
    auto rangeUpperBound = max - min;
    int64_t uint32Max = UINT32_MAX;
    const auto [minPtr, maxPtr] = std::minmax_element(values, values + length);
    max = std::max(max, ToInt64(*maxPtr));
    min = std::min(min, ToInt64(*minPtr));
    if (max > 0 && min < 0 && min + ARRAY_THRESHOLD * rangeUpperBound < max) {
        return false;
    }
    if (max - min > uint32Max) {
        return false;
    }

    if (max < min) {
        return false;
    }
    if (min < 0 && std::numeric_limits<T>::max() + min < max) {
        return false;
    }

    if (max - min > uint32Max || max - min > ARRAY_THRESHOLD * rangeUpperBound) {
        return false;
    }

    // upate range and exppand size
    if (groupbyColsRange.max < max) {
        groupbyColsRange.max = max;
    }
    if (groupbyColsRange.min < min) {
        groupbyColsRange.min = min;
    }
    return true;
}


template <typename T>
T VectorAnalyzer::ComputerKey(const T value)
{
    return value - static_cast<T>(groupbyColsRange.min);
}

bool VectorAnalyzer::DecideHashMode(omniruntime::vec::VectorBatch * vectorBatch)
{
    // skip analyze value
    if (hashMode == HashTableType::NORMAL_HASH_TABLE) {
        return false;
    }

    auto groupColNum = static_cast<int32_t>(this->groupByCols.size());
    // current only support one columnar for array mode
    if (groupColNum != 1) {
        hashMode = HashTableType::NORMAL_HASH_TABLE;
        return false;
    }
    
    bool blChangeMap = false;
    auto omniId = groupByCols[0].input->GetId();
    int32_t idx = groupByCols[0].idx;
    switch (omniId) {
        case OMNI_INT:
        case OMNI_DATE32:
            blChangeMap = HandleInputValues<OMNI_INT>(vectorBatch, idx);
            break;
        case OMNI_SHORT:
            blChangeMap = HandleInputValues<OMNI_SHORT>(vectorBatch, idx);
            break;
        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64:
            blChangeMap = HandleInputValues<OMNI_LONG>(vectorBatch, idx);
            break;
        default:
            hashMode = HashTableType::NORMAL_HASH_TABLE;
            blChangeMap = false;
            break;
    }
    return blChangeMap;
}

template <type::DataTypeId typeId>
bool VectorAnalyzer::HandleInputValues(omniruntime::vec::VectorBatch * vectorBatch, int32_t idx)
{
    using RawDataType = typename NativeAndVectorType<typeId>::type;
    using RawVectorType = typename NativeAndVectorType<typeId>::vector;
    auto vector = vectorBatch->Get(idx);

    auto rowCount = vectorBatch->GetRowCount();
    if (vector->GetEncoding() != OMNI_DICTIONARY) {
        auto valuePtr = unsafe::UnsafeVector::GetRawValues(static_cast<RawVectorType *>(vector));
        if (!CheckArrayMap<RawDataType>(valuePtr, rowCount)) {
            hashMode = HashTableType::NORMAL_HASH_TABLE;
            return true;
        }
    } else {
            hashMode = HashTableType::NORMAL_HASH_TABLE;
            return false;
    }
}

bool VectorAnalyzer::IsArrayHashTableType()
{
    return hashMode == HashTableType::ARRAY_HASH_TABLE;
}

}
}