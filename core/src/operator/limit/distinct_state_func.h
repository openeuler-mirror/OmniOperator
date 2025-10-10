/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DISTINCT_STATE_FUNC_H
#define OMNI_RUNTIME_DISTINCT_STATE_FUNC_H
#include "vector/vector_batch.h"

namespace omniruntime {
namespace op {
struct ValueState {
    int64_t val = 0;
    int64_t count = 0;
    void Reset()
    {
        val = 0;
        count = 0;
    }
};

using HashFunc = void (*)(BaseVector *vector, const uint32_t r, const int32_t *ri, uint64_t *hashVal);
using HashFuncVector = void (*)(BaseVector *vector, const uint32_t s, const uint32_t r, uint64_t *hashVal);
using DuplicateKeyValue = void (*)(ValueState &state, BaseVector *vector, const uint32_t offset,
    ExecutionContext *context);
using IsSameNodeFunc = void (*)(BaseVector *vector, const uint32_t offset, const ValueState &slot, bool &isSame);
using SetVector = void (*)(VectorBatch *vecBatch, int32_t rowCount);
using FillValue = void (*)(BaseVector *vector, int32_t rowIndex, ValueState &state);

// following struct is used in original hash aggregation, it has conflict with reconstruct aggregation state
// we remove it from group_aggregation.h.
struct FunctionByDataType {
    DataTypeId dataTypeId;
    HashFunc hashFunc;
    HashFuncVector hashFuncVect;
    IsSameNodeFunc isSameNode;
    DuplicateKeyValue duplicateKey;
    SetVector setVector;
    FillValue fillValue;
};

template <typename V, typename D>
void HashFuncImpl(BaseVector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    int64_t hash;
    std::hash<D> hasher;
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = rowIndexes[i];
        hash = !vector->IsNull(idx) * hasher(static_cast<V *>(vector)->GetValue(idx));
        combinedHash[i] = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), hash);
    }
}

template <typename V = Vector<LargeStringContainer<std::string_view>>>
void HashVarcharFuncImpl(BaseVector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = rowIndexes[i];
        std::string_view str = static_cast<V *>(vector)->GetValue(idx);
        auto val = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(str.data())), str.size());
        combinedHash[i] = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), !vector->IsNull(idx) * val);
    }
}

template <typename V = Vector<Decimal128>>
void HashDecimalFunc(BaseVector *vector, const uint32_t rowCount, const int32_t *rowIndexes, uint64_t *combinedHash)
{
    for (uint32_t i = 0; i < rowCount; ++i) {
        int32_t idx = rowIndexes[i];
        Decimal128 val = static_cast<V *>(vector)->GetValue(idx);
        auto hash = HashUtil::CombineHash(combinedHash[i],
            !vector->IsNull(idx) * HashUtil::HashValue(val.LowBits(), val.HighBits()));
        combinedHash[i] = static_cast<uint64_t>(hash);
    }
}

template <typename V, typename D>
void HashFuncVectImpl(BaseVector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    uint64_t hash;
    std::hash<D> hasher;
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = i + start;
        hash = !vector->IsNull(idx) * hasher(static_cast<V *>(vector)->GetValue(idx));
        combinedHash[i] = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), static_cast<int64_t>(hash));
    }
}

template <typename V>
void HashVarcharVectFuncImpl(BaseVector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = i + start;
        std::string_view str = static_cast<V *>(vector)->GetValue(idx);
        auto val = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(str.data())), str.size());
        auto hash = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]), !vector->IsNull(idx) * val);
        combinedHash[i] = static_cast<uint64_t>(hash);
    }
}

template <typename V>
void HashDecimalVectFunc(BaseVector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    for (uint32_t i = 0; i < rowCount; ++i) {
        auto idx = i + start;
        Decimal128 val = static_cast<V *>(vector)->GetValue(idx);
        auto hash = HashUtil::CombineHash(static_cast<int64_t>(combinedHash[i]),
            !vector->IsNull(idx) * HashUtil::HashValue(val.LowBits(), val.HighBits()));
        combinedHash[i] = static_cast<uint64_t>(hash);
    }
}

template <typename V, typename D>
void HashFuncVectImplProxy(BaseVector *vector, uint32_t start, uint32_t rowCount, uint64_t *combinedHash)
{
    HashFuncVectImpl<V, D>(vector, start, rowCount, combinedHash);
}

template <typename V = Vector<LargeStringContainer<std::string_view>>>
void HashVarcharVectFuncImplProxy(BaseVector *vector, const uint32_t start, const uint32_t rowCount,
    uint64_t *combinedHash)
{
    HashVarcharVectFuncImpl<V>(vector, start, rowCount, combinedHash);
}

template <typename V = Vector<Decimal128>>
void HashDecimalVectFuncProxy(BaseVector *vector, const uint32_t start, const uint32_t rowCount, uint64_t *combinedHash)
{
    HashDecimalVectFunc<V>(vector, start, rowCount, combinedHash);
}

template <typename V, typename D>
void IsSameNodeFuncImpl(BaseVector *vector, const uint32_t offset, const ValueState &slot, bool &isSame)
{
    bool isIntermediateNull = (slot.count == 0);
    bool isInputNull = vector->IsNull(offset);
    if (!isInputNull && !isIntermediateNull) {
        auto intTmp = static_cast<V *>(vector)->GetValue(offset);
        if constexpr (std::is_same_v<D, Decimal128> || std::is_floating_point_v<D>) {
            isSame = (intTmp == *reinterpret_cast<D *>(slot.val));
        } else {
            isSame = (intTmp == static_cast<D>(slot.val));
        }

        return;
    }
    if (isInputNull != isIntermediateNull) {
        isSame = false;
        return;
    }
    isSame = true;
    return;
}

template <typename V = Vector<LargeStringContainer<std::string_view>>>
void IsSameNodeFuncVarcharImpl(BaseVector *vector, const uint32_t offset, const ValueState &slot, bool &isSame)
{
    bool isIntermediateNull = (slot.val == 0);
    bool isInputNull = vector->IsNull(offset);
    if (!isInputNull && !isIntermediateNull) {
        std::string_view str = static_cast<V *>(vector)->GetValue(offset);
        auto valLen = str.size();
        isSame = (static_cast<int64_t>(valLen) == slot.count) &&
            (memcmp(str.data(), reinterpret_cast<void *>(slot.val), valLen) == 0);
        return;
    }
    if (isInputNull != isIntermediateNull) {
        isSame = false;
        return;
    }
    isSame = true;
    return;
}

template <typename V, typename D>
void DuplicateKeyValueImpl(ValueState &state, BaseVector *vector, const uint32_t offset, ExecutionContext *context)
{
    if (vector->IsNull(offset)) {
        return;
    }

    D data = static_cast<V *>(vector)->GetValue(offset);
    if constexpr (std::is_floating_point_v<D> || std::is_same_v<D, Decimal128>) {
        constexpr auto len = sizeof(D);
        uint8_t *ptr = context->GetArena()->Allocate(len);
        memcpy_s(ptr, len, &data, len);
        state.val = reinterpret_cast<int64_t>(ptr);
    } else {
        state.val = data;
    }
    state.count = 1;
}

template <typename V = Vector<LargeStringContainer<std::string_view>>>
void DuplicateVarcharKeyValue(ValueState &state, BaseVector *vector, const uint32_t offset, ExecutionContext *context)
{
    if (vector->IsNull(offset)) {
        return;
    }

    std::string_view str = static_cast<V *>(vector)->GetValue(offset);
    int32_t valLen = str.size();
    uint8_t *tmp = reinterpret_cast<uint8_t *>(const_cast<char *>(str.data()));
    uint8_t *data = context->GetArena()->Allocate(valLen);
    memcpy_s(data, valLen, tmp, static_cast<size_t>(valLen));
    state.val = reinterpret_cast<int64_t>(data);
    state.count = valLen;
}

template <typename V> void SetVectorImpl(VectorBatch *vecBatch, int32_t rowCount)
{
    vecBatch->Append(new V(rowCount));
}

void SetVarcharVector(VectorBatch *vecBatch, int32_t rowCount);
void SetContainerVector(VectorBatch *vecBatch, int32_t rowCount);

template <typename V, typename D> void FillValueImpl(BaseVector *v, int32_t rowIndex, ValueState &state)
{
    if (state.count == 0) {
        static_cast<V *>(v)->SetNull(rowIndex);
    } else {
        if constexpr (std::is_same_v<D, Decimal128> || std::is_floating_point_v<D>) {
            static_cast<V *>(v)->SetValue(rowIndex, *reinterpret_cast<D *>(state.val));
        } else {
            static_cast<V *>(v)->SetValue(rowIndex, static_cast<D>(state.val));
        }
    }
}
}
}
#endif // OMNI_RUNTIME_DISTINCT_STATE_FUNC_H
