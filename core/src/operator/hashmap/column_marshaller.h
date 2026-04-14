/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */

#ifndef OMNI_RUNTIME_COLUMN_MARSHALLER_H
#define OMNI_RUNTIME_COLUMN_MARSHALLER_H

#include <cstdint>
#include <type_traits>
#include <utility>
#include "vector/vector_helper.h"
#include "type/string_ref.h"
#include "type/data_type.h"
#include "operator/hashmap/base_hash_map.h"
#include "operator/execution_context.h"
#include "vector_marshaller.h"

namespace omniruntime {
namespace op {
using namespace vec;
enum class HandleType {
    serialize,
    fixedInt16,
    fixedInt32,
    fixedInt64,
    packedInt32,
    packedInt64,
    packedInt128,
    fixed256Bytes,
    onlyOneKey
};

template <typename Hashmap> class ColumnSerializeHandler {
public:
    Hashmap hashmap;
    static constexpr bool HasSpecialNullFunc = false;
    using KeyType = typename Hashmap::Keys;
    using ValueType = typename Hashmap::Values;
    using Result = typename Hashmap::ResultType;
    ColumnSerializeHandler(uint8_t initDegree = 16) : hashmap(initDegree) {}

    Result InsertValueToHashmap(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx,
        mem::SimpleArenaAllocator &arenaAllocator)
    {
        type::StringRef key;
        for (int32_t groupColIdx = 0; groupColIdx < groupColNum; groupColIdx++) {
            auto curVector = groupVectors[groupColIdx];
            auto &curFunc = serializers[groupColIdx];
            curFunc(curVector, rowIdx, arenaAllocator, key);
        }
        return hashmap.Emplace(key);
    }

    Result InsertDictValueToHashmap(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx,
                                mem::SimpleArenaAllocator &arenaAllocator)
    {
        type::StringRef key;
        for (int32_t groupColIdx = 0; groupColIdx < groupColNum; groupColIdx++) {
            auto curVector = groupVectors[groupColIdx];
            auto &curFunc = serializers[groupColIdx];
            curFunc(curVector, rowIdx, arenaAllocator, key);
        }
        return hashmap.Emplace(key);
    }

    Result InsertConstValueToHashmap(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx,
                                mem::SimpleArenaAllocator &arenaAllocator)
    {
        type::StringRef key;
        for (int32_t groupColIdx = 0; groupColIdx < groupColNum; groupColIdx++) {
            auto curVector = groupVectors[groupColIdx];
            auto &curFunc = serializers[groupColIdx];
            curFunc(curVector, rowIdx, arenaAllocator, key);
        }
        return hashmap.Emplace(key);
    }

    ALWAYS_INLINE void TryToInsertJoinKeysToHashmap(BaseVector **joinVectors, int32_t joinColNum, int32_t rowIdx,
        int32_t i, mem::SimpleArenaAllocator &arenaAllocator, std::vector<type::StringRef> &keys,
        std::vector<int8_t> &isNotNullKeys)
    {
        keys[i].size = 0;
        keys[i].data = nullptr;
        for (int32_t joinColIdx = 0; joinColIdx < joinColNum; joinColIdx++) {
            auto curVector = joinVectors[joinColIdx];
            auto &curFunc = ignoreNullSerializers[joinColIdx];
            if (UNLIKELY(!curFunc(curVector, rowIdx, arenaAllocator, keys[i]))) {
                isNotNullKeys[i] = false;
                return;
            }
        }
        isNotNullKeys[i] = true;
    }

    ALWAYS_INLINE void TryToInsertFixedJoinKeysToHashmap(BaseVector **joinVectors, int32_t joinColNum, int32_t rowIdx,
        type::StringRef &key, bool &isNotNullKey)
    {
        size_t pos = 0;
        for (int32_t groupColIdx = 0; groupColIdx < joinColNum; groupColIdx++) {
            auto curVector = joinVectors[groupColIdx];
            auto &curFunc = fixedKeysIgnoreNullSerializers[groupColIdx];
            if (UNLIKELY(!curFunc(curVector, rowIdx, key, pos))) {
                isNotNullKey = false;
                return;
            }
        }
        isNotNullKey = true;
    }

    ALWAYS_INLINE void TryToInsertFixedJoinKeysToHashmapSimd(BaseVector **joinVectors, int32_t joinRowNum,
        int32_t colIdx, std::vector<type::StringRef> &keys, std::vector<bool> &isNotNullKey, size_t &pos)
    {
        auto &curFunc = fixedKeysIgnoreNullSerializersSimd[colIdx];
        auto curVector = joinVectors[colIdx];
        for (int32_t rowid = 0; rowid < joinRowNum; rowid++) {
            if (UNLIKELY(!curFunc(curVector, rowid, keys, pos, joinRowNum))) {
                isNotNullKey[rowid] = false;
            }
            isNotNullKey[rowid] = isNotNullKey[rowid] & true;
        }
    }

    ALWAYS_INLINE void BatchCalculateHash(std::vector<KeyType> &keys, std::vector<int8_t> &isNotNullKeys,
        std::vector<size_t> &hashes, int32_t maxStep)
    {
        for (int i = 0; i < maxStep; ++i) {
            if (LIKELY(isNotNullKeys[i])) {
                hashes[i] = hashmap.CalculateHash(keys[i]);
            }
        }
    }

    ALWAYS_INLINE Result InsertJoinKeysToHashmap(KeyType &key)
    {
        return hashmap.EmplaceNotNullKey(key);
    }

    ALWAYS_INLINE Result InsertNullKeysToHashmap(KeyType &key)
    {
        return hashmap.EmplaceNullValue(key);
    }

    ALWAYS_INLINE Result InsertJoinKeysToHashmap(KeyType &key, size_t &hashValue)
    {
        return hashmap.EmplaceNotNullKey(key, hashValue);
    }

    void ParseKeyToCols(const KeyType &key, std::vector<vec::BaseVector *> &groupOutputVectors, int32_t groupColNum,
        const int32_t rowIdx)
    {
        auto *pos = key.data;
        for (int32_t i = 0; i < groupColNum; ++i) {
            auto curVectorPtr = groupOutputVectors[i];
            auto deserializeFunc = deserializers[i];
            pos = deserializeFunc(curVectorPtr, rowIdx, pos);
        }
    }

    ALWAYS_INLINE Result FindValueFromHashmap(KeyType &key)
    {
        return hashmap.FindMatchPosition(key);
    }

    void InitSize(int groupBySize)
    {
        serializers.reserve(groupBySize);
        deserializers.reserve(groupBySize);
    }

    void ResetSerializer()
    {
        serializers.clear();
        deserializers.clear();
    }

    void ResetIgnoreNullSerializer()
    {
        ignoreNullSerializers.clear();
    }

    void ResetFixedKeysIgnoreNullSerializer()
    {
        fixedKeysIgnoreNullSerializers.clear();
    }

    void ResetFixedKeysIgnoreNullSerializerSimd()
    {
        fixedKeysIgnoreNullSerializersSimd.clear();
    }

    void PushBackSerializer(VectorSerializer &serializer)
    {
        serializers.push_back(serializer);
    }

    void PushBackIgnoreNullSerializer(VectorSerializerIgnoreNull &serializer)
    {
        ignoreNullSerializers.push_back(serializer);
    }

    void PushBackFixedKeysIgnoreNullSerializer(FixedKeyVectorSerializerIgnoreNull &serializer)
    {
        fixedKeysIgnoreNullSerializers.push_back(serializer);
    }

    void PushBackFixedKeysIgnoreNullSerializerSimd(FixedKeyVectorSerializerIgnoreNullSimd &serializer)
    {
        fixedKeysIgnoreNullSerializersSimd.push_back(serializer);
    }

    void PushBackDeSerializer(VectorDeSerializer &deserializer)
    {
        deserializers.push_back(deserializer);
    }

    size_t GetElementsSize() const
    {
        return hashmap.GetElementsSize();
    }

    void ResetHashmap()
    {
        hashmap.Reset();
    };

private:
    std::vector<VectorSerializer> serializers;
    std::vector<VectorDeSerializer> deserializers;

    std::vector<VectorSerializerIgnoreNull> ignoreNullSerializers;
    std::vector<FixedKeyVectorSerializerIgnoreNull> fixedKeysIgnoreNullSerializers;

    std::vector<FixedKeyVectorSerializerIgnoreNullSimd> fixedKeysIgnoreNullSerializersSimd;
};

template <typename Hashmap, typename T>
class GroupbySingleFixHandler {
public:
    static constexpr bool HasSpecialNullFunc = true;
    Hashmap hashmap;
    using Result = typename Hashmap::ResultType;

    Result InsertValueToHashmap(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx,
        mem::SimpleArenaAllocator &arenaAllocator)
    {
        auto *curVector = groupVectors[0];
        if (curVector->IsNull(rowIdx)) {
            T value = 0;
            return hashmap.EmplaceNullValue(value);
        }
        return hashmap.Emplace(reinterpret_cast<Vector<T>*>(curVector)->GetValue(rowIdx));
    }

    Result InsertDictValueToHashmap(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx,
                                mem::SimpleArenaAllocator &arenaAllocator)
    {
        auto *curVector = groupVectors[0];
        if (curVector->IsNull(rowIdx)) {
            T value = 0;
            return hashmap.EmplaceNullValue(value);
        }
        auto dictionaryVector = static_cast<Vector<DictionaryContainer<T>> *>(curVector);
        auto value = dictionaryVector->GetValue(rowIdx);
        return hashmap.Emplace(value);
    }

    Result InsertConstValueToHashmap(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx,
                                mem::SimpleArenaAllocator &arenaAllocator)
    {
        auto *curVector = groupVectors[0];
        if (curVector->IsNull(rowIdx)) {
            T value = 0;
            return hashmap.EmplaceNullValue(value);
        }
        auto constVector = static_cast<ConstVector<T> *>(curVector);
        return hashmap.Emplace(constVector->GetConstValue());
    }

    template<bool isNull>
    Result InsertOneValueToHashmap(T value)
    {
        if constexpr (isNull) {
            value = 0;
            return hashmap.EmplaceNullValue(value);
        }
        return hashmap.Emplace(value);
    }

    void ParseKeyToCols(const T &key, std::vector<vec::BaseVector *> &groupOutputVectors, int32_t groupColNum,
        const int rowIdx)
    {
        auto curVectorPtr = groupOutputVectors[0];
        if (curVectorPtr->GetEncoding() == Encoding::OMNI_DICTIONARY) {
            auto dictionaryVector = reinterpret_cast<Vector<DictionaryContainer<T>> *>(curVectorPtr);
            dictionaryVector->SetValue(rowIdx, key);
        } else {
            reinterpret_cast<Vector<T>*>(curVectorPtr)->SetValue(rowIdx, key);
        }
    }

    void ParseNull(const T &key, std::vector<vec::BaseVector *> &groupOutputVectors, int32_t groupColNum,
        const int rowIdx)
    {
        auto curVectorPtr = groupOutputVectors[0];
        curVectorPtr->SetNull(rowIdx);
    }

    size_t GetElementsSize() const
    {
        return hashmap.GetElementsSize();
    }

    void ResetHashmap()
    {
        hashmap.Reset();
    }
};

template <typename Hashmap, typename KeyType>
class GroupbyPackedFixHandler {
public:
    static constexpr bool HasSpecialNullFunc = false;
    Hashmap hashmap;
    using Result = typename Hashmap::ResultType;

    explicit GroupbyPackedFixHandler(std::vector<int32_t> typeIds, std::vector<uint8_t> bitWidths)
        : hashmap(16)
    {
        InitPlan(std::move(typeIds), std::move(bitWidths));
    }

    Result InsertValueToHashmap(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx,
        mem::SimpleArenaAllocator &arenaAllocator)
    {
        (void)arenaAllocator;
        auto key = PackKey(groupVectors, groupColNum, rowIdx);
        return hashmap.Emplace(key);
    }

    Result InsertDictValueToHashmap(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx,
        mem::SimpleArenaAllocator &arenaAllocator)
    {
        (void)arenaAllocator;
        auto key = PackKey(groupVectors, groupColNum, rowIdx);
        return hashmap.Emplace(key);
    }

    Result InsertConstValueToHashmap(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx,
        mem::SimpleArenaAllocator &arenaAllocator)
    {
        (void)arenaAllocator;
        auto key = PackKey(groupVectors, groupColNum, rowIdx);
        return hashmap.Emplace(key);
    }

    void Prepare(BaseVector **groupVectors, int32_t groupColNum)
    {
        for (int32_t col = 0; col < groupColNum; ++col) {
            const auto encoding = groupVectors[col]->GetEncoding();
            if (encoding == Encoding::OMNI_DICTIONARY) {
                plan[col].activeLoader = plan[col].dictLoader;
            } else if (encoding == Encoding::OMNI_ENCODING_CONST) {
                plan[col].activeLoader = plan[col].constLoader;
            } else {
                plan[col].activeLoader = plan[col].flatLoader;
            }
        }
    }

    void ParseKeyToCols(const KeyType &key, std::vector<vec::BaseVector *> &groupOutputVectors, int32_t groupColNum,
        const int rowIdx)
    {
        UnpackKey(key, groupColNum);
        for (int32_t col = 0; col < groupColNum; ++col) {
            auto *outVector = groupOutputVectors[col];
            if (unpackIsNull[col]) {
                outVector->SetNull(rowIdx);
                continue;
            }
            SetValueByType(outVector, rowIdx, plan[col].typeId, unpackValues[col]);
        }
    }

    size_t GetElementsSize() const
    {
        return hashmap.GetElementsSize();
    }

    void ResetHashmap()
    {
        hashmap.Reset();
    }

private:
    using UnsignedKey = std::conditional_t<std::is_same_v<KeyType, omniruntime::type::int128_t>,
        __uint128_t, std::make_unsigned_t<KeyType>>;

    using LoaderFn = UnsignedKey (*)(BaseVector *vector, int32_t rowIdx, UnsignedKey mask);

    struct PlanEntry {
        int32_t typeId = OMNI_INVALID;
        uint8_t bitWidth = 0;
        UnsignedKey mask = 0;
        LoaderFn flatLoader = nullptr;
        LoaderFn dictLoader = nullptr;
        LoaderFn constLoader = nullptr;
        LoaderFn activeLoader = nullptr;
    };

    std::vector<PlanEntry> plan;
    mutable std::vector<uint8_t> unpackIsNull;
    mutable std::vector<UnsignedKey> unpackValues;

    static ALWAYS_INLINE UnsignedKey MaskForWidth(uint8_t width)
    {
        if (width == 0) {
            return 0;
        }
        constexpr uint8_t kBits = static_cast<uint8_t>(sizeof(UnsignedKey) * 8);
        if (width >= kBits) {
            return static_cast<UnsignedKey>(~static_cast<UnsignedKey>(0));
        }
        return (static_cast<UnsignedKey>(1) << width) - 1;
    }

    template<typename T, bool isDict>
    static ALWAYS_INLINE UnsignedKey LoadBits(BaseVector *vector, int32_t rowIdx, UnsignedKey mask)
    {
        if constexpr (isDict) {
            auto v = reinterpret_cast<Vector<DictionaryContainer<T>> *>(vector)->GetValue(rowIdx);
            return static_cast<UnsignedKey>(static_cast<std::make_unsigned_t<T>>(v)) & mask;
        } else {
            auto v = reinterpret_cast<Vector<T> *>(vector)->GetValue(rowIdx);
            return static_cast<UnsignedKey>(static_cast<std::make_unsigned_t<T>>(v)) & mask;
        }
    }

    template<typename T>
    static ALWAYS_INLINE UnsignedKey LoadBitsConst(BaseVector *vector, int32_t rowIdx, UnsignedKey mask)
    {
        (void)rowIdx;
        auto v = reinterpret_cast<ConstVector<T> *>(vector)->GetConstValue();
        return static_cast<UnsignedKey>(static_cast<std::make_unsigned_t<T>>(v)) & mask;
    }

    static ALWAYS_INLINE void SetValueByType(BaseVector *vector, int32_t rowIdx, int32_t typeId, UnsignedKey value)
    {
        switch (typeId) {
            case OMNI_BYTE:
                reinterpret_cast<Vector<int8_t> *>(vector)->SetValue(rowIdx, static_cast<int8_t>(value));
                break;
            case OMNI_SHORT:
                reinterpret_cast<Vector<int16_t> *>(vector)->SetValue(rowIdx, static_cast<int16_t>(value));
                break;
            case OMNI_INT:
            case OMNI_DATE32:
            case OMNI_TIME32:
                reinterpret_cast<Vector<int32_t> *>(vector)->SetValue(rowIdx, static_cast<int32_t>(value));
                break;
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
            case OMNI_DATE64:
            case OMNI_TIME64:
                reinterpret_cast<Vector<int64_t> *>(vector)->SetValue(rowIdx, static_cast<int64_t>(value));
                break;
            default:
                break;
        }
    }

    ALWAYS_INLINE KeyType PackKey(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx) const
    {
        UnsignedKey packed = 0;
        for (int32_t col = 0; col < groupColNum; ++col) {
            auto &entry = plan[col];
            bool isNull = groupVectors[col]->IsNull(rowIdx);
            packed = (packed << 1) | static_cast<UnsignedKey>(isNull ? 1 : 0);
            UnsignedKey valueBits = 0;
            if (!isNull) {
                valueBits = entry.activeLoader(groupVectors[col], rowIdx, entry.mask);
            }
            packed = (packed << entry.bitWidth) | valueBits;
        }
        return static_cast<KeyType>(packed);
    }

    ALWAYS_INLINE void UnpackKey(const KeyType &key, int32_t groupColNum) const
    {
        UnsignedKey packed = static_cast<UnsignedKey>(key);
        if (UNLIKELY(static_cast<size_t>(groupColNum) != plan.size())) {
            unpackIsNull.resize(groupColNum);
            unpackValues.resize(groupColNum);
        }
        for (int32_t col = groupColNum - 1; col >= 0; --col) {
            auto width = plan[col].bitWidth;
            auto mask = plan[col].mask;
            unpackValues[col] = packed & mask;
            packed >>= width;
            unpackIsNull[col] = static_cast<uint8_t>(packed & 1);
            packed >>= 1;
        }
    }

    void InitPlan(std::vector<int32_t> typeIds, std::vector<uint8_t> bitWidths)
    {
        plan.resize(typeIds.size());
        unpackIsNull.resize(typeIds.size());
        unpackValues.resize(typeIds.size());
        for (size_t i = 0; i < typeIds.size(); ++i) {
            plan[i].typeId = typeIds[i];
            plan[i].bitWidth = bitWidths[i];
            plan[i].mask = MaskForWidth(bitWidths[i]);
            switch (typeIds[i]) {
                case OMNI_BYTE:
                    plan[i].flatLoader = &LoadBits<int8_t, false>;
                    plan[i].dictLoader = &LoadBits<int8_t, true>;
                    plan[i].constLoader = &LoadBitsConst<int8_t>;
                    break;
                case OMNI_SHORT:
                    plan[i].flatLoader = &LoadBits<int16_t, false>;
                    plan[i].dictLoader = &LoadBits<int16_t, true>;
                    plan[i].constLoader = &LoadBitsConst<int16_t>;
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                case OMNI_TIME32:
                    plan[i].flatLoader = &LoadBits<int32_t, false>;
                    plan[i].dictLoader = &LoadBits<int32_t, true>;
                    plan[i].constLoader = &LoadBitsConst<int32_t>;
                    break;
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64:
                case OMNI_DATE64:
                case OMNI_TIME64:
                    plan[i].flatLoader = &LoadBits<int64_t, false>;
                    plan[i].dictLoader = &LoadBits<int64_t, true>;
                    plan[i].constLoader = &LoadBitsConst<int64_t>;
                    break;
                default:
                    plan[i].flatLoader = nullptr;
                    plan[i].dictLoader = nullptr;
                    plan[i].constLoader = nullptr;
                    break;
            }
            plan[i].activeLoader = plan[i].flatLoader;
        }
    }
};
}
}
#endif // OMNI_RUNTIME_COLUMN_MARSHALLER_H
