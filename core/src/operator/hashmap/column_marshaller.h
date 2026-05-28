/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 */

#ifndef OMNI_RUNTIME_COLUMN_MARSHALLER_H
#define OMNI_RUNTIME_COLUMN_MARSHALLER_H

#include <cstdint>
#include <type_traits>
#include <utility>
#include <algorithm>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <vector>
#include <folly/container/F14Map.h>
#include "vector/vector_helper.h"
#include "type/string_ref.h"
#include "type/data_type.h"
#include "type/integer256.h"
#include "operator/omni_id_type_vector_traits.h"
#include "operator/hashmap/base_hash_map.h"
#include "operator/execution_context.h"
#include "vector/unsafe_vector.h"
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
    multiNormalize,
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

#define OMNI_NK_FIXED_TYPE_DISPATCH(CALLBACK, typeId, ...)                                          \
    [&]() {                                                                                         \
        switch (typeId) {                                                                           \
            case type::OMNI_BYTE:                                                                   \
                return CALLBACK<type::OMNI_BYTE>(__VA_ARGS__);                                      \
            case type::OMNI_SHORT:                                                                  \
                return CALLBACK<type::OMNI_SHORT>(__VA_ARGS__);                                     \
            case type::OMNI_INT:                                                                    \
                return CALLBACK<type::OMNI_INT>(__VA_ARGS__);                                       \
            case type::OMNI_DATE32:                                                                 \
                return CALLBACK<type::OMNI_DATE32>(__VA_ARGS__);                                    \
            case type::OMNI_LONG:                                                                   \
                return CALLBACK<type::OMNI_LONG>(__VA_ARGS__);                                      \
            case type::OMNI_TIMESTAMP:                                                              \
                return CALLBACK<type::OMNI_TIMESTAMP>(__VA_ARGS__);                                 \
            case type::OMNI_DECIMAL64:                                                              \
                return CALLBACK<type::OMNI_DECIMAL64>(__VA_ARGS__);                                 \
            default:                                                                                \
                throw omniruntime::exception::OmniException(                                        \
                    "UNSUPPORTED_ERROR",                                                           \
                    "GroupbyMultiNormalizeKeyHandler unsupported group by type");                   \
        }                                                                                           \
    }()

template <typename Hashmap>
class GroupbyMultiNormalizeKeyHandler {
public:
    static constexpr bool HasSpecialNullFunc = true;
    using KeyType = typename Hashmap::Keys;
    using Result = typename Hashmap::ResultType;
    using SignedKey = omniruntime::type::int128_t;
    using EncodedKey = omniruntime::type::uint128_t;

    static bool CanUse(const std::vector<type::DataTypeId> &groupByTypeIds)
    {
        return BuildBitLayout(groupByTypeIds, nullptr, nullptr, nullptr, nullptr);
    }

    bool Init(const std::vector<type::DataTypeId> &groupByTypeIds)
    {
        colCount_ = static_cast<int32_t>(groupByTypeIds.size());
        if (colCount_ <= 1) {
            return false;
        }
        typeIds_.clear();
        typeIds_.reserve(colCount_);
        for (const auto &typeId : groupByTypeIds) {
            if (!IsSupportedType(typeId)) {
                return false;
            }
            typeIds_.push_back(typeId);
        }
        if (!InitBitLayout()) {
            return false;
        }
        valueToId_.assign(colCount_, {});
        idToValue_.assign(colCount_, {});
        hasRange_.assign(colCount_, false);
        isRange_.assign(colCount_, false);
        min_.assign(colCount_, 0);
        max_.assign(colCount_, 0);
        for (int32_t col = 0; col < colCount_; ++col) {
            InitRangeModeForType(col, typeIds_[col]);
        }
        return true;
    }

    static constexpr KeyType kEncodeFailure = static_cast<KeyType>(EncodedKey(1) << 127);

    Result InsertValueToHashmap(
        BaseVector **groupVectors,
        int32_t groupColNum,
        int32_t rowIdx,
        mem::SimpleArenaAllocator &arenaAllocator)
    {
        KeyType encoded = 0;
        if (!TryEncode(groupVectors, groupColNum, rowIdx, encoded)) {
            unmappable_ = true;
            return hashmap.Emplace(kEncodeFailure);
        }
        return hashmap.Emplace(encoded);
    }

    Result InsertDictValueToHashmap(
        BaseVector **groupVectors,
        int32_t groupColNum,
        int32_t rowIdx,
        mem::SimpleArenaAllocator &arenaAllocator)
    {
        KeyType encoded = 0;
        if (!TryEncode(groupVectors, groupColNum, rowIdx, encoded)) {
            unmappable_ = true;
            return hashmap.Emplace(kEncodeFailure);
        }
        return hashmap.Emplace(encoded);
    }

    Result InsertConstValueToHashmap(
        BaseVector **groupVectors,
        int32_t groupColNum,
        int32_t rowIdx,
        mem::SimpleArenaAllocator &arenaAllocator)
    {
        KeyType encoded = 0;
        if (!TryEncode(groupVectors, groupColNum, rowIdx, encoded)) {
            unmappable_ = true;
            return hashmap.Emplace(kEncodeFailure);
        }
        return hashmap.Emplace(encoded);
    }

    bool IsUnmappable() const { return unmappable_; }

    void ResetEncodeModeStatsForBatch()
    {
        batchUsedRangePath_ = false;
        batchUsedMapPath_ = false;
    }

    bool BatchUsedRangePath() const
    {
        return batchUsedRangePath_;
    }

    bool BatchUsedMapPath() const
    {
        return batchUsedMapPath_;
    }

    void WarmupRangeWindows(
        BaseVector **groupVectors,
        int32_t groupColNum,
        int32_t rowCount,
        int32_t sampleRows)
    {
        if (groupColNum != colCount_ || rowCount <= 0 || sampleRows <= 0) {
            return;
        }
        if (!PrepareBatchEncodePlans(groupVectors, groupColNum)) {
            return;
        }
        WarmupRangeWindows(batchEncodePlans_, rowCount, sampleRows);
    }

    void ParseKeyToCols(
        const KeyType &key,
        std::vector<vec::BaseVector *> &groupOutputVectors,
        int32_t groupColNum,
        const int rowIdx)
    {
        EncodedKey encoded = static_cast<EncodedKey>(key);
        for (int32_t col = 0; col < groupColNum; ++col) {
            const auto id = (encoded >> bitOffsets_[col]) & idMasks_[col];
            auto *vector = groupOutputVectors[col];
            if (id == 0) {
                vector->SetNull(rowIdx);
                continue;
            }
            if (isRange_[col]) {
                SignedKey value = ToSigned(min_[col]) + static_cast<SignedKey>(id) - 1;
                if (value < std::numeric_limits<int64_t>::min() || value > std::numeric_limits<int64_t>::max()) {
                    throw omniruntime::exception::OmniException(
                        "INVALID_STATE",
                        "GroupbyMultiNormalizeKeyHandler decoded range value out of int64 range");
                }
                SetNormalizedFixedValue(vector, rowIdx, typeIds_[col], static_cast<int64_t>(value));
                continue;
            }
            if (id > static_cast<EncodedKey>(std::numeric_limits<size_t>::max())) {
                throw omniruntime::exception::OmniException(
                    "INVALID_STATE",
                    "GroupbyMultiNormalizeKeyHandler decode id out of size_t range");
            }
            const auto valueIndex = static_cast<size_t>(id - 1);
            if (valueIndex >= idToValue_[col].size()) {
                throw omniruntime::exception::OmniException(
                    "INVALID_STATE",
                    "GroupbyMultiNormalizeKeyHandler decode id out of range");
            }
            SetNormalizedFixedValue(vector, rowIdx, typeIds_[col], idToValue_[col][valueIndex]);
        }
    }

    void ParseNull(
        const KeyType &key,
        std::vector<vec::BaseVector *> &groupOutputVectors,
        int32_t groupColNum,
        const int rowIdx)
    {
        for (int32_t col = 0; col < groupColNum; ++col) {
            groupOutputVectors[col]->SetNull(rowIdx);
        }
    }

    size_t GetElementsSize() const
    {
        return hashmap.GetElementsSize();
    }

    void ResetHashmap()
    {
        hashmap.Reset();
        unmappable_ = false;
        unmappableLogged_ = false;
    }

    bool TryEncode(
        BaseVector **groupVectors,
        int32_t groupColNum,
        int32_t rowIdx,
        KeyType &encoded,
        const uint8_t *columnHasNull = nullptr)
    {
        if (groupColNum != colCount_) {
            throw omniruntime::exception::OmniException(
                "INVALID_STATE",
                "GroupbyMultiNormalizeKeyHandler group column count mismatch");
        }
        EncodedKey packed = 0;
        for (int32_t col = 0; col < groupColNum; ++col) {
            auto *vector = groupVectors[col];
            if (UNLIKELY(vector == nullptr)) {
                return false;
            }
            const auto enc = vector->GetEncoding();
            if (enc != vec::OMNI_FLAT && enc != vec::OMNI_DICTIONARY && enc != vec::OMNI_ENCODING_CONST) {
                return false;
            }
            EncodedKey id = 0;
            bool isNull = columnHasNull == nullptr ? vector->IsNull(rowIdx)
                                                   : (columnHasNull[col] != 0 && vector->IsNull(rowIdx));
            if (!isNull) {
                const int64_t value = ReadNormalizedFixedValue(vector, typeIds_[col], rowIdx);
                id = valueId(col, value);
                if (id == kUnmappable) {
                    return false;
                }
            }
            if (id > idMasks_[col]) {
                return false;
            }
            packed |= id << bitOffsets_[col];
        }
        encoded = static_cast<KeyType>(packed);
        return true;
    }

    bool TryEncodeBatch(
        BaseVector **groupVectors,
        int32_t groupColNum,
        int32_t rowCount,
        int32_t warmupSampleRows)
    {
        if (groupColNum != colCount_) {
            throw omniruntime::exception::OmniException(
                "INVALID_STATE", "GroupbyMultiNormalizeKeyHandler group column count mismatch");
        }
        if (UNLIKELY(rowCount < 0)) {
            return false;
        }
        if (!PrepareBatchEncodePlans(groupVectors, groupColNum)) {
            return false;
        }
        WarmupRangeWindows(batchEncodePlans_, rowCount, warmupSampleRows);
        if (encodedKeysBuffer_.size() < static_cast<size_t>(rowCount)) {
            encodedKeysBuffer_.resize(static_cast<size_t>(rowCount));
        }
        std::fill_n(encodedKeysBuffer_.begin(), rowCount, KeyType(0));
        for (const auto &plan : batchEncodePlans_) {
            auto fn = PickEnc(typeIds_[plan.col], plan.vector->GetEncoding());
            if (fn == nullptr || !fn(this, plan, rowCount, encodedKeysBuffer_.data())) {
                return false;
            }
        }
        return true;
    }

    std::vector<KeyType> &GetEncodedKeys()
    {
        return encodedKeysBuffer_;
    }

    Hashmap hashmap;

private:
    struct BatchEncodePlan;
    using EncFn = bool (*)(GroupbyMultiNormalizeKeyHandler *, const BatchEncodePlan &, int32_t, KeyType *);
    using PreparedReadFn = int64_t (*)(const BatchEncodePlan &, int32_t);

    struct BatchEncodePlan {
        BaseVector *vector = nullptr;
        PreparedReadFn readValue = nullptr;
        const void *values = nullptr;
        const int32_t *ids = nullptr;
        int64_t constValue = 0;
        EncodedKey mask = 0;
        uint8_t offset = 0;
        int32_t col = 0;
        bool hasNull = false;
    };

    static constexpr uint8_t kTotalKeyBits = 127;
    static constexpr EncodedKey kUnmappable = ~static_cast<EncodedKey>(0);

    static bool IsSupportedType(type::DataTypeId typeId)
    {
        return typeId == type::OMNI_BYTE || typeId == type::OMNI_SHORT ||
            typeId == type::OMNI_INT || typeId == type::OMNI_DATE32 ||
            typeId == type::OMNI_LONG || typeId == type::OMNI_TIMESTAMP ||
            typeId == type::OMNI_DECIMAL64;
    }

    template<type::DataTypeId TypeId>
    static int64_t ReadNormalizedFixedValueTyped(BaseVector *vector, int32_t rowIdx)
    {
        using NativeType = typename NativeAndVectorType<TypeId>::type;
        switch (vector->GetEncoding()) {
            case vec::OMNI_DICTIONARY:
                return static_cast<int64_t>(
                    static_cast<Vector<DictionaryContainer<NativeType>> *>(vector)->GetValue(rowIdx));
            case vec::OMNI_ENCODING_CONST:
                return static_cast<int64_t>(
                    static_cast<ConstVector<NativeType> *>(vector)->GetConstValue());
            case vec::OMNI_FLAT:
                return static_cast<int64_t>(
                    static_cast<Vector<NativeType> *>(vector)->GetValue(rowIdx));
            default:
                break;
        }
        throw omniruntime::exception::OmniException(
            "UNSUPPORTED_ERROR",
            "GroupbyMultiNormalizeKeyHandler unsupported vector encoding");
    }

    static int64_t ReadNormalizedFixedValue(BaseVector *vector, type::DataTypeId typeId, int32_t rowIdx)
    {
        return OMNI_NK_FIXED_TYPE_DISPATCH(ReadNormalizedFixedValueTyped, typeId, vector, rowIdx);
    }

    template<type::DataTypeId TypeId>
    static ALWAYS_INLINE int64_t ReadPreparedFlat(const BatchEncodePlan &plan, int32_t rowIdx)
    {
        using NativeType = typename NativeAndVectorType<TypeId>::type;
        return static_cast<int64_t>(static_cast<const NativeType *>(plan.values)[rowIdx]);
    }

    template<type::DataTypeId TypeId>
    static ALWAYS_INLINE int64_t ReadPreparedDict(const BatchEncodePlan &plan, int32_t rowIdx)
    {
        using NativeType = typename NativeAndVectorType<TypeId>::type;
        const auto *dictionary = static_cast<const NativeType *>(plan.values);
        return static_cast<int64_t>(dictionary[plan.ids[rowIdx]]);
    }

    static ALWAYS_INLINE int64_t ReadPreparedConst(const BatchEncodePlan &plan, int32_t rowIdx)
    {
        (void)rowIdx;
        return plan.constValue;
    }

    static ALWAYS_INLINE bool EncodePrepared(
        GroupbyMultiNormalizeKeyHandler *handler,
        const BatchEncodePlan &plan,
        int32_t rowCount,
        KeyType *keys)
    {
        if (plan.hasNull) {
            for (int32_t rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
                if (plan.vector->IsNull(rowIdx)) {
                    continue;
                }
                const EncodedKey id = handler->valueId(plan.col, plan.readValue(plan, rowIdx));
                if (id == kUnmappable || id > plan.mask) {
                    return false;
                }
                keys[rowIdx] = static_cast<KeyType>(static_cast<EncodedKey>(keys[rowIdx]) | (id << plan.offset));
            }
            return true;
        }
        for (int32_t rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
            const EncodedKey id = handler->valueId(plan.col, plan.readValue(plan, rowIdx));
            if (id == kUnmappable || id > plan.mask) {
                return false;
            }
            keys[rowIdx] = static_cast<KeyType>(static_cast<EncodedKey>(keys[rowIdx]) | (id << plan.offset));
        }
        return true;
    }

    template<type::DataTypeId TypeId>
    static EncFn PickTypedEnc(vec::Encoding encoding)
    {
        switch (encoding) {
            case vec::OMNI_FLAT:
                return &EncodePrepared;
            case vec::OMNI_DICTIONARY:
                return &EncodePrepared;
            case vec::OMNI_ENCODING_CONST:
                return &EncodePrepared;
            default:
                return nullptr;
        }
    }

    static EncFn PickEnc(type::DataTypeId typeId, vec::Encoding encoding)
    {
        switch (typeId) {
            case type::OMNI_BYTE:
                return PickTypedEnc<type::OMNI_BYTE>(encoding);
            case type::OMNI_SHORT:
                return PickTypedEnc<type::OMNI_SHORT>(encoding);
            case type::OMNI_INT:
                return PickTypedEnc<type::OMNI_INT>(encoding);
            case type::OMNI_DATE32:
                return PickTypedEnc<type::OMNI_DATE32>(encoding);
            case type::OMNI_LONG:
                return PickTypedEnc<type::OMNI_LONG>(encoding);
            case type::OMNI_TIMESTAMP:
                return PickTypedEnc<type::OMNI_TIMESTAMP>(encoding);
            case type::OMNI_DECIMAL64:
                return PickTypedEnc<type::OMNI_DECIMAL64>(encoding);
            default:
                return nullptr;
        }
    }

    template<type::DataTypeId TypeId>
    static bool InitBatchEncodePlanTyped(BatchEncodePlan &plan, BaseVector *vector, vec::Encoding encoding)
    {
        using NativeType = typename NativeAndVectorType<TypeId>::type;
        plan.vector = vector;
        plan.hasNull = vector->HasNull();
        switch (encoding) {
            case vec::OMNI_FLAT:
                plan.values = unsafe::UnsafeVector::GetRawValues(static_cast<Vector<NativeType> *>(vector));
                plan.readValue = &ReadPreparedFlat<TypeId>;
                return true;
            case vec::OMNI_DICTIONARY: {
                auto *dictVector = static_cast<Vector<DictionaryContainer<NativeType>> *>(vector);
                plan.values = unsafe::UnsafeDictionaryVector::GetDictionary(dictVector);
                plan.ids = unsafe::UnsafeDictionaryVector::GetIds(dictVector);
                plan.readValue = &ReadPreparedDict<TypeId>;
                return true;
            }
            case vec::OMNI_ENCODING_CONST:
                plan.constValue = static_cast<int64_t>(static_cast<ConstVector<NativeType> *>(vector)->GetConstValue());
                plan.readValue = &ReadPreparedConst;
                return true;
            default:
                return false;
        }
    }

    static bool InitBatchEncodePlan(BatchEncodePlan &plan, type::DataTypeId typeId, BaseVector *vector, vec::Encoding encoding)
    {
        switch (typeId) {
            case type::OMNI_BYTE:
                return InitBatchEncodePlanTyped<type::OMNI_BYTE>(plan, vector, encoding);
            case type::OMNI_SHORT:
                return InitBatchEncodePlanTyped<type::OMNI_SHORT>(plan, vector, encoding);
            case type::OMNI_INT:
                return InitBatchEncodePlanTyped<type::OMNI_INT>(plan, vector, encoding);
            case type::OMNI_DATE32:
                return InitBatchEncodePlanTyped<type::OMNI_DATE32>(plan, vector, encoding);
            case type::OMNI_LONG:
                return InitBatchEncodePlanTyped<type::OMNI_LONG>(plan, vector, encoding);
            case type::OMNI_TIMESTAMP:
                return InitBatchEncodePlanTyped<type::OMNI_TIMESTAMP>(plan, vector, encoding);
            case type::OMNI_DECIMAL64:
                return InitBatchEncodePlanTyped<type::OMNI_DECIMAL64>(plan, vector, encoding);
            default:
                return false;
        }
    }

    bool PrepareBatchEncodePlans(BaseVector **groupVectors, int32_t groupColNum)
    {
        batchEncodePlans_.resize(static_cast<size_t>(groupColNum));
        for (int32_t col = 0; col < groupColNum; ++col) {
            auto *vector = groupVectors[col];
            if (UNLIKELY(vector == nullptr)) {
                return false;
            }
            const auto encoding = vector->GetEncoding();
            if (encoding != vec::OMNI_FLAT && encoding != vec::OMNI_DICTIONARY && encoding != vec::OMNI_ENCODING_CONST) {
                return false;
            }
            auto &plan = batchEncodePlans_[col];
            plan = BatchEncodePlan {};
            plan.col = col;
            plan.offset = bitOffsets_[col];
            plan.mask = idMasks_[col];
            if (!InitBatchEncodePlan(plan, typeIds_[col], vector, encoding)) {
                return false;
            }
        }
        return true;
    }

    template<type::DataTypeId TypeId>
    static void SetNormalizedFixedValueTyped(BaseVector *vector, int32_t rowIdx, int64_t value)
    {
        using NativeType = typename NativeAndVectorType<TypeId>::type;
        static_cast<Vector<NativeType> *>(vector)->SetValue(rowIdx, static_cast<NativeType>(value));
    }

    static void SetNormalizedFixedValue(BaseVector *vector, int32_t rowIdx, type::DataTypeId typeId, int64_t value)
    {
        OMNI_NK_FIXED_TYPE_DISPATCH(SetNormalizedFixedValueTyped, typeId, vector, rowIdx, value);
    }

    static uint8_t RequiredFullBits(type::DataTypeId typeId)
    {
        switch (typeId) {
            case type::OMNI_BYTE:
                return 9;  // 256 values + null.
            case type::OMNI_SHORT:
                return 17; // 65536 values + null.
            case type::OMNI_INT:
            case type::OMNI_DATE32:
                return 33; // 2^32 values + null.
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
            case type::OMNI_DECIMAL64:
                return 65; // 2^64 values + null.
            default:
                return 0;
        }
    }

    static uint8_t MinUsefulBits(type::DataTypeId typeId)
    {
        const auto fullBits = RequiredFullBits(typeId);
        if (fullBits == 0) {
            return 0;
        }
        if (fullBits <= 17) {
            return fullBits;
        }
        return static_cast<uint8_t>((static_cast<uint32_t>(fullBits) * 60 + 99) / 100);
    }

    static EncodedKey MaskForBits(uint8_t bits)
    {
        if (bits == 0) {
            return 0;
        }
        if (bits >= 128) {
            return ~static_cast<EncodedKey>(0);
        }
        return (static_cast<EncodedKey>(1) << bits) - 1;
    }

    static bool BuildBitLayout(
        const std::vector<type::DataTypeId> &typeIds,
        std::vector<uint8_t> *fullBits,
        std::vector<uint8_t> *idBits,
        std::vector<uint8_t> *bitOffsets,
        std::vector<EncodedKey> *idMasks)
    {
        const auto colCount = static_cast<int32_t>(typeIds.size());
        if (colCount <= 1) {
            return false;
        }

        std::vector<uint8_t> localFullBits(colCount, 0);
        std::vector<uint8_t> localIdBits(colCount, 0);
        std::vector<uint8_t> localMinBits(colCount, 0);
        uint32_t requiredBits = 0;
        uint32_t minRequiredBits = 0;
        for (int32_t col = 0; col < colCount; ++col) {
            localFullBits[col] = RequiredFullBits(typeIds[col]);
            localMinBits[col] = MinUsefulBits(typeIds[col]);
            if (localFullBits[col] == 0 || localMinBits[col] == 0) {
                return false;
            }
            requiredBits += localFullBits[col];
            minRequiredBits += localMinBits[col];
        }

        if (requiredBits <= kTotalKeyBits) {
            localIdBits = localFullBits;
        } else {
            if (minRequiredBits > kTotalKeyBits) {
                return false;
            }
            // Full domains do not fit. Keep narrow columns full-domain and
            // reserve a useful minimum for wide columns first.
            localIdBits = localMinBits;
            uint32_t usedBits = minRequiredBits;
            while (usedBits < kTotalKeyBits) {
                bool assigned = false;
                for (int32_t col = 0; col < colCount && usedBits < kTotalKeyBits; ++col) {
                    if (localIdBits[col] >= localFullBits[col]) {
                        continue;
                    }
                    ++localIdBits[col];
                    ++usedBits;
                    assigned = true;
                }
                if (!assigned) {
                    break;
                }
            }
        }

        std::vector<uint8_t> localBitOffsets(colCount, 0);
        std::vector<EncodedKey> localIdMasks(colCount, 0);
        uint32_t offset = 0;
        for (int32_t col = 0; col < colCount; ++col) {
            if (localIdBits[col] == 0 || offset + localIdBits[col] > kTotalKeyBits) {
                return false;
            }
            localBitOffsets[col] = static_cast<uint8_t>(offset);
            localIdMasks[col] = MaskForBits(localIdBits[col]);
            offset += localIdBits[col];
        }

        if (fullBits != nullptr) {
            *fullBits = std::move(localFullBits);
        }
        if (idBits != nullptr) {
            *idBits = std::move(localIdBits);
        }
        if (bitOffsets != nullptr) {
            *bitOffsets = std::move(localBitOffsets);
        }
        if (idMasks != nullptr) {
            *idMasks = std::move(localIdMasks);
        }
        return true;
    }

    static SignedKey ToSigned(int64_t value)
    {
        return static_cast<SignedKey>(value);
    }

    bool InitBitLayout()
    {
        return BuildBitLayout(typeIds_, &fullBits_, &idBits_, &bitOffsets_, &idMasks_);
    }

    bool CanRepresentFullDomain(int32_t col) const
    {
        return idBits_[col] >= fullBits_[col];
    }

    void SetFullRangeForType(int32_t col, type::DataTypeId typeId)
    {
        switch (typeId) {
            case type::OMNI_BYTE:
                min_[col] = std::numeric_limits<int8_t>::min();
                max_[col] = std::numeric_limits<int8_t>::max();
                break;
            case type::OMNI_SHORT:
                min_[col] = std::numeric_limits<int16_t>::min();
                max_[col] = std::numeric_limits<int16_t>::max();
                break;
            case type::OMNI_INT:
            case type::OMNI_DATE32:
                min_[col] = std::numeric_limits<int32_t>::min();
                max_[col] = std::numeric_limits<int32_t>::max();
                break;
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
            case type::OMNI_DECIMAL64:
                min_[col] = std::numeric_limits<int64_t>::min();
                max_[col] = std::numeric_limits<int64_t>::max();
                break;
            default:
                return;
        }
        hasRange_[col] = true;
        isRange_[col] = true;
    }

    void InitRangeModeForType(int32_t col, type::DataTypeId typeId)
    {
        if (CanRepresentFullDomain(col)) {
            SetFullRangeForType(col, typeId);
        } else {
            // Deferred range initialization: sample rows initialize a range
            // window in WarmupRangeWindows().
            hasRange_[col] = false;
            isRange_[col] = false;
        }
    }

    void WarmupRangeWindowForColumn(int32_t col, int64_t sampledMin, int64_t sampledMax)
    {
        const EncodedKey capacity = idMasks_[col];
        if (capacity == 0) {
            return;
        }
        const auto sampledWidth = static_cast<EncodedKey>(ToSigned(sampledMax) - ToSigned(sampledMin));
        if (sampledWidth >= capacity) {
            return;
        }

        const SignedKey typeMin = TypeMin(typeIds_[col]);
        const SignedKey typeMax = TypeMax(typeIds_[col]);
        const SignedKey width = static_cast<SignedKey>(capacity) - 1;
        const SignedKey center = ToSigned(sampledMin) + (ToSigned(sampledMax) - ToSigned(sampledMin)) / 2;

        SignedKey newMin = center - width / 2;
        SignedKey newMax = newMin + width;
        if (newMin < typeMin) {
            newMin = typeMin;
            newMax = newMin + width;
        }
        if (newMax > typeMax) {
            newMax = typeMax;
            newMin = newMax - width;
        }
        if (newMin < typeMin) {
            newMin = typeMin;
        }

        min_[col] = static_cast<int64_t>(newMin);
        max_[col] = static_cast<int64_t>(newMax);
        hasRange_[col] = true;
        isRange_[col] = true;
    }

    static int64_t TypeMin(type::DataTypeId typeId)
    {
        switch (typeId) {
            case type::OMNI_BYTE:
                return std::numeric_limits<int8_t>::min();
            case type::OMNI_SHORT:
                return std::numeric_limits<int16_t>::min();
            case type::OMNI_INT:
            case type::OMNI_DATE32:
                return std::numeric_limits<int32_t>::min();
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
            case type::OMNI_DECIMAL64:
                return std::numeric_limits<int64_t>::min();
            default:
                return std::numeric_limits<int64_t>::min();
        }
    }

    static int64_t TypeMax(type::DataTypeId typeId)
    {
        switch (typeId) {
            case type::OMNI_BYTE:
                return std::numeric_limits<int8_t>::max();
            case type::OMNI_SHORT:
                return std::numeric_limits<int16_t>::max();
            case type::OMNI_INT:
            case type::OMNI_DATE32:
                return std::numeric_limits<int32_t>::max();
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
            case type::OMNI_DECIMAL64:
                return std::numeric_limits<int64_t>::max();
            default:
                return std::numeric_limits<int64_t>::max();
        }
    }

    EncodedKey valueId(int32_t col, int64_t value)
    {
        if (isRange_[col]) {
            batchUsedRangePath_ = true;
            if (value < min_[col] || value > max_[col]) {
                if (!unmappableLogged_) {
                    std::cout << "[HashAggFallback] multiNormalize range overflow: col=" << col
                              << ", value=" << value << ", min=" << min_[col] << ", max=" << max_[col]
                              << ", idBits=" << static_cast<int32_t>(idBits_[col]) << std::endl;
                    unmappableLogged_ = true;
                }
                return kUnmappable;
            }
            const auto id =
                static_cast<EncodedKey>(ToSigned(value) - ToSigned(min_[col]) + 1);
            return id <= idMasks_[col] ? id : kUnmappable;
        }

        batchUsedMapPath_ = true;
        auto &map = valueToId_[col];
        auto iter = map.find(value);
        if (iter != map.end()) {
            return iter->second;
        }

        const auto nextId = static_cast<uint64_t>(map.size() + 1);
        if (static_cast<EncodedKey>(nextId) > idMasks_[col]) {
            return kUnmappable;
        }
        map.emplace(value, nextId);
        idToValue_[col].push_back(value);
        updateRange(col, value);
        return nextId;
    }

    void updateRange(int32_t col, int64_t value)
    {
        if (hasRange_[col]) {
            if (value < min_[col]) {
                min_[col] = value;
            } else if (value > max_[col]) {
                max_[col] = value;
            }
        } else {
            hasRange_[col] = true;
            min_[col] = value;
            max_[col] = value;
        }
    }

    void WarmupRangeWindows(const std::vector<BatchEncodePlan> &plans, int32_t rowCount, int32_t sampleRows)
    {
        if (rowCount <= 0 || sampleRows <= 0) {
            return;
        }
        bool needsWarmup = false;
        for (int32_t col = 0; col < colCount_; ++col) {
            if (!hasRange_[col]) {
                needsWarmup = true;
                break;
            }
        }
        if (!needsWarmup) {
            return;
        }

        const int32_t safeSampleRows = std::max(1, sampleRows);
        const int32_t step = std::max(1, rowCount / safeSampleRows);
        for (const auto &plan : plans) {
            const int32_t col = plan.col;
            if (hasRange_[col]) {
                continue;
            }
            int64_t sampledMin = std::numeric_limits<int64_t>::max();
            int64_t sampledMax = std::numeric_limits<int64_t>::min();
            bool hasValue = false;
            if (plan.readValue == nullptr) {
                continue;
            }
            if (plan.vector->GetEncoding() == vec::OMNI_ENCODING_CONST) {
                if (!plan.hasNull || !plan.vector->IsNull(0)) {
                    int64_t value = plan.readValue(plan, 0);
                    sampledMin = value;
                    sampledMax = value;
                    hasValue = true;
                }
            } else if (plan.hasNull) {
                for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx += step) {
                    if (plan.vector->IsNull(rowIdx)) {
                        continue;
                    }
                    int64_t value = plan.readValue(plan, rowIdx);
                    sampledMin = std::min(sampledMin, value);
                    sampledMax = std::max(sampledMax, value);
                    hasValue = true;
                }
            } else {
                for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx += step) {
                    int64_t value = plan.readValue(plan, rowIdx);
                    sampledMin = std::min(sampledMin, value);
                    sampledMax = std::max(sampledMax, value);
                    hasValue = true;
                }
            }
            if (!hasValue) {
                continue;
            }
            WarmupRangeWindowForColumn(col, sampledMin, sampledMax);
        }
    }

    int32_t colCount_ = 0;
    bool unmappable_ = false;
    bool unmappableLogged_ = false;
    bool batchUsedRangePath_ = false;
    bool batchUsedMapPath_ = false;
    std::vector<type::DataTypeId> typeIds_;
    std::vector<uint8_t> fullBits_;
    std::vector<uint8_t> idBits_;
    std::vector<uint8_t> bitOffsets_;
    std::vector<EncodedKey> idMasks_;
    // F14FastMap for O(1) lookup, faster than vector traversal
    std::vector<folly::F14FastMap<int64_t, uint64_t>> valueToId_;
    std::vector<std::vector<int64_t>> idToValue_;
    std::vector<bool> hasRange_;
    std::vector<bool> isRange_;
    std::vector<int64_t> min_;
    std::vector<int64_t> max_;
    std::vector<BatchEncodePlan> batchEncodePlans_;
    std::vector<KeyType> encodedKeysBuffer_;
};
#undef OMNI_NK_FIXED_TYPE_DISPATCH
}
}
#endif // OMNI_RUNTIME_COLUMN_MARSHALLER_H
