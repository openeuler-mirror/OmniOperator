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
#include "operator/hashmap/taper_hashtable.h"
#include "operator/omni_id_type_vector_traits.h"
#include "operator/execution_context.h"
#include "vector_marshaller.h"
#include "vector/vector.h"
#include "row_container.h"

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
static constexpr uint64_t kNullHash = 1;

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

class TaperColumnSerializeHandler {
public:
    static constexpr bool HasSpecialNullFunc = false;
    using HashTable = TaperFlatHashTable<int64_t, true>;
    int32_t totalAggValueSize = 0;
    int32_t totalAggStatesSize = 0;
    std::unique_ptr<HashTable> table;
    std::unique_ptr<RowContainer> aggRows;
    std::vector<int64_t> workingHashVals;
    std::vector<int32_t> workingUpdateIndices;
    std::vector<int32_t> keyTypeSizes;
    std::vector<bool> isVariableLenType;
    RowContainerIterator rowContainerIter;
    bool nullableKeys = true;

    uint8_t*& RowFromData(char* data)
    {
        return *reinterpret_cast<uint8_t**>(data);
    }

    TaperColumnSerializeHandler(mem::SimpleArenaAllocator &pool, int32_t size)
    {
        table = std::make_unique<HashTable>(pool, sizeof(uint64_t), sizeof(char*));
        totalAggStatesSize = size;
        totalAggValueSize = size + sizeof(size_t);
    }

    /// Initialize the RowContainer with key type information.
    /// Called after InitSize to set up the row layout with fixed-width key columns.
    /// @param keySizes Fixed row sizes for each key column (or sizeof(char*)+sizeof(size_t) for variable-length)
    /// @param isVariableLen True for each column that stores variable-length data (VARCHAR, ARRAY, etc.)
    /// @param nullableKeys Whether keys can contain nulls
    /// @param pool Memory pool for row allocation
    void InitRowContainer(const std::vector<int32_t>& keySizes,
                          const std::vector<bool>& isVariableLen,
                          bool nullableKeys,
                          mem::SimpleArenaAllocator& pool)
    {
        keyTypeSizes = keySizes;
        isVariableLenType = isVariableLen;
        nullableKeys = nullableKeys;
        aggRows = std::make_unique<RowContainer>(
            keySizes, static_cast<int32_t>(keySizes.size()),
            nullableKeys, totalAggStatesSize, pool);
    }

    /// Get the offset where AggState data begins within a row.
    int32_t AggStateOffset() const
    {
        return aggRows ? aggRows->AggStateOffset() : totalAggValueSize;
    }

    void EmplaceTable(BaseVector **groupVectors, int32_t groupColNum, int32_t rowsNum,
        std::vector<uint8_t*>& groups, std::vector<uint8_t*>& newGroups, Encoding encoding)
    {
        auto initRow = [&](uint32_t rowIdx, char* data) -> char* {
            auto* row = aggRows->NewRow();
            RowFromData(data) = reinterpret_cast<uint8_t*>(row);
            newGroups.push_back(RowFromData(data));
            StoreKeys(row, groupVectors, groupColNum, rowIdx);
            return row;
        };
        workingUpdateIndices.clear();
        workingHashVals.resize(rowsNum);
        for (size_t i = 0; i < groupColNum; ++i) {
            BaseVector *baseVector = groupVectors[i];
            auto type = baseVector->GetTypeId();
            if (type == type::OMNI_ARRAY) {
                DoArrayHash(baseVector, rowsNum, workingHashVals);
            } else if (type == type::OMNI_ROW) {
                DoRowHash(baseVector, rowsNum, workingHashVals);
            } else {
                DYNAMIC_TYPE_DISPATCH(Hash, type, baseVector, rowsNum, workingHashVals, i > 0);
            }
        }
        table->EmplaceBatch(
        workingHashVals.data(),
            rowsNum,
        [&](uint32_t) { return false; },
        [&](uint32_t rowIdx, char* data) { initRow(rowIdx, data); },
        [&](uint32_t rowIdx, char* data, bool initFlag) {
            groups[rowIdx] = RowFromData(data);
          if (!initFlag) {
            workingUpdateIndices.push_back((rowIdx));
          }
        });
        if (workingUpdateIndices.empty()) {
            return;
        }
        int32_t unequalsNum = GetUnequalsNum(workingUpdateIndices, groupVectors, groupColNum, groups);

        for (int32_t i = 0; i < unequalsNum; i++) {
            auto rowIdx = workingUpdateIndices[i];
            table->Emplace(
                workingHashVals[rowIdx],
                [&](auto, TaperHashTableChunk& chunk, uint8_t slot) {
                  auto* row = RowFromData(table->GetChunkValue(chunk, slot).buf);
                  return CompareKeys(groupVectors, row, groupColNum, rowIdx);
                },
                [&](char* data) {
                  auto* row = initRow(rowIdx, data);
                },
                [&](char* data, bool) { groups[rowIdx] = RowFromData(data); });
        }
    }

    /// Store key values from vectors into the RowContainer row.
    /// For fixed-width types, store directly at the column offset.
    /// For complex types (VARCHAR, ARRAY, ROW), use the serializer to
    /// serialize into arena memory and store a StringRef (pointer + size)
    /// at the column offset.
    void StoreKeys(char* row, BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx)
    {
        for (int32_t colIdx = 0; colIdx < groupColNum; ++colIdx) {
            auto* vector = groupVectors[colIdx];
            auto typeId = vector->GetTypeId();
            auto col = aggRows->ColumnAt(colIdx);
            auto offset = col.Offset();
            auto nullByte = col.NullByte();
            auto nullMask = col.NullMask();

            // Check null first
            if (nullableKeys && vector->IsNull(rowIdx)) {
                RowContainer::SetNullAt(row, nullByte, nullMask);
                continue;
            }
            if (nullableKeys) {
                RowContainer::ClearNullAt(row, nullByte, nullMask);
            }

            // Store value by type
            switch (typeId) {
                case type::OMNI_BYTE: {
                    auto value = static_cast<Vector<int8_t>*>(vector)->GetValue(rowIdx);
                    RowContainer::StoreValue(row, offset, value);
                    break;
                }
                case type::OMNI_SHORT: {
                    auto value = static_cast<Vector<int16_t>*>(vector)->GetValue(rowIdx);
                    RowContainer::StoreValue(row, offset, value);
                    break;
                }
                case type::OMNI_INT:
                case type::OMNI_DATE32:
                case type::OMNI_TIME32: {
                    auto value = static_cast<Vector<int32_t>*>(vector)->GetValue(rowIdx);
                    RowContainer::StoreValue(row, offset, value);
                    break;
                }
                case type::OMNI_LONG:
                case type::OMNI_TIMESTAMP:
                case type::OMNI_DECIMAL64:
                case type::OMNI_DATE64:
                case type::OMNI_TIME64: {
                    auto value = static_cast<Vector<int64_t>*>(vector)->GetValue(rowIdx);
                    RowContainer::StoreValue(row, offset, value);
                    break;
                }
                case type::OMNI_DOUBLE: {
                    auto value = static_cast<Vector<double>*>(vector)->GetValue(rowIdx);
                    RowContainer::StoreValue(row, offset, value);
                    break;
                }
                case type::OMNI_FLOAT: {
                    auto value = static_cast<Vector<float>*>(vector)->GetValue(rowIdx);
                    RowContainer::StoreValue(row, offset, value);
                    break;
                }
                case type::OMNI_DECIMAL128: {
                    auto value = static_cast<Vector<Decimal128>*>(vector)->GetValue(rowIdx);
                    RowContainer::StoreValue(row, offset, value);
                    break;
                }
                case type::OMNI_BOOLEAN: {
                    auto value = static_cast<Vector<bool>*>(vector)->GetValue(rowIdx);
                    RowContainer::StoreValue(row, offset, value);
                    break;
                }
                default: {
                    // Complex types (VARCHAR, ARRAY, ROW): serialize into arena
                    // and store StringRef (char* + size_t) at the column offset
                    type::StringRef key;
                    key.data = nullptr;
                    key.size = 0;
                    auto &curFunc = serializers[colIdx];
                    curFunc(vector, rowIdx, table->Pool(), key);
                    // Store the serialized key pointer and size at the column offset
                    // Layout: [char* data_ptr] [size_t data_size]
                    *reinterpret_cast<char**>(row + offset) = const_cast<char*>(key.data);
                    *reinterpret_cast<size_t*>(row + offset + sizeof(char*)) = key.size;
                    break;
                }
            }
        }
    }

    int32_t GetUnequalsNum(std::vector<int32_t> workingUpdateIndices, BaseVector **groupVectors, int32_t groupColNum,
        std::vector<uint8_t*>& groups)
    {
        uint32_t unequalsNum = 0;
        for (int32_t index : workingUpdateIndices) {
            if (!CompareKeys(groupVectors, groups[index], groupColNum, index)) {
                unequalsNum++;
            }
        }
        return unequalsNum;
    }

    bool CompareKeys(BaseVector **groupVectors, uint8_t *row, int32_t groupColNum, int32_t rowIdx)
    {
        for (int32_t groupColIdx = 0; groupColIdx < groupColNum; groupColIdx++) {
            auto* vector = groupVectors[groupColIdx];
            auto typeId = vector->GetTypeId();
            auto col = aggRows->ColumnAt(groupColIdx);
            auto offset = col.Offset();
            auto nullByte = col.NullByte();
            auto nullMask = col.NullMask();

            // Check nulls
            bool rowIsNull = nullableKeys && RowContainer::IsNullAt(
                reinterpret_cast<char*>(row), nullByte, nullMask);
            bool vecIsNull = vector->IsNull(rowIdx);
            if (rowIsNull != vecIsNull) {
                return false;
            }
            if (rowIsNull) {
                continue; // both null, match
            }

            // Compare fixed-width types by value
            switch (typeId) {
                case type::OMNI_BYTE:
                    if (RowContainer::ReadValue<int8_t>(reinterpret_cast<char*>(row), offset) !=
                        static_cast<Vector<int8_t>*>(vector)->GetValue(rowIdx)) return false;
                    break;
                case type::OMNI_SHORT:
                    if (RowContainer::ReadValue<int16_t>(reinterpret_cast<char*>(row), offset) !=
                        static_cast<Vector<int16_t>*>(vector)->GetValue(rowIdx)) return false;
                    break;
                case type::OMNI_INT:
                case type::OMNI_DATE32:
                case type::OMNI_TIME32:
                    if (RowContainer::ReadValue<int32_t>(reinterpret_cast<char*>(row), offset) !=
                        static_cast<Vector<int32_t>*>(vector)->GetValue(rowIdx)) return false;
                    break;
                case type::OMNI_LONG:
                case type::OMNI_TIMESTAMP:
                case type::OMNI_DECIMAL64:
                case type::OMNI_DATE64:
                case type::OMNI_TIME64:
                    if (RowContainer::ReadValue<int64_t>(reinterpret_cast<char*>(row), offset) !=
                        static_cast<Vector<int64_t>*>(vector)->GetValue(rowIdx)) return false;
                    break;
                case type::OMNI_DOUBLE:
                    if (RowContainer::ReadValue<double>(reinterpret_cast<char*>(row), offset) !=
                        static_cast<Vector<double>*>(vector)->GetValue(rowIdx)) return false;
                    break;
                case type::OMNI_FLOAT:
                    if (RowContainer::ReadValue<float>(reinterpret_cast<char*>(row), offset) !=
                        static_cast<Vector<float>*>(vector)->GetValue(rowIdx)) return false;
                    break;
                case type::OMNI_DECIMAL128:
                    if (RowContainer::ReadValue<Decimal128>(reinterpret_cast<char*>(row), offset) !=
                        static_cast<Vector<Decimal128>*>(vector)->GetValue(rowIdx)) return false;
                    break;
                default: {
                    // Complex types: use the comparator on the serialized data
                    auto& curFunc = comparators[groupColIdx];
                    uint8_t* addr = reinterpret_cast<uint8_t*>(
                        *reinterpret_cast<char**>(reinterpret_cast<char*>(row) + offset));
                    if (!curFunc(vector, rowIdx, addr)) {
                        return false;
                    }
                    break;
                }
            }
        }
        return true;
    }

    /// Use RowContainer::ListRows to iterate through rows for output.
    /// Returns row pointers where each row has:
    ///   [key data at column offsets] [null bits] [AggState at aggStateOffset]
    template <bool withHashVal = false, class Func, class NullFunc>
    void Extract(int32_t rowsNum, OutputState &outputState, Func func, NullFunc nullFunc)
    {
        std::vector<char*> rowPtrs(rowsNum);
        int32_t numExtracted = aggRows->ListRows(&rowContainerIter, rowsNum, rowPtrs.data());

        for (int32_t idx = 0; idx < numExtracted; ++idx) {
            auto* row = rowPtrs[idx];
            // Build a StringRef key for complex type columns from the row
            // For the new RowContainer layout, keys are stored in-place
            // We pass the row pointer directly so ParseKeyToCols can read from it
            func(reinterpret_cast<uint8_t*>(row), reinterpret_cast<uint8_t*>(row), idx);
        }
        outputState.hasBeenOutputNum += numExtracted;
    }

    /// Parse key columns from a RowContainer row into output vectors.
    /// For fixed-width types, read directly from row offsets.
    /// For complex types, read from the serialized StringRef stored in the row.
    void ParseKeyToCols(uint8_t* rowPtr, std::vector<vec::BaseVector *> &groupOutputVectors, int32_t groupColNum,
        const int32_t rowIdx)
    {
        auto* row = reinterpret_cast<char*>(rowPtr);
        for (int32_t i = 0; i < groupColNum; ++i) {
            auto col = aggRows->ColumnAt(i);
            auto offset = col.Offset();
            auto nullByte = col.NullByte();
            auto nullMask = col.NullMask();
            auto* outVector = groupOutputVectors[i];
            auto typeId = outVector->GetTypeId();

            // Check null
            if (nullableKeys && RowContainer::IsNullAt(row, nullByte, nullMask)) {
                outVector->SetNull(rowIdx);
                continue;
            }

            // Extract value by type
            switch (typeId) {
                case type::OMNI_BYTE:
                    static_cast<Vector<int8_t>*>(outVector)->SetValue(rowIdx,
                        RowContainer::ReadValue<int8_t>(row, offset));
                    break;
                case type::OMNI_SHORT:
                    static_cast<Vector<int16_t>*>(outVector)->SetValue(rowIdx,
                        RowContainer::ReadValue<int16_t>(row, offset));
                    break;
                case type::OMNI_INT:
                case type::OMNI_DATE32:
                case type::OMNI_TIME32:
                    static_cast<Vector<int32_t>*>(outVector)->SetValue(rowIdx,
                        RowContainer::ReadValue<int32_t>(row, offset));
                    break;
                case type::OMNI_LONG:
                case type::OMNI_TIMESTAMP:
                case type::OMNI_DECIMAL64:
                case type::OMNI_DATE64:
                case type::OMNI_TIME64:
                    static_cast<Vector<int64_t>*>(outVector)->SetValue(rowIdx,
                        RowContainer::ReadValue<int64_t>(row, offset));
                    break;
                case type::OMNI_DOUBLE:
                    static_cast<Vector<double>*>(outVector)->SetValue(rowIdx,
                        RowContainer::ReadValue<double>(row, offset));
                    break;
                case type::OMNI_FLOAT:
                    static_cast<Vector<float>*>(outVector)->SetValue(rowIdx,
                        RowContainer::ReadValue<float>(row, offset));
                    break;
                case type::OMNI_DECIMAL128:
                    static_cast<Vector<Decimal128>*>(outVector)->SetValue(rowIdx,
                        RowContainer::ReadValue<Decimal128>(row, offset));
                    break;
                default: {
                    // Complex types: deserialize from the StringRef stored in the row
                    char* dataPtr = *reinterpret_cast<char**>(row + offset);
                    size_t dataSize = *reinterpret_cast<size_t*>(row + offset + sizeof(char*));
                    auto deserializeFunc = deserializers[i];
                    const char* pos = dataPtr;
                    deserializeFunc(outVector, rowIdx, pos);
                    break;
                }
            }
        }
    }

    void ParseNull(const char *key, std::vector<vec::BaseVector *> &groupOutputVectors, int32_t groupColNum,
        const int rowIdx)
    {
        for (int32_t i = 0; i < groupColNum; ++i) {
            groupOutputVectors[i]->SetNull(rowIdx);
        }
    }

    /// SpillExtract traverses the hash table for spill operations.
    /// It uses the hash table Visitor and reconstructs serialized key data
    /// for writing to disk. The key is reconstructed from the RowContainer
    /// row by re-serializing each key column.
    template <class Func, class NullFunc>
    void SpillExtract(int32_t rowsNum, OutputState &outputState, Func func, NullFunc nullFunc)
    {
        auto tblVisitor = [&] {
            if (outputState.rowBegin) {
                return table->GetResultVisitor(
                    outputState.rowBegin, static_cast<uint16_t>(outputState.rowOffset));
            }
            return table->GetResultVisitor();
        }();
        uint32_t idx = 0;
        while (idx < rowsNum && !tblVisitor.Finished()) {
            auto* row = RowFromData(tblVisitor.CurVal().buf);
            // Reconstruct serialized key data from RowContainer row
            // For fixed-width types, re-serialize each column into a contiguous buffer
            // For complex types, the serialized data pointer is stored in the row
            type::StringRef key;
            for (int32_t colIdx = 0; colIdx < static_cast<int32_t>(serializers.size()); ++colIdx) {
                auto col = aggRows->ColumnAt(colIdx);
                auto offset = col.Offset();
                auto nullByte = col.NullByte();
                auto nullMask = col.NullMask();

                // Check if this column is null in the row
                if (nullableKeys && RowContainer::IsNullAt(reinterpret_cast<char*>(row), nullByte, nullMask)) {
                    // Serialize null marker: [uint8_t: 0]
                    auto* pos = table->Pool().AllocateContinue(sizeof(uint8_t), reinterpret_cast<const uint8_t*&>(key.data));
                    *pos = 0;
                    key.size += sizeof(uint8_t);
                    continue;
                }

                // For complex types stored as StringRef in the row, copy the serialized data
                if (isVariableLenType[colIdx]) {
                    // This is a complex type column - copy the serialized data from the StringRef
                    char* dataPtr = *reinterpret_cast<char**>(reinterpret_cast<char*>(row) + offset);
                    size_t dataSize = *reinterpret_cast<size_t*>(reinterpret_cast<char*>(row) + offset + sizeof(char*));
                    if (dataPtr != nullptr && dataSize > 0) {
                        auto* dest = table->Pool().AllocateContinue(dataSize, reinterpret_cast<const uint8_t*&>(key.data));
                        memcpy(dest, dataPtr, dataSize);
                        key.size += dataSize;
                    }
                } else {
                    // For fixed-width types, we need to re-serialize them
                    // using the original serializer format
                    // The simplest approach: re-serialize using FixedLenTypeSerializer format
                    // Format: [uint8_t rowDataSize][rowDataSize bytes of value]
                    // Determine the size from the key column size
                    int32_t colSize = keyTypeSizes[colIdx];
                    uint8_t rowDataSize = static_cast<uint8_t>(colSize);
                    // Round up to power of 2 for the serialization format
                    if (colSize > 1) {
                        rowDataSize = 1;
                        auto tmp = colSize;
                        while (tmp > 1) { tmp >>= 1; rowDataSize <<= 1; }
                    }
                    auto resSize = sizeof(uint8_t) + rowDataSize;
                    auto* pos = table->Pool().AllocateContinue(resSize, reinterpret_cast<const uint8_t*&>(key.data));
                    *pos = rowDataSize;
                    memcpy(pos + sizeof(uint8_t), reinterpret_cast<char*>(row) + offset, rowDataSize);
                    key.size += resSize;
                }
            }

            // Ensure space for the key size trailer
            table->Pool().AllocateContinue(sizeof(size_t), reinterpret_cast<const uint8_t*&>(key.data));

            // Store key size trailer
            *reinterpret_cast<size_t*>(const_cast<char*>(key.data) + key.size) = key.size;

            // Adjust value pointer to AggState offset
            uint8_t* valuePtr = reinterpret_cast<uint8_t*>(row) + aggRows->AggStateOffset();

            func(key, tblVisitor.CurKey(), valuePtr, idx);
            tblVisitor.Next();
            idx++;
        }
        tblVisitor.SavePos([&](auto ptr, auto tagPos) {
            outputState.rowBegin = reinterpret_cast<char*>(ptr);
            outputState.rowOffset = tagPos;
            outputState.hasBeenOutputNum += rowsNum;
        });
    }

    template <DataTypeId id>
    void Hash(BaseVector *vector, int32_t rowsNum, std::vector<int64_t> &workingHashVals, bool isMix)
    {
        bool isDictEncoded = (vector->GetEncoding() == vec::OMNI_DICTIONARY);
        bool isConstEncoded = (vector->GetEncoding() == vec::OMNI_ENCODING_CONST);
        if (isDictEncoded) {
            DoHash<id, true>(vector, rowsNum, workingHashVals, isMix);
        } else if (isConstEncoded) {
            DoHash<id, false, true>(vector, rowsNum, workingHashVals, isMix);
        } else {
            DoHash<id>(vector, rowsNum, workingHashVals, isMix);
        }
    }

    template<DataTypeId id, bool isDic = false, bool isConst = false>
    void DoHash(BaseVector *vector, int32_t rowsNum, std::vector<int64_t> &workingHashVals, bool isMix)
    {
        using RealVector = typename NativeAndVectorType<id>::vector;
        using Type = typename NativeAndVectorType<id>::type;
        GroupbyHashCalculator<Type> calculator {};
        for (int32_t row = 0; row < rowsNum; row++) {
            int64_t hashVal = 0;
            if constexpr (isDic) {
                hashVal = vector->IsNull(row) ? kNullHash :
                    calculator(static_cast<Vector<DictionaryContainer<Type>> *>(vector)->GetValue(row));
            } else if constexpr (isConst) {
                hashVal = vector->IsNull(row) ? kNullHash :
                    calculator(static_cast<ConstVector<Type> *>(vector)->GetConstValue());
            } else {
                auto realVector = static_cast<RealVector *>(vector);
                hashVal = vector->IsNull(row) ? kNullHash : calculator(realVector->GetValue(row));
            }
            workingHashVals[row] = isMix ? BitUtil::HashMix(workingHashVals[row], hashVal): hashVal;
        }
    }

    void DoRowHash(BaseVector *vector, int32_t rowsNum, std::vector<int64_t> &workingHashVals)
    {
        auto rowVector = dynamic_cast<RowVector *>(vector);
        int32_t childCount = rowVector->ChildSize();
        for (int32_t i = 0; i < childCount; i++) {
            auto &childVec = rowVector->ChildAt(i);
            auto childTypeId = childVec->GetTypeId();
            DYNAMIC_TYPE_DISPATCH(Hash, childTypeId, childVec.get(), rowsNum, workingHashVals, i > 0);
        }
    }

    void DoArrayHash(BaseVector *vector, int32_t rowsNum, std::vector<int64_t> &workingHashVals)
    {
        auto arrayVector = dynamic_cast<ArrayVector *>(vector);
        for (int32_t i = 0; i < rowsNum; i++) {
            int64_t offset = arrayVector->GetOffset(i);
            int64_t size = arrayVector->GetSize(i);
            auto elementVec = arrayVector->GetElementVector().get();
            int64_t start = offset;
            int64_t end = offset + size;
            auto elementTypeId = elementVec->GetTypeId();
            workingHashVals[i] = DYNAMIC_TYPE_DISPATCH(CalculateArrayHash, elementTypeId, elementVec, start, end);
        }
    }

    template <DataTypeId id>
    int64_t CalculateArrayHash(BaseVector *vector, int64_t start, int64_t end)
    {
        using RealVector = typename NativeAndVectorType<id>::vector;
        using Type = typename NativeAndVectorType<id>::type;
        auto realVector = static_cast<RealVector *>(vector);
        GroupbyHashCalculator<Type> calculator {};
        int64_t finalHash = 0;
        for (int32_t row = start; row < end; row++) {
            auto hash = vector->IsNull(row) ? kNullHash : calculator(realVector->GetValue(row));
            finalHash = (row == start) ? hash : BitUtil::HashMix(finalHash, hash);
        }
        return finalHash;
    }

    /*template <bool withHashVal = false, class Func, class NullFunc>
    void Extract(int32_t rowsNum, OutputState &outputState, Func func, NullFunc nullFunc)
    {
        auto tblVisitor = [&] {
            if (outputState.rowBegin) {
                return table->getResultVisitor(
                    outputState.rowBegin, static_cast<uint16_t>(outputState.rowOffset));
            }
            return table->getResultVisitor();
        }();
        uint32_t idx = 0;
        while (idx < rowsNum && !tblVisitor.finished()) {
            auto *row = rowFromData(tblVisitor.curVal().buf);
            type::StringRef key;
            key.data = row + totalAggValueSize;
            key.size = *reinterpret_cast<size_t*>(row + totalAggValueSize - sizeof(size_t));
            if constexpr (withHashVal) {
                func(key, tblVisitor.curKey(), row, idx);
            } else {
                func(key, row, idx);
            }
            tblVisitor.next();
            idx++;
        }
        tblVisitor.savePos([&](auto ptr, auto tagPos) {
            outputState.rowBegin = reinterpret_cast<char*>(ptr);
            outputState.rowOffset = tagPos;
            outputState.hasBeenOutputNum += rowsNum;
        });
    }*/

    void ParseKeyToCols(const type::StringRef &key, std::vector<vec::BaseVector *> &groupOutputVectors, int32_t groupColNum,
        const int32_t rowIdx)
    {
        auto *pos = key.data;
        for (int32_t i = 0; i < groupColNum; ++i) {
            auto curVectorPtr = groupOutputVectors[i];
            auto deserializeFunc = deserializers[i];
            pos = deserializeFunc(curVectorPtr, rowIdx, pos);
        }
    }

    void InitSize(int groupBySize)
    {
        serializers.reserve(groupBySize);
        deserializers.reserve(groupBySize);
        comparators.reserve(groupBySize);
    }

    void ResetSerializer()
    {
        serializers.clear();
        deserializers.clear();
        comparators.clear();
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

    void PushBackComparator(VectorComparator &comparator)
    {
        comparators.push_back(comparator);
    }

    size_t GetElementsSize() const
    {
        return table->Size();
    }

    void ResetHashmap()
    {
        table->Clear();
    };

private:
    std::vector<VectorSerializer> serializers;
    std::vector<VectorDeSerializer> deserializers;
    std::vector<VectorComparator> comparators;

    std::vector<VectorSerializerIgnoreNull> ignoreNullSerializers;
    std::vector<FixedKeyVectorSerializerIgnoreNull> fixedKeysIgnoreNullSerializers;

    std::vector<FixedKeyVectorSerializerIgnoreNullSimd> fixedKeysIgnoreNullSerializersSimd;
};

template <typename T, bool isPacked = false>
class TaperGroupbySingleFixHandler {
public:
    static constexpr bool HasSpecialNullFunc = true;
    using HashTable = TaperFlatHashTable<T, true>;
    int32_t totalAggValueSize = 0;
    std::unique_ptr<HashTable> table;
    bool shouldExtractNull = false;
    std::vector<T> keys;
    std::string nullValue;

    TaperGroupbySingleFixHandler(mem::SimpleArenaAllocator &pool, int32_t size)
    {
        table = std::make_unique<HashTable>(pool, sizeof(T), sizeof(char*));
        totalAggValueSize = size;
    }

    TaperGroupbySingleFixHandler(mem::SimpleArenaAllocator &pool, int32_t size,
        std::vector<int32_t> typeIds, std::vector<uint8_t> bitWidths)
    {
        table = std::make_unique<HashTable>(pool, sizeof(T), sizeof(char*));
        totalAggValueSize = size;
        if constexpr (isPacked) {
            InitPlan(std::move(typeIds), std::move(bitWidths));
        }
    }

    uint8_t*& RowFromData(char* data)
    {
        return *reinterpret_cast<uint8_t**>(data);
    }

    void EmplaceTable(BaseVector **groupVectors, int32_t groupColNum, int32_t rowsNum,
        std::vector<uint8_t*>& groups, std::vector<uint8_t*>& newGroups, Encoding encoding)
    {
        auto initRow = [&](uint32_t rowIdx, char* data) {
            if (totalAggValueSize > 0) {
                auto* row = table->Pool().Allocate(totalAggValueSize);
                RowFromData(data) = reinterpret_cast<uint8_t*>(row);
                newGroups.push_back(RowFromData(data));
            }
        };

        bool isDictEncoded = (encoding == vec::OMNI_DICTIONARY);
        bool isConstEncoded = (encoding == vec::OMNI_ENCODING_CONST);
        auto *curVector = groupVectors[0];
        if (isDictEncoded) {
            InitKeys<true>(groupVectors, rowsNum, groupColNum, groups, newGroups);
        } else if (isConstEncoded) {
            InitKeys<false, true>(groupVectors, rowsNum, groupColNum, groups, newGroups);
        } else {
            InitKeys(groupVectors, rowsNum, groupColNum, groups, newGroups);
        }

        table->EmplaceBatch(
        keys.data(),
            rowsNum,
        [&](uint32_t idx) { return !isPacked && curVector->IsNull(idx); },
        [&](uint32_t rowIdx, char* data) { initRow(rowIdx, data); },
        [&](uint32_t rowIdx, char* data, bool initFlag) {
            groups[rowIdx] = RowFromData(data);
        });
    }

    template<bool isDic = false, bool isConst = false>
    void InitKeys(BaseVector **groupVectors, int32_t rowsNum, int32_t groupColNum,
        std::vector<uint8_t*>& groups, std::vector<uint8_t*>& newGroups)
    {
        auto *curVector = groupVectors[0];
        keys.resize(rowsNum);
        for (int32_t i = 0; i < rowsNum; i++) {
            if constexpr (isPacked) {
                Prepare(groupVectors, groupColNum);
                keys[i] = PackKey(groupVectors, groupColNum, i);
            } else {
                if (!curVector->IsNull(i)) {
                    if constexpr (isDic) {
                        keys[i] = static_cast<Vector<DictionaryContainer<T>> *>(curVector)->GetValue(i);
                    } else if constexpr  (isConst) {
                        keys[i] = static_cast<ConstVector<T> *>(curVector)->GetConstValue();
                    } else {
                        keys[i] = reinterpret_cast<Vector<T>*>(curVector)->GetValue(i);
                    }
                } else if (totalAggValueSize > 0) {
                    if (nullValue.empty()) {
                        nullValue.resize(totalAggValueSize);
                        newGroups.push_back(reinterpret_cast<uint8_t*>(nullValue.data()));
                        shouldExtractNull = true;
                    }
                    groups[i] = reinterpret_cast<uint8_t*>(nullValue.data());
                }
            }
        }
    }

    template<bool isNull>
    void InsertOneValueToHashmap(T key, uint8_t *value)
    {
        if constexpr (isNull) {
            if (nullValue.empty()) {
                nullValue.resize(totalAggValueSize);
                shouldExtractNull = true;
                std::memcpy(nullValue.data(), value, totalAggValueSize);
            }
            return;
        }

        table->Emplace(
          key,
          [&](char* data) {
             RowFromData(data) = value;
          },
          [&](char* data, bool) { });
    }

   template <class Func, class NullFunc>
   void Extract(int32_t rowsNum, OutputState &outputState, Func func, NullFunc nullFunc)
    {
        auto tblVisitor = [&] {
            if (outputState.rowBegin) {
                return table->GetResultVisitor(
                    outputState.rowBegin, static_cast<uint16_t>(outputState.rowOffset));
            }
            return table->GetResultVisitor();
        }();
        uint32_t idx = 0;
        if (shouldExtractNull) {
            auto key = static_cast<T>(0);
            nullFunc(key, reinterpret_cast<uint8_t*>(nullValue.data()), idx);
            shouldExtractNull = false;
            idx++;
        }
        while (idx < rowsNum && !tblVisitor.Finished()) {
            auto *row = RowFromData(tblVisitor.CurVal().buf);
            func(tblVisitor.CurKey(), row, idx);
            tblVisitor.Next();
            idx++;
        }
        tblVisitor.SavePos([&](auto ptr, auto tagPos) {
            outputState.rowBegin = reinterpret_cast<char*>(ptr);
            outputState.rowOffset = tagPos;
            outputState.hasBeenOutputNum += rowsNum;
        });
    }

    void ParseKeyToCols(const T &key, std::vector<vec::BaseVector *> &groupOutputVectors, int32_t groupColNum,
        const int rowIdx)
    {
        if constexpr (isPacked) {
            UnpackKey(key, groupColNum);
            for (int32_t col = 0; col < groupColNum; ++col) {
                auto *outVector = groupOutputVectors[col];
                if (unpackIsNull[col]) {
                    outVector->SetNull(rowIdx);
                    continue;
                }
                SetValueByType(outVector, rowIdx, plan[col].typeId, unpackValues[col]);
            }
        } else {
            auto curVectorPtr = groupOutputVectors[0];
            if (curVectorPtr->GetEncoding() == Encoding::OMNI_DICTIONARY) {
                auto dictionaryVector = reinterpret_cast<Vector<DictionaryContainer<T>> *>(curVectorPtr);
                dictionaryVector->SetValue(rowIdx, key);
            } else {
                reinterpret_cast<Vector<T>*>(curVectorPtr)->SetValue(rowIdx, key);
            }
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
        return table->Size() + (shouldExtractNull ? 1 : 0);
    }

    void ResetHashmap()
    {
        table->Clear();
        nullValue.clear();
    }
    private:
    using UnsignedKey = std::conditional_t<std::is_same_v<T, omniruntime::type::int128_t>,
        __uint128_t, std::make_unsigned_t<T>>;

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

    ALWAYS_INLINE void Prepare(BaseVector **groupVectors, int32_t groupColNum)
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

    template<typename K, bool isDict>
    static ALWAYS_INLINE UnsignedKey LoadBits(BaseVector *vector, int32_t rowIdx, UnsignedKey mask)
    {
        if constexpr (isDict) {
            auto v = reinterpret_cast<Vector<DictionaryContainer<K>> *>(vector)->GetValue(rowIdx);
            return static_cast<UnsignedKey>(static_cast<std::make_unsigned_t<K>>(v)) & mask;
        } else {
            auto v = reinterpret_cast<Vector<K> *>(vector)->GetValue(rowIdx);
            return static_cast<UnsignedKey>(static_cast<std::make_unsigned_t<K>>(v)) & mask;
        }
    }

    template<typename K>
    static ALWAYS_INLINE UnsignedKey LoadBitsConst(BaseVector *vector, int32_t rowIdx, UnsignedKey mask)
    {
        (void)rowIdx;
        auto v = reinterpret_cast<ConstVector<K> *>(vector)->GetConstValue();
        return static_cast<UnsignedKey>(static_cast<std::make_unsigned_t<K>>(v)) & mask;
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

    ALWAYS_INLINE T PackKey(BaseVector **groupVectors, int32_t groupColNum, int32_t rowIdx) const
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
        return static_cast<T>(packed);
    }

    ALWAYS_INLINE void UnpackKey(const T &key, int32_t groupColNum) const
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
