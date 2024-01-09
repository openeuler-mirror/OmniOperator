/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: join hash table
 */

#ifndef OMNI_RUNTIME_JOIN_HASH_TABLE_VARIANTS_H
#define OMNI_RUNTIME_JOIN_HASH_TABLE_VARIANTS_H

#include <variant>
#include <vector>

#include "operator/hashmap/crc_hasher.h"
#include "operator/hashmap/base_hash_map.h"
#include "operator/hashmap/array_map.h"
#include "operator/hashmap/column_marshaller.h"
#include "type/data_types.h"
#include "common_join.h"
#include "row_ref.h"

namespace omniruntime {
namespace op {
template <typename KeyType, typename ValueType>
using JoinHashMap =
    BaseHashMap<KeyType, ValueType, omniruntime::simdutil::HashCRC32<KeyType>, Grower, OmniHashmapAllocator>;

template <typename KeyType, typename RowRefListType>
using JoinHashTableVariant = ColumnSerializeHandler<JoinHashMap<KeyType, RowRefListType *>>;

enum class HashTableImplementationType {
    NORMAL_HASH_TABLE,
    ARRAY_HASH_TABLE
};

template <typename KeyType, typename RowRefListType> class JoinHashTableVariants {
public:
    using Key = KeyType;
    using Mapped = RowRefListType;

    explicit JoinHashTableVariants(uint32_t hashTableCount, DataTypes *buildDataTypes,
        std::vector<int32_t> &buildHashCols, JoinType joinType);

    ~JoinHashTableVariants();

    ALWAYS_INLINE uint32_t GetHashTableCount() const
    {
        return hashTableCount;
    }

    ALWAYS_INLINE uint32_t GetHashTableSize() const
    {
        return hashTableSize;
    }

    ALWAYS_INLINE DataTypes *GetBuildDataTypes()
    {
        return this->buildTypes;
    }

    ALWAYS_INLINE void SetProbeTypes(DataTypes *probeDataTypes)
    {
        this->probeTypes = probeDataTypes;
    }

    ALWAYS_INLINE std::unique_ptr<JoinHashTableVariant<KeyType, RowRefListType>> &GetHashTable(int32_t partitionIndex)
    {
        return hashTables[partitionIndex];
    }

    ALWAYS_INLINE std::unique_ptr<DefaultArrayMap<RowRefListType>> &GetArrayTable(int32_t partitionIndex)
    {
        return arrayTables[partitionIndex];
    }

    ALWAYS_INLINE HashTableImplementationType GetHashTableTypes(int32_t partitionIndex)
    {
        return hashTableTypes[partitionIndex];
    }

    ALWAYS_INLINE void AddVecBatch(int32_t partitionIndex, omniruntime::vec::VectorBatch *vecBatch)
    {
        inputVecBatches[partitionIndex].emplace_back(vecBatch);
        totalRowCount[partitionIndex] += vecBatch->GetRowCount();
    }

    InsertResult<RowRefListType *> Find(std::vector<VectorSerializerIgnoreNull> &probeSerializers,
        ExecutionContext *probeArena, BaseVector **probeHashColumns, int32_t probeHashColCount, int32_t probePosition,
        uint32_t partition);

    void PositionVisited(ForwardIterator<RowRefListType> it)
    {
        if constexpr (std::is_same_v<RowRefListType, RowRefListWithFlags>) {
            if (!it->visited) {
                it->visited = true;
                visitedCounts++;
            }
        }
    }

    ALWAYS_INLINE uint32_t GetVisitedCounts() const
    {
        return visitedCounts;
    }

    ALWAYS_INLINE uint32_t GetTotalVisitedCounts() const
    {
        return totalVisitedCounts;
    }

    ALWAYS_INLINE void SetTotalVisitedCounts(int cnt)
    {
        totalVisitedCounts += cnt;
    }

    ALWAYS_INLINE BaseVector ***GetColumns(int32_t partitionIndex) const
    {
        return columns[partitionIndex];
    }

    void BuildHashTable(int32_t partitionIndex);

    void Prepare(int32_t partitionIndex);

    void InitBuildFilterCols(std::vector<int32_t> &buildFilterCols, int32_t originalProbeColsCount,
        std::vector<std::vector<BaseVector **>> &tableBuildFilterColPtrs);

private:
    template <typename T>
    bool TryToBuildArrayTable(uint32_t colIndex, T &min, T &max, int64_t rangeUpperBound, int32_t partitionIndex);

    template <typename T>
    void EmplaceNotNullKeyToArrayTable(T &min, T &max, int64_t rangeUpperBound, int32_t partitionIndex);

    template <typename T> void EmplaceKeyToArrayTable(T &min, T &max, int64_t rangeUpperBound, int32_t partitionIndex);

    void BuildNormalHashTableWithFixedKey(int32_t partitionIndex, uint8_t initDegree);

    void BuildNormalHashTableWithVariableKey(int32_t partitionIndex, uint8_t initDegree);

    template <typename HashTableType>
    void EmplaceFixedKey(HashTableType &hashTable, int32_t partitionIndex, VectorBatch *vecBatch, int32_t vecBatchIdx,
        BaseVector **buildVectors, int32_t buildColNum);

    template <typename HashTableType>
    void EmplaceFixedNotNullKeyToNormalHashTable(HashTableType &hashTable, int32_t partitionIndex,
        VectorBatch *vecBatch, int32_t vecBatchIdx, BaseVector **buildVectors, int32_t buildColNum);

    template <typename HashTableType>
    void EmplaceFixedKeyToNormalHashTable(HashTableType &hashTable, int32_t partitionIndex, VectorBatch *vecBatch,
        int32_t vecBatchIdx, BaseVector **buildVectors, int32_t buildColNum);

    template <typename HashTableType>
    void EmplaceSingleKey(HashTableType &hashTable, int32_t partitionIndex, VectorBatch *vecBatch, int32_t vecBatchIdx,
        BaseVector **buildVectors);

    template <bool isDic, typename HashTableType>
    void EmplaceSingleNotNullKeyToNormalHashTable(HashTableType &hashTable, int32_t partitionIndex,
        VectorBatch *vecBatch, int32_t vecBatchIdx, BaseVector **buildVectors);

    template <bool isDic, typename HashTableType>
    void EmplaceSingleKeyToNormalHashTable(HashTableType &hashTable, int32_t partitionIndex, VectorBatch *vecBatch,
        int32_t vecBatchIdx, BaseVector **buildVectors);

    template <typename HashTableType>
    void EmplaceVariableKey(HashTableType &hashTable, int32_t partitionIndex, VectorBatch *vecBatch,
        int32_t vecBatchIdx, BaseVector **buildVectors, int32_t buildColNum, std::vector<int8_t> &isNotNullKeys,
        std::vector<size_t> &hashes, std::vector<KeyType> &tryRes);

    template <typename HashTableType>
    void EmplaceVariableNotNullKeyToNormalHashTable(HashTableType &hashTable, int32_t partitionIndex,
        VectorBatch *vecBatch, int32_t vecBatchIdx, BaseVector **buildVectors, int32_t buildColNum,
        std::vector<int8_t> &isNotNullKeys, std::vector<size_t> &hashes, std::vector<KeyType> &tryRes);

    template <typename HashTableType>
    void EmplaceVariableKeyToNormalHashTable(HashTableType &hashTable, int32_t partitionIndex, VectorBatch *vecBatch,
        int32_t vecBatchIdx, BaseVector **buildVectors, int32_t buildColNum, std::vector<int8_t> &isNotNullKeys,
        std::vector<size_t> &hashes, std::vector<KeyType> &tryRes);

    uint32_t hashTableCount;            // the number of hashTables will be built
    std::atomic_uint32_t hashTableSize; // the number of hashTables having been built already
    std::vector<std::unique_ptr<JoinHashTableVariant<KeyType, RowRefListType>>> hashTables;
    std::vector<std::unique_ptr<DefaultArrayMap<RowRefListType>>> arrayTables;
    std::vector<HashTableImplementationType> hashTableTypes;
    std::vector<std::pair<int64_t, int64_t>> maxMins;
    DataTypes *probeTypes;
    DataTypes *buildTypes;
    std::vector<int32_t> &buildHashCols;
    std::vector<uint32_t> totalRowCount;
    std::vector<std::vector<omniruntime::vec::VectorBatch *>> inputVecBatches;
    std::vector<std::unique_ptr<ExecutionContext>> executionContexts;
    std::vector<size_t> buildHashes;
    uint32_t visitedCounts = 0;
    uint32_t totalVisitedCounts = 0;
    std::vector<omniruntime::vec::BaseVector ***> columns; // Vector* [partitionIdx][columnIdx][vecBatchIdx]
    JoinType joinType;
    bool isFixedKeys = true;
    size_t fixedKeysSize = 0;
    size_t sizeOfRowRefList = 0;
};

using HashTableVariants =
    std::variant<JoinHashTableVariants<int8_t, RowRefList>, JoinHashTableVariants<int16_t, RowRefList>,
    JoinHashTableVariants<int32_t, RowRefList>, JoinHashTableVariants<int64_t, RowRefList>,
    JoinHashTableVariants<Decimal128, RowRefList>, JoinHashTableVariants<StringRef, RowRefList>,
    JoinHashTableVariants<int8_t, RowRefListWithFlags>, JoinHashTableVariants<int16_t, RowRefListWithFlags>,
    JoinHashTableVariants<int32_t, RowRefListWithFlags>, JoinHashTableVariants<int64_t, RowRefListWithFlags>,
    JoinHashTableVariants<Decimal128, RowRefListWithFlags>, JoinHashTableVariants<StringRef, RowRefListWithFlags>>;
}
}
#endif // OMNI_RUNTIME_JOIN_HASH_TABLE_VARIANTS_H
