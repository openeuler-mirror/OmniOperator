/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash table implementations
 */
#ifndef __JOIN_HASH_TABLE_H__
#define __JOIN_HASH_TABLE_H__

#include <cstdint>
#include <string>
#include "vector/vector.h"
#include "type/data_types.h"
#include "operator/filter/filter_and_project.h"
#include "operator/pages_hash_strategy.h"

namespace omniruntime {
namespace op {
constexpr uint32_t INVALID_POSITION = UINT32_MAX;
constexpr uint64_t INVALID_PARTITION_POSITION = UINT64_MAX;
/*
 * ArrayPositionLinks is used to storing the conflict position, it is a link.
 * pos = key[hashPos]
 * next = positionLinks[pos]
 * next = positionLinks[pos]
 * if (next == -1) means finding the end for a link.
 */
class ArrayPositionLinks {
public:
    explicit ArrayPositionLinks(uint32_t capacity);

    ~ArrayPositionLinks();

    ALWAYS_INLINE uint32_t *GetPositionLinks() const
    {
        return positionLinks;
    }

    ALWAYS_INLINE uint32_t GetSize() const
    {
        return size;
    }

private:
    uint32_t capacity;
    uint32_t size;
    uint32_t *positionLinks;
};

class PagesHash {
public:
    PagesHash(uint64_t *addresses, uint32_t addressesCount, PagesHashStrategy *pagesHashStrategy,
        ArrayPositionLinks *positionLinks);
    ~PagesHash();
    ALWAYS_INLINE uint32_t *GetKey() const
    {
        return key;
    }
    ALWAYS_INLINE uint32_t GetKeySize() const
    {
        return keySize;
    }
    void SetAddressIndex(ArrayPositionLinks &positionLinks, uint32_t realPosition, int64_t hash,
        uint64_t &totalHashCollisions) const;
    uint32_t GetAddressIndex(uint32_t probePosition, omniruntime::vec::Vector **joinColumns, int64_t rawHash) const;

    ALWAYS_INLINE int8_t *GetPositionToHashes() const
    {
        return positionToHashes;
    }
    ALWAYS_INLINE uint64_t *GetAddresses() const
    {
        return addresses;
    }
    ALWAYS_INLINE uint32_t GetAddressesCount() const
    {
        return addressesCount;
    }

    ALWAYS_INLINE PagesHashStrategy *GetPagesHashStrategy() const
    {
        return pagesHashStrategy;
    }

private:
    PagesHashStrategy *pagesHashStrategy;
    uint64_t *addresses;
    uint32_t addressesCount;
    uint32_t keySize;
    uint32_t *key; // it is used to store the addresses index. the key index is from getRawHashPosition()
    uint32_t mask;
    int8_t *positionToHashes;
    uint64_t hashCollisions;
};

class JoinHashTable {
public:
    JoinHashTable(PagesHashStrategy *pagesHashStrategy, uint64_t *addresses, uint32_t addressesCount);

    ~JoinHashTable();

    ALWAYS_INLINE PagesHash *GetPagesHash() const
    {
        return pagesHash;
    }

    ALWAYS_INLINE ArrayPositionLinks *GetPositionLinks() const
    {
        return positionLinks;
    }

    ALWAYS_INLINE void Visit(uint32_t joinPosition)
    {
        visitedPositions[joinPosition] = true;
    }

    ALWAYS_INLINE bool HasVisited(uint32_t joinPosition)
    {
        return visitedPositions[joinPosition];
    }

    ALWAYS_INLINE uint32_t GetVisitedPositionsSize() const
    {
        return visitedPositionsSize;
    }

    void PrintHashTable(uint32_t partitionIndex) const;

private:
    ArrayPositionLinks *positionLinks;
    PagesHash *pagesHash;
    std::vector<bool> visitedPositions;
    uint32_t visitedPositionsSize;
};

class JoinHashTables {
public:
    explicit JoinHashTables(uint32_t hashTableCount);

    ~JoinHashTables();

    ALWAYS_INLINE uint32_t GetHashTableSize()
    {
        return hashTableSize;
    }

    ALWAYS_INLINE uint32_t GetHashTableCount() const
    {
        return hashTableCount;
    }

    ALWAYS_INLINE uint64_t EncodePartitionedJoinPosition(uint32_t partition, uint32_t joinPosition) const
    {
        auto result = static_cast<uint64_t>(joinPosition) << shiftSize;
        result |= partition;
        return result;
    }

    ALWAYS_INLINE uint32_t DecodePartition(uint64_t partitionedJoinPosition) const
    {
        auto partition = static_cast<uint32_t>(partitionedJoinPosition & partitionMask);
        return partition;
    }

    ALWAYS_INLINE uint32_t DecodeJoinPosition(uint64_t partitionedJoinPosition) const
    {
        return static_cast<uint32_t>(partitionedJoinPosition >> shiftSize);
    }

    ALWAYS_INLINE void SetBuildTypes(DataTypes *buildDataTypes)
    {
        this->buildTypes = buildDataTypes;
    }

    ALWAYS_INLINE DataTypes *GetBuildDataTypes()
    {
        return this->buildTypes;
    }

    ALWAYS_INLINE void SetProbeTypes(DataTypes *probeDataTypes)
    {
        this->probeTypes = probeDataTypes;
    }

    ALWAYS_INLINE void SetOriginalProbeColsCount(int32_t originalColsCount)
    {
        this->originalProbeColsCount = originalColsCount;
    }

    ALWAYS_INLINE void SetFilterExpression(std::string &expression)
    {
        this->filterExpression = expression;
    }

    ALWAYS_INLINE std::string &GetFilterExpression()
    {
        return this->filterExpression;
    }

    void JoinFilterCodeGen(OverflowConfig *overflowConfig);

    ALWAYS_INLINE SimpleFilter *GetSimpleFilter()
    {
        return simpleFilter;
    }

    ALWAYS_INLINE void SetFilterExpr(omniruntime::expressions::Expr *filterExpr)
    {
        this->filterExpr = filterExpr;
    }

    ALWAYS_INLINE uint32_t GetVisitedCounts() const
    {
        return visitedCounts;
    }

    ALWAYS_INLINE JoinHashTable *GetHashTable(uint32_t partitionIndex) const
    {
        JoinHashTable *hashTable = hashTables[partitionIndex];
        return hashTable;
    }

    void AddHashTable(uint32_t partitionIndex, JoinHashTable *hashTable);
    bool IsJoinPositionEligible(uint64_t partitionedJoinPosition, uint32_t probePosition,
        omniruntime::vec::Vector **probeColumns, uint32_t probeColsCount, ExecutionContext *executionContext) const;
    uint64_t GetNextJoinPosition(uint64_t currentJoinPosition) const;
    uint64_t GetJoinPosition(uint32_t position, omniruntime::vec::Vector **joinColumns, int64_t rawHash) const;
    void PositionVisited(uint64_t currentJoinPosition);
    uint32_t GetTotalVisitedCounts() const;

private:
    uint32_t hashTableCount;
    JoinHashTable **hashTables; // actually, the type is JoinHashTable **
    std::atomic_uint32_t hashTableSize;
    uint32_t partitionMask;
    uint32_t shiftSize;
    DataTypes *probeTypes;
    DataTypes *buildTypes;
    int32_t originalProbeColsCount; // this is for lookup join with expression operator when join key and join filter
                                    // both are expressions
    std::string filterExpression;
    omniruntime::expressions::Expr *filterExpr = nullptr;
    SimpleFilter *simpleFilter = nullptr;
    std::set<int32_t> usedVectors;
    uint32_t visitedCounts = 0;
    uint32_t totalVisitedCounts = 0;
};
} // end of op
} // end of omniruntime
#endif
