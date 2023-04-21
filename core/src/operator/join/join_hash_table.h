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
/*
 * ArrayPositionLinks is used to storing the conflict position, it is a link.
 * pos = key[hashPos]
 * next = positionLinks[pos]
 * next = positionLinks[pos]
 * if (next == -1) means finding the end for a link.
 */
class ArrayPositionLinks {
public:
    explicit ArrayPositionLinks(uint32_t capacity)
    {
        this->capacity = capacity;
        this->size = 0;
        this->positionLinks = new uint32_t[capacity];
        for (uint32_t i = 0; i < capacity; i++) {
            positionLinks[i] = INVALID_POSITION;
        }
    }
    ~ArrayPositionLinks()
    {
        delete[] positionLinks;
        positionLinks = nullptr;
        capacity = 0;
        size = 0;
    }

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
    void SetAddressIndex(ArrayPositionLinks &positionLinks, uint32_t realPosition, int64_t hash, uint32_t initialPos,
        uint64_t &totalHashCollisions) const;

    ALWAYS_INLINE uint32_t GetAddressIndex(uint32_t probePosition, BaseVector **probeColumns, int64_t rawHash) const
    {
        auto pos = HashUtil::GetRawHashPosition(rawHash, mask);
        auto buildPosition = key[pos];
        while (buildPosition != INVALID_POSITION) {
            if (positionToHashes[buildPosition] != static_cast<int8_t>(rawHash)) {
                pos = (pos + 1) & mask;
                buildPosition = key[pos];
                continue;
            }
            if (PositionEqualsCurrentRowIgnoreNulls(buildPosition, probePosition, probeColumns)) {
                return buildPosition;
            }

            pos = (pos + 1) & mask;
            buildPosition = key[pos];
        }

        return INVALID_POSITION;
    }

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
    bool ALWAYS_INLINE PositionEqualsCurrentRowIgnoreNulls(uint32_t buildPosition, uint32_t probePosition,
                                                           BaseVector **probeColumns) const
    {
        auto address = addresses[buildPosition];
        auto vecBatchIndex = DecodeSliceIndex(address);
        auto rowIndex = DecodePosition(address);

        auto buildHashColumns = pagesHashStrategy->GetBuildHashColumns();
        auto hashColTypes = pagesHashStrategy->GetBuildHashColTypes();
        auto hashColCount = pagesHashStrategy->GetBuildHashColsCount();

        for (uint32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
            auto buildColumn = buildHashColumns[columnIdx][vecBatchIndex];
            auto probeColumn = probeColumns[columnIdx];
            if (!ValueEqualsValueIgnoreNulls(hashColTypes[columnIdx], buildColumn, rowIndex, probeColumn,
                                             probePosition)) {
                return false;
            }
        }
        return true;
    }

    bool PositionEqualsPositionIgnoreNulls(uint32_t leftPosition, uint32_t rightPosition) const;

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

    ALWAYS_INLINE uint32_t GetNextJoinPosition(uint32_t currentJoinPosition) const
    {
        auto nextJoinPosition = positionLinks->GetPositionLinks()[currentJoinPosition];
        return nextJoinPosition;
    }

    void PrintHashTable(uint32_t partitionIndex) const;

    ALWAYS_INLINE BaseVector ***GetBuildColumns() const
    {
        return buildColumns;
    }

    ALWAYS_INLINE uint64_t *GetBuildValueAddresses() const
    {
        return buildValueAddresses;
    }

private:
    ArrayPositionLinks *positionLinks;
    PagesHash *pagesHash;
    std::vector<bool> visitedPositions;
    uint32_t visitedPositionsSize;
    uint64_t *buildValueAddresses;
    BaseVector ***buildColumns;
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

    ALWAYS_INLINE std::vector<int32_t> &GetProbeFilterCols()
    {
        return probeFilterCols;
    }

    ALWAYS_INLINE std::vector<int32_t> &GetBuildFilterCols()
    {
        return buildFilterCols;
    }

    ALWAYS_INLINE uint32_t GetVisitedCounts() const
    {
        return visitedCounts;
    }

    ALWAYS_INLINE JoinHashTable *GetHashTable(uint32_t partitionIndex) const
    {
        return hashTables[partitionIndex];
    }

    ALWAYS_INLINE void GetJoinPosition(uint32_t probePosition, BaseVector **probeColumns, int64_t rawHash,
        uint32_t &partition, uint32_t &joinPosition) const
    {
        partition = (hashTableCount != 1) ? HashUtil::GetRawHashPartition(rawHash, partitionMask) : 0U;
        auto hashTable = hashTables[partition];
        joinPosition = hashTable->GetPagesHash()->GetAddressIndex(probePosition, probeColumns, rawHash);
    }

    ALWAYS_INLINE void AddHashTable(uint32_t partitionIndex, JoinHashTable *hashTable)
    {
        hashTables[partitionIndex] = hashTable;
        hashTableSize++;
        totalVisitedCounts += hashTable->GetVisitedPositionsSize();
    }

    ALWAYS_INLINE void InitBuildFilterCols()
    {
        tableBuildFilterColPtrs.resize(hashTableSize);
        auto buildFilterColsCount = buildFilterCols.size();
        for (uint32_t i = 0; i < buildFilterColsCount; i++) {
            auto buildFilterCol = buildFilterCols[i] - originalProbeColsCount;
            for (uint32_t hashTableIdx = 0; hashTableIdx < hashTableSize; ++hashTableIdx) {
                auto buildColumns = hashTables[hashTableIdx]->GetBuildColumns()[buildFilterCol];
                tableBuildFilterColPtrs[hashTableIdx].emplace_back(buildColumns);
            }
        }
    }

    ALWAYS_INLINE std::vector<BaseVector **> &GetBuildFilterColPtrs(uint32_t partition)
    {
        return tableBuildFilterColPtrs[partition];
    }

    ALWAYS_INLINE void PositionVisited(uint32_t partition, uint32_t joinPosition)
    {
        auto hashTable = hashTables[partition];
        if (!hashTable->HasVisited(joinPosition)) {
            hashTable->Visit(joinPosition);
            visitedCounts++;
        }
    }

    ALWAYS_INLINE uint32_t GetTotalVisitedCounts() const
    {
        return totalVisitedCounts;
    }

    ALWAYS_INLINE int32_t GetOriginalProbeColsCount() const
    {
        return originalProbeColsCount;
    }

private:
    uint32_t hashTableCount;
    JoinHashTable **hashTables; // actually, the type is JoinHashTable **
    std::atomic_uint32_t hashTableSize;
    uint32_t partitionMask;
    uint32_t shiftSize;
    DataTypes *probeTypes;
    DataTypes *buildTypes;
    // this is for lookup join with expression operator when join key and join filter both are expressions
    int32_t originalProbeColsCount;
    std::string filterExpression;
    omniruntime::expressions::Expr *filterExpr = nullptr;
    SimpleFilter *simpleFilter = nullptr;
    std::vector<int32_t> probeFilterCols;
    std::vector<int32_t> buildFilterCols;
    std::vector<std::vector<BaseVector **>> tableBuildFilterColPtrs;
    uint32_t visitedCounts = 0;
    uint32_t totalVisitedCounts = 0;
};
} // end of op
} // end of omniruntime
#endif
