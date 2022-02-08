/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash table implementations
 */
#ifndef __JOIN_HASH_TABLE_H__
#define __JOIN_HASH_TABLE_H__

#include "../../vector/vector.h"
#include "../../vector/vector_types.h"
#include "../filter/filter_and_project.h"

#include <stdint.h>

class PagesHashStrategy;

namespace omniruntime {
namespace op {
class ArrayPositionLinks;
class PagesHash;
class JoinHashTable;

class JoinHashTables {
public:
    explicit JoinHashTables(int32_t hashTableCount);

    ~JoinHashTables();

    int32_t GetHashTableSize()
    {
        return hashTableSize;
    }

    int32_t GetHashTableCount() const
    {
        return hashTableCount;
    }

    int64_t EncodePartitionedJoinPosition(int32_t partition, int32_t joinPosition) const
    {
        int64_t result = static_cast<int64_t>(joinPosition) << shiftSize;
        result |= partition;
        return result;
    }

    int32_t DecodePartition(int64_t partitionedJoinPosition) const
    {
        auto result = static_cast<int32_t>(partitionedJoinPosition & partitionMask);
        return result;
    }

    int32_t DecodeJoinPosition(int64_t partitionedJoinPosition) const
    {
        auto result = static_cast<uint64_t>(partitionedJoinPosition);
        result = result >> shiftSize;
        return static_cast<int32_t>(result);
    }

    void SetBuildTypes(omniruntime::vec::VecTypes *buildTypes)
    {
        this->buildTypes = buildTypes;
    }

    omniruntime::vec::VecTypes* GetBuildVecTypes()
    {
        return this->buildTypes;
    }

    void SetProbeTypes(omniruntime::vec::VecTypes *probeTypes)
    {
        this->probeTypes = probeTypes;
    }

    void SetFilterExpression(std::string &filterExpression)
    {
        this->filterExpression = filterExpression;
    }

    std::string &GetFilterExpression()
    {
        return this->filterExpression;
    }

    void JoinFilterCodeGen();

    SimpleFilter *GetSimpleFilter()
    {
        return simpleFilter;
    }

    void SetFilterExpr(omniruntime::expressions::Expr *filterExpr)
    {
        this->filterExpr = filterExpr;
    }

    void AddHashTable(int32_t partitionIndex, const JoinHashTable *hashTable);
    JoinHashTable *GetHashTable(int32_t partitionIndex) const;
    bool IsJoinPositionEligible(int64_t partitionedJoinPosition, int32_t probePosition,
        omniruntime::vec::Vector **probeColumns, int32_t probeColsCount, ExecutionContext *executionContext) const;
    int64_t GetNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition) const;
    int64_t GetJoinPosition(int32_t position, omniruntime::vec::Vector **joinColumns, int32_t *joinColumnTypes,
        int32_t joinColumnsCount, omniruntime::vec::Vector **allColumns, int32_t allColumnsCount,
        int64_t rawHash) const;
    int64_t GetJoinPosition(int32_t position, omniruntime::vec::Vector **joinColumns, int32_t joinColumnsCount,
        omniruntime::vec::Vector **allColumns, int32_t allColumnsCount, int64_t rawHash) const;

private:
    JoinHashTable **hashTables; // actually, the type is JoinHashTable **
    int32_t hashTableCount;
    std::atomic_int32_t hashTableSize;
    int32_t partitionMask;
    int32_t shiftSize;
    omniruntime::vec::VecTypes *probeTypes;
    omniruntime::vec::VecTypes *buildTypes;
    std::string filterExpression;
    omniruntime::expressions::Expr *filterExpr = nullptr;
    SimpleFilter *simpleFilter = nullptr;
    std::set<int32_t> usedVectors;
};

class JoinHashTable {
public:
    JoinHashTable(PagesHashStrategy *pagesHashStrategy, int64_t *addresses, int32_t addressesCount);
    ~JoinHashTable();
    PagesHash *GetPagesHash() const
    {
        return pagesHash;
    }

    ArrayPositionLinks *GetPositionLinks() const
    {
        return positionLinks;
    }

    int64_t GetJoinPosition(int32_t position, omniruntime::vec::Vector **joinColumns,
        omniruntime::vec::Vector **allColumns) const;
    int64_t GetJoinPosition(int32_t position, omniruntime::vec::Vector **joinColumns,
        omniruntime::vec::Vector **allColumns, int64_t rawHash) const;
    int32_t GetNextJoinPosition(int32_t currentJoinPosition, int probePosition) const;
    int32_t GetJoinPosition(int32_t position, omniruntime::vec::Vector **joinColumns, int32_t joinColumnsCount,
        omniruntime::vec::Vector **allColumns, int32_t allColumnsCount, int64_t rawHash) const;
    void PrintHashTable(int32_t partitionIndex) const;

private:
    int32_t StartJoinPosition(int32_t currentJoinPosition, int32_t probePosition, omniruntime::vec::Vector **allColumns,
        int32_t allColumnsCount) const;

    PagesHash *pagesHash;
    ArrayPositionLinks *positionLinks;
};

class PagesHash {
public:
    PagesHash(int64_t *addresses, int32_t addressesSize, PagesHashStrategy *pagesHashStrategy,
        ArrayPositionLinks *positionLinks);
    ~PagesHash();
    int32_t *GetKey() const
    {
        return key;
    }
    int32_t GetKeySize() const
    {
        return keySize;
    }
    void SetAddressIndex(ArrayPositionLinks *positionLinks, int32_t realPosition, int64_t hash,
        int64_t *totalHashCollisions) const;
    int32_t GetAddressIndex(int probePosition, omniruntime::vec::Vector **joinColumns, int32_t joinColumnsCount,
        int64_t rawHash) const;

    int8_t *GetPositionToHashes() const
    {
        return positionToHashes;
    }
    int64_t *GetAddresses() const
    {
        return addresses;
    }
    int32_t GetAddressesCount() const
    {
        return addressesCount;
    }

    PagesHashStrategy *GetPagesHashStrategy() const
    {
        return pagesHashStrategy;
    }

private:
    int64_t GetRawHash(int32_t position) const;
    bool IsPositionNull(int32_t position) const;
    bool PositionEqualsPositionIgnoreNulls(int32_t leftPosition, int32_t rightPosition) const;
    bool PositionEqualsCurrentRowIgnoreNulls(int32_t buildPosition, int8_t rawHash, int32_t probePosition,
        omniruntime::vec::Vector **joinColumns) const;

    PagesHashStrategy *pagesHashStrategy;
    int64_t *addresses;
    int32_t addressesCount;
    int32_t mask;
    int32_t *key; // it is used to store the addresses index. the key index is from getRawHashPosition()
    int32_t keySize;
    int8_t *positionToHashes;
    int64_t hashCollisions;

    void SetAddressIndex(ArrayPositionLinks *positionLinks, int64_t hashCollisionsLocal, int32_t realPosition,
        int64_t hash);
};

/*
 * ArrayPositionLinks is used to storing the conflict position, it is a link.
 * pos = key[hashPos]
 * next = positionLinks[pos]
 * next = positionLinks[pos]
 * if (next == -1) means finding the end for a link.
 */
class ArrayPositionLinks {
public:
    explicit ArrayPositionLinks(int32_t capacity);
    ~ArrayPositionLinks();
    int32_t *GetPositionLinks() const
    {
        return positionLinks;
    }
    int32_t GetSize() const
    {
        return size;
    }
    int32_t Link(int32_t left, int32_t right);
    int32_t Start(int32_t position) const;
    int32_t Next(int32_t position) const;

private:
    int32_t *positionLinks;
    int32_t capacity;
    int32_t size;
};
} // end of op
} // end of omniruntime
#endif
