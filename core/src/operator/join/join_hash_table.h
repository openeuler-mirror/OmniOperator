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
    uint32_t *GetPositionLinks() const
    {
        return positionLinks;
    }
    uint32_t GetSize() const
    {
        return size;
    }
    uint32_t Link(uint32_t left, uint32_t right);
    uint32_t Start(uint32_t position) const;
    uint32_t Next(uint32_t position) const;

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
    uint32_t *GetKey() const
    {
        return key;
    }
    uint32_t GetKeySize() const
    {
        return keySize;
    }
    void SetAddressIndex(ArrayPositionLinks &positionLinks, uint32_t realPosition, int64_t hash,
        uint64_t &totalHashCollisions) const;
    uint32_t GetAddressIndex(uint32_t probePosition, omniruntime::vec::Vector **joinColumns, int64_t rawHash) const;

    int8_t *GetPositionToHashes() const
    {
        return positionToHashes;
    }
    uint64_t *GetAddresses() const
    {
        return addresses;
    }
    uint32_t GetAddressesCount() const
    {
        return addressesCount;
    }

    PagesHashStrategy *GetPagesHashStrategy() const
    {
        return pagesHashStrategy;
    }

private:
    bool PositionEqualsPositionIgnoreNulls(uint32_t leftPosition, uint32_t rightPosition) const;
    bool PositionEqualsCurrentRowIgnoreNulls(uint32_t buildPosition, int8_t rawHash, uint32_t probePosition,
        omniruntime::vec::Vector **joinColumns) const;

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
    PagesHash *GetPagesHash() const
    {
        return pagesHash;
    }

    ArrayPositionLinks *GetPositionLinks() const
    {
        return positionLinks;
    }

    uint32_t GetNextJoinPosition(uint32_t currentJoinPosition) const;
    uint32_t GetJoinPosition(uint32_t position, omniruntime::vec::Vector **joinColumns, int64_t rawHash) const;
    void PrintHashTable(uint32_t partitionIndex) const;

private:
    uint32_t StartJoinPosition(uint32_t currentJoinPosition) const;

    ArrayPositionLinks *positionLinks;
    PagesHash *pagesHash;
};

class JoinHashTables {
public:
    explicit JoinHashTables(uint32_t hashTableCount);

    ~JoinHashTables();

    uint32_t GetHashTableSize()
    {
        return hashTableSize;
    }

    uint32_t GetHashTableCount() const
    {
        return hashTableCount;
    }

    uint64_t EncodePartitionedJoinPosition(uint32_t partition, uint32_t joinPosition) const
    {
        auto result = static_cast<uint64_t>(joinPosition) << shiftSize;
        result |= partition;
        return result;
    }

    uint32_t DecodePartition(uint64_t partitionedJoinPosition) const
    {
        auto partition = static_cast<uint32_t>(partitionedJoinPosition & partitionMask);
        return partition;
    }

    uint32_t DecodeJoinPosition(uint64_t partitionedJoinPosition) const
    {
        return static_cast<uint32_t>(partitionedJoinPosition >> shiftSize);
    }

    void SetBuildTypes(DataTypes *buildDataTypes)
    {
        this->buildTypes = buildDataTypes;
    }

    DataTypes *GetBuildDataTypes()
    {
        return this->buildTypes;
    }

    void SetProbeTypes(DataTypes *probeDataTypes)
    {
        this->probeTypes = probeDataTypes;
    }

    void SetFilterExpression(std::string &expression)
    {
        this->filterExpression = expression;
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

    void AddHashTable(uint32_t partitionIndex, JoinHashTable *hashTable);
    JoinHashTable *GetHashTable(uint32_t partitionIndex) const;
    bool IsJoinPositionEligible(uint64_t partitionedJoinPosition, uint32_t probePosition,
        omniruntime::vec::Vector **probeColumns, uint32_t probeColsCount, ExecutionContext *executionContext) const;
    uint64_t GetNextJoinPosition(uint64_t currentJoinPosition) const;
    uint64_t GetJoinPosition(uint32_t position, omniruntime::vec::Vector **joinColumns, int64_t rawHash) const;

private:
    uint32_t hashTableCount;
    JoinHashTable **hashTables; // actually, the type is JoinHashTable **
    std::atomic_uint32_t hashTableSize;
    uint32_t partitionMask;
    uint32_t shiftSize;
    DataTypes *probeTypes;
    DataTypes *buildTypes;
    std::string filterExpression;
    omniruntime::expressions::Expr *filterExpr = nullptr;
    SimpleFilter *simpleFilter = nullptr;
    std::set<int32_t> usedVectors;
};
} // end of op
} // end of omniruntime
#endif
