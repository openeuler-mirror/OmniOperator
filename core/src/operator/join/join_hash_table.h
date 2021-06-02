#ifndef __JOIN_HASH_TABLE_H__
#define __JOIN_HASH_TABLE_H__

#include "../../vector/table.h"

#include <stdint.h>
#include <shared_mutex>

class ArrayPositionLinks;
class PagesHashStrategy;
class PagesHash;
class JoinHashTable;

class JoinHashTables
{
public:
    JoinHashTables(int32_t hashTableCount);
    ~JoinHashTables();
    void addHashTable(int32_t partitionIndex, JoinHashTable *hashTable);
    int32_t getHashTableCount()
    {
        return hashTableCount;
    }
    JoinHashTable *getHashTable(int32_t partitionIndex);
    bool isJoinPositionEligible();
    int64_t getNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition);
    int64_t getJoinPosition(int32_t position, Column **joinColumns, int32_t joinColumnsCount, Column **allColumns, int32_t allColumnsCount);
    int64_t getJoinPosition(int32_t position, Column **joinColumns, int32_t joinColumnsCount, Column **allColumns, int32_t allColumnsCount, int64_t rawHash);
    void *getBuildData(int64_t partitionedJoinPosition, int32_t outputCol);

private:
    int64_t encodePartitionedJoinPosition(int32_t partition, int32_t joinPosition);
    int32_t decodePartition(int64_t partitionedJoinPosition);
    int32_t decodeJoinPosition(int64_t partitionedJoinPosition);

    int64_t *hashTables;  // actually, the type is JoinHashTable **
    int32_t hashTableCount;
    std::shared_timed_mutex mutex;
    int32_t partitionMask;
    int32_t shiftSize;
};

class JoinHashTable
{
public:
    JoinHashTable(){}
    JoinHashTable(PagesHashStrategy *pagesHashStrategy, int64_t *addresses, int32_t addressesCount);
    ~JoinHashTable();
    PagesHash *getPagesHash()
    {
        return pagesHash;
    }

    ArrayPositionLinks *getPositionLinks()
    {
        return positionLinks;
    }

    int64_t getJoinPosition(int32_t position, Column **joinColumns, Column **allColumns);
    int64_t getJoinPosition(int32_t position, Column **joinColumns, Column **allColumns, int64_t rawHash);
    int32_t getNextJoinPosition(int32_t currentJoinPosition, int probePosition);
    int32_t getJoinPosition(int32_t position, Column **joinColumns, int32_t joinColumnsCount, Column **allColumns, int32_t allColumnsCount, int64_t rawHash);
    void *getBuildData(int32_t joinPosition, int32_t outputCol);

private:
    PagesHash *pagesHash;
    ArrayPositionLinks *positionLinks;
};

class PagesHash
{
public:
    PagesHash(int64_t *addresses, int32_t addressesSize, PagesHashStrategy *pagesHashStrategy, ArrayPositionLinks *positionLinks);
    ~PagesHash();
    int32_t *getKey()
    {
        return key;
    }
    int32_t getKeySize()
    {
        return keySize;
    }
    int32_t getAddressIndex(int probePosition, Column **joinColumns, int32_t joinColumnsCount, int64_t rawHash);
    void *getBuildData(int32_t joinPosition, int32_t outputCol);

private:
    int64_t getRawHash(int32_t position);
    bool isPositionNull(int32_t position);
    bool positionEqualsPositionIgnoreNulls(int32_t leftPosition, int32_t rightPosition);
    bool positionEqualsCurrentRowIgnoreNulls(int32_t buildPosition, int8_t rawHash, int32_t probePosition, Column **joinColumns, int32_t joinColumnsCount);

    PagesHashStrategy *pagesHashStrategy;
    int64_t *addresses;
    int32_t addressesCount;
    int32_t mask;
    int32_t *key; // it is used to store the addresses index. the key index is from getRawHashPosition()
    int32_t keySize;
    int8_t *positionToHashes;
    int64_t hashCollisions;
};

/*
 * ArrayPositionLinks is used to storing the conflict position, it is a link.
 * pos = key[hashPos]
 * next = positionLinks[pos]
 * next = positionLinks[pos]
 * if (next == -1) means finding the end for a link.
 */
class ArrayPositionLinks
{
public:
    ArrayPositionLinks(int32_t size);
    ~ArrayPositionLinks();
    int32_t *getPositionLinks()
    {
        return positionLinks;
    }
    int32_t getPositionLinkSize()
    {
        return size;
    }
    int32_t link(int32_t left, int32_t right);
    int32_t start(int32_t position);
    int32_t next(int32_t position);
private:
    int32_t *positionLinks;
    int32_t size;
};

/*
 * select * from t1 join t2 on t1.a1=t2.a1 and t1.b1=t2.b1
 * join columns for build table t2 is t2.a1 and t2.b1, so column count is 2.
 */
class PagesHashStrategy
{
public:
    PagesHashStrategy(Column ***columns, int32_t tableCount, int32_t columnCount, int32_t *joinCols, int32_t joinColsCount);
    ~PagesHashStrategy();
    int64_t hashPosition(int32_t tableIndex, int32_t rowIndex);
    int64_t hashRow(int32_t rowIndex, Table *table);
    bool isPositionNull(int32_t tableIndex, int32_t rowIndex);
    bool positionEqualsPositionIgnoreNulls(int32_t leftTableIndex, int32_t leftRowIndex, int32_t rightTableIndex, int32_t rightRowIndex);
    bool positionEqualsRowIgnoreNulls(int32_t buildTableIndex, int32_t buildRowIndex, int32_t probePosition, Column **joinColumns, int32_t joinColumnsCount);
    Column ***getBuildColumns()
    {
        return buildColumns;
    }

private:
    Column ***buildColumns; // Column *[colCount][tableCount]
    int32_t buildTableCount;
    int32_t buildColumnCount;  // column count
    //int32_t *joinCols;
    Column ***buildHashColumns; // Column *[join colCount][tableCount]
    int32_t buildHashColsCount; // join column count
};

#endif
