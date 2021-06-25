#include "join_hash_table.h"
#include "../pages_hash_strategy.h"
#include "../hash_util.h"
#include "../pages_index.h"
#include "../../vector/vector_common.h"
#include "../../vector/vector_helper.h"

#include <algorithm>
namespace omniruntime {
namespace op {
int32_t CACHE_SIZE = 131072; // 128KB

int32_t numberOfTrailingZeros(int32_t value)
{
    if (value == 0) {
        return 32;
    }

    int32_t y;
    int32_t n = 31;
    y = value << 16;
    if (y != 0) {
        n = n - 16;
        value = y;
    }

    y = value << 8;
    if (y != 0) {
        n = n - 8;
        value = y;
    }

    y = value << 4;
    if (y != 0) {
        n = n - 4;
        value = y;
    }

    y = value << 2;
    if (y != 0) {
        n = n - 2;
        value = y;
    }

    uint32_t temp = (uint32_t)(value << 1);
    temp = temp >> 31;
    return n - temp;
}

JoinHashTables::JoinHashTables(int32_t hashTableCount)
{
    hashTables = (int64_t *)malloc(hashTableCount * sizeof(int64_t));
    this->hashTableCount = hashTableCount;
    this->hashTableSize = 0;
    this->partitionMask = hashTableCount - 1;
    this->shiftSize = numberOfTrailingZeros(hashTableCount) + 1;
}

JoinHashTables::~JoinHashTables()
{

}

void JoinHashTables::addHashTable(int32_t partitionIndex, JoinHashTable *hashTable)
{
    std::lock_guard<std::shared_timed_mutex> writerLock(mutex);
    hashTables[partitionIndex] = (int64_t)hashTable;
    hashTableSize++;
}

JoinHashTable *JoinHashTables::getHashTable(int32_t partitionIndex)
{
    std::shared_lock<std::shared_timed_mutex> readerLock(mutex);
    JoinHashTable *hashTable = (JoinHashTable *)(hashTables[partitionIndex]);
    return hashTable;
}

bool JoinHashTables::isJoinPositionEligible()
{
    return true;
}

int64_t JoinHashTables::getNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition)
{
    int32_t partition = decodePartition(currentJoinPosition);
    int32_t joinPosition = decodeJoinPosition(currentJoinPosition);
    JoinHashTable *hashTable = (JoinHashTable *)(hashTables[partition]);
    int32_t nextJoinPosition = hashTable->getNextJoinPosition(joinPosition, probePosition);
    if (nextJoinPosition < 0) {
        return nextJoinPosition;
    }

    int64_t partitionedJoinPosition = encodePartitionedJoinPosition(partition, nextJoinPosition);
    return partitionedJoinPosition;
}

int64_t hashRow(int32_t rowIndex, Vector **columns, int32_t columnCount)
{
    int64_t result = 0;
    Vector *column;
    int64_t hash;

    for (int32_t columnIdx = 0; columnIdx < columnCount; columnIdx++) {
        column = columns[columnIdx];
        if (column->isValueNull(rowIndex)) {
            continue;
        }
        switch (column->getType()) {
            case OMNI_VEC_TYPE_INT: {
                int32_t intValue = ((IntVector *)column)->getValue(rowIndex);
                hash = HashUtil::hashValue((int64_t)intValue);
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                int64_t int64Value = ((LongVector *)column)->getValue(rowIndex);
                hash = HashUtil::hashValue(int64Value);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                double doubleValue = ((DoubleVector *)column)->getValue(rowIndex);
                hash = HashUtil::hashValue((int64_t)doubleValue);
                break;
            }
            default: {
                hash = 0;
                break;
            }
        }

        result = HashUtil::getHash(result, hash);
    }

    return result;
}

int64_t JoinHashTables::getJoinPosition(int32_t position, Vector **joinColumns, int32_t joinColumnsCount, Vector **allColumns, int32_t allColumnsCount)
{
    return getJoinPosition(position, joinColumns, joinColumnsCount, allColumns, allColumnsCount, hashRow(position, joinColumns, joinColumnsCount));
}

int64_t JoinHashTables::getJoinPosition(int32_t position, Vector **joinColumns, int32_t joinColumnsCount, Vector **allColumns, int32_t allColumnsCount, int64_t rawHash)
{
    int32_t partition = HashUtil::getRawHashPartition(rawHash, partitionMask);
    JoinHashTable *hashTable = (JoinHashTable *)(hashTables[partition]);
    int32_t joinPosition = hashTable->getJoinPosition(position, joinColumns, joinColumnsCount, allColumns, allColumnsCount, rawHash);
    if (joinPosition < 0) {
        return joinPosition;
    }

    int64_t partitionedJoinPosition = encodePartitionedJoinPosition(partition, joinPosition);
    return partitionedJoinPosition;
}

void JoinHashTables::getBuildValue(void *value, int64_t partitionedJoinPosition, int32_t outputCol)
{
    int32_t partition = decodePartition(partitionedJoinPosition);
    int32_t joinPosition = decodeJoinPosition(partitionedJoinPosition);
    JoinHashTable *hashTable = (JoinHashTable *)(hashTables[partition]);
    hashTable->getBuildValue(value, joinPosition, outputCol);
}

int64_t JoinHashTables::encodePartitionedJoinPosition(int32_t partition, int32_t joinPosition)
{
    int64_t result = ((int64_t)joinPosition) << shiftSize;
    result |= partition;
    return result;
}

int32_t JoinHashTables::decodePartition(int64_t partitionedJoinPosition)
{
    int32_t result = (int32_t)(partitionedJoinPosition & partitionMask);
    return result;
}

int32_t JoinHashTables::decodeJoinPosition(int64_t partitionedJoinPosition)
{
    uint64_t result = (uint64_t)partitionedJoinPosition;
    result = result >> shiftSize;
    return (int32_t)result;
}

JoinHashTable::JoinHashTable(PagesHashStrategy *pagesHashStrategy, int64_t *addresses, int32_t addressesCount)
{
    positionLinks = new ArrayPositionLinks(addressesCount);
    pagesHash = new PagesHash(addresses, addressesCount, pagesHashStrategy, positionLinks);
}

int32_t JoinHashTable::getNextJoinPosition(int32_t currentJoinPosition, int probePosition)
{
    if (positionLinks == nullptr) {
        return -1;
    }

    return positionLinks->next(currentJoinPosition);
}

int32_t JoinHashTable::getJoinPosition(int32_t position, Vector **joinColumns, int32_t joinColumnsCount, Vector **allColumns, int32_t allColumnsCount, int64_t rawHash)
{
    int32_t addressIndex = pagesHash->getAddressIndex(position, joinColumns, joinColumnsCount, rawHash);
    return positionLinks->start(addressIndex);
}

void JoinHashTable::getBuildValue(void *value, int32_t joinPosition, int32_t outputCol)
{
    pagesHash->getBuildValue(value, joinPosition, outputCol);
}

PagesHash::PagesHash(int64_t *addresses, int32_t addressesCount, PagesHashStrategy *pagesHashStrategy, ArrayPositionLinks *positionLinks)
{
    this->pagesHashStrategy = pagesHashStrategy;
    this->addresses = addresses;
    this->addressesCount = addressesCount;

    keySize = HashUtil::hashArraySize(addressesCount, 0.75f);
    mask = keySize - 1;
    key = new int32_t[keySize];
    for (int32_t i = 0; i < keySize; i++) {
        key[i] = -1;
    }
    positionToHashes = new int8_t[addressesCount];

    int32_t positionsInStep = std::min(addressesCount + 1, CACHE_SIZE / (int32_t)sizeof(int32_t));
    int64_t positionToFullHashes[positionsInStep];
    int64_t hashCollisionsLocal = 0;

    for (int32_t step = 0; step * positionsInStep <= addressesCount; step++) {
        int32_t stepBeginPosition = step * positionsInStep;
        int32_t stepEndPosition = std::min((step + 1) * positionsInStep, addressesCount);
        int32_t stepSize = stepEndPosition - stepBeginPosition;

        for (int32_t position = 0; position < stepSize; position++) {
            int32_t realPosition = position + stepBeginPosition;
            int64_t hash = getRawHash(realPosition);
            positionToFullHashes[position] = hash;
            positionToHashes[realPosition] = (int8_t) hash;
        }

        for (int32_t position = 0; position < stepSize; position++) {
            int32_t realPosition = position + stepBeginPosition;
            if (isPositionNull(realPosition)) {
                continue;
            }

            int64_t hash = positionToFullHashes[position];
            int32_t pos = HashUtil::getRawHashPosition(hash, mask);

            // look for an empty slot or a slot containing this key
            while (key[pos] != -1) {
                int32_t currentKey = key[pos];
                if (((int8_t) hash) == positionToHashes[currentKey] && positionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
                    // found a slot for this key
                    // link the new key position to the current key position
                    realPosition = positionLinks->link(realPosition, currentKey);

                    // key[pos] updated outside of this loop
                    break;
                }
                // increment position and mask to handler wrap around
                pos = (pos + 1) & mask;
                hashCollisionsLocal++;
            }

            key[pos] = realPosition;
        }
    }
    hashCollisions = hashCollisionsLocal;
}

PagesHash::~PagesHash()
{
    delete[] key;
    delete[] positionToHashes;
}

int32_t PagesHash::getAddressIndex(int probePosition, Vector **joinColumns, int32_t joinColumnsCount, int64_t rawHash)
{
    int32_t pos = HashUtil::getRawHashPosition(rawHash, mask);

    while (key[pos] != -1) {
        if (positionEqualsCurrentRowIgnoreNulls(key[pos], (int8_t)rawHash, probePosition, joinColumns)) {
            return key[pos];
        }

        pos = (pos + 1) & mask;
    }

    return -1;
}

void PagesHash::getBuildValue(void *value, int32_t joinPosition, int32_t outputCol)
{
   int64_t address = addresses[joinPosition];
   int32_t vecBatchIndex = decodeSliceIndex(address);
   int32_t rowIndex = decodePosition(address);

   VectorHelper::getValue(pagesHashStrategy->getBuildColumns()[outputCol][vecBatchIndex], rowIndex, value);
}

int64_t PagesHash::getRawHash(int32_t position)
{
    int64_t address = addresses[position];
    int32_t vecBatchIndex = decodeSliceIndex(address);
    int32_t rowIndex = decodePosition(address);

    return pagesHashStrategy->hashPosition(vecBatchIndex, rowIndex);
}

bool PagesHash::isPositionNull(int32_t position)
{
    int64_t address = addresses[position];
    int32_t vecBatchIndex = decodeSliceIndex(address);
    int32_t rowIndex = decodePosition(address);

    return pagesHashStrategy->isPositionNull(vecBatchIndex, rowIndex);
}

bool PagesHash::positionEqualsPositionIgnoreNulls(int32_t leftPosition, int32_t rightPosition)
{
    int64_t leftAddress = addresses[leftPosition];
    int32_t leftTableIndex = decodeSliceIndex(leftAddress);
    int32_t leftRowIndex = decodePosition(leftAddress);

    int64_t rightAddress = addresses[rightPosition];
    int32_t rightTableIndex = decodeSliceIndex(rightAddress);
    int32_t rightRowIndex = decodePosition(rightAddress);

    return pagesHashStrategy->positionEqualsPositionIgnoreNulls(leftTableIndex, leftRowIndex, rightTableIndex, rightRowIndex);
}

bool PagesHash::positionEqualsCurrentRowIgnoreNulls(int32_t buildPosition, int8_t rawHash, int32_t probePosition, Vector **joinColumns)
{
    if (positionToHashes[buildPosition] != rawHash) {
        return false;
    }

    int64_t address = addresses[buildPosition];
    int32_t vecBatchIndex = decodeSliceIndex(address);
    int32_t rowIndex = decodePosition(address);

    return pagesHashStrategy->positionEqualsRowIgnoreNulls(vecBatchIndex, rowIndex, probePosition, joinColumns);
}

ArrayPositionLinks::ArrayPositionLinks(int32_t size)
{
    positionLinks = new int32_t[size];
    this->size = size;
    for (int32_t i = 0; i < size; i++) {
        positionLinks[i] = -1;
    }
}

ArrayPositionLinks::~ArrayPositionLinks()
{
    delete[] positionLinks;
    positionLinks = nullptr;
    size = 0;
}
int32_t ArrayPositionLinks::link(int32_t left, int32_t right)
{
    positionLinks[left] = right;
    return left;
}

int32_t ArrayPositionLinks::start(int32_t position)
{
    return position;
}

int32_t ArrayPositionLinks::next(int32_t position)
{
    return positionLinks[position];
}
} // end of op
} // end of omniruntime