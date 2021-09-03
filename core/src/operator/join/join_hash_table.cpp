/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash table implementations
 */
#include "join_hash_table.h"
#include "../pages_hash_strategy.h"
#include "../../vector/vector_helper.h"
#include "../optimization.h"
#include "../../jit/annotation.h"
#include "../util/operator_util.h"

#include <algorithm>
#include <memory>

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
const int32_t CACHE_SIZE = 131072; // 128KB

int32_t NumberOfTrailingZeros(int32_t value)
{
    if (value == 0) {
        return ROTATE_DISTANCE_32;
    }

    int32_t y;
    int32_t n = ROTATE_DISTANCE_31;
    y = value << ROTATE_DISTANCE_16;
    if (y != 0) {
        n = n - ROTATE_DISTANCE_16;
        value = y;
    }

    y = value << ROTATE_DISTANCE_8;
    if (y != 0) {
        n = n - ROTATE_DISTANCE_8;
        value = y;
    }

    y = value << ROTATE_DISTANCE_4;
    if (y != 0) {
        n = n - ROTATE_DISTANCE_4;
        value = y;
    }

    y = value << ROTATE_DISTANCE_2;
    if (y != 0) {
        n = n - ROTATE_DISTANCE_2;
        value = y;
    }

    uint32_t temp = static_cast<uint32_t>(value << ROTATE_DISTANCE_1);
    temp = temp >> ROTATE_DISTANCE_31;
    return n - temp;
}

void ArraysFill(int32_t *array, int32_t size, int32_t value)
{
    for (int32_t i = 0; i < size; i++) {
        array[i] = value;
    }
}

JoinHashTables::JoinHashTables(int32_t hashTableCount)
{
    this->hashTables = std::make_unique<JoinHashTable *[]>(hashTableCount).release();
    for (int32_t idx = 0; idx < hashTableCount; idx++) {
        this->hashTables[idx] = nullptr;
    }
    this->hashTableCount = hashTableCount;
    this->hashTableSize = 0;
    this->partitionMask = hashTableCount - 1;
    this->shiftSize = NumberOfTrailingZeros(hashTableCount) + 1;
}

JoinHashTables::~JoinHashTables()
{
    delete[] hashTables;
    hashTables = nullptr;
}

void JoinHashTables::AddHashTable(int32_t partitionIndex, const JoinHashTable *hashTable)
{
    hashTables[partitionIndex] = const_cast<JoinHashTable *>(hashTable);
    hashTableSize++;
}

JoinHashTable *JoinHashTables::GetHashTable(int32_t partitionIndex) const
{
    JoinHashTable *hashTable = hashTables[partitionIndex];
    return hashTable;
}

bool JoinHashTables::IsJoinPositionEligible() const
{
    return true;
}

int64_t JoinHashTables::GetNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition) const
{
    if (hashTableCount != 1) {
        int32_t partition = DecodePartition(currentJoinPosition);
        int32_t joinPosition = DecodeJoinPosition(currentJoinPosition);
        JoinHashTable *hashTable = hashTables[partition];
        int32_t nextJoinPosition = hashTable->GetNextJoinPosition(joinPosition, probePosition);
        if (nextJoinPosition < 0) {
            return nextJoinPosition;
        }

        int64_t partitionedJoinPosition = EncodePartitionedJoinPosition(partition, nextJoinPosition);
        return partitionedJoinPosition;
    } else {
        JoinHashTable *hashTable = hashTables[0];
        return hashTable->GetNextJoinPosition(static_cast<int32_t>(currentJoinPosition), probePosition);
    }
}

ALWAYS_INLINE int64_t ReadHash(int32_t vecType, omniruntime::vec::Vector *vector, int32_t rowIndex)
{
    switch (vecType) {
        case omniruntime::vec::OMNI_VEC_TYPE_INT:
        case omniruntime::vec::OMNI_VEC_TYPE_DATE32:
            return HashUtil::HashValue(static_cast<omniruntime::vec::IntVector *>(vector)->GetValue(rowIndex));
        case omniruntime::vec::OMNI_VEC_TYPE_LONG:
            return HashUtil::HashValue(static_cast<omniruntime::vec::LongVector *>(vector)->GetValue(rowIndex));
        case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE:
            return HashUtil::HashValue(static_cast<omniruntime::vec::DoubleVector *>(vector)->GetValue(rowIndex));
        case omniruntime::vec::OMNI_VEC_TYPE_BOOLEAN:
            return HashUtil::HashValue(static_cast<omniruntime::vec::BooleanVector *>(vector)->GetValue(rowIndex));
        case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL64:
            return HashUtil::HashDecimal64Value(static_cast<LongVector *>(vector)->GetValue(rowIndex));
        case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 decimal128Value = static_cast<omniruntime::vec::Decimal128Vector *>(vector)->GetValue(rowIndex);
            return HashUtil::HashValue(decimal128Value.LowBits(), decimal128Value.HighBits());
        }
        case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *varcharValue = nullptr;
            int32_t valueLength =
                static_cast<omniruntime::vec::VarcharVector *>(vector)->GetValue(rowIndex, &varcharValue);
            return HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
        }
        default: {
            return 0;
        }
    }
}

SPECIALIZE(OMNIJIT_HASH_STRATEGY_HASH_POSITION)
int64_t HashPosition(int32_t vecBatchIdx, int32_t rowIndex, Vector ***buildHashColumns, const int32_t *hashColTypes,
    int32_t hashColCount)
{
    int64_t result = 0;
    Vector *column = nullptr;
    int64_t hash = 0;

    for (int32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        column = buildHashColumns[columnIdx][vecBatchIdx];
        if (column->IsValueNull(rowIndex)) {
            continue;
        }
        column = OperatorUtil::GetDictionary(column, rowIndex);
        hash = ReadHash(hashColTypes[columnIdx], column, rowIndex);
        result = HashUtil::CombineHash(result, hash);
    }
    return result;
}

SPECIALIZE(OMNIJIT_HASH_ROW)
int64_t HashRow(int32_t rowIndex, Vector **columns, const int32_t *columnTypes, int32_t columnCount)
{
    int64_t result = 0;
    Vector *column = nullptr;
    int64_t hash = 0;

    for (int32_t columnIdx = 0; columnIdx < columnCount; columnIdx++) {
        column = columns[columnIdx];
        if (column->IsValueNull(rowIndex)) {
            continue;
        }
        column = OperatorUtil::GetDictionary(column, rowIndex);
        hash = ReadHash(columnTypes[columnIdx], column, rowIndex);
        result = HashUtil::CombineHash(result, hash);
    }

    return result;
}

int64_t JoinHashTables::GetJoinPosition(int32_t position, Vector **joinColumns, int32_t *joinColumnTypes,
    int32_t joinColumnsCount, Vector **allColumns, int32_t allColumnsCount) const
{
    int64_t rawHash = HashRow(position, joinColumns, joinColumnTypes, joinColumnsCount);
    JoinHashTable *hashTable = nullptr;
    if (hashTableCount != 1) {
        int32_t partition = HashUtil::GetRawHashPartition(rawHash, partitionMask);
        hashTable = hashTables[partition];
        if (hashTable == nullptr) {
            return -1;
        }
        int32_t joinPosition =
            hashTable->GetJoinPosition(position, joinColumns, joinColumnsCount, allColumns, allColumnsCount, rawHash);
        if (joinPosition < 0) {
            return joinPosition;
        }
        return EncodePartitionedJoinPosition(partition, joinPosition);
    } else {
        hashTable = hashTables[0];
        if (hashTable == nullptr) {
            return -1;
        }
        return hashTable->GetJoinPosition(position, joinColumns, joinColumnsCount, allColumns, allColumnsCount,
            rawHash);
    }
}

int32_t JoinHashTables::GetBuildValue(void *value, int64_t partitionedJoinPosition, int32_t outputCol) const
{
    JoinHashTable *hashTable = nullptr;
    if (hashTableCount != 1) {
        int32_t partition = DecodePartition(partitionedJoinPosition);
        int32_t joinPosition = DecodeJoinPosition(partitionedJoinPosition);
        hashTable = hashTables[partition];
        return hashTable->GetBuildValue(value, joinPosition, outputCol);
    } else {
        hashTable = hashTables[0];
        return hashTable->GetBuildValue(value, static_cast<int32_t>(partitionedJoinPosition), outputCol);
    }
}

void JoinHashTables::Clear(int32_t partitionIndex)
{
    if (hashTableSize == 0) {
        return;
    }
    if (hashTables[partitionIndex] != nullptr) {
        delete hashTables[partitionIndex];
    }
    hashTables[partitionIndex] = nullptr;
    hashTableSize--;
}

int64_t JoinHashTables::EncodePartitionedJoinPosition(int32_t partition, int32_t joinPosition) const
{
    int64_t result = static_cast<int64_t>(joinPosition) << shiftSize;
    result |= partition;
    return result;
}

int32_t JoinHashTables::DecodePartition(int64_t partitionedJoinPosition) const
{
    int32_t result = static_cast<int32_t>(partitionedJoinPosition & partitionMask);
    return result;
}

int32_t JoinHashTables::DecodeJoinPosition(int64_t partitionedJoinPosition) const
{
    uint64_t result = static_cast<uint64_t>(partitionedJoinPosition);
    result = result >> shiftSize;
    return static_cast<int32_t>(result);
}

JoinHashTable::JoinHashTable(PagesHashStrategy *pagesHashStrategy, int64_t *addresses, int32_t addressesCount)
{
    positionLinks = std::make_unique<ArrayPositionLinks>(addressesCount).release();
    pagesHash = std::make_unique<PagesHash>(addresses, addressesCount, pagesHashStrategy, positionLinks).release();
}

JoinHashTable::~JoinHashTable()
{
    delete positionLinks;
    delete pagesHash;
}

int32_t JoinHashTable::GetNextJoinPosition(int32_t currentJoinPosition, int probePosition) const
{
    if (positionLinks->GetSize() == 0) {
        return -1;
    }

    return positionLinks->Next(currentJoinPosition);
}

int32_t JoinHashTable::GetJoinPosition(int32_t position, Vector **joinColumns, int32_t joinColumnsCount,
    Vector **allColumns, int32_t allColumnsCount, int64_t rawHash) const
{
    int32_t addressIndex = pagesHash->GetAddressIndex(position, joinColumns, joinColumnsCount, rawHash);
    return StartJoinPosition(addressIndex, position, allColumns, allColumnsCount);
}

int32_t JoinHashTable::StartJoinPosition(int32_t currentJoinPosition, int32_t probePosition, Vector **allColumns,
    int32_t allColumnsCount) const
{
    if (currentJoinPosition == -1) {
        return -1;
    }

    if (positionLinks->GetSize() == 0) {
        return currentJoinPosition;
    }

    return positionLinks->Start(currentJoinPosition);
}

int32_t JoinHashTable::GetBuildValue(void *value, int32_t joinPosition, int32_t outputCol) const
{
    return pagesHash->GetBuildValue(value, joinPosition, outputCol);
}

void JoinHashTable::PrintHashTable(int32_t partitionIndex) const
{
    int32_t *key = pagesHash->GetKey();
    int32_t keySize = pagesHash->GetKeySize();
    int8_t *positionToHash = pagesHash->GetPositionToHashes();
    int64_t *addresses = pagesHash->GetAddresses();
    int32_t positionCount = pagesHash->GetAddressesCount();
    int32_t *positionLinks = this->positionLinks->GetPositionLinks();
    for (int32_t i = 0; i < keySize; i++) {
        std::cout << "partitionIndex=" << partitionIndex << ", key[" << i << "]=" << key[i] << std::endl;
    }
    for (int32_t i = 0; i < positionCount; i++) {
        int32_t hash = static_cast<int32_t>(positionToHash[i]);
        std::cout << "partitionIndex=" << partitionIndex << ", addresses[" << i << "]=" << addresses[i] <<
            ", positionToHash[" << i << "]=" << hash << ", positionLinks[" << i << "]=" << positionLinks[i] <<
            std::endl;
    }
}

PagesHash::PagesHash(int64_t *addresses, int32_t addressesCount, PagesHashStrategy *pagesHashStrategy,
    ArrayPositionLinks *positionLinks)
{
    this->pagesHashStrategy = pagesHashStrategy;
    this->addresses = addresses;
    this->addressesCount = addressesCount;

    keySize = HashUtil::HashArraySize(addressesCount, 0.75f);
    mask = keySize - 1;
    key = std::make_unique<int32_t[]>(keySize).release();
    ArraysFill(key, keySize, -1);
    positionToHashes = std::make_unique<int8_t[]>(addressesCount).release();

    int32_t positionsInStep = std::min(addressesCount + 1, CACHE_SIZE / static_cast<int32_t>(sizeof(int32_t)));
    int64_t positionToFullHashes[positionsInStep];
    int64_t hashCollisionsLocal = 0;

    for (int32_t step = 0; step * positionsInStep <= addressesCount; step++) {
        int32_t stepBeginPosition = step * positionsInStep;
        int32_t stepEndPosition = std::min((step + 1) * positionsInStep, addressesCount);
        int32_t stepSize = stepEndPosition - stepBeginPosition;

        for (int32_t position = 0; position < stepSize; position++) {
            int32_t realPosition = position + stepBeginPosition;
            int64_t hash = GetRawHash(realPosition);
            positionToFullHashes[position] = hash;
            positionToHashes[realPosition] = static_cast<int8_t>(hash);
        }

        for (int32_t position = 0; position < stepSize; position++) {
            int32_t realPosition = position + stepBeginPosition;
            if (IsPositionNull(realPosition)) {
                continue;
            }

            int64_t hash = positionToFullHashes[position];
            SetAddressIndex(positionLinks, realPosition, hash, &hashCollisionsLocal);
        }
    }
    hashCollisions = hashCollisionsLocal;
}

void PagesHash::SetAddressIndex(ArrayPositionLinks *positionLinks, int32_t realPosition, int64_t hash,
    int64_t *totalHashCollisions) const
{
    int64_t hashCollisionsLocal = 0;
    int32_t pos = HashUtil::GetRawHashPosition(hash, mask);
    // look for an empty slot or a slot containing this key
    while (key[pos] != -1) {
        int32_t currentKey = key[pos];
        if (static_cast<int8_t>(hash) == positionToHashes[currentKey] &&
            PositionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
            // found a slot for this key
            // link the new key position to the current key position
            realPosition = positionLinks->Link(realPosition, currentKey);

            // key[pos] updated outside of this loop
            break;
        }
        // increment position and mask to handler wrap around
        pos = (pos + 1) & mask;
        hashCollisionsLocal++;
    }
    key[pos] = realPosition;
    *totalHashCollisions += hashCollisionsLocal;
}

PagesHash::~PagesHash()
{
    delete[] key;
    delete[] positionToHashes;
    delete pagesHashStrategy;
}

int32_t PagesHash::GetAddressIndex(int probePosition, Vector **joinColumns, int32_t joinColumnsCount,
    int64_t rawHash) const
{
    int32_t pos = HashUtil::GetRawHashPosition(rawHash, mask);
    while (key[pos] != -1) {
        if (PositionEqualsCurrentRowIgnoreNulls(key[pos], static_cast<int8_t>(rawHash), probePosition, joinColumns)) {
            return key[pos];
        }

        pos = (pos + 1) & mask;
    }

    return -1;
}

int32_t PagesHash::GetBuildValue(void *value, int32_t joinPosition, int32_t outputCol) const
{
    int64_t address = addresses[joinPosition];
    int32_t vecBatchIndex = DecodeSliceIndex(address);
    int32_t rowIndex = DecodePosition(address);

    return VectorHelper::GetValue(pagesHashStrategy->GetBuildColumns()[outputCol][vecBatchIndex], rowIndex, value);
}

int64_t PagesHash::GetRawHash(int32_t position) const
{
    int64_t address = addresses[position];
    int32_t vecBatchIndex = DecodeSliceIndex(address);
    int32_t rowIndex = DecodePosition(address);

    return HashPosition(vecBatchIndex, rowIndex, pagesHashStrategy->GetBuildHashColumns(),
        pagesHashStrategy->GetBuildHashColTypes(), pagesHashStrategy->GetBuildHashColsCount());
}

bool PagesHash::IsPositionNull(int32_t position) const
{
    int64_t address = addresses[position];
    int32_t vecBatchIndex = DecodeSliceIndex(address);
    int32_t rowIndex = DecodePosition(address);

    return pagesHashStrategy->IsPositionNull(vecBatchIndex, rowIndex);
}

bool PagesHash::PositionEqualsPositionIgnoreNulls(int32_t leftPosition, int32_t rightPosition) const
{
    int64_t leftAddress = addresses[leftPosition];
    int32_t leftTableIndex = DecodeSliceIndex(leftAddress);
    int32_t leftRowIndex = DecodePosition(leftAddress);

    int64_t rightAddress = addresses[rightPosition];
    int32_t rightTableIndex = DecodeSliceIndex(rightAddress);
    int32_t rightRowIndex = DecodePosition(rightAddress);

    return ::PositionEqualsPositionIgnoreNulls(leftTableIndex, leftRowIndex, rightTableIndex, rightRowIndex,
        pagesHashStrategy->GetBuildHashColumns(), pagesHashStrategy->GetBuildHashColTypes(),
        pagesHashStrategy->GetBuildHashColsCount());
}

bool PagesHash::PositionEqualsCurrentRowIgnoreNulls(int32_t buildPosition, int8_t rawHash, int32_t probePosition,
    Vector **joinColumns) const
{
    if (positionToHashes[buildPosition] != rawHash) {
        return false;
    }

    int64_t address = addresses[buildPosition];
    int32_t vecBatchIndex = DecodeSliceIndex(address);
    int32_t rowIndex = DecodePosition(address);

    return ::PositionEqualsRowIgnoreNulls(vecBatchIndex, rowIndex, probePosition, joinColumns,
        pagesHashStrategy->GetBuildHashColumns(), pagesHashStrategy->GetBuildHashColTypes(),
        pagesHashStrategy->GetBuildHashColsCount());
}

ArrayPositionLinks::ArrayPositionLinks(int32_t capacity)
{
    this->positionLinks = std::make_unique<int32_t[]>(capacity).release();
    this->capacity = capacity;
    ArraysFill(this->positionLinks, capacity, -1);
    this->size = 0;
}

ArrayPositionLinks::~ArrayPositionLinks()
{
    delete[] positionLinks;
    positionLinks = nullptr;
    capacity = 0;
    size = 0;
}
int32_t ArrayPositionLinks::Link(int32_t left, int32_t right)
{
    size++;
    positionLinks[left] = right;
    return left;
}

int32_t ArrayPositionLinks::Start(int32_t position) const
{
    return position;
}

int32_t ArrayPositionLinks::Next(int32_t position) const
{
    return positionLinks[position];
}
} // end of op
} // end of omniruntime