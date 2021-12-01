/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash table implementations
 */
#include "join_hash_table.h"
#include "../pages_hash_strategy.h"
#include "../optimization.h"
#include "../../jit/annotation.h"

#include <algorithm>
#include <memory>

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
const int32_t BLOCK_SIZE = 1024;

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

static int64_t ReadHash(int32_t vecType, omniruntime::vec::Vector *vector, int32_t rowIndex)
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
        case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR:
        case omniruntime::vec::OMNI_VEC_TYPE_CHAR: {
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
        int32_t originalIndex;
        column = VectorHelper::ExpandVectorAndIndex(column, rowIndex, originalIndex);
        if (column->IsValueNull(originalIndex)) {
            continue;
        }
        hash = ReadHash(hashColTypes[columnIdx], column, originalIndex);
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
        int32_t originalIndex;
        column = VectorHelper::ExpandVectorAndIndex(column, rowIndex, originalIndex);
        if (column->IsValueNull(originalIndex)) {
            continue;
        }
        hash = ReadHash(columnTypes[columnIdx], column, originalIndex);
        result = HashUtil::CombineHash(result, hash);
    }

    return result;
}

int64_t JoinHashTables::GetJoinPosition(int32_t position, Vector **joinColumns, int32_t *joinColumnTypes,
    int32_t joinColumnsCount, Vector **allColumns, int32_t allColumnsCount, int64_t rawHash) const
{
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

static void ReadColumnIntHashes(int32_t offset, int32_t addressesCount, int64_t *addresses, Vector **columns,
    int64_t *hashes, bool *nullPositions)
{
    Vector *column = nullptr, *result = nullptr;
    int64_t address;
    int64_t hash;
    int32_t vecBatchIndex, rowIndex;
    int32_t currVecBatchIndex = -1;
    bool dictionary = false;
    int32_t idIndex;
    for (int32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetTypeId() == omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (!dictionary) {
            if (column->IsValueNull(rowIndex)) {
                nullPositions[step] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<omniruntime::vec::IntVector *>(column)->GetValue(rowIndex));
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(rowIndex, idIndex);
            if (result->IsValueNull(idIndex)) {
                nullPositions[step] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<omniruntime::vec::IntVector *>(result)->GetValue(idIndex));
        }
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnLongHashes(int32_t offset, int32_t addressesCount, int64_t *addresses, Vector **columns,
    int64_t *hashes, bool *nullPositions)
{
    Vector *column = nullptr, *result = nullptr;
    int64_t address;
    int64_t hash;
    int32_t vecBatchIndex, rowIndex;
    int32_t currVecBatchIndex = -1;
    bool dictionary = false;
    int32_t idIndex;
    for (int32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetTypeId() == omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (!dictionary) {
            if (column->IsValueNull(rowIndex)) {
                nullPositions[step] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<omniruntime::vec::LongVector *>(column)->GetValue(rowIndex));
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(rowIndex, idIndex);
            if (result->IsValueNull(idIndex)) {
                nullPositions[step] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<omniruntime::vec::LongVector *>(result)->GetValue(idIndex));
        }
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnDoubleHashes(int32_t offset, int32_t addressesCount, int64_t *addresses, Vector **columns,
    int64_t *hashes, bool *nullPositions)
{
    Vector *column = nullptr, *result = nullptr;
    int64_t address;
    int64_t hash;
    int32_t vecBatchIndex, rowIndex;
    int32_t currVecBatchIndex = -1;
    bool dictionary = false;
    int32_t idIndex;
    for (int32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetTypeId() == omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (!dictionary) {
            if (column->IsValueNull(rowIndex)) {
                nullPositions[step] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<omniruntime::vec::DoubleVector *>(column)->GetValue(rowIndex));
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(rowIndex, idIndex);
            if (result->IsValueNull(idIndex)) {
                nullPositions[step] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<omniruntime::vec::DoubleVector *>(result)->GetValue(idIndex));
        }
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnDecimal64Hashes(int32_t offset, int32_t addressesCount, int64_t *addresses,
    Vector **columns, int64_t *hashes, bool *nullPositions)
{
    Vector *column = nullptr, *result = nullptr;
    int64_t address;
    int64_t hash;
    int32_t vecBatchIndex, rowIndex;
    int32_t currVecBatchIndex = -1;
    bool dictionary = false;
    int32_t idIndex;
    for (int32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetTypeId() == omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (!dictionary) {
            if (column->IsValueNull(rowIndex)) {
                nullPositions[step] = true;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(static_cast<LongVector *>(column)->GetValue(rowIndex));
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(rowIndex, idIndex);
            if (result->IsValueNull(idIndex)) {
                nullPositions[step] = true;
                continue;
            }
            hash = HashUtil::HashDecimal64Value(static_cast<LongVector *>(result)->GetValue(idIndex));
        }
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnBooleanHashes(int32_t offset, int32_t addressesCount, int64_t *addresses, Vector **columns,
    int64_t *hashes, bool *nullPositions)
{
    Vector *column = nullptr, *result = nullptr;
    int64_t address;
    int64_t hash;
    int32_t vecBatchIndex, rowIndex;
    int32_t currVecBatchIndex = -1;
    bool dictionary = false;
    int32_t idIndex;
    for (int32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetTypeId() == omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (!dictionary) {
            if (column->IsValueNull(rowIndex)) {
                nullPositions[step] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<omniruntime::vec::BooleanVector *>(column)->GetValue(rowIndex));
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(rowIndex, idIndex);
            if (result->IsValueNull(idIndex)) {
                nullPositions[step] = true;
                continue;
            }
            hash = HashUtil::HashValue(static_cast<omniruntime::vec::BooleanVector *>(result)->GetValue(idIndex));
        }
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnDecimal128Hashes(int32_t offset, int32_t addressesCount, int64_t *addresses,
    Vector **columns, int64_t *hashes, bool *nullPositions)
{
    Vector *column = nullptr, *result = nullptr;
    int64_t address;
    int64_t hash;
    Decimal128 decimal128Value;
    int32_t vecBatchIndex, rowIndex;
    int32_t currVecBatchIndex = -1;
    bool dictionary = false;
    int32_t idIndex;
    for (int32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetTypeId() == omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (!dictionary) {
            if (column->IsValueNull(rowIndex)) {
                nullPositions[step] = true;
                continue;
            }
            decimal128Value = static_cast<omniruntime::vec::Decimal128Vector *>(column)->GetValue(rowIndex);
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(rowIndex, idIndex);
            if (result->IsValueNull(idIndex)) {
                nullPositions[step] = true;
                continue;
            }
            decimal128Value = static_cast<omniruntime::vec::Decimal128Vector *>(result)->GetValue(idIndex);
        }
        hash = HashUtil::HashValue(decimal128Value.LowBits(), decimal128Value.HighBits());
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnVarCharHashes(int32_t offset, int32_t addressesCount, int64_t *addresses, Vector **columns,
    int64_t *hashes, bool *nullPositions)
{
    Vector *column = nullptr, *result = nullptr;
    uint8_t *varcharValue = nullptr;
    int64_t address, hash;
    int32_t vecBatchIndex, rowIndex, idIndex, valueLength, currVecBatchIndex = -1;
    bool dictionary = false;
    for (int32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetTypeId() == omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (!dictionary) {
            if (column->IsValueNull(rowIndex)) {
                nullPositions[step] = true;
                continue;
            }
            varcharValue = nullptr;
            valueLength = static_cast<omniruntime::vec::VarcharVector *>(column)->GetValue(rowIndex, &varcharValue);
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(rowIndex, idIndex);
            if (result->IsValueNull(idIndex)) {
                nullPositions[step] = true;
                continue;
            }
            varcharValue = nullptr;
            valueLength = static_cast<omniruntime::vec::VarcharVector *>(result)->GetValue(idIndex, &varcharValue);
        }
        hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ProcessColumns(int32_t offset, int32_t addressesCount, int64_t *addresses, Vector ***columns,
    int32_t *types, int32_t colCount, int64_t *hashes, bool *nullPositions)
{
    for (int32_t columnIdx = 0; columnIdx < colCount; ++columnIdx) {
        switch (types[columnIdx]) {
            case omniruntime::vec::OMNI_VEC_TYPE_INT:
            case omniruntime::vec::OMNI_VEC_TYPE_DATE32:
                ReadColumnIntHashes(offset, addressesCount, addresses, columns[columnIdx], hashes, nullPositions);
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_LONG:
                ReadColumnLongHashes(offset, addressesCount, addresses, columns[columnIdx], hashes, nullPositions);
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE:
                ReadColumnDoubleHashes(offset, addressesCount, addresses, columns[columnIdx], hashes, nullPositions);
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_BOOLEAN:
                ReadColumnBooleanHashes(offset, addressesCount, addresses, columns[columnIdx], hashes, nullPositions);
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL64:
                ReadColumnDecimal64Hashes(offset, addressesCount, addresses, columns[columnIdx], hashes, nullPositions);
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL128:
                ReadColumnDecimal128Hashes(offset, addressesCount, addresses, columns[columnIdx], hashes,
                    nullPositions);
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR:
            case omniruntime::vec::OMNI_VEC_TYPE_CHAR:
                ReadColumnVarCharHashes(offset, addressesCount, addresses, columns[columnIdx], hashes, nullPositions);
                break;
            default:
                break;
        }
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

    int64_t hashCollisionsLocal = 0;
    auto columns = pagesHashStrategy->GetBuildHashColumns();
    auto types = pagesHashStrategy->GetBuildHashColTypes();
    auto colCount = pagesHashStrategy->GetBuildHashColsCount();
    int64_t hashes[BLOCK_SIZE];
    bool nullPositions[BLOCK_SIZE];
    for (int offset = 0; offset < addressesCount; offset += BLOCK_SIZE) {
        for (int32_t step = 0; step < BLOCK_SIZE; step++) {
            hashes[step] = 0;
            nullPositions[step] = false;
        }
        ProcessColumns(offset, addressesCount, addresses, columns, types, colCount, hashes, nullPositions);
        for (int32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
            positionToHashes[offset + step] = static_cast<int8_t>(hashes[step]);
            if (!nullPositions[step]) {
                SetAddressIndex(positionLinks, offset + step, hashes[step], &hashCollisionsLocal);
            }
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
