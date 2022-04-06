/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash table implementations
 */
#include "join_hash_table.h"
#include <algorithm>
#include <memory>
#include "operator/optimization.h"
#include "jit/annotation.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
static constexpr uint32_t BLOCK_SIZE = 1024;

uint32_t NumberOfTrailingZeros(uint32_t value)
{
    if (value == 0) {
        return ROTATE_DISTANCE_32;
    }

    uint32_t y;
    uint32_t n = ROTATE_DISTANCE_31;
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

    uint32_t temp = value << ROTATE_DISTANCE_1;
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
    : hashTableCount(hashTableCount),
      hashTables(new JoinHashTable *[hashTableCount]),
      hashTableSize(0),
      partitionMask(hashTableCount - 1),
      shiftSize(NumberOfTrailingZeros(hashTableCount) + 1),
      probeTypes(nullptr),
      buildTypes(nullptr)
{
    for (int32_t idx = 0; idx < hashTableCount; idx++) {
        this->hashTables[idx] = nullptr;
    }
}

JoinHashTables::~JoinHashTables()
{
    delete[] hashTables;
    hashTables = nullptr;
    if (simpleFilter != nullptr) {
        delete simpleFilter;
        simpleFilter = nullptr;
    }
}

void JoinHashTables::JoinFilterCodeGen()
{
    if (this->filterExpr == nullptr) {
        return;
    }
    simpleFilter = new SimpleFilter(*this->filterExpr);
    simpleFilter->Initialize();
    usedVectors = simpleFilter->GetVectorIndexes();
}

void JoinHashTables::AddHashTable(int32_t partitionIndex, JoinHashTable *hashTable)
{
    hashTables[partitionIndex] = hashTable;
    hashTableSize++;
}

JoinHashTable *JoinHashTables::GetHashTable(int32_t partitionIndex) const
{
    JoinHashTable *hashTable = hashTables[partitionIndex];
    return hashTable;
}

bool JoinHashTables::IsJoinPositionEligible(int64_t partitionedJoinPosition, int32_t probePosition,
    Vector **probeColumns, int32_t probeColsCount, ExecutionContext *executionContext) const
{
    if (!simpleFilter) {
        return true;
    }

    uint32_t partition = 0;
    auto joinPosition = static_cast<uint32_t>(partitionedJoinPosition);
    if (hashTableCount != 1) {
        partition = DecodePartition(static_cast<uint64_t>(partitionedJoinPosition));
        joinPosition = DecodeJoinPosition(static_cast<uint64_t>(partitionedJoinPosition));
    }
    JoinHashTable *hashTable = hashTables[partition];
    auto pagesHash = hashTable->GetPagesHash();
    auto address = pagesHash->GetAddresses()[joinPosition];
    auto vecBatchIndex = static_cast<int32_t>(DecodeSliceIndex(address));
    auto buildPosition = static_cast<int32_t>(DecodePosition(address));

    auto pagesHashStrategy = pagesHash->GetPagesHashStrategy();
    Vector ***buildColumns = pagesHashStrategy->GetBuildColumns();
    int32_t buildColsCount = pagesHashStrategy->GetBuildColsCount();
    int32_t allColsCount = probeColsCount + buildColsCount;

    // left is probe vectors, right is build vectors
    int64_t values[allColsCount];
    bool nulls[allColsCount];
    int32_t lengths[allColsCount];
    memset_s(values, sizeof(values), 0, sizeof(values));
    memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    memset_s(lengths, sizeof(lengths), 0, sizeof(lengths));
    for (auto iter = usedVectors.begin(); iter != usedVectors.end(); ++iter) {
        int32_t vecIdx = *iter;
        auto vector =
            (vecIdx < probeColsCount) ? probeColumns[vecIdx] : buildColumns[vecIdx - probeColsCount][vecBatchIndex];
        auto position = (vecIdx < probeColsCount) ? probePosition : buildPosition;
        nulls[vecIdx] = vector->IsValueNull(position);
        values[vecIdx] = VectorHelper::GetValuePtrAndLength(vector, position, lengths + vecIdx);
    }

    return simpleFilter->Evaluate(values, nulls, lengths, reinterpret_cast<int64_t>(executionContext));
}

int64_t JoinHashTables::GetNextJoinPosition(int64_t currentJoinPosition, int32_t probePosition) const
{
    if (hashTableCount != 1) {
        auto partition = DecodePartition(static_cast<uint64_t>(currentJoinPosition));
        auto joinPosition = DecodeJoinPosition(static_cast<uint64_t>(currentJoinPosition));
        JoinHashTable *hashTable = hashTables[partition];
        int32_t nextJoinPosition = hashTable->GetNextJoinPosition(static_cast<int32_t>(joinPosition), probePosition);
        if (nextJoinPosition < 0) {
            return nextJoinPosition;
        }

        return static_cast<int64_t>(EncodePartitionedJoinPosition(partition, nextJoinPosition));
    } else {
        JoinHashTable *hashTable = hashTables[0];
        return hashTable->GetNextJoinPosition(static_cast<int32_t>(currentJoinPosition), probePosition);
    }
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

JoinHashTable::JoinHashTable(PagesHashStrategy *pagesHashStrategy, uint64_t *addresses, uint32_t addressesCount)
    : positionLinks(new ArrayPositionLinks(addressesCount)),
      pagesHash(new PagesHash(addresses, addressesCount, pagesHashStrategy, positionLinks))
{}

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
    auto key = pagesHash->GetKey();
    auto keySize = pagesHash->GetKeySize();
    auto positionToHash = pagesHash->GetPositionToHashes();
    auto addresses = pagesHash->GetAddresses();
    auto positionCount = pagesHash->GetAddressesCount();
    auto links = this->positionLinks->GetPositionLinks();
    for (uint32_t i = 0; i < keySize; i++) {
        std::cout << "partitionIndex=" << partitionIndex << ", key[" << i << "]=" << key[i] << std::endl;
    }
    for (uint32_t i = 0; i < positionCount; i++) {
        int32_t hash = static_cast<int32_t>(positionToHash[i]);
        std::cout << "partitionIndex=" << partitionIndex << ", addresses[" << i << "]=" << addresses[i] <<
            ", positionToHash[" << i << "]=" << hash << ", positionLinks[" << i << "]=" << links[i] << std::endl;
    }
}

template <typename T>
void ReadColumnHashes(uint32_t offset, uint32_t addressesCount, uint64_t *addresses, Vector **columns, int64_t *hashes,
    bool *nullPositions)
{
    Vector *column = nullptr, *result = nullptr;
    int64_t hash;
    int32_t vecBatchIndex, rowIndex;
    int32_t currVecBatchIndex = -1;
    bool dictionary = false;
    int32_t idIndex;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = static_cast<int32_t>(DecodeSliceIndex(address));
        rowIndex = static_cast<int32_t>(DecodePosition(address));
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsValueNull(rowIndex)) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            hash = HashUtil::HashValue(static_cast<T *>(column)->GetValue(rowIndex));
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(rowIndex, idIndex);
            hash = HashUtil::HashValue(static_cast<T *>(result)->GetValue(idIndex));
        }
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnDecimal64Hashes(uint32_t offset, uint32_t addressesCount, uint64_t *addresses, Vector **columns,
    int64_t *hashes, bool *nullPositions)
{
    Vector *column = nullptr, *result = nullptr;
    int64_t hash;
    int32_t vecBatchIndex, rowIndex;
    int32_t currVecBatchIndex = -1;
    bool dictionary = false;
    int32_t idIndex;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = static_cast<int32_t>(DecodeSliceIndex(address));
        rowIndex = static_cast<int32_t>(DecodePosition(address));
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsValueNull(rowIndex)) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            hash = HashUtil::HashDecimal64Value(static_cast<LongVector *>(column)->GetValue(rowIndex));
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(rowIndex, idIndex);
            hash = HashUtil::HashDecimal64Value(static_cast<LongVector *>(result)->GetValue(idIndex));
        }
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnDecimal128Hashes(uint32_t offset, uint32_t addressesCount, uint64_t *addresses, Vector **columns,
    int64_t *hashes, bool *nullPositions)
{
    Vector *column = nullptr, *result = nullptr;
    int64_t hash;
    Decimal128 decimal128Value;
    int32_t vecBatchIndex, rowIndex;
    int32_t currVecBatchIndex = -1;
    bool dictionary = false;
    int32_t idIndex;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = static_cast<int32_t>(DecodeSliceIndex(address));
        rowIndex = static_cast<int32_t>(DecodePosition(address));
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsValueNull(rowIndex)) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            decimal128Value = static_cast<Decimal128Vector *>(column)->GetValue(rowIndex);
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(rowIndex, idIndex);
            decimal128Value = static_cast<Decimal128Vector *>(result)->GetValue(idIndex);
        }
        hash = HashUtil::HashValue(static_cast<int64_t>(decimal128Value.LowBits()), decimal128Value.HighBits());
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnCharHashes(uint32_t offset, uint32_t addressesCount, uint64_t *addresses, Vector **columns,
    int64_t *hashes, bool *nullPositions)
{
    Vector *column = nullptr, *result = nullptr;
    uint8_t *varcharValue = nullptr;
    int64_t hash;
    int32_t vecBatchIndex, rowIndex, idIndex, valueLength, currVecBatchIndex = -1;
    bool dictionary = false;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = static_cast<int32_t>(DecodeSliceIndex(address));
        rowIndex = static_cast<int32_t>(DecodePosition(address));
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsValueNull(rowIndex)) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            varcharValue = nullptr;
            valueLength = static_cast<VarcharVector *>(column)->GetValue(rowIndex, &varcharValue);
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(rowIndex, idIndex);
            varcharValue = nullptr;
            valueLength = static_cast<VarcharVector *>(result)->GetValue(idIndex, &varcharValue);
        }
        hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

SPECIALIZE(OMNIJIT_JOIN_HASH_TABLE_PROCESS_COLUMNS)
static void ProcessColumns(uint32_t offset, uint32_t addressesCount, uint64_t *addresses, Vector ***columns,
    int32_t *types, int32_t colCount, int64_t *hashes, bool *nullPositions)
{
    for (int32_t columnIdx = 0; columnIdx < colCount; ++columnIdx) {
        switch (types[columnIdx]) {
            case omniruntime::type::OMNI_INT:
            case omniruntime::type::OMNI_DATE32: {
                ReadColumnHashes<IntVector>(offset, addressesCount, addresses, columns[columnIdx], hashes,
                    nullPositions);
                break;
            }
            case omniruntime::type::OMNI_LONG: {
                ReadColumnHashes<LongVector>(offset, addressesCount, addresses, columns[columnIdx], hashes,
                    nullPositions);
                break;
            }
            case omniruntime::type::OMNI_DOUBLE: {
                ReadColumnHashes<DoubleVector>(offset, addressesCount, addresses, columns[columnIdx], hashes,
                    nullPositions);
                break;
            }
            case omniruntime::type::OMNI_BOOLEAN: {
                ReadColumnHashes<BooleanVector>(offset, addressesCount, addresses, columns[columnIdx], hashes,
                    nullPositions);
                break;
            }
            case omniruntime::type::OMNI_DECIMAL64: {
                ReadColumnDecimal64Hashes(offset, addressesCount, addresses, columns[columnIdx], hashes, nullPositions);
                break;
            }
            case omniruntime::type::OMNI_DECIMAL128: {
                ReadColumnDecimal128Hashes(offset, addressesCount, addresses, columns[columnIdx], hashes,
                    nullPositions);
                break;
            }
            case omniruntime::type::OMNI_VARCHAR:
            case omniruntime::type::OMNI_CHAR: {
                ReadColumnCharHashes(offset, addressesCount, addresses, columns[columnIdx], hashes, nullPositions);
                break;
            }
            default: {
                break;
            }
        }
    }
}

PagesHash::PagesHash(uint64_t *addresses, uint32_t addressesCount, PagesHashStrategy *pagesHashStrategy,
    ArrayPositionLinks *positionLinks)
    : pagesHashStrategy(pagesHashStrategy),
      addresses(addresses),
      addressesCount(addressesCount),
      keySize(HashUtil::HashArraySize(addressesCount, 0.75f)),
      key(new int32_t[keySize]),
      mask(keySize - 1),
      positionToHashes(new int8_t[addressesCount]),
      hashCollisions(0)
{
    ArraysFill(key, keySize, -1);

    uint64_t hashCollisionsLocal = 0;
    auto columns = pagesHashStrategy->GetBuildHashColumns();
    auto types = pagesHashStrategy->GetBuildHashColTypes();
    auto colCount = pagesHashStrategy->GetBuildHashColsCount();
    int64_t hashes[BLOCK_SIZE];
    bool nullPositions[BLOCK_SIZE];
    for (uint32_t offset = 0; offset < addressesCount; offset += BLOCK_SIZE) {
        for (uint32_t step = 0; step < BLOCK_SIZE; step++) {
            hashes[step] = 0;
            nullPositions[step] = false;
        }
        ProcessColumns(offset, addressesCount, addresses, columns, types, colCount, hashes, nullPositions);
        for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
            positionToHashes[offset + step] = static_cast<int8_t>(hashes[step]);
            if (!nullPositions[step]) {
                SetAddressIndex(*positionLinks, offset + step, hashes[step], hashCollisionsLocal);
            }
        }
    }
    hashCollisions = hashCollisionsLocal;
}

void PagesHash::SetAddressIndex(ArrayPositionLinks &positionLinks, int32_t realPosition, int64_t hash,
    uint64_t &totalHashCollisions) const
{
    uint64_t hashCollisionsLocal = 0;
    auto pos = HashUtil::GetRawHashPosition(hash, mask);
    // look for an empty slot or a slot containing this key
    while (key[pos] != -1) {
        int32_t currentKey = key[pos];
        if (static_cast<int8_t>(hash) == positionToHashes[currentKey] &&
            PositionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
            // found a slot for this key
            // link the new key position to the current key position
            realPosition = positionLinks.Link(realPosition, currentKey);

            // key[pos] updated outside of this loop
            break;
        }
        // increment position and mask to handler wrap around
        pos = static_cast<uint32_t>(pos + 1) & mask;
        hashCollisionsLocal++;
    }
    key[pos] = realPosition;
    totalHashCollisions += hashCollisionsLocal;
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
    auto pos = HashUtil::GetRawHashPosition(rawHash, mask);
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
    auto leftAddress = addresses[leftPosition];
    auto leftTableIndex = static_cast<int32_t>(DecodeSliceIndex(leftAddress));
    auto leftRowIndex = static_cast<int32_t>(DecodePosition(leftAddress));

    auto rightAddress = addresses[rightPosition];
    auto rightTableIndex = static_cast<int32_t>(DecodeSliceIndex(rightAddress));
    auto rightRowIndex = static_cast<int32_t>(DecodePosition(rightAddress));

    return omniruntime::op::PositionEqualsPositionIgnoreNulls(leftTableIndex, leftRowIndex, rightTableIndex,
        rightRowIndex, pagesHashStrategy->GetBuildHashColumns(), pagesHashStrategy->GetBuildHashColTypes(),
        pagesHashStrategy->GetBuildHashColsCount());
}

bool PagesHash::PositionEqualsCurrentRowIgnoreNulls(int32_t buildPosition, int8_t rawHash, int32_t probePosition,
    Vector **joinColumns) const
{
    if (positionToHashes[buildPosition] != rawHash) {
        return false;
    }

    auto address = addresses[buildPosition];
    auto vecBatchIndex = static_cast<int32_t>(DecodeSliceIndex(address));
    auto rowIndex = static_cast<int32_t>(DecodePosition(address));

    return omniruntime::op::PositionEqualsRowIgnoreNulls(vecBatchIndex, rowIndex, probePosition, joinColumns,
        pagesHashStrategy->GetBuildHashColumns(), pagesHashStrategy->GetBuildHashColTypes(),
        pagesHashStrategy->GetBuildHashColsCount());
}

ArrayPositionLinks::ArrayPositionLinks(int32_t capacity)
    : capacity(capacity), size(0), positionLinks(new int32_t[capacity])
{
    ArraysFill(this->positionLinks, capacity, -1);
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
