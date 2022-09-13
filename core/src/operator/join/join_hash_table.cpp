/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash table implementations
 */
#include "join_hash_table.h"
#include <algorithm>
#include <memory>

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

void ArraysFill(uint32_t *array, uint32_t size, uint32_t value)
{
    for (uint32_t i = 0; i < size; i++) {
        array[i] = value;
    }
}

JoinHashTables::JoinHashTables(uint32_t hashTableCount)
    : hashTableCount(hashTableCount),
      hashTables(new JoinHashTable *[hashTableCount]),
      hashTableSize(0),
      partitionMask(hashTableCount - 1),
      shiftSize(NumberOfTrailingZeros(hashTableCount) + 1),
      probeTypes(nullptr),
      buildTypes(nullptr)
{
    for (uint32_t idx = 0; idx < hashTableCount; idx++) {
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
    auto result = simpleFilter->Initialize();
    if (!result) {
        delete simpleFilter;
        simpleFilter = nullptr;
        throw omniruntime::exception::OmniException("EXPRESSION_NOT_SUPPORT", "The expression is not supported yet.");
    }
    usedVectors = simpleFilter->GetVectorIndexes();
}

void JoinHashTables::AddHashTable(uint32_t partitionIndex, JoinHashTable *hashTable)
{
    hashTables[partitionIndex] = hashTable;
    hashTableSize++;
    totalVisitedCounts += hashTable->GetVisitedPositionsSize();
}

JoinHashTable *JoinHashTables::GetHashTable(uint32_t partitionIndex) const
{
    JoinHashTable *hashTable = hashTables[partitionIndex];
    return hashTable;
}

bool JoinHashTables::IsJoinPositionEligible(uint64_t partitionedJoinPosition, uint32_t probePosition,
    Vector **probeColumns, uint32_t probeColsCount, ExecutionContext *executionContext) const
{
    if (!simpleFilter) {
        return true;
    }

    auto partition = DecodePartition(partitionedJoinPosition);
    auto joinPosition = DecodeJoinPosition(partitionedJoinPosition);
    auto hashTable = hashTables[partition];
    auto pagesHash = hashTable->GetPagesHash();
    auto address = pagesHash->GetAddresses()[joinPosition];
    auto vecBatchIndex = DecodeSliceIndex(address);
    auto buildPosition = DecodePosition(address);

    auto pagesHashStrategy = pagesHash->GetPagesHashStrategy();
    Vector ***buildColumns = pagesHashStrategy->GetBuildColumns();
    auto buildColsCount = pagesHashStrategy->GetBuildColsCount();
    auto allColsCount = probeColsCount + buildColsCount;

    // left is probe vectors, right is build vectors
    int64_t values[allColsCount];
    bool nulls[allColsCount];
    int32_t lengths[allColsCount];
    memset_s(values, sizeof(values), 0, sizeof(values));
    memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    memset_s(lengths, sizeof(lengths), 0, sizeof(lengths));
    for (auto iter = usedVectors.begin(); iter != usedVectors.end(); ++iter) {
        auto vecIdx = static_cast<uint32_t>(*iter);
        auto vector =
            (vecIdx < probeColsCount) ? probeColumns[vecIdx] : buildColumns[vecIdx - probeColsCount][vecBatchIndex];
        auto position = static_cast<int32_t>((vecIdx < probeColsCount) ? probePosition : buildPosition);
        nulls[vecIdx] = vector->IsValueNull(position);
        values[vecIdx] = VectorHelper::GetValuePtrAndLength(vector, position, lengths + vecIdx);
    }

    return simpleFilter->Evaluate(values, nulls, lengths, reinterpret_cast<int64_t>(executionContext));
}

uint64_t JoinHashTables::GetNextJoinPosition(uint64_t currentJoinPosition) const
{
    auto partition = DecodePartition(currentJoinPosition);
    auto joinPosition = DecodeJoinPosition(currentJoinPosition);
    auto hashTable = hashTables[partition];
    auto nextJoinPosition = hashTable->GetNextJoinPosition(joinPosition);
    if (nextJoinPosition == INVALID_POSITION) {
        return INVALID_PARTITION_POSITION;
    } else {
        return EncodePartitionedJoinPosition(partition, nextJoinPosition);
    }
}

uint64_t JoinHashTables::GetJoinPosition(uint32_t position, Vector **joinColumns, int64_t rawHash) const
{
    auto partition = (hashTableCount != 1) ? HashUtil::GetRawHashPartition(rawHash, partitionMask) : 0U;
    auto hashTable = hashTables[partition];
    auto joinPosition = hashTable->GetJoinPosition(position, joinColumns, rawHash);
    if (joinPosition == INVALID_POSITION) {
        return INVALID_PARTITION_POSITION;
    }

    return EncodePartitionedJoinPosition(partition, joinPosition);
}

void JoinHashTables::PositionVisited(uint64_t currentJoinPosition)
{
    auto partition = DecodePartition(currentJoinPosition);
    auto joinPosition = DecodeJoinPosition(currentJoinPosition);
    auto hashTable = hashTables[partition];
    if (!hashTable->HasVisited(joinPosition)) {
        hashTable->Visit(joinPosition);
        visitedCounts++;
    }
}

uint32_t JoinHashTables::GetTotalVisitedCounts() const
{
    return totalVisitedCounts;
}

JoinHashTable::JoinHashTable(PagesHashStrategy *pagesHashStrategy, uint64_t *addresses, uint32_t addressesCount)
    : positionLinks(new ArrayPositionLinks(addressesCount)),
      pagesHash(new PagesHash(addresses, addressesCount, pagesHashStrategy, positionLinks)),
      visitedPositionsSize(pagesHash->GetAddressesCount())
{
    visitedPositions.resize(visitedPositionsSize);
    std::fill(visitedPositions.begin(), visitedPositions.end(), false);
}

JoinHashTable::~JoinHashTable()
{
    delete positionLinks;
    delete pagesHash;
}

uint32_t JoinHashTable::GetNextJoinPosition(uint32_t currentJoinPosition) const
{
    if (positionLinks->GetSize() == 0) {
        return INVALID_POSITION;
    }

    return positionLinks->Next(currentJoinPosition);
}

uint32_t JoinHashTable::GetJoinPosition(uint32_t position, Vector **joinColumns, int64_t rawHash) const
{
    auto addressIndex = pagesHash->GetAddressIndex(position, joinColumns, rawHash);
    return StartJoinPosition(addressIndex);
}

uint32_t JoinHashTable::StartJoinPosition(uint32_t currentJoinPosition) const
{
    if (currentJoinPosition == INVALID_POSITION) {
        return INVALID_POSITION;
    }

    if (positionLinks->GetSize() == 0) {
        return currentJoinPosition;
    }

    return positionLinks->Start(currentJoinPosition);
}

void JoinHashTable::PrintHashTable(uint32_t partitionIndex) const
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
        auto hash = static_cast<int32_t>(positionToHash[i]);
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
    uint32_t vecBatchIndex, rowIndex;
    uint32_t currVecBatchIndex = INVALID_POSITION;
    bool dictionary = false;
    int32_t idIndex;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsValueNull(static_cast<int32_t>(rowIndex))) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            hash = HashUtil::HashValue(static_cast<T *>(column)->GetValue(static_cast<int32_t>(rowIndex)));
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(static_cast<int32_t>(rowIndex),
                idIndex);
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
    uint32_t vecBatchIndex, rowIndex;
    uint32_t currVecBatchIndex = INVALID_POSITION;
    bool dictionary = false;
    int32_t idIndex;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsValueNull(static_cast<int32_t>(rowIndex))) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            hash = HashUtil::HashDecimal64Value(
                static_cast<LongVector *>(column)->GetValue(static_cast<int32_t>(rowIndex)));
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(static_cast<int32_t>(rowIndex),
                idIndex);
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
    uint32_t vecBatchIndex, rowIndex;
    uint32_t currVecBatchIndex = INVALID_POSITION;
    bool dictionary = false;
    int32_t idIndex;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsValueNull(static_cast<int32_t>(rowIndex))) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            decimal128Value = static_cast<Decimal128Vector *>(column)->GetValue(static_cast<int32_t>(rowIndex));
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(static_cast<int32_t>(rowIndex),
                idIndex);
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
    uint32_t vecBatchIndex, rowIndex, currVecBatchIndex = INVALID_POSITION;
    int32_t idIndex, valueLength;
    bool dictionary = false;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == omniruntime::vec::OMNI_VEC_ENCODING_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsValueNull(static_cast<int32_t>(rowIndex))) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            varcharValue = nullptr;
            valueLength = static_cast<VarcharVector *>(column)->GetValue(static_cast<int32_t>(rowIndex), &varcharValue);
        } else {
            result = static_cast<DictionaryVector *>(column)->ExtractDictionaryAndId(static_cast<int32_t>(rowIndex),
                idIndex);
            varcharValue = nullptr;
            valueLength = static_cast<VarcharVector *>(result)->GetValue(idIndex, &varcharValue);
        }
        hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ProcessColumns(uint32_t offset, uint32_t addressesCount, uint64_t *addresses, Vector ***columns,
    int32_t *types, uint32_t colCount, int64_t *hashes, bool *nullPositions)
{
    for (uint32_t columnIdx = 0; columnIdx < colCount; ++columnIdx) {
        switch (types[columnIdx]) {
            case omniruntime::type::OMNI_INT:
            case omniruntime::type::OMNI_DATE32: {
                ReadColumnHashes<IntVector>(offset, addressesCount, addresses, columns[columnIdx], hashes,
                    nullPositions);
                break;
            }
            case omniruntime::type::OMNI_SHORT: {
                ReadColumnHashes<ShortVector>(offset, addressesCount, addresses, columns[columnIdx], hashes,
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
      key(new uint32_t[keySize]),
      mask(keySize - 1),
      positionToHashes(new int8_t[addressesCount]),
      hashCollisions(0)
{
    ArraysFill(key, keySize, INVALID_POSITION);

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

void PagesHash::SetAddressIndex(ArrayPositionLinks &positionLinks, uint32_t realPosition, int64_t hash,
    uint64_t &totalHashCollisions) const
{
    uint64_t hashCollisionsLocal = 0;
    auto pos = HashUtil::GetRawHashPosition(hash, mask);
    // look for an empty slot or a slot containing this key
    while (key[pos] != INVALID_POSITION) {
        auto currentKey = key[pos];
        if (static_cast<int8_t>(hash) == positionToHashes[currentKey] &&
            PositionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
            // found a slot for this key
            // link the new key position to the current key position
            realPosition = positionLinks.Link(realPosition, currentKey);

            // key[pos] updated outside of this loop
            break;
        }
        // increment position and mask to handler wrap around
        pos = (pos + 1) & mask;
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

uint32_t PagesHash::GetAddressIndex(uint32_t probePosition, Vector **joinColumns, int64_t rawHash) const
{
    auto pos = HashUtil::GetRawHashPosition(rawHash, mask);
    while (key[pos] != INVALID_POSITION) {
        if (PositionEqualsCurrentRowIgnoreNulls(key[pos], static_cast<int8_t>(rawHash), probePosition, joinColumns)) {
            return key[pos];
        }

        pos = (pos + 1) & mask;
    }

    return INVALID_POSITION;
}

bool PagesHash::PositionEqualsPositionIgnoreNulls(uint32_t leftPosition, uint32_t rightPosition) const
{
    auto leftAddress = addresses[leftPosition];
    auto leftTableIndex = DecodeSliceIndex(leftAddress);
    auto leftRowIndex = DecodePosition(leftAddress);

    auto rightAddress = addresses[rightPosition];
    auto rightTableIndex = DecodeSliceIndex(rightAddress);
    auto rightRowIndex = DecodePosition(rightAddress);

    return omniruntime::op::PositionEqualsPositionIgnoreNulls(leftTableIndex, leftRowIndex, rightTableIndex,
        rightRowIndex, pagesHashStrategy->GetBuildHashColumns(), pagesHashStrategy->GetBuildHashColTypes(),
        pagesHashStrategy->GetBuildHashColsCount());
}

bool PagesHash::PositionEqualsCurrentRowIgnoreNulls(uint32_t buildPosition, int8_t rawHash, uint32_t probePosition,
    Vector **joinColumns) const
{
    if (positionToHashes[buildPosition] != rawHash) {
        return false;
    }

    auto address = addresses[buildPosition];
    auto vecBatchIndex = DecodeSliceIndex(address);
    auto rowIndex = DecodePosition(address);
    return omniruntime::op::PositionEqualsRowIgnoreNulls(vecBatchIndex, rowIndex, probePosition, joinColumns,
        pagesHashStrategy->GetBuildHashColumns(), pagesHashStrategy->GetBuildHashColTypes(),
        pagesHashStrategy->GetBuildHashColsCount());
}

ArrayPositionLinks::ArrayPositionLinks(uint32_t capacity)
    : capacity(capacity), size(0), positionLinks(new uint32_t[capacity])
{
    ArraysFill(this->positionLinks, capacity, INVALID_POSITION);
}

ArrayPositionLinks::~ArrayPositionLinks()
{
    delete[] positionLinks;
    positionLinks = nullptr;
    capacity = 0;
    size = 0;
}
uint32_t ArrayPositionLinks::Link(uint32_t left, uint32_t right)
{
    size++;
    positionLinks[left] = right;
    return left;
}

uint32_t ArrayPositionLinks::Start(uint32_t position) const
{
    return position;
}

uint32_t ArrayPositionLinks::Next(uint32_t position) const
{
    return positionLinks[position];
}
} // end of op
} // end of omniruntime
