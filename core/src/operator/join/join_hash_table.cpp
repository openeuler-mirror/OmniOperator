/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: hash table implementations
 */
#include "join_hash_table.h"
#include <algorithm>
#include <memory>
#include "operator/hash_util.h"

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

void JoinHashTables::JoinFilterCodeGen(OverflowConfig *overflowConfig)
{
    if (this->filterExpr == nullptr) {
        return;
    }
    simpleFilter = new SimpleFilter(*this->filterExpr);
    auto result = simpleFilter->Initialize(overflowConfig);
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

bool JoinHashTables::IsJoinPositionEligible(uint64_t partitionedJoinPosition, uint32_t probePosition,
    BaseVector **probeColumns, uint32_t probeColsCount, ExecutionContext *executionContext) const
{
    auto partition = DecodePartition(partitionedJoinPosition);
    auto joinPosition = DecodeJoinPosition(partitionedJoinPosition);
    auto hashTable = hashTables[partition];
    auto pagesHash = hashTable->GetPagesHash();
    auto address = pagesHash->GetAddresses()[joinPosition];
    auto vecBatchIndex = DecodeSliceIndex(address);
    auto buildPosition = DecodePosition(address);

    auto pagesHashStrategy = pagesHash->GetPagesHashStrategy();
    BaseVector ***buildColumns = pagesHashStrategy->GetBuildColumns();
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
        auto vecIdx = *iter;
        auto vector = (vecIdx < originalProbeColsCount) ? probeColumns[vecIdx] :
                                                          buildColumns[vecIdx - originalProbeColsCount][vecBatchIndex];
        auto typeId = (vecIdx < originalProbeColsCount) ? probeTypes->GetType(vecIdx)->GetId() :
                      buildTypes->GetType(vecIdx - originalProbeColsCount)->GetId();
        auto position = static_cast<int32_t>((vecIdx < originalProbeColsCount) ? probePosition : buildPosition);
        nulls[vecIdx] = vector->IsNull(position);
        values[vecIdx] = OperatorUtil::GetValuePtrAndLength(vector, position, lengths + vecIdx, typeId);
    }

    return simpleFilter->Evaluate(values, nulls, lengths, reinterpret_cast<int64_t>(executionContext));
}

uint64_t JoinHashTables::GetNextJoinPosition(uint64_t currentJoinPosition) const
{
    auto partition = DecodePartition(currentJoinPosition);
    auto joinPosition = DecodeJoinPosition(currentJoinPosition);
    auto hashTable = hashTables[partition];
    auto positionLinks = hashTable->GetPositionLinks();
    auto nextJoinPosition = positionLinks->GetPositionLinks()[joinPosition];
    if (nextJoinPosition == INVALID_POSITION) {
        return INVALID_PARTITION_POSITION;
    } else {
        return EncodePartitionedJoinPosition(partition, nextJoinPosition);
    }
}

uint64_t JoinHashTables::GetJoinPosition(uint32_t position, BaseVector **joinColumns, int64_t rawHash) const
{
    auto partition = (hashTableCount != 1) ? HashUtil::GetRawHashPartition(rawHash, partitionMask) : 0U;
    auto hashTable = hashTables[partition];
    auto joinPosition = hashTable->GetPagesHash()->GetAddressIndex(position, joinColumns, rawHash);
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
void ReadColumnHashes(uint32_t offset, uint32_t addressesCount, uint64_t *addresses, BaseVector **columns,
    int64_t *hashes, bool *nullPositions)
{
    using FlatVector = Vector<T>;
    using DictionaryVector = Vector<DictionaryContainer<T>>;
    BaseVector *column = nullptr;
    int64_t hash;
    uint32_t vecBatchIndex;
    uint32_t rowIndex;
    uint32_t currVecBatchIndex = INVALID_POSITION;
    bool dictionary = false;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == OMNI_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsNull(static_cast<int32_t>(rowIndex))) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            hash = HashUtil::HashValue(static_cast<FlatVector *>(column)->GetValue(static_cast<int32_t>(rowIndex)));
        } else {
            hash =
                HashUtil::HashValue(static_cast<DictionaryVector *>(column)->GetValue(static_cast<int32_t>(rowIndex)));
        }
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnDecimal64Hashes(uint32_t offset, uint32_t addressesCount, uint64_t *addresses,
    BaseVector **columns, int64_t *hashes, bool *nullPositions)
{
    BaseVector *column = nullptr;
    int64_t hash;
    uint32_t vecBatchIndex;
    uint32_t rowIndex;
    uint32_t currVecBatchIndex = INVALID_POSITION;
    bool dictionary = false;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == OMNI_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsNull(static_cast<int32_t>(rowIndex))) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            hash = HashUtil::HashDecimal64Value(
                static_cast<Vector<int64_t> *>(column)->GetValue(static_cast<int32_t>(rowIndex)));
        } else {
            hash = HashUtil::HashDecimal64Value(
                static_cast<Vector<DictionaryContainer<int64_t>> *>(column)->GetValue(rowIndex));
        }
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnDecimal128Hashes(uint32_t offset, uint32_t addressesCount, uint64_t *addresses,
    BaseVector **columns, int64_t *hashes, bool *nullPositions)
{
    BaseVector *column = nullptr;
    int64_t hash;
    Decimal128 decimal128Value;
    uint32_t vecBatchIndex, rowIndex;
    uint32_t currVecBatchIndex = INVALID_POSITION;
    bool dictionary = false;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == OMNI_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsNull(static_cast<int32_t>(rowIndex))) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            decimal128Value = static_cast<Vector<Decimal128> *>(column)->GetValue(static_cast<int32_t>(rowIndex));
        } else {
            decimal128Value = static_cast<Vector<DictionaryContainer<Decimal128>> *>(column)->GetValue(rowIndex);
        }
        hash = HashUtil::HashValue(static_cast<int64_t>(decimal128Value.LowBits()), decimal128Value.HighBits());
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnCharHashes(uint32_t offset, uint32_t addressesCount, uint64_t *addresses, BaseVector **columns,
    int64_t *hashes, bool *nullPositions)
{
    BaseVector *column = nullptr;
    int64_t hash;
    uint32_t vecBatchIndex;
    uint32_t rowIndex;
    uint32_t currVecBatchIndex = INVALID_POSITION;
    bool dictionary = false;
    std::string_view varcharValue;
    for (uint32_t step = 0; offset + step < addressesCount && step < BLOCK_SIZE; step++) {
        auto address = addresses[offset + step];
        vecBatchIndex = DecodeSliceIndex(address);
        rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == OMNI_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsNull(static_cast<int32_t>(rowIndex))) {
            nullPositions[step] = true;
            continue;
        }
        if (!dictionary) {
            varcharValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(column)->GetValue(
                static_cast<int32_t>(rowIndex));
        } else {
            varcharValue =
                static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(column)->GetValue(
                rowIndex);
        }
        hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(varcharValue.data())),
            varcharValue.length());
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

template <DataTypeId typeId>
void ReadOneColumnHash(uint32_t offset, uint32_t addressesCount, uint64_t *addresses, BaseVector **columns,
    int64_t *hashes, bool *nullPositions)
{
    using T = typename NativeType<typeId>::type;
    if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
        ReadColumnCharHashes(offset, addressesCount, addresses, columns, hashes, nullPositions);
    } else if constexpr (std::is_same_v<T, Decimal128>) {
        ReadColumnDecimal128Hashes(offset, addressesCount, addresses, columns, hashes, nullPositions);
    } else if constexpr (typeId == OMNI_DECIMAL64) {
        ReadColumnDecimal64Hashes(offset, addressesCount, addresses, columns, hashes, nullPositions);
    } else {
        ReadColumnHashes<T>(offset, addressesCount, addresses, columns, hashes, nullPositions);
    }
}

static void ProcessColumns(uint32_t offset, uint32_t addressesCount, uint64_t *addresses, BaseVector ***columns,
    int32_t *types, uint32_t colCount, int64_t *hashes, bool *nullPositions)
{
    for (uint32_t columnIdx = 0; columnIdx < colCount; ++columnIdx) {
        DYNAMIC_TYPE_DISPATCH(ReadOneColumnHash, types[columnIdx], offset, addressesCount, addresses,
            columns[columnIdx], hashes, nullPositions);
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
    auto links = positionLinks.GetPositionLinks();

    uint64_t hashCollisionsLocal = 0;
    auto pos = HashUtil::GetRawHashPosition(hash, mask);
    auto currentKey = key[pos];
    // look for an empty slot or a slot containing this key
    while (currentKey != INVALID_POSITION) {
        if (static_cast<int8_t>(hash) == positionToHashes[currentKey]) {
            auto leftAddress = addresses[currentKey];
            auto leftTableIndex = DecodeSliceIndex(leftAddress);
            auto leftRowIndex = DecodePosition(leftAddress);
            auto rightAddress = addresses[realPosition];
            auto rightTableIndex = DecodeSliceIndex(rightAddress);
            auto rightRowIndex = DecodePosition(rightAddress);
            auto result = pagesHashStrategy->PositionEqualsPositionIgnoreNulls(leftTableIndex, leftRowIndex,
                rightTableIndex, rightRowIndex);
            if (result) {
                // found a slot for this key
                // link the new key position to the current key position
                links[realPosition] = currentKey;
                // key[pos] updated outside of this loop
                break;
            }
        }
        // increment position and mask to handler wrap around
        pos = (pos + 1) & mask;
        currentKey = key[pos];
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

uint32_t PagesHash::GetAddressIndex(uint32_t probePosition, BaseVector **joinColumns, int64_t rawHash) const
{
    auto pos = HashUtil::GetRawHashPosition(rawHash, mask);
    auto buildPosition = key[pos];
    while (buildPosition != INVALID_POSITION) {
        if (positionToHashes[buildPosition] != static_cast<int8_t>(rawHash)) {
            pos = (pos + 1) & mask;
            buildPosition = key[pos];
            continue;
        }

        auto address = addresses[buildPosition];
        auto vecBatchIndex = DecodeSliceIndex(address);
        auto rowIndex = DecodePosition(address);
        auto result =
            pagesHashStrategy->PositionEqualsRowIgnoreNulls(vecBatchIndex, rowIndex, probePosition, joinColumns);
        if (result) {
            return buildPosition;
        }
        pos = (pos + 1) & mask;
        buildPosition = key[pos];
    }

    return INVALID_POSITION;
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
} // end of op
} // end of omniruntime
