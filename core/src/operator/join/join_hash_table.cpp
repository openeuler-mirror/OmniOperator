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

    auto usedColumns = simpleFilter->GetVectorIndexes();
    for (auto col : usedColumns) {
        if (col < originalProbeColsCount) {
            probeFilterCols.emplace_back(col);
        } else {
            buildFilterCols.emplace_back(col);
        }
    }
}

JoinHashTable::JoinHashTable(PagesHashStrategy *pagesHashStrategy, uint64_t *addresses, uint32_t addressesCount)
    : positionLinks(new ArrayPositionLinks(addressesCount)),
      pagesHash(new PagesHash(addresses, addressesCount, pagesHashStrategy, positionLinks)),
      visitedPositionsSize(addressesCount),
      buildValueAddresses(addresses),
      buildColumns(pagesHashStrategy->GetBuildColumns())
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
ALWAYS_INLINE void ReadColumnHashes(uint32_t maxStep, uint64_t *addresses, BaseVector **columns, int64_t *hashes,
    int64_t *rawHashPositions)
{
    using FlatVector = Vector<T>;
    using DictionaryVector = Vector<DictionaryContainer<T>>;
    BaseVector *column = nullptr;
    int64_t hash;
    uint32_t currVecBatchIndex = INVALID_POSITION;
    bool dictionary = false;
    for (uint32_t step = 0; step < maxStep; step++) {
        auto address = addresses[step];
        auto vecBatchIndex = DecodeSliceIndex(address);
        auto rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == OMNI_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsNull(static_cast<int32_t>(rowIndex))) {
            rawHashPositions[step] = -1;
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

static void ReadColumnDecimal64Hashes(uint32_t maxStep, uint64_t *addresses, BaseVector **columns, int64_t *hashes,
    int64_t *rawHashPositions)
{
    BaseVector *column = nullptr;
    int64_t hash;
    uint32_t currVecBatchIndex = INVALID_POSITION;
    bool dictionary = false;
    for (uint32_t step = 0; step < maxStep; step++) {
        auto address = addresses[step];
        auto vecBatchIndex = DecodeSliceIndex(address);
        auto rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == OMNI_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsNull(static_cast<int32_t>(rowIndex))) {
            rawHashPositions[step] = -1;
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

static void ReadColumnDecimal128Hashes(uint32_t maxStep, uint64_t *addresses, BaseVector **columns, int64_t *hashes,
    int64_t *rawHashPositions)
{
    BaseVector *column = nullptr;
    Decimal128 decimal128Value;
    uint32_t currVecBatchIndex = INVALID_POSITION;
    bool dictionary = false;
    for (uint32_t step = 0; step < maxStep; step++) {
        auto address = addresses[step];
        auto vecBatchIndex = DecodeSliceIndex(address);
        auto rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == OMNI_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsNull(static_cast<int32_t>(rowIndex))) {
            rawHashPositions[step] = -1;
            continue;
        }
        if (!dictionary) {
            decimal128Value = static_cast<Vector<Decimal128> *>(column)->GetValue(static_cast<int32_t>(rowIndex));
        } else {
            decimal128Value = static_cast<Vector<DictionaryContainer<Decimal128>> *>(column)->GetValue(rowIndex);
        }
        auto hash = HashUtil::HashValue(static_cast<int64_t>(decimal128Value.LowBits()), decimal128Value.HighBits());
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static void ReadColumnCharHashes(uint32_t maxStep, uint64_t *addresses, BaseVector **columns, int64_t *hashes,
    int64_t *rawHashPositions)
{
    BaseVector *column = nullptr;
    std::string_view varcharValue;
    uint32_t currVecBatchIndex = INVALID_POSITION;
    bool dictionary = false;
    for (uint32_t step = 0; step < maxStep; step++) {
        auto address = addresses[step];
        auto vecBatchIndex = DecodeSliceIndex(address);
        auto rowIndex = DecodePosition(address);
        if (currVecBatchIndex != vecBatchIndex) {
            column = columns[vecBatchIndex];
            dictionary = false;
            if (column->GetEncoding() == OMNI_DICTIONARY) {
                dictionary = true;
            }
            currVecBatchIndex = vecBatchIndex;
        }
        if (column->IsNull(static_cast<int32_t>(rowIndex))) {
            rawHashPositions[step] = -1;
            continue;
        }
        if (!dictionary) {
            varcharValue = static_cast<Vector<LargeStringContainer<std::string_view>> *>(column)->GetValue(
                static_cast<int32_t>(rowIndex));
        } else {
            varcharValue = static_cast<Vector<DictionaryContainer<std::string_view>> *>(column)->GetValue(rowIndex);
        }
        auto hash = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(varcharValue.data())),
            varcharValue.length());
        hashes[step] = HashUtil::CombineHash(hashes[step], hash);
    }
}

static NO_INLINE void ProcessColumns(uint32_t maxStep, uint64_t *addresses, BaseVector ***columns, int32_t *types,
    uint32_t colCount, int64_t *hashes, int64_t *rawHashPositions)
{
    for (uint32_t columnIdx = 0; columnIdx < colCount; ++columnIdx) {
        switch (types[columnIdx]) {
            case omniruntime::type::OMNI_INT:
            case omniruntime::type::OMNI_DATE32: {
                ReadColumnHashes<int32_t>(maxStep, addresses, columns[columnIdx], hashes, rawHashPositions);
                break;
            }
            case omniruntime::type::OMNI_SHORT: {
                ReadColumnHashes<int16_t>(maxStep, addresses, columns[columnIdx], hashes, rawHashPositions);
                break;
            }
            case omniruntime::type::OMNI_LONG: {
                ReadColumnHashes<int64_t>(maxStep, addresses, columns[columnIdx], hashes, rawHashPositions);
                break;
            }
            case omniruntime::type::OMNI_DOUBLE: {
                ReadColumnHashes<double>(maxStep, addresses, columns[columnIdx], hashes, rawHashPositions);
                break;
            }
            case omniruntime::type::OMNI_BOOLEAN: {
                ReadColumnHashes<bool>(maxStep, addresses, columns[columnIdx], hashes, rawHashPositions);
                break;
            }
            case omniruntime::type::OMNI_DECIMAL64: {
                ReadColumnDecimal64Hashes(maxStep, addresses, columns[columnIdx], hashes, rawHashPositions);
                break;
            }
            case omniruntime::type::OMNI_DECIMAL128: {
                ReadColumnDecimal128Hashes(maxStep, addresses, columns[columnIdx], hashes, rawHashPositions);
                break;
            }
            case omniruntime::type::OMNI_VARCHAR:
            case omniruntime::type::OMNI_CHAR: {
                ReadColumnCharHashes(maxStep, addresses, columns[columnIdx], hashes, rawHashPositions);
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

    auto hashColumns = pagesHashStrategy->GetBuildHashColumns();
    auto hashColTypes = pagesHashStrategy->GetBuildHashColTypes();
    auto hashColCount = pagesHashStrategy->GetBuildHashColsCount();
    auto links = positionLinks->GetPositionLinks();
    int64_t hashes[BLOCK_SIZE];
    int64_t rawHashPositions[BLOCK_SIZE]; // rawHashPositions == -1: row has null column
    for (uint32_t offset = 0; offset < addressesCount; offset += BLOCK_SIZE) {
        uint32_t maxStep = std::min(addressesCount - offset, BLOCK_SIZE);
        for (uint32_t step = 0; step < maxStep; step++) {
            hashes[step] = 0;
            rawHashPositions[step] = 0;
        }

        ProcessColumns(maxStep, addresses + offset, hashColumns, hashColTypes, hashColCount, hashes, rawHashPositions);
        auto positionToHashesBegin = positionToHashes + offset;
        for (uint32_t step = 0; step < maxStep; ++step) {
            int64_t h = hashes[step];
            positionToHashesBegin[step] = static_cast<int8_t>(h);
            if (rawHashPositions[step] != -1) {
                rawHashPositions[step] = HashUtil::GetRawHashPosition(h, mask);
            }
        }

        for (uint32_t step = 0; step < maxStep; ++step) {
            int64_t pos = rawHashPositions[step];
            if (pos != -1) {
                SetAddressIndex(links, offset + step, hashes[step], pos, hashColumns, hashColTypes, hashColCount);
            }
        }
    }
}

PagesHash::~PagesHash()
{
    delete[] key;
    delete[] positionToHashes;
    delete pagesHashStrategy;
}

void ALWAYS_INLINE PagesHash::SetAddressIndex(uint32_t *links, uint32_t realPosition, int64_t hash, uint32_t pos,
    BaseVector ***hashColumns, int32_t *hashColTypes, uint32_t hashColCount) const
{
    // look for an empty slot or a slot containing this key
    uint32_t currentKey = key[pos];
    while (currentKey != INVALID_POSITION) {
        if (static_cast<int8_t>(hash) == positionToHashes[currentKey] &&
            PositionEqualsPositionIgnoreNulls(currentKey, realPosition, hashColumns, hashColTypes, hashColCount)) {
            // found a slot for this key
            // link the new key position to the current key position
            links[realPosition] = currentKey;

            // key[pos] updated outside of this loop
            break;
        }
        // increment position and mask to handler wrap around
        pos = (pos + 1) & mask;
        currentKey = key[pos];
    }
    key[pos] = realPosition;
}

NO_INLINE bool PagesHash::PositionEqualsPositionIgnoreNulls(uint32_t leftPosition, uint32_t rightPosition,
    BaseVector ***hashColumns, int32_t *hashColTypes, uint32_t hashColCount) const
{
    auto leftAddress = addresses[leftPosition];
    auto leftTableIndex = DecodeSliceIndex(leftAddress);
    auto leftRowIndex = DecodePosition(leftAddress);

    auto rightAddress = addresses[rightPosition];
    auto rightTableIndex = DecodeSliceIndex(rightAddress);
    auto rightRowIndex = DecodePosition(rightAddress);

    for (uint32_t columnIdx = 0; columnIdx < hashColCount; columnIdx++) {
        auto buildHashVecBatches = hashColumns[columnIdx];
        auto leftColumn = buildHashVecBatches[leftTableIndex];
        auto rightColumn = buildHashVecBatches[rightTableIndex];
        if (!ValueEqualsValueIgnoreNulls(hashColTypes[columnIdx], leftColumn, leftRowIndex, rightColumn,
            rightRowIndex)) {
            return false;
        }
    }
    return true;
}
} // end of op
} // end of omniruntime
