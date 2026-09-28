
#include <algorithm>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#if defined(__aarch64__) || defined(__ARM_NEON)
#include <arm_neon.h>
#endif

#include "operator/join/cascading_lookup_join.h"
#include "operator/util/operator_util.h"
#include "vector/vector_helper.h"
#include "operator/join/join_hash_table_variants.h"

using namespace omniruntime::vec;
namespace omniruntime::op {

namespace {

template <typename T>
struct IsJoinHashTableVariants : std::false_type {
};

template <typename KeyType, typename RowRefType>
struct IsJoinHashTableVariants<
    JoinHashTableVariants<KeyType, RowRefType>> : std::true_type {
};

#if defined(__aarch64__) || defined(__ARM_NEON)
// -----------------------------------------------------------------------------
// Cascading ArrayTable/NEON helpers shared by level 0 and frontier levels.
//
// These stay local to this translation unit. CascadingLookupJoin cannot call
// LookupJoinOperator::ArrayJoinProbeSIMDNeon() directly because LookupJoin owns
// different output semantics. We reuse the same core idea instead:
//
//   key range check -> slot = key - minValue -> isAssigned[slot]
//   -> slots[slot] -> RowRefList
//
// Level 0 can consume a contiguous root probe vector directly. Level > 0 first
// tries the same direct contiguous block when frontier root positions preserve
// row order; otherwise it gathers the referenced root keys into a small aligned
// buffer and runs the same ArrayTable/NEON probe.
// -----------------------------------------------------------------------------
inline void CascadingInt16ArrayCompareNeon(
    int16_t *originKey,
    int16_t *keyResult,
    int16_t *cmpResult,
    int16x8_t vMin,
    int16x8_t vMax,
    int16_t minValue,
    int16_t maxValue)
{
    int16x8x3_t vCmp;
    int16x8x3_t vecKey;
    int16x8x3_t vIndex;

    vecKey = vld3q_s16(originKey + 8);

    keyResult[0] = originKey[0] - minValue;
    keyResult[1] = originKey[1] - minValue;
    vIndex.val[0] = vsubq_s16(vecKey.val[0], vMin);
    keyResult[2] = originKey[2] - minValue;
    keyResult[3] = originKey[3] - minValue;
    keyResult[4] = originKey[4] - minValue;
    keyResult[5] = originKey[5] - minValue;
    vIndex.val[1] = vsubq_s16(vecKey.val[1], vMin);
    keyResult[6] = originKey[6] - minValue;
    keyResult[7] = originKey[7] - minValue;
    vIndex.val[2] = vsubq_s16(vecKey.val[2], vMin);
    vst3q_s16(keyResult + 8, vIndex);

    cmpResult[0] = maxValue >= originKey[0] && originKey[0] >= minValue;
    cmpResult[1] = maxValue >= originKey[1] && originKey[1] >= minValue;
    cmpResult[2] = maxValue >= originKey[2] && originKey[2] >= minValue;
    vCmp.val[0] = (int16x8_t)vcgeq_s16(vecKey.val[0], vMin);
    vCmp.val[0] = vandq_s16(
        vCmp.val[0],
        (int16x8_t)vcleq_s16(vecKey.val[0], vMax));
    cmpResult[3] = maxValue >= originKey[3] && originKey[3] >= minValue;
    cmpResult[4] = maxValue >= originKey[4] && originKey[4] >= minValue;
    cmpResult[5] = maxValue >= originKey[5] && originKey[5] >= minValue;
    vCmp.val[1] = (int16x8_t)vcgeq_s16(vecKey.val[1], vMin);
    vCmp.val[1] = vandq_s16(
        vCmp.val[1],
        (int16x8_t)vcleq_s16(vecKey.val[1], vMax));
    cmpResult[6] = maxValue >= originKey[6] && originKey[6] >= minValue;
    cmpResult[7] = maxValue >= originKey[7] && originKey[7] >= minValue;
    vCmp.val[2] = (int16x8_t)vcgeq_s16(vecKey.val[2], vMin);
    vCmp.val[2] = vandq_s16(
        vCmp.val[2],
        (int16x8_t)vcleq_s16(vecKey.val[2], vMax));

    vst3q_s16(cmpResult + 8, vCmp);
}

inline void CascadingInt32ArrayCompareNeon(
    int32_t *originKey,
    int32_t *keyResult,
    int32_t *cmpResult,
    int32x4_t vMin,
    int32x4_t vMax,
    int32_t minValue,
    int32_t maxValue)
{
    int32x4x3_t vCmp;
    int32x4x3_t vecKey;
    int32x4x3_t vIndex;

    vecKey = vld3q_s32(originKey + 4);

    keyResult[0] = originKey[0] - minValue;
    vIndex.val[0] = vsubq_s32(vecKey.val[0], vMin);
    keyResult[1] = originKey[1] - minValue;
    vIndex.val[1] = vsubq_s32(vecKey.val[1], vMin);
    keyResult[2] = originKey[2] - minValue;
    vIndex.val[2] = vsubq_s32(vecKey.val[2], vMin);
    keyResult[3] = originKey[3] - minValue;
    vst3q_s32(keyResult + 4, vIndex);

    cmpResult[0] = maxValue >= originKey[0] && originKey[0] >= minValue;
    vCmp.val[0] = (int32x4_t)vcgeq_s32(vecKey.val[0], vMin);
    vCmp.val[0] = vandq_s32(
        vCmp.val[0],
        (int32x4_t)vcleq_s32(vecKey.val[0], vMax));
    cmpResult[1] = maxValue >= originKey[1] && originKey[1] >= minValue;
    vCmp.val[1] = (int32x4_t)vcgeq_s32(vecKey.val[1], vMin);
    vCmp.val[1] = vandq_s32(
        vCmp.val[1],
        (int32x4_t)vcleq_s32(vecKey.val[1], vMax));
    cmpResult[2] = maxValue >= originKey[2] && originKey[2] >= minValue;
    vCmp.val[2] = (int32x4_t)vcgeq_s32(vecKey.val[2], vMin);
    vCmp.val[2] = vandq_s32(
        vCmp.val[2],
        (int32x4_t)vcleq_s32(vecKey.val[2], vMax));
    cmpResult[3] = maxValue >= originKey[3] && originKey[3] >= minValue;

    vst3q_s32(cmpResult + 4, vCmp);
}

inline void CascadingInt64ArrayCompareNeon(
    int64_t *originKey,
    int64_t *keyResult,
    int64_t *cmpResult,
    int64x2_t vMin,
    int64x2_t vMax,
    int64_t minValue,
    int64_t maxValue)
{
    int64x2x3_t vCmp;
    int64x2x3_t vecKey;
    int64x2x3_t vHashes;

    vecKey = vld3q_s64(originKey + 2);

    keyResult[0] = originKey[0] - minValue;
    vHashes.val[0] = vsubq_s64(vecKey.val[0], vMin);
    vHashes.val[1] = vsubq_s64(vecKey.val[1], vMin);
    keyResult[1] = originKey[1] - minValue;
    vHashes.val[2] = vsubq_s64(vecKey.val[2], vMin);
    vst3q_s64(keyResult + 2, vHashes);

    cmpResult[0] = maxValue >= originKey[0] && originKey[0] >= minValue;
    vCmp.val[0] = (int64x2_t)vcgeq_s64(vecKey.val[0], vMin);
    vCmp.val[0] = vandq_s64(
        vCmp.val[0],
        (int64x2_t)vcleq_s64(vecKey.val[0], vMax));
    vCmp.val[1] = (int64x2_t)vcgeq_s64(vecKey.val[1], vMin);
    vCmp.val[1] = vandq_s64(
        vCmp.val[1],
        (int64x2_t)vcleq_s64(vecKey.val[1], vMax));
    cmpResult[1] = maxValue >= originKey[1] && originKey[1] >= minValue;
    vCmp.val[2] = (int64x2_t)vcgeq_s64(vecKey.val[2], vMin);
    vCmp.val[2] = vandq_s64(
        vCmp.val[2],
        (int64x2_t)vcleq_s64(vecKey.val[2], vMax));

    vst3q_s64(cmpResult + 2, vCmp);
}
#endif


template <typename T>
BaseVector *MaterializeCascadingProbeColumn(
    BaseVector *srcCol,
    const int32_t *positions,
    int32_t numRows)
{
    if (srcCol == nullptr || positions == nullptr || numRows <= 0) {
        return nullptr;
    }

    const auto encoding = srcCol->GetEncoding();
    const int32_t vectorSize = srcCol->GetSize();
    auto *ret = new Vector<T>(numRows);

    for (int32_t i = 0; i < numRows; ++i) {
        const int32_t sourceRow = positions[i];

        if (sourceRow < 0 ||
            (encoding != OMNI_ENCODING_CONST && sourceRow >= vectorSize)) {
            delete ret;
            return nullptr;
        }

        const int32_t nullRow =
            encoding == OMNI_ENCODING_CONST ? 0 : sourceRow;

        if (srcCol->IsNull(nullRow)) {
            ret->SetNull(i);
            continue;
        }

        T value;
        if (encoding == OMNI_ENCODING_CONST) {
            value =
                static_cast<ConstVector<T> *>(srcCol)
                    ->GetConstValue();
        } else if (encoding == OMNI_DICTIONARY) {
            value =
                static_cast<Vector<DictionaryContainer<T>> *>(srcCol)
                    ->GetValue(sourceRow);
        } else if (encoding == OMNI_FLAT) {
            value =
                static_cast<Vector<T> *>(srcCol)
                    ->GetValue(sourceRow);
        } else {
            delete ret;
            return nullptr;
        }

        ret->SetValue(i, value);
    }

    return ret;
}

BaseVector *MaterializeCascadingProbeVarcharColumn(
    BaseVector *srcCol,
    const int32_t *positions,
    int32_t numRows)
{
    if (srcCol == nullptr || positions == nullptr || numRows <= 0) {
        return nullptr;
    }

    using VarcharVector =
        Vector<LargeStringContainer<std::string_view>>;
    using DictionaryVector =
        Vector<DictionaryContainer<std::string_view>>;

    const auto encoding = srcCol->GetEncoding();
    const int32_t vectorSize = srcCol->GetSize();
    auto *ret = new VarcharVector(numRows);

    for (int32_t i = 0; i < numRows; ++i) {
        const int32_t sourceRow = positions[i];

        if (sourceRow < 0 ||
            (encoding != OMNI_ENCODING_CONST && sourceRow >= vectorSize)) {
            delete ret;
            return nullptr;
        }

        const int32_t nullRow =
            encoding == OMNI_ENCODING_CONST ? 0 : sourceRow;

        if (srcCol->IsNull(nullRow)) {
            ret->SetNull(i);
            continue;
        }

        std::string_view value;
        if (encoding == OMNI_ENCODING_CONST) {
            value =
                static_cast<ConstVector<std::string_view> *>(srcCol)
                    ->GetConstValue();
        } else if (encoding == OMNI_DICTIONARY) {
            value =
                static_cast<DictionaryVector *>(srcCol)
                    ->GetValue(sourceRow);
        } else if (encoding == OMNI_FLAT) {
            value =
                static_cast<VarcharVector *>(srcCol)
                    ->GetValue(sourceRow);
        } else {
            delete ret;
            return nullptr;
        }

        ret->SetValue(i, value);
    }

    return ret;
}

template <typename Visitor>
decltype(auto) VisitHashTableVariant(
    HashTableVariants *variants,
    int32_t level,
    const char *site,
    Visitor&& visitor)
{
    if (variants == nullptr) {

        throw OmniException(
            "INVALID_ARGUMENT",
            "Cascading HashTableVariants pointer is null");
    }

    if (variants->valueless_by_exception()) {

        throw OmniException(
            "INVALID_STATE",
            "Cascading HashTableVariants is valueless");
    }

    try {
        return std::visit(
            std::forward<Visitor>(visitor),
            *variants);
    } catch (const std::bad_variant_access &) {

        throw OmniException(
            "INVALID_STATE",
            "Cascading HashTableVariants became valueless during std::visit");
    }
}
} // namespace

CascadingJoinOutputBuilder::CascadingJoinOutputBuilder(
    int32_t maxRowCount,
    int32_t cascadeLevels)
    : maxRowCount(maxRowCount),
      cascadeLevels(cascadeLevels),
      buildRowPositions(static_cast<size_t>(cascadeLevels)),
      buildBatchIndices(static_cast<size_t>(cascadeLevels)),
      firstBatchIndices(static_cast<size_t>(cascadeLevels), 0),
      singleBatchFlags(static_cast<size_t>(cascadeLevels), true)
{
    probePositions.reserve(static_cast<size_t>(maxRowCount));
    for (int32_t level = 0; level < cascadeLevels; ++level) {
        buildRowPositions[static_cast<size_t>(level)].reserve(
            static_cast<size_t>(maxRowCount));
        buildBatchIndices[static_cast<size_t>(level)].reserve(
            static_cast<size_t>(maxRowCount));
    }
}

void CascadingJoinOutputBuilder::AppendBuildAddress(
    int32_t level,
    const CascadingBuildAddress & address)
{
    const size_t levelIndex = static_cast<size_t>(level);
    auto & rows = buildRowPositions[levelIndex];
    auto & batches = buildBatchIndices[levelIndex];

    if (rows.empty()) {
        firstBatchIndices[levelIndex] = address.vecBatchIdx;
    } else if (singleBatchFlags[levelIndex] &&
               firstBatchIndices[levelIndex] != address.vecBatchIdx) {
        singleBatchFlags[levelIndex] = false;
    }

    rows.push_back(static_cast<int32_t>(address.rowIdx));
    batches.push_back(address.vecBatchIdx);
}

void CascadingJoinOutputBuilder::AppendMatchChain(
    int32_t probePosition,
    const std::vector<CascadingBuildAddress> & addresses)
{
    if (IsFull()) {
        return;
    }

    // A final match must contain exactly one build address for every level.
    if (static_cast<int32_t>(addresses.size()) != cascadeLevels) {
        return;
    }

    probePositions.push_back(probePosition);
    for (int32_t level = 0; level < cascadeLevels; ++level) {
        AppendBuildAddress(level, addresses[static_cast<size_t>(level)]);
    }
}

void CascadingJoinOutputBuilder::BuildFinalOutput(
    BaseVector ** probeColumns,
    std::vector<std::vector<BaseVector ***>> & allBuildColumns,
    std::vector<int32_t> & probeOutputCols,
    std::vector<std::vector<int32_t>> & buildOutputColsList,
    VectorBatch ** outputVecBatch)
{
    *outputVecBatch = nullptr;

    if (probePositions.empty()) {
        return;
    }

    if (probeColumns == nullptr && !probeOutputCols.empty()) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading probe output columns are unavailable");
    }

    if (allBuildColumns.size() < static_cast<size_t>(cascadeLevels) ||
        buildOutputTypesList.size() < static_cast<size_t>(cascadeLevels) ||
        buildOutputColsList.size() < static_cast<size_t>(cascadeLevels)) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading output metadata level count mismatch");
    }

    int32_t expectedOutputColumnCount =
        static_cast<int32_t>(probeOutputCols.size());

    for (int32_t level = 0; level < cascadeLevels; ++level) {
        expectedOutputColumnCount +=
            static_cast<int32_t>(
                buildOutputColsList[static_cast<size_t>(level)].size());
    }

    const int32_t outputRowCount =
        static_cast<int32_t>(probePositions.size());

    *outputVecBatch =
        new VectorBatch(outputRowCount);

    auto failOutput =
        [&](const char *message) {
            if (*outputVecBatch != nullptr) {
                VectorHelper::FreeVecBatch(*outputVecBatch);
                *outputVecBatch = nullptr;
            }

            throw OmniException(
                "INVALID_STATE",
                message);
        };

    for (int32_t col : probeOutputCols) {
        if (col < 0) {
            failOutput(
                "Cascading probe output column is invalid");
        }

        BaseVector *srcCol =
            probeColumns[col];

        if (srcCol == nullptr) {
            failOutput(
                "Cascading probe output vector is null");
        }

        BaseVector *outputCol = nullptr;
        const int32_t dataTypeId =
            static_cast<int32_t>(srcCol->GetTypeId());

        switch (dataTypeId) {
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                outputCol = MaterializeCascadingProbeColumn<int64_t>(
                    srcCol,
                    probePositions.data(),
                    outputRowCount);
                break;

            case OMNI_INT:
            case OMNI_DATE32:
                outputCol = MaterializeCascadingProbeColumn<int32_t>(
                    srcCol,
                    probePositions.data(),
                    outputRowCount);
                break;

            case OMNI_SHORT:
                outputCol = MaterializeCascadingProbeColumn<int16_t>(
                    srcCol,
                    probePositions.data(),
                    outputRowCount);
                break;

            case OMNI_BYTE:
                outputCol = MaterializeCascadingProbeColumn<int8_t>(
                    srcCol,
                    probePositions.data(),
                    outputRowCount);
                break;

            case OMNI_DOUBLE:
                outputCol = MaterializeCascadingProbeColumn<double>(
                    srcCol,
                    probePositions.data(),
                    outputRowCount);
                break;

            case OMNI_FLOAT:
                outputCol = MaterializeCascadingProbeColumn<float>(
                    srcCol,
                    probePositions.data(),
                    outputRowCount);
                break;

            case OMNI_BOOLEAN:
                outputCol = MaterializeCascadingProbeColumn<bool>(
                    srcCol,
                    probePositions.data(),
                    outputRowCount);
                break;

            case OMNI_DECIMAL128:
                outputCol =
                    MaterializeCascadingProbeColumn<type::Decimal128>(
                        srcCol,
                        probePositions.data(),
                        outputRowCount);
                break;

            case OMNI_VARBINARY:
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                outputCol = MaterializeCascadingProbeVarcharColumn(
                    srcCol,
                    probePositions.data(),
                    outputRowCount);
                break;

            default:
                outputCol = srcCol->CopyPositions(
                    probePositions.data(),
                    0,
                    outputRowCount);
                break;
        }

        if (outputCol == nullptr) {
            failOutput(
                "Failed to materialize Cascading probe output column");
        }

        (*outputVecBatch)->Append(outputCol);
    }

    for (int32_t level = 0; level < cascadeLevels; ++level) {
        const size_t levelIndex =
            static_cast<size_t>(level);

        const auto & buildCols =
            buildOutputColsList[levelIndex];

        if (buildCols.empty()) {
            continue;
        }

        auto & levelColumns =
            allBuildColumns[levelIndex];
        auto & buildOutputTypes =
            buildOutputTypesList[levelIndex];

        if (levelColumns.empty()) {
            failOutput(
                "Cascading build output columns are unavailable");
        }

        const int32_t buildTypeCount =
            buildOutputTypes.GetSize();
        const auto buildTypeIds =
            buildOutputTypes.GetIds();

        for (int32_t outputCol : buildCols) {
            if (outputCol < 0 ||
                outputCol >= buildTypeCount) {
                failOutput(
                    "Cascading build output column is outside build schema");
            }

            BaseVector *buildColumn = nullptr;
            const int32_t dataTypeId =
                buildTypeIds[outputCol];

            switch (dataTypeId) {
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64:
                    buildColumn = ConstructBuildColumn<int64_t>(
                        levelColumns,
                        outputCol,
                        level,
                        outputRowCount);
                    break;

                case OMNI_INT:
                case OMNI_DATE32:
                    buildColumn = ConstructBuildColumn<int32_t>(
                        levelColumns,
                        outputCol,
                        level,
                        outputRowCount);
                    break;

                case OMNI_SHORT:
                    buildColumn = ConstructBuildColumn<int16_t>(
                        levelColumns,
                        outputCol,
                        level,
                        outputRowCount);
                    break;

                case OMNI_BYTE:
                    buildColumn = ConstructBuildColumn<int8_t>(
                        levelColumns,
                        outputCol,
                        level,
                        outputRowCount);
                    break;

                case OMNI_DOUBLE:
                    buildColumn = ConstructBuildColumn<double>(
                        levelColumns,
                        outputCol,
                        level,
                        outputRowCount);
                    break;

                case OMNI_FLOAT:
                    buildColumn = ConstructBuildColumn<float>(
                        levelColumns,
                        outputCol,
                        level,
                        outputRowCount);
                    break;

                case OMNI_BOOLEAN:
                    buildColumn = ConstructBuildColumn<bool>(
                        levelColumns,
                        outputCol,
                        level,
                        outputRowCount);
                    break;

                case OMNI_DECIMAL128:
                    buildColumn = ConstructBuildColumn<type::Decimal128>(
                        levelColumns,
                        outputCol,
                        level,
                        outputRowCount);
                    break;

                case OMNI_VARBINARY:
                case OMNI_VARCHAR:
                case OMNI_CHAR:
                    buildColumn = ConstructBuildVarcharColumn(
                        levelColumns,
                        outputCol,
                        level,
                        outputRowCount);
                    break;

                default:
                    failOutput(
                        "Cascading build output type is not supported");
            }

            if (buildColumn == nullptr) {
                failOutput(
                    "Failed to materialize Cascading build output column");
            }

            (*outputVecBatch)->Append(buildColumn);
        }
    }

    if ((*outputVecBatch)->GetVectorCount() !=
        expectedOutputColumnCount) {
        failOutput(
            "Cascading physical output column count mismatch");
    }
}


template <typename T>
BaseVector *CascadingJoinOutputBuilder::ConstructBuildColumn(
    std::vector<BaseVector ***> & levelColumns,
    int32_t outputCol,
    int32_t level,
    int32_t numRows)
{
    // Current cascading probe implementation supports the single-hash-table
    // layout and always obtains next-level vectors from partition 0.
    if (levelColumns.size() != 1 || levelColumns[0] == nullptr || outputCol < 0) {
        return nullptr;
    }

    BaseVector *** partitionColumns = levelColumns[0];
    BaseVector ** columnBatches = partitionColumns[outputCol];
    if (columnBatches == nullptr) {
        return nullptr;
    }

    // High-locality fast path.  Whether every output row refers to the same
    // build VectorBatch is maintained incrementally in AppendBuildAddress(),
    // so there is no O(numRows) batch-id pre-scan here.  The row positions are
    // already stored contiguously per level and can be passed directly.
    const size_t levelIndex = static_cast<size_t>(level);
    if (numRows > 0 && singleBatchFlags[levelIndex]) {
        const uint32_t firstVecBatchIdx = firstBatchIndices[levelIndex];
        BaseVector *srcCol = columnBatches[firstVecBatchIdx];
        if (srcCol != nullptr) {
            int32_t *positions = buildRowPositions[levelIndex].data();
            const auto encoding = srcCol->GetEncoding();
            if (encoding == vec::OMNI_DICTIONARY) {
                return VectorHelper::CopyPositionsVector(
                    srcCol, positions, 0, numRows);
            }

            // FLAT/CONST are materialized by the generic row-copy path below.
            // In this Omni branch BaseVector::CopyPositions() may return
            // nullptr for these scalar encodings.
            if (encoding != vec::OMNI_FLAT &&
                encoding != vec::OMNI_ENCODING_CONST) {
                BaseVector *copied =
                    srcCol->CopyPositions(
                        positions, 0, numRows);
                if (copied != nullptr) {
                    return copied;
                }
            }
        }
    }

    auto *ret = new Vector<T>(numRows);

    for (int32_t i = 0; i < numRows; ++i) {
        const CascadingBuildAddress address = GetBuildAddress(i, level);
        const uint32_t buildRowIdx = address.rowIdx;
        const uint32_t vecBatchIdx = address.vecBatchIdx;

        // Correct build-side pointer order:
        //   levelColumns[partition][column][vecBatch]
        BaseVector *buildVector = columnBatches[vecBatchIdx];
        if (buildVector == nullptr) {
            ret->SetNull(i);
            continue;
        }

        const auto encoding = buildVector->GetEncoding();
        const auto vectorSize = buildVector->GetSize();

        if (encoding != vec::OMNI_ENCODING_CONST &&
            buildRowIdx >= static_cast<uint32_t>(vectorSize)) {
            ret->SetNull(i);
            continue;
        }

        const uint32_t nullCheckIndex =
            encoding == vec::OMNI_ENCODING_CONST ? 0 : buildRowIdx;
        if (buildVector->IsNull(nullCheckIndex)) {
            ret->SetNull(i);
            continue;
        }

        T value;
        if (encoding == vec::OMNI_ENCODING_CONST) {
            value = static_cast<ConstVector<T> *>(buildVector)->GetConstValue();
        } else if (encoding == vec::OMNI_DICTIONARY) {
            value = static_cast<Vector<DictionaryContainer<T>> *>(buildVector)
                        ->GetValue(buildRowIdx);
        } else if (encoding == vec::OMNI_FLAT) {
            value = static_cast<Vector<T> *>(buildVector)->GetValue(buildRowIdx);
        } else {
            ret->SetNull(i);
            continue;
        }

        ret->SetValue(i, value);
    }

    return ret;
}

BaseVector *CascadingJoinOutputBuilder::ConstructBuildVarcharColumn(
    std::vector<BaseVector ***> & levelColumns,
    int32_t outputCol,
    int32_t level,
    int32_t numRows)
{
    if (levelColumns.size() != 1 || levelColumns[0] == nullptr ||
        outputCol < 0 || numRows <= 0) {
        return nullptr;
    }

    BaseVector *** partitionColumns = levelColumns[0];
    BaseVector ** columnBatches = partitionColumns[outputCol];
    if (columnBatches == nullptr) {
        return nullptr;
    }

    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    using DictionaryVector = Vector<DictionaryContainer<std::string_view>>;

    auto *ret = new VarcharVector(numRows);
    std::string_view value;

    for (int32_t i = 0; i < numRows; ++i) {
        const CascadingBuildAddress address = GetBuildAddress(i, level);
        const uint32_t vecBatchIdx = address.vecBatchIdx;
        const uint32_t buildRowIdx = address.rowIdx;

        BaseVector *buildVector = columnBatches[vecBatchIdx];
        if (buildVector == nullptr) {
            ret->SetNull(i);
            continue;
        }

        const auto typeId = buildVector->GetTypeId();
        const auto encoding = buildVector->GetEncoding();

        if (typeId != OMNI_VARCHAR &&
            typeId != OMNI_CHAR &&
            typeId != OMNI_VARBINARY) {
            ret->SetNull(i);
            continue;
        }

        if (buildRowIdx >= static_cast<uint32_t>(buildVector->GetSize()) &&
            encoding != OMNI_ENCODING_CONST) {
            ret->SetNull(i);
            continue;
        }

        const uint32_t nullCheckRow =
            encoding == OMNI_ENCODING_CONST ? 0 : buildRowIdx;

        if (buildVector->IsNull(nullCheckRow)) {
            ret->SetNull(i);
            continue;
        }

        if (encoding == OMNI_ENCODING_CONST) {
            value =
                static_cast<ConstVector<std::string_view> *>(buildVector)
                    ->GetConstValue();
        } else if (encoding == OMNI_DICTIONARY) {
            value =
                static_cast<DictionaryVector *>(buildVector)
                    ->GetValue(buildRowIdx);
        } else if (encoding == OMNI_FLAT) {
            value =
                static_cast<VarcharVector *>(buildVector)
                    ->GetValue(buildRowIdx);
        } else {
            ret->SetNull(i);
            continue;
        }

        ret->SetValue(i, value);
    }

    return ret;
}

CascadingLookupJoinOperatorFactory::CascadingLookupJoinOperatorFactory(
    std::vector<type::DataTypes> & probeTypesList,
    std::vector<std::vector<int32_t>> & probeHashColsList,
    std::vector<std::vector<int32_t>> & probeHashColTypesList,
    std::vector<std::vector<int32_t>> & buildOutputColsList,
    std::vector<type::DataTypes> & buildOutputTypesList,
    std::vector<HashTableVariants *> & hashTablesList,
    int32_t outputRowSize)
    : outputRowSize(outputRowSize),
      cascadeLevels(static_cast<int32_t>(hashTablesList.size()))
{
    this->probeTypesList = probeTypesList;
    this->probeHashColsList = probeHashColsList;
    this->probeHashColTypesList = probeHashColTypesList;
    this->buildOutputColsList = buildOutputColsList;
    this->buildOutputTypesList = buildOutputTypesList;
    this->hashTablesList = hashTablesList;
}

CascadingLookupJoinOperatorFactory *
CascadingLookupJoinOperatorFactory::CreateCascadingLookupJoinOperatorFactory(
    std::vector<std::shared_ptr<const HashJoinNode>> & joinNodes,
    std::vector<HashBuilderWithExprOperatorFactory *> & hashBuilderFactories,
    const config::QueryConfig & queryConfig)
{
    (void)queryConfig;

    if (joinNodes.empty() || hashBuilderFactories.empty()) {
        throw OmniException(
            "INVALID_ARGUMENT",
            "joinNodes and hashBuilderFactories cannot be empty");
    }

    std::vector<type::DataTypes> probeTypesList;
    std::vector<std::vector<int32_t>> probeHashColsList;
    std::vector<std::vector<int32_t>> probeHashColTypesList;
    std::vector<std::vector<int32_t>> buildOutputColsList;
    std::vector<type::DataTypes> buildOutputTypesList;
    std::vector<HashTableVariants *> hashTablesList;

    for (size_t i = 0; i < joinNodes.size(); ++i) {
        auto & joinNode = joinNodes[i];
        auto & hashBuilderFactory = hashBuilderFactories[i];

        auto probeOutputTypes = joinNode->LeftOutputType();
        probeTypesList.push_back(*probeOutputTypes);

        std::vector<int32_t> probeHashCols;
        for (const auto & key : joinNode->LeftKeys()) {
            auto *fieldKey = dynamic_cast<FieldExpr *>(key);
            probeHashCols.push_back(fieldKey->colVal);
        }
        probeHashColsList.push_back(probeHashCols);

        std::vector<int32_t> probeHashColTypes;
        auto probeTypeIds = probeOutputTypes->GetIds();
        for (int32_t col : probeHashCols) {
            probeHashColTypes.push_back(probeTypeIds[col]);
        }
        probeHashColTypesList.push_back(probeHashColTypes);

        auto buildOutputTypes = joinNode->RightOutputType();
        const int32_t buildOutputColsCount = buildOutputTypes->GetSize();
        std::vector<int32_t> buildOutputCols;
        buildOutputCols.reserve(static_cast<size_t>(buildOutputColsCount));
        for (int32_t idx = 0; idx < buildOutputColsCount; ++idx) {
            buildOutputCols.push_back(idx);
        }
        buildOutputColsList.push_back(buildOutputCols);
        buildOutputTypesList.push_back(*buildOutputTypes);

        auto hashBuilderOperatorFactory =
            hashBuilderFactory->GetHashBuilderOperatorFactory();
        hashTablesList.push_back(
            hashBuilderOperatorFactory->GetHashTablesVariants());
    }

    int32_t outputRowSize = OperatorUtil::GetRowSize(probeTypesList[0].Get());
    for (const auto & buildTypes : buildOutputTypesList) {
        outputRowSize += OperatorUtil::GetRowSize(buildTypes.Get());
    }

    return new CascadingLookupJoinOperatorFactory(
        probeTypesList,
        probeHashColsList,
        probeHashColTypesList,
        buildOutputColsList,
        buildOutputTypesList,
        hashTablesList,
        outputRowSize);
}

CascadingLookupJoinOperatorFactory *
CascadingLookupJoinOperatorFactory::CreateCascadingLookupJoinOperatorFactory(
    std::shared_ptr<const CascadingJoinNode> cascadingNode,
    std::vector<HashBuilderWithExprOperatorFactory *> & hashBuilderFactories,
    const config::QueryConfig & queryConfig)
{
    (void)queryConfig;

    if (!cascadingNode) {
        throw OmniException(
            "INVALID_ARGUMENT",
            "cascadingNode cannot be null");
    }

    const int32_t cascadeLevels = cascadingNode->CascadeLevels();
    if (cascadeLevels <= 0) {
        throw OmniException(
            "INVALID_ARGUMENT",
            "cascadeLevels must be positive");
    }

    if (cascadeLevels != static_cast<int32_t>(hashBuilderFactories.size())) {
        throw OmniException(
            "INVALID_ARGUMENT",
            "cascadeLevels must match hashBuilderFactories size");
    }

    const auto & probeKeysList = cascadingNode->ProbeKeysList();
    const auto & buildKeysList = cascadingNode->BuildKeysList();
    const auto & buildNodes = cascadingNode->BuildNodes();
    const auto & buildOutputTypesPtrs = cascadingNode->BuildOutputTypesList();
    const auto & buildOutputColsListFromNode = cascadingNode->BuildOutputColsList();

    if (!cascadingNode->ProbeNode() ||
        !cascadingNode->ProbeNode()->OutputType()) {
        throw OmniException(
            "INVALID_ARGUMENT",
            "Cascading root probe/output type cannot be null");
    }

    if (probeKeysList.size() != static_cast<size_t>(cascadeLevels) ||
        buildKeysList.size() != static_cast<size_t>(cascadeLevels) ||
        buildNodes.size() != static_cast<size_t>(cascadeLevels) ||
        buildOutputTypesPtrs.size() != static_cast<size_t>(cascadeLevels) ||
        buildOutputColsListFromNode.size() != static_cast<size_t>(cascadeLevels)) {
        throw OmniException(
            "INVALID_ARGUMENT",
            "CascadingJoinNode level metadata size mismatch");
    }

    const type::DataTypes rootProbeTypes =
        *(cascadingNode->ProbeNode()->OutputType());
    const auto rootProbeTypeIds = rootProbeTypes.GetIds();
    const int32_t rootProbeColumnCount = rootProbeTypes.GetSize();

    std::vector<int32_t> expectedOutputTypeIds;
    expectedOutputTypeIds.reserve(
        static_cast<size_t>(rootProbeColumnCount));
    for (int32_t column = 0;
         column < rootProbeColumnCount;
         ++column) {
        expectedOutputTypeIds.push_back(
            rootProbeTypeIds[column]);
    }

    std::vector<type::DataTypes> probeTypesList;
    std::vector<std::vector<int32_t>> probeHashColsList;
    std::vector<std::vector<int32_t>> probeHashColTypesList;
    std::vector<std::vector<int32_t>> buildOutputColsList;
    std::vector<type::DataTypes> buildOutputTypesList;
    std::vector<HashTableVariants *> hashTablesList;

    probeTypesList.reserve(static_cast<size_t>(cascadeLevels));
    probeHashColsList.reserve(static_cast<size_t>(cascadeLevels));
    probeHashColTypesList.reserve(static_cast<size_t>(cascadeLevels));
    buildOutputColsList.reserve(static_cast<size_t>(cascadeLevels));
    buildOutputTypesList.reserve(static_cast<size_t>(cascadeLevels));
    hashTablesList.reserve(static_cast<size_t>(cascadeLevels));

    for (int32_t level = 0; level < cascadeLevels; ++level) {
        const size_t levelIndex = static_cast<size_t>(level);

        // Root-probe semantics:
        // every cascading level probes its hash table with columns from A.
        // Do NOT switch level>0 to the previous build table's schema.
        probeTypesList.push_back(rootProbeTypes);

        const auto & probeKeys = probeKeysList[levelIndex];
        if (probeKeys.empty()) {
            throw OmniException(
                "INVALID_ARGUMENT",
                "Cascading probe keys cannot be empty");
        }

        std::vector<int32_t> probeHashCols;
        probeHashCols.reserve(probeKeys.size());

        std::vector<int32_t> probeHashColTypes;
        probeHashColTypes.reserve(probeKeys.size());

        for (const auto & key : probeKeys) {
            auto *fieldKey = dynamic_cast<FieldExpr *>(key);
            if (fieldKey == nullptr) {
                throw OmniException(
                    "NOT_SUPPORTED",
                    "Cascading join currently supports direct field probe keys only");
            }

            const int32_t col = fieldKey->colVal;
            if (col < 0 || col >= rootProbeColumnCount) {
                throw OmniException(
                    "INVALID_ARGUMENT",
                    "Cascading probe key column is outside root probe schema");
            }

            probeHashCols.push_back(col);
            probeHashColTypes.push_back(rootProbeTypeIds[col]);
        }

        if (!buildNodes[levelIndex] || !buildNodes[levelIndex]->OutputType() ||
            !buildOutputTypesPtrs[levelIndex]) {
            throw OmniException(
                "INVALID_ARGUMENT",
                "Cascading build node/output type cannot be null");
        }

        // ------------------------------------------------------------------
        // The runtime hash-table probe path performs typed vector reads.
        // Therefore every root-probe key must have the exact same physical
        // Omni type as the corresponding build key used to create this level's
        // hash table.  Catch a bad Project/key rewrite here instead of allowing
        // table.Find() to reinterpret a Vector<T> as the wrong T.
        // ------------------------------------------------------------------
        const auto & buildKeys = buildKeysList[levelIndex];
        if (buildKeys.size() != probeKeys.size()) {
            throw OmniException(
                "INVALID_ARGUMENT",
                "Cascading probe/build key count mismatch");
        }

        const auto buildTypeIds = buildOutputTypesPtrs[levelIndex]->GetIds();
        const int32_t buildColumnCount =
            buildOutputTypesPtrs[levelIndex]->GetSize();

        for (size_t keyIndex = 0; keyIndex < buildKeys.size(); ++keyIndex) {
            auto *buildField =
                dynamic_cast<FieldExpr *>(buildKeys[keyIndex]);

            if (buildField == nullptr) {
                throw OmniException(
                    "NOT_SUPPORTED",
                    "Cascading join currently supports direct field build keys only");
            }

            const int32_t buildCol = buildField->colVal;
            if (buildCol < 0 || buildCol >= buildColumnCount) {
                throw OmniException(
                    "INVALID_ARGUMENT",
                    "Cascading build key column is outside build schema");
            }

            const int32_t probeType =
                probeHashColTypes[keyIndex];
            const int32_t buildType =
                buildTypeIds[buildCol];

            if (probeType != buildType) {


                throw OmniException(
                    "INVALID_ARGUMENT",
                    "Cascading probe/build key physical types do not match");
            }
        }

        probeHashColsList.emplace_back(std::move(probeHashCols));
        probeHashColTypesList.emplace_back(std::move(probeHashColTypes));

        const auto & buildOutputCols =
            buildOutputColsListFromNode[levelIndex];

        for (int32_t outputCol : buildOutputCols) {
            if (outputCol < 0 ||
                outputCol >= buildColumnCount) {
                throw OmniException(
                    "INVALID_ARGUMENT",
                    "Cascading build output column is outside build schema");
            }

            expectedOutputTypeIds.push_back(
                buildTypeIds[outputCol]);
        }

        buildOutputColsList.push_back(
            buildOutputCols);
        buildOutputTypesList.push_back(
            *buildOutputTypesPtrs[levelIndex]);

        if (hashBuilderFactories[levelIndex] == nullptr) {
            throw OmniException(
                "INVALID_ARGUMENT",
                "Cascading hash builder cannot be null");
        }

        auto hashBuilderOperatorFactory =
            hashBuilderFactories[levelIndex]->GetHashBuilderOperatorFactory();

        if (hashBuilderOperatorFactory == nullptr ||
            hashBuilderOperatorFactory->GetHashTablesVariants() == nullptr) {
            throw OmniException(
                "INVALID_ARGUMENT",
                "Cascading hash builder/hash table cannot be null");
        }

        hashTablesList.push_back(
            hashBuilderOperatorFactory->GetHashTablesVariants());
    }

    if (cascadingNode->OutputType() == nullptr) {
        throw OmniException(
            "INVALID_ARGUMENT",
            "CascadingJoinNode output type cannot be null");
    }

    const auto plannedOutputTypeIds =
        cascadingNode->OutputType()->GetIds();
    const int32_t plannedOutputColumnCount =
        cascadingNode->OutputType()->GetSize();

    if (plannedOutputColumnCount !=
        static_cast<int32_t>(expectedOutputTypeIds.size())) {
        throw OmniException(
            "INTERNAL_ERROR",
            "Cascading planned output column count does not match runtime output");
    }

    for (int32_t i = 0;
         i < plannedOutputColumnCount;
         ++i) {
        if (plannedOutputTypeIds[i] !=
            expectedOutputTypeIds[static_cast<size_t>(i)]) {
            throw OmniException(
                "INTERNAL_ERROR",
                "Cascading planned output type order does not match runtime output");
        }
    }

    int32_t outputRowSize =
        OperatorUtil::GetRowSize(rootProbeTypes.Get());

    for (size_t level = 0;
         level < buildOutputTypesList.size();
         ++level) {
        std::vector<type::DataTypePtr> selectedBuildTypes;
        const auto & buildTypes =
            buildOutputTypesList[level];
        const auto & outputCols =
            buildOutputColsList[level];

        selectedBuildTypes.reserve(outputCols.size());
        for (int32_t outputCol : outputCols) {
            selectedBuildTypes.push_back(
                buildTypes.GetType(outputCol));
        }

        outputRowSize +=
            OperatorUtil::GetRowSize(selectedBuildTypes);
    }

    return new CascadingLookupJoinOperatorFactory(
        probeTypesList,
        probeHashColsList,
        probeHashColTypesList,
        buildOutputColsList,
        buildOutputTypesList,
        hashTablesList,
        outputRowSize);
}

Operator *CascadingLookupJoinOperatorFactory::CreateOperator()
{
    return new CascadingLookupJoinOperator(
        probeTypesList,
        probeHashColsList,
        probeHashColTypesList,
        buildOutputColsList,
        buildOutputTypesList,
        hashTablesList,
        outputRowSize);
}

CascadingLookupJoinOperator::CascadingLookupJoinOperator(
    std::vector<type::DataTypes> & probeTypesList,
    std::vector<std::vector<int32_t>> & probeHashColsList,
    std::vector<std::vector<int32_t>> & probeHashColTypesList,
    std::vector<std::vector<int32_t>> & buildOutputColsList,
    std::vector<type::DataTypes> & buildOutputTypesList,
    std::vector<HashTableVariants *> & hashTablesList,
    int32_t outputRowSize)
    : cascadeLevels(static_cast<int32_t>(hashTablesList.size())),
      outputRowSize(outputRowSize)
{
    SetOperatorName("CascadingLookupJoin");

    this->probeTypesList = probeTypesList;
    this->probeHashColsList = probeHashColsList;
    this->probeHashColTypesList = probeHashColTypesList;
    this->buildOutputColsList = buildOutputColsList;
    this->hashTablesList = hashTablesList;
    this->buildOutputTypesList = buildOutputTypesList;

    if (cascadeLevels <= 0 ||
        this->probeTypesList.size() != static_cast<size_t>(cascadeLevels) ||
        this->probeHashColsList.size() != static_cast<size_t>(cascadeLevels) ||
        this->probeHashColTypesList.size() != static_cast<size_t>(cascadeLevels) ||
        this->buildOutputColsList.size() != static_cast<size_t>(cascadeLevels) ||
        this->buildOutputTypesList.size() != static_cast<size_t>(cascadeLevels)) {
        throw OmniException(
            "INVALID_ARGUMENT",
            "Cascading operator level metadata size mismatch");
    }

    const auto rootProbeTypeIds =
        this->probeTypesList[0].GetIds();
    const int32_t rootProbeColumnCount =
        this->probeTypesList[0].GetSize();

    for (int32_t level = 0; level < cascadeLevels; ++level) {
        const size_t levelIndex =
            static_cast<size_t>(level);

        if (this->probeTypesList[levelIndex].GetSize() !=
            rootProbeColumnCount) {
            throw OmniException(
                "INVALID_ARGUMENT",
                "Cascading levels must share the same root probe schema");
        }

        const auto levelProbeTypeIds =
            this->probeTypesList[levelIndex].GetIds();
        for (int32_t column = 0;
             column < rootProbeColumnCount;
             ++column) {
            if (levelProbeTypeIds[column] !=
                rootProbeTypeIds[column]) {
                throw OmniException(
                    "INVALID_ARGUMENT",
                    "Cascading levels must share the same root probe schema");
            }
        }

        if (this->probeHashColsList[levelIndex].empty() ||
            this->probeHashColsList[levelIndex].size() !=
                this->probeHashColTypesList[levelIndex].size()) {
            throw OmniException(
                "INVALID_ARGUMENT",
                "Cascading probe key metadata is invalid");
        }

        for (size_t key = 0;
             key < this->probeHashColsList[levelIndex].size();
             ++key) {
            const int32_t col =
                this->probeHashColsList[levelIndex][key];

            if (col < 0 || col >= rootProbeColumnCount ||
                this->probeHashColTypesList[levelIndex][key] !=
                    rootProbeTypeIds[col]) {
                throw OmniException(
                    "INVALID_ARGUMENT",
                    "Cascading probe key does not match root probe schema");
            }
        }

        const int32_t buildColumnCount =
            this->buildOutputTypesList[levelIndex].GetSize();

        for (int32_t outputCol :
             this->buildOutputColsList[levelIndex]) {
            if (outputCol < 0 ||
                outputCol >= buildColumnCount) {
                throw OmniException(
                    "INVALID_ARGUMENT",
                    "Cascading build output column is outside build schema");
            }
        }
    }

    constexpr int32_t kTargetOutputBatchBytes = 1 << 20; // 1 MiB
    constexpr int32_t kMinOutputBatchRows = 1024;
    constexpr int32_t kMaxOutputBatchRows = 65536;

    int32_t maxOutputRows = kMinOutputBatchRows;
    if (outputRowSize > 0) {
        maxOutputRows = kTargetOutputBatchBytes / outputRowSize;
        if (maxOutputRows < kMinOutputBatchRows) {
            maxOutputRows = kMinOutputBatchRows;
        } else if (maxOutputRows > kMaxOutputBatchRows) {
            maxOutputRows = kMaxOutputBatchRows;
        }
    }

    outputBuilder = std::make_unique<CascadingJoinOutputBuilder>(
        maxOutputRows, cascadeLevels);
    outputBuilder->SetBuildOutputTypes(this->buildOutputTypesList);

    if (cascadeLevels > 0) {
        probeHashColumns =
            new BaseVector *[this->probeHashColsList[0].size()]();
    }

    probeOutputCols.reserve(
        static_cast<size_t>(this->probeTypesList[0].GetSize()));
    for (int32_t i = 0; i < this->probeTypesList[0].GetSize(); ++i) {
        probeOutputCols.push_back(i);
    }

    levelProbeSerializers.resize(static_cast<size_t>(cascadeLevels));
    levelProbeColumnBuffers.resize(static_cast<size_t>(cascadeLevels));
    levelBuildColumns.resize(static_cast<size_t>(cascadeLevels));
    frontiers.resize(static_cast<size_t>(cascadeLevels));

    reusableMatchChain.resize(static_cast<size_t>(cascadeLevels));

    for (int32_t level = 0; level < cascadeLevels; ++level) {
        levelExecutionContexts.push_back(
            std::make_unique<ExecutionContext>());

        levelProbeSerializers[static_cast<size_t>(level)].resize(
            this->probeHashColsList[level].size(), nullptr);
        levelProbeColumnBuffers[static_cast<size_t>(level)].resize(
            this->probeHashColsList[level].size(), nullptr);

        // Do not inspect HashTableVariants while the build side may still be
        // constructing it.  InitFirst() initializes these values when probe
        // input actually starts.
        levelPartitionMasks.push_back(0);
        levelIsSingleHT.push_back(false);
    }
}

CascadingLookupJoinOperator::~CascadingLookupJoinOperator()
{
    delete[] probeHashColumns;
}

void CascadingLookupJoinOperator::InitFirst()
{
    for (int32_t level = 0; level < cascadeLevels; ++level) {
        VisitHashTableVariant(
            hashTablesList[level],
            level,
            "InitFirst",
            [&](auto && arg) {
                using TableType = std::decay_t<decltype(arg)>;
                if constexpr (IsJoinHashTableVariants<TableType>::value) {
                    const auto hashTableCount = arg.GetHashTableCount();

                    if (hashTableCount == 0) {
                        throw OmniException(
                            "INVALID_STATE",
                            "Cascading hash table count cannot be zero");
                    }

                    levelPartitionMasks[static_cast<size_t>(level)] =
                        hashTableCount - 1;
                    levelIsSingleHT[static_cast<size_t>(level)] =
                        hashTableCount == 1;

                    levelBuildColumns[static_cast<size_t>(level)].resize(
                        hashTableCount);
                    for (uint32_t partition = 0;
                         partition < hashTableCount;
                         ++partition) {
                        levelBuildColumns[static_cast<size_t>(level)][partition] =
                            arg.GetColumns(partition);
                    }
                } else {
                    throw OmniException(
                        "NOT_SUPPORTED",
                        "CascadingJoin only supports JoinHashTableVariants");
                }
            });
    }
}

VectorSerializerIgnoreNull
CascadingLookupJoinOperator::SelectSerializer(BaseVector *vector) const
{
    if (vector == nullptr) {
        return nullptr;
    }

    const auto typeId = static_cast<size_t>(vector->GetTypeId());
    VectorSerializerIgnoreNull serializer = nullptr;

    if (vector->GetEncoding() == Encoding::OMNI_DICTIONARY &&
        typeId < dicVectorSerializerIgnoreNullCenter.size()) {
        serializer = dicVectorSerializerIgnoreNullCenter[typeId];
    } else if (
        vector->GetEncoding() == Encoding::OMNI_ENCODING_CONST &&
        typeId < constVectorSerializerIgnoreNullCenter.size()) {
        serializer = constVectorSerializerIgnoreNullCenter[typeId];
    }

    if (serializer == nullptr &&
        typeId < vectorSerializerIgnoreNullCenter.size()) {
        serializer = vectorSerializerIgnoreNullCenter[typeId];
    }

    return serializer;
}

void CascadingLookupJoinOperator::PopulateLevel0ProbeNulls()
{
    const int32_t rowCount = curInputBatch->GetRowCount();
    curProbeNulls.resize(static_cast<size_t>(rowCount));
    std::fill(curProbeNulls.begin(), curProbeNulls.end(), 0);

    level0HasAnyNull = false;

    for (size_t key = 0; key < probeHashColsList[0].size(); ++key) {
        BaseVector *column = probeHashColumns[key];
        if (column == nullptr || !column->HasNull()) {
            continue;
        }

        level0HasAnyNull = true;

        for (int32_t row = 0; row < rowCount; ++row) {
            if (column->IsNull(row)) {
                curProbeNulls[static_cast<size_t>(row)] = 1;
            }
        }
    }
}
void CascadingLookupJoinOperator::PrepareLevel0Serializers()
{
    auto & serializers = levelProbeSerializers[0];

    for (size_t key = 0; key < probeHashColsList[0].size(); ++key) {
        serializers[key] = SelectSerializer(probeHashColumns[key]);
    }
}

void CascadingLookupJoinOperator::PrepareProbeCapabilities()
{
    level0ProbeSimdCapable = false;

    if (cascadeLevels <= 0 ||
        hashTablesList.empty() ||
        !levelIsSingleHT[0] ||
        probeHashColumns == nullptr) {
        return;
    }

    level0ProbeSimdCapable = VisitHashTableVariant(
        hashTablesList[0],
        0,
        "PrepareProbeCapabilities",
        [&](auto && table) {
            return table.CanProbeSIMD(
                probeHashColumns,
                probeHashColsList[0].size(),
                levelPartitionMasks[0]);
        });
}

void CascadingLookupJoinOperator::PrepareCurrentProbe()
{
    if (curInputBatch == nullptr || cascadeLevels <= 0) {
        return;
    }

    // Bind every level's probe columns to the SAME root A VectorBatch.
    for (int32_t level = 0; level < cascadeLevels; ++level) {
        const size_t levelIndex = static_cast<size_t>(level);
        const auto & hashCols = probeHashColsList[levelIndex];
        auto & columnBuffer = levelProbeColumnBuffers[levelIndex];
        auto & serializers = levelProbeSerializers[levelIndex];

        if (levelIndex >= probeTypesList.size() ||
            levelIndex >= probeHashColTypesList.size()) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading probe metadata level is out of range");
        }

        if (columnBuffer.size() != hashCols.size() ||
            serializers.size() != hashCols.size() ||
            probeHashColTypesList[levelIndex].size() != hashCols.size()) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading probe metadata/key buffer size mismatch");
        }

        const int32_t rootColumnCount =
            probeTypesList[0].GetSize();

        for (size_t key = 0; key < hashCols.size(); ++key) {
            const int32_t col = hashCols[key];
            if (col < 0 || col >= rootColumnCount) {


                throw OmniException(
                    "INVALID_STATE",
                    "Cascading probe column is outside current root schema");
            }

            BaseVector *vector = curInputBatch->Get(col);
            if (vector == nullptr) {
                throw OmniException(
                    "INVALID_STATE",
                    "Cascading root probe vector is null");
            }

            const int32_t expectedType =
                probeHashColTypesList[levelIndex][key];
            const int32_t actualType =
                static_cast<int32_t>(vector->GetTypeId());

            if (actualType != expectedType) {
                throw OmniException(
                    "INVALID_STATE",
                    "Cascading root probe vector type does not match planned key type");
            }

            columnBuffer[key] = vector;
            serializers[key] = SelectSerializer(vector);

            // VARCHAR/CHAR/VARBINARY probing actually invokes the serializer.
            // Numeric fixed-key probing may not use it, so only require it for
            // variable-width string/binary keys.
            if ((actualType == OMNI_VARCHAR ||
                 actualType == OMNI_CHAR ||
                 actualType == OMNI_VARBINARY) &&
                serializers[key] == nullptr) {
                throw OmniException(
                    "INVALID_STATE",
                    "Cascading variable-width probe serializer is null");
            }

            if (level == 0) {
                probeHashColumns[key] = vector;
            }
        }
    }

    PopulateLevel0ProbeNulls();
    PrepareLevel0Serializers();
    PrepareProbeCapabilities();
}

// Legacy helper retained for source/header compatibility.
 // The root-probe frontier hot path no longer calls this function.
bool CascadingLookupJoinOperator::PrepareLevelProbeGroup(
    int32_t level,
    uint32_t sourceVecBatchIdx,
    std::vector<BaseVector *> & probeColumns,
    std::vector<VectorSerializerIgnoreNull> & serializers)
{
    // Root-probe execution: every level reads its probe keys from the current
    // root A VectorBatch.  sourceVecBatchIdx belongs to the previous build
    // frontier and is intentionally irrelevant to the probe source.
    (void)sourceVecBatchIdx;

    if (curInputBatch == nullptr ||
        level < 0 ||
        static_cast<size_t>(level) >= probeHashColsList.size()) {
        return false;
    }

    const auto & hashCols =
        probeHashColsList[static_cast<size_t>(level)];

    if (probeColumns.size() != hashCols.size() ||
        serializers.size() != hashCols.size()) {
        return false;
    }

    for (size_t key = 0; key < hashCols.size(); ++key) {
        const int32_t col = hashCols[key];
        if (col < 0 || col >= probeTypesList[0].GetSize()) {
            return false;
        }

        BaseVector *vector = curInputBatch->Get(col);
        if (vector == nullptr) {
            return false;
        }

        probeColumns[key] = vector;
        serializers[key] = SelectSerializer(vector);
        if (serializers[key] == nullptr) {
            return false;
        }
    }

    return true;
}

// Legacy helper retained for source/header compatibility.
 // Root-probe level>0 execution no longer groups by previous-build vecBatchIdx.
void CascadingLookupJoinOperator::BuildGroupedParentIndices(
    const std::vector<CascadingFrontierEntry> & inputFrontier)
{
    groupedParentIndices.clear();
    batchCounts.clear();
    batchOffsets.clear();
    batchWriteOffsets.clear();

    if (inputFrontier.empty()) {
        return;
    }

    uint32_t maxBatchIdx = 0;
    for (const auto & entry : inputFrontier) {
        if (entry.address.vecBatchIdx > maxBatchIdx) {
            maxBatchIdx = entry.address.vecBatchIdx;
        }
    }

    batchCounts.assign(
        static_cast<size_t>(maxBatchIdx) + 1, 0);

    for (const auto & entry : inputFrontier) {
        ++batchCounts[entry.address.vecBatchIdx];
    }

    batchOffsets.resize(
        static_cast<size_t>(maxBatchIdx) + 2, 0);

    for (uint32_t batch = 0; batch <= maxBatchIdx; ++batch) {
        batchOffsets[static_cast<size_t>(batch) + 1] =
            batchOffsets[static_cast<size_t>(batch)] +
            batchCounts[batch];
    }

    batchWriteOffsets.assign(
        batchOffsets.begin(),
        batchOffsets.end() - 1);

    groupedParentIndices.resize(inputFrontier.size());

    for (uint32_t parentIndex = 0;
         parentIndex < static_cast<uint32_t>(inputFrontier.size());
         ++parentIndex) {
        const uint32_t batch =
            inputFrontier[parentIndex].address.vecBatchIdx;

        const uint32_t writeIndex =
            batchWriteOffsets[batch]++;

        groupedParentIndices[writeIndex] = parentIndex;
    }
}

template <bool HAS_NULLS, typename TableType>
void CascadingLookupJoinOperator::ProbeLevel0WithTable(TableType & table)
{
    auto & outputFrontier = frontiers[0];
    outputFrontier.clear();

    const int32_t rowCount = curInputBatch->GetRowCount();
    auto & serializers = levelProbeSerializers[0];
    ExecutionContext *context = levelExecutionContexts[0].get();
    const uint32_t partition = levelPartitionMasks[0];
    const size_t hashColCount = probeHashColsList[0].size();

    // ---------------------------------------------------------------------
    // LookupJoin-style ArrayTable/NEON fast path for level 0.
    //
    // Level 0 is the ideal case: root probe rows are physically contiguous, so
    // the raw key pointer can be consumed directly without a gather step. The
    // match side still emits CascadingFrontierEntry instead of final rows.
    // ---------------------------------------------------------------------
#if defined(__aarch64__) || defined(__ARM_NEON)
    if (level0ProbeSimdCapable &&
        levelIsSingleHT[0] &&
        hashColCount == 1) {
        using Table = std::remove_reference_t<TableType>;

        if constexpr (Table::IS_SIMPLE_KEY) {
            auto *probeVector =
                table.GetSingleProbeHashKeyBase(probeHashColumns);

            if (probeVector != nullptr) {
                using KeyType =
                    typename std::remove_pointer<decltype(probeVector)>::type;

                if constexpr (
                    std::is_same_v<KeyType, int16_t> ||
                    std::is_same_v<KeyType, int32_t> ||
                    std::is_same_v<KeyType, int64_t>) {

                    // Same chunk width used by LookupJoinOperator's NEON path:
                    // 4 * 128-bit = 64 bytes of keys per iteration.
                    constexpr int32_t kSimdBytes = 64;
                    constexpr int32_t kVecLanes =
                        kSimdBytes / static_cast<int32_t>(sizeof(KeyType));

                    auto maxMinPair = table.GetmaxMinValue(partition);
                    const KeyType minValue =
                        static_cast<KeyType>(maxMinPair.second);
                    const KeyType maxValue =
                        static_cast<KeyType>(maxMinPair.first);

                    auto & arrayTable = table.GetArrayTable(partition);
                    bool *isAssigned = arrayTable->GetAssigned();
                    auto *slots = reinterpret_cast<int64_t *>(
                        arrayTable->GetSlots());

                    // If any prerequisite unexpectedly disappears, keep the
                    // existing scalar path instead of risking a bad probe.
                    if (isAssigned != nullptr && slots != nullptr) {
                        alignas(64) KeyType hashes[kVecLanes];
                        alignas(64) KeyType matches[kVecLanes];

                        int32_t probePosition = 0;
                        const int32_t simdEnd =
                            rowCount / kVecLanes * kVecLanes;
                        KeyType *currentProbe = probeVector;

                        __builtin_prefetch(currentProbe, 0, 3);

                        auto appendMatches =
                            [&](int32_t rowStart) {
                                for (int32_t lane = 0;
                                     lane < kVecLanes;
                                     ++lane) {
                                    const int32_t row = rowStart + lane;

                                    if constexpr (HAS_NULLS) {
                                        if (curProbeNulls[
                                                static_cast<size_t>(row)] != 0) {
                                            continue;
                                        }
                                    }

                                    if (!matches[lane]) {
                                        continue;
                                    }

                                    const KeyType hash = hashes[lane];
                                    if (hash < 0) {
                                        continue;
                                    }

                                    const size_t slotIndex =
                                        static_cast<size_t>(hash);
                                    if (!isAssigned[slotIndex]) {
                                        continue;
                                    }

                                    auto it =
                                        arrayTable
                                            ->TransformPtr(slots[slotIndex])
                                            ->Begin();

                                    while (it.IsOk()) {
                                        outputFrontier.push_back(
                                            CascadingFrontierEntry{
                                                row,
                                                CascadingBuildAddress{
                                                    static_cast<uint32_t>(
                                                        it->rowIdx),
                                                    static_cast<uint32_t>(
                                                        it->vecBatchIdx)},
                                                INVALID_PARENT});
                                        ++it;
                                    }
                                }
                            };

                        if constexpr (std::is_same_v<KeyType, int16_t>) {
                            const int16x8_t vMin =
                                vdupq_n_s16(minValue);
                            const int16x8_t vMax =
                                vdupq_n_s16(maxValue);

                            for (; probePosition < simdEnd;
                                 probePosition += kVecLanes) {
                                CascadingInt16ArrayCompareNeon(
                                    currentProbe,
                                    hashes,
                                    matches,
                                    vMin,
                                    vMax,
                                    minValue,
                                    maxValue);

                                appendMatches(probePosition);

                                currentProbe += kVecLanes;
                                __builtin_prefetch(
                                    currentProbe, 0, 3);
                            }
                        } else if constexpr (
                            std::is_same_v<KeyType, int32_t>) {
                            const int32x4_t vMin =
                                vdupq_n_s32(minValue);
                            const int32x4_t vMax =
                                vdupq_n_s32(maxValue);

                            for (; probePosition < simdEnd;
                                 probePosition += kVecLanes) {
                                CascadingInt32ArrayCompareNeon(
                                    currentProbe,
                                    hashes,
                                    matches,
                                    vMin,
                                    vMax,
                                    minValue,
                                    maxValue);

                                appendMatches(probePosition);

                                currentProbe += kVecLanes;
                                __builtin_prefetch(
                                    currentProbe, 0, 3);
                            }
                        } else if constexpr (
                            std::is_same_v<KeyType, int64_t>) {
                            const int64x2_t vMin =
                                vdupq_n_s64(minValue);
                            const int64x2_t vMax =
                                vdupq_n_s64(maxValue);

                            for (; probePosition < simdEnd;
                                 probePosition += kVecLanes) {
                                CascadingInt64ArrayCompareNeon(
                                    currentProbe,
                                    hashes,
                                    matches,
                                    vMin,
                                    vMax,
                                    minValue,
                                    maxValue);

                                appendMatches(probePosition);

                                currentProbe += kVecLanes;
                                __builtin_prefetch(
                                    currentProbe, 0, 3);
                            }
                        }

                        // Scalar tail.  This also keeps correctness for the
                        // final rows when rowCount is not a multiple of lanes.
                        for (; probePosition < rowCount;
                             ++probePosition) {
                            if constexpr (HAS_NULLS) {
                                if (curProbeNulls[
                                        static_cast<size_t>(probePosition)] != 0) {
                                    continue;
                                }
                            }

                            auto result = table.Find(
                                serializers,
                                context,
                                probeHashColumns,
                                hashColCount,
                                probePosition,
                                partition);

                            if (!result.IsInsert()) {
                                continue;
                            }

                            auto it = result.GetValue()->Begin();
                            while (it.IsOk()) {
                                outputFrontier.push_back(
                                    CascadingFrontierEntry{
                                        probePosition,
                                        CascadingBuildAddress{
                                            static_cast<uint32_t>(it->rowIdx),
                                            static_cast<uint32_t>(
                                                it->vecBatchIdx)},
                                        INVALID_PARENT});
                                ++it;
                            }
                        }

                        return;
                    }
                }
            }
        }
    }
#endif
    // Original scalar fallback.  This path is unchanged semantically and is
    // used on non-ARM builds, unsupported key/table layouts, multi-column
    // keys, or whenever CanProbeSIMD() returned false.
    // ---------------------------------------------------------------------
    for (int32_t probePosition = 0;
         probePosition < rowCount;
         ++probePosition) {
        if constexpr (HAS_NULLS) {
            if (curProbeNulls[static_cast<size_t>(probePosition)] != 0) {
                continue;
            }
        }

        auto result = table.Find(
            serializers,
            context,
            probeHashColumns,
            hashColCount,
            probePosition,
            partition);

        if (!result.IsInsert()) {
            continue;
        }

        auto it = result.GetValue()->Begin();
        while (it.IsOk()) {
            outputFrontier.push_back(CascadingFrontierEntry{
                probePosition,
                CascadingBuildAddress{
                    static_cast<uint32_t>(it->rowIdx),
                    static_cast<uint32_t>(it->vecBatchIdx)},
                INVALID_PARENT});
            ++it;
        }
    }
}

template <typename TableType>
void CascadingLookupJoinOperator::ProbeFrontierLevelWithTable(
    int32_t level,
    TableType & table,
    const std::vector<CascadingFrontierEntry> & inputFrontier,
    std::vector<CascadingFrontierEntry> & outputFrontier)
{
    outputFrontier.clear();

    if (inputFrontier.empty()) {
        return;
    }

    if (curInputBatch == nullptr) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading level probe has no current root input batch");
    }

    if (level <= 0 || level >= cascadeLevels) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading frontier level is outside valid range");
    }

    const size_t levelIndex = static_cast<size_t>(level);
    if (levelIndex >= levelProbeColumnBuffers.size() ||
        levelIndex >= levelProbeSerializers.size() ||
        levelIndex >= levelExecutionContexts.size() ||
        levelIndex >= levelPartitionMasks.size() ||
        levelIndex >= probeHashColsList.size() ||
        levelIndex >= probeHashColTypesList.size()) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading level metadata is incomplete");
    }

    auto & probeColumns = levelProbeColumnBuffers[levelIndex];
    auto & serializers = levelProbeSerializers[levelIndex];
    ExecutionContext *context = levelExecutionContexts[levelIndex].get();
    const uint32_t partition = levelPartitionMasks[levelIndex];
    const auto & hashCols = probeHashColsList[levelIndex];
    const auto & expectedHashTypes = probeHashColTypesList[levelIndex];
    const size_t hashColCount = hashCols.size();

    if (hashColCount == 0 ||
        probeColumns.size() != hashColCount ||
        serializers.size() != hashColCount ||
        expectedHashTypes.size() != hashColCount) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading level probe key metadata size mismatch");
    }

    // Current CascadingJoinNode/Gluten rule uses root-probe semantics: every
    // level's equality key is an A-side field. Keep that contract here. The
    // frontier address is still retained for lineage/final materialization.
    for (size_t key = 0; key < hashColCount; ++key) {
        BaseVector *vector = probeColumns[key];
        if (vector == nullptr) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading level probe vector is null");
        }

        const int32_t actualType =
            static_cast<int32_t>(vector->GetTypeId());
        if (actualType != expectedHashTypes[key]) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading level probe vector type mismatch");
        }

        if ((actualType == OMNI_VARCHAR ||
             actualType == OMNI_CHAR ||
             actualType == OMNI_VARBINARY) &&
            serializers[key] == nullptr) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading level variable-width probe serializer is null");
        }
    }

    const int32_t rootRowCount = curInputBatch->GetRowCount();

    // ---------------------------------------------------------------------
    // LookupJoin-style ArrayTable/NEON path for level > 0.
    //
    // A frontier is not guaranteed to be a contiguous row range because one
    // root row may create zero/one/many parent entries. For each SIMD block:
    //
    //   1. collect parentIndex + rootProbePosition;
    //   2. if the positions are contiguous and the source is a null-free FLAT
    //      vector, probe directly from the raw key pointer (same shape as
    //      LookupJoinOperator::ArrayJoinProbeSIMDNeon);
    //   3. otherwise gather the referenced keys into gatheredKeys[];
    //   4. run key-min/range NEON and directly index ArrayTable slots;
    //   5. append frontier lineage for every matched RowRefList entry.
    //
    // Unsupported layouts fall through to scalar table.Find().
    // ---------------------------------------------------------------------
#if defined(__aarch64__) || defined(__ARM_NEON)
    const bool levelSimdCapable =
        levelIsSingleHT[levelIndex] &&
        hashColCount == 1 &&
        table.CanProbeSIMD(
            probeColumns.data(),
            hashColCount,
            partition);

    if (levelSimdCapable) {
        using Table = std::remove_reference_t<TableType>;

        if constexpr (Table::IS_SIMPLE_KEY) {
            auto *directProbeVector =
                table.GetSingleProbeHashKeyBase(probeColumns.data());

            if (directProbeVector != nullptr) {
                using KeyType =
                    typename std::remove_pointer<decltype(directProbeVector)>::type;

                if constexpr (
                    std::is_same_v<KeyType, int16_t> ||
                    std::is_same_v<KeyType, int32_t> ||
                    std::is_same_v<KeyType, int64_t>) {

                    BaseVector *probeBase = probeColumns[0];
                    auto & arrayTable = table.GetArrayTable(partition);
                    bool *isAssigned = arrayTable->GetAssigned();
                    auto *slots = reinterpret_cast<int64_t *>(
                        arrayTable->GetSlots());

                    if (probeBase != nullptr &&
                        isAssigned != nullptr &&
                        slots != nullptr) {
                        constexpr int32_t kSimdBytes = 64;
                        constexpr int32_t kVecLanes =
                            kSimdBytes /
                            static_cast<int32_t>(sizeof(KeyType));

                        const auto maxMinPair =
                            table.GetmaxMinValue(partition);
                        const KeyType minValue =
                            static_cast<KeyType>(maxMinPair.second);
                        const KeyType maxValue =
                            static_cast<KeyType>(maxMinPair.first);

                        const auto probeEncoding = probeBase->GetEncoding();
                        const bool probeHasNull = probeBase->HasNull();

                        auto *flatVector =
                            probeEncoding == OMNI_FLAT
                                ? static_cast<Vector<KeyType> *>(probeBase)
                                : nullptr;
                        auto *dictVector =
                            probeEncoding == OMNI_DICTIONARY
                                ? static_cast<
                                      Vector<DictionaryContainer<KeyType>> *>(
                                      probeBase)
                                : nullptr;
                        auto *constVector =
                            probeEncoding == OMNI_ENCODING_CONST
                                ? static_cast<ConstVector<KeyType> *>(probeBase)
                                : nullptr;

                        alignas(64) KeyType gatheredKeys[kVecLanes];
                        alignas(64) KeyType hashes[kVecLanes];
                        alignas(64) KeyType matches[kVecLanes];
                        alignas(64) uint32_t parentIndices[kVecLanes];
                        alignas(64) int32_t rootRows[kVecLanes];
                        alignas(64) uint8_t validLane[kVecLanes];

                        auto readKey = [&](int32_t row, KeyType & value) -> bool {
                            const int32_t nullRow =
                                probeEncoding == OMNI_ENCODING_CONST ? 0 : row;

                            if (probeHasNull && probeBase->IsNull(nullRow)) {
                                return false;
                            }

                            if (flatVector != nullptr) {
                                value = flatVector->GetValue(row);
                                return true;
                            }
                            if (dictVector != nullptr) {
                                value = dictVector->GetValue(row);
                                return true;
                            }
                            if (constVector != nullptr) {
                                value = constVector->GetConstValue();
                                return true;
                            }
                            return false;
                        };

                        size_t cursor = 0;
                        while (cursor < inputFrontier.size()) {
                            int32_t laneCount = 0;

                            while (laneCount < kVecLanes &&
                                   cursor < inputFrontier.size()) {
                                const uint32_t parentIndex =
                                    static_cast<uint32_t>(cursor++);
                                const CascadingFrontierEntry & parent =
                                    inputFrontier[parentIndex];
                                const int32_t rootRow =
                                    parent.rootProbePosition;

                                if (rootRow < 0 || rootRow >= rootRowCount) {
                                    throw OmniException(
                                        "INVALID_STATE",
                                        "Cascading frontier contains invalid root probe position");
                                }

                                parentIndices[laneCount] = parentIndex;
                                rootRows[laneCount] = rootRow;
                                validLane[laneCount] = 1;
                                ++laneCount;
                            }

                            // Exact LookupJoin-like direct block: Case8's 1:1
                            // frontier normally keeps root rows contiguous, so
                            // avoid gather/GetValue entirely for those blocks.
                            bool directContiguousBlock =
                                flatVector != nullptr &&
                                !probeHasNull &&
                                laneCount == kVecLanes;

                            if (directContiguousBlock) {
                                const int32_t firstRow = rootRows[0];
                                for (int32_t lane = 1;
                                     lane < laneCount;
                                     ++lane) {
                                    if (rootRows[lane] != firstRow + lane) {
                                        directContiguousBlock = false;
                                        break;
                                    }
                                }
                            }

                            KeyType *compareInput = gatheredKeys;
                            if (directContiguousBlock) {
                                compareInput = directProbeVector + rootRows[0];
                            } else {
                                for (int32_t lane = 0;
                                     lane < laneCount;
                                     ++lane) {
                                    KeyType value{};
                                    const bool valid =
                                        readKey(rootRows[lane], value);
                                    gatheredKeys[lane] = value;
                                    validLane[lane] = valid ? 1 : 0;
                                }

                                for (int32_t lane = laneCount;
                                     lane < kVecLanes;
                                     ++lane) {
                                    gatheredKeys[lane] = minValue;
                                    parentIndices[lane] = 0;
                                    rootRows[lane] = 0;
                                    validLane[lane] = 0;
                                }
                            }

                            // The compare helpers consume one full 64-byte key
                            // block, matching LookupJoin's ArrayJoin kernel.
                            if constexpr (std::is_same_v<KeyType, int16_t>) {
                                const int16x8_t vMin = vdupq_n_s16(minValue);
                                const int16x8_t vMax = vdupq_n_s16(maxValue);
                                CascadingInt16ArrayCompareNeon(
                                    compareInput,
                                    hashes,
                                    matches,
                                    vMin,
                                    vMax,
                                    minValue,
                                    maxValue);
                            } else if constexpr (
                                std::is_same_v<KeyType, int32_t>) {
                                const int32x4_t vMin = vdupq_n_s32(minValue);
                                const int32x4_t vMax = vdupq_n_s32(maxValue);
                                CascadingInt32ArrayCompareNeon(
                                    compareInput,
                                    hashes,
                                    matches,
                                    vMin,
                                    vMax,
                                    minValue,
                                    maxValue);
                            } else {
                                const int64x2_t vMin = vdupq_n_s64(minValue);
                                const int64x2_t vMax = vdupq_n_s64(maxValue);
                                CascadingInt64ArrayCompareNeon(
                                    compareInput,
                                    hashes,
                                    matches,
                                    vMin,
                                    vMax,
                                    minValue,
                                    maxValue);
                            }

                            for (int32_t lane = 0;
                                 lane < laneCount;
                                 ++lane) {
                                if (!validLane[lane] || !matches[lane]) {
                                    continue;
                                }

                                const KeyType hash = hashes[lane];
                                if (hash < 0) {
                                    continue;
                                }

                                const size_t slotIndex =
                                    static_cast<size_t>(hash);
                                if (!isAssigned[slotIndex]) {
                                    continue;
                                }

                                const uint32_t parentIndex =
                                    parentIndices[lane];
                                const CascadingFrontierEntry & parent =
                                    inputFrontier[parentIndex];

                                auto it = arrayTable
                                    ->TransformPtr(slots[slotIndex])
                                    ->Begin();

                                while (it.IsOk()) {
                                    outputFrontier.push_back(
                                        CascadingFrontierEntry{
                                            parent.rootProbePosition,
                                            CascadingBuildAddress{
                                                static_cast<uint32_t>(
                                                    it->rowIdx),
                                                static_cast<uint32_t>(
                                                    it->vecBatchIdx)},
                                            parentIndex});
                                    ++it;
                                }
                            }
                        }

                        return;
                    }
                }
            }
        }
    }
#endif

    bool levelHasAnyNull = false;
    for (size_t key = 0; key < hashColCount; ++key) {
        if (probeColumns[key]->HasNull()) {
            levelHasAnyNull = true;
            break;
        }
    }

    for (uint32_t parentIndex = 0;
         parentIndex < static_cast<uint32_t>(inputFrontier.size());
         ++parentIndex) {
        const CascadingFrontierEntry & parent =
            inputFrontier[parentIndex];
        const int32_t probePosition =
            parent.rootProbePosition;

        if (probePosition < 0 || probePosition >= rootRowCount) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading frontier contains invalid root probe position");
        }

        if (levelHasAnyNull) {
            bool hasNull = false;
            for (size_t key = 0; key < hashColCount; ++key) {
                if (probeColumns[key]->IsNull(probePosition)) {
                    hasNull = true;
                    break;
                }
            }
            if (hasNull) {
                continue;
            }
        }

        auto result = table.Find(
            serializers,
            context,
            probeColumns.data(),
            hashColCount,
            probePosition,
            partition);

        if (!result.IsInsert()) {
            continue;
        }

        auto it = result.GetValue()->Begin();
        while (it.IsOk()) {
            outputFrontier.push_back(CascadingFrontierEntry{
                parent.rootProbePosition,
                CascadingBuildAddress{
                    static_cast<uint32_t>(it->rowIdx),
                    static_cast<uint32_t>(it->vecBatchIdx)},
                parentIndex});
            ++it;
        }
    }
}

bool CascadingLookupJoinOperator::AppendDirectFinalMatch(
    int32_t finalLevel,
    uint32_t parentIndex,
    const CascadingBuildAddress & finalAddress)
{
    if (outputBuilder == nullptr || outputBuilder->IsFull()) {
        return false;
    }

    if (finalLevel <= 0 || finalLevel >= cascadeLevels) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading direct-final level is outside valid range");
    }

    const size_t parentLevelIndex =
        static_cast<size_t>(finalLevel - 1);
    if (parentLevelIndex >= frontiers.size() ||
        parentIndex >= frontiers[parentLevelIndex].size()) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading direct-final parent index is outside frontier");
    }

    // The final address is known from the current hash-table match.  Recover
    // only the prefix [0, finalLevel-1] from the compact parentIndex chain.
    reusableMatchChain[static_cast<size_t>(finalLevel)] = finalAddress;

    uint32_t currentIndex = parentIndex;
    for (int32_t level = finalLevel - 1; level >= 0; --level) {
        const auto & frontier = frontiers[static_cast<size_t>(level)];
        if (currentIndex >= frontier.size()) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading direct-final lineage index is outside frontier");
        }

        const CascadingFrontierEntry & entry = frontier[currentIndex];
        reusableMatchChain[static_cast<size_t>(level)] = entry.address;

        if (level > 0) {
            currentIndex = entry.parentIndex;
        }
    }

    const CascadingFrontierEntry & finalParent =
        frontiers[parentLevelIndex][parentIndex];
    outputBuilder->AppendMatchChain(
        finalParent.rootProbePosition,
        reusableMatchChain);
    return true;
}

template <typename TableType>
void CascadingLookupJoinOperator::ProbeFinalLevelDirectWithTable(
    int32_t finalLevel,
    TableType & table,
    const std::vector<CascadingFrontierEntry> & inputFrontier)
{
    if (finalProbeComplete || outputBuilder->IsFull()) {
        return;
    }

    if (curInputBatch == nullptr) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading direct-final probe has no current root input batch");
    }

    if (finalLevel <= 0 || finalLevel >= cascadeLevels) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading direct-final level is outside valid range");
    }

    const size_t levelIndex = static_cast<size_t>(finalLevel);
    if (levelIndex >= levelProbeColumnBuffers.size() ||
        levelIndex >= levelProbeSerializers.size() ||
        levelIndex >= levelExecutionContexts.size() ||
        levelIndex >= levelPartitionMasks.size() ||
        levelIndex >= probeHashColsList.size() ||
        levelIndex >= probeHashColTypesList.size()) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading direct-final metadata is incomplete");
    }

    auto & probeColumns = levelProbeColumnBuffers[levelIndex];
    auto & serializers = levelProbeSerializers[levelIndex];
    ExecutionContext *context = levelExecutionContexts[levelIndex].get();
    const uint32_t partition = levelPartitionMasks[levelIndex];
    const auto & hashCols = probeHashColsList[levelIndex];
    const auto & expectedHashTypes = probeHashColTypesList[levelIndex];
    const size_t hashColCount = hashCols.size();

    if (hashColCount == 0 ||
        probeColumns.size() != hashColCount ||
        serializers.size() != hashColCount ||
        expectedHashTypes.size() != hashColCount) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading direct-final key metadata size mismatch");
    }

    for (size_t key = 0; key < hashColCount; ++key) {
        BaseVector *vector = probeColumns[key];
        if (vector == nullptr) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading direct-final probe vector is null");
        }

        const int32_t actualType =
            static_cast<int32_t>(vector->GetTypeId());
        if (actualType != expectedHashTypes[key]) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading direct-final probe vector type mismatch");
        }

        if ((actualType == OMNI_VARCHAR ||
             actualType == OMNI_CHAR ||
             actualType == OMNI_VARBINARY) &&
            serializers[key] == nullptr) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading direct-final serializer is null");
        }
    }

    if (inputFrontier.empty() ||
        finalProbeParentIndex >= inputFrontier.size()) {
        finalProbeParentIndex = inputFrontier.size();
        finalProbeMatchOffset = 0;
        finalProbeComplete = true;
        return;
    }

    const int32_t rootRowCount = curInputBatch->GetRowCount();
    const size_t resumeParentIndex = finalProbeParentIndex;
    const uint32_t resumeMatchOffset = finalProbeMatchOffset;

    // ---------------------------------------------------------------------
    // Final level ArrayTable/NEON path.
    //
    // This is the important Case8 optimization: the final hash-table probe no
    // longer calls table.Find() per parent when the same prerequisites used by
    // LookupJoin's ArrayJoin path hold. We compute direct array slots in SIMD
    // and immediately append final lineage into outputBuilder. No final frontier
    // is materialized.
    // ---------------------------------------------------------------------
#if defined(__aarch64__) || defined(__ARM_NEON)
    const bool levelSimdCapable =
        levelIsSingleHT[levelIndex] &&
        hashColCount == 1 &&
        table.CanProbeSIMD(
            probeColumns.data(),
            hashColCount,
            partition);

    if (levelSimdCapable) {
        using Table = std::remove_reference_t<TableType>;

        if constexpr (Table::IS_SIMPLE_KEY) {
            auto *directProbeVector =
                table.GetSingleProbeHashKeyBase(probeColumns.data());

            if (directProbeVector != nullptr) {
                using KeyType =
                    typename std::remove_pointer<decltype(directProbeVector)>::type;

                if constexpr (
                    std::is_same_v<KeyType, int16_t> ||
                    std::is_same_v<KeyType, int32_t> ||
                    std::is_same_v<KeyType, int64_t>) {

                    BaseVector *probeBase = probeColumns[0];
                    auto & arrayTable = table.GetArrayTable(partition);
                    bool *isAssigned = arrayTable->GetAssigned();
                    auto *slots = reinterpret_cast<int64_t *>(
                        arrayTable->GetSlots());

                    if (probeBase != nullptr &&
                        isAssigned != nullptr &&
                        slots != nullptr) {

                        constexpr int32_t kSimdBytes = 64;
                        constexpr int32_t kVecLanes =
                            kSimdBytes /
                            static_cast<int32_t>(sizeof(KeyType));

                        const auto maxMinPair =
                            table.GetmaxMinValue(partition);
                        const KeyType minValue =
                            static_cast<KeyType>(maxMinPair.second);
                        const KeyType maxValue =
                            static_cast<KeyType>(maxMinPair.first);

                        const auto probeEncoding = probeBase->GetEncoding();
                        const bool probeHasNull = probeBase->HasNull();

                        auto *flatVector =
                            probeEncoding == OMNI_FLAT
                                ? static_cast<Vector<KeyType> *>(probeBase)
                                : nullptr;
                        auto *dictVector =
                            probeEncoding == OMNI_DICTIONARY
                                ? static_cast<
                                      Vector<DictionaryContainer<KeyType>> *>(
                                      probeBase)
                                : nullptr;
                        auto *constVector =
                            probeEncoding == OMNI_ENCODING_CONST
                                ? static_cast<ConstVector<KeyType> *>(probeBase)
                                : nullptr;

                        alignas(64) KeyType gatheredKeys[kVecLanes];
                        alignas(64) KeyType hashes[kVecLanes];
                        alignas(64) KeyType matches[kVecLanes];
                        alignas(64) uint32_t parentIndices[kVecLanes];
                        alignas(64) int32_t rootRows[kVecLanes];
                        alignas(64) uint8_t validLane[kVecLanes];

                        auto readKey = [&](int32_t row, KeyType & value) -> bool {
                            const int32_t nullRow =
                                probeEncoding == OMNI_ENCODING_CONST ? 0 : row;

                            if (probeHasNull && probeBase->IsNull(nullRow)) {
                                return false;
                            }

                            if (flatVector != nullptr) {
                                value = flatVector->GetValue(row);
                                return true;
                            }
                            if (dictVector != nullptr) {
                                value = dictVector->GetValue(row);
                                return true;
                            }
                            if (constVector != nullptr) {
                                value = constVector->GetConstValue();
                                return true;
                            }
                            return false;
                        };

                        size_t cursor = finalProbeParentIndex;
                        while (cursor < inputFrontier.size()) {
                            int32_t laneCount = 0;

                            while (laneCount < kVecLanes &&
                                   cursor < inputFrontier.size()) {
                                const uint32_t parentIndex =
                                    static_cast<uint32_t>(cursor++);
                                const CascadingFrontierEntry & parent =
                                    inputFrontier[parentIndex];
                                const int32_t rootRow =
                                    parent.rootProbePosition;

                                if (rootRow < 0 || rootRow >= rootRowCount) {
                                    throw OmniException(
                                        "INVALID_STATE",
                                        "Cascading direct-final frontier contains invalid root position");
                                }

                                parentIndices[laneCount] = parentIndex;
                                rootRows[laneCount] = rootRow;
                                validLane[laneCount] = 1;
                                ++laneCount;
                            }

                            bool directContiguousBlock =
                                flatVector != nullptr &&
                                !probeHasNull &&
                                laneCount == kVecLanes;

                            if (directContiguousBlock) {
                                const int32_t firstRow = rootRows[0];
                                for (int32_t lane = 1;
                                     lane < laneCount;
                                     ++lane) {
                                    if (rootRows[lane] != firstRow + lane) {
                                        directContiguousBlock = false;
                                        break;
                                    }
                                }
                            }

                            KeyType *compareInput = gatheredKeys;
                            if (directContiguousBlock) {
                                compareInput = directProbeVector + rootRows[0];
                            } else {
                                for (int32_t lane = 0;
                                     lane < laneCount;
                                     ++lane) {
                                    KeyType value{};
                                    const bool valid =
                                        readKey(rootRows[lane], value);
                                    gatheredKeys[lane] = value;
                                    validLane[lane] = valid ? 1 : 0;
                                }

                                for (int32_t lane = laneCount;
                                     lane < kVecLanes;
                                     ++lane) {
                                    gatheredKeys[lane] = minValue;
                                    parentIndices[lane] = 0;
                                    rootRows[lane] = 0;
                                    validLane[lane] = 0;
                                }
                            }

                            if constexpr (std::is_same_v<KeyType, int16_t>) {
                                const int16x8_t vMin = vdupq_n_s16(minValue);
                                const int16x8_t vMax = vdupq_n_s16(maxValue);
                                CascadingInt16ArrayCompareNeon(
                                    compareInput,
                                    hashes,
                                    matches,
                                    vMin,
                                    vMax,
                                    minValue,
                                    maxValue);
                            } else if constexpr (
                                std::is_same_v<KeyType, int32_t>) {
                                const int32x4_t vMin = vdupq_n_s32(minValue);
                                const int32x4_t vMax = vdupq_n_s32(maxValue);
                                CascadingInt32ArrayCompareNeon(
                                    compareInput,
                                    hashes,
                                    matches,
                                    vMin,
                                    vMax,
                                    minValue,
                                    maxValue);
                            } else {
                                const int64x2_t vMin = vdupq_n_s64(minValue);
                                const int64x2_t vMax = vdupq_n_s64(maxValue);
                                CascadingInt64ArrayCompareNeon(
                                    compareInput,
                                    hashes,
                                    matches,
                                    vMin,
                                    vMax,
                                    minValue,
                                    maxValue);
                            }

                            for (int32_t lane = 0;
                                 lane < laneCount;
                                 ++lane) {
                                const uint32_t parentIndex =
                                    parentIndices[lane];

                                // Null/miss parents are consumed completely.
                                if (!validLane[lane] || !matches[lane]) {
                                    finalProbeParentIndex =
                                        static_cast<size_t>(parentIndex) + 1;
                                    finalProbeMatchOffset = 0;
                                    continue;
                                }

                                const KeyType hash = hashes[lane];
                                if (hash < 0) {
                                    finalProbeParentIndex =
                                        static_cast<size_t>(parentIndex) + 1;
                                    finalProbeMatchOffset = 0;
                                    continue;
                                }

                                const size_t slotIndex =
                                    static_cast<size_t>(hash);
                                if (!isAssigned[slotIndex]) {
                                    finalProbeParentIndex =
                                        static_cast<size_t>(parentIndex) + 1;
                                    finalProbeMatchOffset = 0;
                                    continue;
                                }

                                auto it = arrayTable
                                    ->TransformPtr(slots[slotIndex])
                                    ->Begin();

                                uint32_t ordinal = 0;
                                const uint32_t skip =
                                    parentIndex == resumeParentIndex
                                        ? resumeMatchOffset
                                        : 0;
                                while (it.IsOk() && ordinal < skip) {
                                    ++it;
                                    ++ordinal;
                                }

                                while (it.IsOk()) {
                                    if (outputBuilder->IsFull()) {
                                        finalProbeParentIndex = parentIndex;
                                        finalProbeMatchOffset = ordinal;
                                        return;
                                    }

                                    const CascadingBuildAddress address{
                                        static_cast<uint32_t>(it->rowIdx),
                                        static_cast<uint32_t>(it->vecBatchIdx)};

                                    if (!AppendDirectFinalMatch(
                                            finalLevel,
                                            parentIndex,
                                            address)) {
                                        finalProbeParentIndex = parentIndex;
                                        finalProbeMatchOffset = ordinal;
                                        return;
                                    }

                                    ++it;
                                    ++ordinal;
                                }

                                finalProbeParentIndex =
                                    static_cast<size_t>(parentIndex) + 1;
                                finalProbeMatchOffset = 0;
                            }
                        }

                        finalProbeParentIndex = inputFrontier.size();
                        finalProbeMatchOffset = 0;
                        finalProbeComplete = true;
                        return;
                    }
                }
            }
        }
    }
#endif
    // unsupported encodings/types, non-ArrayTable hash tables and non-ARM.
    bool levelHasAnyNull = false;
    for (size_t key = 0; key < hashColCount; ++key) {
        if (probeColumns[key]->HasNull()) {
            levelHasAnyNull = true;
            break;
        }
    }

    for (size_t parentCursor = finalProbeParentIndex;
         parentCursor < inputFrontier.size();
         ++parentCursor) {
        const uint32_t parentIndex =
            static_cast<uint32_t>(parentCursor);
        const CascadingFrontierEntry & parent =
            inputFrontier[parentIndex];
        const int32_t probePosition = parent.rootProbePosition;

        if (probePosition < 0 || probePosition >= rootRowCount) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading direct-final frontier contains invalid root position");
        }

        if (levelHasAnyNull) {
            bool hasNull = false;
            for (size_t key = 0; key < hashColCount; ++key) {
                if (probeColumns[key]->IsNull(probePosition)) {
                    hasNull = true;
                    break;
                }
            }
            if (hasNull) {
                finalProbeParentIndex = parentCursor + 1;
                finalProbeMatchOffset = 0;
                continue;
            }
        }

        auto result = table.Find(
            serializers,
            context,
            probeColumns.data(),
            hashColCount,
            probePosition,
            partition);

        if (!result.IsInsert()) {
            finalProbeParentIndex = parentCursor + 1;
            finalProbeMatchOffset = 0;
            continue;
        }

        auto it = result.GetValue()->Begin();
        uint32_t ordinal = 0;
        const uint32_t skip =
            parentCursor == resumeParentIndex
                ? resumeMatchOffset
                : 0;
        while (it.IsOk() && ordinal < skip) {
            ++it;
            ++ordinal;
        }

        while (it.IsOk()) {
            if (outputBuilder->IsFull()) {
                finalProbeParentIndex = parentCursor;
                finalProbeMatchOffset = ordinal;
                return;
            }

            const CascadingBuildAddress address{
                static_cast<uint32_t>(it->rowIdx),
                static_cast<uint32_t>(it->vecBatchIdx)};
            if (!AppendDirectFinalMatch(
                    finalLevel,
                    parentIndex,
                    address)) {
                finalProbeParentIndex = parentCursor;
                finalProbeMatchOffset = ordinal;
                return;
            }

            ++it;
            ++ordinal;
        }

        finalProbeParentIndex = parentCursor + 1;
        finalProbeMatchOffset = 0;
    }

    finalProbeParentIndex = inputFrontier.size();
    finalProbeMatchOffset = 0;
    finalProbeComplete = true;
}

void CascadingLookupJoinOperator::ProbeFinalLevelDirect()
{
    if (cascadeLevels < 2 || finalProbeComplete || curInputBatch == nullptr) {
        return;
    }

    const int32_t finalLevel = cascadeLevels - 1;
    const auto & inputFrontier =
        frontiers[static_cast<size_t>(finalLevel - 1)];

    if (inputFrontier.empty()) {
        finalProbeParentIndex = 0;
        finalProbeMatchOffset = 0;
        finalProbeComplete = true;
        return;
    }

    VisitHashTableVariant(
        hashTablesList[static_cast<size_t>(finalLevel)],
        finalLevel,
        "ProbeFinalLevelDirect",
        [&](auto & table) {
            using TableType = std::decay_t<decltype(table)>;
            if constexpr (IsJoinHashTableVariants<TableType>::value) {
                ProbeFinalLevelDirectWithTable(
                    finalLevel,
                    table,
                    inputFrontier);
            } else {
                throw OmniException(
                    "NOT_SUPPORTED",
                    "CascadingJoin only supports JoinHashTableVariants");
            }
        });
}
void CascadingLookupJoinOperator::BuildAllFrontiers()
{
    for (auto & frontier : frontiers) {
        frontier.clear();
    }

    finalProbeParentIndex = 0;
    finalProbeMatchOffset = 0;
    finalProbeComplete = false;
    finalFrontierEmitIndex = 0;

    if (cascadeLevels <= 0 || curInputBatch == nullptr) {
        frontiersBuiltForCurrentInput = true;
        finalProbeComplete = true;
        curProbePosition =
            curInputBatch == nullptr
                ? 0
                : curInputBatch->GetRowCount();
        return;
    }

    // Keep the existing correctness guard: this implementation still expects
    // one physical hash table per level.
    for (int32_t level = 0; level < cascadeLevels; ++level) {
        if (!levelIsSingleHT[static_cast<size_t>(level)]) {
            frontiersBuiltForCurrentInput = true;
            finalProbeComplete = true;
            curProbePosition = curInputBatch->GetRowCount();
            return;
        }
    }

    // A single-level CascadingJoin keeps the old path because there is no
    // intermediate parent frontier to resume from.
    {
        VisitHashTableVariant(
            hashTablesList[0],
            0,
            "BuildAllFrontiers-level0",
            [&](auto & table) {
                using TableType =
                    std::decay_t<decltype(table)>;

                if constexpr (
                    IsJoinHashTableVariants<TableType>::value) {

                    if (level0HasAnyNull) {
                        ProbeLevel0WithTable<true>(table);
                    } else {
                        ProbeLevel0WithTable<false>(table);
                    }

                } else {
                    throw OmniException(
                        "NOT_SUPPORTED",
                        "CascadingJoin only supports JoinHashTableVariants");
                }
            });
    }

    if (cascadeLevels == 1) {
        frontiersBuiltForCurrentInput = true;
        finalProbeComplete = true;
        finalFrontierEmitIndex = 0;
        curProbePosition = curInputBatch->GetRowCount();
        return;
    }

    // Materialize only intermediate levels [1, N-2].  The final level N-1 is
    // deliberately omitted and will probe directly into outputBuilder.
    for (int32_t level = 1;
         level < cascadeLevels - 1;
         ++level) {
        const auto & inputFrontier =
            frontiers[static_cast<size_t>(level - 1)];
        auto & outputFrontier =
            frontiers[static_cast<size_t>(level)];

        if (inputFrontier.empty()) {
            outputFrontier.clear();
            break;
        }

        VisitHashTableVariant(
            hashTablesList[static_cast<size_t>(level)],
            level,
            "BuildAllFrontiers-intermediate",
            [&](auto & table) {
                using TableType = std::decay_t<decltype(table)>;
                if constexpr (IsJoinHashTableVariants<TableType>::value) {
                    ProbeFrontierLevelWithTable(
                        level,
                        table,
                        inputFrontier,
                        outputFrontier);
                } else {
                    throw OmniException(
                        "NOT_SUPPORTED",
                        "CascadingJoin only supports JoinHashTableVariants");
                }
            });
    }

    frontiersBuiltForCurrentInput = true;
    finalFrontierEmitIndex = 0;
    finalProbeParentIndex = 0;
    finalProbeMatchOffset = 0;

    const auto & finalInputFrontier =
        frontiers[static_cast<size_t>(cascadeLevels - 2)];
    finalProbeComplete = finalInputFrontier.empty();
    curProbePosition = curInputBatch->GetRowCount();
}

void CascadingLookupJoinOperator::EmitFinalFrontier()
{
    if (!frontiersBuiltForCurrentInput ||
        cascadeLevels <= 0 ||
        frontiers.empty()) {
        return;
    }

    const auto & finalFrontier = frontiers.back();

    while (finalFrontierEmitIndex < finalFrontier.size() &&
           !outputBuilder->IsFull()) {
        const size_t finalIndex = finalFrontierEmitIndex;
        const CascadingFrontierEntry *entry =
            &finalFrontier[finalIndex];

        uint32_t currentIndex =
            static_cast<uint32_t>(finalIndex);
        bool validChain = true;

        for (int32_t level = cascadeLevels - 1;
             level >= 0;
             --level) {
            const auto & levelFrontier =
                frontiers[static_cast<size_t>(level)];

            if (currentIndex >= levelFrontier.size()) {
                validChain = false;
                break;
            }

            entry = &levelFrontier[currentIndex];
            reusableMatchChain[static_cast<size_t>(level)] =
                entry->address;

            if (level > 0) {
                currentIndex = entry->parentIndex;
            }
        }

        if (validChain) {
            outputBuilder->AppendMatchChain(
                finalFrontier[finalIndex].rootProbePosition,
                reusableMatchChain);
        }

        ++finalFrontierEmitIndex;
    }
}

void CascadingLookupJoinOperator::ResetFrontierState()
{
    for (auto & frontier : frontiers) {
        frontier.clear();
    }

    groupedParentIndices.clear();
    batchCounts.clear();
    batchOffsets.clear();
    batchWriteOffsets.clear();

    frontiersBuiltForCurrentInput = false;
    finalFrontierEmitIndex = 0;
    finalProbeParentIndex = 0;
    finalProbeMatchOffset = 0;
    finalProbeComplete = false;
}

void CascadingLookupJoinOperator::ProcessProbe()
{
    if (!frontiersBuiltForCurrentInput) {
        BuildAllFrontiers();
    }

    if (cascadeLevels <= 1) {
        EmitFinalFrontier();
        return;
    }

    ProbeFinalLevelDirect();
}

int32_t CascadingLookupJoinOperator::AddInput(VectorBatch *vecBatch)
{
    if (vecBatch == nullptr) {
        return 0;
    }

    if (vecBatch->GetRowCount() <= 0) {
        VectorHelper::FreeVecBatch(vecBatch);
        ResetInputVecBatch();
        SetStatus(OMNI_STATUS_NORMAL);
        return 0;
    }

    UpdateAddInputInfo(vecBatch->GetRowCount());

    if (firstVecBatch) {
        firstVecBatch = false;
        InitFirst();
    }

    // The benchmark uses AFTER_EACH_INPUT_FINISHED, so the previous input is
    // fully consumed before the next AddInput(). Keep the same operator contract
    // as the original implementation and avoid introducing a project-specific
    // error status here.
    curInputBatch = vecBatch;
    curProbePosition = 0;
    ResetFrontierState();

    const size_t inputRows =
        static_cast<size_t>(vecBatch->GetRowCount());
    for (auto & frontier : frontiers) {
        if (frontier.capacity() < inputRows) {
            frontier.reserve(inputRows);
        }
    }
    if (groupedParentIndices.capacity() < inputRows) {
        groupedParentIndices.reserve(inputRows);
    }

    PrepareCurrentProbe();

    SetStatus(OMNI_STATUS_NORMAL);
    return 0;
}

int32_t CascadingLookupJoinOperator::GetOutput(
    VectorBatch ** outputVecBatch)
{
    *outputVecBatch = nullptr;

    if (curInputBatch == nullptr) {
        if (noMoreInput_) {
            SetStatus(OMNI_STATUS_FINISHED);
        }
        UpdateGetOutputInfo(0);
        return 0;
    }

    if (probeTypesList.empty()) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading root probe schema is unavailable");
    }

    const int32_t rootProbeColumnCount =
        probeTypesList[0].GetSize();

    if (curInputBatch->GetVectorCount() !=
        rootProbeColumnCount) {
        throw OmniException(
            "INVALID_STATE",
            "Cascading root input VectorBatch column count does not match planned schema");
    }

    for (int32_t column = 0;
         column < rootProbeColumnCount;
         ++column) {
        if (curInputBatch->Get(column) == nullptr) {
            throw OmniException(
                "INVALID_STATE",
                "Cascading root input VectorBatch contains null column");
        }
    }

    const bool hasPendingFinalRows =
        frontiersBuiltForCurrentInput &&
        (cascadeLevels <= 1
             ? (cascadeLevels > 0 &&
                finalFrontierEmitIndex < frontiers.back().size())
             : !finalProbeComplete);

    bool processedProbe = false;

    if (!frontiersBuiltForCurrentInput || hasPendingFinalRows) {
        ProcessProbe();
        processedProbe = true;
    }

    if (!outputBuilder->IsEmpty()) {
        outputBuilder->BuildFinalOutput(
            curInputBatch->GetVectors(),
            levelBuildColumns,
            probeOutputCols,
            buildOutputColsList,
            outputVecBatch);

        outputBuilder->Clear();
    }

    if (processedProbe) {
        for (auto & context : levelExecutionContexts) {
            context->GetArena()->Reset();
        }
    }

    if (*outputVecBatch != nullptr) {
        int32_t expectedOutputColumnCount =
            rootProbeColumnCount;

        for (int32_t level = 0;
             level < cascadeLevels;
             ++level) {
            expectedOutputColumnCount +=
                static_cast<int32_t>(
                    buildOutputColsList[static_cast<size_t>(level)].size());
        }

        if ((*outputVecBatch)->GetVectorCount() !=
            expectedOutputColumnCount) {
            VectorHelper::FreeVecBatch(*outputVecBatch);
            *outputVecBatch = nullptr;

            throw OmniException(
                "INVALID_STATE",
                "Cascading physical output column count does not match planned schema");
        }

        for (int32_t column = 0;
             column < expectedOutputColumnCount;
             ++column) {
            if ((*outputVecBatch)->Get(column) == nullptr) {
                VectorHelper::FreeVecBatch(*outputVecBatch);
                *outputVecBatch = nullptr;

                throw OmniException(
                    "INVALID_STATE",
                    "Cascading physical output contains null column");
            }
        }
    }

    const bool allFinalRowsEmitted =
        frontiersBuiltForCurrentInput &&
        (cascadeLevels <= 0 ||
         (cascadeLevels == 1
              ? (frontiers.empty() ||
                 finalFrontierEmitIndex >= frontiers.back().size())
              : finalProbeComplete));

    if (allFinalRowsEmitted && outputBuilder->IsEmpty()) {
        VectorHelper::FreeVecBatch(curInputBatch);
        curInputBatch = nullptr;
        curProbePosition = 0;
        ResetFrontierState();

        if (noMoreInput_) {
            SetStatus(OMNI_STATUS_FINISHED);
        }
    }

    if (*outputVecBatch != nullptr) {
        UpdateGetOutputInfo(
            (*outputVecBatch)->GetRowCount());
    } else {
        UpdateGetOutputInfo(0);
    }

    return 0;
}

OmniStatus CascadingLookupJoinOperator::Close()
{
    if (curInputBatch != nullptr) {
        VectorHelper::FreeVecBatch(curInputBatch);
        curInputBatch = nullptr;
    }

    ResetFrontierState();
    UpdateCloseInfo();
    return OMNI_STATUS_NORMAL;
}

BlockingReason CascadingLookupJoinOperator::IsBlocked(
    ContinueFuture *future)
{
    (void)future;

    for (int32_t level = 0;
         level < cascadeLevels;
         ++level) {
        const OmniStatus tableStatus = VisitHashTableVariant(
            hashTablesList[static_cast<size_t>(level)],
            level,
            "IsBlocked",
            [&](auto && arg) {
                return arg.GetStatus();
            });

        if (tableStatus ==
            OmniStatus::OMNI_STATUS_NORMAL) {
            return BlockingReason::kWaitForJoinBuild;
        }
    }

    return BlockingReason::kNotBlocked;
}

} // end of omniruntime
