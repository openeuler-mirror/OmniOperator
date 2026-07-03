/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: TAPER join hash table variants — skeleton for speculative probe framework
 */

#pragma once

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "operator/status.h"
#include "type/data_types.h"
#include "common_join.h"
#include "operator/hashmap/column_marshaller.h"
#include "operator/hashmap/array_map.h"
#include "operator/execution_context.h"
#include "operator/join/taper_join_handler.h"
#include "vector/decoded_vector.h"

namespace omniruntime {
namespace vec {
class VectorBatch;
class BaseVector;
}  // namespace vec

namespace op {

// TAPER hash table variant that mirrors JoinHashTableVariants in the
// non-TAPER path. Provides the same operator-visible interface but all
// implementations are empty stubs.
template <typename KeyType, bool NeedVisited>
class TaperJoinHashTableVariants {
public:
    using Key = KeyType;
    static constexpr bool IS_SIMPLE_KEY = false;

    TaperJoinHashTableVariants(int32_t operatorCount, DataTypes* buildTypes,
        const std::vector<int32_t>& buildHashCols, JoinType joinType, BuildSide buildSide)
        : buildTypes_(buildTypes), buildHashCols_(buildHashCols),
          tableCount_(static_cast<uint32_t>(operatorCount)),
          joinType_(joinType), buildSide_(buildSide),
          handlers_(tableCount_),
          executionContexts_(tableCount_),
          inputVecBatches_(std::vector<std::vector<vec::VectorBatch*>>(tableCount_)),
          totalRowCount_(std::vector<size_t>(tableCount_)) {
        for (uint32_t i = 0; i < tableCount_; ++i) {
            executionContexts_[i] = std::make_unique<ExecutionContext>();
        }
        // Store all build columns (keys + output + filter are subsets)
        int numBuildCols = buildTypes_->GetSize();
        taperStoredColIndices_.reserve(numBuildCols);
        for (int i = 0; i < numBuildCols; ++i) {
            taperStoredColIndices_.push_back(i);
        }
        // Multi-column key: compute pack plan (bit-widths + masks per key column)
        if (buildHashCols_.size() > 1) {
            isPacked_ = true;
            bitWidths_.reserve(buildHashCols_.size());
            masks_.reserve(buildHashCols_.size());
            for (size_t i = 0; i < buildHashCols_.size(); ++i) {
                auto typeId = buildTypes_->GetIds()[buildHashCols_[i]];
                auto w = PackedBitWidth(typeId);
                bitWidths_.push_back(w);
                if (w >= static_cast<decltype(w)>(sizeof(KeyType) * 8)) {
                    masks_.push_back(static_cast<KeyType>(~static_cast<KeyType>(0)));
                } else {
                    masks_.push_back((static_cast<KeyType>(1) << w) - 1);
                }
            }
        }
    }
    ~TaperJoinHashTableVariants() = default;

    /// Bit-width for packable types; returns 0 for unsupported (varchar etc.).
    static uint8_t PackedBitWidth(int32_t typeId) {
        switch (typeId) {
            case OMNI_BYTE:   case OMNI_BOOLEAN: return 8;
            case OMNI_SHORT:  return 16;
            case OMNI_INT:    case OMNI_DATE32: case OMNI_TIME32: return 32;
            case OMNI_LONG:   case OMNI_TIMESTAMP: case OMNI_DECIMAL64:
            case OMNI_DATE64: case OMNI_TIME64: return 64;
            default: return 0;
        }
    }

    // --- Metadata / accessors -------------------------------------------------

    uint32_t GetHashTableCount() const { return tableCount_; }
    uint32_t GetHashTableSize() const { return 0; } //TODO

    // Combined stub that satisfies both array-table and hash-table interfaces.
    struct TaperArrayHashStub {
        uint32_t GetElementsSize() const { return 0; }
        template <typename F> void ForEachValue(F&&) {}
        struct HashmapStub { template <typename F> void ForEachValue(F&&) {} };
        HashmapStub hashmap;
    };
    TaperArrayHashStub* GetHashTable(int32_t /*partitionIndex*/) { static TaperArrayHashStub st; return &st; }

    DataTypes* GetBuildDataTypes() { return buildTypes_; }
    void SetProbeTypes(DataTypes* /*probeDataTypes*/) {}

    HashTableImplementationType GetHashTableTypes(int /*partitionIndex*/) const {
        return HashTableImplementationType::NORMAL_HASH_TABLE;
    }
    bool GetIsMultiCols() const { return false; }
    JoinType GetJoinType() const { return joinType_; }
    BuildSide GetBuildSide() const { return buildSide_; }
    OmniStatus GetStatus() const { return status_; }
    void SetStatus(OmniStatus s) { status_ = s; }

    // --- Column data ----------------------------------------------------------

    omniruntime::vec::BaseVector*** GetColumns(int /*partitionIndex*/) const { return nullptr; }  //TODO

    void SetTaperStoredColumns(const std::vector<int32_t>& indices) {
        taperStoredColIndices_ = indices;
    }

    // --- Build pipeline -------------------------------------------------------

    void AddVecBatch(int partitionIndex, omniruntime::vec::VectorBatch* vecBatch) {
        inputVecBatches_[partitionIndex].push_back(vecBatch);
        totalRowCount_[partitionIndex] += vecBatch->GetRowCount();
    }
    void Prepare(int partitionIndex) {
        auto& vecBatches = inputVecBatches_[partitionIndex];
        for (size_t i = 0; i < vecBatches.size(); ++i) {
            CollectTaperRows(partitionIndex, vecBatches[i], static_cast<uint32_t>(i));
        }
    }
    void BuildHashTable(int partitionIndex) {
        BuildTaperHashTable(partitionIndex);
    }

    // --- TAPER-specific build pipeline -----------------------------------------

    void CollectTaperRows(int32_t partitionIndex, vec::VectorBatch* vecBatch, uint32_t batchIdx);
    void BuildTaperHashTable(int32_t partitionIndex);

    // --- Find / probe ---------------------------------------------------------

    // 4-param Find: TAPER probe — decode key column(s) and call handler.
    char* Find(omniruntime::vec::BaseVector** probeHashColumns, int32_t probeHashColCount,
               int32_t probePosition, uint32_t partition) {
        auto& handler = handlers_[partition];
        if (!handler) return nullptr;
        if (isPacked_) {
            // Multi-column: decode each probe key column and bit-pack
            KeyType packed = 0;
            for (int32_t k = 0; k < probeHashColCount; ++k) {
                vec::DecodedVector dv;
                dv.Decode(probeHashColumns[k], probeHashColumns[k]->GetSize());
                bool isNull = dv.IsNull(probePosition);
                packed = (packed << 1) | static_cast<KeyType>(isNull ? 1 : 0);
                KeyType val = 0;
                if (!isNull) {
                    switch (bitWidths_[k]) {
                        case 8:  val = static_cast<KeyType>(dv.GetValue<int8_t>(probePosition)); break;
                        case 16: val = static_cast<KeyType>(dv.GetValue<int16_t>(probePosition)); break;
                        case 32: val = static_cast<KeyType>(dv.GetValue<int32_t>(probePosition)); break;
                        case 64: val = static_cast<KeyType>(dv.GetValue<int64_t>(probePosition)); break;
                        default: return nullptr;
                    }
                    val &= masks_[k];
                }
                packed = (packed << bitWidths_[k]) | val;
            }
            return handler->Find(packed);
        }
        // Single column: original logic
        auto* vec = probeHashColumns[0];
        vec::DecodedVector dv;
        dv.Decode(vec, vec->GetSize());
        if (dv.IsNull(probePosition)) return nullptr;
        KeyType key = dv.GetValue<KeyType>(probePosition);
        return handler->Find(key);
    }


    // --- SIMD helpers ---------------------------------------------------------

    bool CanProbeSIMD(omniruntime::vec::BaseVector** /*cols*/, int /*cnt*/, uint32_t /*partition*/) {
        return false;
    } //TODO
    KeyType* GetSingleProbeHashKeyBase(omniruntime::vec::BaseVector** /*cols*/) { return nullptr; } //TODO
    KeyType GetKeyValue(omniruntime::vec::BaseVector**, int32_t) const { return KeyType{}; } //TODO

    // --- Array-map helpers ----------------------------------------------------

    TaperArrayHashStub* GetArrayTable(int /*idx*/) {
        static TaperArrayHashStub stub;
        return &stub;
    }
    std::pair<int64_t, int64_t> GetmaxMinValue(int /*partitionIndex*/) { return {0, 0}; } //TODO

    // --- Build filter support -------------------------------------------------

    void InitBuildFilterCols(std::vector<int32_t>& /*buildFilterCols*/,
                             int /*probeColCount*/,
                             std::vector<std::vector<omniruntime::vec::BaseVector**>>& /*tableBfColPtrs*/) {}

    // --- Visited tracking -----------------------------------------------------

    uint32_t GetVisitedCounts() const { return 0; } //TODO
    uint32_t GetTotalVisitedCounts() const { return 0; } //TODO
    void SetTotalVisitedCounts(int /*cnt*/) {} //TODO

    // --- Iteration ------------------------------------------------------------

    template <typename F>
    void ForEachValue(F&& /*fn*/) {} //TODO

    // --- TAPER-specific accessors ---------------------------------------------

    RowContainer* GetTaperRowContainer(int idx) { 
        return idx >= 0 && static_cast<size_t>(idx) < handlers_.size() && handlers_[idx] 
            ? handlers_[idx]->Rows() : nullptr; 
    }
    const std::vector<int32_t>& GetTaperStoredColIndices() const { return taperStoredColIndices_; }
    const std::vector<int32_t>& GetBuildHashCols() const { return buildHashCols_; }

    // --- Key type for template dispatch ---------------------------------------
    KeyType keyType{};

private:
    DataTypes* buildTypes_ = nullptr;
    std::vector<int32_t> buildHashCols_;
    std::vector<int32_t> buildOutputCols;
    std::vector<int32_t> buildFilterCols;
    std::vector<int32_t> taperStoredColIndices_;
    std::vector<omniruntime::vec::BaseVector***> columns_;
    uint32_t tableCount_ = 0;
    JoinType joinType_ = OMNI_JOIN_TYPE_INNER;
    BuildSide buildSide_ = OMNI_BUILD_UNKNOWN;
    OmniStatus status_ = OmniStatus::OMNI_STATUS_NORMAL;
    std::vector<std::unique_ptr<TaperJoinFixedHandler<KeyType, NeedVisited>>> handlers_;
    std::vector<std::unique_ptr<ExecutionContext>> executionContexts_;
    std::vector<std::vector<omniruntime::vec::VectorBatch*>> inputVecBatches_;
    std::vector<size_t> totalRowCount_;

    // Multi-column key bit-packing support (same as agg packed mode)
    bool isPacked_ = false;
    std::vector<uint8_t> bitWidths_;
    std::vector<KeyType> masks_;
};

// --- Out-of-line implementations ----------------------------------------------

template <typename KeyType, bool NeedVisited>
void TaperJoinHashTableVariants<KeyType, NeedVisited>::CollectTaperRows(
    int32_t partitionIndex, vec::VectorBatch* vecBatch, uint32_t batchIdx) {
    auto rowCount = vecBatch->GetRowCount();
    if (rowCount <= 0) return;

    auto& storedCols = taperStoredColIndices_.empty() ? buildHashCols_ : taperStoredColIndices_;
    int storedColNum = static_cast<int>(storedCols.size());

    // Lazy-init handler
    if (!handlers_[partitionIndex]) {
        std::vector<bool> isVar(storedColNum, false);
        std::vector<int32_t> typeIds(storedColNum);
        std::vector<int32_t> varcharCols;
        for (int c = 0; c < storedColNum; ++c) {
            typeIds[c] = buildTypes_->GetIds()[storedCols[c]];
        }
        std::vector<int32_t> keySizes(storedColNum);
        for (int c = 0; c < storedColNum; ++c) {
            switch (typeIds[c]) {
                case type::OMNI_BYTE: case type::OMNI_BOOLEAN: keySizes[c] = 1; break;
                case type::OMNI_SHORT: keySizes[c] = 2; break;
                case type::OMNI_INT: case type::OMNI_DATE32: case type::OMNI_FLOAT:
                case type::OMNI_TIME32: keySizes[c] = 4; break;
                case type::OMNI_LONG: case type::OMNI_TIMESTAMP: case type::OMNI_DECIMAL64:
                case type::OMNI_DOUBLE: case type::OMNI_DATE64: case type::OMNI_TIME64:
                    keySizes[c] = 8; break;
                default: keySizes[c] = sizeof(KeyType); break;
            }
        }
        auto* ctx = executionContexts_[partitionIndex].get();
        handlers_[partitionIndex] = std::make_unique<TaperJoinFixedHandler<KeyType, NeedVisited>>(
            *ctx->GetArena(), 0);
        handlers_[partitionIndex]->InitRowContainer(keySizes, isVar, typeIds, varcharCols);
    }

    auto* handler = handlers_[partitionIndex].get();
    // Decode and append each row
    std::vector<vec::DecodedVector> decoded(storedColNum);
    for (int c = 0; c < storedColNum; ++c) {
        int colIdx = storedCols[c];
        auto* colVec = vecBatch->Get(colIdx);
        decoded[c].Decode(colVec, rowCount);
    }
    for (int32_t r = 0; r < rowCount; ++r) {
        handler->AppendRow(decoded.data(), r, batchIdx);
    }
}

template <typename KeyType, bool NeedVisited>
void TaperJoinHashTableVariants<KeyType, NeedVisited>::BuildTaperHashTable(int32_t partitionIndex) {
    auto* handler = handlers_[partitionIndex].get();
    if (!handler) return;
    auto* rc = handler->Rows();
    if (!rc) return;

    constexpr uint32_t BLOCK_SIZE = 1024;
    KeyType keys[BLOCK_SIZE];
    char* rows[BLOCK_SIZE];
    RowContainerIterator it;
    int32_t n;
    while ((n = rc->ListRows(&it, BLOCK_SIZE, rows)) > 0) {
        if (isPacked_) {
            for (int32_t i = 0; i < n; ++i) {
                KeyType packed = 0;
                for (size_t k = 0; k < buildHashCols_.size(); ++k) {
                    int32_t colIdx = buildHashCols_[k];
                    auto col = rc->ColumnAt(colIdx);
                    bool isNull = RowContainer::IsNullAt(rows[i], col.NullByte(), col.NullMask());
                    packed = (packed << 1) | static_cast<KeyType>(isNull ? 1 : 0);
                    KeyType val = 0;
                    if (!isNull) {
                        switch (bitWidths_[k]) {
                            case 8:
                                val = static_cast<KeyType>(
                                    RowContainer::ReadValue<int8_t>(rows[i], col.Offset())); break;
                            case 16:
                                val = static_cast<KeyType>(
                                    RowContainer::ReadValue<int16_t>(rows[i], col.Offset())); break;
                            case 32:
                                val = static_cast<KeyType>(
                                    RowContainer::ReadValue<int32_t>(rows[i], col.Offset())); break;
                            case 64:
                                val = static_cast<KeyType>(
                                    RowContainer::ReadValue<int64_t>(rows[i], col.Offset())); break;
                        }
                        val &= masks_[k];
                    }
                    packed = (packed << bitWidths_[k]) | val;
                }
                keys[i] = packed;
            }
        } else {
            for (int32_t i = 0; i < n; ++i) {
                keys[i] = RowContainer::ReadValue<KeyType>(rows[i], rc->ColumnAt(0).Offset());
            }
        }
        handler->template EmplaceTable<true>(keys, rows, n);
    }
}

}  // namespace op
}  // namespace omniruntime
