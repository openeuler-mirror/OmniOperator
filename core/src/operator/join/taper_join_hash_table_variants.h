/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: TAPER join hash table variants — skeleton for speculative probe framework
 */

#pragma once

#include <algorithm>
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
#include "operator/hash_util.h"
#include "operator/join/taper_join_handler.h"
#include "vector/decoded_vector.h"
#include "operator/hashmap/vector_analyzer.h"

namespace omniruntime {
namespace vec {
class VectorBatch;
class BaseVector;
}  // namespace vec

namespace op {

enum class HashTableImplementationType {
    NORMAL_HASH_TABLE,
    ARRAY_HASH_TABLE
};

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
            isMultiColumn_ = true;
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
            case OMNI_INT:    case OMNI_DATE32: case OMNI_TIME32:  case OMNI_FLOAT: return 32;
            case OMNI_LONG:   case OMNI_TIMESTAMP: case OMNI_DECIMAL64:  case OMNI_DOUBLE:
            case OMNI_DATE64: case OMNI_TIME64: return 64;
            default: return 0;
        }
    }

    // --- Metadata / accessors -------------------------------------------------

    uint32_t GetHashTableCount() const { return tableCount_; }
    uint32_t GetHashTableSize() const { return 0; } //TODO

    // Combined stub that satisfies both array-table and hash-table interfaces.
    struct TaperArrayHashStub {
        //TODO
        uint32_t GetElementsSize() const { return 0; }
        template <typename F> void ForEachValue(F&&) {}
        struct HashmapStub { template <typename F> void ForEachValue(F&&) {} };
        HashmapStub hashmap;
    };
    TaperArrayHashStub* GetHashTable(int32_t /*partitionIndex*/) { static TaperArrayHashStub st; return &st; }//TODO

    DataTypes* GetBuildDataTypes() { return buildTypes_; }
    void SetProbeTypes(DataTypes* /*probeDataTypes*/) {}//TODO

    HashTableImplementationType GetHashTableTypes(int /*partitionIndex*/) const {
        return HashTableImplementationType::NORMAL_HASH_TABLE;
    }//TODO
    bool GetIsMultiCols() const { return false; }//TODO
    JoinType GetJoinType() const { return joinType_; }
    BuildSide GetBuildSide() const { return buildSide_; }
    OmniStatus GetStatus() const { return status_; }
    void SetStatus(OmniStatus s) { status_ = s; }


    omniruntime::vec::BaseVector*** GetColumns(int /*partitionIndex*/) const { return nullptr; }  //TODO

    void SetTaperStoredColumns(const std::vector<int32_t>& indices) {
        taperStoredColIndices_ = indices;
    }

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
        if(isSer_){
            BuildTaperHashTableSerialized(partitionIndex);
        } else {
            BuildTaperHashTableFixed(partitionIndex);
        }   
    }

    void CollectTaperRows(int32_t partitionIndex, vec::VectorBatch* vecBatch, uint32_t batchIdx);
    void BuildTaperHashTableSerialized(int32_t partitionIndex);
    void BuildTaperHashTableFixed(int32_t partitionIndex);

    void FindBatch(
        int32_t probeStart,
        int32_t probeEnd,
        const std::vector<int8_t>& probeNulls,
        omniruntime::vec::BaseVector** probeHashColumns,
        int32_t probeHashColCount,
        bool singleHT,
        uint32_t partitionMask,
        const std::vector<int64_t>& probeHashes,
        std::vector<char*>& chainHeads) const {
        int32_t numRows = probeEnd - probeStart;
        if (isSer_) {
            std::vector<int64_t> rowHashes(numRows);
            std::vector<int32_t> positions(numRows);
            for (int32_t pos = probeStart; pos < probeEnd; ++pos) {
                int32_t idx = pos - probeStart;
                chainHeads[idx] = nullptr;
                if (probeNulls[pos]) continue;
                int64_t hash = 0;
                for (int32_t k = 0; k < probeHashColCount; ++k) {
                    vec::DecodedVector dv;
                    dv.Decode(probeHashColumns[k], probeHashColumns[k]->GetSize());
                    bool isNull = dv.IsNull(pos);
                    int64_t colHash;
                    if (isNull) {
                        colHash = kNullHash;
                    } else {
                        auto typeId = probeHashColumns[k]->GetTypeId();
                        auto w = PackedBitWidth(static_cast<int32_t>(typeId));
                        if (w == 0) {
                            auto* vec = dv.Base();
                            auto enc = vec->GetEncoding();
                            std::string_view sv;
                            if (enc == OMNI_ENCODING_CONST) {
                                sv = static_cast<ConstVector<std::string_view>*>(vec)->GetConstValue();
                            } else if (enc == OMNI_DICTIONARY) {
                                sv = dv.GetValue<std::string_view>(pos);
                            } else {
                                sv = static_cast<Vector<LargeStringContainer<std::string_view>>*>(vec)->GetValue(pos);
                            }
                            colHash = HashUtil::HashValue(
                                reinterpret_cast<int8_t*>(const_cast<char*>(sv.data())),
                                static_cast<int32_t>(sv.size()));
                        } else {
                            switch (w) {
                                case 8:
                                    colHash = HashUtil::HashValue(dv.GetValue<int8_t>(pos)); break;
                                case 16:
                                    colHash = HashUtil::HashValue(dv.GetValue<int16_t>(pos)); break;
                                case 32:
                                    colHash = HashUtil::HashValue(dv.GetValue<int32_t>(pos)); break;
                                case 64:
                                    colHash = HashUtil::HashValue(dv.GetValue<int64_t>(pos)); break;
                                default: continue;
                            }
                        }
                    }
                    hash = SerFastHashMix(hash, colHash);
                }
                rowHashes[idx] = hash;
                positions[idx] = pos;
            }
            if (numRows > 0 && serHandlers_[partitionMask]) {
                serHandlers_[partitionMask]->ProbeBatch(
                    rowHashes.data(), numRows,
                    probeHashColumns, probeHashColCount,
                    positions.data(), chainHeads.data());
            }
        } else {
            std::vector<Key> rowHashes(numRows);
            std::vector<int32_t> positions(numRows);
            if (isMultiColumn_) {
                std::vector<vec::DecodedVector> dv(probeHashColCount);
                for (int32_t k = 0; k < probeHashColCount; ++k) {
                    dv[k].Decode(probeHashColumns[k], probeHashColumns[k]->GetSize());
                }
                for (int32_t pos = probeStart; pos < probeEnd; ++pos) {
                    int32_t idx = pos - probeStart;
                    chainHeads[idx] = nullptr;
                    if (probeNulls[pos]) continue;
                    KeyType packed = 0;
                    for (int32_t k = 0; k < probeHashColCount; ++k) {
                        bool isNull = dv[k].IsNull(pos);
                        packed = (packed << 1) | isNull;
                        KeyType val = 0;
                        if (!isNull) {
                            switch (bitWidths_[k]) {
                                case 8:  val = static_cast<KeyType>(dv[k].GetValue<int8_t>(pos)); break;
                                case 16: val = static_cast<KeyType>(dv[k].GetValue<int16_t>(pos)); break;
                                case 32: val = static_cast<KeyType>(dv[k].GetValue<int32_t>(pos)); break;
                                case 64: val = static_cast<KeyType>(dv[k].GetValue<int64_t>(pos)); break;
                                default: continue;
                            }
                            val &= masks_[k];
                        }
                        packed = (packed << bitWidths_[k]) | val;
                    }
                    rowHashes[idx] = packed;
                    positions[idx] = pos;
                }
            }else {
                auto* vec = probeHashColumns[0];
                vec::DecodedVector dv;
                dv.Decode(vec, vec->GetSize());
                for (int32_t pos = probeStart; pos < probeEnd; ++pos) {
                    int32_t idx = pos - probeStart;
                    chainHeads[idx] = nullptr;
                    if (probeNulls[pos] || dv.IsNull(pos)) continue;
                    Key key = dv.GetValue<Key>(pos);
                    rowHashes[idx] = key;
                    positions[idx] = pos;
                }
            }
            if (numRows > 0 && handlers_[partitionMask]) {
                handlers_[partitionMask]->ProbeBatch(
                    rowHashes.data(), numRows,
                    probeHashColumns, probeHashColCount,
                    positions.data(), chainHeads.data());
            }
        }
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
        if (isSer_ && idx >= 0 && static_cast<size_t>(idx) < serHandlers_.size() && serHandlers_[idx])
            return serHandlers_[idx]->Rows();
        return idx >= 0 && static_cast<size_t>(idx) < handlers_.size() && handlers_[idx] 
            ? handlers_[idx]->Rows() : nullptr; 
    }
    const std::vector<int32_t>& GetTaperStoredColIndices() const { return taperStoredColIndices_; }
    const std::vector<int32_t>& GetBuildHashCols() const { return buildHashCols_; }

    void SetSerMode() { isSer_ = true; serHandlers_.resize(tableCount_); }
    bool IsSerMode() const { return isSer_; }

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
    std::vector<std::unique_ptr<TaperJoinSerializedHandler>> serHandlers_;
    std::vector<std::unique_ptr<ExecutionContext>> executionContexts_;
    std::vector<std::vector<omniruntime::vec::VectorBatch*>> inputVecBatches_;
    std::vector<size_t> totalRowCount_;

    // Multi-column key bit-packing support (same as agg packed mode)
    bool isMultiColumn_ = false;
    bool isSer_ = false;
    std::vector<uint8_t> bitWidths_;
    std::vector<KeyType> masks_;
};

// --- Out-of-line implementations ----------------------------------------------

inline uint64_t SerFastHashMix(uint64_t a, uint64_t b)
{
    return a ^ (b + 0x9e3779b97f4a7c15ULL + (a << 6) + (a >> 2));
}

template <typename KeyType, bool NeedVisited>
void TaperJoinHashTableVariants<KeyType, NeedVisited>::CollectTaperRows(
    int32_t partitionIndex, vec::VectorBatch* vecBatch, uint32_t batchIdx) {
    auto rowCount = vecBatch->GetRowCount();
    if (rowCount <= 0) return;

    auto& storedCols = taperStoredColIndices_.empty() ? buildHashCols_ : taperStoredColIndices_;
    int storedColNum = static_cast<int>(storedCols.size());

    // Lazy-init handler
    if ((isSer_ && !serHandlers_[partitionIndex]) || (!isSer_ && !handlers_[partitionIndex])) {
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
                case type::OMNI_VARCHAR: case type::OMNI_CHAR: case type::OMNI_VARBINARY:
                    keySizes[c] = sizeof(char*); break;
                default: keySizes[c] = sizeof(KeyType); break;
            }
        }
        std::vector<int32_t> keyColIndices;
        keyColIndices.reserve(buildHashCols_.size());
        for (int32_t buildCol : buildHashCols_) {
            auto it = std::find(storedCols.begin(), storedCols.end(), buildCol);
            if (it == storedCols.end()) {
                throw omniruntime::exception::OmniException("TAPER_NOT_SUPPORTED",
                    "Join key column not found in stored columns");
            }
            keyColIndices.push_back(static_cast<int32_t>(std::distance(storedCols.begin(), it)));
        }
        auto* ctx = executionContexts_[partitionIndex].get();
        if (isSer_) {
            serHandlers_[partitionIndex] = std::make_unique<TaperJoinSerializedHandler>(*ctx->GetArena());
            serHandlers_[partitionIndex]->InitRowContainer(keySizes, isVar, typeIds, varcharCols, keyColIndices);
        } else {
            handlers_[partitionIndex] = std::make_unique<TaperJoinFixedHandler<KeyType, NeedVisited>>(
                *ctx->GetArena(), 0);
            handlers_[partitionIndex]->InitRowContainer(keySizes, isVar, typeIds, varcharCols);
        }
    }

    // Decode and append each row
    std::vector<vec::DecodedVector> decoded(storedColNum);
    for (int c = 0; c < storedColNum; ++c) {
        int colIdx = storedCols[c];
        auto* colVec = vecBatch->Get(colIdx);
        decoded[c].Decode(colVec, rowCount);
    }
    for (int32_t r = 0; r < rowCount; ++r) {
        if (isSer_) {
            serHandlers_[partitionIndex]->AppendRow(decoded.data(), r, batchIdx);
        } else {
            handlers_[partitionIndex]->AppendRow(decoded.data(), r, batchIdx);
        }
    }
}

template <typename KeyType, bool NeedVisited>
void TaperJoinHashTableVariants<KeyType, NeedVisited>::BuildTaperHashTableSerialized(int32_t partitionIndex) {
    auto* serHandler = isSer_ ? serHandlers_[partitionIndex].get() : nullptr;
    if (!serHandler) {
            throw omniruntime::exception::OmniException("RUNTIME_ERROR", "TaperJoinSerializedHandler is null!");
        }
    auto* rc = serHandler->Rows();
    if (!rc) {
            throw omniruntime::exception::OmniException("RUNTIME_ERROR", "TaperJoinSerializedHandler RowContainer is null!");
    }   

    serHandler->ReserveTable(static_cast<size_t>(rc->NumRows()));
    constexpr uint32_t BLOCK_SIZE = 1024;
    KeyType keys[BLOCK_SIZE];
    char* rows[BLOCK_SIZE];
    RowContainerIterator it;
    int32_t n;
    while ((n = rc->ListRows(&it, BLOCK_SIZE, rows)) > 0) {
        for (int32_t i = 0; i < n; ++i) {
            int64_t hash = 0;
            for (size_t k = 0; k < buildHashCols_.size(); ++k) {
                int32_t colIdx = buildHashCols_[k];
                auto col = rc->ColumnAt(colIdx);
                bool isNull = RowContainer::IsNullAt(rows[i], col.NullByte(), col.NullMask());
                int64_t colHash;
                if (isNull) {
                    colHash = kNullHash;
                } else {
                    auto typeId = buildTypes_->GetIds()[colIdx];
                    int32_t colOff = col.Offset();
                    auto w = PackedBitWidth(typeId);
                    if (w == 0) {
                            auto* dataPtr = RowContainer::ReadValue<char*>(rows[i], colOff);
                        if (dataPtr != nullptr) {
                            uint8_t lenSize = static_cast<uint8_t>(dataPtr[0]);
                            uint32_t strLen = 0;
                            memcpy(&strLen, dataPtr + 1, lenSize);
                            colHash = HashUtil::HashValue(
                                reinterpret_cast<int8_t*>(const_cast<char*>(dataPtr + 1 + lenSize)),
                                static_cast<int32_t>(strLen));
                        } else {
                            colHash = kNullHash;
                        }
                    } else {
                        switch (w) {
                            case 8:
                                colHash = HashUtil::HashValue(
                                    RowContainer::ReadValue<int8_t>(rows[i], colOff)); break;
                            case 16:
                                colHash = HashUtil::HashValue(
                                    RowContainer::ReadValue<int16_t>(rows[i], colOff)); break;
                            case 32:
                                colHash = HashUtil::HashValue(
                                    RowContainer::ReadValue<int32_t>(rows[i], colOff)); break;
                            case 64:
                                colHash = HashUtil::HashValue(
                                    RowContainer::ReadValue<int64_t>(rows[i], colOff)); break;
                        }
                    }
                }
                hash = SerFastHashMix(hash, colHash);
            }
            keys[i] = static_cast<KeyType>(hash);
        }
        serHandler->EmplaceBatch(reinterpret_cast<const int64_t*>(keys), rows, n);
    }
}

template <typename KeyType, bool NeedVisited>
void TaperJoinHashTableVariants<KeyType, NeedVisited>::BuildTaperHashTableFixed(int32_t partitionIndex) {
    auto* handler = handlers_[partitionIndex].get();
    if (!handler) {
            throw omniruntime::exception::OmniException("RUNTIME_ERROR", "TaperJoinFixedHandler is null!");
        }
    auto* rc = handler->Rows();
    if (!rc) {
            throw omniruntime::exception::OmniException("RUNTIME_ERROR", "TaperJoinFixedHandler RowContainer is null!");
    }

    handler->ReserveTable(static_cast<size_t>(rc->NumRows()));
    constexpr uint32_t BLOCK_SIZE = 1024;
    KeyType keys[BLOCK_SIZE];
    char* rows[BLOCK_SIZE];
    RowContainerIterator it;
    int32_t n;
    while ((n = rc->ListRows(&it, BLOCK_SIZE, rows)) > 0) {
        if (isMultiColumn_) {
            for (int32_t i = 0; i < n; ++i) {
                KeyType packed = 0;
                for (size_t k = 0; k < buildHashCols_.size(); ++k) {
                    int32_t colIdx = buildHashCols_[k];
                    auto col = rc->ColumnAt(colIdx);
                    bool isNull = RowContainer::IsNullAt(rows[i], col.NullByte(), col.NullMask());
                    packed = (packed << 1) | isNull;
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
        handler->EmplaceBatch(keys, rows, n);
    }
}

}  // namespace op
}  // namespace omniruntime
