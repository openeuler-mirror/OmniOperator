/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: TAPER join handler — stubs for speculative probe framework
 */

#pragma once

#include <memory>
#include <cstdint>
#include <vector>

#include "operator/hashmap/taper_hashtable.h"
#include "operator/hashmap/row_container.h"
#include "memory/simple_arena_allocator.h"
#include "vector/decoded_vector.h"
#include "type/data_types.h"

namespace omniruntime {
namespace op {

// Fixed-width-key TAPER join handler.
// Maintains a TaperFlatHashTable + RowContainer pair for build data.
template <typename KeyType, bool NeedVisited>
class TaperJoinFixedHandler {
public:
    using Key = KeyType;
    using HashTable = TaperFlatHashTable<KeyType, false>;

    // Size in bytes of the per-row payload stored past key + null data.
    // Layout: [next: char*][visited: uint8_t]
    static constexpr int32_t kPayloadSize = sizeof(char*) + 1;

    TaperJoinFixedHandler() = default;
    TaperJoinFixedHandler(mem::SimpleArenaAllocator& pool, uint8_t /*initDegree*/)
        : arena_(&pool),
          table_(std::make_unique<HashTable>(pool, sizeof(KeyType), sizeof(char*))) {}
    ~TaperJoinFixedHandler() = default;

    void InitRowContainer(const std::vector<int32_t>& keyTypeSizes,
                          const std::vector<bool>& /*isVariableLen*/,
                          const std::vector<int32_t>& typeIds,
                          const std::vector<int32_t>& /*varcharCols*/) {
        numCols_ = static_cast<int32_t>(keyTypeSizes.size());
        typeIds_ = typeIds;
        rows_ = std::make_unique<RowContainer>(keyTypeSizes, numCols_, kPayloadSize, *arena_);
    }

    /// Append a decoded row to the RowContainer, storing key column data and payload.
    char* AppendRow(omniruntime::vec::DecodedVector* decodedCols, int32_t rowIdx, uint32_t vecBatchIdx) {
        char* row = rows_->NewRow();

        // Store all columns (keys + output + filter)
        for (int32_t c = 0; c < numCols_; ++c) {
            int32_t typeId = typeIds_[c];
            auto col = rows_->ColumnAt(c);
            auto offset = col.Offset();
            bool isNull = decodedCols[c].IsNull(rowIdx);
            if (isNull) {
                RowContainer::SetNullAt(row, col.NullByte(), col.NullMask());
                continue;
            }
            switch (typeId) {
                case type::OMNI_BYTE:
                case type::OMNI_BOOLEAN:
                    RowContainer::StoreValue<int8_t>(row, offset, decodedCols[c].GetValue<int8_t>(rowIdx));
                    break;
                case type::OMNI_SHORT:
                    RowContainer::StoreValue<int16_t>(row, offset, decodedCols[c].GetValue<int16_t>(rowIdx));
                    break;
                case type::OMNI_INT:
                case type::OMNI_DATE32:
                case type::OMNI_FLOAT:
                    RowContainer::StoreValue<int32_t>(row, offset, decodedCols[c].GetValue<int32_t>(rowIdx));
                    break;
                case type::OMNI_LONG:
                case type::OMNI_TIMESTAMP:
                case type::OMNI_DECIMAL64:
                case type::OMNI_DOUBLE:
                case type::OMNI_DATE64:
                    RowContainer::StoreValue<int64_t>(row, offset, decodedCols[c].GetValue<int64_t>(rowIdx));
                    break;
            }
        }

        // Set payload: next=null, visited=0
        auto payloadOff = rows_->PayloadOffset();
        *reinterpret_cast<char**>(row + payloadOff) = nullptr;
        *reinterpret_cast<uint8_t*>(row + payloadOff + sizeof(char*)) = 0;

        return row;
    }

    RowContainer* Rows() { return rows_.get(); }
    const RowContainer* Rows() const { return rows_.get(); }

    char* Find(Key key) const {
        if (!table_) return nullptr;
        auto* val = table_->Find(key);
        return val ? *reinterpret_cast<char**>(const_cast<char*>(val->buf)) : nullptr;
    }

    template <bool IsBuild>
    void EmplaceTable(const Key* keys, char** rows, int32_t numRows, char** = nullptr) {
        if constexpr (IsBuild) {
            auto payloadOff = rows_ ? rows_->PayloadOffset() : 0;
            table_->EmplaceBatch(
                keys,
                static_cast<uint32_t>(numRows),
                [](uint32_t) { return false; },
                [rows](uint32_t rowIdx, char* buf) {
                    // fInit: new key — set chain head to this row
                    *reinterpret_cast<char**>(buf) = rows[rowIdx];
                },
                [rows, payloadOff](uint32_t rowIdx, char* buf, bool initFlag) {
                    if (!initFlag && payloadOff) {
                        // fUpdate collision: prepend new row to front of chain
                        // buf currently points to old chain head
                        char* oldHead = *reinterpret_cast<char**>(buf);
                        *reinterpret_cast<char**>(buf) = rows[rowIdx];           // hash slot → new row
                        *reinterpret_cast<char**>(rows[rowIdx] + payloadOff) = oldHead; // newRow.next → old head
                    }
                });
        }
    }

    template <typename F>
    void ForEachValue(F&& /*fn*/) {} //TODO

    uint32_t Size() const { return 0; } //TODO
    uint64_t AllocatedBytes() const { return 0; } //TODO

private:
    int32_t numCols_ = 0;
    mem::SimpleArenaAllocator* arena_ = nullptr;
    std::vector<int32_t> typeIds_;
    std::unique_ptr<HashTable> table_;
    std::unique_ptr<RowContainer> rows_;
};

}  // namespace op
}  // namespace omniruntime
