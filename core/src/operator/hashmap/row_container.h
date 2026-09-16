/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Description: Row Container for Aggregation
 */

#ifndef OMNI_RUNTIME_ROW_CONTAINER_H
#define OMNI_RUNTIME_ROW_CONTAINER_H

#include <cstdint>
#include <cstring>
#include <string_view>
#include <utility>
#include <vector>
#include <memory>
#include "memory/simple_arena_allocator.h"
#include "util/compiler_util.h"
#include "vector/dictionary_container.h"
#include "vector/large_string_container.h"
#include "vector/vector.h"

namespace omniruntime::vec {
class BaseVector;
}

namespace omniruntime::op {

   struct RowContainerIterator;
   using namespace omniruntime::vec;   

/// Packed representation of offset, null byte offset and null mask for
/// a column inside a RowContainer.
class RowColumn {
public:
    static constexpr int32_t kNotNullOffset = -1;

    RowColumn(int32_t offset, int32_t nullOffset)
        : packedOffsets(PackOffsets(offset, nullOffset)) {}

    int32_t Offset() const { return packedOffsets >> 32; }

    int32_t NullByte() const { return static_cast<uint32_t>(packedOffsets) >> 8; }

    uint8_t NullMask() const { return packedOffsets & 0xff; }

private:
    static uint64_t PackOffsets(int32_t offset, int32_t nullOffset) {
        if (nullOffset == kNotNullOffset) {
            return static_cast<uint64_t>(offset) << 32;
        }
        return (1UL << (nullOffset & 7)) | ((static_cast<uint64_t>(nullOffset) & ~7UL) << 5) |
            static_cast<uint64_t>(offset) << 32;
    }

    uint64_t packedOffsets;
};

/// RowContainer stores rows in fixed-size slots allocated from an arena allocator.
/// Each row has the layout:
///   [key data (fixed-width)] [null bits block] [AggState data] [optional padding]
///
/// Key data is at the beginning of the row. Null bits encode nullness of
/// key columns and AggState columns. AggState data follows null bits.
///
/// This design follows the bolt RowContainer pattern but is self-contained
/// within OmniOperator (no bolt headers are included).
class RowContainer {
public:
    /// Constructor.
    /// @param keyTypeSizes  Size in bytes of each key column type (fixed-width only)
    /// @param numKeys       Number of key columns
    /// @param aggStateSize  Total size in bytes of all AggState data per row
    /// @param pool          Arena allocator for row memory
    RowContainer(const std::vector<int32_t>& keyTypeSizes,
                 int32_t numKeys,
                 int32_t aggStateSize,
                 mem::SimpleArenaAllocator& pool);

    /// Allocate a new row and return a pointer to its start.
    /// The entire row is zero-initialized, then AggState null bits
    /// are set to 1 (aggregates start as null).
    char* NewRow();

    /// Batch allocate 'count' contiguous rows (no free-list rows available).
    /// Allocates exactly 'count' rows and zero-initializes them in one pass.
    /// Returns the contiguous base (row i = base + i*fixedRowSize) and the
    /// actual count via 'outCount'. Returns nullptr if free-list rows exist
    /// (caller should fall back to per-row NewRow).
    char* NewRowBatch(int32_t count, int32_t* outCount);

    /// Check if a column is null in the given row.
    static ALWAYS_INLINE bool IsNullAt(const char* row, int32_t nullByte, uint8_t nullMask) {
        return (row[nullByte] & nullMask) != 0;
    }

    /// Set a column to null in the given row.
    static ALWAYS_INLINE void SetNullAt(char* row, int32_t nullByte, uint8_t nullMask) {
        row[nullByte] |= nullMask;
    }

    /// Clear a column's null flag in the given row.
    static ALWAYS_INLINE void ClearNullAt(char* row, int32_t nullByte, uint8_t nullMask) {
        row[nullByte] &= ~nullMask;
    }

    /// Get the RowColumn descriptor for a given column index.
    RowColumn ColumnAt(int32_t colIdx) const { return rowColumns[colIdx]; }

    /// Get the offset where AggState data begins within a row.
    int32_t AggStateOffset() const { return aggStateOffset; }

    // --- TAPER join payload area -------------------------------------------

    /// For TAPER join, the payload region reuses the AggState area.
    int32_t PayloadOffset() const { return aggStateOffset; }

    /// Size of a packed row pointer (6 bytes, saving 2 bytes vs full 8-byte pointer).
    static constexpr int32_t kRowPtrSize = 6;

    /// Pack a pointer into a 6-byte buffer (lower 48 bits).
    static ALWAYS_INLINE void SetPackedPtr(char* buf, char* ptr) {
        uint64_t val = reinterpret_cast<uint64_t>(ptr);
        memcpy(buf, &val, kRowPtrSize);
    }

    /// Unpack a pointer from a 6-byte buffer.
    static ALWAYS_INLINE char* GetPackedPtr(const char* buf) {
        uint64_t val = 0;
        memcpy(&val, buf, kRowPtrSize);
        return reinterpret_cast<char*>(val);
    }

    /// Read the "next" chain pointer stored at the given payload offset (6-byte packed).
    static char* GetNextPtr(char* row, int32_t payloadOffset) {
        return GetPackedPtr(row + payloadOffset);
    }

    /// Write the "next" chain pointer at the given payload offset (6-byte packed).
    static void SetNextPtr(char* row, int32_t payloadOffset, char* ptr) {
        SetPackedPtr(row + payloadOffset, ptr);
    }

    /// Return a pointer to the "visited" byte stored after the next pointer in the payload.
    static uint8_t* VisitedPtr(char* row, int32_t payloadOffset) {
        return reinterpret_cast<uint8_t*>(row + payloadOffset + kRowPtrSize);
    }

    /// Read a 4-byte value (memcpy-based, safe for unaligned offsets).
    static ALWAYS_INLINE uint32_t GetUint32(const char* buf) {
        uint32_t v;
        memcpy(&v, buf, sizeof(uint32_t));
        return v;
    }

    /// Write a 4-byte value (memcpy-based, safe for unaligned offsets).
    static ALWAYS_INLINE void SetUint32(char* buf, uint32_t v) {
        memcpy(buf, &v, sizeof(uint32_t));
    }

    /// Get the fixed row size.
    int32_t FixedRowSize() const { return fixedRowSize; }

    /// Get the number of key columns.
    int32_t NumKeys() const { return numKeys; }

    /// Store a fixed-width value into a row at the given column index.
    template <typename T>
    static ALWAYS_INLINE void StoreValue(char* row, int32_t offset, T value) {
        *reinterpret_cast<T*>(row + offset) = value;
    }

    /// Read a fixed-width value from a row at the given column index.
    template <typename T>
    static ALWAYS_INLINE T ReadValue(const char* row, int32_t offset) {
        return *reinterpret_cast<const T*>(row + offset);
    }

    /// Packed {ptr, size} storage for zero-copy strings in RowContainer rows.
    struct __attribute__((packed)) StringViewStorage {
        const char* data;
        uint32_t size;
    };

    /// Lightweight view over the TAPER handler's per-column/per-batch varchar
    /// container snapshot. Passed to ExtractColumn instead of a std::function
    /// so the per-row resolution inlines into the extraction loop.
    /// All pointers must outlive every row resolved through this struct
    /// (they point at the owning TaperJoin*Handler's members).
    struct VarcharResolver {
        using ContainerVec = std::vector<std::shared_ptr<void>>;
        const std::vector<ContainerVec>* containers;                 // [colIdx][batchId]
        const std::vector<std::vector<int32_t>>* encodings;          // [colIdx][batchId]
        const std::vector<std::vector<StringViewStorage>>* consts;   // [colIdx][batchId]
        const std::vector<std::vector<int32_t>>* vecOffsets;         // [colIdx][batchId]
        int32_t idOffset;                                            // row offset of batchId

        /// Read batchId/rowId from the row payload and resolve the string.
        ALWAYS_INLINE std::string_view Resolve(int32_t colIdx, const char* row) const
        {
            uint32_t batchId = GetUint32(row + idOffset);
            uint32_t rowId = GetUint32(row + idOffset + 4);
            return ResolveById(colIdx, batchId, rowId);
        }

        /// Resolve a string from an already-extracted (batchId, rowId).
        ALWAYS_INLINE std::string_view ResolveById(int32_t colIdx, uint32_t batchId,
            uint32_t rowId) const
        {
            if (colIdx >= static_cast<int32_t>(encodings->size()) ||
                colIdx >= static_cast<int32_t>(containers->size())) {
                return {};
            }
            const auto& encVec = (*encodings)[colIdx];
            const auto& contVec = (*containers)[colIdx];
            if (batchId >= encVec.size() || batchId >= contVec.size()) {
                return {};
            }
            auto enc = encVec[batchId];
            if (enc == OMNI_DICTIONARY) {
                auto* dict = static_cast<DictionaryContainer<std::string_view>*>(
                    contVec[batchId].get());
                return dict->GetValue(static_cast<int32_t>(rowId) + (*vecOffsets)[colIdx][batchId]);
            }
            if (enc == OMNI_ENCODING_CONST) {
                const auto& cs = (*consts)[colIdx][batchId];
                return {cs.data, cs.size};
            }
            auto* lsc = static_cast<LargeStringContainer<std::string_view>*>(
                contVec[batchId].get());
            return lsc->GetValue(static_cast<int32_t>(rowId) + (*vecOffsets)[colIdx][batchId]);
        }
    };

    /// Iterate through all allocated rows and collect pointers to active rows.
    /// This follows the bolt RowContainer::listRows pattern.
    /// @param iter      Iterator tracking position across calls
    /// @param maxRows   Maximum number of rows to collect
    /// @param rows      Output array of row pointers (must have maxRows capacity)
    /// @return Number of rows collected
    int32_t ListRows(RowContainerIterator* iter, int32_t maxRows, char** rows);

    /// Extract a key column from a set of rows into an output vector.
    /// This dispatches by type to the appropriate vector setter.
    /// For varchar columns, 'varcharResolver' (when provided) retrieves the string
    /// via batchId/rowId stored in the payload (TAPER layout with keySizes=0);
    /// otherwise a StringViewStorage is read from the key area.
    void ExtractColumn(char** rows, int32_t totalRows, int32_t colIdx,
                       vec::BaseVector* outputVector,
                       const VarcharResolver* varcharResolver = nullptr);

    /// Compare a key column in a row against a decoded vector value.
    /// Used for speculative key verification.
    bool Equals(const char* row, int32_t colIdx, vec::BaseVector* vector, int32_t rowIdx);

    /// Get the arena allocator.
    mem::SimpleArenaAllocator& Pool() { return pool; }

    /// Get the number of rows in the container.
    int64_t NumRows() const { return numRows; }

    /// True if free-list rows exist (per-row NewRow fallback required).
    bool HasFreeRows() const { return firstFreeRow != nullptr; }

    void Reset()
    {
        allocations.clear();
        firstFreeRow = nullptr;
        numRows = 0;
        numFreeRows = 0;
        batchPtr = nullptr;
        batchRemaining = 0;
    }

private:
    /// Initialize a newly allocated or reused row.
    char* InitializeRow(char* row);

    // Layout configuration
    int32_t numKeys;
    int32_t aggStateSize;
    int32_t fixedRowSize = 0;
    int32_t aggStateOffset = 0;
    int32_t nullBlockStart = 0; // absolute byte offset where null block starts in row

    // Column descriptors
    std::vector<int32_t> offsets;     // byte offset of each column in the row
    std::vector<int32_t> nullOffsets; // bit offset of null flags
    std::vector<RowColumn> rowColumns;

    int32_t nullBytes = 0;

    // Row storage
    mem::SimpleArenaAllocator& pool;
    std::vector<std::pair<char*, int32_t>> allocations; // {block base, row count} per allocation
    char* firstFreeRow = nullptr;
    int64_t numRows = 0;
    int64_t numFreeRows = 0;
    static constexpr int32_t kBatchSize = 1024;
    char* batchPtr = nullptr;
    int32_t batchRemaining = 0;
};

namespace PrefetchHelper
{
    constexpr int32_t kPrefetchDistance = 32;
    // Prefetch helpers for ExtractColumn — 预取行数据和可选字符串内容
    inline void PrefetchRow(const char *row, int32_t offset, int32_t nullByte)
    {
        __builtin_prefetch(row + offset, 0, 2);
        __builtin_prefetch(row + nullByte, 0, 2);
    }
    inline void PrefetchRowString(char** rows, int32_t offset, int32_t nullByte, int32_t nullMask,
        int32_t numRows, int32_t i)
    {
        if (LIKELY(i + 2 * kPrefetchDistance < numRows))
        {
            if (LIKELY(rows[i + 2 * kPrefetchDistance] != nullptr))
            {
                PrefetchRow(rows[i + 2 * kPrefetchDistance], offset, nullByte);
            }
        }

        if (LIKELY(i + kPrefetchDistance < numRows))
        {
            if (LIKELY(rows[i + kPrefetchDistance] != nullptr))
            {
                auto stringViewStorage = RowContainer::ReadValue<RowContainer::StringViewStorage>(
                    rows[i + kPrefetchDistance], offset);
                if (LIKELY(RowContainer::IsNullAt(rows[i + kPrefetchDistance], nullByte, nullMask) == false &&
                           stringViewStorage.data != nullptr && stringViewStorage.size > 0))
                {
                    __builtin_prefetch(stringViewStorage.data, 0, 2);
                }
            }
        }
    }
} // namespace PrefetchHelper

/// Iterator for RowContainer::listRows, tracking position across calls.
struct RowContainerIterator {
    int32_t allocationIndex = 0;
    int32_t rowOffset = 0;
};

} // namespace omniruntime::op

#endif // OMNI_RUNTIME_ROW_CONTAINER_H
