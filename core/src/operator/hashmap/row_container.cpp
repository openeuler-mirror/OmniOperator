/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Description: Row Container for Aggregation Implementation
 */

#include "row_container.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"
#include "type/data_type.h"
#include "type/decimal128.h"
#include "util/bit_util.h"

namespace omniruntime::op {

RowContainer::RowContainer(const std::vector<int32_t>& keyTypeSizes,
                           int32_t numKeys,
                           bool nullableKeys,
                           int32_t aggStateSize,
                           mem::SimpleArenaAllocator& pool)
    : numKeys(numKeys),
      nullableKeys(nullableKeys),
      aggStateSize(aggStateSize),
      pool(pool)
{
    // Calculate row layout: [key columns] [null bits] [AggState] [padding]
    // Following bolt's RowContainer pattern with absolute bit offsets.
    int32_t offset = 0;

    // Step 1: Key column offsets
    offsets.resize(numKeys);
    nullOffsets.resize(numKeys);

    for (int32_t i = 0; i < numKeys; ++i) {
        offsets[i] = offset;
        offset += keyTypeSizes[i];
        // Update alignment to at least the key column size
        alignment = std::max(alignment, keyTypeSizes[i]);
    }

    // Ensure minimum sizeof(void*) for the free-list next pointer at offset 0
    offset = std::max<int32_t>(offset, static_cast<int32_t>(sizeof(void*)));
    alignment = std::max(alignment, static_cast<int32_t>(sizeof(void*)));

    // Step 2: Null bits block
    // Null bits use absolute bit positions from the start of the null block.
    // The null block starts right after key data at the current offset.
    // Each null bit is at a position counted from bit 0 within this block.
    // RowColumn stores null byte offset = (null_bit_position / 8) relative to
    // the null block start, and null mask = (1 << (null_bit_position % 8)).
    //
    // Following bolt: we convert null positions to absolute byte positions
    // within the row by adding the null block start offset.

    int32_t nullBitPos = 0; // bit position within the null block (0-based)

    // Key column null bits
    for (int32_t i = 0; i < numKeys; ++i) {
        if (nullableKeys) {
            // nullByte = offset + nullBitPos / 8, nullMask = 1 << (nullBitPos % 8)
            nullOffsets[i] = nullBitPos;
            nullBitPos++;
        } else {
            nullOffsets[i] = RowColumn::kNotNullOffset;
        }
    }

    // Pad key null bits to fill 8-bit boundary (like bolt's TINYINT padding)
    if (nullableKeys && (numKeys % 8 != 0)) {
        nullBitPos += (8 - numKeys % 8);
    }

    // Accumulator null bits
    firstAggregateNullBit = nullBitPos;
    if (aggStateSize > 0) {
        nullBitPos++; // one null bit for the aggregate state
    }

    // Free flag bit
    freeFlagOffset = nullBitPos;
    nullBitPos++;

    // Calculate null bytes size and add to offset
    nullBytes = BitUtil::Nbytes(nullBitPos); // = RoundUp(nullBitPos, 8) / 8
    nullBlockStart = offset; // byte offset where null block starts in row
    offset += nullBytes;

    // Align offset for AggState
    alignment = std::max(alignment, static_cast<int32_t>(alignof(uint64_t)));
    offset = BitUtil::RoundUp(offset, alignment);

    // Step 3: AggState offset
    aggStateOffset = offset;
    offset += aggStateSize;

    // Final alignment
    alignment = std::max(alignment, static_cast<int32_t>(sizeof(void*)));
    fixedRowSize = BitUtil::RoundUp(offset, alignment);

    // Build initial nulls template: all zeros, then set accumulator null bits to 1
    initialNulls.resize(nullBytes, 0);
    // Set accumulator null bits to 1 (aggregates start as null)
    for (int32_t bit = firstAggregateNullBit; bit < freeFlagOffset; ++bit) {
        initialNulls[bit / 8] |= (1 << (bit & 7));
    }

    // Build RowColumn descriptors
    // nullByte = nullBlockStart + (nullBitPos / 8)
    // nullMask = (1 << (nullBitPos % 8))
    rowColumns.reserve(numKeys);
    for (int32_t i = 0; i < numKeys; ++i) {
        if (nullableKeys) {
            int32_t absNullByte = nullBlockStart + nullOffsets[i] / 8;
            uint8_t absNullMask = 1 << (nullOffsets[i] % 8);
            // Store as absolute bit position for RowColumn
            // RowColumn.packOffsets expects: offset (column data), nullOffset (absolute bit pos)
            // We need to convert: the RowColumn PackOffsets formula uses nullOffset as a bit pos
            // where nullByte() = (nullOffset / 8) and nullMask() = (1 << (nullOffset % 8))
            // But we need absolute byte positions in the row, not relative to null block.
            // So we set nullOffset = nullBlockStart * 8 + relativeBitPos to make
            // nullByte() return absolute row offset.
            int32_t absNullBitPos = nullBlockStart * 8 + nullOffsets[i];
            rowColumns.emplace_back(offsets[i], absNullBitPos);
        } else {
            rowColumns.emplace_back(offsets[i], RowColumn::kNotNullOffset);
        }
    }

    // Also compute the absolute byte offset for the free flag
    // freeFlagOffset is a relative bit position, we need absolute
    freeFlagByteOffset = nullBlockStart + freeFlagOffset / 8;
    freeFlagBitInByte = freeFlagOffset % 8;
}

char* RowContainer::NewRow()
{
    ++numRows;
    char* row = nullptr;

    if (firstFreeRow != nullptr) {
        row = firstFreeRow;
        // Read the next free pointer stored at offset 0 (where key data starts)
        firstFreeRow = *reinterpret_cast<char**>(row);
        --numFreeRows;
    } else {
        // Allocate a new fixed-size row from the arena
        row = reinterpret_cast<char*>(pool.Allocate(fixedRowSize));
        allocations.push_back(row);
    }

    return InitializeRow(row);
}

char* RowContainer::InitializeRow(char* row)
{
    // Zero out the entire row (keys, nulls, aggState)
    memset(row, 0, fixedRowSize);

    // Copy initial nulls template (sets accumulator null bits to 1, key null bits to 0)
    // The null block starts at nullBlockStart offset within the row
    if (nullBytes > 0) {
        memcpy(row + nullBlockStart, initialNulls.data(), initialNulls.size());
    }

    // Clear the free flag bit (mark row as in-use)
    row[freeFlagByteOffset] &= ~(1 << freeFlagBitInByte);

    return row;
}

int32_t RowContainer::ListRows(RowContainerIterator* iter, int32_t maxRows, char** rows)
{
    int32_t count = 0;
    int32_t startIdx = iter->allocationIndex;
    int32_t endIdx = std::min(startIdx + maxRows, static_cast<int32_t>(allocations.size()));

    for (int32_t idx = startIdx; idx < endIdx; ++idx) {
        char* row = allocations[idx];
        // Skip freed rows
        if ((row[freeFlagByteOffset] & (1 << freeFlagBitInByte)) == 0) {
            rows[count++] = row;
        }
    }

    iter->allocationIndex = endIdx;

    // Signal completion if we've exhausted all allocations
    if (endIdx >= static_cast<int32_t>(allocations.size())) {
        iter->allocationIndex = std::numeric_limits<int32_t>::max();
    }

    return count;
}

void RowContainer::ExtractColumn(char** rows, int32_t numRows, int32_t colIdx,
                                  vec::BaseVector* outputVector)
{
    if (colIdx >= numKeys) {
        return; // AggState columns are extracted separately
    }

    auto Col = rowColumns[colIdx];
    auto offset = Col.Offset();
    auto nullByte = Col.NullByte();
    auto nullMask = Col.NullMask();

    // Dispatch based on output vector type
    auto typeId = outputVector->GetTypeId();

    switch (typeId) {
        case type::OMNI_INT:
        case type::OMNI_DATE32:
        case type::OMNI_TIME32: {
            auto* vec = static_cast<Vector<int32_t>*>(outputVector);
            for (int32_t i = 0; i < numRows; ++i) {
                if (nullableKeys && IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<int32_t>(rows[i], offset));
                }
            }
            break;
        }
        case type::OMNI_LONG:
        case type::OMNI_TIMESTAMP:
        case type::OMNI_DECIMAL64:
        case type::OMNI_DATE64:
        case type::OMNI_TIME64: {
            auto* vec = static_cast<Vector<int64_t>*>(outputVector);
            for (int32_t i = 0; i < numRows; ++i) {
                if (nullableKeys && IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<int64_t>(rows[i], offset));
                }
            }
            break;
        }
        case type::OMNI_SHORT: {
            auto* vec = static_cast<Vector<int16_t>*>(outputVector);
            for (int32_t i = 0; i < numRows; ++i) {
                if (nullableKeys && IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<int16_t>(rows[i], offset));
                }
            }
            break;
        }
        case type::OMNI_BYTE: {
            auto* vec = static_cast<Vector<int8_t>*>(outputVector);
            for (int32_t i = 0; i < numRows; ++i) {
                if (nullableKeys && IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<int8_t>(rows[i], offset));
                }
            }
            break;
        }
        case type::OMNI_DOUBLE: {
            auto* vec = static_cast<Vector<double>*>(outputVector);
            for (int32_t i = 0; i < numRows; ++i) {
                if (nullableKeys && IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<double>(rows[i], offset));
                }
            }
            break;
        }
        case type::OMNI_FLOAT: {
            auto* vec = static_cast<Vector<float>*>(outputVector);
            for (int32_t i = 0; i < numRows; ++i) {
                if (nullableKeys && IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<float>(rows[i], offset));
                }
            }
            break;
        }
        case type::OMNI_BOOLEAN: {
            auto* vec = static_cast<Vector<bool>*>(outputVector);
            for (int32_t i = 0; i < numRows; ++i) {
                if (nullableKeys && IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<int8_t>(rows[i], offset) != 0);
                }
            }
            break;
        }
        case type::OMNI_DECIMAL128: {
            auto* vec = static_cast<Vector<Decimal128>*>(outputVector);
            for (int32_t i = 0; i < numRows; ++i) {
                if (nullableKeys && IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<Decimal128>(rows[i], offset));
                }
            }
            break;
        }
        default:
            // For complex types (VARCHAR, ARRAY, ROW), we still use the serializer approach
            // These are handled separately via serialized key extraction
            break;
    }
}

bool RowContainer::Equals(const char* row, int32_t colIdx, vec::BaseVector* vector, int32_t rowIdx)
{
    if (colIdx >= numKeys) {
        return true; // AggState columns are not compared
    }

    auto Col = rowColumns[colIdx];
    auto offset = Col.Offset();
    auto nullByte = Col.NullByte();
    auto nullMask = Col.NullMask();

    // Check nulls first
    bool rowIsNull = nullableKeys && IsNullAt(row, nullByte, nullMask);
    bool vecIsNull = vector->IsNull(rowIdx);
    if (rowIsNull != vecIsNull) {
        return false;
    }
    if (rowIsNull) {
        return true; // both null, match
    }

    // Compare values by type
    auto typeId = vector->GetTypeId();
    switch (typeId) {
        case type::OMNI_INT:
        case type::OMNI_DATE32:
        case type::OMNI_TIME32:
            return ReadValue<int32_t>(row, offset) == static_cast<Vector<int32_t>*>(vector)->GetValue(rowIdx);
        case type::OMNI_LONG:
        case type::OMNI_TIMESTAMP:
        case type::OMNI_DECIMAL64:
        case type::OMNI_DATE64:
        case type::OMNI_TIME64:
            return ReadValue<int64_t>(row, offset) == static_cast<Vector<int64_t>*>(vector)->GetValue(rowIdx);
        case type::OMNI_SHORT:
            return ReadValue<int16_t>(row, offset) == static_cast<Vector<int16_t>*>(vector)->GetValue(rowIdx);
        case type::OMNI_BYTE:
            return ReadValue<int8_t>(row, offset) == static_cast<Vector<int8_t>*>(vector)->GetValue(rowIdx);
        case type::OMNI_DOUBLE:
            return ReadValue<double>(row, offset) == static_cast<Vector<double>*>(vector)->GetValue(rowIdx);
        case type::OMNI_FLOAT:
            return ReadValue<float>(row, offset) == static_cast<Vector<float>*>(vector)->GetValue(rowIdx);
        case type::OMNI_BOOLEAN:
            return (ReadValue<int8_t>(row, offset) != 0) == static_cast<Vector<bool>*>(vector)->GetValue(rowIdx);
        case type::OMNI_DECIMAL128:
            return ReadValue<Decimal128>(row, offset) == static_cast<Vector<Decimal128>*>(vector)->GetValue(rowIdx);
        default:
            return false; // Complex types need serialized comparison
    }
}

} // namespace omniruntime::op
