/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Description: Row Container for Aggregation Implementation
 */

#include "row_container.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"
#include "vector/unsafe_vector.h"
#include "type/data_type.h"
#include "type/decimal128.h"
#include "util/bit_util.h"

#ifdef __ARM_FEATURE_SVE
#include <arm_sve.h>
#endif

namespace omniruntime::op {

RowContainer::RowContainer(const std::vector<int32_t>& keyTypeSizes,
                           int32_t numKeys,
                           int32_t aggStateSize,
                           mem::SimpleArenaAllocator& pool)
    : numKeys(numKeys),
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
        nullOffsets[i] = nullBitPos;
        nullBitPos++;
    }

    // Pad key null bits to fill 8-bit boundary (like bolt's TINYINT padding)
    if (numKeys % 8 != 0) {
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

    // Build RowColumn descriptors
    // nullByte = nullBlockStart + (nullBitPos / 8)
    // nullMask = (1 << (nullBitPos % 8))
    rowColumns.reserve(numKeys);
    for (int32_t i = 0; i < numKeys; ++i) {
        int32_t absNullBitPos = nullBlockStart * 8 + nullOffsets[i];
        rowColumns.emplace_back(offsets[i], absNullBitPos);
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
    memset(row, 0, fixedRowSize);

    // Set accumulator null bits to 1 (aggregates start as null)
    for (int32_t bit = firstAggregateNullBit; bit < freeFlagOffset; ++bit) {
        row[nullBlockStart + bit / 8] |= (1 << (bit & 7));
    }

    return row;
}

int32_t RowContainer::ListRows(RowContainerIterator* iter, int32_t maxRows, char** rows)
{
    int32_t count = 0;
    int32_t numAllocations = static_cast<int32_t>(allocations.size());

    while (count < maxRows && iter->allocationIndex < numAllocations) {
        char* row = allocations[iter->allocationIndex];
        iter->allocationIndex++;

        // Skip freed rows
        if ((row[freeFlagByteOffset] & (1 << freeFlagBitInByte)) == 0) {
            rows[count++] = row;
        }
    }

    // Signal completion if we've exhausted all allocations
    if (iter->allocationIndex >= numAllocations) {
        iter->allocationIndex = std::numeric_limits<int32_t>::max();
    }

    return count;
}

#ifdef __ARM_FEATURE_SVE
template <typename T>
static void SveExtractColumnImpl(char** rows, int32_t totalRows, int32_t offset,
                                  int32_t nullByte, uint8_t nullMask,
                                  Vector<T>* vec)
{
    T* outValues = vec::unsafe::UnsafeVector::GetRawValues(vec);
    uint64_t* outNulls = reinterpret_cast<uint64_t*>(vec::unsafe::UnsafeBaseVector::GetNulls(vec));
    bool hasNull = false;

    svbool_t pgAll = svptrue_b64();
    int64_t tmpBuf[32];

    for (int32_t i = 0; i < totalRows;) {
        svbool_t pg = svwhilelt_b64_s64((int64_t)i, (int64_t)totalRows);
        int32_t activeCount = svcntp_b64(pgAll, pg);

        svuint64_t vIdx = svindex_u64(i, 1);
        svuint64_t vPtrOffsets = svlsl_n_u64_x(pg, vIdx, 3);
        svuint64_t vRowPtrs = svld1_gather_offset_u64(pg, vPtrOffsets, (uint64_t)rows);

        svuint64_t vNullAddr = svadd_n_u64_x(pg, vRowPtrs, (uint64_t)nullByte);
        svuint64_t vRowNullByte = svld1ub_gather_u64(pg, vNullAddr);
        svbool_t vRowIsNull = svcmpne_n_u64(pg, svand_n_u64_x(pg, vRowNullByte, (uint64_t)nullMask), 0);

        if (svptest_any(pgAll, vRowIsNull)) {
            hasNull = true;
        }

        svuint64_t vValueAddr = svadd_n_u64_x(pg, vRowPtrs, (uint64_t)offset);
        svint64_t vZero = svdup_n_s64(0);

        if constexpr (std::is_same_v<T, int64_t>) {
            svint64_t vRowValues = svld1_gather_s64(pg, vValueAddr);
            svst1_s64(pg, outValues + i, svsel_s64(vRowIsNull, vZero, vRowValues));
        } else if constexpr (std::is_same_v<T, double>) {
            svfloat64_t vRowValues = svld1_gather_f64(pg, vValueAddr);
            svfloat64_t vZeroF = svdup_n_f64(0.0);
            svst1_f64(pg, outValues + i, svsel_f64(vRowIsNull, vZeroF, vRowValues));
        } else {
            svint64_t vRowValues;
            if constexpr (std::is_same_v<T, int32_t>) {
                vRowValues = svld1sw_gather_s64(pg, vValueAddr);
            } else if constexpr (std::is_same_v<T, int16_t>) {
                vRowValues = svld1sh_gather_s64(pg, vValueAddr);
            } else if constexpr (std::is_same_v<T, int8_t>) {
                vRowValues = svld1sb_gather_s64(pg, vValueAddr);
            } else {
                for (int32_t j = 0; j < activeCount; j++) {
                    if (RowContainer::IsNullAt(rows[i + j], nullByte, nullMask)) {
                        outValues[i + j] = T{};
                    } else {
                        outValues[i + j] = RowContainer::ReadValue<T>(rows[i + j], offset);
                    }
                }
                i += activeCount;
                continue;
            }

            svint64_t vSelected = svsel_s64(vRowIsNull, vZero, vRowValues);
            svst1_s64(pg, tmpBuf, vSelected);
            for (int32_t j = 0; j < activeCount; j++) {
                outValues[i + j] = static_cast<T>(tmpBuf[j]);
            }
        }

        if (hasNull) {
            for (int32_t j = 0; j < activeCount; j++) {
                if (RowContainer::IsNullAt(rows[i + j], nullByte, nullMask)) {
                    BitUtil::SetBit(outNulls, i + j);
                }
            }
        }

        i += activeCount;
    }

    if (hasNull) {
        vec->SetNullFlag(true);
    }
}
#endif

void RowContainer::ExtractColumn(char** rows, int32_t totalRows, int32_t colIdx,
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
#ifdef __ARM_FEATURE_SVE
            SveExtractColumnImpl<int32_t>(rows, totalRows, offset, nullByte, nullMask, vec);
#else
            for (int32_t i = 0; i < totalRows; ++i) {
                if (IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<int32_t>(rows[i], offset));
                }
            }
#endif
            break;
        }
        case type::OMNI_LONG:
        case type::OMNI_TIMESTAMP:
        case type::OMNI_DECIMAL64:
        case type::OMNI_DATE64:
        case type::OMNI_TIME64: {
            auto* vec = static_cast<Vector<int64_t>*>(outputVector);
#ifdef __ARM_FEATURE_SVE
            SveExtractColumnImpl<int64_t>(rows, totalRows, offset, nullByte, nullMask, vec);
#else
            for (int32_t i = 0; i < totalRows; ++i) {
                if (IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<int64_t>(rows[i], offset));
                }
            }
#endif
            break;
        }
        case type::OMNI_SHORT: {
            auto* vec = static_cast<Vector<int16_t>*>(outputVector);
#ifdef __ARM_FEATURE_SVE
            SveExtractColumnImpl<int16_t>(rows, totalRows, offset, nullByte, nullMask, vec);
#else
            for (int32_t i = 0; i < totalRows; ++i) {
                if (IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<int16_t>(rows[i], offset));
                }
            }
#endif
            break;
        }
        case type::OMNI_BYTE: {
            auto* vec = static_cast<Vector<int8_t>*>(outputVector);
#ifdef __ARM_FEATURE_SVE
            SveExtractColumnImpl<int8_t>(rows, totalRows, offset, nullByte, nullMask, vec);
#else
            for (int32_t i = 0; i < totalRows; ++i) {
                if (IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<int8_t>(rows[i], offset));
                }
            }
#endif
            break;
        }
        case type::OMNI_DOUBLE: {
            auto* vec = static_cast<Vector<double>*>(outputVector);
#ifdef __ARM_FEATURE_SVE
            SveExtractColumnImpl<double>(rows, totalRows, offset, nullByte, nullMask, vec);
#else
            for (int32_t i = 0; i < totalRows; ++i) {
                if (IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<double>(rows[i], offset));
                }
            }
#endif
            break;
        }
        case type::OMNI_FLOAT: {
            auto* vec = static_cast<Vector<float>*>(outputVector);
#ifdef __ARM_FEATURE_SVE
            SveExtractColumnImpl<float>(rows, totalRows, offset, nullByte, nullMask, vec);
#else
            for (int32_t i = 0; i < totalRows; ++i) {
                if (IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<float>(rows[i], offset));
                }
            }
#endif
            break;
        }
        case type::OMNI_BOOLEAN: {
            auto* vec = static_cast<Vector<bool>*>(outputVector);
            for (int32_t i = 0; i < totalRows; ++i) {
                if (IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<int8_t>(rows[i], offset) != 0);
                }
            }
            break;
        }
        case type::OMNI_DECIMAL128: {
            auto* vec = static_cast<Vector<Decimal128>*>(outputVector);
            for (int32_t i = 0; i < totalRows; ++i) {
                if (IsNullAt(rows[i], nullByte, nullMask)) {
                    vec->SetNull(i);
                } else {
                    vec->SetValue(i, ReadValue<Decimal128>(rows[i], offset));
                }
            }
            break;
        }
        default:
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

    if (IsNullAt(row, nullByte, nullMask)) {
        return vector->IsNull(rowIdx);
    }

    if (vector->IsNull(rowIdx)) {
        return false;
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
