/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: HyperLogLog for approx_count_distinct aggregate.
 * Reference: Velox HllAccumulator with SparseHll and DenseHll.
 */
#ifndef OMNI_RUNTIME_HYPERLOGLOG_H
#define OMNI_RUNTIME_HYPERLOGLOG_H

#include <cstdint>
#include <vector>
#include <cstring>
#include "bias_correction.h"
#include "util/omni_exception.h"
#include <algorithm>
#include <cmath>
#include <unordered_map>
#if defined(_MSC_VER)
#include <immintrin.h>
#endif

namespace omniruntime {
namespace op {

// Default index bit length (p). 2^11 = 2048 buckets, ~0.023 standard error.
constexpr int8_t kDefaultHllIndexBitLength = 11;
constexpr double kDefaultHllStandardError = 0.023;
// Velox-aligned range for approx_distinct(col, maxStandardError).
constexpr double kLowestMaxStandardError = 0.0040625;
constexpr double kHighestMaxStandardError = 0.26;

// --- Boolean special case: 1 byte state, no Sparse/Dense ---
/**
 * Returns approximate distinct count for boolean state: bit0 = seen false, bit1 = seen true; cardinality 0, 1, or 2.
 *
 * @param state: 1-byte state (bit0/bit1).
 * @return 0, 1, or 2.
 */
inline int8_t HllBooleanCardinality(int8_t state)
{
    return (state & 1) + ((state & 2) >> 1);
}

/**
 * Merges another boolean state into current: *state |= other.
 *
 * @param state: Pointer to current 1-byte state.
 * @param other: Incoming state to OR in.
 */
inline void HllBooleanMerge(int8_t *state, int8_t other)
{
    *state |= other;
}

/** Serialized boolean state: 2 bytes [type=0][state]. */
constexpr int32_t kHllBooleanSerializedSize = 2;

/**
 * Serializes boolean state into 2 bytes: out[0]=0 (type), out[1]=state.
 *
 * @param state: 1-byte state.
 * @param out: Buffer of at least 2 bytes.
 */
inline void HllBooleanSerialize(int8_t state, char *out)
{
    out[0] = 0;  // type: boolean
    out[1] = state;
}

/**
 * Returns true if serialized data is boolean format (type byte 0, length >= 2).
 *
 * @param data: Serialized buffer.
 * @param len: Length.
 */
inline bool HllCanDeserializeBoolean(const char *data, int32_t len)
{
    return len >= 2 && data[0] == 0;
}

/**
 * Deserializes boolean state from 2-byte form; data[0] must be 0.
 *
 * @param data: Buffer with at least 2 bytes.
 * @return data[1] as int8_t.
 */
inline int8_t HllBooleanDeserialize(const char *data)
{
    return static_cast<int8_t>(data[1]);
}

/**
 * SparseHll: variable-length list of (index, value) entries, 4 bytes per entry (26-bit bucket index + 6-bit value).
 */
class SparseHll {
public:
    static constexpr int32_t kEntrySize = 4;  /**< bytes per entry */

    /** Constructs SparseHll with given index bit length (p). */
    explicit SparseHll(int8_t indexBitLength);

    /** Sets soft entry limit; overLimit() becomes true when entries >= limit (used to trigger switch to Dense). */
    void setSoftMemoryLimit(uint32_t softMemoryLimit)
    {
        softNumEntriesLimit_ = softMemoryLimit / kEntrySize;
    }

    bool overLimit() const
    {
        return static_cast<uint32_t>(entries_.size()) >= softNumEntriesLimit_;
    }

    /** Inserts hash; returns true if soft limit reached after insert (caller may switch to Dense). */
    bool insertHash(uint64_t hash);

    /** Returns estimated cardinality (linear counting or HLL + bias correction). */
    int64_t cardinality() const;

    /** Serialized size in bytes. */
    int32_t serializedSize() const;
    /** Serializes to output (type=1, p, entry count, entries). */
    void serialize(char *output) const;

    void mergeWith(const SparseHll &other);
    void mergeWith(const char *serialized, int32_t len);
    /** Converts this Sparse to a new Dense (denseHll is initialized). */
    void toDense(class DenseHll *denseHll) const;
    /** Merges this Sparse into an existing Dense without re-initializing it (for distributed merge). */
    void mergeIntoDense(class DenseHll *denseHll) const;

    void reset()
    {
        entries_.clear();
        entries_.shrink_to_fit();
    }

    /** Returns true if data is Sparse format (type=1, len>=6). */
    static bool canDeserialize(const char *data, int32_t len);
    /** Returns index bit length from serialized Sparse (data[1]). */
    static int8_t deserializeIndexBitLength(const char *data);

private:
    int8_t indexBitLength_;
    std::vector<uint32_t> entries_;
    uint32_t softNumEntriesLimit_{0};

    static uint32_t computeIndex(uint64_t hash, int8_t p);
    static int8_t computeValue(uint64_t hash, int8_t p);
};

/**
 * DenseHll: fixed 2^p registers, each 0..63 (6-bit value).
 */
class DenseHll {
public:
    explicit DenseHll(int8_t indexBitLength);
    /** Deserializes from buffer (type=2, p, registers). */
    DenseHll(const char *serialized, int32_t len);

    void initialize(int8_t indexBitLength);
    int8_t indexBitLength() const { return indexBitLength_; }

    void insertHash(uint64_t hash);
    /** Inserts (index, value) directly; used when merging Sparse into Dense. */
    void insert(int32_t index, int8_t value);

    /** Returns estimated cardinality (linear counting or HLL + bias correction). */
    int64_t cardinality() const;

    int32_t serializedSize() const;
    void serialize(char *output) const;

    void mergeWith(const DenseHll &other);
    void mergeWith(const char *serialized, int32_t len);

    /** Returns true if data is Dense format (type=2, p in [4,16]). */
    static bool canDeserialize(const char *data, int32_t len);
    static int8_t deserializeIndexBitLength(const char *data);
    /** Bytes needed for 2^p registers. */
    static int32_t estimateInMemorySize(int8_t indexBitLength);

private:
    int8_t indexBitLength_{0};
    std::vector<int8_t> registers_;

    static uint32_t computeIndex(uint64_t hash, int8_t p);
    static int8_t computeValue(uint64_t hash, int8_t p);
};

/**
 * HllAccumulator: starts with SparseHll, switches to DenseHll when over soft memory limit.
 */
class HllAccumulator {
public:
    explicit HllAccumulator(int8_t indexBitLength = kDefaultHllIndexBitLength);

    void insertHash(uint64_t hash);
    int64_t cardinality() const;

    /** Merges serialized partial (boolean, Sparse, or Dense) into this accumulator. */
    void mergeWith(const char *serialized, int32_t len);
    void mergeWith(const HllAccumulator &other);

    int32_t serializedSize() const;
    void serialize(char *output) const;

    bool isSparse() const { return isSparse_; }

    /** Returns true if data is boolean(0), Sparse(1), or Dense(2) format. */
    static bool canDeserialize(const char *data, int32_t len);

private:
    void toDense();

    bool isSparse_{true};
    int8_t indexBitLength_{-1};
    SparseHll sparseHll_;
    DenseHll denseHll_;
};

/** Throws if maxStandardError is not in [kLowestMaxStandardError, kHighestMaxStandardError]. */
void HllCheckMaxStandardError(double maxStandardError);
/** Maps max standard error to HLL index bit length (p). */
int8_t HllToIndexBitLength(double maxStandardError);
/** Returns approximate standard error for given p. */
double HllToStandardError(int8_t indexBitLength);

/**
 * Merges incoming serialized partial into current buffer (Final phase). Current may be boolean or HLL; incoming same.
 * Updates current/currentLen in-place when capacity allows.
 *
 * @param current: Buffer for current state (at least 2 bytes).
 * @param currentLen: In/out length of current state.
 * @param currentCapacity: Maximum bytes in current buffer.
 * @param incoming: Serialized partial to merge.
 * @param incomingLen: Length of incoming.
 */
void HllMergeSerialized(char *current, int32_t *currentLen, int32_t currentCapacity,
    const char *incoming, int32_t incomingLen);

/**
 * Returns estimated cardinality from serialized form (boolean type=0, Sparse type=1, or Dense type=2).
 *
 * @param data: Serialized buffer.
 * @param len: Length.
 * @return Estimated distinct count.
 */
int64_t HllCardinalityFromSerialized(const char *data, int32_t len);

}  // namespace op
}  // namespace omniruntime

#endif  // OMNI_RUNTIME_HYPERLOGLOG_H
