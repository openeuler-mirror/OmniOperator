/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: HyperLogLog implementation for approx_count_distinct.
 */
#include "hyperloglog.h"

namespace omniruntime {
namespace op {

/**
 * Bucket index from hash: high p bits. Matches Velox HllUtils::computeIndex.
 */
static uint32_t ComputeIndex(uint64_t hash, int8_t p)
{
    return static_cast<uint32_t>(hash >> (64 - p));
}

/**
 * Number of contiguous zeros after the first p bits in hash. Matches Velox HllUtils::numberOfLeadingZeros.
 * OR with (1 << (p-1)) so the argument to clz is never 0 (avoids UB; preserves count when hash<<p != 0).
 */
static int NumberOfLeadingZeros(uint64_t hash, int p)
{
    uint64_t rest = (hash << p) | (UINT64_C(1) << (p - 1));
#if defined(_MSC_VER)
    return static_cast<int>(_lzcnt_u64(rest));
#else
    return static_cast<int>(__builtin_clzll(rest));
#endif
}

/**
 * HLL register value: leading-zero count + 1. Matches Velox numberOfLeadingZeros(hash, p) + 1.
 */
static int8_t ComputeValue(uint64_t hash, int8_t p)
{
    return static_cast<int8_t>(NumberOfLeadingZeros(hash, p) + 1);
}

/** Alpha constant for raw HLL estimate; p=4,5,6 use fixed values per Velox. */
static double Alpha(int8_t indexBitLength, int32_t m)
{
    switch (indexBitLength) {
        case 4: return 0.673;
        case 5: return 0.697;
        case 6: return 0.709;
        default: return 0.7213 / (1.0 + 1.079 / static_cast<double>(m));
    }
}

// Linear counting threshold: match Velox kLinearCountingMinEmptyBuckets = 0.4
static constexpr double kLinearCountingMinEmptyRatio = 0.4;

// --- SparseHll implementation ---
uint32_t SparseHll::computeIndex(uint64_t hash, int8_t p)
{
    return ComputeIndex(hash, p);
}

int8_t SparseHll::computeValue(uint64_t hash, int8_t p)
{
    return ComputeValue(hash, p);
}

SparseHll::SparseHll(int8_t indexBitLength) : indexBitLength_(indexBitLength)
{
    if (indexBitLength_ < 4 || indexBitLength_ > 16) {
        indexBitLength_ = kDefaultHllIndexBitLength;
    }
}

bool SparseHll::insertHash(uint64_t hash)
{
    uint32_t idx = computeIndex(hash, indexBitLength_);
    int8_t val = computeValue(hash, indexBitLength_);
    uint32_t entry = (idx << 6) | (val & 0x3F);  // 26-bit index + 6-bit value

    auto it = std::lower_bound(entries_.begin(), entries_.end(), entry,
        [](uint32_t a, uint32_t b) {
            return (a >> 6) < (b >> 6);
        });
    if (it != entries_.end() && (*it >> 6) == (entry >> 6)) {
        int8_t oldVal = *it & 0x3F;
        if (val > oldVal) {
            *it = entry;  // keep max value per bucket
        }
    } else {
        entries_.insert(it, entry);
    }
    return overLimit();
}

int64_t SparseHll::cardinality() const
{
    int32_t m = 1 << indexBitLength_;
    if (entries_.empty()) {
        return 0;
    }
    int32_t zeros = m - static_cast<int32_t>(entries_.size());
    // Linear counting when empty buckets > 0.4*m; otherwise raw HLL + bias correction
    if (zeros > kLinearCountingMinEmptyRatio * m) {
        return static_cast<int64_t>(std::round(m * std::log(static_cast<double>(m) / zeros)));
    }
    double sum = static_cast<double>(zeros);
    for (uint32_t e : entries_) {
        int8_t v = e & 0x3F;
        sum += std::pow(2.0, -static_cast<double>(v));
    }
    double raw = Alpha(indexBitLength_, m) * static_cast<double>(m) * m / sum;
    raw = CorrectHllBias(raw, indexBitLength_);
    return static_cast<int64_t>(std::round(raw));
}

int32_t SparseHll::serializedSize() const
{
    return 2 + 4 + static_cast<int32_t>(entries_.size()) * kEntrySize;
}

void SparseHll::serialize(char *output) const
{
    output[0] = 1;  // type: sparse
    output[1] = indexBitLength_;
    int32_t n = static_cast<int32_t>(entries_.size());
    output[2] = (n >> 24) & 0xFF;
    output[3] = (n >> 16) & 0xFF;
    output[4] = (n >> 8) & 0xFF;
    output[5] = n & 0xFF;
    std::memcpy(output + 6, entries_.data(), n * kEntrySize);
}

bool SparseHll::canDeserialize(const char *data, int32_t len)
{
    return len >= 6 && data[0] == 1;
}

int8_t SparseHll::deserializeIndexBitLength(const char *data)
{
    return static_cast<int8_t>(data[1]);
}

void SparseHll::mergeWith(const SparseHll &other)
{
    for (uint32_t e : other.entries_) {
        uint32_t idx = e >> 6;
        int8_t val = e & 0x3F;
        auto it = std::lower_bound(entries_.begin(), entries_.end(), e,
            [](uint32_t a, uint32_t b) { return (a >> 6) < (b >> 6); });
        if (it != entries_.end() && (*it >> 6) == idx) {
            if (val > (*it & 0x3F)) {
                *it = e;
            }
        } else {
            entries_.insert(it, e);
        }
    }
}

void SparseHll::mergeWith(const char *serialized, int32_t len)
{
    if (!canDeserialize(serialized, len) || len < 6) {
        return;
    }
    int32_t n = (static_cast<uint8_t>(serialized[2]) << 24) | (static_cast<uint8_t>(serialized[3]) << 16) |
                (static_cast<uint8_t>(serialized[4]) << 8) | static_cast<uint8_t>(serialized[5]);
    int32_t payload = n * kEntrySize;
    if (6 + payload > len) {
        return;
    }
    for (int32_t i = 0; i < n; ++i) {
        uint32_t e;
        std::memcpy(&e, serialized + 6 + i * kEntrySize, kEntrySize);
        uint32_t idx = e >> 6;
        auto it = std::lower_bound(entries_.begin(), entries_.end(), e,
            [](uint32_t a, uint32_t b) { return (a >> 6) < (b >> 6); });
        if (it != entries_.end() && (*it >> 6) == idx) {
            if ((e & 0x3F) > (*it & 0x3F)) {
                *it = e;
            }
        } else {
            entries_.insert(it, e);
        }
    }
}

void SparseHll::toDense(DenseHll *denseHll) const
{
    denseHll->initialize(indexBitLength_);
    for (uint32_t e : entries_) {
        int32_t idx = e >> 6;
        int8_t val = e & 0x3F;
        denseHll->insert(idx, val);
    }
}

void SparseHll::mergeIntoDense(DenseHll *denseHll) const
{
    for (uint32_t e : entries_) {
        int32_t idx = e >> 6;
        int8_t val = e & 0x3F;
        denseHll->insert(idx, val);
    }
}

// --- DenseHll ---
int32_t DenseHll::estimateInMemorySize(int8_t indexBitLength)
{
    return 1 << indexBitLength;
}

uint32_t DenseHll::computeIndex(uint64_t hash, int8_t p)
{
    return ComputeIndex(hash, p);
}

int8_t DenseHll::computeValue(uint64_t hash, int8_t p)
{
    return ComputeValue(hash, p);
}

DenseHll::DenseHll(int8_t indexBitLength)
{
    initialize(indexBitLength);
}

DenseHll::DenseHll(const char *serialized, int32_t len)
{
    if (!canDeserialize(serialized, len)) {
        return;
    }
    indexBitLength_ = static_cast<int8_t>(serialized[1]);
    int32_t m = 1 << indexBitLength_;
    if (len < 2 + m) {
        return;
    }
    registers_.resize(m);
    std::memcpy(registers_.data(), serialized + 2, m);
}

void DenseHll::initialize(int8_t indexBitLength)
{
    if (indexBitLength < 4 || indexBitLength > 16) {
        indexBitLength = kDefaultHllIndexBitLength;
    }
    indexBitLength_ = indexBitLength;
    int32_t m = 1 << indexBitLength_;
    registers_.assign(m, 0);
}

void DenseHll::insertHash(uint64_t hash)
{
    uint32_t idx = computeIndex(hash, indexBitLength_);
    int8_t val = computeValue(hash, indexBitLength_);
    if (registers_[idx] < val) {
        registers_[idx] = val;
    }
}

void DenseHll::insert(int32_t index, int8_t value)
{
    if (index >= 0 && index < static_cast<int32_t>(registers_.size()) && registers_[index] < value) {
        registers_[index] = value;
    }
}

int64_t DenseHll::cardinality() const
{
    int32_t m = static_cast<int32_t>(registers_.size());
    if (m == 0) {
        return 0;
    }
    double sum = 0;
    int32_t zeros = 0;
    for (int8_t v : registers_) {
        sum += std::pow(2.0, -static_cast<double>(v));
        if (v == 0) {
            ++zeros;
        }
    }
    // Linear counting when zeros > 0.4*m; else raw HLL + bias correction
    if (zeros > kLinearCountingMinEmptyRatio * m) {
        return static_cast<int64_t>(std::round(m * std::log(static_cast<double>(m) / zeros)));
    }
    double raw = Alpha(indexBitLength_, m) * static_cast<double>(m) * m / sum;
    raw = CorrectHllBias(raw, indexBitLength_);
    return static_cast<int64_t>(std::round(raw));
}

int32_t DenseHll::serializedSize() const
{
    return 2 + static_cast<int32_t>(registers_.size());
}

void DenseHll::serialize(char *output) const
{
    output[0] = 2;  // type: dense
    output[1] = indexBitLength_;
    std::memcpy(output + 2, registers_.data(), registers_.size());
}

bool DenseHll::canDeserialize(const char *data, int32_t len)
{
    return len >= 2 && data[0] == 2 && data[1] >= 4 && data[1] <= 16;
}

int8_t DenseHll::deserializeIndexBitLength(const char *data)
{
    return static_cast<int8_t>(data[1]);
}

void DenseHll::mergeWith(const DenseHll &other)
{
    if (registers_.size() != other.registers_.size()) {
        return;
    }
    for (size_t i = 0; i < registers_.size(); ++i) {
        if (other.registers_[i] > registers_[i]) {
            registers_[i] = other.registers_[i];
        }
    }
}

void DenseHll::mergeWith(const char *serialized, int32_t len)
{
    if (!canDeserialize(serialized, len)) {
        return;
    }
    int8_t p = static_cast<int8_t>(serialized[1]);
    int32_t m = 1 << p;
    if (len < 2 + m) {
        return;
    }
    if (registers_.empty()) {
        initialize(p);
    }
    for (int32_t i = 0; i < m; ++i) {
        int8_t v = static_cast<int8_t>(serialized[2 + i]);
        if (v > registers_[i]) {
            registers_[i] = v;
        }
    }
}

// --- HllAccumulator ---
HllAccumulator::HllAccumulator(int8_t indexBitLength)
    : indexBitLength_(indexBitLength >= 4 && indexBitLength <= 16 ? indexBitLength : kDefaultHllIndexBitLength),
      sparseHll_(indexBitLength_),
      denseHll_(indexBitLength_)
{
    sparseHll_.setSoftMemoryLimit(static_cast<uint32_t>(DenseHll::estimateInMemorySize(indexBitLength_)));
}

void HllAccumulator::toDense()
{
    isSparse_ = false;
    denseHll_.initialize(indexBitLength_);
    sparseHll_.toDense(&denseHll_);
    sparseHll_.reset();
}

void HllAccumulator::insertHash(uint64_t hash)
{
    if (isSparse_) {
        if (sparseHll_.insertHash(hash)) {
            toDense();
        }
    } else {
        denseHll_.insertHash(hash);
    }
}

int64_t HllAccumulator::cardinality() const
{
    return isSparse_ ? sparseHll_.cardinality() : denseHll_.cardinality();
}

void HllAccumulator::mergeWith(const char *serialized, int32_t len)
{
    if (len < 2) {
        return;
    }
    if (HllCanDeserializeBoolean(serialized, len)) {
        return;  // Boolean partial is not merged into HLL accumulator (handled in HllMergeSerialized)
    }
    if (SparseHll::canDeserialize(serialized, len)) {
        if (indexBitLength_ < 0) {
            indexBitLength_ = SparseHll::deserializeIndexBitLength(serialized);
            sparseHll_ = SparseHll(indexBitLength_);
            sparseHll_.setSoftMemoryLimit(
                static_cast<uint32_t>(DenseHll::estimateInMemorySize(indexBitLength_)));
        }
        if (isSparse_) {
            sparseHll_.mergeWith(serialized, len);
            if (sparseHll_.overLimit()) {
                toDense();
            }
        } else {
            SparseHll other(indexBitLength_);
            other.mergeWith(serialized, len);
            other.mergeIntoDense(&denseHll_);
        }
    } else if (DenseHll::canDeserialize(serialized, len)) {
        if (indexBitLength_ < 0) {
            indexBitLength_ = DenseHll::deserializeIndexBitLength(serialized);
            denseHll_.initialize(indexBitLength_);
        }
        if (isSparse_) {
            toDense();
        }
        denseHll_.mergeWith(serialized, len);
    }
}

void HllAccumulator::mergeWith(const HllAccumulator &other)
{
    if (other.indexBitLength_ < 0) {
        return;
    }
    if (indexBitLength_ < 0) {
        indexBitLength_ = other.indexBitLength_;
        sparseHll_ = SparseHll(indexBitLength_);
        sparseHll_.setSoftMemoryLimit(
            static_cast<uint32_t>(DenseHll::estimateInMemorySize(indexBitLength_)));
    }
    if (other.isSparse_) {
        if (isSparse_) {
            sparseHll_.mergeWith(other.sparseHll_);
            if (sparseHll_.overLimit()) {
                toDense();
            }
        } else {
            other.sparseHll_.mergeIntoDense(&denseHll_);
        }
    } else {
        if (isSparse_) {
            toDense();
        }
        denseHll_.mergeWith(other.denseHll_);
    }
}

int32_t HllAccumulator::serializedSize() const
{
    return isSparse_ ? sparseHll_.serializedSize() : denseHll_.serializedSize();
}

void HllAccumulator::serialize(char *output) const
{
    if (isSparse_) {
        sparseHll_.serialize(output);
    } else {
        denseHll_.serialize(output);
    }
}

bool HllAccumulator::canDeserialize(const char *data, int32_t len)
{
    return len >= 2 && (data[0] == 0 || data[0] == 1 || data[0] == 2);
}

/**
 * Validates max standard error for approx_count_distinct(col, maxStandardError). Throws if out of range.
 *
 * @param maxStandardError: User-provided max standard error (e.g. 0.023).
 */
void HllCheckMaxStandardError(double maxStandardError)
{
    if (maxStandardError < kLowestMaxStandardError || maxStandardError > kHighestMaxStandardError) {
        throw omniruntime::exception::OmniException("ILLEGAL_INPUT",
            "approx_count_distinct max standard error must be in [" +
                std::to_string(kLowestMaxStandardError) + ", " + std::to_string(kHighestMaxStandardError) + "]");
    }
}

/**
 * Maps max standard error to HLL index bit length (p). Larger p gives lower error, more memory.
 *
 * @param maxStandardError: Desired max standard error.
 * @return p in [11, 16].
 */
int8_t HllToIndexBitLength(double maxStandardError)
{
    if (maxStandardError <= 0.004) {
        return 16;
    }
    if (maxStandardError <= 0.008) {
        return 15;
    }
    if (maxStandardError <= 0.016) {
        return 14;
    }
    if (maxStandardError <= 0.032) {
        return 13;
    }
    return 11;  // ~0.023
}

/**
 * Returns approximate standard error for given index bit length (p).
 *
 * @param indexBitLength: HLL parameter p.
 * @return 1.04 / sqrt(2^p).
 */
double HllToStandardError(int8_t indexBitLength)
{
    int32_t m = 1 << indexBitLength;
    return 1.04 / std::sqrt(m);
}

/**
 * Merges incoming serialized partial into current buffer. Handles boolean+boolean (OR), boolean+HLL (copy incoming),
 * HLL+boolean (ignore), HLL+HLL (deserialize both into HllAccumulator and re-serialize).
 */
void HllMergeSerialized(char *current, int32_t *currentLen, int32_t currentCapacity,
    const char *incoming, int32_t incomingLen)
{
    if (incomingLen < 2) {
        return;
    }
    if (*currentLen < 2) {
        if (currentCapacity >= incomingLen) {
            std::memcpy(current, incoming, incomingLen);
            *currentLen = incomingLen;
        }
        return;
    }
    if (current[0] == 0 && incoming[0] == 0) {
        if (incomingLen >= 2) {
            current[1] |= incoming[1];  // boolean OR
            *currentLen = 2;
        }
        return;
    }
    if (current[0] == 0 && incoming[0] != 0) {
        current[0] = incoming[0];
        if (currentCapacity >= incomingLen) {
            std::memcpy(current + 1, incoming + 1, incomingLen - 1);
            *currentLen = incomingLen;
        }
        return;
    }
    if (current[0] != 0 && incoming[0] == 0) {
        return;  // ignore boolean when state is already HLL
    }
    int8_t p = kDefaultHllIndexBitLength;
    if (*currentLen >= 2 && SparseHll::canDeserialize(current, *currentLen)) {
        p = SparseHll::deserializeIndexBitLength(current);
    } else if (*currentLen >= 2 && DenseHll::canDeserialize(current, *currentLen)) {
        p = DenseHll::deserializeIndexBitLength(current);
    }
    HllAccumulator acc(p);
    acc.mergeWith(current, *currentLen);
    acc.mergeWith(incoming, incomingLen);
    int32_t need = acc.serializedSize();
    if (need <= currentCapacity) {
        acc.serialize(current);
        *currentLen = need;
    }
}

/**
 * Computes cardinality from serialized form: boolean (0), Sparse (1), or Dense (2).
 *
 * @param data: Serialized buffer.
 * @param len: Length.
 * @return Estimated distinct count.
 */
int64_t HllCardinalityFromSerialized(const char *data, int32_t len)
{
    if (len < 2) {
        return 0;
    }
    if (data[0] == 0) {
        return HllBooleanCardinality(static_cast<int8_t>(data[1]));
    }
    if (SparseHll::canDeserialize(data, len)) {
        SparseHll sparse(SparseHll::deserializeIndexBitLength(data));
        sparse.mergeWith(data, len);
        return sparse.cardinality();
    }
    if (DenseHll::canDeserialize(data, len)) {
        DenseHll dense(data, len);
        return dense.cardinality();
    }
    return 0;
}

}  // namespace op
}  // namespace omniruntime
