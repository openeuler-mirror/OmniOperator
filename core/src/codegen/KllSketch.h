/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: KLL sketch for approximate quantiles. Used by ApproxPercentile aggregator.
 * Ported from Velox; serialization format is compatible for merge across partials.
 */
#ifndef OMNI_RUNTIME_KLL_SKETCH_H
#define OMNI_RUNTIME_KLL_SKETCH_H

#include <cstdint>
#include <cstddef>
#include <functional>
#include <random>
#include <vector>
#include <cmath>
#include <cstring>
#include "util/omni_exception.h"

namespace omniruntime {
namespace op {
namespace kll {
namespace detail {

template <typename T>
struct View {
    uint32_t k;
    size_t n;
    T minValue;
    T maxValue;
    const T* itemsData;
    size_t itemsSize;
    const uint32_t* levelsData;
    size_t levelsSize;

    uint8_t numLevels() const {
        return levelsSize >= 2 ? static_cast<uint8_t>(levelsSize - 1) : 0;
    }
    uint32_t safeLevelSize(uint8_t level) const {
        return (level + 1 < levelsSize) ? (levelsData[level + 1] - levelsData[level]) : 0;
    }
};

constexpr uint8_t kMaxLevel = 60;
constexpr int16_t kVersion = 1;

uint32_t computeTotalCapacity(uint32_t k, uint8_t numLevels);
uint32_t levelCapacity(uint32_t k, uint8_t numLevels, uint8_t height);
uint8_t floorLog2(uint64_t p, uint64_t q);
uint64_t sumSampleWeights(uint8_t numLevels, const uint32_t* levels);

}  // namespace detail

constexpr uint32_t kDefaultK = 200;
uint32_t kFromEpsilon(double epsilon);

template <typename T, typename Allocator = std::allocator<T>, typename Compare = std::less<T>>
struct KllSketch {
    explicit KllSketch(uint32_t k = kDefaultK, const Allocator& alloc = Allocator(), uint32_t seed = 0u);

    void setK(uint32_t k);
    void insert(T value);
    void compact();
    void merge(const KllSketch<T, Allocator, Compare>& other);
    void finish();
    T estimateQuantile(double quantile) const;
    void estimateQuantiles(const double* quantiles, size_t numQuantiles, T* out) const;

    size_t totalCount() const { return n_; }
    uint32_t k() const { return k_; }
    size_t serializedByteSize() const;
    void serialize(char* out) const;
    static KllSketch deserialize(const char* data, const Allocator& alloc = Allocator(), uint32_t seed = 0u);
    static KllSketch fromRepeatedValue(T value, size_t count, uint32_t k = kDefaultK,
        const Allocator& alloc = Allocator(), uint32_t seed = 0u);
    void mergeDeserialized(const char* data);
    void mergeViews(const detail::View<T>* views, size_t numViews);
    detail::View<T> toView() const;
    static KllSketch fromView(const detail::View<T>& view, const Allocator& alloc, uint32_t seed);

private:
    using AllocU32 = typename std::allocator_traits<Allocator>::template rebind_alloc<uint32_t>;
    KllSketch(const Allocator& alloc, uint32_t seed);
    void doInsert(T value);
    uint32_t insertPosition();
    int findLevelToCompact() const;
    void addEmptyTopLevelToCompletelyFullSketch();
    void shiftItems(uint32_t delta);
    uint8_t numLevels() const { return levels_.size() >= 2u ? static_cast<uint8_t>(levels_.size() - 1) : 0; }
    uint32_t getNumRetained() const { return levels_.empty() ? 0 : levels_.back() - levels_[0]; }
    uint32_t safeLevelSize(uint8_t level) const {
        return (level + 1 < levels_.size()) ? (levels_[level + 1] - levels_[level]) : 0;
    }
    std::vector<std::pair<T, uint64_t>> getFrequencies() const;

    uint32_t k_;
    Allocator allocator_;
    std::independent_bits_engine<std::minstd_rand0, 1, uint32_t> randomBit_;
    size_t n_;
    T minValue_;
    T maxValue_;
    std::vector<T, Allocator> items_;
    std::vector<uint32_t, AllocU32> levels_;
    bool isLevelZeroSorted_;
};

}  // namespace kll
}  // namespace op
}  // namespace omniruntime

#include "KllSketch-inl.h"
#endif  // OMNI_RUNTIME_KLL_SKETCH_H
