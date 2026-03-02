/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: KLL sketch template implementation (insert, merge, serialize, estimateQuantile). Ported from Velox, no Folly.
 * Used by ApproxPercentile aggregator for BYTE/SHORT/INT/LONG/FLOAT/DOUBLE value types.
 */
#pragma once

#include <algorithm>
#include <cmath>
#include <cstring>
#include <numeric>
#include <queue>
#include "util/omni_exception.h"

namespace omniruntime {
namespace op {
namespace kll {
namespace detail {

template <typename T, typename RandomBit>
void randomlyHalveDown(T* buf, uint32_t start, uint32_t length, RandomBit& randomBit) {
    (void)(length & 1);
    const uint32_t halfLength = length / 2;
    const uint32_t offset = randomBit();
    uint32_t j = start + offset;
    for (uint32_t i = start; i < start + halfLength; i++) {
        buf[i] = buf[j];
        j += 2;
    }
}

template <typename T, typename RandomBit>
void randomlyHalveUp(T* buf, uint32_t start, uint32_t length, RandomBit& randomBit) {
    const uint32_t halfLength = length / 2;
    const uint32_t offset = randomBit();
    uint32_t j = (start + length) - 1 - offset;
    for (uint32_t i = (start + length) - 1; i >= (start + halfLength); i--) {
        buf[i] = buf[j];
        j -= 2;
    }
}

template <typename T, typename C>
void mergeOverlap(T* buf, uint32_t startA, uint32_t lenA, uint32_t startB, uint32_t lenB,
    uint32_t startC, C compare) {
    const uint32_t limA = startA + lenA;
    const uint32_t limB = startB + lenB;
    uint32_t a = startA, b = startB, c = startC;
    while (a < limA && b < limB) {
        if (compare(buf[a], buf[b])) buf[c++] = buf[a++];
        else buf[c++] = buf[b++];
    }
    while (a < limA) buf[c++] = buf[a++];
    while (b < limB) buf[c++] = buf[b++];
}

struct CompressResult {
    uint8_t finalNumLevels;
    uint32_t finalCapacity;
    uint32_t finalNumItems;
};

template <typename T, typename C, typename RandomBit>
CompressResult generalCompress(uint32_t k, uint8_t numLevelsIn, T* items, uint32_t* inLevels,
    uint32_t* outLevels, bool isLevelZeroSorted, RandomBit& randomBit) {
    uint8_t currentNumLevels = numLevelsIn;
    uint32_t currentItemCount = inLevels[numLevelsIn] - inLevels[0];
    uint32_t targetItemCount = computeTotalCapacity(k, currentNumLevels);
    outLevels[0] = 0;
    for (uint8_t level = 0; level < currentNumLevels; ++level) {
        if (level == currentNumLevels - 1) inLevels[level + 2] = inLevels[level + 1];
        const uint32_t rawBeg = inLevels[level], rawLim = inLevels[level + 1], rawPop = rawLim - rawBeg;
        if (currentItemCount < targetItemCount || rawPop < levelCapacity(k, currentNumLevels, level)) {
            std::move(&items[rawBeg], &items[rawLim], &items[outLevels[level]]);
            outLevels[level + 1] = outLevels[level] + rawPop;
        } else {
            const uint32_t popAbove = inLevels[level + 2] - rawLim;
            const bool oddPop = rawPop & 1;
            const uint32_t adjBeg = rawBeg + oddPop, adjPop = rawPop - oddPop, halfAdjPop = adjPop / 2;
            if (oddPop) {
                items[outLevels[level]] = std::move(items[rawBeg]);
                outLevels[level + 1] = outLevels[level] + 1;
            } else outLevels[level + 1] = outLevels[level];
            if (level == 0 && !isLevelZeroSorted)
                std::sort(&items[adjBeg], &items[adjBeg + adjPop], C());
            if (popAbove == 0) randomlyHalveUp(items, adjBeg, adjPop, randomBit);
            else {
                randomlyHalveDown(items, adjBeg, adjPop, randomBit);
                mergeOverlap(items, adjBeg, halfAdjPop, rawLim, popAbove, adjBeg + halfAdjPop, C());
            }
            currentItemCount -= halfAdjPop;
            inLevels[level + 1] -= halfAdjPop;
            if (level == currentNumLevels - 1) {
                ++currentNumLevels;
                targetItemCount += levelCapacity(k, currentNumLevels, 0);
            }
        }
    }
    return {currentNumLevels, targetItemCount, currentItemCount};
}

template <typename T>
void write(T value, char* out, size_t& offset) {
    *reinterpret_cast<T*>(out + offset) = value;
    offset += sizeof(T);
}
template <typename T, typename A>
void writeVector(const std::vector<T, A>& data, char* out, size_t& offset) {
    write(data.size(), out, offset);
    size_t bytes = sizeof(T) * data.size();
    memcpy(out + offset, data.data(), bytes);
    offset += bytes;
}
template <typename T>
void read(const char* data, size_t& offset, T& out) {
    out = *reinterpret_cast<const T*>(data + offset);
    offset += sizeof(T);
}
template <typename T>
void readVector(const char* data, size_t& offset, std::vector<T>& out) {
    size_t size;
    read(data, offset, size);
    out.resize(size);
    if (size) { memcpy(out.data(), data + offset, sizeof(T) * size); offset += sizeof(T) * size; }
}

}  // namespace detail

template <typename T, typename A, typename C>
KllSketch<T, A, C>::KllSketch(uint32_t k, const A& allocator, uint32_t seed)
    : k_(k), allocator_(allocator), randomBit_(seed ? seed : 1u),
      n_(0), items_(allocator), levels_(2, AllocU32(allocator)), isLevelZeroSorted_(false) {
    levels_[0] = 0;
    levels_[1] = 0;
}

template <typename T, typename A, typename C>
KllSketch<T, A, C>::KllSketch(const A& allocator, uint32_t seed)
    : k_(0), allocator_(allocator), randomBit_(seed ? seed : 1u),
      n_(0), items_(allocator), levels_(AllocU32(allocator)), isLevelZeroSorted_(false) {}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::setK(uint32_t k) {
    if (k_ != k) {
        if (n_ != 0)
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "KllSketch::setK only when n==0");
        k_ = k;
        levels_.resize(2);
        levels_[0] = levels_[1] = 0;
    }
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::insert(T value) {
    if (n_ == 0) minValue_ = maxValue_ = value;
    else { minValue_ = std::min(minValue_, value, C()); maxValue_ = std::max(maxValue_, value, C()); }
    doInsert(value);
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::doInsert(T value) {
    if (items_.size() < k_ && numLevels() == 1) {
        items_.push_back(value);
        ++levels_[1];
    } else {
        items_[insertPosition()] = value;
    }
    ++n_;
    isLevelZeroSorted_ = false;
}

template <typename T, typename A, typename C>
uint32_t KllSketch<T, A, C>::insertPosition() {
    if (levels_[0] == 0) {
        const uint8_t level = findLevelToCompact();
        if (level == numLevels() - 1) {
            int delta = static_cast<int>(detail::computeTotalCapacity(k_, numLevels())) - static_cast<int>(items_.size());
            if (delta > 0) { shiftItems(static_cast<uint32_t>(delta)); return --levels_[0]; }
            addEmptyTopLevelToCompletelyFullSketch();
        }
        const uint32_t rawBeg = levels_[level], rawLim = levels_[level + 1];
        const uint32_t popAbove = levels_[level + 2] - rawLim, rawPop = rawLim - rawBeg;
        const bool oddPop = rawPop & 1;
        const uint32_t adjBeg = rawBeg + oddPop, adjPop = rawPop - oddPop, halfAdjPop = adjPop / 2;
        if (level == 0 && !isLevelZeroSorted_)
            std::sort(items_.data() + adjBeg, items_.data() + adjBeg + adjPop, C());
        if (popAbove == 0) detail::randomlyHalveUp(items_.data(), adjBeg, adjPop, randomBit_);
        else {
            detail::randomlyHalveDown(items_.data(), adjBeg, adjPop, randomBit_);
            detail::mergeOverlap(items_.data(), adjBeg, halfAdjPop, rawLim, popAbove, adjBeg + halfAdjPop, C());
        }
        levels_[level + 1] -= halfAdjPop;
        if (oddPop) {
            levels_[level] = levels_[level + 1] - 1;
            if (levels_[level] != rawBeg) items_[levels_[level]] = std::move(items_[rawBeg]);
        } else levels_[level] = levels_[level + 1];
        if (level > 0) {
            const uint32_t amount = rawBeg - levels_[0];
            std::move_backward(items_.data() + levels_[0], items_.data() + levels_[0] + amount,
                items_.data() + levels_[0] + halfAdjPop + amount);
            for (uint8_t lvl = 0; lvl < level; lvl++) levels_[lvl] += halfAdjPop;
        }
    }
    return --levels_[0];
}

template <typename T, typename A, typename C>
int KllSketch<T, A, C>::findLevelToCompact() const {
    for (int level = 0;; ++level) {
        uint32_t pop = levels_[level + 1] - levels_[level];
        if (pop >= detail::levelCapacity(k_, numLevels(), level) || level + 1 == numLevels()) return level;
    }
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::addEmptyTopLevelToCompletelyFullSketch() {
    const uint32_t deltaCap = detail::levelCapacity(k_, numLevels() + 1, 0);
    shiftItems(deltaCap);
    levels_.push_back(levels_.back());
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::shiftItems(uint32_t delta) {
    size_t oldTotal = items_.size();
    items_.resize(oldTotal + delta);
    std::move_backward(items_.begin(), items_.begin() + oldTotal, items_.end());
    for (auto& lv : levels_) lv += delta;
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::finish() {
    if (!isLevelZeroSorted_) {
        std::sort(items_.data() + levels_[0], items_.data() + levels_[1], C());
        isLevelZeroSorted_ = true;
    }
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::compact() {
    finish();
    uint32_t kk = 0;
    std::vector<T> tmp;
    uint32_t beg = levels_[0], end = levels_[1];
    levels_[0] = 0;
    for (int i = 0; i < numLevels(); ++i) {
        if (!tmp.empty()) {
            uint32_t beg2 = beg - static_cast<uint32_t>(tmp.size());
            std::copy(tmp.begin(), tmp.end(), items_.begin() + beg2);
            std::inplace_merge(items_.data() + beg2, items_.data() + beg, items_.data() + end, C());
            beg = beg2;
            tmp.clear();
        }
        for (uint32_t j = beg; j < end;) {
            if (j + 1 < end && !C()(items_[j], items_[j+1]) && !C()(items_[j+1], items_[j])) {
                tmp.push_back(items_[j]);
                j += 2;
            } else { items_[kk++] = items_[j++]; }
        }
        if (i + 1 == numLevels() && !tmp.empty()) levels_.push_back(levels_.back());
        beg = end;
        if (i + 1 < numLevels()) end = levels_[i + 2];
        levels_[i + 1] = kk;
    }
    items_.resize(kk);
}

template <typename T, typename A, typename C>
std::vector<std::pair<T, uint64_t>> KllSketch<T, A, C>::getFrequencies() const {
    std::vector<std::pair<T, uint64_t>> entries;
    entries.reserve(levels_.back());
    for (int level = 0; level < numLevels(); ++level) {
        size_t oldLen = entries.size();
        for (uint32_t i = levels_[level]; i < levels_[level + 1]; ++i)
            entries.emplace_back(items_[i], 1ULL << level);
        if (oldLen > 0)
            std::inplace_merge(entries.begin(), entries.begin() + oldLen, entries.end(),
                [](const auto& x, const auto& y) { return C()(x.first, y.first); });
    }
    size_t kk = 0;
    for (size_t i = 0; i < entries.size();) {
        entries[kk] = entries[i];
        size_t j = i + 1;
        while (j < entries.size() && !C()(entries[j].first, entries[i].first) && !C()(entries[i].first, entries[j].first))
            entries[kk].second += entries[j++].second;
        ++kk; i = j;
    }
    entries.resize(kk);
    return entries;
}

template <typename T, typename A, typename C>
T KllSketch<T, A, C>::estimateQuantile(double fraction) const {
    T out;
    estimateQuantiles(&fraction, 1, &out);
    return out;
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::estimateQuantiles(const double* quantiles, size_t numQuantiles, T* out) const {
    if (n_ == 0)
        throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "estimateQuantiles on empty sketch");
    auto entries = getFrequencies();
    uint64_t totalWeight = 0;
    for (auto& e : entries) { totalWeight += e.second; e.second = totalWeight; }
    for (size_t i = 0; i < numQuantiles; ++i) {
        double q = quantiles[i];
        if (q <= 0.0) { out[i] = minValue_; continue; }
        if (q >= 1.0) { out[i] = maxValue_; continue; }
        uint64_t threshold = static_cast<uint64_t>(std::ceil(q * totalWeight));
        if (threshold > totalWeight) threshold = totalWeight;
        auto it = std::lower_bound(entries.begin(), entries.end(), std::make_pair(T(), threshold),
            [](const std::pair<T, uint64_t>& x, const std::pair<T, uint64_t>& y) { return x.second < y.second; });
        out[i] = (it == entries.end()) ? entries.back().first : it->first;
    }
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::mergeViews(const detail::View<T>* views, size_t numViews) {
    size_t newN = n_;
    for (size_t i = 0; i < numViews; ++i) {
        const auto& other = views[i];
        if (other.n == 0) continue;
        if (newN == 0) { minValue_ = other.minValue; maxValue_ = other.maxValue; }
        else { minValue_ = std::min(minValue_, other.minValue, C()); maxValue_ = std::max(maxValue_, other.maxValue, C()); }
        newN += other.n;
    }
    if (newN == n_) return;
    size_t extra = 0;
    for (size_t i = 0; i < numViews; ++i) extra += views[i].safeLevelSize(0);
    items_.reserve(items_.size() + extra);
    for (size_t i = 0; i < numViews; ++i) {
        const auto& other = views[i];
        if (other.n == 0) continue;
        for (uint32_t j = other.levelsData[0]; j < other.levelsData[1]; ++j) doInsert(other.itemsData[j]);
    }
    uint32_t tmpNumItems = getNumRetained();
    uint8_t provisionalNumLevels = numLevels();
    for (size_t i = 0; i < numViews; ++i) {
        const auto& other = views[i];
        if (other.numLevels() >= 2) {
            tmpNumItems += other.levelsData[other.levelsSize - 1] - other.levelsData[1];
            provisionalNumLevels = std::max(provisionalNumLevels, other.numLevels());
        }
    }
    if (tmpNumItems > getNumRetained()) {
        std::vector<T, A> workbuf(tmpNumItems, allocator_);
        const uint8_t ub = 1 + detail::floorLog2(newN, 1);
        const size_t workLevelsSize = ub + 2;
        std::vector<uint32_t, AllocU32> worklevels(workLevelsSize, 0, AllocU32(allocator_));
        std::vector<uint32_t, AllocU32> outlevels(workLevelsSize, 0, AllocU32(allocator_));
        worklevels[0] = 0;
        std::move(items_.data() + levels_[0], items_.data() + levels_[1], workbuf.data());
        worklevels[1] = safeLevelSize(0);
        for (uint8_t lvl = 1; lvl < provisionalNumLevels; ++lvl) {
            using Entry = std::pair<const T*, const T*>;
            auto gt = [this](const Entry& x, const Entry& y) { return C()(*y.first, *x.first); };
            std::priority_queue<Entry, std::vector<Entry>, decltype(gt)> pq(gt);
            if (uint32_t sz = safeLevelSize(lvl); sz > 0)
                pq.emplace(items_.data() + levels_[lvl], items_.data() + levels_[lvl] + sz);
            for (size_t i = 0; i < numViews; ++i) {
                if (uint32_t sz = views[i].safeLevelSize(lvl); sz > 0)
                    pq.emplace(&views[i].itemsData[views[i].levelsData[lvl]],
                        &views[i].itemsData[views[i].levelsData[lvl]] + sz);
            }
            uint32_t outIndex = worklevels[lvl];
            while (!pq.empty()) {
                auto [s, t] = pq.top(); pq.pop();
                workbuf[outIndex++] = *s++;
                if (s < t) pq.emplace(s, t);
            }
            worklevels[lvl + 1] = outIndex;
        }
        auto result = detail::generalCompress<T, C>(k_, provisionalNumLevels, workbuf.data(),
            worklevels.data(), outlevels.data(), isLevelZeroSorted_, randomBit_);
        items_.resize(result.finalCapacity);
        uint32_t freeSpaceAtBottom = result.finalCapacity - result.finalNumItems;
        std::move(workbuf.data() + outlevels[0], workbuf.data() + outlevels[0] + result.finalNumItems,
            items_.data() + freeSpaceAtBottom);
        levels_.resize(result.finalNumLevels + 1);
        int32_t offset = static_cast<int32_t>(freeSpaceAtBottom) - static_cast<int32_t>(outlevels[0]);
        for (size_t lvl = 0; lvl < levels_.size(); ++lvl)
            levels_[lvl] = outlevels[lvl] + offset;
    }
    n_ = newN;
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::merge(const KllSketch<T, A, C>& other) {
    detail::View<T> v = other.toView();
    mergeViews(&v, 1);
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::mergeDeserialized(const char* data) {
    size_t i = 0;
    int16_t version;
    detail::read(data, i, version);
    if (version != detail::kVersion)
        throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "KLL unsupported version");
    uint32_t kk; size_t nn; T minV, maxV;
    detail::read(data, i, kk); detail::read(data, i, nn);
    detail::read(data, i, minV); detail::read(data, i, maxV);
    std::vector<T> items; std::vector<uint32_t> lvls;
    detail::readVector(data, i, items); detail::readVector(data, i, lvls);
    detail::View<T> v{kk, nn, minV, maxV, items.data(), items.size(), lvls.data(), lvls.size()};
    mergeViews(&v, 1);
}

template <typename T, typename A, typename C>
typename detail::View<T> KllSketch<T, A, C>::toView() const {
    return detail::View<T>{k_, n_, minValue_, maxValue_,
        items_.data(), items_.size(), levels_.data(), levels_.size()};
}

template <typename T, typename A, typename C>
KllSketch<T, A, C> KllSketch<T, A, C>::fromView(const detail::View<T>& view, const A& allocator, uint32_t seed) {
    KllSketch<T, A, C> ans(allocator, seed);
    ans.k_ = view.k; ans.n_ = view.n; ans.minValue_ = view.minValue; ans.maxValue_ = view.maxValue;
    ans.items_.assign(view.itemsData, view.itemsData + view.itemsSize);
    ans.levels_.assign(view.levelsData, view.levelsData + view.levelsSize);
    ans.isLevelZeroSorted_ = std::is_sorted(ans.items_.data() + ans.levels_[0],
        ans.items_.data() + ans.levels_[1], C());
    return ans;
}

template <typename T, typename A, typename C>
size_t KllSketch<T, A, C>::serializedByteSize() const {
    return sizeof(detail::kVersion) + sizeof(k_) + sizeof(n_) + sizeof(minValue_) + sizeof(maxValue_)
        + sizeof(size_t) + sizeof(T) * items_.size() + sizeof(size_t) + sizeof(uint32_t) * levels_.size();
}

template <typename T, typename A, typename C>
void KllSketch<T, A, C>::serialize(char* out) const {
    size_t i = 0;
    detail::write(detail::kVersion, out, i);
    detail::write(k_, out, i);
    detail::write(n_, out, i);
    detail::write(minValue_, out, i);
    detail::write(maxValue_, out, i);
    detail::writeVector(items_, out, i);
    detail::writeVector(levels_, out, i);
}

template <typename T, typename A, typename C>
KllSketch<T, A, C> KllSketch<T, A, C>::deserialize(const char* data, const A& allocator, uint32_t seed) {
    size_t i = 0;
    int16_t version; detail::read(data, i, version);
    if (version != detail::kVersion)
        throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "KLL unsupported version");
    uint32_t kk; size_t nn; T minV, maxV;
    detail::read(data, i, kk); detail::read(data, i, nn);
    detail::read(data, i, minV); detail::read(data, i, maxV);
    std::vector<T> items; std::vector<uint32_t> lvls;
    detail::readVector(data, i, items); detail::readVector(data, i, lvls);
    detail::View<T> v{kk, nn, minV, maxV, items.data(), items.size(), lvls.data(), lvls.size()};
    return fromView(v, allocator, seed);
}

template <typename T, typename A, typename C>
KllSketch<T, A, C> KllSketch<T, A, C>::fromRepeatedValue(T value, size_t count, uint32_t k,
    const A& allocator, uint32_t seed) {
    int numLevels = 0;
    for (size_t x = count; x > 0; x >>= 1) ++numLevels;
    if (numLevels > detail::kMaxLevel)
        throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "KLL fromRepeatedValue too large");
    KllSketch<T, A, C> ans(allocator, seed);
    ans.k_ = k; ans.n_ = count; ans.minValue_ = ans.maxValue_ = value; ans.isLevelZeroSorted_ = true;
    ans.levels_.resize(static_cast<size_t>(numLevels) + 1);
    size_t c = count;
    for (int i = 1; i <= numLevels; ++i) {
        ans.levels_[i] = ans.levels_[i - 1] + (c & 1);
        c >>= 1;
    }
    ans.items_.resize(ans.levels_.back(), value);
    return ans;
}

}  // namespace kll
}  // namespace op
}  // namespace omniruntime
