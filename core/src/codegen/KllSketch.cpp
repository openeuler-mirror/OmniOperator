/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: KLL sketch non-template implementation (epsilon->k, level capacity, weights).
 * Used by ApproxPercentile for approximate quantile sketch sizing and level layout.
 */
#include "KllSketch.h"
#include <mutex>

namespace omniruntime {
namespace op {
namespace kll {

namespace {
constexpr double kEpsilonMin = 0.0;
constexpr double kEpsilonMax = 1.0;
constexpr double kKllFormulaCoeff = 1.0285;
constexpr double kKllFormulaBase = 2.296;
}  // namespace

uint32_t kFromEpsilon(double eps) {
    if (!(eps > kEpsilonMin && eps < kEpsilonMax)) {
        throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "KLL epsilon must be in (0, 1)");
    }
    if (eps == 0) {
        throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "KLL epsilon must be in (0, 1)");
    }
    const double ratio = kKllFormulaBase / eps;
    const double logRatio = std::log(ratio);
    const double exponent = kKllFormulaCoeff * logRatio;
    const double kFloat = std::exp(exponent);
    return static_cast<uint32_t>(std::ceil(kFloat));
}

namespace detail {

namespace {
constexpr uint8_t kMinBufferWidth = 8;
constexpr double kLevelShrinkFactor = 2.0 / 3.0;

double levelScaleFactor(int exponent) {
    static double cache[kMaxLevel];
    static std::once_flag once;
    std::call_once(once, []() {
        cache[0] = 1.0;
        for (int i = 1; i < kMaxLevel; ++i) {
            cache[i] = cache[i - 1] * kLevelShrinkFactor;
        }
    });
    if (exponent >= 0 && exponent < kMaxLevel) {
        return cache[exponent];
    }
    return std::pow(kLevelShrinkFactor, static_cast<double>(exponent));
}
}  // namespace

uint32_t levelCapacity(uint32_t k, uint8_t numLevels, uint8_t height) {
    if (height >= numLevels || numLevels > kMaxLevel) {
        return 0;
    }
    const int exponent = static_cast<int>(numLevels) - static_cast<int>(height) - 1;
    const double scale = levelScaleFactor(exponent);
    const uint32_t rawCap = static_cast<uint32_t>(static_cast<double>(k) * scale);
    return rawCap < kMinBufferWidth ? kMinBufferWidth : rawCap;
}

uint32_t computeTotalCapacity(uint32_t k, uint8_t numLevels) {
    uint32_t acc = 0;
    uint8_t h = 0;
    while (h < numLevels) {
        acc += levelCapacity(k, numLevels, h);
        ++h;
    }
    return acc;
}

uint8_t floorLog2(uint64_t p, uint64_t q) {
    uint8_t result = 0;
    uint64_t q2 = q;
    while (p >= q2) {
        q2 <<= 1;
        ++result;
    }
    return result;
}

uint64_t sumSampleWeights(uint8_t numLevels, const uint32_t* levels) {
    uint64_t sum = 0;
    uint64_t levelWeight = 1;
    for (uint8_t i = 0; i < numLevels; ++i) {
        const uint32_t levelSpan = levels[i + 1] - levels[i];
        sum += levelWeight * static_cast<uint64_t>(levelSpan);
        levelWeight *= 2;
    }
    return sum;
}

}  // namespace detail
}  // namespace kll
}  // namespace op
}  // namespace omniruntime
