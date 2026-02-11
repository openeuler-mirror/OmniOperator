/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
 * Description: HyperLogLog bias correction (from Presto/Velox) for ~2% standard error.
 * Tables from https://github.com/airlift/airlift/blob/master/stats/src/main/java/io/airlift/stats/cardinality/BiasCorrection.java
 */
#ifndef OMNI_RUNTIME_BIAS_CORRECTION_H
#define OMNI_RUNTIME_BIAS_CORRECTION_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include <cstdint>

namespace omniruntime {
namespace op {

/**
 * Corrects raw HLL estimate using empirical bias tables (Presto/Airlift). indexBitLength must be in [4, 16].
 * If rawEstimate is outside the table range for the given p, returns rawEstimate unchanged.
 *
 * @param rawEstimate: Raw HyperLogLog estimate (before bias correction).
 * @param indexBitLength: HLL parameter p (number of index bits).
 * @return Corrected estimate, or rawEstimate if outside table range.
 */
double CorrectHllBias(double rawEstimate, int8_t indexBitLength);

}  // namespace op
}  // namespace omniruntime

#endif  // OMNI_RUNTIME_BIAS_CORRECTION_H
