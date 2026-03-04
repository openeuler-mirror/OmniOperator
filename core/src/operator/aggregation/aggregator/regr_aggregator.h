/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Linear regression aggregate functions (regr_count, regr_intercept, regr_r2,
 *              regr_slope, regr_sxx, regr_sxy, regr_syy). Spark signature: regr_*(y, x).
 */
#ifndef OMNI_RUNTIME_REGR_AGGREGATOR_H
#define OMNI_RUNTIME_REGR_AGGREGATOR_H

#include "typed_aggregator.h"
#include "operator/aggregation/vector_getter.h"
#include <cmath>

namespace omniruntime::op {

// Merge two partial states (ExtendedRegrAccumulator merge from Velox CovarianceAggregatesBase).
inline void RegrMerge(int64_t &count, double &meanX, double &meanY, double &c2, double &m2X, double &m2Y,
    int64_t countOther, double meanXOther, double meanYOther, double c2Other, double m2XOther, double m2YOther)
{
    if (countOther == 0)
        return;
    if (count == 0) {
        count = countOther;
        meanX = meanXOther;
        meanY = meanYOther;
        c2 = c2Other;
        m2X = m2XOther;
        m2Y = m2YOther;
        return;
    }
    int64_t newCount = count + countOther;
    double deltaMeanX = meanXOther - meanX;
    double deltaMeanY = meanYOther - meanY;
    c2 += c2Other +
        deltaMeanX * deltaMeanY * static_cast<double>(count) * static_cast<double>(countOther) /
            static_cast<double>(newCount);
    double meanXOld = meanX;
    double meanYOld = meanY;
    meanX += deltaMeanX * static_cast<double>(countOther) / static_cast<double>(newCount);
    meanY += deltaMeanY * static_cast<double>(countOther) / static_cast<double>(newCount);
    double k = 1.0 * count / static_cast<double>(newCount) * countOther;
    m2X += m2XOther + k * std::pow(meanXOld - meanXOther, 2);
    m2Y += m2YOther + k * std::pow(meanYOld - meanYOther, 2);
    count = newCount;
}

#pragma pack(push, 1)
struct RegrState {
    int64_t count{0};
    double meanX{0};
    double meanY{0};
    double c2{0};
    double m2X{0};
    double m2Y{0};

    bool IsEmpty() const { return count == 0; }
};
#pragma pack(pop)

// Single-column variance state for Spark RegrReplacement (regr_sxx/regr_syy). Final output is m2 only.
#pragma pack(push, 1)
struct RegrReplacementState {
    int64_t n{0};
    double avg{0};
    double m2{0};

    bool IsEmpty() const { return n == 0; }
};
#pragma pack(pop)

inline void RegrReplacementMerge(int64_t &n, double &avg, double &m2,
    int64_t nOther, double avgOther, double m2Other)
{
    if (nOther == 0)
        return;
    if (n == 0) {
        n = nOther;
        avg = avgOther;
        m2 = m2Other;
        return;
    }
    int64_t newN = n + nOther;
    double delta = avgOther - avg;
    double avgOld = avg;
    avg += delta * static_cast<double>(nOther) / static_cast<double>(newN);
    double k = static_cast<double>(n) * static_cast<double>(nOther) / static_cast<double>(newN);
    m2 += m2Other + k * std::pow(avgOld - avgOther, 2);
    n = newN;
}

class RegrReplacementAggregator : public TypedAggregator {
public:
    RegrReplacementAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull);

    ~RegrReplacementAggregator() override = default;

    size_t GetStateSize() override { return sizeof(RegrReplacementState); }

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
        const std::shared_ptr<NullsHelper> nullMap) override;

    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

    std::vector<DataTypePtr> GetSpillType() override;

private:
    void ProcessSingleRaw(RegrReplacementState *acc, BaseVector *vec, int32_t rowOffset, int32_t rowCount,
        const std::shared_ptr<NullsHelper> &nullMap = nullptr);
    void ProcessSingleMerge(RegrReplacementState *acc, int32_t rowOffset, int32_t rowCount);
};

class RegrAggregator : public TypedAggregator {
public:
    RegrAggregator(FunctionType aggType, const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull);

    ~RegrAggregator() override = default;

    size_t GetStateSize() override { return sizeof(RegrState); }

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
        const std::shared_ptr<NullsHelper> nullMap) override;

    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;

    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

    std::vector<DataTypePtr> GetSpillType() override;

private:
    void ProcessSingleRaw(RegrState *acc, BaseVector *yVec, BaseVector *xVec, int32_t rowOffset, int32_t rowCount,
        const std::shared_ptr<NullsHelper> &nullMap = nullptr);
    void ProcessSingleMerge(RegrState *acc, int32_t rowOffset, int32_t rowCount);
    void ExtractFinalValue(const RegrState *s, BaseVector *outVec, int32_t rowIndex) const;

    FunctionType aggType;
};

} // namespace omniruntime::op

#endif // OMNI_RUNTIME_REGR_AGGREGATOR_H
