/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: CovarPop aggregate (Partial: 4 values compatible with Spark/Gluten/Velox)
 *   Partial order: c2, count, meanX, meanY (alphabetical, same as Velox)
 *   covar_pop = c2 / count
 */
#ifndef OMNI_RUNTIME_COVAR_POP_AGGREGATOR_H
#define OMNI_RUNTIME_COVAR_POP_AGGREGATOR_H

#include <cstdint>
#include <cmath>
#include "typed_aggregator.h"
#include "operator/aggregation/container_vector.h"

namespace omniruntime {
namespace op {

// Number of partial output columns for covar_pop/covar_samp (n, xAvg, yAvg, ck). Compatible with Spark/Gluten/Velox.
constexpr int kCovarPartialColumnCount = 4;

// 4-value partial state compatible with Spark/Gluten/Velox: (c2, count, meanX, meanY)
#pragma pack(push, 1)
struct CovarPartialState {
    int64_t count{0};
    double meanX{0};
    double meanY{0};
    double c2{0};
    AggValueState valueState{AggValueState::EMPTY_VALUE};

    static const CovarPartialState *ConstCastState(const AggregateState *state) {
        return reinterpret_cast<const CovarPartialState *>(state);
    }
    static CovarPartialState *CastState(AggregateState *state) {
        return reinterpret_cast<CovarPartialState *>(state);
    }
    bool IsEmpty() const { return valueState == AggValueState::EMPTY_VALUE; }
    bool IsOverFlowed() const { return valueState == AggValueState::OVERFLOWED; }

    // Welford-style update (same as Velox CovarAccumulator::update). Sets valueState to OVERFLOWED on overflow.
    void Update(double x, double y) {
        count += 1;
        double deltaX = x - meanX;
        meanX += deltaX / count;
        double deltaY = y - meanY;
        meanY += deltaY / count;
        c2 += deltaX * (y - meanY);
        if (!std::isfinite(meanX) || !std::isfinite(meanY) || !std::isfinite(c2)) {
            valueState = AggValueState::OVERFLOWED;
        }
    }

    // Merge (same as Velox CovarAccumulator::merge). Sets valueState to OVERFLOWED on overflow.
    void Merge(int64_t countOther, double meanXOther, double meanYOther, double c2Other) {
        if (countOther == 0) return;
        if (valueState == AggValueState::OVERFLOWED) return;
        if (!std::isfinite(meanXOther) || !std::isfinite(meanYOther) || !std::isfinite(c2Other)) {
            valueState = AggValueState::OVERFLOWED;
            return;
        }
        if (count == 0) {
            count = countOther;
            meanX = meanXOther;
            meanY = meanYOther;
            c2 = c2Other;
            return;
        }
        int64_t newCount = count + countOther;
        double deltaMeanX = meanXOther - meanX;
        double deltaMeanY = meanYOther - meanY;
        c2 += c2Other + deltaMeanX * deltaMeanY * count * countOther / static_cast<double>(newCount);
        meanX += deltaMeanX * countOther / static_cast<double>(newCount);
        meanY += deltaMeanY * countOther / static_cast<double>(newCount);
        count = newCount;
        if (!std::isfinite(meanX) || !std::isfinite(meanY) || !std::isfinite(c2)) {
            valueState = AggValueState::OVERFLOWED;
        }
    }
};
#pragma pack(pop)

inline double CovarPopFromState(int64_t n, double, double, double c2) {
    return n < 1 ? 0.0 : (c2 / n);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
class CovarPopAggregator : public TypedAggregator {
    using InVector = typename AggNativeAndVectorType<IN_ID>::vector;
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using OutVector = typename AggNativeAndVectorType<OUT_ID>::vector;
    using OutType = typename AggNativeAndVectorType<OUT_ID>::type;

public:
    ~CovarPopAggregator() override = default;

    size_t GetStateSize() override { return sizeof(CovarPartialState); }

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override;
    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override;
    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override;
    void InitState(AggregateState *state) override;
    void InitStates(std::vector<AggregateState *> &groupStates) override;
    std::vector<DataTypePtr> GetSpillType() override;

    static std::unique_ptr<Aggregator> Create(const DataTypes &inputTypes, const DataTypes &outputTypes,
        std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull);

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override;
    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    CovarPopAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
        const std::shared_ptr<NullsHelper> nullMap) override;
};
}
}
#endif // OMNI_RUNTIME_COVAR_POP_AGGREGATOR_H
