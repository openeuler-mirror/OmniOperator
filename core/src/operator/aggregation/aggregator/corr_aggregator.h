/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Corr aggregate (Partial: 6 values, Final: 1 double)
 *   Partial state order: count, sum_x, sum_y, sum_xx, sum_yy, sum_xy
 */
#ifndef OMNI_RUNTIME_CORR_AGGREGATOR_H
#define OMNI_RUNTIME_CORR_AGGREGATOR_H

#include <cstdint>
#include <cmath>
#include <type_traits>
#include "typed_aggregator.h"
#include "operator/aggregation/container_vector.h"

namespace omniruntime {
namespace op {

// Number of partial output columns for corr (n, xAvg, yAvg, ck, xMk, yMk). Compatible with Spark/Gluten.
constexpr int kCorrPartialColumnCount = 6;

// Accumulate (x,y) into partial state (count, sum_x, sum_y, sum_xx, sum_yy, sum_xy). Sets valueState to OVERFLOWED on overflow.
template <typename IN, bool addIf>
VECTORIZE_LOOP FAST_MATH NO_INLINE void CorrAccumulateRaw(int64_t &count, double &sum_x, double &sum_y,
    double &sum_xx, double &sum_yy, double &sum_xy, AggValueState &valueState,
    const IN *__restrict col1ptr, const IN *__restrict col2ptr,
    const size_t rowCount, const NullsHelper &condition) {
    if (rowCount == 0) return;
    col1ptr = (const IN *)__builtin_assume_aligned(col1ptr, ARRAY_ALIGNMENT);
    col2ptr = (const IN *)__builtin_assume_aligned(col2ptr, ARRAY_ALIGNMENT);
    for (size_t i = 0; i < rowCount; ++i) {
        if (condition[i] != addIf) continue;
        double x;
        double y;
        if constexpr (std::is_same_v<IN, type::Decimal128>) {
            x = static_cast<double>(type::Decimal128Wrapper(col1ptr[i]));
            y = static_cast<double>(type::Decimal128Wrapper(col2ptr[i]));
        } else {
            x = static_cast<double>(col1ptr[i]);
            y = static_cast<double>(col2ptr[i]);
        }
        count += 1;
        sum_x += x;
        sum_y += y;
        sum_xx += x * x;
        sum_yy += y * y;
        sum_xy += x * y;
        if (!std::isfinite(sum_x) || !std::isfinite(sum_y) || !std::isfinite(sum_xx) ||
            !std::isfinite(sum_yy) || !std::isfinite(sum_xy)) {
            valueState = AggValueState::OVERFLOWED;
            return;
        }
    }
}

// Compute Pearson correlation from 6 sufficient statistics. Returns 0.0 if n<2 or den<=0; may return non-finite on overflow.
inline double CorrFromSuffStats(int64_t n, double sum_x, double sum_y, double sum_xx, double sum_yy, double sum_xy) {
    if (n < 2) return 0.0;
    double num = n * sum_xy - sum_x * sum_y;
    double den_x = n * sum_xx - sum_x * sum_x;
    double den_y = n * sum_yy - sum_y * sum_y;
    double den = std::sqrt(den_x * den_y);
    if (den <= 0) return 0.0;
    return num / den;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
class CorrAggregator : public TypedAggregator {
    using InVector = typename AggNativeAndVectorType<IN_ID>::vector;
    using InType = typename AggNativeAndVectorType<IN_ID>::type;
    using OutVector = typename AggNativeAndVectorType<OUT_ID>::vector;
    using OutType = typename AggNativeAndVectorType<OUT_ID>::type;
    using ResultType = typename AggNativeAndVectorType<OUT_ID>::type;

#pragma pack(push, 1)
    struct CorrPartialState {
        int64_t count{0};
        double sum_x{0};
        double sum_y{0};
        double sum_xx{0};
        double sum_yy{0};
        double sum_xy{0};
        AggValueState valueState{AggValueState::EMPTY_VALUE};

        static const CorrPartialState *ConstCastState(const AggregateState *state) {
            return reinterpret_cast<const CorrPartialState *>(state);
        }
        static CorrPartialState *CastState(AggregateState *state) {
            return reinterpret_cast<CorrPartialState *>(state);
        }
        bool IsEmpty() const { return valueState == AggValueState::EMPTY_VALUE; }
        bool IsOverFlowed() const { return valueState == AggValueState::OVERFLOWED; }
        void Merge(const CorrPartialState &other) {
            if (other.IsEmpty()) return;
            if (other.IsOverFlowed()) {
                valueState = AggValueState::OVERFLOWED;
                return;
            }
            if (IsEmpty()) {
                count = other.count;
                sum_x = other.sum_x;
                sum_y = other.sum_y;
                sum_xx = other.sum_xx;
                sum_yy = other.sum_yy;
                sum_xy = other.sum_xy;
                valueState = other.valueState;
                return;
            }
            count += other.count;
            sum_x += other.sum_x;
            sum_y += other.sum_y;
            sum_xx += other.sum_xx;
            sum_yy += other.sum_yy;
            sum_xy += other.sum_xy;
            if (!std::isfinite(sum_x) || !std::isfinite(sum_y) || !std::isfinite(sum_xx) ||
                !std::isfinite(sum_yy) || !std::isfinite(sum_xy)) {
                valueState = AggValueState::OVERFLOWED;
            }
        }
    };
#pragma pack(pop)

public:
    ~CorrAggregator() override = default;

    size_t GetStateSize() override { return sizeof(CorrPartialState); }

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
    void AlignAggSchema(VectorBatch *result, VectorBatch *inputVecBatch) override;
    void AlignAggSchemaWithFilter(VectorBatch *result, VectorBatch *inputVecBatch,
        const int32_t filterIndex) override;
    void ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
        const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter) override;

protected:
    CorrAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels,
        const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull);

    void ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
        const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) override;

    void ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector, const int32_t rowOffset,
        const std::shared_ptr<NullsHelper> nullMap) override;
};
} // namespace op
} // namespace omniruntime

#endif // OMNI_RUNTIME_CORR_AGGREGATOR_H
