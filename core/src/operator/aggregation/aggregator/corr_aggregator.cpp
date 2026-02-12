/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Corr aggregate - Partial (6 values) + Final (1 double)
 */

#include "corr_aggregator.h"
#include "operator/aggregation/vector_getter.h"

namespace omniruntime {
namespace op {

template <DataTypeId IN_ID, DataTypeId OUT_ID>
std::unique_ptr<Aggregator> CorrAggregator<IN_ID, OUT_ID>::Create(const DataTypes &inputTypes,
    const DataTypes &outputTypes, std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull) {
    constexpr bool kInSupported = (IN_ID == OMNI_SHORT || IN_ID == OMNI_INT || IN_ID == OMNI_LONG ||
        IN_ID == OMNI_FLOAT || IN_ID == OMNI_DOUBLE || IN_ID == OMNI_DECIMAL64 || IN_ID == OMNI_DECIMAL128 ||
        IN_ID == OMNI_CONTAINER);
    constexpr bool kOutSupported = (OUT_ID == OMNI_DOUBLE || OUT_ID == OMNI_CONTAINER);
    if constexpr (!kInSupported) {
        LogError("Error in corr aggregator: Unsupported input type %s", TypeUtil::TypeToStringLog(IN_ID).c_str());
        return nullptr;
    }
    if constexpr (!kOutSupported) {
        LogError("Error in corr aggregator: Unsupported output type %s", TypeUtil::TypeToStringLog(OUT_ID).c_str());
        return nullptr;
    }
    if (IN_ID != OMNI_CONTAINER && (inputTypes.GetSize() != 2 || !TypedAggregator::CheckTypes("corr", inputTypes, outputTypes, IN_ID, OUT_ID)))
        return nullptr;
    // Merge: Gluten sends 6 RAW Double columns only.
    if (IN_ID == OMNI_CONTAINER && inputTypes.GetSize() != 6)
        return nullptr;
    return std::unique_ptr<CorrAggregator<IN_ID, OUT_ID>>(
        new CorrAggregator<IN_ID, OUT_ID>(inputTypes, outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CorrAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex) {
    auto *s = CorrPartialState::ConstCastState(state + aggStateOffset);
    if (s->IsEmpty()) {
        if (outputPartial) {
            if (vectors.size() >= 6) {
                for (int i = 0; i < 6; i++)
                    vectors[i]->SetNull(rowIndex);
            } else {
                auto *vec = static_cast<ContainerVector *>(vectors[0]);
                for (int i = 0; i < 6; i++)
                    reinterpret_cast<Vector<double> *>(vec->GetValue(i))->SetNull(rowIndex);
            }
        } else {
            static_cast<OutVector *>(vectors[0])->SetNull(rowIndex);
        }
        return;
    }
    if (outputPartial) {
        // Spark order: (n, xAvg, yAvg, ck, xMk, yMk) all Double
        double n = static_cast<double>(s->count);
        double xAvg = (n > 0) ? (s->sum_x / n) : 0.0;
        double yAvg = (n > 0) ? (s->sum_y / n) : 0.0;
        double ck = (n > 0) ? (s->sum_xy - s->sum_x * s->sum_y / n) : 0.0;
        double xMk = (n > 0) ? (s->sum_xx - s->sum_x * s->sum_x / n) : 0.0;
        double yMk = (n > 0) ? (s->sum_yy - s->sum_y * s->sum_y / n) : 0.0;
        if (vectors.size() >= 6) {
            reinterpret_cast<Vector<double> *>(vectors[0])->SetValue(rowIndex, n);
            reinterpret_cast<Vector<double> *>(vectors[1])->SetValue(rowIndex, xAvg);
            reinterpret_cast<Vector<double> *>(vectors[2])->SetValue(rowIndex, yAvg);
            reinterpret_cast<Vector<double> *>(vectors[3])->SetValue(rowIndex, ck);
            reinterpret_cast<Vector<double> *>(vectors[4])->SetValue(rowIndex, xMk);
            reinterpret_cast<Vector<double> *>(vectors[5])->SetValue(rowIndex, yMk);
        } else {
            auto *vec = static_cast<ContainerVector *>(vectors[0]);
            reinterpret_cast<Vector<double> *>(vec->GetValue(0))->SetValue(rowIndex, n);
            reinterpret_cast<Vector<double> *>(vec->GetValue(1))->SetValue(rowIndex, xAvg);
            reinterpret_cast<Vector<double> *>(vec->GetValue(2))->SetValue(rowIndex, yAvg);
            reinterpret_cast<Vector<double> *>(vec->GetValue(3))->SetValue(rowIndex, ck);
            reinterpret_cast<Vector<double> *>(vec->GetValue(4))->SetValue(rowIndex, xMk);
            reinterpret_cast<Vector<double> *>(vec->GetValue(5))->SetValue(rowIndex, yMk);
        }
    } else {
        double corr = CorrFromSuffStats(s->count, s->sum_x, s->sum_y, s->sum_xx, s->sum_yy, s->sum_xy);
        static_cast<OutVector *>(vectors[0])->SetValue(rowIndex, static_cast<OutType>(corr));
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CorrAggregator<IN_ID, OUT_ID>::ExtractValuesBatch(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount) {
    (void)rowOffset;
    if (outputPartial) {
        Vector<double> *vN = nullptr, *vXAvg = nullptr, *vYAvg = nullptr, *vCk = nullptr, *vXMk = nullptr, *vYMk = nullptr;
        if (vectors.size() >= 6) {
            vN = reinterpret_cast<Vector<double> *>(vectors[0]);
            vXAvg = reinterpret_cast<Vector<double> *>(vectors[1]);
            vYAvg = reinterpret_cast<Vector<double> *>(vectors[2]);
            vCk = reinterpret_cast<Vector<double> *>(vectors[3]);
            vXMk = reinterpret_cast<Vector<double> *>(vectors[4]);
            vYMk = reinterpret_cast<Vector<double> *>(vectors[5]);
        } else {
            auto *vec = static_cast<ContainerVector *>(vectors[0]);
            vN = reinterpret_cast<Vector<double> *>(vec->GetValue(0));
            vXAvg = reinterpret_cast<Vector<double> *>(vec->GetValue(1));
            vYAvg = reinterpret_cast<Vector<double> *>(vec->GetValue(2));
            vCk = reinterpret_cast<Vector<double> *>(vec->GetValue(3));
            vXMk = reinterpret_cast<Vector<double> *>(vec->GetValue(4));
            vYMk = reinterpret_cast<Vector<double> *>(vec->GetValue(5));
        }
        for (int32_t i = 0; i < rowCount; i++) {
            auto *s = CorrPartialState::CastState(groupStates[i] + aggStateOffset);
            if (s->IsEmpty()) {
                vN->SetNull(i); vXAvg->SetNull(i); vYAvg->SetNull(i);
                vCk->SetNull(i); vXMk->SetNull(i); vYMk->SetNull(i);
            } else {
                double n = static_cast<double>(s->count);
                double xAvg = s->sum_x / n, yAvg = s->sum_y / n;
                vN->SetValue(i, n);
                vXAvg->SetValue(i, xAvg);
                vYAvg->SetValue(i, yAvg);
                vCk->SetValue(i, s->sum_xy - s->sum_x * s->sum_y / n);
                vXMk->SetValue(i, s->sum_xx - s->sum_x * s->sum_x / n);
                vYMk->SetValue(i, s->sum_yy - s->sum_y * s->sum_y / n);
            }
        }
    } else {
        auto *v = static_cast<OutVector *>(vectors[0]);
        for (int32_t i = 0; i < rowCount; i++) {
            auto *s = CorrPartialState::CastState(groupStates[i] + aggStateOffset);
            if (s->IsEmpty())
                v->SetNull(i);
            else
                v->SetValue(i, static_cast<OutType>(CorrFromSuffStats(s->count, s->sum_x, s->sum_y, s->sum_xx, s->sum_yy, s->sum_xy)));
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
std::vector<DataTypePtr> CorrAggregator<IN_ID, OUT_ID>::GetSpillType() {
    std::vector<DataTypePtr> spillTypes;
    for (int i = 0; i < 6; i++)
        spillTypes.emplace_back(std::make_shared<DataType>(OMNI_DOUBLE));  // Spark order: n, xAvg, yAvg, ck, xMk, yMk
    return spillTypes;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CorrAggregator<IN_ID, OUT_ID>::ExtractValuesForSpill(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors) {
    throw OmniException("OPERATOR_RUNTIME_ERROR", "Corr aggregator spill not supported.");
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CorrAggregator<IN_ID, OUT_ID>::InitState(AggregateState *state) {
    auto *s = CorrPartialState::CastState(state + aggStateOffset);
    s->count = 0;
    s->sum_x = s->sum_y = s->sum_xx = s->sum_yy = s->sum_xy = 0;
    s->valueState = AggValueState::EMPTY_VALUE;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CorrAggregator<IN_ID, OUT_ID>::InitStates(std::vector<AggregateState *> &groupStates) {
    for (auto *st : groupStates)
        InitState(st);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CorrAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState *state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) {
    auto *s = CorrPartialState::CastState(state);
    if (inputRaw) {
        auto *col1 = curVectorBatch->Get(channels[0]);
        auto *col2 = curVectorBatch->Get(channels[1]);
        auto *p1 = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(col1));
        auto *p2 = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(col2));
        p1 += rowOffset;
        p2 += rowOffset;
        std::shared_ptr<NullsHelper> nm = nullMap ? nullMap : std::make_shared<NullsHelper>(std::make_shared<NullsBuffer>(rowCount));
        CorrAccumulateRaw<InType, false>(s->count, s->sum_x, s->sum_y, s->sum_xx, s->sum_yy, s->sum_xy, p1, p2, rowCount, *nm);
        if (s->count > 0)
            s->valueState = AggValueState::NORMAL;
    } else {
        // Merge: Gluten sends 6 RAW Double columns. Spark order: n, xAvg, yAvg, ck, xMk, yMk
        double *vN = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[0]))) + rowOffset;
        double *vXAvg = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[1]))) + rowOffset;
        double *vYAvg = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[2]))) + rowOffset;
        double *vCk = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[3]))) + rowOffset;
        double *vXMk = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[4]))) + rowOffset;
        double *vYMk = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[5]))) + rowOffset;
        BaseVector *nullCol = curVectorBatch->Get(channels[0]);
        for (int32_t i = 0; i < rowCount; i++) {
            if (nullCol->IsNull(rowOffset + i)) continue;
            double n = vN[i];
            if (n <= 0) continue;
            CorrPartialState other;
            other.count = static_cast<int64_t>(n);
            other.sum_x = n * vXAvg[i];
            other.sum_y = n * vYAvg[i];
            other.sum_xx = vXMk[i] + n * vXAvg[i] * vXAvg[i];
            other.sum_yy = vYMk[i] + n * vYAvg[i] * vYAvg[i];
            other.sum_xy = vCk[i] + n * vXAvg[i] * vYAvg[i];
            other.valueState = AggValueState::NORMAL;
            s->Merge(other);
        }
        if (s->count > 0)
            s->valueState = AggValueState::NORMAL;
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CorrAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector,
    const int32_t rowOffset, const std::shared_ptr<NullsHelper> nullMap) {
    if (inputRaw) {
        auto *col1 = curVectorBatch->Get(channels[0]);
        auto *col2 = curVectorBatch->Get(channels[1]);
        auto *p1 = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(col1));
        auto *p2 = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(col2));
        p1 += rowOffset;
        p2 += rowOffset;
        const size_t rowCount = rowStates.size();
        for (size_t i = 0; i < rowCount; i++) {
            if (nullMap && (*nullMap)[i]) continue;
            if (col1->IsNull(rowOffset + i) || col2->IsNull(rowOffset + i)) continue;
            auto *s = CorrPartialState::CastState(rowStates[i] + aggStateOffset);
            double x, y;
            if constexpr (std::is_same_v<InType, type::Decimal128>) {
                x = static_cast<double>(type::Decimal128Wrapper(p1[i]));
                y = static_cast<double>(type::Decimal128Wrapper(p2[i]));
            } else {
                x = static_cast<double>(p1[i]);
                y = static_cast<double>(p2[i]);
            }
            s->count += 1;
            s->sum_x += x; s->sum_y += y;
            s->sum_xx += x * x; s->sum_yy += y * y; s->sum_xy += x * y;
            s->valueState = AggValueState::NORMAL;
        }
    } else {
        // Merge: 6 RAW Double columns (Gluten). Spark order: n, xAvg, yAvg, ck, xMk, yMk
        double *vN = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[0]))) + rowOffset;
        double *vXAvg = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[1]))) + rowOffset;
        double *vYAvg = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[2]))) + rowOffset;
        double *vCk = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[3]))) + rowOffset;
        double *vXMk = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[4]))) + rowOffset;
        double *vYMk = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[5]))) + rowOffset;
        BaseVector *nullCol = curVectorBatch->Get(channels[0]);
        const size_t rowCount = rowStates.size();
        for (size_t i = 0; i < rowCount; i++) {
            if (nullCol->IsNull(rowOffset + i)) continue;
            double n = vN[i];
            if (n <= 0) continue;
            CorrPartialState other;
            other.count = static_cast<int64_t>(n);
            other.sum_x = n * vXAvg[i];
            other.sum_y = n * vYAvg[i];
            other.sum_xx = vXMk[i] + n * vXAvg[i] * vXAvg[i];
            other.sum_yy = vYMk[i] + n * vYAvg[i] * vYAvg[i];
            other.sum_xy = vCk[i] + n * vXAvg[i] * vYAvg[i];
            other.valueState = AggValueState::NORMAL;
            auto *s = CorrPartialState::CastState(rowStates[i] + aggStateOffset);
            s->Merge(other);
            if (s->count > 0)
                s->valueState = AggValueState::NORMAL;
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CorrAggregator<IN_ID, OUT_ID>::ProcessGroupUnspill(std::vector<UnspillRowInfo> &, int32_t, int32_t &) {}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CorrAggregator<IN_ID, OUT_ID>::ProcessAlignAggSchema(VectorBatch *, BaseVector *,
    const std::shared_ptr<NullsHelper>, const bool) {}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
CorrAggregator<IN_ID, OUT_ID>::CorrAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_CORR, inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull) {}

template class CorrAggregator<OMNI_SHORT, OMNI_DOUBLE>;
template class CorrAggregator<OMNI_INT, OMNI_DOUBLE>;
template class CorrAggregator<OMNI_LONG, OMNI_DOUBLE>;
template class CorrAggregator<OMNI_FLOAT, OMNI_DOUBLE>;
template class CorrAggregator<OMNI_DOUBLE, OMNI_DOUBLE>;
template class CorrAggregator<OMNI_DECIMAL64, OMNI_DOUBLE>;
template class CorrAggregator<OMNI_DECIMAL128, OMNI_DOUBLE>;
template class CorrAggregator<OMNI_CONTAINER, OMNI_DOUBLE>;
template class CorrAggregator<OMNI_CONTAINER, OMNI_CONTAINER>;
}
}
