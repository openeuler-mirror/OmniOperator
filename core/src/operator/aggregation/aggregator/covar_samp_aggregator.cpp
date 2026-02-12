/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: CovarSamp aggregate - Partial (4 values: c2, count, meanX, meanY) + Final (1 double)
 */

#include "covar_samp_aggregator.h"
#include "operator/aggregation/vector_getter.h"

namespace omniruntime {
namespace op {

template <DataTypeId IN_ID, DataTypeId OUT_ID>
std::unique_ptr<Aggregator> CovarSampAggregator<IN_ID, OUT_ID>::Create(const DataTypes &inputTypes,
    const DataTypes &outputTypes, std::vector<int32_t> &channels, bool rawIn, bool partialOut, bool isOverflowAsNull) {
    constexpr bool kInSupported = (IN_ID == OMNI_DOUBLE || IN_ID == OMNI_CONTAINER);
    constexpr bool kOutSupported = (OUT_ID == OMNI_DOUBLE || OUT_ID == OMNI_CONTAINER);
    if constexpr (!kInSupported) {
        LogError("Error in covar_samp aggregator: Unsupported input type %s", TypeUtil::TypeToStringLog(IN_ID).c_str());
        return nullptr;
    }
    if constexpr (!kOutSupported) {
        LogError("Error in covar_samp aggregator: Unsupported output type %s", TypeUtil::TypeToStringLog(OUT_ID).c_str());
        return nullptr;
    }
    if (IN_ID != OMNI_CONTAINER && (inputTypes.GetSize() != 2 || !TypedAggregator::CheckTypes("covar_samp", inputTypes, outputTypes, IN_ID, OUT_ID)))
        return nullptr;
    // Merge: Gluten sends 4 RAW Double columns only.
    if (IN_ID == OMNI_CONTAINER && inputTypes.GetSize() != 4)
        return nullptr;
    return std::unique_ptr<CovarSampAggregator<IN_ID, OUT_ID>>(
        new CovarSampAggregator<IN_ID, OUT_ID>(inputTypes, outputTypes, channels, rawIn, partialOut, isOverflowAsNull));
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CovarSampAggregator<IN_ID, OUT_ID>::ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex) {
    auto *s = CovarPartialState::ConstCastState(state + aggStateOffset);
    if (s->IsEmpty()) {
        if (outputPartial) {
            if (vectors.size() >= 4) {
                for (int i = 0; i < 4; i++)
                    reinterpret_cast<Vector<double> *>(vectors[i])->SetNull(rowIndex);
            } else {
                auto *vec = static_cast<ContainerVector *>(vectors[0]);
                for (int i = 0; i < 4; i++)
                    reinterpret_cast<Vector<double> *>(vec->GetValue(i))->SetNull(rowIndex);
            }
        } else {
            static_cast<OutVector *>(vectors[0])->SetNull(rowIndex);
        }
        return;
    }
    if (outputPartial) {
        // Spark order: (n, xAvg, yAvg, ck) all Double
        double n = static_cast<double>(s->count);
        if (vectors.size() >= 4) {
            reinterpret_cast<Vector<double> *>(vectors[0])->SetValue(rowIndex, n);
            reinterpret_cast<Vector<double> *>(vectors[1])->SetValue(rowIndex, s->meanX);
            reinterpret_cast<Vector<double> *>(vectors[2])->SetValue(rowIndex, s->meanY);
            reinterpret_cast<Vector<double> *>(vectors[3])->SetValue(rowIndex, s->c2);
        } else {
            auto *vec = static_cast<ContainerVector *>(vectors[0]);
            reinterpret_cast<Vector<double> *>(vec->GetValue(0))->SetValue(rowIndex, n);
            reinterpret_cast<Vector<double> *>(vec->GetValue(1))->SetValue(rowIndex, s->meanX);
            reinterpret_cast<Vector<double> *>(vec->GetValue(2))->SetValue(rowIndex, s->meanY);
            reinterpret_cast<Vector<double> *>(vec->GetValue(3))->SetValue(rowIndex, s->c2);
        }
    } else {
        double v = CovarSampFromState(s->count, s->meanX, s->meanY, s->c2);
        static_cast<OutVector *>(vectors[0])->SetValue(rowIndex, static_cast<OutType>(v));
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CovarSampAggregator<IN_ID, OUT_ID>::ExtractValuesBatch(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount) {
    (void)rowOffset;
    if (outputPartial) {
        Vector<double> *vN = nullptr, *vXAvg = nullptr, *vYAvg = nullptr, *vCk = nullptr;
        if (vectors.size() >= 4) {
            vN = reinterpret_cast<Vector<double> *>(vectors[0]);
            vXAvg = reinterpret_cast<Vector<double> *>(vectors[1]);
            vYAvg = reinterpret_cast<Vector<double> *>(vectors[2]);
            vCk = reinterpret_cast<Vector<double> *>(vectors[3]);
        } else {
            auto *vec = static_cast<ContainerVector *>(vectors[0]);
            vN = reinterpret_cast<Vector<double> *>(vec->GetValue(0));
            vXAvg = reinterpret_cast<Vector<double> *>(vec->GetValue(1));
            vYAvg = reinterpret_cast<Vector<double> *>(vec->GetValue(2));
            vCk = reinterpret_cast<Vector<double> *>(vec->GetValue(3));
        }
        for (int32_t i = 0; i < rowCount; i++) {
            auto *s = CovarPartialState::CastState(groupStates[i] + aggStateOffset);
            if (s->IsEmpty()) {
                vN->SetNull(i); vXAvg->SetNull(i); vYAvg->SetNull(i); vCk->SetNull(i);
            } else {
                vN->SetValue(i, static_cast<double>(s->count));
                vXAvg->SetValue(i, s->meanX);
                vYAvg->SetValue(i, s->meanY);
                vCk->SetValue(i, s->c2);
            }
        }
    } else {
        auto *v = static_cast<OutVector *>(vectors[0]);
        for (int32_t i = 0; i < rowCount; i++) {
            auto *s = CovarPartialState::CastState(groupStates[i] + aggStateOffset);
            if (s->IsEmpty())
                v->SetNull(i);
            else
                v->SetValue(i, static_cast<OutType>(CovarSampFromState(s->count, s->meanX, s->meanY, s->c2)));
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
std::vector<DataTypePtr> CovarSampAggregator<IN_ID, OUT_ID>::GetSpillType() {
    std::vector<DataTypePtr> spillTypes;
    for (int i = 0; i < 4; i++)
        spillTypes.emplace_back(std::make_shared<DataType>(OMNI_DOUBLE));  // Spark order: n, xAvg, yAvg, ck
    return spillTypes;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CovarSampAggregator<IN_ID, OUT_ID>::ExtractValuesForSpill(std::vector<AggregateState *> &,
    std::vector<BaseVector *> &) {
    throw OmniException("OPERATOR_RUNTIME_ERROR", "CovarSamp aggregator spill not supported.");
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CovarSampAggregator<IN_ID, OUT_ID>::InitState(AggregateState *state) {
    auto *s = CovarPartialState::CastState(state + aggStateOffset);
    s->count = 0;
    s->meanX = s->meanY = s->c2 = 0;
    s->valueState = AggValueState::EMPTY_VALUE;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CovarSampAggregator<IN_ID, OUT_ID>::InitStates(std::vector<AggregateState *> &groupStates) {
    for (auto *st : groupStates)
        InitState(st);
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CovarSampAggregator<IN_ID, OUT_ID>::ProcessSingleInternal(AggregateState *state, BaseVector *vector,
    const int32_t rowOffset, const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap) {
    auto *s = CovarPartialState::CastState(state);
    if (inputRaw) {
        auto *col1 = curVectorBatch->Get(channels[0]);
        auto *col2 = curVectorBatch->Get(channels[1]);
        auto *p1 = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(col1));
        auto *p2 = reinterpret_cast<InType *>(GetValuesFromVector<IN_ID>(col2));
        p1 += rowOffset;
        p2 += rowOffset;
        for (int32_t i = 0; i < rowCount; i++) {
            if (nullMap && (*nullMap)[i]) continue;
            if (col1->IsNull(rowOffset + i) || col2->IsNull(rowOffset + i)) continue;
            s->Update(p1[i], p2[i]);
        }
        if (s->count > 0)
            s->valueState = AggValueState::NORMAL;
    } else {
        // Merge: Gluten sends 4 RAW Double columns. Spark order: n, xAvg, yAvg, ck
        double *vN = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[0]))) + rowOffset;
        double *vXAvg = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[1]))) + rowOffset;
        double *vYAvg = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[2]))) + rowOffset;
        double *vCk = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[3]))) + rowOffset;
        BaseVector *nullCol = curVectorBatch->Get(channels[0]);
        for (int32_t i = 0; i < rowCount; i++) {
            if (nullCol->IsNull(rowOffset + i)) continue;
            double n = vN[i];
            if (n <= 0) continue;
            s->Merge(static_cast<int64_t>(n), vXAvg[i], vYAvg[i], vCk[i]);
        }
        if (s->count > 0)
            s->valueState = AggValueState::NORMAL;
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CovarSampAggregator<IN_ID, OUT_ID>::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector,
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
            auto *s = CovarPartialState::CastState(rowStates[i] + aggStateOffset);
            s->Update(p1[i], p2[i]);
            s->valueState = AggValueState::NORMAL;
        }
    } else {
        // Merge: 4 RAW Double columns (Gluten). Spark order: n, xAvg, yAvg, ck
        double *vN = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[0]))) + rowOffset;
        double *vXAvg = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[1]))) + rowOffset;
        double *vYAvg = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[2]))) + rowOffset;
        double *vCk = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(curVectorBatch->Get(channels[3]))) + rowOffset;
        BaseVector *nullCol = curVectorBatch->Get(channels[0]);
        const size_t rowCount = rowStates.size();
        for (size_t i = 0; i < rowCount; i++) {
            if (nullCol->IsNull(rowOffset + i)) continue;
            double n = vN[i];
            if (n <= 0) continue;
            auto *s = CovarPartialState::CastState(rowStates[i] + aggStateOffset);
            s->Merge(static_cast<int64_t>(n), vXAvg[i], vYAvg[i], vCk[i]);
            if (s->count > 0)
                s->valueState = AggValueState::NORMAL;
        }
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CovarSampAggregator<IN_ID, OUT_ID>::ProcessGroupUnspill(std::vector<UnspillRowInfo> &, int32_t, int32_t &) {}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
void CovarSampAggregator<IN_ID, OUT_ID>::ProcessAlignAggSchema(VectorBatch *, BaseVector *,
    const std::shared_ptr<NullsHelper>, const bool) {}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
CovarSampAggregator<IN_ID, OUT_ID>::CovarSampAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, const bool inputRaw, const bool outputPartial, const bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_COVAR_SAMP, inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull) {}

template class CovarSampAggregator<OMNI_DOUBLE, OMNI_DOUBLE>;
template class CovarSampAggregator<OMNI_CONTAINER, OMNI_DOUBLE>;
template class CovarSampAggregator<OMNI_CONTAINER, OMNI_CONTAINER>;
}
}
