/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
#include "regr_aggregator.h"
#include "vector/vector_helper.h"
#include "type/data_type.h"
#include "util/type_util.h"

namespace omniruntime {
namespace op {

// Read one numeric value from vector at row as double (for regr (y,x) multi-type support).
static double RegrGetDoubleAt(BaseVector *vec, int32_t row)
{
    if (vec->GetEncoding() == vec::OMNI_ENCODING_CONST) {
        switch (vec->GetTypeId()) {
            case OMNI_BOOLEAN: return static_cast<vec::ConstVector<bool> *>(vec)->GetConstValue() ? 1.0 : 0.0;
            case OMNI_BYTE: return static_cast<double>(static_cast<vec::ConstVector<int8_t> *>(vec)->GetConstValue());
            case OMNI_SHORT: return static_cast<double>(static_cast<vec::ConstVector<int16_t> *>(vec)->GetConstValue());
            case OMNI_INT: return static_cast<double>(static_cast<vec::ConstVector<int32_t> *>(vec)->GetConstValue());
            case OMNI_LONG: return static_cast<double>(static_cast<vec::ConstVector<int64_t> *>(vec)->GetConstValue());
            case OMNI_FLOAT: return static_cast<double>(static_cast<vec::ConstVector<float> *>(vec)->GetConstValue());
            case OMNI_DOUBLE: return static_cast<vec::ConstVector<double> *>(vec)->GetConstValue();
            default: return 0.0;
        }
    }
    switch (vec->GetTypeId()) {
        case OMNI_BOOLEAN: {
            auto *p = reinterpret_cast<bool *>(GetValuesFromVector<OMNI_BOOLEAN>(vec));
            return p[row] ? 1.0 : 0.0;
        }
        case OMNI_BYTE: {
            auto *p = reinterpret_cast<int8_t *>(GetValuesFromVector<OMNI_BYTE>(vec));
            return static_cast<double>(p[row]);
        }
        case OMNI_SHORT: {
            auto *p = reinterpret_cast<int16_t *>(GetValuesFromVector<OMNI_SHORT>(vec));
            return static_cast<double>(p[row]);
        }
        case OMNI_INT: {
            auto *p = reinterpret_cast<int32_t *>(GetValuesFromVector<OMNI_INT>(vec));
            return static_cast<double>(p[row]);
        }
        case OMNI_LONG: {
            auto *p = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(vec));
            return static_cast<double>(p[row]);
        }
        case OMNI_FLOAT: {
            auto *p = reinterpret_cast<float *>(GetValuesFromVector<OMNI_FLOAT>(vec));
            return static_cast<double>(p[row]);
        }
        case OMNI_DOUBLE: {
            auto *p = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(vec));
            return p[row];
        }
        default:
            return 0.0;
    }
}

// --- RegrReplacementAggregator (single-column n/avg/m2, final = m2)  ---

RegrReplacementAggregator::RegrReplacementAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
    : TypedAggregator(OMNI_AGGREGATION_TYPE_REGR_REPLACEMENT, inputTypes, outputTypes,
          channels, inputRaw, outputPartial, isOverflowAsNull)
{
}

void RegrReplacementAggregator::ProcessSingleRaw(RegrReplacementState *acc, BaseVector *vec, int32_t rowOffset,
    int32_t rowCount, const std::shared_ptr<NullsHelper> &nullMap)
{
    auto *ptr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(vec));
    ptr += rowOffset;
    for (int32_t i = 0; i < rowCount; i++) {
        if (nullMap != nullptr && (*nullMap)[i])
            continue;
        if (vec->IsNull(i + rowOffset))
            continue;
        double x = ptr[i];
        double oldAvg = acc->avg;
        acc->n += 1;
        acc->avg += (x - acc->avg) / acc->n;
        acc->m2 += (x - oldAvg) * (x - acc->avg);
    }
}

void RegrReplacementAggregator::ProcessSingleMerge(RegrReplacementState *acc, int32_t rowOffset, int32_t rowCount)
{
    auto *v0 = curVectorBatch->Get(channels[0]);
    auto *v1 = curVectorBatch->Get(channels[1]);
    auto *v2 = curVectorBatch->Get(channels[2]);
    double *p1 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v1));
    double *p2 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v2));
    if (v0->GetTypeId() == OMNI_LONG) {
        auto *p0 = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(v0));
        for (int32_t i = 0; i < rowCount; i++) {
            int32_t row = i + rowOffset;
            if (v0->IsNull(row))
                continue;
            RegrReplacementMerge(acc->n, acc->avg, acc->m2, p0[row], p1[row], p2[row]);
        }
    } else {
        double *p0 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v0));
        for (int32_t i = 0; i < rowCount; i++) {
            int32_t row = i + rowOffset;
            if (v0->IsNull(row))
                continue;
            RegrReplacementMerge(acc->n, acc->avg, acc->m2, static_cast<int64_t>(p0[row]), p1[row], p2[row]);
        }
    }
}

void RegrReplacementAggregator::ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
    const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap)
{
    RegrReplacementState *acc = reinterpret_cast<RegrReplacementState *>(state + aggStateOffset);
    if (inputRaw) {
        BaseVector *vec = curVectorBatch->Get(channels[0]);
        ProcessSingleRaw(acc, vec, rowOffset, rowCount, nullMap);
    } else {
        ProcessSingleMerge(acc, rowOffset, rowCount);
    }
}

void RegrReplacementAggregator::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector,
    const int32_t rowOffset, const std::shared_ptr<NullsHelper> nullMap)
{
    if (inputRaw) {
        BaseVector *vec = curVectorBatch->Get(channels[0]);
        auto *ptr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(vec));
        ptr += rowOffset;
        size_t n = rowStates.size();
        for (size_t i = 0; i < n; i++) {
            if (nullMap != nullptr && (*nullMap)[i])
                continue;
            int32_t row = static_cast<int32_t>(i) + rowOffset;
            if (vec->IsNull(row))
                continue;
            RegrReplacementState *acc = reinterpret_cast<RegrReplacementState *>(rowStates[i] + aggStateOffset);
            double x = ptr[i];
            double oldAvg = acc->avg;
            acc->n += 1;
            acc->avg += (x - acc->avg) / acc->n;
            acc->m2 += (x - oldAvg) * (x - acc->avg);
        }
    } else {
        auto *v0 = curVectorBatch->Get(channels[0]);
        auto *v1 = curVectorBatch->Get(channels[1]);
        auto *v2 = curVectorBatch->Get(channels[2]);
        double *p1 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v1)) + rowOffset;
        double *p2 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v2)) + rowOffset;
        size_t n = rowStates.size();
        if (v0->GetTypeId() == OMNI_LONG) {
            auto *p0 = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(v0)) + rowOffset;
            for (size_t i = 0; i < n; i++) {
                if (nullMap != nullptr && (*nullMap)[i])
                    continue;
                int32_t row = static_cast<int32_t>(i) + rowOffset;
                if (v0->IsNull(row))
                    continue;
                RegrReplacementState *acc = reinterpret_cast<RegrReplacementState *>(rowStates[i] + aggStateOffset);
                RegrReplacementMerge(acc->n, acc->avg, acc->m2, p0[i], p1[i], p2[i]);
            }
        } else {
            double *p0 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v0)) + rowOffset;
            for (size_t i = 0; i < n; i++) {
                if (nullMap != nullptr && (*nullMap)[i])
                    continue;
                int32_t row = static_cast<int32_t>(i) + rowOffset;
                if (v0->IsNull(row))
                    continue;
                RegrReplacementState *acc = reinterpret_cast<RegrReplacementState *>(rowStates[i] + aggStateOffset);
                RegrReplacementMerge(acc->n, acc->avg, acc->m2, static_cast<int64_t>(p0[i]), p1[i], p2[i]);
            }
        }
    }
}

void RegrReplacementAggregator::InitState(AggregateState *state)
{
    RegrReplacementState *acc = reinterpret_cast<RegrReplacementState *>(state + aggStateOffset);
    acc->n = 0;
    acc->avg = 0;
    acc->m2 = 0;
}

void RegrReplacementAggregator::InitStates(std::vector<AggregateState *> &groupStates)
{
    for (auto *s : groupStates)
        InitState(s);
}

void RegrReplacementAggregator::ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors,
    int32_t rowIndex)
{
    const RegrReplacementState *s = reinterpret_cast<const RegrReplacementState *>(state + aggStateOffset);
    if (outputPartial) {
        if (vectors.size() < 3)
            return;
        if (s->IsEmpty()) {
            vectors[0]->SetNull(rowIndex);
            static_cast<Vector<double> *>(vectors[1])->SetNull(rowIndex);
            static_cast<Vector<double> *>(vectors[2])->SetNull(rowIndex);
            return;
        }
        if (outputTypes.GetType(0)->GetId() == OMNI_LONG)
            static_cast<Vector<int64_t> *>(vectors[0])->SetValue(rowIndex, s->n);
        else
            static_cast<Vector<double> *>(vectors[0])->SetValue(rowIndex, static_cast<double>(s->n));
        static_cast<Vector<double> *>(vectors[1])->SetValue(rowIndex, s->avg);
        static_cast<Vector<double> *>(vectors[2])->SetValue(rowIndex, s->m2);
    } else {
        if (s->IsEmpty()) {
            vectors[0]->SetNull(rowIndex);
            return;
        }
        static_cast<Vector<double> *>(vectors[0])->SetValue(rowIndex, s->m2);
    }
}

void RegrReplacementAggregator::ExtractValuesBatch(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors, int32_t rowOffset, int32_t rowCount)
{
    if (outputPartial) {
        if (vectors.size() < 3)
            return;
        bool firstLong = (outputTypes.GetType(0)->GetId() == OMNI_LONG);
        auto *v1 = static_cast<Vector<double> *>(vectors[1]);
        auto *v2 = static_cast<Vector<double> *>(vectors[2]);
        for (int32_t i = 0; i < rowCount; i++) {
            RegrReplacementState *s = reinterpret_cast<RegrReplacementState *>(groupStates[i] + aggStateOffset);
            int32_t row = rowOffset + i;
            if (s->IsEmpty()) {
                vectors[0]->SetNull(row);
                v1->SetNull(row);
                v2->SetNull(row);
            } else {
                if (firstLong)
                    static_cast<Vector<int64_t> *>(vectors[0])->SetValue(row, s->n);
                else
                    static_cast<Vector<double> *>(vectors[0])->SetValue(row, static_cast<double>(s->n));
                v1->SetValue(row, s->avg);
                v2->SetValue(row, s->m2);
            }
        }
    } else {
        auto *outVec = static_cast<Vector<double> *>(vectors[0]);
        for (int32_t i = 0; i < rowCount; i++) {
            const RegrReplacementState *s = reinterpret_cast<const RegrReplacementState *>(groupStates[i] + aggStateOffset);
            int32_t row = rowOffset + i;
            if (s->IsEmpty())
                outVec->SetNull(row);
            else
                outVec->SetValue(row, s->m2);
        }
    }
}

void RegrReplacementAggregator::ExtractValuesForSpill(std::vector<AggregateState *> &groupStates,
    std::vector<BaseVector *> &vectors)
{
    auto *v0 = static_cast<Vector<int64_t> *>(vectors[0]);
    auto *v1 = static_cast<Vector<double> *>(vectors[1]);
    auto *v2 = static_cast<Vector<double> *>(vectors[2]);
    int32_t rowCount = static_cast<int32_t>(groupStates.size());
    for (int32_t i = 0; i < rowCount; i++) {
        RegrReplacementState *s = reinterpret_cast<RegrReplacementState *>(groupStates[i] + aggStateOffset);
        if (s->IsEmpty()) {
            v0->SetNull(i);
            v1->SetNull(i);
            v2->SetNull(i);
        } else {
            v0->SetValue(i, s->n);
            v1->SetValue(i, s->avg);
            v2->SetValue(i, s->m2);
        }
    }
}

void RegrReplacementAggregator::ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
    int32_t &vectorIndex)
{
    auto nIdx = vectorIndex++;
    auto avgIdx = vectorIndex++;
    auto m2Idx = vectorIndex++;
    for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        auto &row = unspillRows[rowIdx];
        auto *batch = row.batch;
        auto index = row.rowIdx;
        auto *nVec = static_cast<Vector<int64_t> *>(batch->Get(nIdx));
        if (nVec->IsNull(index))
            continue;
        int64_t n = nVec->GetValue(index);
        auto *avgVec = static_cast<Vector<double> *>(batch->Get(avgIdx));
        auto *m2Vec = static_cast<Vector<double> *>(batch->Get(m2Idx));
        auto *acc = reinterpret_cast<RegrReplacementState *>(row.state + aggStateOffset);
        RegrReplacementMerge(acc->n, acc->avg, acc->m2, n, avgVec->GetValue(index), m2Vec->GetValue(index));
    }
}

void RegrReplacementAggregator::ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
    const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter)
{
    (void)result;
    (void)originVector;
    (void)nullMap;
    (void)aggFilter;
    throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
        "RegrReplacementAggregator::ProcessAlignAggSchema not supported");
}

std::vector<DataTypePtr> RegrReplacementAggregator::GetSpillType()
{
    std::vector<DataTypePtr> types;
    types.push_back(std::make_shared<type::DataType>(type::DataTypeId::OMNI_LONG));
    types.push_back(std::make_shared<type::DataType>(type::DataTypeId::OMNI_DOUBLE));
    types.push_back(std::make_shared<type::DataType>(type::DataTypeId::OMNI_DOUBLE));
    return types;
}

// --- RegrAggregator (full regr state; no longer used for REGR_REPLACEMENT) ---

RegrAggregator::RegrAggregator(FunctionType aggType_, const DataTypes &inputTypes, const DataTypes &outputTypes,
    std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
    : TypedAggregator(aggType_, inputTypes, outputTypes, channels, inputRaw, outputPartial, isOverflowAsNull)
    , aggType(aggType_)
{
}

void RegrAggregator::ProcessSingleRaw(RegrState *acc, BaseVector *yVec, BaseVector *xVec, int32_t rowOffset,
    int32_t rowCount, const std::shared_ptr<NullsHelper> &nullMap)
{
    if (aggType == OMNI_AGGREGATION_TYPE_REGR_SXX) {
        for (int32_t i = 0; i < rowCount; i++) {
            if (nullMap != nullptr && (*nullMap)[i])
                continue;
            if (yVec->IsNull(i + rowOffset) || xVec->IsNull(i + rowOffset))
                continue;
            double x = RegrGetDoubleAt(xVec, i + rowOffset);
            double oldMeanX = acc->meanX;
            acc->count += 1;
            acc->meanX += (x - acc->meanX) / acc->count;
            acc->m2X += (x - oldMeanX) * (x - acc->meanX);
        }
        return;
    }
    if (aggType == OMNI_AGGREGATION_TYPE_REGR_SYY) {
        for (int32_t i = 0; i < rowCount; i++) {
            if (nullMap != nullptr && (*nullMap)[i])
                continue;
            if (yVec->IsNull(i + rowOffset) || xVec->IsNull(i + rowOffset))
                continue;
            double y = RegrGetDoubleAt(yVec, i + rowOffset);
            double oldMeanY = acc->meanY;
            acc->count += 1;
            acc->meanY += (y - acc->meanY) / acc->count;
            acc->m2Y += (y - oldMeanY) * (y - acc->meanY);
        }
        return;
    }
    for (int32_t i = 0; i < rowCount; i++) {
        if (nullMap != nullptr && (*nullMap)[i])
            continue;
        if (yVec->IsNull(i + rowOffset) || xVec->IsNull(i + rowOffset))
            continue;
        double y = RegrGetDoubleAt(yVec, i + rowOffset);
        double x = RegrGetDoubleAt(xVec, i + rowOffset);
        double oldMeanX = acc->meanX;
        double oldMeanY = acc->meanY;
        acc->count += 1;
        double deltaX = x - acc->meanX;
        acc->meanX += deltaX / acc->count;
        double deltaY = y - acc->meanY;
        acc->meanY += deltaY / acc->count;
        acc->c2 += deltaX * (y - acc->meanY);
        acc->m2X += (x - oldMeanX) * (x - acc->meanX);
        acc->m2Y += (y - oldMeanY) * (y - acc->meanY);
    }
}

void RegrAggregator::ProcessSingleMerge(RegrState *acc, int32_t rowOffset, int32_t rowCount)
{
    const size_t nCol = channels.size();
    if (nCol == 6) {
        /* Native 6-col: (count, meanX, meanY, c2, m2X, m2Y) */
        auto *cntVec = curVectorBatch->Get(channels[0]);
        auto *meanXVec = curVectorBatch->Get(channels[1]);
        auto *meanYVec = curVectorBatch->Get(channels[2]);
        auto *c2Vec = curVectorBatch->Get(channels[3]);
        auto *m2XVec = curVectorBatch->Get(channels[4]);
        auto *m2YVec = curVectorBatch->Get(channels[5]);
        auto *meanXPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(meanXVec));
        auto *meanYPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(meanYVec));
        auto *c2Ptr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(c2Vec));
        auto *m2XPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(m2XVec));
        auto *m2YPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(m2YVec));
        if (cntVec->GetTypeId() == OMNI_LONG) {
            auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(cntVec));
            for (int32_t i = 0; i < rowCount; i++) {
                int32_t row = i + rowOffset;
                if (cntVec->IsNull(row))
                    continue;
                RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                    cntPtr[row], meanXPtr[row], meanYPtr[row], c2Ptr[row], m2XPtr[row], m2YPtr[row]);
            }
        } else if (cntVec->GetTypeId() == OMNI_INT) {
            auto *cntPtr = reinterpret_cast<int32_t *>(GetValuesFromVector<OMNI_INT>(cntVec));
            for (int32_t i = 0; i < rowCount; i++) {
                int32_t row = i + rowOffset;
                if (cntVec->IsNull(row))
                    continue;
                RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                    static_cast<int64_t>(cntPtr[row]), meanXPtr[row], meanYPtr[row], c2Ptr[row], m2XPtr[row], m2YPtr[row]);
            }
        } else {
            auto *cntPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(cntVec));
            for (int32_t i = 0; i < rowCount; i++) {
                int32_t row = i + rowOffset;
                if (cntVec->IsNull(row))
                    continue;
                RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                    static_cast<int64_t>(cntPtr[row]), meanXPtr[row], meanYPtr[row], c2Ptr[row], m2XPtr[row], m2YPtr[row]);
            }
        }
        return;
    }
    if (nCol == 7) {
        /* regr_r2 with key+6 buffer: (key, n, xAvg, yAvg, ck, xMk, yMk) — read buffer from channels 1..6 */
        if (aggType == OMNI_AGGREGATION_TYPE_REGR_R2) {
            auto *cntVec = curVectorBatch->Get(channels[1]);
            auto *meanXVec = curVectorBatch->Get(channels[2]);
            auto *meanYVec = curVectorBatch->Get(channels[3]);
            auto *c2Vec = curVectorBatch->Get(channels[4]);
            auto *m2XVec = curVectorBatch->Get(channels[5]);
            auto *m2YVec = curVectorBatch->Get(channels[6]);
            double *meanXPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(meanXVec));
            double *meanYPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(meanYVec));
            double *c2Ptr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(c2Vec));
            double *m2XPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(m2XVec));
            double *m2YPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(m2YVec));
            if (cntVec->GetTypeId() == OMNI_LONG) {
                auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(cntVec));
                for (int32_t i = 0; i < rowCount; i++) {
                    int32_t row = i + rowOffset;
                    if (cntVec->IsNull(row))
                        continue;
                    RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                        cntPtr[row], meanXPtr[row], meanYPtr[row], c2Ptr[row], m2XPtr[row], m2YPtr[row]);
                }
            } else if (cntVec->GetTypeId() == OMNI_INT) {
                auto *cntPtr = reinterpret_cast<int32_t *>(GetValuesFromVector<OMNI_INT>(cntVec));
                for (int32_t i = 0; i < rowCount; i++) {
                    int32_t row = i + rowOffset;
                    if (cntVec->IsNull(row))
                        continue;
                    RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                        static_cast<int64_t>(cntPtr[row]), meanXPtr[row], meanYPtr[row], c2Ptr[row], m2XPtr[row], m2YPtr[row]);
                }
            } else {
                double *cntPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(cntVec));
                for (int32_t i = 0; i < rowCount; i++) {
                    int32_t row = i + rowOffset;
                    if (cntVec->IsNull(row))
                        continue;
                    RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                        static_cast<int64_t>(cntPtr[row]), meanXPtr[row], meanYPtr[row], c2Ptr[row], m2XPtr[row], m2YPtr[row]);
                }
            }
            return;
        }
        /* Spark Covariance+VarPop: (n, xAvg, yAvg, ck, n, avg, m2) -> (count, meanX, meanY, c2, m2X, m2Y=0) */
        auto *v0 = curVectorBatch->Get(channels[0]);
        auto *v2 = curVectorBatch->Get(channels[2]);
        auto *v3 = curVectorBatch->Get(channels[3]);
        auto *v5 = curVectorBatch->Get(channels[5]);
        auto *v6 = curVectorBatch->Get(channels[6]);
        double *p2 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v2));
        double *p3 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v3));
        double *p5 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v5));
        double *p6 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v6));
        if (v0->GetTypeId() == OMNI_LONG) {
            auto *p0 = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(v0));
            for (int32_t i = 0; i < rowCount; i++) {
                int32_t row = i + rowOffset;
                if (v0->IsNull(row))
                    continue;
                RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                    p0[row], p5[row], p2[row], p3[row], p6[row], 0.0);
            }
        } else if (v0->GetTypeId() == OMNI_INT) {
            auto *p0 = reinterpret_cast<int32_t *>(GetValuesFromVector<OMNI_INT>(v0));
            for (int32_t i = 0; i < rowCount; i++) {
                int32_t row = i + rowOffset;
                if (v0->IsNull(row))
                    continue;
                RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                    static_cast<int64_t>(p0[row]), p5[row], p2[row], p3[row], p6[row], 0.0);
            }
        } else {
            double *p0 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v0));
            for (int32_t i = 0; i < rowCount; i++) {
                int32_t row = i + rowOffset;
                if (v0->IsNull(row))
                    continue;
                RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                    static_cast<int64_t>(p0[row]), p5[row], p2[row], p3[row], p6[row], 0.0);
            }
        }
        return;
    }
    if (nCol == 4) {
        /* Spark regr_sxy / Covariance partial buffer: (n, xAvg, yAvg, ck) -> merge with m2X=0, m2Y=0 */
        auto *v0 = curVectorBatch->Get(channels[0]);
        auto *v1 = curVectorBatch->Get(channels[1]);
        auto *v2 = curVectorBatch->Get(channels[2]);
        auto *v3 = curVectorBatch->Get(channels[3]);
        double *p1 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v1));
        double *p2 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v2));
        double *p3 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v3));
        if (v0->GetTypeId() == OMNI_LONG) {
            auto *p0 = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(v0));
            for (int32_t i = 0; i < rowCount; i++) {
                int32_t row = i + rowOffset;
                if (v0->IsNull(row))
                    continue;
                RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                    p0[row], p1[row], p2[row], p3[row], 0.0, 0.0);
            }
        } else {
            double *p0 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v0));
            for (int32_t i = 0; i < rowCount; i++) {
                int32_t row = i + rowOffset;
                if (v0->IsNull(row))
                    continue;
                RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                    static_cast<int64_t>(p0[row]), p1[row], p2[row], p3[row], 0.0, 0.0);
            }
        }
        return;
    }
    if (nCol == 3 && (aggType == OMNI_AGGREGATION_TYPE_REGR_SXX || aggType == OMNI_AGGREGATION_TYPE_REGR_SYY)) {
        /* Spark RegrReplacement 3-col buffer (n, avg, m2) for regr_sxx/regr_syy as two-arg aggregate */
        auto *v0 = curVectorBatch->Get(channels[0]);
        auto *v1 = curVectorBatch->Get(channels[1]);
        auto *v2 = curVectorBatch->Get(channels[2]);
        double *p1 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v1));
        double *p2 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v2));
        if (v0->GetTypeId() == OMNI_LONG) {
            auto *p0 = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(v0));
            for (int32_t i = 0; i < rowCount; i++) {
                int32_t row = i + rowOffset;
                if (v0->IsNull(row))
                    continue;
                if (aggType == OMNI_AGGREGATION_TYPE_REGR_SXX)
                    RegrReplacementMerge(acc->count, acc->meanX, acc->m2X, p0[row], p1[row], p2[row]);
                else
                    RegrReplacementMerge(acc->count, acc->meanY, acc->m2Y, p0[row], p1[row], p2[row]);
            }
        } else {
            double *p0 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v0));
            for (int32_t i = 0; i < rowCount; i++) {
                int32_t row = i + rowOffset;
                if (v0->IsNull(row))
                    continue;
                if (aggType == OMNI_AGGREGATION_TYPE_REGR_SXX)
                    RegrReplacementMerge(acc->count, acc->meanX, acc->m2X, static_cast<int64_t>(p0[row]), p1[row], p2[row]);
                else
                    RegrReplacementMerge(acc->count, acc->meanY, acc->m2Y, static_cast<int64_t>(p0[row]), p1[row], p2[row]);
            }
        }
        return;
    }
    throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
        "Regr aggregate merge requires 3, 4, 6, or 7 columns, got " + std::to_string(nCol));
}

void RegrAggregator::ProcessSingleInternal(AggregateState *state, BaseVector *vector, const int32_t rowOffset,
    const int32_t rowCount, const std::shared_ptr<NullsHelper> nullMap)
{
    RegrState *acc = reinterpret_cast<RegrState *>(state + aggStateOffset);
    if (inputRaw) {
        BaseVector *yVec = curVectorBatch->Get(channels[0]);
        BaseVector *xVec = curVectorBatch->Get(channels[1]);
        ProcessSingleRaw(acc, yVec, xVec, rowOffset, rowCount, nullMap);
    } else {
        ProcessSingleMerge(acc, rowOffset, rowCount);
    }
}

void RegrAggregator::ProcessGroupInternal(std::vector<AggregateState *> &rowStates, BaseVector *vector,
    const int32_t rowOffset, const std::shared_ptr<NullsHelper> nullMap)
{
    if (inputRaw) {
        BaseVector *yVec = curVectorBatch->Get(channels[0]);
        BaseVector *xVec = curVectorBatch->Get(channels[1]);
        size_t n = rowStates.size();
        for (size_t i = 0; i < n; i++) {
            if (nullMap != nullptr && (*nullMap)[i])
                continue;
            int32_t row = static_cast<int32_t>(i) + rowOffset;
            if (yVec->IsNull(row) || xVec->IsNull(row))
                continue;
            RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
            double y = RegrGetDoubleAt(yVec, row);
            double x = RegrGetDoubleAt(xVec, row);
            double oldMeanX = acc->meanX;
            double oldMeanY = acc->meanY;
            acc->count += 1;
            double deltaX = x - acc->meanX;
            acc->meanX += deltaX / acc->count;
            double deltaY = y - acc->meanY;
            acc->meanY += deltaY / acc->count;
            acc->c2 += deltaX * (y - acc->meanY);
            acc->m2X += (x - oldMeanX) * (x - acc->meanX);
            acc->m2Y += (y - oldMeanY) * (y - acc->meanY);
        }
    } else {
        const size_t nCol = channels.size();
        if (nCol == 6) {
            /* Native 6-col: (count, meanX, meanY, c2, m2X, m2Y) */
            auto *cntVec = curVectorBatch->Get(channels[0]);
            auto *meanXVec = curVectorBatch->Get(channels[1]);
            auto *meanYVec = curVectorBatch->Get(channels[2]);
            auto *c2Vec = curVectorBatch->Get(channels[3]);
            auto *m2XVec = curVectorBatch->Get(channels[4]);
            auto *m2YVec = curVectorBatch->Get(channels[5]);
            auto *meanXPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(meanXVec));
            auto *meanYPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(meanYVec));
            auto *c2Ptr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(c2Vec));
            auto *m2XPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(m2XVec));
            auto *m2YPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(m2YVec));
            meanXPtr += rowOffset;
            meanYPtr += rowOffset;
            c2Ptr += rowOffset;
            m2XPtr += rowOffset;
            m2YPtr += rowOffset;
            size_t n = rowStates.size();
            if (cntVec->GetTypeId() == OMNI_LONG) {
                auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(cntVec)) + rowOffset;
                for (size_t i = 0; i < n; i++) {
                    int32_t row = static_cast<int32_t>(i) + rowOffset;
                    if (cntVec->IsNull(row))
                        continue;
                    RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                    RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                        cntPtr[i], meanXPtr[i], meanYPtr[i], c2Ptr[i], m2XPtr[i], m2YPtr[i]);
                }
            } else if (cntVec->GetTypeId() == OMNI_INT) {
                auto *cntPtr = reinterpret_cast<int32_t *>(GetValuesFromVector<OMNI_INT>(cntVec)) + rowOffset;
                for (size_t i = 0; i < n; i++) {
                    int32_t row = static_cast<int32_t>(i) + rowOffset;
                    if (cntVec->IsNull(row))
                        continue;
                    RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                    RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                        static_cast<int64_t>(cntPtr[i]), meanXPtr[i], meanYPtr[i], c2Ptr[i], m2XPtr[i], m2YPtr[i]);
                }
            } else {
                auto *cntPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(cntVec)) + rowOffset;
                for (size_t i = 0; i < n; i++) {
                    int32_t row = static_cast<int32_t>(i) + rowOffset;
                    if (cntVec->IsNull(row))
                        continue;
                    RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                    RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                        static_cast<int64_t>(cntPtr[i]), meanXPtr[i], meanYPtr[i], c2Ptr[i], m2XPtr[i], m2YPtr[i]);
                }
            }
        } else if (nCol == 7) {
            /* regr_r2 with key+6 buffer: (key, n, xAvg, yAvg, ck, xMk, yMk) — read buffer from channels 1..6 */
            if (aggType == OMNI_AGGREGATION_TYPE_REGR_R2) {
                auto *cntVec = curVectorBatch->Get(channels[1]);
                auto *meanXVec = curVectorBatch->Get(channels[2]);
                auto *meanYVec = curVectorBatch->Get(channels[3]);
                auto *c2Vec = curVectorBatch->Get(channels[4]);
                auto *m2XVec = curVectorBatch->Get(channels[5]);
                auto *m2YVec = curVectorBatch->Get(channels[6]);
                double *meanXPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(meanXVec)) + rowOffset;
                double *meanYPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(meanYVec)) + rowOffset;
                double *c2Ptr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(c2Vec)) + rowOffset;
                double *m2XPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(m2XVec)) + rowOffset;
                double *m2YPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(m2YVec)) + rowOffset;
                size_t n = rowStates.size();
                if (cntVec->GetTypeId() == OMNI_LONG) {
                    auto *cntPtr = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(cntVec)) + rowOffset;
                    for (size_t i = 0; i < n; i++) {
                        int32_t row = static_cast<int32_t>(i) + rowOffset;
                        if (cntVec->IsNull(row))
                            continue;
                        RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                        RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                            cntPtr[i], meanXPtr[i], meanYPtr[i], c2Ptr[i], m2XPtr[i], m2YPtr[i]);
                    }
                } else if (cntVec->GetTypeId() == OMNI_INT) {
                    auto *cntPtr = reinterpret_cast<int32_t *>(GetValuesFromVector<OMNI_INT>(cntVec)) + rowOffset;
                    for (size_t i = 0; i < n; i++) {
                        int32_t row = static_cast<int32_t>(i) + rowOffset;
                        if (cntVec->IsNull(row))
                            continue;
                        RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                        RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                            static_cast<int64_t>(cntPtr[i]), meanXPtr[i], meanYPtr[i], c2Ptr[i], m2XPtr[i], m2YPtr[i]);
                    }
                } else {
                    double *cntPtr = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(cntVec)) + rowOffset;
                    for (size_t i = 0; i < n; i++) {
                        int32_t row = static_cast<int32_t>(i) + rowOffset;
                        if (cntVec->IsNull(row))
                            continue;
                        RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                        RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                            static_cast<int64_t>(cntPtr[i]), meanXPtr[i], meanYPtr[i], c2Ptr[i], m2XPtr[i], m2YPtr[i]);
                    }
                }
            } else {
            auto *v0 = curVectorBatch->Get(channels[0]);
            auto *v2 = curVectorBatch->Get(channels[2]);
            auto *v3 = curVectorBatch->Get(channels[3]);
            auto *v5 = curVectorBatch->Get(channels[5]);
            auto *v6 = curVectorBatch->Get(channels[6]);
            double *p2 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v2)) + rowOffset;
            double *p3 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v3)) + rowOffset;
            double *p5 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v5)) + rowOffset;
            double *p6 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v6)) + rowOffset;
            size_t n = rowStates.size();
            if (v0->GetTypeId() == OMNI_LONG) {
                auto *p0 = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(v0)) + rowOffset;
                for (size_t i = 0; i < n; i++) {
                    int32_t row = static_cast<int32_t>(i) + rowOffset;
                    if (v0->IsNull(row))
                        continue;
                    RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                    RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                        p0[i], p5[i], p2[i], p3[i], p6[i], 0.0);
                }
            } else if (v0->GetTypeId() == OMNI_INT) {
                auto *p0 = reinterpret_cast<int32_t *>(GetValuesFromVector<OMNI_INT>(v0)) + rowOffset;
                for (size_t i = 0; i < n; i++) {
                    int32_t row = static_cast<int32_t>(i) + rowOffset;
                    if (v0->IsNull(row))
                        continue;
                    RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                    RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                        static_cast<int64_t>(p0[i]), p5[i], p2[i], p3[i], p6[i], 0.0);
                }
            } else {
                double *p0 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v0)) + rowOffset;
                for (size_t i = 0; i < n; i++) {
                    int32_t row = static_cast<int32_t>(i) + rowOffset;
                    if (v0->IsNull(row))
                        continue;
                    RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                    RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                        static_cast<int64_t>(p0[i]), p5[i], p2[i], p3[i], p6[i], 0.0);
                }
            }
            }
        } else if (nCol == 4) {
            auto *v0 = curVectorBatch->Get(channels[0]);
            auto *v1 = curVectorBatch->Get(channels[1]);
            auto *v2 = curVectorBatch->Get(channels[2]);
            auto *v3 = curVectorBatch->Get(channels[3]);
            double *p1 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v1)) + rowOffset;
            double *p2 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v2)) + rowOffset;
            double *p3 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v3)) + rowOffset;
            size_t n = rowStates.size();
            if (v0->GetTypeId() == OMNI_LONG) {
                auto *p0 = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(v0)) + rowOffset;
                for (size_t i = 0; i < n; i++) {
                    int32_t row = static_cast<int32_t>(i) + rowOffset;
                    if (v0->IsNull(row))
                        continue;
                    RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                    RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                        p0[i], p1[i], p2[i], p3[i], 0.0, 0.0);
                }
            } else {
                double *p0 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v0)) + rowOffset;
                for (size_t i = 0; i < n; i++) {
                    int32_t row = static_cast<int32_t>(i) + rowOffset;
                    if (v0->IsNull(row))
                        continue;
                    RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                    RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y,
                        static_cast<int64_t>(p0[i]), p1[i], p2[i], p3[i], 0.0, 0.0);
                }
            }
        } else if (nCol == 3 && (aggType == OMNI_AGGREGATION_TYPE_REGR_SXX || aggType == OMNI_AGGREGATION_TYPE_REGR_SYY)) {
            auto *v0 = curVectorBatch->Get(channels[0]);
            auto *v1 = curVectorBatch->Get(channels[1]);
            auto *v2 = curVectorBatch->Get(channels[2]);
            double *p1 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v1)) + rowOffset;
            double *p2 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v2)) + rowOffset;
            size_t n = rowStates.size();
            if (v0->GetTypeId() == OMNI_LONG) {
                auto *p0 = reinterpret_cast<int64_t *>(GetValuesFromVector<OMNI_LONG>(v0)) + rowOffset;
                for (size_t i = 0; i < n; i++) {
                    int32_t row = static_cast<int32_t>(i) + rowOffset;
                    if (v0->IsNull(row))
                        continue;
                    RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                    if (aggType == OMNI_AGGREGATION_TYPE_REGR_SXX)
                        RegrReplacementMerge(acc->count, acc->meanX, acc->m2X, p0[i], p1[i], p2[i]);
                    else
                        RegrReplacementMerge(acc->count, acc->meanY, acc->m2Y, p0[i], p1[i], p2[i]);
                }
            } else {
                double *p0 = reinterpret_cast<double *>(GetValuesFromVector<OMNI_DOUBLE>(v0)) + rowOffset;
                for (size_t i = 0; i < n; i++) {
                    int32_t row = static_cast<int32_t>(i) + rowOffset;
                    if (v0->IsNull(row))
                        continue;
                    RegrState *acc = reinterpret_cast<RegrState *>(rowStates[i] + aggStateOffset);
                    if (aggType == OMNI_AGGREGATION_TYPE_REGR_SXX)
                        RegrReplacementMerge(acc->count, acc->meanX, acc->m2X, static_cast<int64_t>(p0[i]), p1[i], p2[i]);
                    else
                        RegrReplacementMerge(acc->count, acc->meanY, acc->m2Y, static_cast<int64_t>(p0[i]), p1[i], p2[i]);
                }
            }
        } else {
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
                "Regr aggregate merge requires 3, 4, 6, or 7 columns, got " + std::to_string(nCol));
        }
    }
}

void RegrAggregator::InitState(AggregateState *state)
{
    RegrState *acc = reinterpret_cast<RegrState *>(state + aggStateOffset);
    acc->count = 0;
    acc->meanX = 0;
    acc->meanY = 0;
    acc->c2 = 0;
    acc->m2X = 0;
    acc->m2Y = 0;
}

void RegrAggregator::InitStates(std::vector<AggregateState *> &groupStates)
{
    for (auto *s : groupStates)
        InitState(s);
}

void RegrAggregator::ExtractFinalValue(const RegrState *s, BaseVector *outVec, int32_t rowIndex) const
{
    if (s->IsEmpty()) {
        outVec->SetNull(rowIndex);
        return;
    }
    double result = 0;
    bool hasResult = false;
    switch (aggType) {
        case OMNI_AGGREGATION_TYPE_REGR_COUNT:
            hasResult = s->count > 0;
            result = static_cast<double>(s->count);
            break;
        case OMNI_AGGREGATION_TYPE_REGR_SXX:
            hasResult = s->count > 0;
            result = s->m2X;
            break;
        case OMNI_AGGREGATION_TYPE_REGR_SYY:
            hasResult = s->count > 0;
            result = s->m2Y;
            break;
        case OMNI_AGGREGATION_TYPE_REGR_SXY:
            hasResult = s->count > 0;
            result = s->c2;
            break;
        case OMNI_AGGREGATION_TYPE_REGR_SLOPE:
            if (s->m2X != 0) {
                hasResult = true;
                result = s->c2 / s->m2X;
            }
            break;
        case OMNI_AGGREGATION_TYPE_REGR_INTERCEPT:
            if (s->m2X != 0) {
                hasResult = true;
                double slope = s->c2 / s->m2X;
                result = s->meanY - slope * s->meanX;
            }
            break;
        case OMNI_AGGREGATION_TYPE_REGR_R2:
            /* Degenerate: n>1 and (sxx==0 or syy==0) => 1.0 to match Spark. */
            if (s->count > 1 && (s->m2X == 0.0 || s->m2Y == 0.0)) {
                hasResult = true;
                result = 1.0;
            } else if (s->m2X != 0.0 && s->m2Y != 0.0) {
                double denom = s->m2X * s->m2Y;
                hasResult = true;
                if (denom == 0.0 || !std::isfinite(denom))
                    result = 1.0;
                else
                    result = (s->c2 * s->c2) / denom;
            }
            break;
        default:
            break;
    }
    if (!hasResult) {
        outVec->SetNull(rowIndex);
        return;
    }
    if (outputTypes.GetType(0)->GetId() == OMNI_LONG && aggType == OMNI_AGGREGATION_TYPE_REGR_COUNT) {
        static_cast<Vector<int64_t> *>(outVec)->SetValue(rowIndex, static_cast<int64_t>(result));
    } else {
        // Match Spark/JVM: normalize IEEE -0.0 to +0.0 for display and equality (e.g. ROUND).
        if (result == 0.0) {
            result = 0.0;
        }
        static_cast<Vector<double> *>(outVec)->SetValue(rowIndex, result);
    }
}

void RegrAggregator::ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex)
{
    const RegrState *s = reinterpret_cast<const RegrState *>(state + aggStateOffset);
    if (outputPartial) {
        const size_t nOut = static_cast<size_t>(outputTypes.GetSize());
        if (nOut >= 7) {
            /* Spark RegrSlope/RegrIntercept buffer: (n, xAvg, yAvg, ck, n, avg, m2) as 7 doubles */
            if (vectors.size() < 7)
                return;
            auto *v0 = static_cast<Vector<double> *>(vectors[0]);
            auto *v1 = static_cast<Vector<double> *>(vectors[1]);
            auto *v2 = static_cast<Vector<double> *>(vectors[2]);
            auto *v3 = static_cast<Vector<double> *>(vectors[3]);
            auto *v4 = static_cast<Vector<double> *>(vectors[4]);
            auto *v5 = static_cast<Vector<double> *>(vectors[5]);
            auto *v6 = static_cast<Vector<double> *>(vectors[6]);
            if (s->IsEmpty()) {
                v0->SetNull(rowIndex);
                v1->SetNull(rowIndex);
                v2->SetNull(rowIndex);
                v3->SetNull(rowIndex);
                v4->SetNull(rowIndex);
                v5->SetNull(rowIndex);
                v6->SetNull(rowIndex);
                return;
            }
            double n = static_cast<double>(s->count);
            v0->SetValue(rowIndex, n);
            v1->SetValue(rowIndex, s->meanX);
            v2->SetValue(rowIndex, s->meanY);
            v3->SetValue(rowIndex, s->c2);
            v4->SetValue(rowIndex, n);
            v5->SetValue(rowIndex, s->meanX);
            v6->SetValue(rowIndex, s->m2X);
        } else if (nOut == 3 && vectors.size() >= 3 &&
            (aggType == OMNI_AGGREGATION_TYPE_REGR_SXX || aggType == OMNI_AGGREGATION_TYPE_REGR_SYY)) {
            /* regr_sxx/regr_syy partial: (count, mean, m2) — count/meanX/m2X for SXX, count/meanY/m2Y for SYY */
            if (s->IsEmpty()) {
                vectors[0]->SetNull(rowIndex);
                static_cast<Vector<double> *>(vectors[1])->SetNull(rowIndex);
                static_cast<Vector<double> *>(vectors[2])->SetNull(rowIndex);
                return;
            }
            if (outputTypes.GetType(0)->GetId() == OMNI_LONG)
                static_cast<Vector<int64_t> *>(vectors[0])->SetValue(rowIndex, s->count);
            else
                static_cast<Vector<double> *>(vectors[0])->SetValue(rowIndex, static_cast<double>(s->count));
            if (aggType == OMNI_AGGREGATION_TYPE_REGR_SXX) {
                static_cast<Vector<double> *>(vectors[1])->SetValue(rowIndex, s->meanX);
                static_cast<Vector<double> *>(vectors[2])->SetValue(rowIndex, s->m2X);
            } else {
                static_cast<Vector<double> *>(vectors[1])->SetValue(rowIndex, s->meanY);
                static_cast<Vector<double> *>(vectors[2])->SetValue(rowIndex, s->m2Y);
            }
        } else if (nOut == 4 && vectors.size() >= 4) {
            /* Spark regr_sxy partial buffer: (n, xAvg, yAvg, ck) as 4 columns */
            if (outputTypes.GetType(0)->GetId() == OMNI_LONG) {
                auto *v0 = static_cast<Vector<int64_t> *>(vectors[0]);
                auto *v1 = static_cast<Vector<double> *>(vectors[1]);
                auto *v2 = static_cast<Vector<double> *>(vectors[2]);
                auto *v3 = static_cast<Vector<double> *>(vectors[3]);
                if (s->IsEmpty()) {
                    v0->SetNull(rowIndex);
                    v1->SetNull(rowIndex);
                    v2->SetNull(rowIndex);
                    v3->SetNull(rowIndex);
                    return;
                }
                v0->SetValue(rowIndex, s->count);
                v1->SetValue(rowIndex, s->meanX);
                v2->SetValue(rowIndex, s->meanY);
                v3->SetValue(rowIndex, s->c2);
            } else {
                auto *v0 = static_cast<Vector<double> *>(vectors[0]);
                auto *v1 = static_cast<Vector<double> *>(vectors[1]);
                auto *v2 = static_cast<Vector<double> *>(vectors[2]);
                auto *v3 = static_cast<Vector<double> *>(vectors[3]);
                if (s->IsEmpty()) {
                    v0->SetNull(rowIndex);
                    v1->SetNull(rowIndex);
                    v2->SetNull(rowIndex);
                    v3->SetNull(rowIndex);
                    return;
                }
                v0->SetValue(rowIndex, static_cast<double>(s->count));
                v1->SetValue(rowIndex, s->meanX);
                v2->SetValue(rowIndex, s->meanY);
                v3->SetValue(rowIndex, s->c2);
            }
        } else {
            /* 6-column native partial: (count, meanX, meanY, c2, m2X, m2Y) */
            if (vectors.size() < 6)
                return;
            auto *cntVec = static_cast<Vector<int64_t> *>(vectors[0]);
            auto *meanXVec = static_cast<Vector<double> *>(vectors[1]);
            auto *meanYVec = static_cast<Vector<double> *>(vectors[2]);
            auto *c2Vec = static_cast<Vector<double> *>(vectors[3]);
            auto *m2XVec = static_cast<Vector<double> *>(vectors[4]);
            auto *m2YVec = static_cast<Vector<double> *>(vectors[5]);
            if (s->IsEmpty()) {
                cntVec->SetNull(rowIndex);
                meanXVec->SetNull(rowIndex);
                meanYVec->SetNull(rowIndex);
                c2Vec->SetNull(rowIndex);
                m2XVec->SetNull(rowIndex);
                m2YVec->SetNull(rowIndex);
                return;
            }
            cntVec->SetValue(rowIndex, s->count);
            meanXVec->SetValue(rowIndex, s->meanX);
            meanYVec->SetValue(rowIndex, s->meanY);
            c2Vec->SetValue(rowIndex, s->c2);
            m2XVec->SetValue(rowIndex, s->m2X);
            m2YVec->SetValue(rowIndex, s->m2Y);
        }
    } else {
        ExtractFinalValue(s, vectors[0], rowIndex);
    }
}

void RegrAggregator::ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
    int32_t rowOffset, int32_t rowCount)
{
    if (outputPartial) {
        const size_t nOut = static_cast<size_t>(outputTypes.GetSize());
        if (nOut >= 7 && vectors.size() >= 7) {
            auto *v0 = static_cast<Vector<double> *>(vectors[0]);
            auto *v1 = static_cast<Vector<double> *>(vectors[1]);
            auto *v2 = static_cast<Vector<double> *>(vectors[2]);
            auto *v3 = static_cast<Vector<double> *>(vectors[3]);
            auto *v4 = static_cast<Vector<double> *>(vectors[4]);
            auto *v5 = static_cast<Vector<double> *>(vectors[5]);
            auto *v6 = static_cast<Vector<double> *>(vectors[6]);
            for (int32_t i = 0; i < rowCount; i++) {
                RegrState *s = reinterpret_cast<RegrState *>(groupStates[i] + aggStateOffset);
                int32_t row = rowOffset + i;
                if (s->IsEmpty()) {
                    v0->SetNull(row);
                    v1->SetNull(row);
                    v2->SetNull(row);
                    v3->SetNull(row);
                    v4->SetNull(row);
                    v5->SetNull(row);
                    v6->SetNull(row);
                } else {
                    double n = static_cast<double>(s->count);
                    v0->SetValue(row, n);
                    v1->SetValue(row, s->meanX);
                    v2->SetValue(row, s->meanY);
                    v3->SetValue(row, s->c2);
                    v4->SetValue(row, n);
                    v5->SetValue(row, s->meanX);
                    v6->SetValue(row, s->m2X);
                }
            }
        } else if (nOut == 3 && vectors.size() >= 3 &&
            (aggType == OMNI_AGGREGATION_TYPE_REGR_SXX || aggType == OMNI_AGGREGATION_TYPE_REGR_SYY)) {
            /* regr_sxx/regr_syy partial: (count, mean, m2) */
            bool firstLong = (outputTypes.GetType(0)->GetId() == OMNI_LONG);
            auto *v1 = static_cast<Vector<double> *>(vectors[1]);
            auto *v2 = static_cast<Vector<double> *>(vectors[2]);
            for (int32_t i = 0; i < rowCount; i++) {
                RegrState *s = reinterpret_cast<RegrState *>(groupStates[i] + aggStateOffset);
                int32_t row = rowOffset + i;
                if (s->IsEmpty()) {
                    vectors[0]->SetNull(row);
                    v1->SetNull(row);
                    v2->SetNull(row);
                } else {
                    if (firstLong)
                        static_cast<Vector<int64_t> *>(vectors[0])->SetValue(row, s->count);
                    else
                        static_cast<Vector<double> *>(vectors[0])->SetValue(row, static_cast<double>(s->count));
                    if (aggType == OMNI_AGGREGATION_TYPE_REGR_SXX) {
                        v1->SetValue(row, s->meanX);
                        v2->SetValue(row, s->m2X);
                    } else {
                        v1->SetValue(row, s->meanY);
                        v2->SetValue(row, s->m2Y);
                    }
                }
            }
        } else if (nOut == 4 && vectors.size() >= 4) {
            /* Spark regr_sxy partial buffer: (n, xAvg, yAvg, ck) */
            if (outputTypes.GetType(0)->GetId() == OMNI_LONG) {
                auto *v0 = static_cast<Vector<int64_t> *>(vectors[0]);
                auto *v1 = static_cast<Vector<double> *>(vectors[1]);
                auto *v2 = static_cast<Vector<double> *>(vectors[2]);
                auto *v3 = static_cast<Vector<double> *>(vectors[3]);
                for (int32_t i = 0; i < rowCount; i++) {
                    RegrState *s = reinterpret_cast<RegrState *>(groupStates[i] + aggStateOffset);
                    int32_t row = rowOffset + i;
                    if (s->IsEmpty()) {
                        v0->SetNull(row);
                        v1->SetNull(row);
                        v2->SetNull(row);
                        v3->SetNull(row);
                    } else {
                        v0->SetValue(row, s->count);
                        v1->SetValue(row, s->meanX);
                        v2->SetValue(row, s->meanY);
                        v3->SetValue(row, s->c2);
                    }
                }
            } else {
                auto *v0 = static_cast<Vector<double> *>(vectors[0]);
                auto *v1 = static_cast<Vector<double> *>(vectors[1]);
                auto *v2 = static_cast<Vector<double> *>(vectors[2]);
                auto *v3 = static_cast<Vector<double> *>(vectors[3]);
                for (int32_t i = 0; i < rowCount; i++) {
                    RegrState *s = reinterpret_cast<RegrState *>(groupStates[i] + aggStateOffset);
                    int32_t row = rowOffset + i;
                    if (s->IsEmpty()) {
                        v0->SetNull(row);
                        v1->SetNull(row);
                        v2->SetNull(row);
                        v3->SetNull(row);
                    } else {
                        v0->SetValue(row, static_cast<double>(s->count));
                        v1->SetValue(row, s->meanX);
                        v2->SetValue(row, s->meanY);
                        v3->SetValue(row, s->c2);
                    }
                }
            }
        } else if (vectors.size() >= 6) {
            /* 6-column native partial */
            auto *cntVec = static_cast<Vector<int64_t> *>(vectors[0]);
            auto *meanXVec = static_cast<Vector<double> *>(vectors[1]);
            auto *meanYVec = static_cast<Vector<double> *>(vectors[2]);
            auto *c2Vec = static_cast<Vector<double> *>(vectors[3]);
            auto *m2XVec = static_cast<Vector<double> *>(vectors[4]);
            auto *m2YVec = static_cast<Vector<double> *>(vectors[5]);
            for (int32_t i = 0; i < rowCount; i++) {
                RegrState *s = reinterpret_cast<RegrState *>(groupStates[i] + aggStateOffset);
                int32_t row = rowOffset + i;
                if (s->IsEmpty()) {
                    cntVec->SetNull(row);
                    meanXVec->SetNull(row);
                    meanYVec->SetNull(row);
                    c2Vec->SetNull(row);
                    m2XVec->SetNull(row);
                    m2YVec->SetNull(row);
                } else {
                    cntVec->SetValue(row, s->count);
                    meanXVec->SetValue(row, s->meanX);
                    meanYVec->SetValue(row, s->meanY);
                    c2Vec->SetValue(row, s->c2);
                    m2XVec->SetValue(row, s->m2X);
                    m2YVec->SetValue(row, s->m2Y);
                }
            }
        }
    } else {
        auto *outVec = vectors[0];
        for (int32_t i = 0; i < rowCount; i++) {
            const RegrState *s = reinterpret_cast<const RegrState *>(groupStates[i] + aggStateOffset);
            ExtractFinalValue(s, outVec, rowOffset + i);
        }
    }
}

void RegrAggregator::ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors)
{
    auto *cntVec = static_cast<Vector<int64_t> *>(vectors[0]);
    auto *meanXVec = static_cast<Vector<double> *>(vectors[1]);
    auto *meanYVec = static_cast<Vector<double> *>(vectors[2]);
    auto *c2Vec = static_cast<Vector<double> *>(vectors[3]);
    auto *m2XVec = static_cast<Vector<double> *>(vectors[4]);
    auto *m2YVec = static_cast<Vector<double> *>(vectors[5]);
    int32_t rowCount = static_cast<int32_t>(groupStates.size());
    for (int32_t i = 0; i < rowCount; i++) {
        RegrState *s = reinterpret_cast<RegrState *>(groupStates[i] + aggStateOffset);
        if (s->IsEmpty()) {
            cntVec->SetNull(i);
            meanXVec->SetNull(i);
            meanYVec->SetNull(i);
            c2Vec->SetNull(i);
            m2XVec->SetNull(i);
            m2YVec->SetNull(i);
        } else {
            cntVec->SetValue(i, s->count);
            meanXVec->SetValue(i, s->meanX);
            meanYVec->SetValue(i, s->meanY);
            c2Vec->SetValue(i, s->c2);
            m2XVec->SetValue(i, s->m2X);
            m2YVec->SetValue(i, s->m2Y);
        }
    }
}

void RegrAggregator::ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount,
    int32_t &vectorIndex)
{
    auto cntIdx = vectorIndex++;
    auto meanXIdx = vectorIndex++;
    auto meanYIdx = vectorIndex++;
    auto c2Idx = vectorIndex++;
    auto m2XIdx = vectorIndex++;
    auto m2YIdx = vectorIndex++;
    for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        auto &row = unspillRows[rowIdx];
        auto *batch = row.batch;
        auto index = row.rowIdx;
        auto *cntVec = static_cast<Vector<int64_t> *>(batch->Get(cntIdx));
        if (cntVec->IsNull(index))
            continue;
        int64_t count = cntVec->GetValue(index);
        auto *meanXVec = static_cast<Vector<double> *>(batch->Get(meanXIdx));
        auto *meanYVec = static_cast<Vector<double> *>(batch->Get(meanYIdx));
        auto *c2Vec = static_cast<Vector<double> *>(batch->Get(c2Idx));
        auto *m2XVec = static_cast<Vector<double> *>(batch->Get(m2XIdx));
        auto *m2YVec = static_cast<Vector<double> *>(batch->Get(m2YIdx));
        auto *acc = reinterpret_cast<RegrState *>(row.state + aggStateOffset);
        RegrMerge(acc->count, acc->meanX, acc->meanY, acc->c2, acc->m2X, acc->m2Y, count, meanXVec->GetValue(index),
            meanYVec->GetValue(index), c2Vec->GetValue(index), m2XVec->GetValue(index), m2YVec->GetValue(index));
    }
}

void RegrAggregator::ProcessAlignAggSchema(VectorBatch *result, BaseVector *originVector,
    const std::shared_ptr<NullsHelper> nullMap, const bool aggFilter)
{
    (void)result;
    (void)originVector;
    (void)nullMap;
    (void)aggFilter;
    throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR",
        "RegrAggregator::ProcessAlignAggSchema not supported");
}

std::vector<DataTypePtr> RegrAggregator::GetSpillType()
{
    std::vector<DataTypePtr> types;
    types.push_back(std::make_shared<type::DataType>(type::DataTypeId::OMNI_LONG));
    types.push_back(std::make_shared<type::DataType>(type::DataTypeId::OMNI_DOUBLE));
    types.push_back(std::make_shared<type::DataType>(type::DataTypeId::OMNI_DOUBLE));
    types.push_back(std::make_shared<type::DataType>(type::DataTypeId::OMNI_DOUBLE));
    types.push_back(std::make_shared<type::DataType>(type::DataTypeId::OMNI_DOUBLE));
    types.push_back(std::make_shared<type::DataType>(type::DataTypeId::OMNI_DOUBLE));
    return types;
}

} // namespace op
} // namespace omniruntime
