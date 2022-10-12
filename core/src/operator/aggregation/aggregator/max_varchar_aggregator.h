/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: Max aggregate for varchar
 */
#ifndef OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
#define OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H

#include "min_varchar_aggregator.h"
#ifdef ENABLE_HMPP
#include "HMPP/hmpps.h"
#endif

namespace omniruntime {
namespace op {
inline uint8_t *maxCharOp(uint8_t *res, int64_t &lenAndFlag, const VarcharVector *vector, const int32_t idx)
{
    uint8_t *curVal = nullptr;
    int32_t curLen = vector->GetValue(idx, &curVal);
    int32_t len = static_cast<int32_t>(lenAndFlag & valueFlag);
    auto result = memcmp(res, curVal, std::min(len, curLen));
    if (result < 0 || (result == 0 && len < curLen)) {
        lenAndFlag = curLen;
        lenAndFlag |= updateFlag;
        return curVal;
    } else {
        return res;
    }
}

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW, DataTypeId IN_ID, DataTypeId OUT_ID>
class MaxVarcharAggregator : public TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW> {
public:
    MaxVarcharAggregator(const DataTypes &inputTypes, const DataTypes &outputTypes, std::vector<int32_t> &channels)
        : TypedAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>(
            OMNI_AGGREGATION_TYPE_MAX, inputTypes, outputTypes, channels)
    {}

    ~MaxVarcharAggregator() override = default;

#ifdef ENABLE_HMPP
    void ProcessGroupWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        auto vector = vectorBatch->GetVector(this->channels[0]);

        auto offsets =
            static_cast<int32_t *>(static_cast<int32_t *>(vector->GetValueOffsets()) + vector->GetPositionOffset());
        auto width = static_cast<VarcharDataType *>(this->inputTypes.GetType(0).get())->GetWidth();
        int32_t maxLen = 0;
        uint8_t *maxVal = this->executionContext->GetArena()->Allocate(3 * width);

        LogDebug("HMPP-Agg-max");
        auto result =
            HMPPS_Max_varchar(static_cast<uint8_t *>(vector->GetValues()), offsets, vector->GetSize(), maxVal, &maxLen);
        if (result != HMPP_STS_NO_ERR) {
            throw OmniException("HMPP ERROR", "max failed for hmpp error");
        }

        if (state.val == nullptr) {
            state.val = maxVal;
            state.count = maxLen;
        } else {
            auto preMaxVal = reinterpret_cast<char *>(state.val);

            int32_t result = memcmp(
                    preMaxVal,
                    reinterpret_cast<char *>(maxVal),
                    std::min(state.count, static_cast<int64_t>(maxLen))
            );
            if (result < 0 || (result == 0 && state.count < maxLen)) {
                state.val = maxVal;
                state.count = maxLen;
            }
        }
    }

    bool CanProcessWithHMPP(AggregateState &state, VectorBatch *vectorBatch) override
    {
        // must no null inpout
        if (vectorBatch->GetVector(this->channels[0])->MayHaveNull()) {
            return false;
        }
        // not accept dictionnary vector
        if (vectorBatch->GetVector(this->channels[0])->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            return false;
        }
        return true;
    }
#endif

    void ExtractValues(const AggregateState &state, std::vector<Vector *> &vectors, int32_t rowIndex) override
    {
        int32_t offset;
        auto v = static_cast<VarcharVector *>(VectorHelper::ExpandVectorAndIndex(vectors[0], rowIndex, offset));
        if (state.val == nullptr || state.count == 0) {
            // Note: due to issue #614 we should call SetValueNull on VarcharVector vector not Vector base class
            v->SetValueNull(offset);
        } else {
            v->SetValue(offset, reinterpret_cast<uint8_t *>(state.val), state.count);
        }
    }

protected:
    ALWAYS_INLINE void ProcessRawInput(
        AggregateState &state, Vector *v, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) override
    {
        VarcharVector *vector = static_cast<VarcharVector *>(v);

        if (indexMap == nullptr) {
            if (nullMap == nullptr) {
                addChar<maxCharOp>(state, vector, rowOffset, rowCount);
            } else {
                addConditionalChar<maxCharOp>(state, vector, rowOffset, rowCount, nullMap);
            }
        } else {
            if (nullMap == nullptr) {
                addDictChar<maxCharOp>(state, vector, rowCount, indexMap);
            } else {
                addDictConditionalChar<maxCharOp>(state, vector, rowCount, nullMap, indexMap);
            }
        }

        SaveState(state);
    }

    ALWAYS_INLINE void ProcessGroupRawInput(std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *v,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) override
    {
        VarcharVector *vector = static_cast<VarcharVector *>(v);

        if (indexMap == nullptr) {
            if (nullMap == nullptr) {
                addUseRowIndexChar<maxCharOp>(rowStates, aggIdx, vector, rowOffset);
            } else {
                addConditionalUseRowIndexChar<maxCharOp>(rowStates, aggIdx, vector, rowOffset, nullMap);
            }
        } else {
            if (nullMap == nullptr) {
                addDictUseRowIndexChar<maxCharOp>(rowStates, aggIdx, vector, indexMap);
            } else {
                addDictConditionalUseRowIndexChar<maxCharOp>(rowStates, aggIdx, vector, nullMap, indexMap);
            }
        }

        for (AggregateState *states : rowStates) {
            SaveState(states[aggIdx]);
        }
    }


private:
    void SaveState(AggregateState &state)
    {
        if ((state.count & updateFlag) == 0) {
            return;
        }

        int32_t len = static_cast<int32_t>(state.count & valueFlag);
        if (state.val == nullptr || len == 0) {
            state.val = nullptr;
            state.count = 0;
            return;
        }

        uint8_t *ptr = reinterpret_cast<uint8_t *>(this->executionContext->GetArena()->Allocate(len));
        memcpy(ptr, state.val, len);
        state.val = ptr;
        state.count = len;
    }
};
}
}
#endif // OMNI_RUNTIME_MAX_VARCHAR_AGGREGATOR_H
