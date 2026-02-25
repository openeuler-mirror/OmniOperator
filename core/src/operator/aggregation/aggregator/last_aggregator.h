/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Returns the last value of `child` for a group of rows.
 * Caution: The function is non-deterministic.
 * Returns the last value of `child` for a group of rows.
 * If the last value of `child` is `null`, it returns `null`.
 * Even if [[Last]] is used on an already sorted column, if we do partial aggregation and final aggregation
 * its result will not be deterministic(unless the input table is sorted and has a single partition,
 * and we use a single reducer to do the aggregation.)
 *
 * If `isIgnoreNull` is true, returns only non-null values.
 */
#ifndef OMNI_RUNTIME_LAST_AGGREGATOR_H
#define OMNI_RUNTIME_LAST_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"

namespace omniruntime {
namespace op {
// Last Aggregator date type information:
// input: InputType
// intermediate: InputType + bool(wrap with ContainerDataType and ContainerVector)
// final: InputType
template <bool INPUT_RAW, bool OUT_PARTIAL, bool IGNORE_NULL, typename InputType>
class LastAggregator : public Aggregator {
public:
    using ResultType = InputType;

#pragma pack(push, 1)
    struct LastState {
        ResultType val;
        bool valueSet = false;
        bool valIsNull = true;

        static const LastAggregator<INPUT_RAW, OUT_PARTIAL, IGNORE_NULL, InputType>::LastState *ConstCastState(
            const AggregateState *state)
        {
            return reinterpret_cast<
                const LastAggregator<INPUT_RAW, OUT_PARTIAL, IGNORE_NULL, InputType>::LastState *>(state);
        }

        static LastAggregator<INPUT_RAW, OUT_PARTIAL, IGNORE_NULL, InputType>::LastState *CastState(
            AggregateState *state)
        {
            return reinterpret_cast<LastAggregator<INPUT_RAW, OUT_PARTIAL, IGNORE_NULL, InputType>::LastState *>(
                state);
        }
    };
#pragma pack(pop)

    LastAggregator(FunctionType aggregateType, const DataTypes &in, const DataTypes &out,
        std::vector<int32_t> &channels)
        : Aggregator(aggregateType, in, out, channels)
    {}

    LastAggregator(FunctionType aggregateType, const DataTypes &in, const DataTypes &out,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : Aggregator(aggregateType, in, out, channels, inputRaw, outputPartial, isOverflowAsNull)
    {}

    ~LastAggregator() override = default;

    // For varchar: update value and length in lastState
    void UpdateLastStateVarcharVal(LastState *lastState, const std::string_view &lastVarcharValue)
    {
        auto size = lastVarcharValue.size();
        auto ptr = arenaAllocator->Allocate(size);
        memcpy_s(ptr, size, lastVarcharValue.data(), size);
        lastState->val = std::string_view(reinterpret_cast<const char *>(ptr), size);
    }

    void UpdateLastStateVarchar(LastState *lastState, BaseVector *vector, int32_t rowIndex)
    {
        if constexpr (INPUT_RAW) {
            if constexpr (IGNORE_NULL) {
                lastState->valIsNull = false;
                std::string_view varcharVal;
                if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
                    varcharVal = static_cast<vec::ConstVector<std::string_view> *>(vector)->GetConstValue();
                } else if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    auto varcharVector = static_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
                    varcharVal = varcharVector->GetValue(rowIndex);
                } else {
                    auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
                    varcharVal = varcharVector->GetValue(rowIndex);
                }
                return UpdateLastStateVarcharVal(lastState, varcharVal);
            } else {
                lastState->valIsNull = vector->IsNull(rowIndex);
                if (lastState->valIsNull) {
                    return;
                }
                std::string_view varcharVal;
                if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
                    varcharVal = static_cast<vec::ConstVector<std::string_view> *>(vector)->GetConstValue();
                } else if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    auto varcharVector = static_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
                    varcharVal = varcharVector->GetValue(rowIndex);
                } else {
                    auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
                    varcharVal = varcharVector->GetValue(rowIndex);
                }
                return UpdateLastStateVarcharVal(lastState, varcharVal);
            }
        } else {
            if constexpr (IGNORE_NULL) {
                lastState->valIsNull = false;
            } else {
                lastState->valIsNull = vector->IsNull(rowIndex);
            }
            if (lastState->valIsNull) {
                return;
            } else {
                if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
                    auto varcharVal = static_cast<vec::ConstVector<std::string_view> *>(vector)->GetConstValue();
                    return UpdateLastStateVarcharVal(lastState, varcharVal);
                } else {
                    auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
                    auto varcharVal = varcharVector->GetValue(rowIndex);
                    return UpdateLastStateVarcharVal(lastState, varcharVal);
                }
            }
        }
    }

    void UpdateLastState(LastState *lastState, BaseVector *vector, int32_t rowIndex) const
    {
        if constexpr (INPUT_RAW) {
            if constexpr (IGNORE_NULL) {
                lastState->valIsNull = false;
                if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
                    lastState->val = static_cast<vec::ConstVector<InputType> *>(vector)->GetConstValue();
                } else if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    lastState->val = static_cast<Vector<DictionaryContainer<InputType>> *>(vector)->GetValue(rowIndex);
                } else {
                    lastState->val = static_cast<Vector<InputType> *>(vector)->GetValue(rowIndex);
                }
            } else {
                lastState->valIsNull = vector->IsNull(rowIndex);
                if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
                    lastState->val = static_cast<vec::ConstVector<InputType> *>(vector)->GetConstValue();
                } else if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    lastState->val = static_cast<Vector<DictionaryContainer<InputType>> *>(vector)->GetValue(rowIndex);
                } else {
                    lastState->val = static_cast<Vector<InputType> *>(vector)->GetValue(rowIndex);
                }
            }
        } else {
            lastState->valIsNull = vector->IsNull(rowIndex);
            if (vector->GetEncoding() == OMNI_ENCODING_CONST) {
                lastState->val = static_cast<vec::ConstVector<InputType> *>(vector)->GetConstValue();
            } else {
                lastState->val = static_cast<Vector<InputType> *>(vector)->GetValue(rowIndex);
            }
        }
    }

    void ProcessGroup(AggregateState *state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        auto lastState = LastState::CastState(state);
        if constexpr (std::is_same_v<InputType, std::string_view>) {
            if constexpr (INPUT_RAW) {
                BaseVector *vector = vectorBatch->Get(channels[0]);
                if constexpr (IGNORE_NULL) {
                    if (!vector->IsNull(rowIndex)) {
                        lastState->valueSet = true;
                        UpdateLastStateVarchar(lastState, vector, rowIndex);
                    }
                } else {
                    lastState->valueSet = true;
                    UpdateLastStateVarchar(lastState, vector, rowIndex);
                }
            } else {
                BaseVector *lastVector = vectorBatch->Get(channels[0]);
                BaseVector *valueSetVector = vectorBatch->Get(channels[1]);

                bool intermediateState = reinterpret_cast<Vector<bool> *>(valueSetVector)->GetValue(rowIndex);
                if (intermediateState) {
                    UpdateLastStateVarchar(lastState, lastVector, rowIndex);
                }
                lastState->valueSet = lastState->valueSet || intermediateState;
            }
        } else {
            if constexpr (INPUT_RAW) {
                BaseVector *vector = vectorBatch->Get(channels[0]);
                if constexpr (IGNORE_NULL) {
                    if (!vector->IsNull(rowIndex)) {
                        lastState->valueSet = true;
                        UpdateLastState(lastState, vector, rowIndex);
                    }
                } else {
                    lastState->valueSet = true;
                    UpdateLastState(lastState, vector, rowIndex);
                }
            } else {
                BaseVector *lastVector = vectorBatch->Get(channels[0]);
                BaseVector *valueSetVector = vectorBatch->Get(channels[1]);

                bool intermediateState = reinterpret_cast<Vector<bool> *>(valueSetVector)->GetValue(rowIndex);
                if (intermediateState) {
                    UpdateLastState(lastState, lastVector, rowIndex);
                }
                lastState->valueSet = lastState->valueSet || intermediateState;
            }
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override
    {
        auto lastVecIdx = vectorIndex++;
        if constexpr (OUT_PARTIAL) {
            vectorIndex++;
        }
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            auto &row = unspillRows[rowIdx];
            auto batch = row.batch;
            auto index = row.rowIdx;
            auto *state = row.state + aggStateOffset;

            auto lastVector = batch->Get(lastVecIdx);
            auto lastState = LastState::CastState(state);
            if constexpr (std::is_same_v<InputType, std::string_view>) {
                if constexpr (IGNORE_NULL) {
                    if (!lastVector->IsNull(index)) {
                        lastState->valueSet = true;
                        lastState->valIsNull = false;
                        auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(lastVector);
                        auto varcharVal = varcharVector->GetValue(index);
                        UpdateLastStateVarcharVal(lastState, varcharVal);
                    }
                } else {
                    lastState->valIsNull = lastVector->IsNull(index);
                    lastState->valueSet = true;
                    if (not lastState->valIsNull) {
                        auto varcharVector =
                            static_cast<Vector<LargeStringContainer<std::string_view>> *>(lastVector);
                        auto varcharVal = varcharVector->GetValue(index);
                        UpdateLastStateVarcharVal(lastState, varcharVal);
                    }
                }
            } else {
                if constexpr (IGNORE_NULL) {
                    if (!lastVector->IsNull(index)) {
                        lastState->valueSet = true;
                        lastState->valIsNull = false;
                        lastState->val = static_cast<Vector<InputType> *>(lastVector)->GetValue(index);
                    }
                } else {
                    lastState->valueSet = true;
                    lastState->valIsNull = lastVector->IsNull(index);
                    if (not lastState->valIsNull) {
                        lastState->val = static_cast<Vector<InputType> *>(lastVector)->GetValue(index);
                    }
                }
            }
            if constexpr (OUT_PARTIAL) {
                auto valueSetVector = batch->Get(lastVecIdx + 1);
                bool intermediateState = reinterpret_cast<Vector<bool> *>(valueSetVector)->GetValue(index);
                lastState->valueSet = lastState->valueSet || intermediateState;
            }
        }
    }

    std::vector<DataTypePtr> GetSpillType() override
    {
        std::vector<DataTypePtr> spillTypes;
        spillTypes.emplace_back(this->inputTypes.GetType(0));
        if constexpr (OUT_PARTIAL) {
            spillTypes.emplace_back(std::make_shared<DataType>(OMNI_BOOLEAN));
        }
        return spillTypes;
    }

    void ExtractValuesForSpill(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors) override
    {
        auto rowCount = static_cast<int32_t>(groupStates.size());
        if constexpr (std::is_same_v<InputType, std::string_view>) {
            auto lastVarcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
            for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto *lastState = LastState::CastState(groupStates[rowIndex] + aggStateOffset);
                if (lastState->valIsNull) {
                    lastVarcharVector->SetNull(rowIndex);
                } else {
                    lastVarcharVector->SetValue(rowIndex, lastState->val);
                }

                if constexpr (OUT_PARTIAL) {
                    auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
                    valueSetVector->SetValue(rowIndex, lastState->valueSet);
                }
            }
        } else {
            auto lastVector = reinterpret_cast<Vector<InputType> *>(vectors[0]);
            for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto *lastState = LastState::CastState(groupStates[rowIndex] + aggStateOffset);
                if (lastState->valIsNull) {
                    lastVector->SetNull(rowIndex);
                } else {
                    lastVector->SetValue(rowIndex, lastState->val);
                }

                if constexpr (OUT_PARTIAL) {
                    auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
                    valueSetVector->SetValue(rowIndex, lastState->valueSet);
                }
            }
        }
    }

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        if constexpr (std::is_same_v<InputType, std::string_view>) {
            auto lastVarcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
            const auto *lastState = LastState::ConstCastState(state + aggStateOffset);
            if constexpr (OUT_PARTIAL) {
                if (lastState->valIsNull) {
                    lastVarcharVector->SetNull(rowIndex);
                } else {
                    lastVarcharVector->SetNotNull(rowIndex);
                    lastVarcharVector->SetValue(rowIndex, const_cast<ResultType &>(lastState->val));
                }

                auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
                valueSetVector->SetValue(rowIndex, lastState->valueSet);
            } else {
                if (lastState->valIsNull) {
                    lastVarcharVector->SetNull(rowIndex);
                } else {
                    lastVarcharVector->SetNotNull(rowIndex);
                    lastVarcharVector->SetValue(rowIndex, const_cast<ResultType &>(lastState->val));
                }
            }
        } else {
            auto lastVector = reinterpret_cast<Vector<InputType> *>(vectors[0]);
            const auto *lastState = LastState::ConstCastState(state + aggStateOffset);
            if constexpr (OUT_PARTIAL) {
                if (lastState->valIsNull) {
                    lastVector->SetNull(rowIndex);
                } else {
                    lastVector->SetNotNull(rowIndex);
                    lastVector->SetValue(rowIndex, const_cast<ResultType &>(lastState->val));
                }

                auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
                valueSetVector->SetValue(rowIndex, lastState->valueSet);
            } else {
                if (lastState->valIsNull) {
                    lastVector->SetNull(rowIndex);
                } else {
                    lastVector->SetNotNull(rowIndex);
                    lastVector->SetValue(rowIndex, const_cast<ResultType &>(lastState->val));
                }
            }
        }
    }

    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override
    {
        if constexpr (std::is_same_v<InputType, std::string_view>) {
            auto lastVarcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
            auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
            for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto *lastState = LastState::CastState(groupStates[rowIndex] + aggStateOffset);
                if constexpr (OUT_PARTIAL) {
                    if (lastState->valIsNull) {
                        lastVarcharVector->SetNull(rowIndex);
                    } else {
                        lastVarcharVector->SetNotNull(rowIndex);
                        lastVarcharVector->SetValue(rowIndex, lastState->val);
                    }

                    valueSetVector->SetValue(rowIndex, lastState->valueSet);
                } else {
                    if (lastState->valIsNull) {
                        lastVarcharVector->SetNull(rowIndex);
                    } else {
                        lastVarcharVector->SetNotNull(rowIndex);
                        lastVarcharVector->SetValue(rowIndex, lastState->val);
                    }
                }
            }
        } else {
            auto lastVector = reinterpret_cast<Vector<InputType> *>(vectors[0]);
            auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
            for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto lastState = LastState::CastState(groupStates[rowIndex] + aggStateOffset);
                if constexpr (OUT_PARTIAL) {
                    if (lastState->valIsNull) {
                        lastVector->SetNull(rowIndex);
                    } else {
                        lastVector->SetNotNull(rowIndex);
                        lastVector->SetValue(rowIndex, lastState->val);
                    }

                    valueSetVector->SetValue(rowIndex, lastState->valueSet);
                } else {
                    if (lastState->valIsNull) {
                        lastVector->SetNull(rowIndex);
                    } else {
                        lastVector->SetNotNull(rowIndex);
                        lastVector->SetValue(rowIndex, lastState->val);
                    }
                }
            }
        }
    }

    void InitState(AggregateState *state) override
    {
        auto *lastState = LastState::CastState(state + aggStateOffset);
        lastState->val = ResultType{};
        lastState->valueSet = false;
        lastState->valIsNull = true;
    }

    void InitStates(std::vector<AggregateState *> &groupStates) override
    {
        for (auto *groupState : groupStates) {
            InitState(groupState);
        }
    }

    size_t GetStateSize() override
    {
        return sizeof(LastState);
    }

    void AlignAggSchemaWithFilter(VectorBatch *result, VectorBatch *inputVecBatch, const int32_t filterIndex) override
    {
        std::string message = "the interface don't support last aggregator";
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", message);
    }

    void AlignAggSchema(VectorBatch *result, VectorBatch *inputVecBatch) override
    {
        std::string message = "the interface don't support last aggregator";
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", message);
    }
};
}
}
#endif // OMNI_RUNTIME_LAST_AGGREGATOR_H
