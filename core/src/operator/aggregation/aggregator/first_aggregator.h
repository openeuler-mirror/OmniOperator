/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * Description: Returns the first value of `child` for a group of rows.
 * Caution: The function is non-deterministic.
 * Returns the first value of `child` for a group of rows.
 * If the first value of `child` is `null`, it returns `null`.
 * Even if [[First]] is used on an already sorted column, if we do partial aggregation and final aggregation
 * its result will not be deterministic(unless the input table is sorted and has a single partition,
 * and we use a single reducer to do the aggregation.)
 *
 * If `isIgnoreNull` is true, returns only non-null values.
 */
#ifndef OMNI_RUNTIME_FIRST_AGGREGATOR_H
#define OMNI_RUNTIME_FIRST_AGGREGATOR_H

#include "aggregator.h"
#include "type/decimal_operations.h"

namespace omniruntime {
namespace op {
// First Aggregator date type information:
// input: InputType
// intermediate: InputType + bool(wrap with ContainerDataType and ContainerVector)
// final: InputType
template <bool INPUT_RAW, bool OUT_PARTIAL, bool IGNORE_NULL, typename InputType>
class FirstAggregator : public Aggregator {
public:
    using ResultType = InputType;

#pragma pack(push, 1)
    struct FirstState {
        ResultType val;
        bool valueSet = false;
        bool valIsNull = true;

        static const FirstAggregator<INPUT_RAW, OUT_PARTIAL, IGNORE_NULL, InputType>::FirstState *ConstCastState(
            const AggregateState *state)
        {
            return reinterpret_cast<
                const FirstAggregator<INPUT_RAW, OUT_PARTIAL, IGNORE_NULL, InputType>::FirstState *>(state);
        }

        static FirstAggregator<INPUT_RAW, OUT_PARTIAL, IGNORE_NULL, InputType>::FirstState *CastState(
            AggregateState *state)
        {
            return reinterpret_cast<FirstAggregator<INPUT_RAW, OUT_PARTIAL, IGNORE_NULL, InputType>::FirstState *>(
                state);
        }
    };
#pragma pack(pop)

    FirstAggregator(FunctionType aggregateType, const DataTypes &in, const DataTypes &out,
        std::vector<int32_t> &channels)
        : Aggregator(aggregateType, in, out, channels)
    {}

    FirstAggregator(FunctionType aggregateType, const DataTypes &in, const DataTypes &out,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : Aggregator(aggregateType, in, out, channels, inputRaw, outputPartial, isOverflowAsNull)
    {}

    ~FirstAggregator() override = default;

    // For varchar: update value and length in firstState
    void UpdateFirstStateVarcharVal(FirstState *firstState, const std::string_view &firstVarcharValue)
    {
        auto size = firstVarcharValue.size();
        auto ptr = arenaAllocator->Allocate(size);
        memcpy(ptr, firstVarcharValue.data(), size);
        firstState->val = std::string_view(reinterpret_cast<const char *>(ptr), size);
    }

    void UpdateFirstStateVarchar(FirstState *firstState, BaseVector *vector, int32_t rowIndex)
    {
        if constexpr (INPUT_RAW) {
            if constexpr (IGNORE_NULL) {
                firstState->valIsNull = false;
                std::string_view varcharVal;
                if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    auto varcharVector = static_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
                    varcharVal = varcharVector->GetValue(rowIndex);
                } else {
                    auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
                    varcharVal = varcharVector->GetValue(rowIndex);
                }
                return UpdateFirstStateVarcharVal(firstState, varcharVal);
            } else {
                firstState->valIsNull = vector->IsNull(rowIndex);
                if (firstState->valIsNull) {
                    return;
                }
                std::string_view varcharVal;
                if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    auto varcharVector = static_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
                    varcharVal = varcharVector->GetValue(rowIndex);
                } else {
                    auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
                    varcharVal = varcharVector->GetValue(rowIndex);
                }
                return UpdateFirstStateVarcharVal(firstState, varcharVal);
            }
        } else {
            if constexpr (IGNORE_NULL) {
                firstState->valIsNull = false;
            } else {
                firstState->valIsNull = vector->IsNull(rowIndex);
            }
            if (firstState->valIsNull) {
                return;
            } else {
                auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
                auto varcharVal = varcharVector->GetValue(rowIndex);
                return UpdateFirstStateVarcharVal(firstState, varcharVal);
            }
        }
    }

    void UpdateFirstState(FirstState *firstState, BaseVector *vector, int32_t rowIndex) const
    {
        if constexpr (INPUT_RAW) {
            if constexpr (IGNORE_NULL) {
                firstState->valIsNull = false;
                if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    firstState->val = static_cast<Vector<DictionaryContainer<InputType>> *>(vector)->GetValue(rowIndex);
                } else {
                    firstState->val = static_cast<Vector<InputType> *>(vector)->GetValue(rowIndex);
                }
            } else {
                firstState->valIsNull = vector->IsNull(rowIndex);
                if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    firstState->val = static_cast<Vector<DictionaryContainer<InputType>> *>(vector)->GetValue(rowIndex);
                } else {
                    firstState->val = static_cast<Vector<InputType> *>(vector)->GetValue(rowIndex);
                }
            }
        } else {
            firstState->valIsNull = vector->IsNull(rowIndex);
            firstState->val = static_cast<Vector<InputType> *>(vector)->GetValue(rowIndex);
        }
    }

    void ProcessGroup(AggregateState *state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        auto firstState = FirstState::CastState(state);
        if constexpr (std::is_same_v<InputType, std::string_view>) {
            if constexpr (INPUT_RAW) {
                BaseVector *vector = vectorBatch->Get(channels[0]);
                if constexpr (IGNORE_NULL) {
                    if (!firstState->valueSet && !vector->IsNull(rowIndex)) {
                        firstState->valueSet = true;
                        UpdateFirstStateVarchar(firstState, vector, rowIndex);
                    }
                } else {
                    if (!firstState->valueSet) {
                        firstState->valueSet = true;
                        UpdateFirstStateVarchar(firstState, vector, rowIndex);
                    }
                }
            } else {
                BaseVector *firstVector = vectorBatch->Get(channels[0]);
                BaseVector *valueSetVector = vectorBatch->Get(channels[1]);

                if (!firstState->valueSet) {
                    UpdateFirstStateVarchar(firstState, firstVector, rowIndex);
                }
                bool intermediateState = reinterpret_cast<Vector<bool> *>(valueSetVector)->GetValue(rowIndex);
                firstState->valueSet = firstState->valueSet || intermediateState;
            }
        } else {
            if constexpr (INPUT_RAW) {
                BaseVector *vector = vectorBatch->Get(channels[0]);
                if constexpr (IGNORE_NULL) {
                    if (!firstState->valueSet && !vector->IsNull(rowIndex)) {
                        firstState->valueSet = true;
                        UpdateFirstState(firstState, vector, rowIndex);
                    }
                } else {
                    if (!firstState->valueSet) {
                        firstState->valueSet = true;
                        UpdateFirstState(firstState, vector, rowIndex);
                    }
                }
            } else {
                BaseVector *firstVector = vectorBatch->Get(channels[0]);
                BaseVector *valueSetVector = vectorBatch->Get(channels[1]);

                if (!firstState->valueSet) {
                    UpdateFirstState(firstState, firstVector, rowIndex);
                }
                bool intermediateState = reinterpret_cast<Vector<bool> *>(valueSetVector)->GetValue(rowIndex);
                firstState->valueSet = firstState->valueSet || intermediateState;
            }
        }
    }

    void ProcessGroupUnspill(std::vector<UnspillRowInfo> &unspillRows, int32_t rowCount, int32_t &vectorIndex) override
    {
        auto firstVecIdx = vectorIndex++;
        if constexpr (OUT_PARTIAL) {
            vectorIndex++;
        }
        for (int32_t rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            auto &row = unspillRows[rowIdx];
            auto batch = row.batch;
            auto index = row.rowIdx;
            auto *state = row.state + aggStateOffset;

            auto firstVector = batch->Get(firstVecIdx);
            auto firstState = FirstState::CastState(state);
            if constexpr (std::is_same_v<InputType, std::string_view>) {
                if constexpr (IGNORE_NULL) {
                    if (!firstState->valueSet && !firstVector->IsNull(index)) {
                        firstState->valueSet = true;
                        firstState->valIsNull = false;
                        auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(firstVector);
                        auto varcharVal = varcharVector->GetValue(index);
                        UpdateFirstStateVarcharVal(firstState, varcharVal);
                    }
                } else {
                    if (!firstState->valueSet) {
                        firstState->valIsNull = firstVector->IsNull(index);
                        firstState->valueSet = true;
                        if (not firstState->valIsNull) {
                            auto varcharVector =
                                static_cast<Vector<LargeStringContainer<std::string_view>> *>(firstVector);
                            auto varcharVal = varcharVector->GetValue(index);
                            UpdateFirstStateVarcharVal(firstState, varcharVal);
                        }
                    }
                }
            } else {
                if constexpr (IGNORE_NULL) {
                    if (!firstState->valueSet && !firstVector->IsNull(index)) {
                        firstState->valueSet = true;
                        firstState->valIsNull = false;
                        firstState->val = static_cast<Vector<InputType> *>(firstVector)->GetValue(index);
                    }
                } else {
                    if (!firstState->valueSet) {
                        firstState->valueSet = true;
                        firstState->valIsNull = firstVector->IsNull(index);
                        if (not firstState->valIsNull) {
                            firstState->val = static_cast<Vector<InputType> *>(firstVector)->GetValue(index);
                        }
                    }
                }
            }
            if constexpr (OUT_PARTIAL) {
                auto valueSetVector = batch->Get(firstVecIdx + 1);
                bool intermediateState = reinterpret_cast<Vector<bool> *>(valueSetVector)->GetValue(index);
                firstState->valueSet = firstState->valueSet || intermediateState;
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
            auto firstVarcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
            for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto *firstState = FirstState::CastState(groupStates[rowIndex] + aggStateOffset);
                if (firstState->valIsNull) {
                    firstVarcharVector->SetNull(rowIndex);
                } else {
                    firstVarcharVector->SetValue(rowIndex, firstState->val);
                }

                if constexpr (OUT_PARTIAL) {
                    auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
                    valueSetVector->SetValue(rowIndex, firstState->valueSet);
                }
            }
        } else {
            auto firstVector = reinterpret_cast<Vector<InputType> *>(vectors[0]);
            for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto *firstState = FirstState::CastState(groupStates[rowIndex] + aggStateOffset);
                if (firstState->valIsNull) {
                    firstVector->SetNull(rowIndex);
                } else {
                    firstVector->SetValue(rowIndex, firstState->val);
                }

                if constexpr (OUT_PARTIAL) {
                    auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
                    valueSetVector->SetValue(rowIndex, firstState->valueSet);
                }
            }
        }
    }

    void ExtractValues(const AggregateState *state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        if constexpr (std::is_same_v<InputType, std::string_view>) {
            auto firstVarcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
            const auto *firstState = FirstState::ConstCastState(state + aggStateOffset);
            if constexpr (OUT_PARTIAL) {
                if (firstState->valIsNull) {
                    firstVarcharVector->SetNull(rowIndex);
                } else {
                    firstVarcharVector->SetNotNull(rowIndex);
                    firstVarcharVector->SetValue(rowIndex, const_cast<ResultType &>(firstState->val));
                }

                auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
                valueSetVector->SetValue(rowIndex, firstState->valueSet);
            } else {
                if (firstState->valIsNull) {
                    firstVarcharVector->SetNull(rowIndex);
                } else {
                    firstVarcharVector->SetNotNull(rowIndex);
                    firstVarcharVector->SetValue(rowIndex, const_cast<ResultType &>(firstState->val));
                }
            }
        } else {
            auto firstVector = reinterpret_cast<Vector<InputType> *>(vectors[0]);
            const auto *firstState = FirstState::ConstCastState(state + aggStateOffset);
            if constexpr (OUT_PARTIAL) {
                if (firstState->valIsNull) {
                    firstVector->SetNull(rowIndex);
                } else {
                    firstVector->SetNotNull(rowIndex);
                    firstVector->SetValue(rowIndex, const_cast<ResultType &>(firstState->val));
                }

                auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
                valueSetVector->SetValue(rowIndex, firstState->valueSet);
            } else {
                if (firstState->valIsNull) {
                    firstVector->SetNull(rowIndex);
                } else {
                    firstVector->SetNotNull(rowIndex);
                    firstVector->SetValue(rowIndex, const_cast<ResultType &>(firstState->val));
                }
            }
        }
    }

    void ExtractValuesBatch(std::vector<AggregateState *> &groupStates, std::vector<BaseVector *> &vectors,
        int32_t rowOffset, int32_t rowCount) override
    {
        if constexpr (std::is_same_v<InputType, std::string_view>) {
            auto firstVarcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
            auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
            for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto *firstState = FirstState::CastState(groupStates[rowIndex] + aggStateOffset);
                if constexpr (OUT_PARTIAL) {
                    if (firstState->valIsNull) {
                        firstVarcharVector->SetNull(rowIndex);
                    } else {
                        firstVarcharVector->SetNotNull(rowIndex);
                        ;
                        firstVarcharVector->SetValue(rowIndex, firstState->val);
                    }

                    valueSetVector->SetValue(rowIndex, firstState->valueSet);
                } else {
                    if (firstState->valIsNull) {
                        firstVarcharVector->SetNull(rowIndex);
                    } else {
                        firstVarcharVector->SetNotNull(rowIndex);
                        firstVarcharVector->SetValue(rowIndex, firstState->val);
                    }
                }
            }
        } else {
            auto firstVector = reinterpret_cast<Vector<InputType> *>(vectors[0]);
            auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
            for (int32_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                auto firstState = FirstState::CastState(groupStates[rowIndex] + aggStateOffset);
                if constexpr (OUT_PARTIAL) {
                    if (firstState->valIsNull) {
                        firstVector->SetNull(rowIndex);
                    } else {
                        firstVector->SetNotNull(rowIndex);
                        firstVector->SetValue(rowIndex, firstState->val);
                    }

                    valueSetVector->SetValue(rowIndex, firstState->valueSet);
                } else {
                    if (firstState->valIsNull) {
                        firstVector->SetNull(rowIndex);
                    } else {
                        firstVector->SetNotNull(rowIndex);
                        firstVector->SetValue(rowIndex, firstState->val);
                    }
                }
            }
        }
    }

    void InitState(AggregateState *state) override
    {
        auto *firstState = FirstState::CastState(state + aggStateOffset);
        firstState->val = ResultType{};
        firstState->valueSet = false;
        firstState->valIsNull = true;
    }

    void InitStates(std::vector<AggregateState *> &groupStates) override
    {
        for (auto *groupState : groupStates) {
            InitState(groupState);
        }
    }

    size_t GetStateSize() override
    {
        return sizeof(FirstState);
    }

    void AlignAggSchemaWithFilter(VectorBatch *result, VectorBatch *inputVecBatch, const int32_t filterIndex) override
    {
        std::string message = "the interface don't support first aggregator";
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", message);
    }

    void AlignAggSchema(VectorBatch *result, VectorBatch *inputVecBatch) override
    {
        std::string message = "the interface don't support first aggregator";
        throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", message);
    }
};
}
}
#endif // OMNI_RUNTIME_FIRST_AGGREGATOR_H
