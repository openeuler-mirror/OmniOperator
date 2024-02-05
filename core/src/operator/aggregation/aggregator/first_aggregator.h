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
static constexpr int32_t PARTIAL_FIRST_OUTPUT_LENGTH = sizeof(FirstState);

// First Aggregator date type information:
// input: InputType
// intermediate: InputType + bool(wrap with ContainerDataType and ContainerVector)
// final: InputType
template <bool INPUT_RAW, bool OUT_PARTIAL, bool IGNORE_NULL, typename InputType>
class FirstAggregator : public Aggregator {
public:
    FirstAggregator(FunctionType aggregateType, const DataTypes &in, const DataTypes &out,
        std::vector<int32_t> &channels)
        : Aggregator(aggregateType, in, out, channels)
    {}

    FirstAggregator(FunctionType aggregateType, const DataTypes &in, const DataTypes &out,
        std::vector<int32_t> &channels, bool inputRaw, bool outputPartial, bool isOverflowAsNull)
        : Aggregator(aggregateType, in, out, channels, inputRaw, outputPartial, isOverflowAsNull)
    {}

    ~FirstAggregator() override = default;

    void InitiateGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        if (state.val == nullptr) {
            InitState(state);
        }
        ProcessGroup(state, vectorBatch, rowIndex);
    }

    int64_t UpdateFirstStateVarcharVal(FirstState *firstState, int64_t length, std::string_view &firstVarcharValue)
    {
        auto firstValueLength = static_cast<int64_t>(firstVarcharValue.size());
        if (firstValueLength > length) {
            auto ptr = executionContext->GetArena()->Allocate(firstValueLength);
            memcpy_s(ptr, firstValueLength, firstVarcharValue.data(), firstValueLength);
            firstState->val = ptr;
        } else {
            auto ptr = firstState->val;
            memcpy_s(ptr, firstValueLength, firstVarcharValue.data(), firstValueLength);
        }
        return firstValueLength;
    }

    int64_t UpdateFirstStateVarchar(FirstState *firstState, int64_t length, BaseVector *vector, int32_t rowIndex)
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
                return UpdateFirstStateVarcharVal(firstState, length, varcharVal);
            } else {
                firstState->valIsNull = vector->IsNull(rowIndex);
                if (firstState->valIsNull) {
                    return 0;
                }
                std::string_view varcharVal;
                if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    auto varcharVector = static_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
                    varcharVal = varcharVector->GetValue(rowIndex);
                } else {
                    auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
                    varcharVal = varcharVector->GetValue(rowIndex);
                }
                return UpdateFirstStateVarcharVal(firstState, length, varcharVal);
            }
        } else {
            if constexpr (IGNORE_NULL) {
                firstState->valIsNull = false;
            } else {
                firstState->valIsNull = vector->IsNull(rowIndex);
            }
            if (firstState->valIsNull) {
                return 0;
            } else {
                auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
                auto varcharVal = varcharVector->GetValue(rowIndex);
                return UpdateFirstStateVarcharVal(firstState, length, varcharVal);
            }
        }
    }

    void UpdateFirstState(FirstState *firstState, BaseVector *vector, int32_t rowIndex) const
    {
        if constexpr (INPUT_RAW) {
            if constexpr (IGNORE_NULL) {
                firstState->valIsNull = false;
                if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    *reinterpret_cast<InputType *>(firstState->val) =
                        static_cast<Vector<DictionaryContainer<InputType>> *>(vector)->GetValue(rowIndex);
                } else {
                    *reinterpret_cast<InputType *>(firstState->val) =
                        static_cast<Vector<InputType> *>(vector)->GetValue(rowIndex);
                }
            } else {
                firstState->valIsNull = vector->IsNull(rowIndex);
                if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    *reinterpret_cast<InputType *>(firstState->val) =
                        static_cast<Vector<DictionaryContainer<InputType>> *>(vector)->GetValue(rowIndex);
                } else {
                    *reinterpret_cast<InputType *>(firstState->val) =
                        static_cast<Vector<InputType> *>(vector)->GetValue(rowIndex);
                }
            }
        } else {
            firstState->valIsNull = vector->IsNull(rowIndex);
            *reinterpret_cast<InputType *>(firstState->val) =
                static_cast<Vector<InputType> *>(vector)->GetValue(rowIndex);
        }
    }

    void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch, int32_t rowIndex) override
    {
        auto firstState = static_cast<FirstState *>(state.val);
        if constexpr (std::is_same_v<InputType, std::string_view>) {
            if constexpr (INPUT_RAW) {
                BaseVector *vector = vectorBatch->Get(channels[0]);
                if constexpr (IGNORE_NULL) {
                    if (!firstState->valueSet && !vector->IsNull(rowIndex)) {
                        firstState->valueSet = true;
                        state.count = UpdateFirstStateVarchar(firstState, state.count, vector, rowIndex);
                    }
                } else {
                    if (!firstState->valueSet) {
                        firstState->valueSet = true;
                        state.count = UpdateFirstStateVarchar(firstState, state.count, vector, rowIndex);
                    }
                }
            } else {
                BaseVector *firstVector = vectorBatch->Get(channels[0]);
                BaseVector *valueSetVector = vectorBatch->Get(channels[1]);

                if (!firstState->valueSet) {
                    state.count = UpdateFirstStateVarchar(firstState, state.count, firstVector, rowIndex);
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

    void ProcessGroupAfterSpill(AggregateState &state, VectorBatch *vectorBatch, int32_t &vectorIndex,
        int32_t rowIdx) override
    {
        auto firstVector = vectorBatch->Get(vectorIndex++);
        auto valueSetVector = vectorBatch->Get(vectorIndex++);
        auto firstState = static_cast<FirstState *>(state.val);

        if constexpr (std::is_same_v<InputType, std::string_view>) {
            if constexpr (IGNORE_NULL) {
                if (!firstState->valueSet && !firstVector->IsNull(rowIdx)) {
                    firstState->valueSet = true;
                    firstState->valIsNull = false;
                    auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(firstVector);
                    auto varcharVal = varcharVector->GetValue(rowIdx);
                    state.count = UpdateFirstStateVarcharVal(firstState, state.count, varcharVal);
                }
            } else {
                if (!firstState->valueSet) {
                    firstState->valIsNull = firstVector->IsNull(rowIdx);
                    firstState->valueSet = true;
                    if (firstState->valIsNull) {
                        state.count = 0;
                    } else {
                        auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(firstVector);
                        auto varcharVal = varcharVector->GetValue(rowIdx);
                        state.count = UpdateFirstStateVarcharVal(firstState, state.count, varcharVal);
                    }
                }
            }
        } else {
            if constexpr (IGNORE_NULL) {
                if (!firstState->valueSet && !firstVector->IsNull(rowIdx)) {
                    firstState->valueSet = true;
                    firstState->valIsNull = false;
                    *reinterpret_cast<InputType *>(firstState->val) =
                        static_cast<Vector<InputType> *>(firstVector)->GetValue(rowIdx);
                }
            } else {
                if (!firstState->valueSet) {
                    firstState->valueSet = true;
                    firstState->valIsNull = firstVector->IsNull(rowIdx);
                    if (not firstState->valIsNull) {
                        *reinterpret_cast<InputType *>(firstState->val) =
                            static_cast<Vector<InputType> *>(firstVector)->GetValue(rowIdx);
                    }
                }
            }
        }
        bool intermediateState = reinterpret_cast<Vector<bool> *>(valueSetVector)->GetValue(rowIdx);
        firstState->valueSet = firstState->valueSet || intermediateState;
    }

    void GetSpillType(std::vector<DataTypeId> &spillTypes) override
    {
        spillTypes.push_back(static_cast<DataTypeId>(this->inputTypes.GetIds()[0]));
        spillTypes.push_back(OMNI_BOOLEAN);
    }

    void ExtractSpillValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        if constexpr (std::is_same_v<InputType, std::string_view>) {
            auto firstVarcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
            auto firstState = static_cast<FirstState *>(state.val);
            if (firstState->valIsNull) {
                firstVarcharVector->SetNull(rowIndex);
            } else {
                std::string_view firstValue(reinterpret_cast<char *>(firstState->val), state.count);
                firstVarcharVector->SetValue(rowIndex, firstValue);
            }

            auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
            valueSetVector->SetValue(rowIndex, firstState->valueSet);
        } else {
            auto firstVector = reinterpret_cast<Vector<InputType> *>(vectors[0]);
            auto firstState = static_cast<FirstState *>(state.val);
            if (firstState->valIsNull) {
                firstVector->SetNull(rowIndex);
            } else {
                firstVector->SetValue(rowIndex, *static_cast<InputType *>(firstState->val));
            }
            auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
            valueSetVector->SetValue(rowIndex, firstState->valueSet);
        }
    }

    void ExtractValues(const AggregateState &state, std::vector<BaseVector *> &vectors, int32_t rowIndex) override
    {
        if constexpr (std::is_same_v<InputType, std::string_view>) {
            auto firstVarcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vectors[0]);
            auto firstState = static_cast<FirstState *>(state.val);
            if constexpr (OUT_PARTIAL) {
                if (firstState->valIsNull) {
                    firstVarcharVector->SetNull(rowIndex);
                } else {
                    firstVarcharVector->SetNotNull(rowIndex);
                    std::string_view firstValue(reinterpret_cast<char *>(firstState->val), state.count);
                    firstVarcharVector->SetValue(rowIndex, firstValue);
                }

                auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
                valueSetVector->SetValue(rowIndex, firstState->valueSet);
            } else {
                if (firstState->valIsNull) {
                    firstVarcharVector->SetNull(rowIndex);
                } else {
                    firstVarcharVector->SetNotNull(rowIndex);
                    std::string_view firstValue(reinterpret_cast<char *>(firstState->val), state.count);
                    firstVarcharVector->SetValue(rowIndex, firstValue);
                }
            }
        } else {
            auto firstVector = reinterpret_cast<Vector<InputType> *>(vectors[0]);
            auto firstState = static_cast<FirstState *>(state.val);
            if constexpr (OUT_PARTIAL) {
                if (firstState->valIsNull) {
                    firstVector->SetNull(rowIndex);
                } else {
                    firstVector->SetNotNull(rowIndex);
                    firstVector->SetValue(rowIndex, *static_cast<InputType *>(firstState->val));
                }

                auto valueSetVector = reinterpret_cast<Vector<bool> *>(vectors[1]);
                valueSetVector->SetValue(rowIndex, firstState->valueSet);
            } else {
                if (firstState->valIsNull) {
                    firstVector->SetNull(rowIndex);
                } else {
                    firstVector->SetNotNull(rowIndex);
                    firstVector->SetValue(rowIndex, *static_cast<InputType *>(firstState->val));
                }
            }
        }
    }

    void InitState(AggregateState &state) override
    {
        state.val = executionContext->GetArena()->Allocate(PARTIAL_FIRST_OUTPUT_LENGTH);
        state.count = 0;
        auto firstState = static_cast<FirstState *>(state.val);
        if constexpr (std::is_same_v<InputType, std::string_view>) {
            // allocate 1 byte for varchar default
            firstState->val = executionContext->GetArena()->Allocate(1);
        } else {
            firstState->val = executionContext->GetArena()->Allocate(sizeof(InputType));
        }
        firstState->valueSet = false;
        firstState->valIsNull = true;
    }
};
}
}
#endif // OMNI_RUNTIME_FIRST_AGGREGATOR_H
