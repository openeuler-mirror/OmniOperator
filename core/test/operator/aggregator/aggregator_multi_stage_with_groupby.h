/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2023. All rights reserved.
 */
#pragma once
#include "aggregator_multi_stage_no_groupby.h"
#include "operator/aggregation/group_aggregation.h"

namespace omniruntime {
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::type;

inline Vector *CreateIntVectorGroupby(VectorAllocator *vectorAllocator, const int32_t nRows, const int32_t nullPercent,
    const int32_t range, const bool isDict)
{
    using V = typename NativeAndVectorType<OMNI_INT>::vector;
    using T = typename NativeAndVectorType<OMNI_INT>::type;
    double v;

    if (isDict) {
        int32_t valueStartIdx = nullPercent > 0 ? 1 : 0;
        V *vector = new V(vectorAllocator, 256 + valueStartIdx);
        if (nullPercent > 0) {
            vector->SetValueNull(0);
        }

        std::set<double> dictValues;
        dictValues.insert(0);
        vector->SetValue(valueStartIdx, static_cast<T>(0));
        for (int32_t i = valueStartIdx + 1; i < vector->GetSize(); ++i) {
            do {
                v = (rand() % range) - ((4 * range) / 10) + ((rand() % 100) / 100.0);
            } while (!dictValues.insert(v).second);

            vector->SetValue(i, static_cast<T>(v));
        }

        int32_t ids[nRows];
        for (int32_t i = 0; i < nRows; ++i) {
            if ((rand() % 100) < nullPercent) {
                ids[i] = 0;
            } else {
                ids[i] = (rand() % 256) + valueStartIdx;
            }
        }
        auto dictVector = new DictionaryVector(vector, ids, nRows);
        delete vector;
        return dictVector;
    } else {
        V *vector = new V(vectorAllocator, nRows);
        for (int32_t i = 0; i < nRows; ++i) {
            if ((rand() % 100) < nullPercent) {
                vector->SetValueNull(i);
            } else {
                if ((rand() % 100) < 5) {
                    // 10% of rows are set to 0
                    v = 0;
                } else {
                    v = (rand() % range) - ((4 * range) / 10) + ((rand() % 100) / 100.0);
                }
                vector->SetValue(i, static_cast<T>(v));
            }
        }
        return vector;
    }
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
class HashAggregatorTesterTemplate : public AggregatorTesterTemplate<IN_ID, OUT_ID> {
public:
    HashAggregatorTesterTemplate(const std::string aggFuncName, const int32_t nullPercent, const bool isDict,
        const bool hasMask, const bool nullWhenOverflow)
        : AggregatorTesterTemplate<IN_ID, OUT_ID>(aggFuncName, nullPercent, isDict, hasMask, nullWhenOverflow)
    {}

    ~HashAggregatorTesterTemplate() override = default;

    int32_t GetValueColumnIndex() override
    {
        return 1;
    }

    // functions to build input data and generate expected results
    std::vector<VectorBatch *> BuildAggInput(const int32_t vecBatchNum, const int32_t rowPerVecBatch) override
    {
        std::vector<VectorBatch *> inputs =
            AggregatorTesterTemplate<IN_ID, OUT_ID>::BuildAggInput(vecBatchNum, rowPerVecBatch);

        for (VectorBatch *vb : inputs) {
            Vector *vector =
                CreateIntVectorGroupby(this->vectorAllocator, rowPerVecBatch, this->nullPercent, 10, this->isDict);
            vb->SetVector(0, vector);
        }

        return inputs;
    }

    bool GeneratePartialExpectedResult(VectorBatch **expectedResult, std::vector<VectorBatch *> &vvb) override
    {
        *expectedResult = this->InitializeExpectedPartialResult(vvb);

        if constexpr (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CHAR) {
            this->GenerateVarcharResult(vvb, *expectedResult, true);
            return false;
        } else if constexpr (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CHAR) {
            throw OmniException("Invalid Arguement",
                "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)) + " for Varchar input");
        } else {
            using T_IN = typename NativeAndVectorType<IN_ID>::type;
            Vector *groups = (*expectedResult)->GetVector(0);
            bool overalOverflow = false;
            const int32_t valueIndex = this->GetValueColumnIndex();
            const int32_t maskIndex = this->GetMaskColumnIndex();
            int32_t orgIdx;

            if (TypeUtil::IsDecimalType(IN_ID) &&
                (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM || this->aggFunc == OMNI_AGGREGATION_TYPE_AVG)) {
                for (int32_t i = 0; i < groups->GetSize(); ++i) {
                    IntVector *orgGroups =
                        static_cast<IntVector *>(VectorHelper::ExpandVectorAndIndex(groups, i, orgIdx));
                    int32_t filterValue = orgGroups->GetValue(orgIdx);
                    int32_t *filterValuePtr = groups->IsValueNull(i) ? nullptr : &filterValue;
                    int64_t count = 0;
                    Decimal128 result {};
                    bool overflow = GenerateExpectedResultNumeric<GroupByFilter, IN_ID, Decimal128, Decimal128>(vvb,
                        valueIndex, maskIndex, SumFunc<T_IN, Decimal128>, count, result, filterValuePtr, 0);

                    static_cast<LongVector *>((*expectedResult)->GetVector(2))->SetValue(i, count);
                    if (overflow || count == 0) {
                        (*expectedResult)->GetVector(1)->SetValueNull(i);
                    } else {
                        static_cast<Decimal128Vector *>((*expectedResult)->GetVector(1))->SetValue(i, result);
                    }

                    overalOverflow |= overflow;
                }

                return overalOverflow;
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_AVG) {
                for (int32_t i = 0; i < groups->GetSize(); ++i) {
                    IntVector *orgGroups =
                        static_cast<IntVector *>(VectorHelper::ExpandVectorAndIndex(groups, i, orgIdx));
                    int32_t filterValue = orgGroups->GetValue(orgIdx);
                    int32_t *filterValuePtr = groups->IsValueNull(i) ? nullptr : &filterValue;
                    int64_t count = 0;
                    double result {};
                    bool overflow = GenerateExpectedResultNumeric<GroupByFilter, IN_ID, double, double>(vvb, valueIndex,
                        maskIndex, SumFunc<T_IN, double>, count, result, filterValuePtr, 0);

                    static_cast<LongVector *>((*expectedResult)->GetVector(2))->SetValue(i, count);
                    if (overflow || count == 0) {
                        (*expectedResult)->GetVector(1)->SetValueNull(i);
                    } else {
                        static_cast<DoubleVector *>((*expectedResult)->GetVector(1))->SetValue(i, result);
                    }

                    overalOverflow |= overflow;
                }

                return overalOverflow;
            } else {
                using T_OUT = typename NativeAndVectorType<OUT_ID>::type;
                using V_OUT = typename NativeAndVectorType<OUT_ID>::vector;
                for (int32_t i = 0; i < groups->GetSize(); ++i) {
                    IntVector *orgGroups =
                        static_cast<IntVector *>(VectorHelper::ExpandVectorAndIndex(groups, i, orgIdx));
                    int32_t filterValue = orgGroups->GetValue(orgIdx);
                    int32_t *filterValuePtr = groups->IsValueNull(i) ? nullptr : &filterValue;
                    int64_t count = 0;
                    T_OUT result {};
                    bool overflow;

                    if (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM) {
                        using T_MID = std::conditional_t<std::is_floating_point_v<T_IN>, double, int64_t>;
                        overflow = GenerateExpectedResultNumeric<GroupByFilter, IN_ID, T_OUT, T_MID>(vvb, valueIndex,
                            maskIndex, SumFunc<T_IN, T_MID>, count, result, filterValuePtr, 0);
                    } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MIN) {
                        overflow = GenerateExpectedResultNumeric<GroupByFilter, IN_ID, T_OUT, T_IN>(vvb, valueIndex,
                            maskIndex, MinFunc<T_IN, T_IN>, count, result, filterValuePtr, 0);
                    } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MAX) {
                        overflow = GenerateExpectedResultNumeric<GroupByFilter, IN_ID, T_OUT, T_IN>(vvb, valueIndex,
                            maskIndex, MaxFunc<T_IN, T_IN>, count, result, filterValuePtr, 0);
                    } else {
                        throw OmniException("Invalid Arguement",
                            "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)));
                    }

                    static_cast<LongVector *>((*expectedResult)->GetVector(2))->SetValue(i, count);
                    if (overflow || count == 0) {
                        (*expectedResult)->GetVector(1)->SetValueNull(i);
                    } else {
                        static_cast<V_OUT *>((*expectedResult)->GetVector(1))->SetValue(i, result);
                    }

                    overalOverflow |= overflow;
                }

                return overalOverflow;
            }
        }

        throw OmniException("Unreachable code", "Unreachable code");
    }

    bool GenerateFinalExpectedResult(VectorBatch **expectedResult, std::vector<VectorBatch *> &vvb) override
    {
        *expectedResult = this->InitializeExpectedFinalResult(vvb);
        Vector *groups = (*expectedResult)->GetVector(0);
        int32_t orgIdx;

        if constexpr (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CHAR) {
            this->GenerateVarcharResult(vvb, *expectedResult, false);
            for (int32_t i = 0; i < groups->GetSize(); ++i) {
                IntVector *orgGroups = static_cast<IntVector *>(VectorHelper::ExpandVectorAndIndex(groups, i, orgIdx));
                int32_t filterValue = orgGroups->GetValue(orgIdx);
                int32_t *filterValuePtr = groups->IsValueNull(i) ? nullptr : &filterValue;
                static_cast<LongVector *>((*expectedResult)->GetVector(2))
                    ->SetValue(i, this->GetGroupCount(vvb, filterValuePtr));
            }

            VectorHelper::FreeVecBatches(vvb);
            return false;
        } else if constexpr (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CHAR) {
            VectorHelper::FreeVecBatches(vvb);
            throw OmniException("Invalid Arguement",
                "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)) + " for Varchar input");
        } else {
            using T_OUT = typename NativeAndVectorType<OUT_ID>::type;
            using V_OUT = typename NativeAndVectorType<OUT_ID>::vector;
            bool overalOverflow = false;
            const int32_t valueIndex = this->GetValueColumnIndex();
            // no mask for final aggregation
            const int32_t maskIndex = -1;

            for (int32_t i = 0; i < groups->GetSize(); ++i) {
                IntVector *orgGroups = static_cast<IntVector *>(VectorHelper::ExpandVectorAndIndex(groups, i, orgIdx));
                int32_t filterValue = orgGroups->GetValue(orgIdx);
                int32_t *filterValuePtr = groups->IsValueNull(i) ? nullptr : &filterValue;
                T_OUT result {};
                int64_t validCount = 0;
                bool overflow;
                int64_t count = this->GetGroupCount(vvb, filterValuePtr);

                if (TypeUtil::IsDecimalType(IN_ID) &&
                    (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM || this->aggFunc == OMNI_AGGREGATION_TYPE_AVG)) {
                    if (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM) {
                        overflow = GenerateExpectedResultNumeric<GroupByFilter, OMNI_DECIMAL128, T_OUT, Decimal128>(vvb,
                            valueIndex, maskIndex, SumFunc<Decimal128, Decimal128>, validCount, result, filterValuePtr,
                            0);
                    } else {
                        Decimal128 result128 {};
                        overflow =
                            GenerateExpectedResultNumeric<GroupByFilter, OMNI_DECIMAL128, Decimal128, Decimal128>(vvb,
                            valueIndex, maskIndex, SumFunc<Decimal128, Decimal128>, validCount, result128,
                            filterValuePtr, 0);
                        if (!overflow && validCount > 0) {
                            // generate actual average from some and count
                            Decimal128Wrapper wrapped =
                                Decimal128Wrapper(result128).Divide(Decimal128Wrapper(count), 0);
                            overflow = !DoCast<Decimal128, T_OUT>(result, wrapped.ToDecimal128());
                        }
                    }
                } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM) {
                    using T_MID = std::conditional_t<std::is_floating_point_v<T_OUT>, double, int64_t>;
                    overflow = GenerateExpectedResultNumeric<GroupByFilter, OUT_ID, T_OUT, T_MID>(vvb, valueIndex,
                        maskIndex, SumFunc<T_OUT, T_MID>, validCount, result, filterValuePtr, 0);
                } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_AVG) {
                    double resultDouble {};
                    overflow = GenerateExpectedResultNumeric<GroupByFilter, OMNI_DOUBLE, double, double>(vvb,
                        valueIndex, maskIndex, SumFunc<double, double>, validCount, resultDouble, filterValuePtr, 0);
                    if (!overflow && validCount > 0) {
                        overflow = !DoCast<double, T_OUT>(result, resultDouble /= count);
                    }
                } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MIN) {
                    overflow = GenerateExpectedResultNumeric<GroupByFilter, OUT_ID, T_OUT, T_OUT>(vvb, valueIndex,
                        maskIndex, MinFunc<T_OUT, T_OUT>, validCount, result, filterValuePtr, 0);
                } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MAX) {
                    overflow = GenerateExpectedResultNumeric<GroupByFilter, OUT_ID, T_OUT, T_OUT>(vvb, valueIndex,
                        maskIndex, MaxFunc<T_OUT, T_OUT>, validCount, result, filterValuePtr, 0);
                } else {
                    VectorHelper::FreeVecBatches(vvb);
                    throw OmniException("Invalid Arguement",
                        "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)));
                }

                static_cast<LongVector *>((*expectedResult)->GetVector(2))->SetValue(i, count);

                if (overflow || validCount == 0) {
                    (*expectedResult)->GetVector(1)->SetValueNull(i);
                } else {
                    static_cast<V_OUT *>((*expectedResult)->GetVector(1))->SetValue(i, result);
                }

                overalOverflow |= overflow;
            }

            VectorHelper::FreeVecBatches(vvb);
            return overalOverflow;
        }

        throw OmniException("Unreachable code", "Unreachable code");
    }

protected:
    std::unique_ptr<OperatorFactory> CreateFactory(std::vector<uint32_t> &aggFuncVec,
        std::vector<DataTypePtr> &inputTypeVec, std::vector<DataTypePtr> &outputTypeVec,
        std::vector<uint32_t> &aggColIdxVec, std::vector<uint32_t> &aggMaskVec, const bool inputRaw,
        const bool outputPartial) override
    {
        std::vector<uint32_t> groupByCol { 0 };
        DataTypes groupInputTypes(std::vector<DataTypePtr> { GetType(OMNI_INT) });
        auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(inputTypeVec));
        auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggColIdxVec);
        auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(outputTypeVec));
        auto inputRawWrap = AggregatorUtil::WrapWithVector(inputRaw, 2);
        auto outputPartialWrap = AggregatorUtil::WrapWithVector(outputPartial, 2);

        auto factory = std::make_unique<HashAggregationOperatorFactory>(groupByCol, groupInputTypes, aggInputColsWrap,
            aggInputTypesWrap, aggOutputTypesWrap, aggFuncVec, aggMaskVec, inputRawWrap, outputPartialWrap,
            this->nullWhenOverflow);

        if (factory == nullptr) {
            return nullptr;
        }

        factory->Init();
        return factory;
    }

    void GenerateVarcharResult(std::vector<VectorBatch *> &vvb, VectorBatch *expectedResult, const bool useMask)
    {
        Vector *groups = expectedResult->GetVector(0);
        const int32_t maskIndex = useMask ? this->GetMaskColumnIndex() : -1;
        const int32_t valueIndex = this->GetValueColumnIndex();
        int32_t orgIdx;

        for (int32_t i = 0; i < groups->GetSize(); ++i) {
            IntVector *orgGroups = static_cast<IntVector *>(VectorHelper::ExpandVectorAndIndex(groups, i, orgIdx));
            int32_t filterValue = orgGroups->GetValue(orgIdx);
            int32_t *filterValuePtr = groups->IsValueNull(i) ? nullptr : &filterValue;
            uint8_t *result;
            int32_t resultLen = 0;
            int64_t count = 0;

            if (this->aggFunc == OMNI_AGGREGATION_TYPE_MIN) {
                result = GenerateExpectedResultVarchar<GroupByFilter, MinCharFunc>(vvb, valueIndex, maskIndex, count,
                    resultLen, filterValuePtr, 0);
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MAX) {
                result = GenerateExpectedResultVarchar<GroupByFilter, MaxCharFunc>(vvb, valueIndex, maskIndex, count,
                    resultLen, filterValuePtr, 0);
            } else {
                throw OmniException("Invalid Arguement",
                    "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)) + " for Varchar input");
            }

            static_cast<LongVector *>(expectedResult->GetVector(2))->SetValue(i, count);
            if (count > 0) {
                static_cast<VarcharVector *>(expectedResult->GetVector(1))->SetValue(i, result, resultLen);
            } else {
                static_cast<VarcharVector *>(expectedResult->GetVector(1))->SetValueNull(i);
            }
        }
    }

    int32_t GetMaskColumnIndex() override
    {
        return this->hasMask ? 2 : -1;
    }

    int32_t GetNumberOfInputColumns() override
    {
        return this->hasMask ? 3 : 2;
    }

private:
    VectorBatch *InitializeExpectedPartialResult(std::vector<VectorBatch *> &vvb)
    {
        int32_t hasNull = 0;
        std::set<int32_t> groups;
        for (VectorBatch *vb : vvb) {
            int32_t orgIdx;
            Vector *v = vb->GetVector(0);
            for (int32_t i = 0; i < v->GetSize(); ++i) {
                if (v->IsValueNull(i)) {
                    hasNull = 1;
                } else {
                    IntVector *orgV = static_cast<IntVector *>(VectorHelper::ExpandVectorAndIndex(v, i, orgIdx));
                    groups.insert(orgV->GetValue(orgIdx));
                }
            }
        }

        int32_t nGroups = groups.size() + hasNull;
        VectorBatch *expectedResult = new VectorBatch(3);
        IntVector *groupCol = new IntVector(this->vectorAllocator, nGroups);
        int32_t rowIdx = 0;
        if (hasNull > 0) {
            groupCol->SetValueNull(rowIdx++);
        }
        for (int32_t v : groups) {
            groupCol->SetValue(rowIdx++, v);
        }

        expectedResult->SetVector(0, groupCol);
        expectedResult->SetVector(2, new LongVector(this->vectorAllocator, nGroups));
        if constexpr (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CHAR) {
            expectedResult->SetVector(1, new VarcharVector(this->vectorAllocator, MAX_VARCHAR_LENGTH, nGroups));
        } else {
            if (TypeUtil::IsDecimalType(IN_ID) &&
                (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM || this->aggFunc == OMNI_AGGREGATION_TYPE_AVG)) {
                expectedResult->SetVector(1, new Decimal128Vector(this->vectorAllocator, nGroups));
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_AVG) {
                expectedResult->SetVector(1, new DoubleVector(this->vectorAllocator, nGroups));
            } else {
                expectedResult->SetVector(1,
                    VectorHelper::CreateFlatVector(this->vectorAllocator, OUT_ID, MAX_VARCHAR_LENGTH, nGroups));
            }
        }

        return expectedResult;
    }

    VectorBatch *InitializeExpectedFinalResult(std::vector<VectorBatch *> &vvb)
    {
        int32_t hasNull = 0;
        std::set<int32_t> groups;
        for (VectorBatch *vb : vvb) {
            IntVector *groupCol = static_cast<IntVector *>(vb->GetVector(0));
            for (int32_t i = 0; i < groupCol->GetSize(); ++i) {
                if (groupCol->IsValueNull(i)) {
                    hasNull = 1;
                } else {
                    groups.insert(groupCol->GetValue(i));
                }
            }
        }

        int32_t nGroups = groups.size() + hasNull;
        VectorBatch *expectedResult = new VectorBatch(3);
        IntVector *groupCol = new IntVector(this->vectorAllocator, nGroups);
        int32_t rowIdx = 0;
        if (hasNull > 0) {
            groupCol->SetValueNull(rowIdx++);
        }
        for (int32_t v : groups) {
            groupCol->SetValue(rowIdx++, v);
        }

        expectedResult->SetVector(0, groupCol);
        expectedResult->SetVector(1,
            VectorHelper::CreateFlatVector(this->vectorAllocator, OUT_ID, MAX_VARCHAR_LENGTH, nGroups));
        expectedResult->SetVector(2, new LongVector(this->vectorAllocator, nGroups));

        return expectedResult;
    }

    int64_t GetGroupCount(std::vector<VectorBatch *> &vvb, int32_t *filterValuePtr)
    {
        int64_t count = 0;
        int32_t orgIdx;
        for (VectorBatch *vb : vvb) {
            Vector *v = vb->GetVector(0);
            for (int32_t i = 0; i < v->GetSize(); ++i) {
                if (v->IsValueNull(i)) {
                    if (filterValuePtr == nullptr) {
                        count += static_cast<LongVector *>(vb->GetVector(2))->GetValue(i);
                    }
                } else {
                    IntVector *vector = static_cast<IntVector *>(VectorHelper::ExpandVectorAndIndex(v, i, orgIdx));
                    int32_t groupKey = vector->GetValue(orgIdx);
                    if (filterValuePtr != nullptr && *filterValuePtr == groupKey) {
                        count += static_cast<LongVector *>(vb->GetVector(2))->GetValue(i);
                    }
                }
            }
        }
        return count;
    }
};
}
