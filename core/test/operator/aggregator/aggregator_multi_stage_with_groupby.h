/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2024. All rights reserved.
 */
#pragma once
#include "aggregator_multi_stage_no_groupby.h"
#include "operator/aggregation/group_aggregation.h"

namespace omniruntime {
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::type;

inline BaseVector *CreateIntVectorGroupby(const int32_t nRows, const int32_t nullPercent, const int32_t range,
    const bool isDict)
{
    using V = typename AggNativeAndVectorType<OMNI_INT>::vector;
    using T = typename AggNativeAndVectorType<OMNI_INT>::type;
    double v;

    if (isDict) {
        int32_t valueStartIdx = nullPercent > 0 ? 1 : 0;
        V *vector = new V(256 + valueStartIdx);
        if (nullPercent > 0) {
            vector->SetNull(0);
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
        auto dictVector = VectorHelper::CreateDictionary(ids, nRows, vector);
        delete vector;
        return dictVector;
    } else {
        V *vector = new V(nRows);
        for (int32_t i = 0; i < nRows; ++i) {
            if ((rand() % 100) < nullPercent) {
                vector->SetNull(i);
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
    HashAggregatorTesterTemplate(const std::string aggFuncName_, const int32_t nullPercent_, const bool isDict_,
        const bool hasMask_, const bool nullWhenOverflow_)
        : AggregatorTesterTemplate<IN_ID, OUT_ID>(aggFuncName_, nullPercent_, isDict_, hasMask_, nullWhenOverflow_)
    {}

    ~HashAggregatorTesterTemplate() override = default;

    int32_t GetValueColumnIndex() override
    {
        return 1;
    }

    // functions to build input data and generate expected results
    std::vector<VectorBatch *> BuildAggInput(const int32_t vecBatchNum, const int32_t rowPerVecBatch) override
    {
        std::vector<VectorBatch *> inputs(vecBatchNum);

        for (int32_t i = 0; i < vecBatchNum; ++i) {
            VectorBatch *vecBatch = new VectorBatch(rowPerVecBatch);
            BaseVector *vector0 = CreateIntVectorGroupby(rowPerVecBatch, this->nullPercent, 10, this->isDict);
            vecBatch->Append(vector0);

            BaseVector *vector = nullptr;
            switch (IN_ID) {
                case OMNI_BOOLEAN:
                    vector = CreateFixedSizeVector<OMNI_BOOLEAN>(rowPerVecBatch, this->nullPercent, 256, this->isDict);
                    break;
                case OMNI_SHORT:
                    vector =
                        CreateFixedSizeVector<OMNI_SHORT>(rowPerVecBatch, this->nullPercent, 256 * 256, this->isDict);
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                    vector = CreateFixedSizeVector<OMNI_INT>(rowPerVecBatch, this->nullPercent, 256 * 256 * 256,
                        this->isDict);
                    break;
                case OMNI_DECIMAL64:
                case OMNI_TIMESTAMP:
                case OMNI_LONG:
                    vector = CreateFixedSizeVector<OMNI_LONG>(rowPerVecBatch, this->nullPercent, 256 * 256 * 256,
                        this->isDict);
                    break;
                case OMNI_DOUBLE:
                    vector = CreateFixedSizeVector<OMNI_DOUBLE>(rowPerVecBatch, this->nullPercent, 256 * 256 * 256,
                        this->isDict);
                    break;
                case OMNI_DECIMAL128:
                    vector = CreateFixedSizeVector<OMNI_DECIMAL128>(rowPerVecBatch, this->nullPercent, 256 * 256 * 256,
                        this->isDict);
                    break;
                case OMNI_VARCHAR:
                case OMNI_CHAR:
                    vector = CreateVarcharVector(rowPerVecBatch, this->nullPercent, 256, this->isDict);
                    break;
                default:
                    throw OmniException("Invalid Arguement",
                        "Unexpected input type " + TypeUtil::TypeToStringLog(IN_ID));
            }
            vecBatch->Append(vector);

            if (this->hasMask) {
                BaseVector *maskVector =
                    CreateFixedSizeVector<OMNI_BOOLEAN>(rowPerVecBatch, this->nullPercent, 256, this->isDict);
                vecBatch->Append(maskVector);
            }

            inputs[i] = vecBatch;
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
            BaseVector *groups = (*expectedResult)->Get(0);
            bool overalOverflow = false;
            const int32_t valueIndex = this->GetValueColumnIndex();
            const int32_t maskIndex = this->GetMaskColumnIndex();

            if (TypeUtil::IsDecimalType(IN_ID) &&
                (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM || this->aggFunc == OMNI_AGGREGATION_TYPE_AVG)) {
                for (int32_t i = 0; i < groups->GetSize(); ++i) {
                    int32_t filterValue;
                    if (groups->GetEncoding() == vec::OMNI_DICTIONARY) {
                        filterValue = static_cast<Vector<DictionaryContainer<int32_t>> *>(groups)->GetValue(i);
                    } else {
                        filterValue = static_cast<Vector<int32_t> *>(groups)->GetValue(i);
                    }
                    int32_t *filterValuePtr = groups->IsNull(i) ? nullptr : &filterValue;
                    int64_t count = 0;
                    Decimal128 result{};
                    bool overflow = GenerateExpectedResultNumeric<GroupByFilter, IN_ID, Decimal128, Decimal128>(vvb,
                        valueIndex, maskIndex, SumFunc<T_IN, Decimal128>, count, result, filterValuePtr, 0);

                    static_cast<Vector<int64_t> *>((*expectedResult)->Get(2))->SetValue(i, count);
                    if (overflow || count == 0) {
                        (*expectedResult)->Get(1)->SetNull(i);
                    } else {
                        static_cast<Vector<Decimal128> *>((*expectedResult)->Get(1))->SetValue(i, result);
                    }

                    overalOverflow |= overflow;
                }

                return overalOverflow;
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_AVG) {
                for (int32_t i = 0; i < groups->GetSize(); ++i) {
                    int32_t filterValue;
                    if (groups->GetEncoding() == vec::OMNI_DICTIONARY) {
                        filterValue = static_cast<Vector<DictionaryContainer<int32_t>> *>(groups)->GetValue(i);
                    } else {
                        filterValue = static_cast<Vector<int32_t> *>(groups)->GetValue(i);
                    }
                    int32_t *filterValuePtr = groups->IsNull(i) ? nullptr : &filterValue;
                    int64_t count = 0;
                    double result{};
                    bool overflow = GenerateExpectedResultNumeric<GroupByFilter, IN_ID, double, double>(vvb, valueIndex,
                        maskIndex, SumFunc<T_IN, double>, count, result, filterValuePtr, 0);

                    static_cast<Vector<int64_t> *>((*expectedResult)->Get(2))->SetValue(i, count);
                    if (overflow || count == 0) {
                        (*expectedResult)->Get(1)->SetNull(i);
                    } else {
                        static_cast<Vector<double> *>((*expectedResult)->Get(1))->SetValue(i, result);
                    }

                    overalOverflow |= overflow;
                }

                return overalOverflow;
            } else {
                using T_OUT = typename NativeAndVectorType<OUT_ID>::type;
                using V_OUT = typename NativeAndVectorType<OUT_ID>::vector;
                for (int32_t i = 0; i < groups->GetSize(); ++i) {
                    int32_t filterValue;
                    if (groups->GetEncoding() == vec::OMNI_DICTIONARY) {
                        filterValue = static_cast<Vector<DictionaryContainer<int32_t>> *>(groups)->GetValue(i);
                    } else {
                        filterValue = static_cast<Vector<int32_t> *>(groups)->GetValue(i);
                    }
                    int32_t *filterValuePtr = groups->IsNull(i) ? nullptr : &filterValue;
                    int64_t count = 0;
                    T_OUT result{};
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

                    static_cast<Vector<int64_t> *>((*expectedResult)->Get(2))->SetValue(i, count);
                    if (overflow || count == 0) {
                        (*expectedResult)->Get(1)->SetNull(i);
                    } else {
                        static_cast<V_OUT *>((*expectedResult)->Get(1))->SetValue(i, result);
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
        BaseVector *groups = (*expectedResult)->Get(0);

        if constexpr (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CHAR) {
            this->GenerateVarcharResult(vvb, *expectedResult, false);
            for (int32_t i = 0; i < groups->GetSize(); ++i) {
                int32_t filterValue;
                if (groups->GetEncoding() == vec::OMNI_DICTIONARY) {
                    filterValue = static_cast<Vector<DictionaryContainer<int32_t>> *>(groups)->GetValue(i);
                } else {
                    filterValue = static_cast<Vector<int32_t> *>(groups)->GetValue(i);
                }
                int32_t *filterValuePtr = groups->IsNull(i) ? nullptr : &filterValue;
                static_cast<Vector<int64_t> *>((*expectedResult)->Get(2))
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
                int32_t filterValue;
                if (groups->GetEncoding() == vec::OMNI_DICTIONARY) {
                    filterValue = static_cast<Vector<DictionaryContainer<int32_t>> *>(groups)->GetValue(i);
                } else {
                    filterValue = static_cast<Vector<int32_t> *>(groups)->GetValue(i);
                }
                int32_t *filterValuePtr = groups->IsNull(i) ? nullptr : &filterValue;
                T_OUT result{};
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
                        Decimal128 result128{};
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
                    double resultDouble{};
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

                static_cast<Vector<int64_t> *>((*expectedResult)->Get(2))->SetValue(i, count);

                if (overflow || validCount == 0) {
                    (*expectedResult)->Get(1)->SetNull(i);
                } else {
                    static_cast<V_OUT *>((*expectedResult)->Get(1))->SetValue(i, result);
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
        std::vector<uint32_t> groupByCol{ 0 };
        DataTypes groupInputTypes(std::vector<DataTypePtr>{ GetType(OMNI_INT) });
        auto aggInputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(inputTypeVec));
        auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggColIdxVec);
        auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(outputTypeVec));
        auto inputRawWrap = std::vector<bool>(2, inputRaw);
        auto outputPartialWrap = std::vector<bool>(2, outputPartial);

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
        BaseVector *groups = expectedResult->Get(0);
        const int32_t maskIndex = useMask ? this->GetMaskColumnIndex() : -1;
        const int32_t valueIndex = this->GetValueColumnIndex();

        for (int32_t i = 0; i < groups->GetSize(); ++i) {
            int32_t filterValue;
            if (groups->GetEncoding() == vec::OMNI_DICTIONARY) {
                filterValue = static_cast<Vector<DictionaryContainer<int32_t>> *>(groups)->GetValue(i);
            } else {
                filterValue = static_cast<Vector<int32_t> *>(groups)->GetValue(i);
            }
            int32_t *filterValuePtr = groups->IsNull(i) ? nullptr : &filterValue;
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

            static_cast<Vector<int64_t> *>(expectedResult->Get(2))->SetValue(i, count);
            if (count > 0) {
                std::string_view str((char *)result, resultLen);
                static_cast<Vector<LargeStringContainer<std::string_view>> *>(expectedResult->Get(1))->SetValue(i, str);
            } else {
                static_cast<Vector<LargeStringContainer<std::string_view>> *>(expectedResult->Get(1))->SetNull(i);
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
            BaseVector *v = vb->Get(0);
            for (int32_t i = 0; i < v->GetSize(); ++i) {
                if (v->IsNull(i)) {
                    hasNull = 1;
                } else {
                    int32_t val;
                    if (v->GetEncoding() == vec::OMNI_DICTIONARY) {
                        val = static_cast<Vector<DictionaryContainer<int32_t>> *>(v)->GetValue(i);
                    } else {
                        val = static_cast<Vector<int32_t> *>(v)->GetValue(i);
                    }
                    groups.insert(val);
                }
            }
        }

        int32_t nGroups = groups.size() + hasNull;
        VectorBatch *expectedResult = new VectorBatch(nGroups);
        Vector<int32_t> *groupCol = new Vector<int32_t>(nGroups);
        int32_t rowIdx = 0;
        if (hasNull > 0) {
            groupCol->SetNull(rowIdx++);
        }
        for (int32_t v : groups) {
            groupCol->SetValue(rowIdx++, v);
        }

        expectedResult->Append(groupCol);
        if constexpr (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CHAR) {
            expectedResult->Append(new Vector<LargeStringContainer<std::string_view>>(nGroups));
        } else {
            if (TypeUtil::IsDecimalType(IN_ID) &&
                (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM || this->aggFunc == OMNI_AGGREGATION_TYPE_AVG)) {
                expectedResult->Append(new Vector<Decimal128>(nGroups));
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_AVG) {
                expectedResult->Append(new Vector<double>(nGroups));
            } else {
                BaseVector *v = DYNAMIC_TYPE_DISPATCH(VectorHelper::CreateFlatVector, OUT_ID, nGroups);
                expectedResult->Append(v);
            }
        }
        expectedResult->Append(new Vector<int64_t>(nGroups));

        return expectedResult;
    }

    VectorBatch *InitializeExpectedFinalResult(std::vector<VectorBatch *> &vvb)
    {
        int32_t hasNull = 0;
        std::set<int32_t> groups;
        for (VectorBatch *vb : vvb) {
            Vector<int32_t> *groupCol = static_cast<Vector<int32_t> *>(vb->Get(0));
            for (int32_t i = 0; i < groupCol->GetSize(); ++i) {
                if (groupCol->IsNull(i)) {
                    hasNull = 1;
                } else {
                    groups.insert(groupCol->GetValue(i));
                }
            }
        }

        int32_t nGroups = groups.size() + hasNull;
        VectorBatch *expectedResult = new VectorBatch(nGroups);
        Vector<int32_t> *groupCol = new Vector<int32_t>(nGroups);
        int32_t rowIdx = 0;
        if (hasNull > 0) {
            groupCol->SetNull(rowIdx++);
        }
        for (int32_t v : groups) {
            groupCol->SetValue(rowIdx++, v);
        }

        expectedResult->Append(groupCol);
        BaseVector *v = DYNAMIC_TYPE_DISPATCH(VectorHelper::CreateFlatVector, OUT_ID, nGroups);
        expectedResult->Append(v);
        expectedResult->Append(new Vector<int64_t>(nGroups));

        return expectedResult;
    }

    int64_t GetGroupCount(std::vector<VectorBatch *> &vvb, int32_t *filterValuePtr)
    {
        int64_t count = 0;
        for (VectorBatch *vb : vvb) {
            BaseVector *v = vb->Get(0);
            for (int32_t i = 0; i < v->GetSize(); ++i) {
                if (v->IsNull(i)) {
                    if (filterValuePtr == nullptr) {
                        count += static_cast<Vector<int64_t> *>(vb->Get(2))->GetValue(i);
                    }
                } else {
                    int32_t groupKey;
                    if (v->GetEncoding() == vec::OMNI_DICTIONARY) {
                        groupKey = static_cast<Vector<DictionaryContainer<int32_t>> *>(v)->GetValue(i);
                    } else {
                        groupKey = static_cast<Vector<int32_t> *>(v)->GetValue(i);
                    }
                    if (filterValuePtr != nullptr && *filterValuePtr == groupKey) {
                        count += static_cast<Vector<int64_t> *>(vb->Get(2))->GetValue(i);
                    }
                }
            }
        }
        return count;
    }
};
}
