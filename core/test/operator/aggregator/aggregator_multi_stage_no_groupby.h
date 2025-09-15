/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2024. All rights reserved.
 */
#pragma once
#include <set>

#include "operator/aggregation/aggregator/all_aggregators.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "operator/omni_id_type_vector_traits.h"
#include "vector/vector_helper.h"
#include "test/util/test_util.h"

namespace omniruntime {
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::type;

constexpr int32_t MAX_VARCHAR_LENGTH = 128;
constexpr int32_t VEC_BATCH_NUM = 10;
constexpr int32_t ROW_SIZE = 2000;
inline static std::map<std::string, FunctionType> aggFuncs = { { "sum", OMNI_AGGREGATION_TYPE_SUM },
                                                               { "avg", OMNI_AGGREGATION_TYPE_AVG },
                                                               { "min", OMNI_AGGREGATION_TYPE_MIN },
                                                               { "max", OMNI_AGGREGATION_TYPE_MAX } };

inline static std::map<DataTypeId, std::vector<DataTypeId>> unsupportedForSum{ { OMNI_BOOLEAN, {} },
    { OMNI_SHORT, { OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_INT, { OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_DATE32, { OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_LONG, { OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_DOUBLE, { OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_DECIMAL64, { OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_DECIMAL128, { OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_CHAR, {} },
    { OMNI_VARCHAR, {} } };

inline static std::map<DataTypeId, std::vector<DataTypeId>> unsupportedForAvg{ { OMNI_BOOLEAN, {} },
    { OMNI_SHORT, { OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_DATE32, OMNI_LONG, OMNI_CHAR,
           OMNI_VARCHAR } },
    { OMNI_INT, { OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_DATE32, OMNI_LONG, OMNI_CHAR,
           OMNI_VARCHAR } },
    { OMNI_DATE32, { OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_DATE32, OMNI_LONG, OMNI_CHAR,
           OMNI_VARCHAR } },
    { OMNI_LONG, { OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_DATE32, OMNI_LONG, OMNI_CHAR,
           OMNI_VARCHAR } },
    { OMNI_DOUBLE, { OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_DATE32, OMNI_LONG, OMNI_CHAR,
           OMNI_VARCHAR } },
    { OMNI_DECIMAL64, { OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_DATE32, OMNI_LONG, OMNI_CHAR,
           OMNI_VARCHAR } },
    { OMNI_DECIMAL128, { OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_DATE32, OMNI_LONG, OMNI_CHAR,
           OMNI_VARCHAR } },
    { OMNI_CHAR, {} },
    { OMNI_VARCHAR, {} } };

inline static std::map<DataTypeId, std::vector<DataTypeId>> unsupportedForMin{ { OMNI_BOOLEAN,
    { OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_SHORT, { OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_INT, { OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_DATE32, { OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_LONG, { OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_DOUBLE, { OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_DECIMAL64, { OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_DECIMAL128, { OMNI_CHAR, OMNI_VARCHAR } },
    { OMNI_CHAR, { OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_DATE32, OMNI_LONG, OMNI_DOUBLE,
          OMNI_DECIMAL64, OMNI_DECIMAL128, OMNI_VARCHAR } },
    { OMNI_VARCHAR, { OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_DATE32, OMNI_LONG, OMNI_DOUBLE,
          OMNI_DECIMAL64, OMNI_DECIMAL128, OMNI_CHAR } } };

inline static std::map<std::string, std::map<DataTypeId, std::vector<DataTypeId>>> unsupported{ { "sum",
    unsupportedForSum },
    { "avg", unsupportedForAvg },
    { "min", unsupportedForMin },
    { "max", unsupportedForMin } };

inline static std::vector<std::string> aggFuncName__{ "sum", "min", "max", "avg" };
inline static std::vector<DataTypeId> dataTypes__{ OMNI_BOOLEAN,   OMNI_SHORT,      OMNI_INT,  OMNI_LONG,   OMNI_DOUBLE,
    OMNI_DECIMAL64, OMNI_DECIMAL128, OMNI_CHAR, OMNI_VARCHAR };
inline static std::vector<int32_t> nullPercent__{ 0, 25, 100 };
inline static std::vector<bool> isDict__{ false, true };
inline static std::vector<bool> hasMask__{ false, true };
inline static std::vector<bool> nullWhenOverflow__{ false, true };
inline static std::vector<bool> groupby__{ false, true };

inline bool CheckSupported(const std::string &aggFuncName, const DataTypeId inId, const DataTypeId outId)
{
    std::vector<DataTypeId> &unsupportedOutputs = unsupported[aggFuncName][inId];
    if (unsupportedOutputs.size() == 0) {
        return false;
    }
    return std::find(unsupportedOutputs.begin(), unsupportedOutputs.end(), outId) == unsupportedOutputs.end();
}

template <DataTypeId TYPE_ID>
BaseVector *CreateFixedSizeVector(const int32_t nRows, const int32_t nullPercent, const int32_t range,
    const bool isDict)
{
    using V = typename NativeAndVectorType<TYPE_ID>::vector;
    using T = typename NativeAndVectorType<TYPE_ID>::type;
    double v;

    if (isDict) {
        int32_t valueStartIdx = nullPercent > 0 ? 1 : 0;
        V *vector = new V(256 + valueStartIdx);
        if (nullPercent > 0) {
            vector->SetNull(0);
        }

        std::set<double> dictValues;
        dictValues.insert(0);
        if constexpr (std::is_same_v<V, Vector<Decimal128>>) {
            vector->SetValue(valueStartIdx, Decimal128Wrapper(static_cast<int64_t>(0)).ToDecimal128());
        } else {
            vector->SetValue(valueStartIdx, static_cast<T>(0));
        }
        for (int32_t i = valueStartIdx + 1; i < vector->GetSize(); ++i) {
            do {
                v = (rand() % range) - ((4 * range) / 10) + ((rand() % 100) / 100.0);
            } while (!dictValues.insert(v).second);

            if constexpr (std::is_same_v<V, Vector<Decimal128>>) {
                vector->SetValue(i, Decimal128Wrapper(static_cast<int64_t>(v * 100)).ToDecimal128());
            } else {
                vector->SetValue(i, static_cast<T>(v));
            }
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
                if constexpr (std::is_same_v<V, Vector<Decimal128>>) {
                    vector->SetValue(i, Decimal128Wrapper(static_cast<int64_t>(v * 100)).ToDecimal128());
                } else {
                    vector->SetValue(i, static_cast<T>(v));
                }
            }
        }
        return vector;
    }
}

inline BaseVector *CreateVarcharVector(const int32_t nRows, const int32_t nullPercent, const int32_t range,
    const bool isDict)
{
    uint8_t charBuffer[MAX_VARCHAR_LENGTH];
    if (isDict) {
        int32_t valueStartIdx = nullPercent > 0 ? 1 : 0;
        Vector<LargeStringContainer<std::string_view>> *vector =
            new Vector<LargeStringContainer<std::string_view>>(range + valueStartIdx);
        if (nullPercent > 0) {
            vector->SetNull(0);
        }

        for (int32_t r = valueStartIdx; r < vector->GetSize(); ++r) {
            const auto valLen = (rand() % (MAX_VARCHAR_LENGTH - 1)) + 1;
            for (int32_t i = 0; i < valLen; ++i) {
                charBuffer[i] = rand() % 95 + 32;
            }
            std::string_view str((char *)charBuffer, valLen);
            vector->SetValue(r, str);
        }

        int32_t ids[nRows];
        for (int32_t i = 0; i < nRows; ++i) {
            if ((rand() % 100) < nullPercent) {
                ids[i] = 0;
            } else {
                ids[i] = (rand() % range) + valueStartIdx;
            }
        }
        auto dictVector = VectorHelper::CreateStringDictionary(ids, nRows, vector);
        delete vector;
        return dictVector;
    } else {
        Vector<LargeStringContainer<std::string_view>> *vector =
            new Vector<LargeStringContainer<std::string_view>>(nRows);
        for (int32_t r = 0; r < nRows; ++r) {
            if ((rand() % 100) < nullPercent) {
                vector->SetNull(r);
            } else {
                const auto valLen = (rand() % (MAX_VARCHAR_LENGTH - 1)) + 1;
                for (int32_t i = 0; i < valLen; ++i) {
                    charBuffer[i] = rand() % 95 + 32;
                }
                std::string_view str((char *)charBuffer, valLen);
                vector->SetValue(r, str);
            }
        }
        return vector;
    }
}

inline std::vector<int32_t> SplitString(const std::string &str, const char &delim)
{
    std::vector<int32_t> result;
    std::string token;
    std::stringstream ss(str);
    while (std::getline(ss, token, delim)) {
        result.push_back(atoi(token.c_str()));
    }
    return result;
}

template <typename IN, typename OUT> inline bool DoCast(OUT &result, const IN &cur)
{
    if constexpr (std::is_same_v<IN, Decimal128>) {
        if constexpr (std::is_same_v<OUT, Decimal128>) {
            result = cur;
        } else {
            Decimal128Wrapper wrapped(cur);
            int64_t cur64 = 0;
            try {
                cur64 = static_cast<int64_t>(wrapped.SetScale(0));
            } catch (std::overflow_error &e) {
                return false;
            }

            result = static_cast<OUT>(cur64);
            if (static_cast<int64_t>(result) != cur64) {
                return false;
            }
        }
    } else if constexpr (std::is_same_v<OUT, Decimal128>) {
        if constexpr (std::is_floating_point_v<IN>) {
            result = Decimal128Wrapper(static_cast<int64_t>(round(cur))).ToDecimal128();
        } else {
            result = Decimal128Wrapper(static_cast<int64_t>(cur)).ToDecimal128();
        }
    } else {
        if constexpr (std::is_floating_point_v<IN>) {
            if constexpr (std::is_floating_point_v<OUT>) {
                result = static_cast<OUT>(cur);
            } else {
                IN integral = round(cur);
                result = static_cast<OUT>(integral);
                if (static_cast<IN>(result) != integral) {
                    return false;
                }
            }
        } else {
            result = static_cast<OUT>(cur);
            if constexpr (!std::is_floating_point_v<OUT>) {
                if (static_cast<IN>(result) != cur) {
                    return false;
                }
            }
        }
    }

    return true;
}

template <typename IN, typename OUT> inline bool MinFunc(OUT &result, const IN &cur, const bool isFirst)
{
    OUT casted{};
    if (!DoCast(casted, cur)) {
        return false;
    }

    if (isFirst || result > casted) {
        result = casted;
    }
    return true;
}

template <typename IN, typename OUT> inline bool MaxFunc(OUT &result, const IN &cur, const bool isFirst)
{
    OUT casted{};
    if (!DoCast(casted, cur)) {
        return false;
    }

    if (isFirst || result < casted) {
        result = casted;
    }
    return true;
}

template <typename IN, typename OUT> inline bool SumFunc(OUT &result, const IN &cur, const bool isFirst)
{
    OUT casted{};
    if (!DoCast(casted, cur)) {
        return false;
    }

    if constexpr (std::is_same_v<OUT, Decimal128>) {
        Decimal128Wrapper wrapped = Decimal128Wrapper(casted).Add(Decimal128Wrapper(result));
        result = wrapped.ToDecimal128();
        return wrapped.IsOverflow() == OpStatus::SUCCESS;
    } else if constexpr (std::is_floating_point_v<OUT>) {
        result += casted;
        return true;
    } else {
        int64_t in64 = static_cast<int64_t>(casted);
        int64_t out64 = static_cast<int64_t>(result);
        bool overflow = __builtin_add_overflow(out64, in64, &out64);
        if (overflow) {
            return false;
        } else {
            result = static_cast<OUT>(out64);
            return static_cast<int64_t>(result) == out64;
        }
    }
}

inline uint8_t *MinCharFunc(uint8_t *res, int32_t &len, uint8_t *curVal, const int32_t curLen)
{
    auto cmpResult = memcmp(res, curVal, std::min(len, curLen));
    if (cmpResult > 0 || (cmpResult == 0 && len > curLen)) {
        len = curLen;
        return curVal;
    } else {
        return res;
    }
}

inline uint8_t *MaxCharFunc(uint8_t *res, int32_t &len, uint8_t *curVal, const int32_t curLen)
{
    auto cmpResult = memcmp(res, curVal, std::min(len, curLen));
    if (cmpResult < 0 || (cmpResult == 0 && len < curLen)) {
        len = curLen;
        return curVal;
    } else {
        return res;
    }
}

inline bool ValidateOverflow(const std::string &stageName, const int32_t valueColIdx, VectorBatch *expectedResult,
    VectorBatch *actualResult)
{
    std::set<int32_t> matchRows;
    for (int32_t i = 0; i < expectedResult->GetRowCount(); ++i) {
        if (expectedResult->Get(valueColIdx)->IsNull(i)) {
            int64_t expectedCount = static_cast<Vector<int64_t> *>(expectedResult->Get(valueColIdx + 1))->GetValue(i);
            bool found = false;
            for (int32_t j = 0; j < expectedResult->GetRowCount(); ++j) {
                if (matchRows.find(j) != matchRows.end()) {
                    continue;
                }
                if (expectedCount == static_cast<Vector<int64_t> *>(actualResult->Get(valueColIdx + 1))->GetValue(j) &&
                    actualResult->Get(valueColIdx)->IsNull(j)) {
                    matchRows.insert(j);
                    found = true;
                    break;
                }
            }
            if (!found) {
                printf("ERROR: no raws in result matches expected row %d\n", i);
                printf("================== Expected Result (%s) ==================\n", stageName.c_str());
                printf("================== Actual Result (%s) ==================\n", stageName.c_str());
                return false;
            }
        }
    }
    return true;
}

inline void PrintVectorBatches(std::vector<VectorBatch *> &vvb, const std::string name)
{
    printf("============ %s ===========\n", name.c_str());
    for (size_t i = 0; i < vvb.size(); ++i) {
        printf("------- %ld / %ld -------\n", i, vvb.size());
    }
}

inline DataTypePtr GetType(DataTypeId typeId)
{
    switch (typeId) {
        case OMNI_NONE:
            return NoneType();
        case OMNI_BOOLEAN:
            return BooleanType();
        case OMNI_SHORT:
            return ShortType();
        case OMNI_INT:
            return IntType();
        case OMNI_DATE32:
            return Date32Type();
        case OMNI_TIME32:
            return Time32Type();
        case OMNI_LONG:
            return LongType();
        case OMNI_DATE64:
            return Date64Type();
        case OMNI_TIME64:
            return Time64Type();
        case OMNI_TIMESTAMP:
            return TimestampType();
        case OMNI_DOUBLE:
            return DoubleType();
        case OMNI_DECIMAL64:
            return Decimal64Type();
        case OMNI_DECIMAL128:
            return Decimal128Type();
        case OMNI_VARCHAR:
            return VarcharType(MAX_VARCHAR_LENGTH);
        case OMNI_CHAR:
            return CharType(MAX_VARCHAR_LENGTH);
        default:
            throw OmniException("Invalid Argument", "Invalid type " + TypeUtil::TypeToStringLog(typeId));
    }
}

inline bool AllMatchFilter(const int32_t *filterValue, VectorBatch *vb, const int32_t colIdx, const int32_t rowIdx)
{
    return true;
}

inline bool GroupByFilter(const int32_t *filterValue, VectorBatch *vb, const int32_t colIdx, const int32_t rowIdx)
{
    BaseVector *v = vb->Get(colIdx);
    if (filterValue == nullptr) {
        return v->IsNull(rowIdx);
    } else if (v->IsNull(rowIdx)) {
        return false;
    } else {
        int32_t val;
        if (v->GetEncoding() == vec::OMNI_DICTIONARY) {
            val = static_cast<Vector<DictionaryContainer<int32_t>> *>(v)->GetValue(rowIdx);
        } else {
            val = static_cast<Vector<int32_t> *>(v)->GetValue(rowIdx);
        }
        return val == *filterValue;
    }
}

class AggregatorTester {
public:
    virtual ~AggregatorTester() = default;

    // functions to build input data and generate expected results
    virtual std::vector<VectorBatch *> BuildAggInput(const int32_t vecBatchNum, const int32_t rowPerVecBatch) = 0;
    virtual bool GeneratePartialExpectedResult(VectorBatch **expectedResult, std::vector<VectorBatch *> &vb) = 0;
    virtual bool GenerateFinalExpectedResult(VectorBatch **expectedResult, std::vector<VectorBatch *> &vvb) = 0;

    // functions to generate aggregator factories
    virtual std::unique_ptr<OperatorFactory> CreatePartialFactory() = 0;
    virtual std::unique_ptr<OperatorFactory> CreateFinalFactory() = 0;

    virtual std::string GetExpectedExceptionMessage() = 0;
    virtual int32_t GetValueColumnIndex() = 0;
};

template <bool(FilterFunc)(const int32_t *, VectorBatch *, const int32_t, const int32_t),
    uint8_t *(UpdateFunc)(uint8_t *, int32_t &, uint8_t *, const int32_t)>
uint8_t *GenerateExpectedResultVarchar(std::vector<VectorBatch *> &vvb, const int32_t valueIndex,
    const int32_t maskIndex, int64_t &count, int32_t &resultLen, const int32_t *filterValue = nullptr,
    const int32_t groupbyColIdx = -1)
{
    count = 0;
    uint8_t *result = nullptr;
    resultLen = 0;
    bool valid;

    for (VectorBatch *vb : vvb) {
        BaseVector *maskVector = maskIndex < 0 ? nullptr : vb->Get(maskIndex);
        BaseVector *vector = vb->Get(valueIndex);

        for (int32_t i = 0; i < vector->GetSize(); ++i) {
            valid = true;
            if (maskVector != nullptr) {
                if (maskVector->IsNull(i)) {
                    valid = false;
                } else {
                    if (maskVector->GetEncoding() == vec::OMNI_DICTIONARY) {
                        valid = static_cast<Vector<DictionaryContainer<bool>> *>(maskVector)->GetValue(i);
                    } else {
                        valid = static_cast<Vector<bool> *>(maskVector)->GetValue(i);
                    }
                }
            }
            valid &= !vector->IsNull(i) & FilterFunc(filterValue, vb, groupbyColIdx, i);
            if (valid) {
                std::string_view str;
                if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    str = static_cast<Vector<DictionaryContainer<std::string_view>> *>(vector)->GetValue(i);
                } else {
                    str = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->GetValue(i);
                }
                uint8_t *curVal = (uint8_t *)str.data();
                int32_t curLen = str.size();
                if (count == 0) {
                    resultLen = curLen;
                    result = curVal;
                } else {
                    result = UpdateFunc(result, resultLen, curVal, curLen);
                }
                count++;
            }
        }
    }

    return result;
}

template <bool(FilterFunc)(const int32_t *, VectorBatch *, const int32_t, const int32_t), DataTypeId IN, typename T_OUT,
    typename ResultType, typename UpdateFunc>
bool GenerateExpectedResultNumeric(std::vector<VectorBatch *> &vvb, const int32_t valueIndex, const int32_t maskIndex,
    UpdateFunc updateFunc, int64_t &count, T_OUT &result, const int32_t *filterValue = nullptr,
    const int32_t groupbyColIdx = -1)
{
    using V_IN = typename NativeAndVectorType<IN>::vector;
    using D_IN = typename NativeAndVectorType<IN>::dictVector;
    count = 0;

    bool valid;
    ResultType midRes{};
    bool overflow = false;

    for (VectorBatch *vb : vvb) {
        BaseVector *maskVector = maskIndex < 0 ? nullptr : vb->Get(maskIndex);
        BaseVector *vector = vb->Get(valueIndex);

        for (int32_t i = 0; i < vector->GetSize(); ++i) {
            valid = true;
            if (maskVector != nullptr) {
                if (maskVector->IsNull(i)) {
                    valid = false;
                } else {
                    if (maskVector->GetEncoding() == vec::OMNI_DICTIONARY) {
                        valid = static_cast<Vector<DictionaryContainer<bool>> *>(maskVector)->GetValue(i);
                    } else {
                        valid = static_cast<Vector<bool> *>(maskVector)->GetValue(i);
                    }
                }
            }
            valid &= !vector->IsNull(i) & FilterFunc(filterValue, vb, groupbyColIdx, i);
            if (valid) {
                if (overflow) {
                    count++;
                    continue;
                }

                if (vector->GetEncoding() == OMNI_DICTIONARY) {
                    overflow |= !updateFunc(midRes, static_cast<D_IN *>(vector)->GetValue(i), count == 0);
                } else {
                    overflow |= !updateFunc(midRes, static_cast<V_IN *>(vector)->GetValue(i), count == 0);
                }
                count++;
            }
        }
    }

    if (!overflow && count > 0) {
        overflow = !DoCast<ResultType, T_OUT>(result, midRes);
    }

    return overflow;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID> class AggregatorTesterTemplate : public AggregatorTester {
public:
    AggregatorTesterTemplate(const std::string aggFuncName, const int32_t nullPercent, const bool isDict,
        const bool hasMask, const bool nullWhenOverflow)
        : aggFunc(aggFuncs[aggFuncName]),
          nullPercent(nullPercent),
          isDict(isDict),
          hasMask(hasMask),
          nullWhenOverflow(nullWhenOverflow)
    {}

    ~AggregatorTesterTemplate() override {}

    virtual int32_t GetValueColumnIndex()
    {
        return 0;
    }

    // functions to build input data and generate expected results
    std::vector<VectorBatch *> BuildAggInput(const int32_t vecBatchNum, const int32_t rowPerVecBatch) override
    {
        std::vector<VectorBatch *> input(vecBatchNum);

        for (int32_t i = 0; i < vecBatchNum; ++i) {
            VectorBatch *vecBatch = new VectorBatch(rowPerVecBatch);
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
                case OMNI_DATE64:
                case OMNI_TIME64:
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

            input[i] = vecBatch;
        }

        return input;
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
            bool overflow;
            int64_t count = 0;
            const int32_t valueIndex = this->GetValueColumnIndex();
            const int32_t maskIndex = this->GetMaskColumnIndex();

            if (TypeUtil::IsDecimalType(IN_ID) &&
                (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM || this->aggFunc == OMNI_AGGREGATION_TYPE_AVG)) {
                Decimal128 result{};
                overflow = GenerateExpectedResultNumeric<AllMatchFilter, IN_ID, Decimal128, Decimal128>(vvb, valueIndex,
                    maskIndex, SumFunc<T_IN, Decimal128>, count, result);

                static_cast<Vector<int64_t> *>((*expectedResult)->Get(1))->SetValue(0, count);
                if (overflow || count == 0) {
                    (*expectedResult)->Get(0)->SetNull(0);
                } else {
                    static_cast<Vector<Decimal128> *>((*expectedResult)->Get(0))->SetValue(0, result);
                }

                return overflow;
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_AVG) {
                double result{};
                overflow = GenerateExpectedResultNumeric<AllMatchFilter, IN_ID, double, double>(vvb, valueIndex,
                    maskIndex, SumFunc<T_IN, double>, count, result);

                static_cast<Vector<int64_t> *>((*expectedResult)->Get(1))->SetValue(0, count);
                if (overflow || count == 0) {
                    (*expectedResult)->Get(0)->SetNull(0);
                } else {
                    static_cast<Vector<double> *>((*expectedResult)->Get(0))->SetValue(0, result);
                }

                return overflow;
            } else {
                using T_OUT = typename NativeAndVectorType<OUT_ID>::type;
                using V_OUT = typename NativeAndVectorType<OUT_ID>::vector;
                T_OUT result{};

                if (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM) {
                    using T_MID = std::conditional_t<std::is_floating_point_v<T_IN>, double, int64_t>;
                    overflow = GenerateExpectedResultNumeric<AllMatchFilter, IN_ID, T_OUT, T_MID>(vvb, valueIndex,
                        maskIndex, SumFunc<T_IN, T_MID>, count, result);
                } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MIN) {
                    overflow = GenerateExpectedResultNumeric<AllMatchFilter, IN_ID, T_OUT, T_IN>(vvb, valueIndex,
                        maskIndex, MinFunc<T_IN, T_IN>, count, result);
                } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MAX) {
                    overflow = GenerateExpectedResultNumeric<AllMatchFilter, IN_ID, T_OUT, T_IN>(vvb, valueIndex,
                        maskIndex, MaxFunc<T_IN, T_IN>, count, result);
                } else {
                    throw OmniException("Invalid Arguement",
                        "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)));
                }

                static_cast<Vector<int64_t> *>((*expectedResult)->Get(1))->SetValue(0, count);
                if (overflow || count == 0) {
                    (*expectedResult)->Get(0)->SetNull(0);
                } else {
                    static_cast<V_OUT *>((*expectedResult)->Get(0))->SetValue(0, result);
                }

                return overflow;
            }
        }

        throw OmniException("Unreachable code", "Unreachable code");
    }

    bool GenerateFinalExpectedResult(VectorBatch **expectedResult, std::vector<VectorBatch *> &vvb) override
    {
        *expectedResult = new VectorBatch(1);
        BaseVector *v = DYNAMIC_TYPE_DISPATCH(VectorHelper::CreateFlatVector, OUT_ID, 1);
        (*expectedResult)->Append(v);
        (*expectedResult)->Append(new Vector<int64_t>(1));

        int64_t count = 0;
        for (VectorBatch *vb : vvb) {
            count += static_cast<Vector<int64_t> *>(vb->Get(1))->GetValue(0);
        }

        if constexpr (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CHAR) {
            this->GenerateVarcharResult(vvb, *expectedResult, false);
            static_cast<Vector<int64_t> *>((*expectedResult)->Get(1))->SetValue(0, count);
            VectorHelper::FreeVecBatches(vvb);
            return false;
        } else if constexpr (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CHAR) {
            VectorHelper::FreeVecBatches(vvb);
            throw OmniException("Invalid Arguement",
                "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)) + " for Varchar input");
        } else {
            using T_OUT = typename NativeAndVectorType<OUT_ID>::type;
            using V_OUT = typename NativeAndVectorType<OUT_ID>::vector;
            T_OUT result{};
            int64_t validCount = 0;
            bool overflow;
            const int32_t valueIndex = this->GetValueColumnIndex();
            // no mask for final aggregation
            const int32_t maskIndex = -1;

            if (TypeUtil::IsDecimalType(IN_ID) &&
                (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM || this->aggFunc == OMNI_AGGREGATION_TYPE_AVG)) {
                if (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM) {
                    overflow = GenerateExpectedResultNumeric<AllMatchFilter, OMNI_DECIMAL128, T_OUT, Decimal128>(vvb,
                        valueIndex, maskIndex, SumFunc<Decimal128, Decimal128>, validCount, result);
                } else {
                    Decimal128 result128{};
                    overflow = GenerateExpectedResultNumeric<AllMatchFilter, OMNI_DECIMAL128, Decimal128, Decimal128>(
                        vvb, valueIndex, maskIndex, SumFunc<Decimal128, Decimal128>, validCount, result128);
                    if (!overflow && validCount > 0) {
                        // generate actual average from some and count
                        Decimal128Wrapper wrapped = Decimal128Wrapper(result128).Divide(Decimal128Wrapper(count), 0);
                        overflow = !DoCast<Decimal128, T_OUT>(result, wrapped.ToDecimal128());
                    }
                }
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM) {
                using T_MID = std::conditional_t<std::is_floating_point_v<T_OUT>, double, int64_t>;
                overflow = GenerateExpectedResultNumeric<AllMatchFilter, OUT_ID, T_OUT, T_MID>(vvb, valueIndex,
                    maskIndex, SumFunc<T_OUT, T_MID>, validCount, result);
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_AVG) {
                double resultDouble{};
                overflow = GenerateExpectedResultNumeric<AllMatchFilter, OMNI_DOUBLE, double, double>(vvb, valueIndex,
                    maskIndex, SumFunc<double, double>, validCount, resultDouble);
                if (!overflow && validCount > 0) {
                    overflow = !DoCast<double, T_OUT>(result, resultDouble /= count);
                }
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MIN) {
                overflow = GenerateExpectedResultNumeric<AllMatchFilter, OUT_ID, T_OUT, T_OUT>(vvb, valueIndex,
                    maskIndex, MinFunc<T_OUT, T_OUT>, validCount, result);
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MAX) {
                overflow = GenerateExpectedResultNumeric<AllMatchFilter, OUT_ID, T_OUT, T_OUT>(vvb, valueIndex,
                    maskIndex, MaxFunc<T_OUT, T_OUT>, validCount, result);
            } else {
                VectorHelper::FreeVecBatches(vvb);
                throw OmniException("Invalid Arguement",
                    "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)));
            }

            static_cast<Vector<int64_t> *>((*expectedResult)->Get(1))->SetValue(0, count);
            if (overflow || validCount == 0) {
                (*expectedResult)->Get(0)->SetNull(0);
            } else {
                static_cast<V_OUT *>((*expectedResult)->Get(0))->SetValue(0, result);
            }

            VectorHelper::FreeVecBatches(vvb);
            return overflow;
        }

        throw OmniException("Unreachable code", "Unreachable code");
    }

    // functions to generate aggregator factories
    std::unique_ptr<OperatorFactory> CreatePartialFactory() override
    {
        std::vector<uint32_t> aggFuncVec;
        std::vector<DataTypePtr> inputTypeVec;
        std::vector<DataTypePtr> outputTypeVec;
        std::vector<uint32_t> aggColIdxVec;
        std::vector<uint32_t> aggMaskVec;
        this->SetPartialAggregateFactoryParams(aggFuncVec, inputTypeVec, outputTypeVec, aggColIdxVec, aggMaskVec);
        return this->CreateFactory(aggFuncVec, inputTypeVec, outputTypeVec, aggColIdxVec, aggMaskVec, true, true);
    }

    std::unique_ptr<OperatorFactory> CreateFinalFactory() override
    {
        std::vector<uint32_t> aggFuncVec;
        std::vector<DataTypePtr> inputTypeVec;
        std::vector<DataTypePtr> outputTypeVec;
        std::vector<uint32_t> aggColIdxVec;
        std::vector<uint32_t> aggMaskVec;
        if (!this->SetFinalAggregateFactoryParams(aggFuncVec, inputTypeVec, outputTypeVec, aggColIdxVec, aggMaskVec)) {
            return nullptr;
        } else {
            return this->CreateFactory(aggFuncVec, inputTypeVec, outputTypeVec, aggColIdxVec, aggMaskVec, false, false);
        }
    }

    std::string GetExpectedExceptionMessage() override
    {
        return nullWhenOverflow ? "" : "overflow";
    }

protected:
    virtual int32_t GetMaskColumnIndex()
    {
        return this->hasMask ? 1 : -1;
    }

    virtual int32_t GetNumberOfInputColumns()
    {
        return this->hasMask ? 2 : 1;
    }

    virtual std::unique_ptr<OperatorFactory> CreateFactory(std::vector<uint32_t> &aggFuncVec,
        std::vector<DataTypePtr> &inputTypeVec, std::vector<DataTypePtr> &outputTypeVec,
        std::vector<uint32_t> &aggColIdxVec, std::vector<uint32_t> &aggMaskVec, const bool inputRaw,
        const bool outputPartial)
    {
        DataTypes inputTypes(inputTypeVec);
        auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggColIdxVec);
        auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(outputTypeVec));
        auto inputRawWrap = std::vector<bool>(2, inputRaw);
        auto outputPartialWrap = std::vector<bool>(2, outputPartial);
        auto factory = std::make_unique<AggregationOperatorFactory>(inputTypes, aggFuncVec, aggInputColsWrap,
            aggMaskVec, aggOutputTypesWrap, inputRawWrap, outputPartialWrap, this->nullWhenOverflow);

        if (factory == nullptr) {
            return nullptr;
        }

        factory->Init();
        return factory;
    }

    void SetPartialAggregateFactoryParams(std::vector<uint32_t> &aggFuncVector,
        std::vector<DataTypePtr> &inputTypeVector, std::vector<DataTypePtr> &outputTypeVector,
        std::vector<uint32_t> &aggColIdxVector, std::vector<uint32_t> &aggMask)
    {
        aggFuncVector.resize(2);
        aggFuncVector[0] = static_cast<uint32_t>(this->aggFunc);
        aggFuncVector[1] = static_cast<uint32_t>(OMNI_AGGREGATION_TYPE_COUNT_COLUMN);

        aggMask.resize(2);
        if (this->hasMask) {
            aggMask[0] = this->GetMaskColumnIndex();
            aggMask[1] = this->GetMaskColumnIndex();
        } else {
            aggMask[0] = static_cast<uint32_t>(-1);
            aggMask[1] = static_cast<uint32_t>(-1);
        }

        inputTypeVector.resize(2);
        inputTypeVector[0] = GetType(IN_ID);
        inputTypeVector[1] = GetType(IN_ID);

        outputTypeVector.resize(2);
        outputTypeVector[1] = LongType();

        aggColIdxVector.resize(2);
        aggColIdxVector[0] = this->GetValueColumnIndex();
        aggColIdxVector[1] = this->GetValueColumnIndex();

        switch (this->aggFunc) {
            case OMNI_AGGREGATION_TYPE_SUM:
                outputTypeVector[0] =
                    TypeUtil::IsDecimalType(IN_ID) ? VarcharType(sizeof(DecimalPartialResult)) : GetType(OUT_ID);
                break;
            case OMNI_AGGREGATION_TYPE_AVG:
                if (TypeUtil::IsDecimalType(IN_ID)) {
                    outputTypeVector[0] = VarcharType(sizeof(DecimalPartialResult));
                } else {
                    std::vector<DataTypePtr> fieldTypes{ DoubleType(), LongType() };
                    outputTypeVector[0] = ContainerType(fieldTypes);
                }
                break;
            case OMNI_AGGREGATION_TYPE_MIN:
            case OMNI_AGGREGATION_TYPE_MAX:
                outputTypeVector[0] = GetType(OUT_ID);
                break;
            default:
                throw OmniException("Invalid Arguement",
                    "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)));
                break;
        }
    }

    bool SetFinalAggregateFactoryParams(std::vector<uint32_t> &aggFuncVector, std::vector<DataTypePtr> &inputTypeVector,
        std::vector<DataTypePtr> &outputTypeVector, std::vector<uint32_t> &aggColIdxVector,
        std::vector<uint32_t> &aggMask)
    {
        aggFuncVector.resize(2);
        aggFuncVector[0] = static_cast<uint32_t>(this->aggFunc);
        aggFuncVector[1] = static_cast<uint32_t>(OMNI_AGGREGATION_TYPE_COUNT_COLUMN);

        aggMask.resize(2);
        aggMask[0] = static_cast<uint32_t>(-1);
        aggMask[1] = static_cast<uint32_t>(-1);

        inputTypeVector.resize(2);
        inputTypeVector[1] = LongType();

        outputTypeVector.resize(2);
        outputTypeVector[0] = GetType(OUT_ID);
        outputTypeVector[1] = LongType();

        aggColIdxVector.resize(2);
        aggColIdxVector[0] = this->GetValueColumnIndex();
        aggColIdxVector[1] = aggColIdxVector[0] + 1;

        switch (this->aggFunc) {
            case OMNI_AGGREGATION_TYPE_SUM:
                inputTypeVector[0] =
                    TypeUtil::IsDecimalType(IN_ID) ? VarcharType(sizeof(DecimalPartialResult)) : GetType(OUT_ID);
                break;
            case OMNI_AGGREGATION_TYPE_AVG:
                if (TypeUtil::IsDecimalType(IN_ID)) {
                    inputTypeVector[0] = VarcharType(sizeof(DecimalPartialResult));
                } else {
                    std::vector<DataTypePtr> fieldTypes{ DoubleType(), LongType() };
                    inputTypeVector[0] = ContainerType(fieldTypes);
                }
                break;
            case OMNI_AGGREGATION_TYPE_MIN:
            case OMNI_AGGREGATION_TYPE_MAX:
                inputTypeVector[0] = GetType(OUT_ID);
                break;
            default:
                throw OmniException("Invalid Arguement",
                    "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)));
        }

        return true;
    }

    void GenerateVarcharResult(std::vector<VectorBatch *> &vvb, VectorBatch *expectedResult, const bool useMask)
    {
        uint8_t *result;
        int32_t resultLen = 0;
        int64_t count = 0;
        const int32_t maskIndex = useMask ? this->GetMaskColumnIndex() : -1;
        const int32_t valueIndex = this->GetValueColumnIndex();

        if (this->aggFunc == OMNI_AGGREGATION_TYPE_MIN) {
            result = GenerateExpectedResultVarchar<AllMatchFilter, MinCharFunc>(vvb, valueIndex, maskIndex, count,
                resultLen);
        } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MAX) {
            result = GenerateExpectedResultVarchar<AllMatchFilter, MaxCharFunc>(vvb, valueIndex, maskIndex, count,
                resultLen);
        } else {
            throw OmniException("Invalid Arguement",
                "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)) + " for Varchar input");
        }

        static_cast<Vector<int64_t> *>(expectedResult->Get(1))->SetValue(0, count);
        if (count > 0) {
            std::string_view str((char *)result, resultLen);
            static_cast<Vector<LargeStringContainer<std::string_view>> *>(expectedResult->Get(0))->SetValue(0, str);
        } else {
            static_cast<Vector<LargeStringContainer<std::string_view>> *>(expectedResult->Get(0))->SetNull(0);
        }
    }

    const FunctionType aggFunc;
    const int32_t nullPercent;
    const bool isDict;
    const bool hasMask;
    const bool nullWhenOverflow;

private:
    VectorBatch *InitializeExpectedPartialResult(std::vector<VectorBatch *> &vvb)
    {
        VectorBatch *expectedResult = new VectorBatch(1);

        if constexpr (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CHAR) {
            expectedResult->Append(new Vector<LargeStringContainer<std::string_view>>(1));
        } else {
            if (TypeUtil::IsDecimalType(IN_ID) &&
                (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM || this->aggFunc == OMNI_AGGREGATION_TYPE_AVG)) {
                expectedResult->Append(new Vector<Decimal128>(1));
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_AVG) {
                expectedResult->Append(new Vector<double>(1));
            } else {
                BaseVector *v = DYNAMIC_TYPE_DISPATCH(VectorHelper::CreateFlatVector, OUT_ID, 1);
                expectedResult->Append(v);
            }
        }
        expectedResult->Append(new Vector<int64_t>(1));

        return expectedResult;
    }
};
}
