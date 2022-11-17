#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2022. All rights reserved.
 */

#include "operator/aggregation/aggregator/all_aggregators.h"
#include "operator/aggregation/aggregator/aggregator_util.h"
#include "operator/aggregation/group_aggregation.h"
#include "operator/aggregation/non_group_aggregation.h"
#include "vector/vector_helper.h"

#include "../util/test_util.h"

namespace omniruntime {
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace omniruntime::type;

constexpr int32_t maxVarcharLength = 128;
constexpr int32_t vecBatchNum = 10;
constexpr int32_t rowSize = 2000;
inline static std::map<std::string, FunctionType> aggFuncs = {
    { "sum", OMNI_AGGREGATION_TYPE_SUM },
    { "avg", OMNI_AGGREGATION_TYPE_AVG },
    { "min", OMNI_AGGREGATION_TYPE_MIN },
    { "max", OMNI_AGGREGATION_TYPE_MAX }};

inline static std::map<DataTypeId, std::vector<DataTypeId>> unsupportedForSum {
    {OMNI_BOOLEAN,    {}},
    {OMNI_SHORT,      {OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_INT,        {OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_LONG,       {OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_DOUBLE,     {OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_DECIMAL64,  {OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_DECIMAL128, {OMNI_BOOLEAN, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_CHAR,       {}},
    {OMNI_VARCHAR,    {}}
};

inline static std::map<DataTypeId, std::vector<DataTypeId>> unsupportedForAvg {
    {OMNI_BOOLEAN,    {}},
    {OMNI_SHORT,      {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_INT,        {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_LONG,       {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_DOUBLE,     {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_DECIMAL64,  {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_DECIMAL128, {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_CHAR,       {}},
    {OMNI_VARCHAR,    {}}
};

inline static std::map<DataTypeId, std::vector<DataTypeId>> unsupportedForMin {
    {OMNI_BOOLEAN,    {OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_SHORT,      {OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_INT,        {OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_LONG,       {OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_DOUBLE,     {OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_DECIMAL64,  {OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_DECIMAL128, {OMNI_CHAR, OMNI_VARCHAR}},
    {OMNI_CHAR,       {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_DECIMAL64, OMNI_DECIMAL128, OMNI_VARCHAR}},
    {OMNI_VARCHAR,    {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_DECIMAL64, OMNI_DECIMAL128, OMNI_CHAR}}
};

inline static std::map<std::string, std::map<DataTypeId, std::vector<DataTypeId>>> unsupported {
    { "sum", unsupportedForSum },
    { "avg", unsupportedForAvg },
    { "min", unsupportedForMin },
    { "max", unsupportedForMin }
};

inline bool CheckSupported(const std::string &aggFuncName, const DataTypeId inId, const DataTypeId outId)
{
    std::vector<DataTypeId> &unsupportedOutputs = unsupported[aggFuncName][inId];
    if (unsupportedOutputs.size() == 0) {
        return false;
    }
    return std::find(unsupportedOutputs.begin(), unsupportedOutputs.end(), outId) == unsupportedOutputs.end();
}

inline void PrintNotMatchBatches(VectorBatch *outputPages, VectorBatch *expectPage)
{
    printf("================ Expected Vector Batch ==================\n");
    VectorHelper::PrintVecBatch(expectPage);
    printf("================= Result Vector Batch ===================\n");
    VectorHelper::PrintVecBatch(outputPages);
}

template <typename D, typename V>
inline bool CompareUnorderedRows(Vector *resultVector, Vector *expectedVector, const double error) {
    std::multiset<D> resRows;
    std::multiset<D> expectedRows;
    size_t resNullCount = 0;
    size_t expNullCount = 0;
    for (int32_t i = 0; i < resultVector->GetSize(); ++i) {
        auto leftVector = static_cast<V*>(resultVector);
        auto rightVector = static_cast<V*>(expectedVector);
        if (leftVector->IsValueNull(i)) {
            resNullCount++;
        } else {
            resRows.emplace(leftVector->GetValue(i));
        }
        if (rightVector->IsValueNull(i)) {
            expNullCount++;
        } else {
            expectedRows.emplace(rightVector->GetValue(i));
        }
    }

    if (resNullCount != expNullCount) {
        return false;
    }

    if (resRows.size() != expectedRows.size()) {
        return false;
    }

    auto it1 = resRows.begin();
    auto it2 = expectedRows.begin();
    for (; it1 != resRows.end(); ++it1, ++it2) {
        if constexpr (std::is_same_v<D, double>) {
            if (fabs(*it1 - *it2) > error) {
                return false;
            }
        } else if constexpr (std::is_same_v<D, Decimal128>) {
            Decimal128Wrapper left(*it1);
            Decimal128Wrapper right(*it2);
            if (left.Subtract(right).Abs() > Decimal128Wrapper(static_cast<int64_t>(error))) {
                return false;
            }
        } else {
            if (abs(*it1 - *it2) > static_cast<D>(error)) {
                return false;
            }
        }
    }

    return true;
}

inline bool CompareUnorderedStringRows(VarcharVector *resultVector, VarcharVector *expectedVector) {
    size_t rowCount = resultVector->GetSize();
    std::multiset<std::string> resRows;
    std::multiset<std::string> expectedRows;
    size_t resNullCount = 0;
    size_t expNullCount = 0;

    for (size_t i = 0; i < rowCount; ++i) {
        if (resultVector->IsValueNull(i)) {
            resNullCount++;
        } else {
            uint8_t *leftCharPtr = nullptr;
            int32_t leftLen = resultVector->GetValue(i, &leftCharPtr);
            std::string leftStr(reinterpret_cast<char *>(leftCharPtr), leftLen);
            resRows.emplace(leftStr);
        }

        if (expectedVector->IsValueNull(i)) {
            expNullCount++;
        } else {
            uint8_t *rightCharPtr = nullptr;
            int32_t rightLen = expectedVector->GetValue(i, &rightCharPtr);
            std::string rightStr(reinterpret_cast<char *>(rightCharPtr), rightLen);
            expectedRows.emplace(rightStr);
        }
    }

    return resRows == expectedRows && resNullCount == expNullCount;
}

inline bool ColumnMatchIgnoreOrder(Vector *resultVector, Vector *expectedVector, const double error)
{
    auto resType = resultVector->GetTypeId();
    bool isMatched = true;
    switch (resType) {
        case OMNI_INT:
        case OMNI_DATE32: {
            isMatched = CompareUnorderedRows<int32_t, IntVector>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_SHORT: {
            isMatched = CompareUnorderedRows<int16_t, ShortVector>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_DOUBLE: {
            isMatched = CompareUnorderedRows<double, DoubleVector>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_LONG:
        case OMNI_DECIMAL64: {
            isMatched = CompareUnorderedRows<int64_t, LongVector>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_BOOLEAN: {
            isMatched = CompareUnorderedRows<bool, BooleanVector>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_DECIMAL128: {
            isMatched = CompareUnorderedRows<Decimal128, Decimal128Vector>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_CHAR:
        case OMNI_VARCHAR: {
            isMatched = CompareUnorderedStringRows(
                static_cast<VarcharVector*>(resultVector), static_cast<VarcharVector*>(expectedVector));
            break;
        }
        case OMNI_CONTAINER: {
            int32_t fieldCount = static_cast<ContainerVector *>(resultVector)->GetVectorCount();
            for (int32_t colIdx = 0; colIdx < fieldCount; colIdx++) {
                auto *actualFieldCol =
                        reinterpret_cast<Vector *>(static_cast<ContainerVector *>(resultVector)->GetValue(colIdx));
                auto *expectFieldCol =
                        reinterpret_cast<Vector *>(static_cast<ContainerVector *>(expectedVector)->GetValue(colIdx));
                isMatched = ColumnMatchIgnoreOrder(actualFieldCol, expectFieldCol, error);
                if (!isMatched) {
                    break;
                }
            }
            break;
        }
        default: {
            return false;
        }
    }
    return isMatched;
}

inline bool VecBatchMatchIgnoreOrder(VectorBatch *resultBatch, VectorBatch *expectedBatch, const double error)
{
    if (resultBatch->GetRowCount() != expectedBatch->GetRowCount()) {
        printf("Invalid row count. Expected=%d, actual=%d\n", expectedBatch->GetRowCount(), resultBatch->GetRowCount());
        PrintNotMatchBatches(resultBatch, expectedBatch);
        return false;
    }

    int32_t columnNumber = resultBatch->GetVectorCount();
    if (columnNumber != expectedBatch->GetVectorCount()) {
        printf("Invalid vector count. Expected=%d, actual=%d\n",
            expectedBatch->GetVectorCount(), resultBatch->GetVectorCount());
        PrintNotMatchBatches(resultBatch, expectedBatch);
        return false;
    }
    for (int32_t i = 0; i < columnNumber; ++i) {
        if (resultBatch->GetVector(i)->GetEncoding() != expectedBatch->GetVector(i)->GetEncoding()) {
            printf("Encoding of column %d not match\n", i);
            PrintNotMatchBatches(resultBatch, expectedBatch);
            return false;
        }
    }

    for (int32_t i = 0; i < columnNumber; i++) {
        if (resultBatch->GetVectorTypeIds()[i] != expectedBatch->GetVectorTypeIds()[i]) {
            printf("Column %d types do not match\n", i);
            PrintNotMatchBatches(resultBatch, expectedBatch);
            return false;
        }
    }

    // validate data
    if (!ColumnMatchIgnoreOrder(resultBatch->GetVector(0), expectedBatch->GetVector(0), error)) {
        printf("Vector 0 (data vector) not matched\n");
        PrintNotMatchBatches(resultBatch, expectedBatch);
        return false;
    }

    //validate count
    if (!ColumnMatchIgnoreOrder(resultBatch->GetVector(1), expectedBatch->GetVector(1), 0)) {
        printf("Vector 1 (count vector) not matched\n");
        PrintNotMatchBatches(resultBatch, expectedBatch);
        return false;
    }

    return true;
}

template <typename IN, typename OUT>
inline bool doCast(OUT &result, const IN &cur_)
{
    if constexpr (std::is_same_v<IN, Decimal128>) {
        if constexpr (std::is_same_v<OUT, Decimal128>) {
            result = cur_;
        } else {
            Decimal128Wrapper wrapped(cur_);
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
            result = Decimal128Wrapper(static_cast<int64_t>(round(cur_))).ToDecimal128();
        } else {
            result = Decimal128Wrapper(static_cast<int64_t>(cur_)).ToDecimal128();
        }
    } else {
        if constexpr (std::is_floating_point_v<IN>) {
            if constexpr (std::is_floating_point_v<OUT>) {
                result = static_cast<OUT>(cur_);
            } else {
                IN integral = round(cur_);
                result = static_cast<OUT>(integral);
                if (static_cast<IN>(result) != integral) {
                    return false;
                }
            }
        } else {
            result = static_cast<OUT>(cur_);
            if constexpr (!std::is_floating_point_v<OUT>) {
                if (static_cast<IN>(result) != cur_) {
                    return false;
                }
            }
        }
    }

    return true;
}

template <typename IN, typename OUT>
inline bool minFunc(OUT &result, const IN &cur, const bool isFirst)
{
    OUT casted {};
    if (!doCast(casted, cur)) {
        return false;
    }

    if (isFirst || result > casted) {
        result = casted;
    }
    return true;
}

template <typename IN, typename OUT>
inline bool maxFunc(OUT &result, const IN &cur, const bool isFirst)
{
    OUT casted {};
    if (!doCast(casted, cur)) {
        return false;
    }

    if (isFirst || result < casted) {
        result = casted;
    }
    return true;
}

template <typename IN, typename OUT>
inline bool sumFunc(OUT &result, const IN &cur, const bool isFirst)
{
    OUT casted {};
    if (!doCast(casted, cur)) {
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

inline uint8_t *minCharFunc(uint8_t *res, int32_t &len, uint8_t *curVal, const int32_t curLen)
{
    auto cmpResult = memcmp(res, curVal, std::min(len, curLen));
    if (cmpResult > 0 || (cmpResult == 0 && len > curLen)) {
        len = curLen;
        return curVal;
    } else {
        return res;
    }
}

inline uint8_t *maxCharFunc(uint8_t *res, int32_t &len, uint8_t *curVal, const int32_t curLen)
{
    auto cmpResult = memcmp(res, curVal, std::min(len, curLen));
    if (cmpResult < 0 || (cmpResult == 0 && len < curLen)) {
        len = curLen;
        return curVal;
    } else {
        return res;
    }
}

template <DataTypeId TYPE_ID>
inline Vector *CreateFixedSizeVector(VectorAllocator *vectorAllocator, const int32_t nRows,
    const int32_t nullPercent, const int32_t range, const bool isDict)
{
    using V = typename NativeAndVectorType<TYPE_ID>::vector;
    using T = typename NativeAndVectorType<TYPE_ID>::type;
    double v;

    if (isDict) {
        int32_t valueStartIdx = nullPercent > 0 ? 1 : 0;
        V *vector = new V(vectorAllocator, 256 + valueStartIdx);
        if (nullPercent > 0) {
            vector->SetValueNull(0);
        }

        std::set<double> dictValues;
        dictValues.insert(0);
        if constexpr (std::is_same_v<V, Decimal128Vector>) {
            vector->SetValue(valueStartIdx, Decimal128Wrapper(static_cast<int64_t>(0)).ToDecimal128());
        } else {
            vector->SetValue(valueStartIdx, static_cast<T>(0));
        }
        for (int32_t i = valueStartIdx + 1; i < vector->GetSize(); ++i) {
            do {
                v = (rand() % range) - ((4 * range) / 10) + ((rand() % 100) / 100.0);
            } while (!dictValues.insert(v).second);

            if constexpr (std::is_same_v<V, Decimal128Vector>) {
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
                if constexpr (std::is_same_v<V, Decimal128Vector>) {
                    vector->SetValue(i, Decimal128Wrapper(static_cast<int64_t>(v * 100)).ToDecimal128());
                } else {
                    vector->SetValue(i, static_cast<T>(v));
                }
            }
        }
        return vector;
    }
}

inline Vector *CreateVarcharVector(VectorAllocator *vectorAllocator, const int32_t nRows,
    const int32_t nullPercent, const int32_t range, const bool isDict)
{
    uint8_t charBuffer[maxVarcharLength];
    if (isDict) {
        int32_t valueStartIdx = nullPercent > 0 ? 1 : 0;
        VarcharVector *vector = new VarcharVector(
            vectorAllocator, maxVarcharLength * (range + valueStartIdx), range + valueStartIdx);
        if (nullPercent > 0) {
            vector->SetValueNull(0);
        }

        for (int32_t r = valueStartIdx; r < vector->GetSize(); ++r) {
            const auto valLen = (rand() % (maxVarcharLength - 1)) + 1;
            for (int32_t i = 0; i < valLen; ++i) {
                charBuffer[i] = rand() % 95 + 32;
            }
            vector->SetValue(r, charBuffer, valLen);
        }

        int32_t ids[nRows];
        for (int32_t i = 0; i < nRows; ++i) {
            if ((rand() % 100) < nullPercent) {
                ids[i] = 0;
            } else {
                ids[i] = (rand() % range) + valueStartIdx;
            }
        }
        auto dictVector = new DictionaryVector(vector, ids, nRows);
        delete vector;
        return dictVector;
    } else {
        VarcharVector *vector = new VarcharVector(vectorAllocator, maxVarcharLength * nRows, nRows);
        for (int32_t r = 0; r < nRows; ++r) {
            if ((rand() % 100) < nullPercent) {
                vector->SetValueNull(r);
            } else {
                const auto valLen = (rand() % (maxVarcharLength - 1)) + 1;
                for (int32_t i = 0; i < valLen; ++i) {
                    charBuffer[i] = rand() % 95 + 32;
                }
                vector->SetValue(r, charBuffer, valLen);
            }
        }
        return vector;
    }
}

inline bool ValidateOverflow(const std::string &stageName, const int32_t valueColIdx,
    VectorBatch *expectedResult, VectorBatch *actualResult)
{
    std::set<int32_t> matchRows;
    for (int32_t i = 0; i < expectedResult->GetRowCount(); ++i) {
        if (expectedResult->GetVector(valueColIdx)->IsValueNull(i)) {
            int64_t expectedCount = static_cast<LongVector *>(expectedResult->GetVector(valueColIdx + 1))->GetValue(i);
            bool found = false;
            for (int32_t j = 0; j < expectedResult->GetRowCount(); ++j) {
                if (matchRows.find(j) != matchRows.end()) {
                    continue;
                }
                if (expectedCount == static_cast<LongVector *>(actualResult->GetVector(valueColIdx + 1))->GetValue(j)
                    && actualResult->GetVector(valueColIdx)->IsValueNull(j)) {
                        matchRows.insert(j);
                    found = true;
                    break;
                }
            }
            if (!found) {
                printf("ERROR: no raws in result matches expected row %d\n", i);
                printf("================== Expected Result (%s) ==================\n", stageName.c_str());
                VectorHelper::PrintVecBatch(expectedResult);
                printf("================== Actual Result (%s) ==================\n", stageName.c_str());
                VectorHelper::PrintVecBatch(actualResult);
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
        VectorHelper::PrintVecBatch(vvb[i]);
    }
}

inline  DataTypePtr GetType(DataTypeId typeId)
{
    switch (typeId) {
        case OMNI_BOOLEAN:
            return BooleanType();
        case OMNI_SHORT:
            return ShortType();
        case OMNI_INT:
            return IntType();
        case OMNI_LONG:
            return LongType();
        case OMNI_DOUBLE:
            return DoubleType();
        case OMNI_DECIMAL64:
            return Decimal64Type();
        case OMNI_DECIMAL128:
            return Decimal128Type();
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            return VarcharType(maxVarcharLength);
        default:
            throw OmniException("Invalid Argument", "Invalid type " + TypeUtil::TypeToString(typeId));
    }
}

inline bool allMatchFilter(const int32_t *filterValue, VectorBatch *vb, const int32_t colIdx, const int32_t rowIdx) {
    return true;
}

inline bool groupByFilter(const int32_t *filterValue, VectorBatch *vb, const int32_t colIdx, const int32_t rowIdx) {
    Vector *v = vb->GetVector(colIdx);
    if (filterValue == nullptr) {
        return v->IsValueNull(rowIdx);
    } else if (v->IsValueNull(rowIdx)) {
        return false;
    } else {
        int32_t orgRowIdx;
        IntVector *orgVector = static_cast<IntVector *>(VectorHelper::ExpandVectorAndIndex(v, rowIdx, orgRowIdx));
        return orgVector->GetValue(orgRowIdx) == *filterValue;
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

template <bool (FilterFunc)(const int32_t *, VectorBatch *, const int32_t, const int32_t),
    uint8_t * (UpdateFunc)(uint8_t *, int32_t &, uint8_t *, const int32_t)>
uint8_t *GenerateExpectedResultVarchar(
    std::vector<VectorBatch *> &vvb, const int32_t valueIndex, const int32_t maskIndex,
    int64_t &count, int32_t &resultLen, 
    const int32_t *filterValue = nullptr, const int32_t groupbyColIdx = -1)
{
    count = 0;
    uint8_t *result = nullptr;
    resultLen = 0;

    int32_t orgIdx;
    VarcharVector *orgVector;
    bool valid;

    for (VectorBatch *vb : vvb) {
        Vector *maskVector = maskIndex < 0 ? nullptr : vb->GetVector(maskIndex);
        Vector *vector = vb->GetVector(valueIndex);

        for (int32_t i = 0; i < vector->GetSize(); ++i) {
            valid = true;
            if (maskVector != nullptr) {
                if (maskVector->IsValueNull(i)) {
                    valid = false;
                } else {
                    BooleanVector *v = static_cast<BooleanVector *>(
                        VectorHelper::ExpandVectorAndIndex(maskVector, i, orgIdx));
                    valid = v->GetValue(orgIdx);
                }
            }
            valid &= !vector->IsValueNull(i) & FilterFunc(filterValue, vb, groupbyColIdx, i);

            if (valid) {
                orgVector = static_cast<VarcharVector *>(VectorHelper::ExpandVectorAndIndex(vector, i, orgIdx));
                uint8_t *curVal = nullptr;
                int32_t curLen = orgVector->GetValue(orgIdx, &curVal);
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

template <bool (FilterFunc)(const int32_t *, VectorBatch *, const int32_t, const int32_t),
    DataTypeId IN, typename T_OUT, typename ResultType, typename UpdateFunc>
bool GenerateExpectedResultNumeric(
    std::vector<VectorBatch *> &vvb, const int32_t valueIndex, const int32_t maskIndex,
    UpdateFunc updateFunc, int64_t &count, T_OUT &result,
    const int32_t *filterValue = nullptr, const int32_t groupbyColIdx = -1)
{
    using V_IN = typename NativeAndVectorType<IN>::vector;
    
    count = 0;

    int32_t orgIdx;
    Vector *orgVector;
    bool valid;
    ResultType midRes {};
    bool overflow = false;

    for (VectorBatch *vb : vvb) {
        Vector *maskVector = maskIndex < 0 ? nullptr : vb->GetVector(maskIndex);
        Vector *vector = vb->GetVector(valueIndex);

        for (int32_t i = 0; i < vector->GetSize(); ++i) {
            valid = true;
            if (maskVector != nullptr) {
                if (maskVector->IsValueNull(i)) {
                    valid = false;
                } else {
                    BooleanVector *v = static_cast<BooleanVector *>(
                        VectorHelper::ExpandVectorAndIndex(maskVector, i, orgIdx));
                    valid = v->GetValue(orgIdx);
                }
            }
            valid &= !vector->IsValueNull(i) & FilterFunc(filterValue, vb, groupbyColIdx, i);

            if (valid) {
                if (overflow) {
                    count++;
                    continue;
                }

                orgVector = VectorHelper::ExpandVectorAndIndex(vector, i, orgIdx);
                overflow |= !updateFunc(midRes, static_cast<V_IN *>(orgVector)->GetValue(orgIdx), count == 0);
                count++;
            }
        }
    }

    if (!overflow && count > 0) {
        overflow = !doCast<ResultType, T_OUT>(result, midRes);
    }

    return overflow;
}

template <DataTypeId IN_ID, DataTypeId OUT_ID>
class AggregatorTesterTemplate : public AggregatorTester {
public:
    AggregatorTesterTemplate(const std::string aggFuncName_, const int32_t nullPercent_,
        const bool isDict_, const bool hasMask_, const bool nullWhenOverflow_) :
        aggFunc(aggFuncs[aggFuncName_]),
        nullPercent(nullPercent_),
        isDict(isDict_),
        hasMask(hasMask_),
        nullWhenOverflow(nullWhenOverflow_)
    {
        vectorAllocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator(
            "AggregatorCompleteTest_" + aggFuncName_ + "_"
                + TypeUtil::TypeToString(IN_ID) + "_"
                + TypeUtil::TypeToString(OUT_ID) + "_"
                + std::to_string(nullPercent_) + "_"
                + (isDict_ ? "dict_" : "flat_")
                + (hasMask_ ? "withMask_" : "noMask_")
                + (nullWhenOverflow_ ? "overflowNull_" : "overflowExcep_") 
                + "noGroupBy");
    }

    virtual ~AggregatorTesterTemplate() override
    {
        delete vectorAllocator;
    }

    virtual int32_t GetValueColumnIndex()
    {
        return 0;
    }

    // functions to build input data and generate expected results
    virtual std::vector<VectorBatch *> BuildAggInput(const int32_t vecBatchNum, const int32_t rowPerVecBatch) override
    {
        std::vector<VectorBatch *> input(vecBatchNum);

        for (int32_t i = 0; i < vecBatchNum; ++i) {
            VectorBatch *vecBatch = new VectorBatch(this->GetNumberOfInputColumns());
            Vector *vector = nullptr;

            switch (IN_ID) {
                case OMNI_BOOLEAN:
                    vector = CreateFixedSizeVector<OMNI_BOOLEAN>(
                        this->vectorAllocator, rowPerVecBatch, this->nullPercent, 256, this->isDict);
                    break;
                case OMNI_SHORT:
                    vector = CreateFixedSizeVector<OMNI_SHORT>(
                        this->vectorAllocator, rowPerVecBatch, this->nullPercent, 256 * 256, this->isDict);
                    break;
                case OMNI_INT:
                    vector = CreateFixedSizeVector<OMNI_INT>(
                        this->vectorAllocator, rowPerVecBatch, this->nullPercent, 256 * 256 * 256, this->isDict);
                    break;
                case OMNI_DECIMAL64:
                case OMNI_LONG:
                    vector = CreateFixedSizeVector<OMNI_LONG>(
                        this->vectorAllocator, rowPerVecBatch, this->nullPercent, 256 * 256 * 256, this->isDict);
                    break;
                case OMNI_DOUBLE:
                    vector = CreateFixedSizeVector<OMNI_DOUBLE>(
                        this->vectorAllocator, rowPerVecBatch, this->nullPercent, 256 * 256 * 256, this->isDict);
                    break;
                case OMNI_DECIMAL128:
                    vector = CreateFixedSizeVector<OMNI_DECIMAL128>(
                        this->vectorAllocator, rowPerVecBatch, this->nullPercent, 256 * 256 * 256, this->isDict);
                    break;
                case OMNI_VARCHAR:
                case OMNI_CHAR:
                    vector = CreateVarcharVector(
                        this->vectorAllocator, rowPerVecBatch, this->nullPercent, 256, this->isDict);
                    break;
                default:
                    throw OmniException("Invalid Arguement", "Unexpected input type " + TypeUtil::TypeToString(IN_ID));
            }
            vecBatch->SetVector(this->GetValueColumnIndex(), vector);

            if (this->hasMask) {
                Vector *maskVector = CreateFixedSizeVector<OMNI_BOOLEAN>(
                        this->vectorAllocator, rowPerVecBatch, this->nullPercent, 256, this->isDict);;
                vecBatch->SetVector(this->GetMaskColumnIndex(), maskVector);
            }

            input[i] = vecBatch;
        }

        return input;
    }

    virtual bool GeneratePartialExpectedResult(VectorBatch **expectedResult, std::vector<VectorBatch *> &vvb) override
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

            if (TypeUtil::IsDecimalType(IN_ID) 
                && (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM || this->aggFunc == OMNI_AGGREGATION_TYPE_AVG)) {

                Decimal128 result {};
                overflow = GenerateExpectedResultNumeric<allMatchFilter, IN_ID, Decimal128, Decimal128>(
                    vvb, valueIndex, maskIndex, sumFunc<T_IN, Decimal128>, count, result);

                static_cast<LongVector *>((*expectedResult)->GetVector(1))->SetValue(0, count);
                if (overflow || count == 0) {
                    (*expectedResult)->GetVector(0)->SetValueNull(0);
                } else {
                    static_cast<Decimal128Vector *>((*expectedResult)->GetVector(0))->SetValue(0, result);
                }

                return overflow;
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_AVG) {
                double result {};
                overflow = GenerateExpectedResultNumeric<allMatchFilter, IN_ID, double, double>(
                    vvb, valueIndex, maskIndex, sumFunc<T_IN, double>, count, result);

                static_cast<LongVector *>((*expectedResult)->GetVector(1))->SetValue(0, count);
                if (overflow || count == 0) {
                    (*expectedResult)->GetVector(0)->SetValueNull(0);
                } else {
                    static_cast<DoubleVector *>((*expectedResult)->GetVector(0))->SetValue(0, result);
                }

                return overflow;
            } else  {
                using T_OUT = typename NativeAndVectorType<OUT_ID>::type;
                using V_OUT = typename NativeAndVectorType<OUT_ID>::vector;
                T_OUT result {};

                if (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM) {
                    using T_MID = std::conditional_t<std::is_floating_point_v<T_IN>, double, int64_t>;
                    overflow = GenerateExpectedResultNumeric<allMatchFilter, IN_ID, T_OUT, T_MID>(
                        vvb, valueIndex, maskIndex, sumFunc<T_IN, T_MID>, count, result);
                } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MIN) {
                    overflow = GenerateExpectedResultNumeric<allMatchFilter, IN_ID, T_OUT, T_IN>(
                        vvb, valueIndex, maskIndex, minFunc<T_IN, T_IN>, count, result);
                } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MAX) {
                    overflow = GenerateExpectedResultNumeric<allMatchFilter, IN_ID, T_OUT, T_IN>(
                        vvb, valueIndex, maskIndex, maxFunc<T_IN, T_IN>, count, result);
                } else {
                    throw OmniException("Invalid Arguement",
                        "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)));
                }

                static_cast<LongVector *>((*expectedResult)->GetVector(1))->SetValue(0, count);
                if (overflow || count == 0) {
                    (*expectedResult)->GetVector(0)->SetValueNull(0);
                } else {
                    static_cast<V_OUT *>((*expectedResult)->GetVector(0))->SetValue(0, result);
                }

                return overflow;
            }
        }

        throw OmniException("Unreachable code", "Unreachable code");
    }

    virtual bool GenerateFinalExpectedResult(VectorBatch **expectedResult, std::vector<VectorBatch *> &vvb) override
    {
        *expectedResult = new VectorBatch(2);
        (*expectedResult)->SetVector(0, VectorHelper::CreateFlatVector(
            this->vectorAllocator, OUT_ID, maxVarcharLength, 1));
        (*expectedResult)->SetVector(1, new LongVector(this->vectorAllocator, 1));

        int64_t count = 0;
        for (VectorBatch *vb : vvb) {
            count += static_cast<LongVector *>(vb->GetVector(1))->GetValue(0);
        }

        if constexpr (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CHAR) {
            this->GenerateVarcharResult(vvb, *expectedResult, false);
            static_cast<LongVector *>((*expectedResult)->GetVector(1))->SetValue(0, count);
            VectorHelper::FreeVecBatches(vvb);
            return false;
        } else if constexpr (OUT_ID == OMNI_VARCHAR || OUT_ID == OMNI_CHAR) {
            VectorHelper::FreeVecBatches(vvb);
            throw OmniException("Invalid Arguement",
                "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)) + " for Varchar input");
        } else {
            using T_OUT = typename NativeAndVectorType<OUT_ID>::type;
            using V_OUT = typename NativeAndVectorType<OUT_ID>::vector;
            T_OUT result {};
            int64_t validCount = 0;
            bool overflow;
            const int32_t valueIndex = this->GetValueColumnIndex();
            // no mask for final aggregation
            const int32_t maskIndex = -1;

            if (TypeUtil::IsDecimalType(IN_ID) 
                && (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM || this->aggFunc == OMNI_AGGREGATION_TYPE_AVG)) {
                
                if (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM) {
                    overflow = GenerateExpectedResultNumeric<allMatchFilter, OMNI_DECIMAL128, T_OUT, Decimal128>(
                        vvb, valueIndex, maskIndex, sumFunc<Decimal128, Decimal128>, validCount, result);
                } else {
                    Decimal128 result128 {};
                    overflow = GenerateExpectedResultNumeric<allMatchFilter, OMNI_DECIMAL128, Decimal128, Decimal128>(
                        vvb, valueIndex, maskIndex, sumFunc<Decimal128, Decimal128>, validCount, result128);
                    if (!overflow && validCount > 0) {
                        // generate actual average from some and count
                        Decimal128Wrapper wrapped = Decimal128Wrapper(result128).Divide(Decimal128Wrapper(count), 0);
                        overflow = !doCast<Decimal128, T_OUT>(result, wrapped.ToDecimal128());
                    }
                }
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM) {
                using T_MID = std::conditional_t<std::is_floating_point_v<T_OUT>, double, int64_t>;
                overflow = GenerateExpectedResultNumeric<allMatchFilter, OUT_ID, T_OUT, T_MID>(
                    vvb, valueIndex, maskIndex, sumFunc<T_OUT, T_MID>, validCount, result);
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_AVG) {
                double resultDouble {};
                overflow = GenerateExpectedResultNumeric<allMatchFilter, OMNI_DOUBLE, double, double>(
                    vvb, valueIndex, maskIndex, sumFunc<double, double>, validCount, resultDouble);
                if (!overflow && validCount > 0) {
                    overflow = !doCast<double, T_OUT>(result, resultDouble /= count);
                }
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MIN) {
                overflow = GenerateExpectedResultNumeric<allMatchFilter, OUT_ID, T_OUT, T_OUT>(
                    vvb, valueIndex, maskIndex, minFunc<T_OUT, T_OUT>, validCount, result);
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MAX) {
                overflow = GenerateExpectedResultNumeric<allMatchFilter, OUT_ID, T_OUT, T_OUT>(
                    vvb, valueIndex, maskIndex, maxFunc<T_OUT, T_OUT>, validCount, result);
            } else {
                VectorHelper::FreeVecBatches(vvb);
                throw OmniException("Invalid Arguement",
                    "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)));
            }

            static_cast<LongVector *>((*expectedResult)->GetVector(1))->SetValue(0, count);
            if (overflow || validCount == 0) {
                (*expectedResult)->GetVector(0)->SetValueNull(0);
            } else {
                static_cast<V_OUT *>((*expectedResult)->GetVector(0))->SetValue(0, result);
            }

            VectorHelper::FreeVecBatches(vvb);
            return overflow;
        }

        throw OmniException("Unreachable code", "Unreachable code");
    }

    // functions to generate aggregator factories
    virtual std::unique_ptr<OperatorFactory> CreatePartialFactory() override
    {
        std::vector<uint32_t> aggFuncVec;
        std::vector<DataTypePtr> inputTypeVec;
        std::vector<DataTypePtr> outputTypeVec;
        std::vector<uint32_t> aggColIdxVec;
        std::vector<uint32_t> aggMaskVec;
        this->SetPartialAggregateFactoryParams(aggFuncVec, inputTypeVec, outputTypeVec, aggColIdxVec, aggMaskVec);
        return this->CreateFactory(aggFuncVec, inputTypeVec, outputTypeVec, aggColIdxVec, aggMaskVec, true, true);
    }

    virtual std::unique_ptr<OperatorFactory> CreateFinalFactory() override
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
        std::vector<uint32_t> &aggColIdxVec, std::vector<uint32_t> &aggMaskVec,
        const bool inputRaw, const bool outputPartial)
    {
        DataTypes inputTypes(inputTypeVec);
        auto aggInputColsWrap = AggregatorUtil::WrapWithVector(aggColIdxVec);
        auto aggOutputTypesWrap = AggregatorUtil::WrapWithVector(DataTypes(outputTypeVec));
        auto inputRawWrap = AggregatorUtil::WrapWithVector(inputRaw, 2);
        auto outputPartialWrap = AggregatorUtil::WrapWithVector(outputPartial, 2);
        auto factory = std::make_unique<AggregationOperatorFactory>(
            inputTypes, aggFuncVec, aggInputColsWrap, aggMaskVec, aggOutputTypesWrap,
            inputRawWrap, outputPartialWrap, this->nullWhenOverflow);

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
                outputTypeVector[0] = TypeUtil::IsDecimalType(IN_ID)
                    ? VarcharType(sizeof(DecimalPartialResult))
                    : GetType(OUT_ID);
                break;
            case OMNI_AGGREGATION_TYPE_AVG:
                if (TypeUtil::IsDecimalType(IN_ID)) {
                    outputTypeVector[0] = VarcharType(sizeof(DecimalPartialResult));
                } else {
                    std::vector<DataTypePtr> fieldTypes {DoubleType(), LongType()};
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

    bool SetFinalAggregateFactoryParams(std::vector<uint32_t> &aggFuncVector,
        std::vector<DataTypePtr> &inputTypeVector, std::vector<DataTypePtr> &outputTypeVector,
        std::vector<uint32_t> &aggColIdxVector, std::vector<uint32_t> &aggMask)
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
                inputTypeVector[0] = TypeUtil::IsDecimalType(IN_ID)
                    ? VarcharType(sizeof(DecimalPartialResult))
                    : GetType(OUT_ID);
                break;
            case OMNI_AGGREGATION_TYPE_AVG:
                if (TypeUtil::IsDecimalType(IN_ID)) {
                    inputTypeVector[0] = VarcharType(sizeof(DecimalPartialResult));
                } else {
                    std::vector<DataTypePtr> fieldTypes {DoubleType(), LongType()};
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
            result = GenerateExpectedResultVarchar<allMatchFilter, minCharFunc>(
                vvb, valueIndex, maskIndex, count, resultLen);
        } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_MAX) {
            result = GenerateExpectedResultVarchar<allMatchFilter, maxCharFunc>(
                vvb, valueIndex, maskIndex, count, resultLen);
        } else {
            throw OmniException("Invalid Arguement",
                "Invalid aggregation type " + std::to_string(as_integer(this->aggFunc)) + " for Varchar input");
        }

        static_cast<LongVector *>(expectedResult->GetVector(1))->SetValue(0, count);
        if (count > 0) {
            static_cast<VarcharVector *>(expectedResult->GetVector(0))->SetValue(0, result, resultLen);
        } else {
            static_cast<VarcharVector *>(expectedResult->GetVector(0))->SetValueNull(0);
        }
    }

    VectorAllocator *vectorAllocator;
    const FunctionType aggFunc;
    const int32_t nullPercent;
    const bool isDict;
    const bool hasMask;
    const bool nullWhenOverflow;

private:
    VectorBatch *InitializeExpectedPartialResult(std::vector<VectorBatch *> &vvb)
    {
        VectorBatch *expectedResult = new VectorBatch(2);
        expectedResult->SetVector(1, new LongVector(this->vectorAllocator, 1));

        if constexpr (IN_ID == OMNI_VARCHAR || IN_ID == OMNI_CHAR) {
            expectedResult->SetVector(0, new VarcharVector(this->vectorAllocator, maxVarcharLength, 1));
        } else {
            if (TypeUtil::IsDecimalType(IN_ID)
                && (this->aggFunc == OMNI_AGGREGATION_TYPE_SUM || this->aggFunc == OMNI_AGGREGATION_TYPE_AVG)) {
                expectedResult->SetVector(0, new Decimal128Vector(this->vectorAllocator, 1));
            } else if (this->aggFunc == OMNI_AGGREGATION_TYPE_AVG) {
                expectedResult->SetVector(0, new DoubleVector(this->vectorAllocator, 1));
            } else {
                expectedResult->SetVector(0, VectorHelper::CreateFlatVector(
                    this->vectorAllocator, OUT_ID, maxVarcharLength, 1));
            }
        }

        return expectedResult;
    }
};
}
