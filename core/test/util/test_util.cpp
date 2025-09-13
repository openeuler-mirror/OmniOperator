/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Type Util Class
 */

#include "test_util.h"
#include <cmath>
#include <cfloat>
#include <cstdarg>
#include <gtest/gtest.h>
#include "vector/vector_helper.h"
#include "type/data_type.h"
#include "type/decimal_operations.h"

using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::codegen;

namespace omniruntime::TestUtil {
void PrintNotMatchBatches(VectorBatch *outputPages, VectorBatch *expectPage)
{
    printf("================ Expected Vector Batch ==================\n");
    VectorHelper::PrintVecBatch(expectPage);
    printf("================= Result Vector Batch ===================\n");
    VectorHelper::PrintVecBatch(outputPages);
}

bool VecBatchMatch(VectorBatch *outputPages, VectorBatch *expectPage)
{
    if (outputPages->GetRowCount() != expectPage->GetRowCount()) {
        printf("Invalid row count. Expected=%d, actual=%d\n", expectPage->GetRowCount(), outputPages->GetRowCount());
        PrintNotMatchBatches(outputPages, expectPage);
        return false;
    }

    int32_t columnNumber = outputPages->GetVectorCount();
    if (columnNumber != expectPage->GetVectorCount()) {
        printf("Invalid vector count. Expected=%d, actual=%d\n", expectPage->GetVectorCount(),
            outputPages->GetVectorCount());
        PrintNotMatchBatches(outputPages, expectPage);
        return false;
    }
    for (int32_t i = 0; i < columnNumber; i++) {
        if (!ColumnMatch(outputPages->Get(i), expectPage->Get(i))) {
            printf("Vector %d not matched\n", i);
            PrintNotMatchBatches(outputPages, expectPage);
            return false;
        }
    }

    return true;
}

bool VecBatchesIgnoreOrderMatch(std::vector<VectorBatch *> &resultBatches, std::vector<VectorBatch *> &expectedBatches)
{
    if (resultBatches.size() != expectedBatches.size()) {
        printf("List of VectorBatches not match. Expecting %ld, got %ld\n", expectedBatches.size(),
            resultBatches.size());
        printf("================ Expected Vector Batch (%ld) ==================\n", expectedBatches.size());
        for (size_t i = 0; i < expectedBatches.size(); ++i) {
            printf("    ---------- Expected Vector Batch %ld / %ld ----------\n", i, expectedBatches.size());
        }
        printf("================ Result Vector Batch (%ld) ==================\n", resultBatches.size());
        for (size_t i = 0; i < resultBatches.size(); ++i) {
            printf("    ---------- Result Vector Batch %ld / %ld ----------\n", i, resultBatches.size());
        }
        return false;
    }

    for (size_t i = 0; i < resultBatches.size(); i++) {
        if (!VecBatchMatchIgnoreOrder(resultBatches[i], expectedBatches[i])) {
            printf("VectorBatch %ld not match\n", i);
            return false;
        }
    }

    return true;
}

template <typename T> ALWAYS_INLINE T GetValue(BaseVector *vector, uint32_t rowIndex)
{
    if (vector->GetEncoding() != OMNI_DICTIONARY) {
        return static_cast<Vector<T> *>(vector)->GetValue(rowIndex);
    } else {
        return reinterpret_cast<Vector<DictionaryContainer<T>> *>(vector)->GetValue(rowIndex);
    }
}

static ALWAYS_INLINE std::string_view GetVarcharValue(BaseVector *vector, uint32_t rowIndex)
{
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    using DictionaryVector = Vector<DictionaryContainer<std::string_view, LargeStringContainer>>;
    if (vector->GetEncoding() != OMNI_DICTIONARY) {
        return static_cast<VarcharVector *>(vector)->GetValue(rowIndex);
    } else {
        return reinterpret_cast<DictionaryVector *>(vector)->GetValue(rowIndex);
    }
}

static ALWAYS_INLINE bool DoubleValueEqualsValueIgnoreNulls(BaseVector *leftVector, BaseVector *rightVector,
    int32_t rowIndex)
{
    auto leftValue = GetValue<double>(leftVector, rowIndex);
    auto rightValue = GetValue<double>(rightVector, rowIndex);
    if (std::abs(leftValue - rightValue) < __DBL_EPSILON__) {
        return true;
    } else {
        return false;
    }
}

static ALWAYS_INLINE bool VarcharValueEqualsValueIgnoreNulls(BaseVector *leftVector, BaseVector *rightVector,
    int32_t rowIndex)
{
    return GetVarcharValue(leftVector, rowIndex) == GetVarcharValue(rightVector, rowIndex);
}

template <typename T>
ALWAYS_INLINE bool PrimitiveValueEqualsValueIgnoreNulls(BaseVector *leftVector, BaseVector *rightVector,
    int32_t rowIndex)
{
    return GetValue<T>(leftVector, rowIndex) == GetValue<T>(rightVector, rowIndex);
}

template <DataTypeId typeId>
static bool ValueEqualsValueIgnoreNulls(BaseVector *leftVector, BaseVector *rightVector, int32_t rowIndex)
{
    using T = typename NativeType<typeId>::type;
    if constexpr (std::is_same_v<T, std::string_view>) {
        return VarcharValueEqualsValueIgnoreNulls(leftVector, rightVector, rowIndex);
    } else if constexpr (std::is_same_v<T, double>) {
        return DoubleValueEqualsValueIgnoreNulls(leftVector, rightVector, rowIndex);
    } else {
        return PrimitiveValueEqualsValueIgnoreNulls<T>(leftVector, rightVector, rowIndex);
    }
}

bool ColumnMatch(BaseVector *actualColumn, BaseVector *expectColumn)
{
    if (actualColumn->GetSize() != expectColumn->GetSize()) {
        return false;
    }

    bool result = true;
    DataTypeId typeId = expectColumn->GetTypeId();
    for (int32_t rowIndex = 0; rowIndex < actualColumn->GetSize(); rowIndex++) {
        if (actualColumn->IsNull(rowIndex) != expectColumn->IsNull(rowIndex)) {
            return false;
        }

        // all is null
        if ((actualColumn->IsNull(rowIndex) == expectColumn->IsNull(rowIndex)) && actualColumn->IsNull(rowIndex)) {
            continue;
        }

        if (typeId == OMNI_CONTAINER) {
            auto vecCount = static_cast<ContainerVector *>(expectColumn)->GetVectorCount();
            for (int32_t vecIdx = 0; vecIdx < vecCount; vecIdx++) {
                auto actualVec = static_cast<ContainerVector *>(actualColumn)->GetValue(vecIdx);
                auto expectVec = static_cast<ContainerVector *>(expectColumn)->GetValue(vecIdx);
                result =
                    ColumnMatch(reinterpret_cast<BaseVector *>(actualVec), reinterpret_cast<BaseVector *>(expectVec));
                if (!result) {
                    return false;
                }
            }
        } else {
            result = DYNAMIC_TYPE_DISPATCH(ValueEqualsValueIgnoreNulls, typeId, actualColumn, expectColumn, rowIndex);
            if (!result) {
                return false;
            }
        }
    }

    return true;
}

VectorBatch *CreateVectorBatch(const DataTypes &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    auto *vectorBatch = new VectorBatch(rowCount);
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i < typesCount; i++) {
        auto &type = types.GetType(i);
        vectorBatch->Append(CreateVector(*type, rowCount, args));
    }
    va_end(args);
    return vectorBatch;
}

void AssertStringEquals(std::vector<std::string> &expected, std::vector<uint8_t *> &result,
    std::vector<int32_t> &outLen)
{
    for (size_t i = 0; i < expected.size(); i++) {
        std::string actual(reinterpret_cast<char *>(result[i]), outLen[i]);
        EXPECT_EQ(actual, expected[i]);
    }
}

void AssertStringEquals(std::vector<std::string> &expected, int32_t offset, int32_t rowCnt,
    std::vector<uint8_t *> &result, std::vector<int32_t> &outLen)
{
    for (int32_t i = 0; i < rowCnt; i++) {
        std::string actual(reinterpret_cast<char *>(result[i]), outLen[i]);
        EXPECT_EQ(actual, expected[i + offset]);
    }
}

void AssertIntEquals(std::vector<int32_t> &expected, std::vector<int32_t> &result)
{
    for (size_t i = 0; i < expected.size(); i++) {
        EXPECT_EQ(result[i], expected[i]);
    }
}

void AssertLongEquals(std::vector<int64_t> &expected, std::vector<int64_t> &result)
{
    for (size_t i = 0; i < expected.size(); i++) {
        EXPECT_EQ(result[i], expected[i]);
    }
}

void AssertBoolEquals(std::vector<bool> &expected, bool *result)
{
    for (size_t i = 0; i < expected.size(); i++) {
        EXPECT_EQ(result[i], expected[i]);
    }
}

BaseVector *CreateVector(DataType &dataType, int32_t rowCount, va_list &args)
{
    return DYNAMIC_TYPE_DISPATCH(CreateFlatVector, dataType.GetId(), rowCount, args);
}

vec::BaseVector *SliceVector(vec::BaseVector *vector, int32_t offset, int32_t length)
{
    using namespace omniruntime::type;
    if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
        return DYNAMIC_TYPE_DISPATCH(FlatVectorSlice, vector->GetTypeId(), vector, offset, length);
    } else {
        return DYNAMIC_TYPE_DISPATCH(DictionaryVectorSlice, vector->GetTypeId(), vector, offset, length);
    }
}

void SetValue(BaseVector *vector, int32_t index, void *value)
{
    DataTypeId typeId = vector->GetTypeId();
    if (value == nullptr) {
        if (typeId == OMNI_VARCHAR || typeId == OMNI_CHAR) {
            static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->SetNull(index);
        } else {
            vector->SetNull(index);
        }
        return;
    }
    switch (typeId) {
        case OMNI_INT:
        case OMNI_DATE32:
            static_cast<Vector<int32_t> *>(vector)->SetValue(index, *static_cast<int32_t *>(value));
            break;
        case OMNI_SHORT:
            static_cast<Vector<int16_t> *>(vector)->SetValue(index, *static_cast<int16_t *>(value));
            break;
        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64:
            static_cast<Vector<int64_t> *>(vector)->SetValue(index, *static_cast<int64_t *>(value));
            break;
        case OMNI_DOUBLE:
            static_cast<Vector<double> *>(vector)->SetValue(index, *static_cast<double *>(value));
            break;
        case OMNI_BOOLEAN:
            static_cast<Vector<bool> *>(vector)->SetValue(index, *static_cast<bool *>(value));
            break;
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            std::string_view data = std::string_view(static_cast<std::string *>(value)->data(),
                static_cast<std::string *>(value)->length());
            static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->SetValue(index, data);
            break;
        }
        case OMNI_DECIMAL128:
            static_cast<Vector<Decimal128> *>(vector)->SetValue(index, *static_cast<Decimal128 *>(value));
            break;
        default:
            LogError("No such data type %d", typeId);
            break;
    }
}

omniruntime::op::Operator *CreateTestOperator(omniruntime::op::OperatorFactory *operatorFactory)
{
    return operatorFactory->CreateOperator();
}

bool VecBatchMatchIgnoreOrder(vec::VectorBatch *resultBatch, vec::VectorBatch *expectedBatch, const double error)
{
    if (resultBatch->GetRowCount() != expectedBatch->GetRowCount()) {
        printf("Invalid row count. Expected=%d, actual=%d\n", expectedBatch->GetRowCount(), resultBatch->GetRowCount());
        PrintNotMatchBatches(resultBatch, expectedBatch);
        return false;
    }

    auto columnNumber = resultBatch->GetVectorCount();
    if (columnNumber != expectedBatch->GetVectorCount()) {
        printf("Invalid vector count. Expected=%d, actual=%d\n", expectedBatch->GetVectorCount(),
            resultBatch->GetVectorCount());
        PrintNotMatchBatches(resultBatch, expectedBatch);
        return false;
    }

    for (int32_t i = 0; i < columnNumber; ++i) {
        if (!ColumnMatchIgnoreOrder(resultBatch->Get(i), expectedBatch->Get(i), error)) {
            printf("Vector %d not matched\n", i);
            PrintNotMatchBatches(resultBatch, expectedBatch);
            return false;
        }
    }

    return true;
}

VectorBatch *DuplicateVectorBatch(VectorBatch *input)
{
    auto vecCount = input->GetVectorCount();
    auto rowCount = input->GetRowCount();
    auto duplication = new VectorBatch(rowCount);
    for (int32_t i = 0; i < vecCount; i++) {
        duplication->Append(SliceVector(input->Get(i), 0, rowCount));
    }
    return duplication;
}

void FreeVecBatches(VectorBatch **vecBatches, int32_t vecBatchCount)
{
    for (int32_t i = 0; i < vecBatchCount; ++i) {
        VectorHelper::FreeVecBatch(vecBatches[i]);
    }
    delete[] vecBatches;
}

void AssertDictionaryVectorShortEquals(BaseVector *vector, int16_t *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsNull(i)) {
            continue;
        }
        ASSERT_EQ(static_cast<Vector<DictionaryContainer<int16_t>> *>(vector)->GetValue(i), values[i]);
    }
}

void AssertDictionaryVectorIntEquals(BaseVector *vector, int32_t *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsNull(i)) {
            continue;
        }
        ASSERT_EQ(static_cast<Vector<DictionaryContainer<int32_t>> *>(vector)->GetValue(i), values[i]);
    }
}

void AssertDictionaryVectorLongEquals(BaseVector *vector, int64_t *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsNull(i)) {
            continue;
        }
        ASSERT_EQ(static_cast<Vector<DictionaryContainer<int64_t>> *>(vector)->GetValue(i), values[i]);
    }
}

void AssertDictionaryVectorBooleanEquals(BaseVector *vector, bool *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsNull(i)) {
            continue;
        }
        ASSERT_EQ(static_cast<Vector<DictionaryContainer<bool>> *>(vector)->GetValue(i), values[i]);
    }
}

void AssertDictionaryVectorDoubleEquals(BaseVector *vector, double *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsNull(i)) {
            continue;
        }
        EXPECT_TRUE(std::fabs(static_cast<Vector<DictionaryContainer<bool>> *>(vector)->GetValue(i) - values[i]) <=
            DBL_EPSILON);
    }
}

void AssertDictionaryVectorVarcharEquals(BaseVector *vector, std::string *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsNull(i)) {
            continue;
        }
        using DictionaryVarcharVector = Vector<DictionaryContainer<std::string_view, LargeStringContainer>>;
        std::string_view value = static_cast<DictionaryVarcharVector *>(vector)->GetValue(i);
        std::string actual(value.data(), value.length());
        ASSERT_EQ(actual, values[i]);
    }
}

void AssertDictionaryVectorDecimal128Equals(BaseVector *vector, Decimal128 *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsNull(i)) {
            continue;
        }
        ASSERT_EQ(static_cast<Vector<DictionaryContainer<Decimal128>> *>(vector)->GetValue(i), values[i]);
    }
}

void AssertDoubleVectorEquals(BaseVector *vector, double *expectedValues)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsNull(i)) {
            continue;
        }
        EXPECT_TRUE(std::fabs(static_cast<Vector<double> *>(vector)->GetValue(i) - expectedValues[i]) <= DBL_EPSILON);
    }
}

void AssertVarcharVectorEquals(BaseVector *vector, std::string *expectedValues)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsNull(i)) {
            continue;
        }
        std::string_view value = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->GetValue(i);
        std::string result(value.data(), value.length());
        EXPECT_EQ(result, expectedValues[i]);
    }
}

void AssertDictionaryVectorEquals(BaseVector *vector, va_list &args)
{
    switch (vector->GetTypeId()) {
        case omniruntime::type::OMNI_SHORT:
            AssertDictionaryVectorShortEquals(vector, va_arg(args, int16_t *));
            break;
        case omniruntime::type::OMNI_INT:
        case omniruntime::type::OMNI_DATE32:
            AssertDictionaryVectorIntEquals(vector, va_arg(args, int32_t *));
            break;
        case omniruntime::type::OMNI_LONG:
        case omniruntime::type::OMNI_TIMESTAMP:
        case omniruntime::type::OMNI_DECIMAL64:
            AssertDictionaryVectorLongEquals(vector, va_arg(args, int64_t *));
            break;
        case omniruntime::type::OMNI_BOOLEAN:
            AssertDictionaryVectorBooleanEquals(vector, va_arg(args, bool *));
            break;
        case omniruntime::type::OMNI_DOUBLE:
            AssertDictionaryVectorDoubleEquals(vector, va_arg(args, double *));
            break;
        case omniruntime::type::OMNI_VARCHAR:
        case omniruntime::type::OMNI_CHAR:
            AssertDictionaryVectorVarcharEquals(vector, va_arg(args, std::string *));
            break;
        case omniruntime::type::OMNI_DECIMAL128:
            AssertDictionaryVectorDecimal128Equals(vector, va_arg(args, Decimal128 *));
            break;
        default:
            std::cerr << "unsupported type:" << vector->GetTypeId() << std::endl;
            break;
    }
}

void AssertVecBatchEquals(VectorBatch *vectorBatch, int32_t expectedVecCount, int32_t expectedRowCount, ...)
{
    int32_t vectorCount = vectorBatch->GetVectorCount();
    int32_t rowCount = vectorBatch->GetRowCount();
    EXPECT_EQ(vectorCount, expectedVecCount);
    EXPECT_EQ(rowCount, expectedRowCount);

    va_list args;
    va_start(args, expectedRowCount);
    for (int32_t i = 0; i < vectorCount; i++) {
        BaseVector *vector = vectorBatch->Get(i);
        EXPECT_EQ(vector->GetSize(), expectedRowCount);
        if (vector->GetEncoding() == OMNI_DICTIONARY) {
            AssertDictionaryVectorEquals(vector, args);
            break;
        }
        DataTypeId dataTypeId = vectorBatch->Get(i)->GetTypeId();
        switch (dataTypeId) {
            case omniruntime::type::OMNI_INT:
            case omniruntime::type::OMNI_DATE32:
                AssertVectorEquals<int32_t>(vector, va_arg(args, int32_t *));
                break;
            case omniruntime::type::OMNI_SHORT:
                AssertVectorEquals<int16_t>(vector, va_arg(args, int16_t *));
                break;
            case omniruntime::type::OMNI_LONG:
            case omniruntime::type::OMNI_TIMESTAMP:
            case omniruntime::type::OMNI_DECIMAL64:
                AssertVectorEquals<int64_t>(vector, va_arg(args, int64_t *));
                break;
            case omniruntime::type::OMNI_DOUBLE:
                AssertDoubleVectorEquals(vector, va_arg(args, double *));
                break;
            case omniruntime::type::OMNI_BOOLEAN:
                AssertVectorEquals<bool>(vector, va_arg(args, bool *));
                break;
            case omniruntime::type::OMNI_DECIMAL128:
                AssertVectorEquals<Decimal128>(vector, va_arg(args, Decimal128 *));
                break;
            case omniruntime::type::OMNI_VARCHAR:
            case omniruntime::type::OMNI_CHAR:
                AssertVarcharVectorEquals(vector, va_arg(args, std::string *));
                break;
            default:
                std::cerr << "Unsupported type : " << dataTypeId << std::endl;
                break;
        }
    }
    va_end(args);
}

BaseVector *CreateDictionaryVector(DataType &dataType, int32_t rowCount, int32_t *ids, int32_t idsCount, ...)
{
    va_list args;
    va_start(args, idsCount);
    auto dictionary = std::unique_ptr<BaseVector>(CreateVector(dataType, rowCount, args));
    va_end(args);
    return DYNAMIC_TYPE_DISPATCH(CreateDictionary, dataType.GetId(), dictionary.get(), ids, idsCount);
}

BaseVector *CreateVarcharVector(std::string *values, int32_t length)
{
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;

    VarcharVector *vector = new VarcharVector(length);
    for (int32_t i = 0; i < length; i++) {
        std::string_view value(values[i].data(), values[i].length());
        vector->SetValue(i, value);
    }
    return vector;
}

FuncExpr *GetFuncExpr(const std::string &funcName, std::vector<Expr *> args, DataTypePtr returnType)
{
    std::vector<DataTypeId> argTypes(args.size());
    std::transform(args.begin(), args.end(), argTypes.begin(),
        [](Expr *expr) -> DataTypeId { return expr->GetReturnTypeId(); });
    auto signature = FunctionSignature(funcName, argTypes, returnType->GetId());
    auto function = FunctionRegistry::LookupFunction(&signature);
    if (function != nullptr) {
        return new FuncExpr(funcName, args, returnType, function);
    }
    return nullptr;
}

std::string GenerateSpillPath()
{
    char *dirName = get_current_dir_name();
    std::string result = std::string(dirName) + std::string("/") + std::to_string(time(nullptr));
    free(dirName);
    return result;
}

int32_t *MakeInts(int32_t size, int32_t start)
{
    if (size > 0) {
        auto *arr = new int32_t[size];
        int32_t idx = 0;
        for (int32_t i = start; i < start + size; i++) {
            arr[idx++] = i;
        }
        return arr;
    } else {
        return nullptr;
    }
}

int64_t *MakeDecimals(int32_t size, int32_t start)
{
    if (size > 0) {
        const int32_t INDEX_FACTOR = 2;
        auto *arr = new int64_t[size * 2];
        int32_t idx = 0;
        for (int64_t i = start; i < start + size; i++) {
            if (i >= 0) {
                arr[INDEX_FACTOR * idx] = i;
                arr[INDEX_FACTOR * idx + 1] = 0;
            } else {
                arr[INDEX_FACTOR * idx] = i;
                arr[INDEX_FACTOR * idx + 1] = -1;
            }
            idx++;
        }
        return arr;
    } else {
        return nullptr;
    }
}

int64_t *MakeLongs(int32_t size, int64_t start)
{
    if (size > 0) {
        auto *arr = new int64_t[size];
        int32_t idx = 0;
        for (int64_t i = start; i < start + size; i++) {
            arr[idx++] = i;
        }
        return arr;
    } else {
        return nullptr;
    }
}

double *MakeDoubles(int32_t size, double start)
{
    if (size > 0) {
        auto *arr = new double[size];
        int32_t idx = 0;
        for (double i = start; i < start + size; i++) {
            arr[idx++] = i;
        }
        return arr;
    } else {
        return nullptr;
    }
}

VectorBatch *CreateEmptyVectorBatch(const DataTypes &dataTypes)
{
    auto *vectorBatch = new VectorBatch(0);
    auto *dataTypeIds = const_cast<int32_t *>(dataTypes.GetIds());
    auto vectorCnt = dataTypes.GetSize();
    BaseVector *vectors[vectorCnt];
    for (int32_t i = 0; i < vectorCnt; ++i) {
        vectors[i] = VectorHelper::CreateVector(OMNI_FLAT, dataTypeIds[i], 0);
        vectorBatch->Append(std::move(vectors[i]));
    }
    return vectorBatch;
}

int32_t DecodeAddFlag(int32_t resultCode)
{
    return resultCode >> 16;
}

int32_t DecodeFetchFlag(int32_t resultCode)
{
    return resultCode & SHRT_MAX;
}

bool CompareVarcharUnorderedRows(BaseVector *resultVector, BaseVector *expectedVector, const double error)
{
    std::multiset<std::string_view> resRows;
    std::multiset<std::string_view> expectedRows;
    size_t resNullCount = 0;
    size_t expNullCount = 0;
    for (int32_t i = 0; i < resultVector->GetSize(); ++i) {
        if (resultVector->GetEncoding() == OMNI_DICTIONARY) {
            auto leftVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(resultVector);
            if (leftVector->IsNull(i)) {
                resNullCount++;
            } else {
                resRows.emplace(leftVector->GetValue(i));
            }
        } else {
            auto leftVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(resultVector);
            if (leftVector->IsNull(i)) {
                resNullCount++;
            } else {
                resRows.emplace(leftVector->GetValue(i));
            }
        }

        if (expectedVector->GetEncoding() == OMNI_DICTIONARY) {
            auto rightVector = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(expectedVector);
            if (rightVector->IsNull(i)) {
                expNullCount++;
            } else {
                expectedRows.emplace(rightVector->GetValue(i));
            }
        } else {
            auto rightVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(expectedVector);
            if (rightVector->IsNull(i)) {
                expNullCount++;
            } else {
                expectedRows.emplace(rightVector->GetValue(i));
            }
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
        if (*it1 != *it2) {
            return false;
        }
    }

    return true;
}

template <typename D, typename V>
bool CompareUnorderedRows(BaseVector *resultVector, BaseVector *expectedVector, const double error)
{
    std::multiset<D> resRows;
    std::multiset<D> expectedRows;
    size_t resNullCount = 0;
    size_t expNullCount = 0;
    for (int32_t i = 0; i < resultVector->GetSize(); ++i) {
        if (resultVector->GetEncoding() == OMNI_DICTIONARY) {
            auto leftVector = reinterpret_cast<Vector<DictionaryContainer<V>> *>(resultVector);
            if (leftVector->IsNull(i)) {
                resNullCount++;
            } else {
                resRows.emplace(leftVector->GetValue(i));
            }
        } else {
            auto leftVector = static_cast<Vector<V> *>(resultVector);
            if (leftVector->IsNull(i)) {
                resNullCount++;
            } else {
                resRows.emplace(leftVector->GetValue(i));
            }
        }

        if (expectedVector->GetEncoding() == OMNI_DICTIONARY) {
            auto rightVector = reinterpret_cast<Vector<DictionaryContainer<V>> *>(expectedVector);
            if (rightVector->IsNull(i)) {
                expNullCount++;
            } else {
                expectedRows.emplace(rightVector->GetValue(i));
            }
        } else {
            auto rightVector = static_cast<Vector<V> *>(expectedVector);
            if (rightVector->IsNull(i)) {
                expNullCount++;
            } else {
                expectedRows.emplace(rightVector->GetValue(i));
            }
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
        } else if constexpr (std::is_same_v<D, std::string_view>) {
            if (*it1 != *it2) {
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

bool ColumnMatchIgnoreOrder(BaseVector *resultVector, BaseVector *expectedVector, const double error)
{
    bool isMatched = true;
    switch (expectedVector->GetTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32: {
            isMatched = CompareUnorderedRows<int32_t, int32_t>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_SHORT: {
            isMatched = CompareUnorderedRows<int16_t, int16_t>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_DOUBLE: {
            isMatched = CompareUnorderedRows<double, double>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64: {
            isMatched = CompareUnorderedRows<int64_t, int64_t>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_BOOLEAN: {
            isMatched = CompareUnorderedRows<bool, bool>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_DECIMAL128: {
            isMatched = CompareUnorderedRows<Decimal128, Decimal128>(resultVector, expectedVector, error);
            break;
        }
        case OMNI_CHAR:
        case OMNI_VARCHAR: {
            isMatched = CompareVarcharUnorderedRows(resultVector, expectedVector, error);
            break;
        }
        case OMNI_CONTAINER: {
            isMatched = CompareUnorderedRowsContainer(static_cast<ContainerVector *>(resultVector),
                static_cast<ContainerVector *>(expectedVector), error);
            break;
        }
        default: {
            return false;
        }
    }
    return isMatched;
}

bool CompareUnorderedRowsContainer(ContainerVector *resultContainerVector, ContainerVector *expectedContainerVector,
    const double error)
{
    int32_t vecCount = expectedContainerVector->GetVectorCount();
    for (int32_t vecIdx = 0; vecIdx < vecCount; vecIdx++) {
        auto resultVector = reinterpret_cast<BaseVector *>(resultContainerVector->GetValue(vecIdx));
        auto expectedVector = reinterpret_cast<BaseVector *>(expectedContainerVector->GetValue(vecIdx));
        auto result = ColumnMatchIgnoreOrder(resultVector, expectedVector, error);
        if (!result) {
            return false;
        }
    }
    return true;
}
}