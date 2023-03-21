/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Type Util Class
 */

#include "test_util.h"
#include <cmath>
#include <cfloat>
#include <cstdarg>
#include <gtest/gtest.h>
#include "vector/vector_helper.h"

using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace std;

namespace TestUtil {
bool TypesMatch(const int32_t *actualTypeIds, const int32_t *expectTypeIds, int32_t columnNumber);

bool VecBatchMatch(VectorBatch *outputPages, VectorBatch *expectPage)
{
    if (outputPages->GetRowCount() != expectPage->GetRowCount()) {
        return false;
    }

    int32_t columnNumber = outputPages->GetVectorCount();
    if (columnNumber != expectPage->GetVectorCount()) {
        return false;
    }

    if (!TypesMatch(outputPages->GetVectorTypeIds(), expectPage->GetVectorTypeIds(), columnNumber)) {
        return false;
    }

    for (int32_t i = 0; i < columnNumber; i++) {
        if (!ColumnMatch(outputPages->GetVector(i), expectPage->GetVector(i))) {
            return false;
        }
    }

    return true;
}

bool VecBatchMatches(std::vector<VectorBatch *> &outputPages, std::vector<VectorBatch *> &expectPage)
{
    if (outputPages.size() != expectPage.size()) {
        return false;
    }

    for (size_t i = 0; i < expectPage.size(); i++) {
        if (!VecBatchMatch(outputPages[i], expectPage[i])) {
            return false;
        }
    }

    return true;
}

bool TypesMatch(const int32_t *actualTypeIds, const int32_t *expectTypeIds, int32_t columnNumber)
{
    for (int32_t i = 0; i < columnNumber; i++) {
        if (actualTypeIds[i] != expectTypeIds[i]) {
            return false;
        }
    }

    return true;
}

bool ColumnMatch(Vector *actualColumn, Vector *expectColumn)
{
    if (actualColumn->GetTypeId() != expectColumn->GetTypeId()) {
        return false;
    }

    if (actualColumn->GetSize() != expectColumn->GetSize()) {
        return false;
    }

    bool result = true;
    for (int32_t i = 0; i < actualColumn->GetSize(); i++) {
        int32_t actualIndex;
        int32_t expectIndex;

        Vector *actualCol = VectorHelper::ExpandVectorAndIndex(actualColumn, i, actualIndex);
        Vector *expectCol = VectorHelper::ExpandVectorAndIndex(expectColumn, i, expectIndex);

        if (actualCol->IsValueNull(actualIndex) != expectCol->IsValueNull(expectIndex)) {
            return false;
        } else if ((actualCol->IsValueNull(actualIndex) == expectCol->IsValueNull(expectIndex)) &&
            actualCol->IsValueNull(actualIndex)) {
            continue;
        } else {
            switch (actualCol->GetTypeId()) {
                case OMNI_SHORT:
                    result = (static_cast<ShortVector *>(actualCol)->GetValue(actualIndex) ==
                        static_cast<ShortVector *>(expectCol)->GetValue(expectIndex));
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                    result = (static_cast<IntVector *>(actualCol)->GetValue(actualIndex) ==
                        static_cast<IntVector *>(expectCol)->GetValue(expectIndex));
                    break;
                case OMNI_LONG:
                case OMNI_DECIMAL64: {
                    int64_t actual = static_cast<LongVector *>(actualCol)->GetValue(actualIndex);
                    int64_t expected = static_cast<LongVector *>(expectCol)->GetValue(expectIndex);
                    result = (actual == expected);
                    break;
                }
                case OMNI_DOUBLE:
                    result = (std::fabs(static_cast<DoubleVector *>(actualCol)->GetValue(actualIndex) -
                        static_cast<DoubleVector *>(expectCol)->GetValue(expectIndex)) <= DBL_EPSILON);
                    break;
                case OMNI_BOOLEAN:
                    result = (static_cast<BooleanVector *>(actualCol)->GetValue(actualIndex) ==
                        static_cast<BooleanVector *>(expectCol)->GetValue(expectIndex));
                    break;
                case OMNI_DECIMAL128:
                    result = (static_cast<Decimal128Vector *>(actualCol)->GetValue(actualIndex) ==
                        static_cast<Decimal128Vector *>(expectCol)->GetValue(expectIndex));
                    break;
                case OMNI_VARCHAR:
                case OMNI_CHAR: {
                    uint8_t *actual = nullptr;
                    int32_t actualLength = static_cast<VarcharVector *>(actualCol)->GetValue(actualIndex, &actual);
                    uint8_t *expected = nullptr;
                    int32_t expectedLength = static_cast<VarcharVector *>(expectCol)->GetValue(expectIndex, &expected);
                    if (actualLength != expectedLength || memcmp(actual, expected, actualLength) != 0) {
                        result = false;
                    } else {
                        result = true;
                    }
                    break;
                }
                case OMNI_CONTAINER: {
                    int32_t fieldCount = static_cast<ContainerVector *>(actualCol)->GetVectorCount();
                    for (int32_t colIdx = 0; colIdx < fieldCount; colIdx++) {
                        auto *actualFieldCol =
                            reinterpret_cast<Vector *>(static_cast<ContainerVector *>(actualCol)->GetValue(colIdx));
                        auto *expectFieldCol =
                            reinterpret_cast<Vector *>(static_cast<ContainerVector *>(expectCol)->GetValue(colIdx));
                        result = ColumnMatch(actualFieldCol, expectFieldCol);
                        if (!result) {
                            break;
                        }
                    }
                    break;
                }
                default:
                    result = false;
            }
        }
        if (!result) {
            return false;
        }
    }

    return true;
}

VarcharVector *CreateVarcharVector(DataType &type, std::string *values, int32_t length)
{
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator();
    uint32_t width = static_cast<VarcharDataType &>(type).GetWidth();
    VarcharVector *vector = new VarcharVector(vecAllocator, length * width, length);
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(values[i].c_str()), values[i].length());
    }
    return vector;
}

Decimal128Vector *CreateDecimal128Vector(Decimal128 *values, int32_t length)
{
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator();
    Decimal128Vector *vector = new Decimal128Vector(vecAllocator, length);
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, values[i]);
    }
    return vector;
}

ContainerVector *CreateContainerVector(std::vector<DataTypePtr> &fieldTypes, int32_t rowCount, va_list &args)
{
    int32_t fieldCount = fieldTypes.size();
    std::vector<uintptr_t> vectorAddresses(fieldCount);
    for (int32_t colIdx = 0; colIdx < fieldCount; colIdx++) {
        auto *fieldVector = fieldTypes[colIdx]->GetId() == OMNI_CONTAINER ?
            CreateContainerVector(static_cast<ContainerDataType *>(fieldTypes[colIdx].get())->GetFieldTypes(), rowCount,
            args) :
            CreateVector(*fieldTypes[colIdx], rowCount, args);
        vectorAddresses[colIdx] = reinterpret_cast<uintptr_t>(fieldVector);
    }
    omniruntime::vec::VectorAllocator *vecAllocator = omniruntime::vec::VectorAllocator::GetGlobalAllocator();
    return new ContainerVector(vecAllocator, rowCount, vectorAddresses, fieldCount, fieldTypes);
}

Vector *CreateVector(DataType &dataType, int32_t rowCount, va_list &args)
{
    switch (dataType.GetId()) {
        case OMNI_SHORT:
            return CreateVector<ShortVector>(va_arg(args, int16_t *), rowCount);
        case OMNI_INT:
        case OMNI_DATE32:
            return CreateVector<IntVector>(va_arg(args, int32_t *), rowCount);
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            return CreateVector<LongVector>(va_arg(args, int64_t *), rowCount);
        case OMNI_DOUBLE:
            return CreateVector<DoubleVector>(va_arg(args, double *), rowCount);
        case OMNI_BOOLEAN:
            return CreateVector<BooleanVector>(va_arg(args, bool *), rowCount);
        case OMNI_VARCHAR:
        case OMNI_CHAR:
            return CreateVarcharVector(dataType, va_arg(args, std::string *), rowCount);
        case OMNI_DECIMAL128:
            return CreateDecimal128Vector(va_arg(args, Decimal128 *), rowCount);
        case OMNI_CONTAINER:
            return static_cast<Vector *>(
                CreateContainerVector(static_cast<ContainerDataType &>(dataType).GetFieldTypes(), rowCount, args));
        default:
            std::cerr << "Unsupported type : " << dataType.GetId() << std::endl;
            return nullptr;
    }
}

DictionaryVector *CreateDictionaryVector(DataType &dataType, int32_t rowCount, int32_t *ids, int32_t idsCount, ...)
{
    va_list args;
    va_start(args, idsCount);
    Vector *dictionary = CreateVector(dataType, rowCount, args);
    va_end(args);
    auto vec = new DictionaryVector(dictionary, ids, idsCount);
    delete dictionary;
    return vec;
}

VectorBatch *CreateVectorBatch(const DataTypes &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    auto *vectorBatch = new VectorBatch(typesCount, rowCount);
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i < typesCount; i++) {
        DataTypePtr type = types.GetType(i);
        vectorBatch->SetVector(i, CreateVector(*type, rowCount, args));
    }
    va_end(args);
    return vectorBatch;
}

VectorBatch *CreateEmptyVectorBatch(const std::vector<DataTypePtr> &dataTypes)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator();
    VectorBatch *vectorBatch = new VectorBatch(dataTypes.size());
    vectorBatch->NewVectors(allocator, dataTypes);
    return vectorBatch;
}

void AssertDoubleVectorEquals(DoubleVector *vector, double *expectedValues)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsValueNull(i)) {
            continue;
        }
        EXPECT_TRUE(std::fabs(vector->GetValue(i) - expectedValues[i]) <= DBL_EPSILON);
    }
}

void AssertVarcharVectorEquals(VarcharVector *vector, std::string *expectedValues)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsValueNull(i)) {
            continue;
        }
        uint8_t *value = nullptr;
        int32_t len = vector->GetValue(i, &value);
        EXPECT_EQ(len, expectedValues[i].length());
        EXPECT_TRUE(memcmp(value, expectedValues[i].c_str(), len) == 0);
    }
}

void AssertDictionaryVectorShortEquals(DictionaryVector *vector, int16_t *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        int32_t rowIndex;
        Vector *originalVec = VectorHelper::ExpandVectorAndIndex(vector, i, rowIndex);
        if (originalVec->IsValueNull(rowIndex)) {
            continue;
        }
        ASSERT_EQ(vector->GetShort(i), values[i]);
    }
}

void AssertDictionaryVectorIntEquals(DictionaryVector *vector, int32_t *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        int32_t rowIndex;
        Vector *originalVec = VectorHelper::ExpandVectorAndIndex(vector, i, rowIndex);
        if (originalVec->IsValueNull(rowIndex)) {
            continue;
        }
        ASSERT_EQ(vector->GetInt(i), values[i]);
    }
}

void AssertDictionaryVectorLongEquals(DictionaryVector *vector, int64_t *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        int32_t rowIndex;
        Vector *originalVec = VectorHelper::ExpandVectorAndIndex(vector, i, rowIndex);
        if (originalVec->IsValueNull(rowIndex)) {
            continue;
        }
        ASSERT_EQ(vector->GetLong(i), values[i]);
    }
}

void AssertDictionaryVectorBooleanEquals(DictionaryVector *vector, bool *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        int32_t rowIndex;
        Vector *originalVec = VectorHelper::ExpandVectorAndIndex(vector, i, rowIndex);
        if (originalVec->IsValueNull(rowIndex)) {
            continue;
        }
        ASSERT_EQ(vector->GetBoolean(i), values[i]);
    }
}

void AssertDictionaryVectorDoubleEquals(DictionaryVector *vector, double *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        int32_t rowIndex;
        Vector *originalVec = VectorHelper::ExpandVectorAndIndex(vector, i, rowIndex);
        if (originalVec->IsValueNull(rowIndex)) {
            continue;
        }
        EXPECT_TRUE(std::fabs(vector->GetDouble(i) - values[i]) <= DBL_EPSILON);
    }
}

void AssertDictionaryVectorVarcharEquals(DictionaryVector *vector, std::string *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        int32_t rowIndex;
        Vector *originalVec = VectorHelper::ExpandVectorAndIndex(vector, i, rowIndex);
        if (originalVec->IsValueNull(rowIndex)) {
            continue;
        }
        uint8_t *data = nullptr;
        int32_t len = vector->GetVarchar(i, &data);
        std::string actual(data, data + len);
        ASSERT_EQ(actual, values[i]);
    }
}

void AssertDictionaryVectorDecimal128Equals(DictionaryVector *vector, Decimal128 *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        int32_t rowIndex;
        Vector *originalVec = VectorHelper::ExpandVectorAndIndex(vector, i, rowIndex);
        if (originalVec->IsValueNull(rowIndex)) {
            continue;
        }
        ASSERT_EQ(vector->GetDecimal128(i), values[i]);
    }
}

void AssertDictionaryVectorEquals(DictionaryVector *vector, va_list &args)
{
    DataTypeId dataTypeId;
    VectorEncoding vectorEncoding;
    Vector *dictionary = vector->GetDictionary();
    while ((vectorEncoding = dictionary->GetEncoding()) == OMNI_VEC_ENCODING_DICTIONARY) {
        dictionary = static_cast<DictionaryVector *>(dictionary)->GetDictionary();
    }
    dataTypeId = dictionary->GetTypeId();
    switch (dataTypeId) {
        case omniruntime::type::OMNI_SHORT:
            AssertDictionaryVectorShortEquals(vector, va_arg(args, int16_t *));
            break;
        case omniruntime::type::OMNI_INT:
        case omniruntime::type::OMNI_DATE32:
            AssertDictionaryVectorIntEquals(vector, va_arg(args, int32_t *));
            break;
        case omniruntime::type::OMNI_LONG:
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
            std::cerr << "unsupported type:" << dataTypeId << std::endl;
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
        Vector *vector = vectorBatch->GetVectors()[i];
        EXPECT_EQ(vector->GetSize(), expectedRowCount);
        if (vector->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            AssertDictionaryVectorEquals(dynamic_cast<DictionaryVector *>(vector), args);
            break;
        }
        switch (vector->GetTypeId()) {
            case omniruntime::type::OMNI_INT:
            case omniruntime::type::OMNI_DATE32:
                AssertVectorEquals(dynamic_cast<IntVector *>(vector), va_arg(args, int32_t *));
                break;
            case omniruntime::type::OMNI_SHORT:
                AssertVectorEquals(dynamic_cast<ShortVector *>(vector), va_arg(args, int16_t *));
                break;
            case omniruntime::type::OMNI_LONG:
            case omniruntime::type::OMNI_DECIMAL64:
                AssertVectorEquals(dynamic_cast<LongVector *>(vector), va_arg(args, int64_t *));
                break;
            case omniruntime::type::OMNI_DOUBLE:
                AssertDoubleVectorEquals(dynamic_cast<DoubleVector *>(vector), va_arg(args, double *));
                break;
            case omniruntime::type::OMNI_BOOLEAN:
                AssertVectorEquals(dynamic_cast<BooleanVector *>(vector), va_arg(args, bool *));
                break;
            case omniruntime::type::OMNI_DECIMAL128:
                AssertVectorEquals(dynamic_cast<Decimal128Vector *>(vector), va_arg(args, Decimal128 *));
                break;
            case omniruntime::type::OMNI_VARCHAR:
            case omniruntime::type::OMNI_CHAR:
                AssertVarcharVectorEquals(dynamic_cast<VarcharVector *>(vector), va_arg(args, std::string *));
                break;
            default:
                std::cerr << "Unsupported type : " << vector->GetTypeId() << std::endl;
                break;
        }
    }
    va_end(args);
}

omniruntime::op::Operator *CreateTestOperator(omniruntime::op::OperatorFactory *operatorFactory)
{
    return operatorFactory->CreateOperator();
}

void DeleteOperatorFactory(omniruntime::op::OperatorFactory *operatorFactory)
{
    delete operatorFactory;
}

VectorBatch *DuplicateVectorBatch(VectorBatch *input)
{
    auto vecCount = input->GetVectorCount();
    auto rowCount = input->GetRowCount();
    auto duplication = new VectorBatch(vecCount, rowCount);
    for (int32_t i = 0; i < vecCount; i++) {
        duplication->SetVector(i, input->GetVector(i)->Slice(0, rowCount));
    }
    return duplication;
}

void ToVectorTypes(const int32_t *dataTypeIds, int32_t dataTypeCount, std::vector<DataTypePtr> &dataTypes)
{
    uint32_t defaultVarcharLength = 50;
    for (int i = 0; i < dataTypeCount; ++i) {
        if (dataTypeIds[i] == OMNI_VARCHAR) {
            dataTypes.push_back(VarcharType(defaultVarcharLength));
            continue;
        } else if (dataTypeIds[i] == OMNI_CHAR) {
            dataTypes.push_back(CharType(defaultVarcharLength));
            continue;
        }
        dataTypes.push_back(std::make_shared<DataType>(dataTypeIds[i]));
    }
}

int32_t GetTestProjectCol(std::string &expression)
{
    // #0 or #5 is not expression
    if (expression.data()[0] == '#') {
        return std::stoi(std::string(expression.data() + 1));
    } else {
        return -1;
    }
}

int32_t GetTestExprReturnType(std::string &expression)
{
    const char *chars = expression.data();
    auto length = expression.size();
    auto start = -1;
    auto end = 0;
    for (uint32_t i = 0; i < length; i++) {
        if (start == -1 && chars[i] == ':') {
            start = i;
        }
        if (start != -1 && chars[i] == '(') {
            end = i;
            break;
        }
    }

    std::string returnType(chars + start + 1, chars + end);
    if (returnType.find_first_not_of("0123456789") == std::string::npos && stoi(returnType) < INT32_MAX) {
        int typeOrdinal = stoi(returnType);
        if (typeOrdinal == OMNI_DECIMAL64) {
            return OMNI_LONG;
        }
        if (typeOrdinal == OMNI_DATE32) {
            return OMNI_INT;
        }
        if (typeOrdinal == OMNI_SHORT || (typeOrdinal >= OMNI_DATE64 && typeOrdinal <= OMNI_INTERVAL_DAY_TIME)) {
            std::cout << "Unsupported return type: " << static_cast<DataTypeId>(typeOrdinal) << std::endl;
        }
        return static_cast<DataTypeId>(stoi(returnType));
    }
    std::cout << "Unsupported return type: " + returnType << std::endl;
    return OMNI_INVALID;
}

void GetTestTypeIds(DataTypes &inputTypes, std::string *projectKeys, int32_t projectKeysCount,
    std::vector<int32_t> &typeIds, int32_t *projectCols)
{
    int32_t *inputTypeIds = const_cast<int32_t *>(inputTypes.GetIds());
    int32_t inputTypesCount = inputTypes.GetSize();
    typeIds.insert(typeIds.end(), inputTypeIds, inputTypeIds + inputTypesCount);

    int32_t newProjectCol = inputTypesCount;
    for (int32_t i = 0; i < projectKeysCount; i++) {
        int32_t projectCol = GetTestProjectCol(projectKeys[i]);
        projectCols[i] = projectCol;
        if (projectCol == -1) {
            int32_t returnType = GetTestExprReturnType(projectKeys[i]);
            typeIds.push_back(returnType);
            projectCols[i] = newProjectCol++;
        }
    }
}

FuncExpr *GetFuncExpr(const std::string &funcName, std::vector<Expr *> args, DataTypePtr returnType)
{
    std::vector<DataTypeId> argTypes(args.size());
    std::transform(args.begin(), args.end(), argTypes.begin(),
        [](Expr *expr) -> DataTypeId { return expr->GetReturnTypeId(); });
    for (size_t i = 0; i < argTypes.size(); i++) {
        if (argTypes[i] == omniruntime::type::OMNI_DATE32) {
            argTypes[i] = omniruntime::type::OMNI_INT;
        }
    }
    auto signature = FunctionSignature(funcName, argTypes, returnType->GetId());
    auto function = omniruntime::FunctionRegistry::LookupFunction(&signature);
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

void SetNulls(omniruntime::vec::Vector *vector, std::vector<bool> &nulls)
{
    for (int32_t i = 0; i < (int32_t)nulls.size(); i++) {
        if (nulls[i]) {
            vector->SetValueNull(i);
        }
    }
}

omniruntime::vec::VarcharVector *CreateVarcharVector(std::vector<std::string> &values, std::vector<bool> &nulls)
{
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator();
    static int32_t initCapacity = 1024; // 1k
    int32_t rowCount = values.size();
    auto *out = new VarcharVector(vecAllocator, initCapacity, rowCount);
    for (int32_t i = 0; i < rowCount; i++) {
        if (nulls[i]) {
            out->SetValueNull(i);
        } else {
            out->SetValue(i, reinterpret_cast<const uint8_t *>(values[i].c_str()), values[i].length());
        }
    }
    return out;
}

omniruntime::vec::VectorBatch *CreateVectorBatch(int32_t rowCount, std::vector<omniruntime::vec::Vector *> &vectors)
{
    int32_t vecCount = vectors.size();
    auto *vectorBatch = new VectorBatch(vecCount, rowCount);
    for (int32_t i = 0; i < vecCount; i++) {
        vectorBatch->SetVector(i, vectors[i]);
    }
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

int32_t *MakeInts(const int32_t size, const int32_t start)
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

int64_t *MakeDecimals(const int32_t size, const int32_t start)
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
                arr[INDEX_FACTOR * idx] = i * -1;
                arr[INDEX_FACTOR * idx + 1] = 1LL << 63;
            }
            idx++;
        }
        return arr;
    } else {
        return nullptr;
    }
}

int64_t *MakeLongs(const int32_t size, const int64_t start)
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

double *MakeDoubles(const int32_t size, const double start)
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

int16_t *MakeShorts(const int32_t size, const int16_t start)
{
    if (size > 0) {
        auto *arr = new int16_t[size];
        int32_t idx = 0;
        for (int16_t i = start; i < start + size; i++) {
            arr[idx++] = i;
        }
        return arr;
    } else {
        return nullptr;
    }
}

int32_t DecodeAddFlag(int32_t resultCode)
{
    return resultCode >> 16;
}

int32_t DecodeFetchFlag(int32_t resultCode)
{
    return resultCode & SHRT_MAX;
}
}