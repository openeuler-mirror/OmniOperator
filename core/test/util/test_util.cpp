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

namespace TestUtil {
bool TypesMatch(const int32_t *actualTypeIds, const int32_t *expectTypeIds, int32_t columnNumber);
bool ColumnMatch(Vector *actualColumn, Vector *expectColumn);

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

VarcharVector *CreateVarcharVector(VarcharDataType type, std::string *values, int32_t length)
{
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator();
    uint32_t width = type.GetWidth();
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

Vector *CreateVector(DataType &dataType, int32_t rowCount, va_list &args)
{
    switch (dataType.GetId()) {
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
            return CreateVarcharVector(static_cast<VarcharDataType &>(dataType), va_arg(args, std::string *), rowCount);
        case OMNI_DECIMAL128:
            return CreateDecimal128Vector(va_arg(args, Decimal128 *), rowCount);
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

VectorBatch *CreateVectorBatch(DataTypes &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    auto *vectorBatch = new VectorBatch(typesCount);
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i < typesCount; i++) {
        DataType type = types.Get()[i];
        vectorBatch->SetVector(i, CreateVector(type, rowCount, args));
    }
    va_end(args);
    return vectorBatch;
}

VectorBatch *CreateEmptyVectorBatch(const std::vector<DataType> &dataTypes)
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
    omniruntime::op::Operator *nativeOperator = nullptr;

#if defined(DEBUG_OPERATOR)
    nativeOperator = operatorFactory->CreateOperator();
#else
    auto jitContext = operatorFactory->GetJitContext();
    if (jitContext == nullptr) {
        nativeOperator = operatorFactory->CreateOperator();
    } else {
        auto optModule = reinterpret_cast<omniruntime::op::OptModule>(jitContext->func);
        nativeOperator = optModule(operatorFactory);
    }
#endif
    return nativeOperator;
}

void DeleteOperatorFactory(omniruntime::op::OperatorFactory *operatorFactory)
{
    if (operatorFactory->GetJitContext() != nullptr) {
        delete operatorFactory->GetJitContext();
    }
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

void ToVectorTypes(const int32_t *dataTypeIds, int32_t dataTypeCount, std::vector<DataType> &dataTypes)
{
    uint32_t defaultVarcharLength = 50;
    for (int i = 0; i < dataTypeCount; ++i) {
        if (dataTypeIds[i] == OMNI_VARCHAR) {
            dataTypes.push_back(VarcharDataType(defaultVarcharLength));
            continue;
        } else if (dataTypeIds[i] == OMNI_CHAR) {
            dataTypes.push_back(CharDataType(defaultVarcharLength));
            continue;
        }
        dataTypes.push_back(DataType(dataTypeIds[i]));
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
        if (OMNI_DECIMAL64 == typeOrdinal) {
            return OMNI_LONG;
        }
        if (OMNI_DATE32 == typeOrdinal) {
            return OMNI_INT;
        }
        if (OMNI_SHORT == typeOrdinal || (OMNI_DATE64 <= typeOrdinal && OMNI_INTERVAL_DAY_TIME >= typeOrdinal)) {
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
        return new FuncExpr(funcName, args, std::move(returnType), function);
    }
    return nullptr;
}

std::string GenerateSpillPath()
{
    char *dirName = get_current_dir_name();
    std::string result =  dirName + std::string("/") + std::to_string(time(0));
    free(dirName);
    return result;
}
}