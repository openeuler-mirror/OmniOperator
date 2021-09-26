/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Type Util Class
 */
#include <cmath>
#include <cfloat>
#include <gtest/gtest.h>
#include "test_util.h"
#include "../../src/vector/dictionary_vector.h"
#include "../../src/vector/vector_types.h"
#include "../../src/vector/vector_helper.h"

using namespace omniruntime::vec;

bool TypesMatch(const VecType *actualTypes, const VecType *expectTypes, int32_t columnNumber);
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

    if (!TypesMatch(outputPages->GetVectorTypes(), expectPage->GetVectorTypes(), columnNumber)) {
        return false;
    }

    for (int32_t i = 0; i < columnNumber; i++) {
        if (!ColumnMatch(outputPages->GetVector(i), expectPage->GetVector(i))) {
            return false;
        }
    }

    return true;
}

bool TypesMatch(const VecType *actualTypes, const VecType *expectTypes, int32_t columnNumber)
{
    for (int32_t i = 0; i < columnNumber; i++) {
        if (actualTypes[i] != expectTypes[i]) {
            return false;
        }
    }

    return true;
}

bool ColumnMatch(Vector *actualColumn, Vector *expectColumn)
{
    if (actualColumn->GetType() != expectColumn->GetType()) {
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
            switch (actualCol->GetType().GetId()) {
                case OMNI_VEC_TYPE_INT:
                case OMNI_VEC_TYPE_DATE32:
                    result = (static_cast<IntVector *>(actualCol)->GetValue(actualIndex) ==
                        static_cast<IntVector *>(expectCol)->GetValue(expectIndex));
                    break;
                case OMNI_VEC_TYPE_LONG:
                case OMNI_VEC_TYPE_DECIMAL64: {
                    int64_t actual = static_cast<LongVector *>(actualCol)->GetValue(actualIndex);
                    int64_t expected = static_cast<LongVector *>(expectCol)->GetValue(expectIndex);
                    result = (actual == expected);
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE:
                    result = (std::fabs(static_cast<DoubleVector *>(actualCol)->GetValue(actualIndex) -
                        static_cast<DoubleVector *>(expectCol)->GetValue(expectIndex)) <= DBL_EPSILON);
                    break;
                case OMNI_VEC_TYPE_BOOLEAN:
                    result = (static_cast<BooleanVector *>(actualCol)->GetValue(actualIndex) ==
                        static_cast<BooleanVector *>(expectCol)->GetValue(expectIndex));
                    break;
                case OMNI_VEC_TYPE_DECIMAL128:
                    result = (static_cast<Decimal128Vector *>(actualCol)->GetValue(actualIndex) ==
                        static_cast<Decimal128Vector *>(expectCol)->GetValue(expectIndex));
                    break;
                case OMNI_VEC_TYPE_VARCHAR: {
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

VarcharVector *CreateVarcharVector(VarcharVecType type, std::string *values, int32_t length)
{
    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    uint32_t width = type.GetWidth();
    VarcharVector *vector = std::make_unique<VarcharVector>(vecAllocator, length * width, length).release();
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(values[i].c_str()), values[i].length());
    }
    return vector;
}

Decimal128Vector *CreateDecimal128Vector(Decimal128 *values, int32_t length)
{
    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    Decimal128Vector *vector = std::make_unique<Decimal128Vector>(vecAllocator, length).release();
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, values[i]);
    }
    return vector;
}

Vector *CreateVector(VecType &vecType, int32_t rowCount, va_list &args)
{
    switch (vecType.GetId()) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32:
            return CreateVector<IntVector>(va_arg(args, int32_t *), rowCount);
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64:
            return CreateVector<LongVector>(va_arg(args, int64_t *), rowCount);
        case OMNI_VEC_TYPE_DOUBLE:
            return CreateVector<DoubleVector>(va_arg(args, double *), rowCount);
        case OMNI_VEC_TYPE_BOOLEAN:
            return CreateVector<BooleanVector>(va_arg(args, bool *), rowCount);
        case OMNI_VEC_TYPE_VARCHAR:
            return CreateVarcharVector(static_cast<VarcharVecType &>(vecType), va_arg(args, std::string *), rowCount);
        case OMNI_VEC_TYPE_DECIMAL128:
            return CreateDecimal128Vector(va_arg(args, Decimal128 *), rowCount);
        default:
            std::cerr << "Unsupported type : " << vecType.GetId() << std::endl;
            return nullptr;
    }
}

DictionaryVector *CreateDictionaryVector(VecType &vecType, int32_t rowCount, int32_t *ids, int32_t idsCount, ...)
{
    va_list args;
    va_start(args, idsCount);
    Vector *dictionary = CreateVector(vecType, rowCount, args);
    va_end(args);
    return std::make_unique<DictionaryVector>(dictionary, ids, idsCount).release();
}

VectorBatch *CreateVectorBatch(VecTypes &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    VectorBatch *vectorBatch = std::make_unique<VectorBatch>(typesCount).release();
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i < typesCount; i++) {
        VecType type = types.Get()[i];
        vectorBatch->SetVector(i, CreateVector(type, rowCount, args));
    }
    va_end(args);
    return vectorBatch;
}

void AssertDoubleVectorEquals(DoubleVector *vector, double *expectedValues)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsValueNull(i)) {
            continue;
        }
        EXPECT_TRUE(vector->GetValue(i) - expectedValues[i] <= DBL_EPSILON);
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
    // TODO::handle null
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
        EXPECT_TRUE(vector->GetDouble(i) - values[i] <= DBL_EPSILON);
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
    VecTypeId vecTypeId;
    Vector *dictionary = vector->GetDictionary();
    while ((vecTypeId = dictionary->GetType().GetId()) == OMNI_VEC_TYPE_DICTIONARY) {
        dictionary = static_cast<DictionaryVector *>(dictionary)->GetDictionary();
    }

    switch (vecTypeId) {
        case omniruntime::vec::OMNI_VEC_TYPE_INT:
        case omniruntime::vec::OMNI_VEC_TYPE_DATE32:
            AssertDictionaryVectorIntEquals(vector, va_arg(args, int32_t *));
            break;
        case omniruntime::vec::OMNI_VEC_TYPE_LONG:
        case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL64:
            AssertDictionaryVectorLongEquals(vector, va_arg(args, int64_t *));
            break;
        case omniruntime::vec::OMNI_VEC_TYPE_BOOLEAN:
            AssertDictionaryVectorBooleanEquals(vector, va_arg(args, bool *));
            break;
        case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE:
            AssertDictionaryVectorDoubleEquals(vector, va_arg(args, double *));
            break;
        case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR:
            AssertDictionaryVectorVarcharEquals(vector, va_arg(args, std::string *));
            break;
        case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL128:
            AssertDictionaryVectorDecimal128Equals(vector, va_arg(args, Decimal128 *));
            break;
        default:
            std::cerr << "unsupported type:" << vecTypeId << std::endl;
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
        switch (vector->GetType().GetId()) {
            case omniruntime::vec::OMNI_VEC_TYPE_INT:
            case omniruntime::vec::OMNI_VEC_TYPE_DATE32:
                AssertVectorEquals(dynamic_cast<IntVector *>(vector), va_arg(args, int32_t *));
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_LONG:
            case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL64:
                AssertVectorEquals(dynamic_cast<LongVector *>(vector), va_arg(args, int64_t *));
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE:
                AssertDoubleVectorEquals(dynamic_cast<DoubleVector *>(vector), va_arg(args, double *));
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_BOOLEAN:
                AssertVectorEquals(dynamic_cast<BooleanVector *>(vector), va_arg(args, bool *));
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL128:
                AssertVectorEquals(dynamic_cast<Decimal128Vector *>(vector), va_arg(args, Decimal128 *));
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR:
                AssertVarcharVectorEquals(dynamic_cast<VarcharVector *>(vector), va_arg(args, std::string *));
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY:
                AssertDictionaryVectorEquals(dynamic_cast<DictionaryVector *>(vector), args);
                break;
            default:
                std::cerr << "Unsupported type : " << vector->GetTypeId() << std::endl;
                break;
        }
    }
    va_end(args);
}

omniruntime::op::Operator *CreateTestOperator(OperatorFactory *operatorFactory)
{
    omniruntime::op::Operator *nativeOperator = nullptr;

#ifdef DEBUG_OPERATOR
    nativeOperator = operatorFactory->CreateOperator();
#else
    JitContext *jitContext = operatorFactory->GetJitContext();
    if (jitContext == nullptr) {
        nativeOperator = operatorFactory->CreateOperator();
    } else {
        opt_module operatorModule = (opt_module)(jitContext->func);
        nativeOperator = operatorModule(operatorFactory);
    }
#endif
    return nativeOperator;
}

void DeleteOperatorFactory(OperatorFactory *operatorFactory)
{
    if (operatorFactory->GetJitContext() != nullptr) {
        delete operatorFactory->GetJitContext();
    }
    delete operatorFactory;
}
