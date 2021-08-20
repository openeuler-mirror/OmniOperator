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

bool ValueMatch(DictionaryVector *actualVector, DictionaryVector *expectedVector, int32_t position)
{
    VecType type = actualVector->GetDictionary()->GetType();
    int32_t actualPosition = actualVector->GetIds()[position];
    int32_t expectPosition = expectedVector->GetIds()[position];
    switch (type.GetId()) {
        case OMNI_VEC_TYPE_INT: {
            int32_t actual = actualVector->GetInt(actualPosition);
            int32_t expect = expectedVector->GetInt(expectPosition);
            return actual == expect;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t actual = actualVector->GetLong(actualPosition);
            int64_t expect = expectedVector->GetLong(expectPosition);
            return actual == expect;
        }
        default:
            return false;
    }
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
        if (actualColumn->IsValueNull(i) != expectColumn->IsValueNull(i)) {
            return false;
        }
        switch (actualColumn->GetType().GetId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
                result = (static_cast<IntVector *>(actualColumn)->GetValue(i) ==
                    static_cast<IntVector *>(expectColumn)->GetValue(i));
                break;
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                result = (static_cast<LongVector *>(actualColumn)->GetValue(i) ==
                    static_cast<LongVector *>(expectColumn)->GetValue(i));
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                result = (std::fabs(static_cast<DoubleVector *>(actualColumn)->GetValue(i) -
                    static_cast<DoubleVector *>(expectColumn)->GetValue(i)) <= DBL_EPSILON);
                break;
            case OMNI_VEC_TYPE_BOOLEAN:
                result = (static_cast<BooleanVector *>(actualColumn)->GetValue(i) ==
                    static_cast<BooleanVector *>(expectColumn)->GetValue(i));
                break;
            case OMNI_VEC_TYPE_DECIMAL128:
                result = (static_cast<Decimal128Vector *>(actualColumn)->GetValue(i) ==
                    static_cast<Decimal128Vector *>(expectColumn)->GetValue(i));
                break;
            case OMNI_VEC_TYPE_DICTIONARY:
                result = ValueMatch(static_cast<DictionaryVector *>(actualColumn),
                    static_cast<DictionaryVector *>(expectColumn), i);
                break;
            case OMNI_VEC_TYPE_VARCHAR: {
                uint8_t *actual = nullptr;
                int32_t actualLength = static_cast<VarcharVector *>(actualColumn)->GetValue(i, &actual);
                uint8_t *expected = nullptr;
                int32_t expectedLength = static_cast<VarcharVector *>(expectColumn)->GetValue(i, &expected);
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
        if (!result) {
            return false;
        }
    }

    return true;
}

template <typename T, typename V> T *CreateVector(V *values, int32_t length)
{
    auto vector = std::make_unique<T>(nullptr, length).release();
    vector->SetValues(0, values, length);
    return vector;
}

VarcharVector *CreateVarcharVector(VarcharVecType &type, std::string *values, int32_t length)
{
    uint32_t width = type.GetWidth();
    VarcharVector *vector =
        std::make_unique<VarcharVector>(static_cast<VectorAllocator *>(nullptr), length * width, length).release();
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, reinterpret_cast<const uint8_t *>(values[i].c_str()), values[i].length());
    }
    return vector;
}

Decimal128Vector *CreateDecimal128Vector(Decimal128 *values, int32_t length)
{
    Decimal128Vector *vector = std::make_unique<Decimal128Vector>(nullptr, length).release();
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, values[i]);
    }
    return vector;
}

VectorBatch *CreateVectorBatch(VecTypes &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    VectorBatch *vectorBatch = std::make_unique<VectorBatch>(typesCount).release();
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i < typesCount; i++) {
        VecType type = types.Get()[i];
        switch (type.GetId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
                vectorBatch->SetVector(i, CreateVector<IntVector>(va_arg(args, int32_t *), rowCount));
                break;
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                vectorBatch->SetVector(i, CreateVector<LongVector>(va_arg(args, int64_t *), rowCount));
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                vectorBatch->SetVector(i, CreateVector<DoubleVector>(va_arg(args, double *), rowCount));
                break;
            case OMNI_VEC_TYPE_BOOLEAN:
                vectorBatch->SetVector(i, CreateVector<BooleanVector>(va_arg(args, bool *), rowCount));
                break;
            case OMNI_VEC_TYPE_VARCHAR:
                vectorBatch->SetVector(i,
                    CreateVarcharVector(static_cast<VarcharVecType &>(type), va_arg(args, std::string *), rowCount));
                break;
            case OMNI_VEC_TYPE_DECIMAL128:
                vectorBatch->SetVector(i, CreateDecimal128Vector(va_arg(args, Decimal128 *), rowCount));
                break;
            default:
                std::cerr << "Unsupported type : " << type.GetId() << std::endl;
                break;
        }
    }
    va_end(args);
    return vectorBatch;
}

template <typename T, typename E> void AssertVectorEquals(T *vector, E *expectedValues)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsValueNull(i)) {
            continue;
        }
        EXPECT_EQ(vector->GetValue(i), expectedValues[i]);
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
        if (vector->IsValueNull(i)) {
            continue;
        }
        ASSERT_EQ(vector->GetInt(i), values[i]);
    }
}

void AssertDictionaryVectorLongEquals(DictionaryVector *vector, int64_t *values)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsValueNull(i)) {
            continue;
        }
        ASSERT_EQ(vector->GetLong(i), values[i]);
    }
}

void AssertDictionaryVectorEquals(DictionaryVector *vector, va_list &args)
{
    Vector *dictionary = vector->GetDictionary();
    switch (dictionary->GetType().GetId()) {
        case omniruntime::vec::OMNI_VEC_TYPE_INT:
            AssertDictionaryVectorIntEquals(vector, va_arg(args, int32_t *));
            break;
        case omniruntime::vec::OMNI_VEC_TYPE_LONG:
            AssertDictionaryVectorLongEquals(vector, va_arg(args, int64_t *));
            break;
        default:
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
                AssertVectorEquals(dynamic_cast<DoubleVector *>(vector), va_arg(args, double *));
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
                std::cerr << "Unsupported type : " << vector->GetType().GetId() << std::endl;
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
