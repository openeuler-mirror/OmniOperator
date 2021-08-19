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
            case OMNI_VEC_TYPE_INT: {
                int32_t actual = static_cast<IntVector *>(actualColumn)->GetValue(i);
                int32_t expect = static_cast<IntVector *>(expectColumn)->GetValue(i);
                result = (actual == expect);
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                int64_t actual = static_cast<LongVector *>(actualColumn)->GetValue(i);
                int64_t expect = static_cast<LongVector *>(expectColumn)->GetValue(i);
                result = (actual == expect);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                double actual = static_cast<DoubleVector *>(actualColumn)->GetValue(i);
                double expect = static_cast<DoubleVector *>(expectColumn)->GetValue(i);
                result = (std::fabs(actual - expect) <= DBL_EPSILON);
                break;
            }
            case OMNI_VEC_TYPE_DICTIONARY: {
                result = ValueMatch(static_cast<DictionaryVector *>(actualColumn),
                    static_cast<DictionaryVector *>(expectColumn), i);
                break;
            }
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


VectorBatch *CreateVectorBatch(VecTypes &types, int32_t rowCount, ...)
{
    int32_t typesCount = types.GetSize();
    VectorBatch *vectorBatch = std::make_unique<VectorBatch>(typesCount).release();
    va_list args;
    va_start(args, rowCount);
    for (int32_t i = 0; i < typesCount; i++) {
        VecType type = types.Get()[i];
        switch (type.GetId()) {
            case OMNI_VEC_TYPE_INT: {
                IntVector *vector = std::make_unique<IntVector>(nullptr, rowCount).release();
                int32_t *values = va_arg(args, int32_t *);
                vector->SetValues(0, values, rowCount);
                vectorBatch->SetVector(i, vector);
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                LongVector *vector = std::make_unique<LongVector>(nullptr, rowCount).release();
                int64_t *values = va_arg(args, int64_t *);
                vector->SetValues(0, values, rowCount);
                vectorBatch->SetVector(i, vector);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                DoubleVector *vector = std::make_unique<DoubleVector>(nullptr, rowCount).release();
                double *values = va_arg(args, double *);
                vector->SetValues(0, values, rowCount);
                vectorBatch->SetVector(i, vector);
                break;
            }
            case OMNI_VEC_TYPE_VARCHAR: {
                uint32_t width = static_cast<VarcharVecType &>(type).GetWidth();
                VarcharVector *vector =
                    std::make_unique<VarcharVector>(static_cast<VectorAllocator *>(nullptr), rowCount * width, rowCount)
                        .release();
                std::string *values = va_arg(args, std::string *);
                for (int32_t j = 0; j < rowCount; j++) {
                    vector->SetValue(j, reinterpret_cast<const uint8_t *>(values[j].c_str()), values[j].length());
                }
                vectorBatch->SetVector(i, vector);
                break;
            }
            default:
                break;
        }
    }
    va_end(args);
    return vectorBatch;
}

void AssertIntVectorEquals(IntVector *vector, int32_t *expectedValues)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsValueNull(i)) {
            continue;
        }
        EXPECT_EQ(vector->GetValue(i), expectedValues[i]);
    }
}

void AssertLongVectorEquals(LongVector *vector, int64_t *expectedValues)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsValueNull(i)) {
            continue;
        }
        EXPECT_EQ(vector->GetValue(i), expectedValues[i]);
    }
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
                AssertIntVectorEquals(dynamic_cast<IntVector *>(vector), va_arg(args, int32_t *));
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_LONG:
                AssertLongVectorEquals(dynamic_cast<LongVector *>(vector), va_arg(args, int64_t *));
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE:
                AssertDoubleVectorEquals(dynamic_cast<DoubleVector *>(vector), va_arg(args, double *));
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR:
                AssertVarcharVectorEquals(dynamic_cast<VarcharVector *>(vector), va_arg(args, std::string *));
                break;
            case omniruntime::vec::OMNI_VEC_TYPE_DICTIONARY:
                AssertDictionaryVectorEquals(dynamic_cast<DictionaryVector *>(vector), args);
            default:
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
