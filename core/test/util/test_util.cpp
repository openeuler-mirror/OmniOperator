/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Type Util Class
 */
#include <cmath>
#include <cfloat>
#include "test_util.h"
#include "../../src/vector/dictionary_vector.h"

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
        switch (actualColumn->GetType().GetId()) {
            case OMNI_VEC_TYPE_INT: {
                int32_t actual = ((IntVector *)actualColumn)->GetValue(i);
                int32_t expect = ((IntVector *)expectColumn)->GetValue(i);
                result = (actual == expect) & result;
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                int64_t actual = ((LongVector *)actualColumn)->GetValue(i);
                int64_t expect = ((LongVector *)expectColumn)->GetValue(i);
                result = (actual == expect) & result;
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                double actual = ((DoubleVector *)expectColumn)->GetValue(i);
                double expect = ((DoubleVector *)expectColumn)->GetValue(i);
                result = (std::fabs(actual - expect) <= DBL_EPSILON) & result;
                break;
            }
            case OMNI_VEC_TYPE_DICTIONARY: {
                result = ValueMatch((DictionaryVector *) actualColumn, (DictionaryVector *) expectColumn, i);
                break;
            }
            default:
                result = false;
        }
        if (!result) {
            return false;
        }
    }

    return result;
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
        opt_module operatorModule = (opt_module) (jitContext->func);
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

void PrintVecBatch(VectorBatch* vecBatch)
{
    int32_t vectorCount = vecBatch->GetVectorCount();
    for (int32_t rowIdx = 0; rowIdx < vecBatch->GetVector(0)->GetSize(); ++rowIdx) {
        for (int32_t colIdx = 0; colIdx < vectorCount; ++colIdx) {
            auto vecType = vecBatch->GetVector(colIdx)->GetType();
            auto vector = vecBatch->GetVector(colIdx);
            switch (vecType.GetId()) {
                case OMNI_VEC_TYPE_INT: {
                    IntVector* vec = (IntVector*)vector;
                    std::cout << vec->GetValue(rowIdx) << "   ";
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    LongVector* vec = (LongVector*)vector;
                    std::cout << vec->GetValue(rowIdx) << "   ";
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    DoubleVector* vec = (DoubleVector*)vector;
                    std::cout << vec->GetValue(rowIdx) << "   ";
                    break;
                }
                case OMNI_VEC_TYPE_CONTAINER: {
                    ContainerVector* vec = reinterpret_cast<ContainerVector*>(vector);
                    DoubleVector* doubleVec = reinterpret_cast<DoubleVector*>(vec->getValue(0));
                    double avgVal = doubleVec->GetValue(rowIdx);
                    LongVector* longVec = reinterpret_cast<LongVector*>(vec->getValue(1));
                    int64_t avgCnt = longVec->GetValue(rowIdx);
                    std::cout << avgVal << "/" << avgCnt << std::endl;
                    break;
                }
                default:
                    DebugError("Error vector type %d", vecType.GetId());
            }
        }
        std::cout << std::endl;
    }
}
