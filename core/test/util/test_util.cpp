/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Type Util Class
 */
#include <cmath>
#include <cfloat>
#include "test_util.h"
#include "../../src/vector/dictionary_vector.h"

bool typesMatch(VecType *actualTypes, VecType *expectTypes, int32_t columnNumber);
bool columnMatch(Vector *actualColumn, Vector *expectColumn);

bool vecBatchMatch(VectorBatch *outputPages, VectorBatch *expectPage)
{
    if (outputPages->getRowCount() != expectPage->getRowCount()) {
        return false;
    }

    int32_t columnNumber = outputPages->getVectorCount();
    if (columnNumber != expectPage->getVectorCount()) {
        return false;
    }

    if (!typesMatch(outputPages->getVectorTypes(), expectPage->getVectorTypes(), columnNumber)) {
        return false;
    }

    for (int32_t i = 0; i < columnNumber; i++) {
        if (!columnMatch(outputPages->getVector(i), expectPage->getVector(i))) {
            return false;
        }
    }

    return true;
}

bool typesMatch(VecType *actualTypes, VecType *expectTypes, int32_t columnNumber)
{
    for (int32_t i = 0; i < columnNumber; i++) {
        if (actualTypes[i] != expectTypes[i]) {
            return false;
        }
    }

    return true;
}

bool valueMatch(DictionaryVector *actualVector, DictionaryVector *expectedVector, int32_t position)
{
    VecType type = actualVector->getDictionary()->getType();
    int32_t actualPosition = actualVector->getIds()[position];
    int32_t expectPosition = expectedVector->getIds()[position];
    switch (type) {
        case OMNI_VEC_TYPE_INT: {
            int32_t actual = actualVector->getInt(actualPosition);
            int32_t expect = expectedVector->getInt(expectPosition);
            return actual == expect;
        }
        case OMNI_VEC_TYPE_LONG: {
            int64_t actual = actualVector->getLong(actualPosition);
            int64_t expect = expectedVector->getLong(expectPosition);
            return actual == expect;
        }
        default:
            return false;
    }
}

bool columnMatch(Vector *actualColumn, Vector *expectColumn)
{
    if (actualColumn->getType() != expectColumn->getType()) {
        return false;
    }

    if (actualColumn->getSize() != expectColumn->getSize()) {
        return false;
    }

    bool result = true;
    for (int32_t i = 0; i < actualColumn->getSize(); i++) {
        switch (actualColumn->getType()) {
            case OMNI_VEC_TYPE_INT: {
                int32_t actual = ((IntVector *)actualColumn)->getValue(i);
                int32_t expect = ((IntVector *)expectColumn)->getValue(i);
                result = (actual == expect) & result;
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                int64_t actual = ((LongVector *)actualColumn)->getValue(i);
                int64_t expect = ((LongVector *)expectColumn)->getValue(i);
                result = (actual == expect) & result;
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                double actual = ((DoubleVector *)expectColumn)->getValue(i);
                double expect = ((DoubleVector *)expectColumn)->getValue(i);
                result = (std::fabs(actual - expect) <= DBL_EPSILON) & result;
                break;
            }
            case OMNI_VEC_TYPE_DICTIONARY: {
                result = valueMatch((DictionaryVector *)actualColumn, (DictionaryVector *)expectColumn, i);
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

omniruntime::op::Operator *createTestOperator(OperatorFactory *operatorFactory)
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

void printVecBatch(VectorBatch* vecBatch)
{
    int32_t vectorCount = vecBatch->getVectorCount();
    for (int32_t rowIdx = 0; rowIdx < vecBatch->getVector(0)->getSize(); ++rowIdx) {
        for (int32_t colIdx = 0; colIdx < vectorCount; ++colIdx) {
            auto vecType = vecBatch->getVector(colIdx)->getType();
            auto vector = vecBatch->getVector(colIdx);
            switch (vecType) {
                case OMNI_VEC_TYPE_INT: {
                    IntVector* vec = (IntVector*)vector;
                    std::cout << vec->getValue(rowIdx) << "   ";
                    break;
                }
                case OMNI_VEC_TYPE_LONG: {
                    LongVector* vec = (LongVector*)vector;
                    std::cout << vec->getValue(rowIdx) << "   ";
                    break;
                }
                case OMNI_VEC_TYPE_DOUBLE: {
                    DoubleVector* vec = (DoubleVector*)vector;
                    std::cout << vec->getValue(rowIdx) << "   ";
                    break;
                }
                default:
                    DebugError("Error vector type %d", vecType);
            }
        }
        std::cout << std::endl;
    }
}
