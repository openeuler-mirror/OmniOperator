#include <cmath>
#include <cfloat>
#include "test_util.h"

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
    nativeOperator = operatorFactory->createOperator();
#else
    JitContext *jitContext = operatorFactory->getJitContext();
    if (jitContext == nullptr) {
        nativeOperator = operatorFactory->createOperator();
    } else {
        opt_module operatorModule = (opt_module) (jitContext->func);
        nativeOperator = operatorModule(operatorFactory);
    }
#endif
    return nativeOperator;
}
