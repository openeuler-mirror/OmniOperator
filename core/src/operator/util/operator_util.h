//
// Created by root on 7/2/21.
//

#ifndef OMNI_RUNTIME_OPERATOR_UTIL_H
#define OMNI_RUNTIME_OPERATOR_UTIL_H

#include "../../vector/int_vector.h"
#include "../../vector/long_vector.h"
#include "../../vector/double_vector.h"

class OperatorUtil {
public:
    static int32_t compareVectorAtPosition(VecType colType, Vector *leftColumn, int32_t leftColumnPosition, Vector *rightColumn,
                                           int32_t rightColumnPosition) {
        switch (colType) {
            case OMNI_VEC_TYPE_INT:
                return ((IntVector *) leftColumn)->getValue(leftColumnPosition) -
                       ((IntVector *) rightColumn)->getValue(rightColumnPosition);
            case OMNI_VEC_TYPE_LONG:
                return ((LongVector *) leftColumn)->getValue(leftColumnPosition) -
                       ((LongVector *) rightColumn)->getValue(rightColumnPosition);
            case OMNI_VEC_TYPE_DOUBLE:
                if (((DoubleVector *) leftColumn)->getValue(leftColumnPosition) >
                    ((DoubleVector *) rightColumn)->getValue(rightColumnPosition)) {
                    return 1;
                } else if (((DoubleVector *) leftColumn)->getValue(leftColumnPosition) <
                           ((DoubleVector *) rightColumn)->getValue(rightColumnPosition)) {
                    return -1;
                }
                return 0;
            default:
                break;
        }
    }
};


#endif //OMNI_RUNTIME_OPERATOR_UTIL_H
