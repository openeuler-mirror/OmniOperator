/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#ifndef OMNI_RUNTIME_OPERATOR_UTIL_H
#define OMNI_RUNTIME_OPERATOR_UTIL_H

#include <memory>
#include "../../vector/int_vector.h"
#include "../../vector/long_vector.h"
#include "../../vector/double_vector.h"

class OperatorUtil {
public:
    static int32_t compareVectorAtPosition(VecType colType, Vector *leftColumn, int32_t leftColumnPosition,
        Vector *rightColumn, int32_t rightColumnPosition)
    {
        switch (colType) {
            case OMNI_VEC_TYPE_INT:
                return ((IntVector *)leftColumn)->GetValue(leftColumnPosition) -
                    ((IntVector *)rightColumn)->GetValue(rightColumnPosition);
            case OMNI_VEC_TYPE_LONG:
                return ((LongVector *)leftColumn)->GetValue(leftColumnPosition) -
                    ((LongVector *)rightColumn)->GetValue(rightColumnPosition);
            case OMNI_VEC_TYPE_DOUBLE:
                if (((DoubleVector *)leftColumn)->GetValue(leftColumnPosition) >
                    ((DoubleVector *)rightColumn)->GetValue(rightColumnPosition)) {
                    return 1;
                } else if (((DoubleVector *)leftColumn)->GetValue(leftColumnPosition) <
                    ((DoubleVector *)rightColumn)->GetValue(rightColumnPosition)) {
                    return -1;
                } else {
                    return 0;
                }
            default:
                break;
        }
        return 0;
    }
};


#endif // OMNI_RUNTIME_OPERATOR_UTIL_H
