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
    inline static int32_t CompareVectorAtPosition(int32_t colTypeId, omniruntime::vec::Vector *leftColumn,
        int32_t leftColumnPosition, omniruntime::vec::Vector *rightColumn, int32_t rightColumnPosition)
    {
        switch (colTypeId) {
            case omniruntime::vec::OMNI_VEC_TYPE_INT:
                return ((omniruntime::vec::IntVector *)leftColumn)->GetValue(leftColumnPosition) -
                    ((omniruntime::vec::IntVector *)rightColumn)->GetValue(rightColumnPosition);
            case omniruntime::vec::OMNI_VEC_TYPE_LONG:
                return ((omniruntime::vec::LongVector *)leftColumn)->GetValue(leftColumnPosition) -
                    ((omniruntime::vec::LongVector *)rightColumn)->GetValue(rightColumnPosition);
            case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE:
                if (((omniruntime::vec::DoubleVector *)leftColumn)->GetValue(leftColumnPosition) >
                    ((omniruntime::vec::DoubleVector *)rightColumn)->GetValue(rightColumnPosition)) {
                    return 1;
                } else if (((omniruntime::vec::DoubleVector *)leftColumn)->GetValue(leftColumnPosition) <
                    ((omniruntime::vec::DoubleVector *)rightColumn)->GetValue(rightColumnPosition)) {
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
