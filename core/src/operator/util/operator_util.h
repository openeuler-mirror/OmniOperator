/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#ifndef OMNI_RUNTIME_OPERATOR_UTIL_H
#define OMNI_RUNTIME_OPERATOR_UTIL_H

#include <memory>
#include "../../vector/int_vector.h"
#include "../../vector/long_vector.h"
#include "../../vector/double_vector.h"
#include "../../vector/varchar_vector.h"

class OperatorUtil {
public:
    static int32_t CompareVectorAtPosition(int32_t colTypeId, omniruntime::vec::Vector *leftColumn,
        int32_t leftColumnPosition, omniruntime::vec::Vector *rightColumn, int32_t rightColumnPosition)
    {
        switch (colTypeId) {
            case omniruntime::vec::OMNI_VEC_TYPE_INT:
                return (static_cast<omniruntime::vec::IntVector *>(leftColumn)->GetValue(leftColumnPosition) -
                        static_cast<omniruntime::vec::IntVector *>(rightColumn)->GetValue(rightColumnPosition));
            case omniruntime::vec::OMNI_VEC_TYPE_LONG:
                return (static_cast<omniruntime::vec::LongVector *>(leftColumn)->GetValue(leftColumnPosition) -
                        static_cast<omniruntime::vec::LongVector *>(rightColumn)->GetValue(rightColumnPosition));
            case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE:
                return CompareDouble(static_cast<omniruntime::vec::DoubleVector *>(leftColumn), leftColumnPosition,
                                     static_cast<omniruntime::vec::DoubleVector *>(rightColumn), rightColumnPosition);
            case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR:
                return CompareVarchar(static_cast<omniruntime::vec::VarcharVector *>(leftColumn), leftColumnPosition,
                                      static_cast<omniruntime::vec::VarcharVector *>(rightColumn), rightColumnPosition);
            default:
                break;
        }
        return 0;
    }

private:
    static int32_t CompareDouble(omniruntime::vec::DoubleVector *leftColumn, int32_t leftColumnPosition,
                                 omniruntime::vec::DoubleVector *rightColumn, int32_t rightColumnPosition)
    {
        if (leftColumn->GetValue(leftColumnPosition) > rightColumn->GetValue(rightColumnPosition)) {
            return 1;
        } else if (leftColumn->GetValue(leftColumnPosition) < rightColumn->GetValue(rightColumnPosition)) {
            return -1;
        } else {
            return 0;
        }
    }

    static int32_t CompareVarchar(omniruntime::vec::VarcharVector *leftColumn, int32_t leftColumnPosition,
                                  omniruntime::vec::VarcharVector *rightColumn, int32_t rightColumnPosition)
    {
        uint8_t *leftValue = nullptr;
        int32_t leftLength = leftColumn->GetValue(leftColumnPosition, &leftValue);
        uint8_t *rightValue = nullptr;
        int32_t rightLength = rightColumn->GetValue(rightColumnPosition, &rightValue);
        int32_t result = memcmp(leftValue, rightValue, std::min(leftLength, rightLength));
        if (result != 0) {
            return result;
        } else {
            return (leftLength - rightLength);
        }
    }
};

#endif // OMNI_RUNTIME_OPERATOR_UTIL_H
