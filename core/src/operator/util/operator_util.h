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
#include "../../vector/decimal128.h"
#include "../../vector/decimal128_vector.h"
#include "../../vector/boolean_vector.h"

class OperatorUtil {
public:
    using CompareStatus = enum CompareStatus {
        COMPARE_STATUS_LESS_THAN = -1,
        COMPARE_STATUS_EQUAL,
        COMPARE_STATUS_GREATER_THAN,
        COMPARE_STATUS_OTHER,
    };

    static constexpr int32_t MAX_VEC_BATCH_SIZE_IN_BYTES = 1024 * 1024;
    static const int32_t SIZE_OF_BYTE = sizeof(int8_t);
    static const int32_t SIZE_OF_BOOL = SIZE_OF_BYTE;
    static const int32_t SIZE_OF_SHORT = sizeof(int16_t);
    static const int32_t SIZE_OF_INT = sizeof(int32_t);
    static const int32_t SIZE_OF_LONG = sizeof(int64_t);
    static const int32_t SIZE_OF_DOUBLE = sizeof(double);
    static const int32_t SIZE_OF_DECIMAL64 = SIZE_OF_LONG;
    static const int32_t SIZE_OF_DECIMAL128 = SIZE_OF_LONG << 1;
    static const int32_t SIZE_OF_DATE32 = SIZE_OF_INT;

    static int32_t GetTypeSize(const omniruntime::vec::VecType &vecType)
    {
        switch (vecType.GetId()) {
            case omniruntime::vec::OMNI_VEC_TYPE_INT:
                return OperatorUtil::SIZE_OF_INT;
            case omniruntime::vec::OMNI_VEC_TYPE_LONG:
                return OperatorUtil::SIZE_OF_LONG;
            case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE:
                return OperatorUtil::SIZE_OF_DOUBLE;
            case omniruntime::vec::OMNI_VEC_TYPE_BOOLEAN:
                return OperatorUtil::SIZE_OF_BOOL;
            case omniruntime::vec::OMNI_VEC_TYPE_SHORT:
                return OperatorUtil::SIZE_OF_SHORT;
            case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL64:
                return OperatorUtil::SIZE_OF_DECIMAL64;
            case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL128:
                return OperatorUtil::SIZE_OF_DECIMAL128;
            case omniruntime::vec::OMNI_VEC_TYPE_DATE32:
                return OperatorUtil::SIZE_OF_DATE32;
            case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR:
                return ((omniruntime::vec::VarcharVecType &)vecType).GetWidth();
            default:
                return 0;
        }
    }

    static int32_t GetOutputRowSize(const std::vector<omniruntime::vec::VecType> &vecTypes, const int32_t *outputCols,
        int32_t outputColsCount)
    {
        int32_t rowSize = 0;
        for (int32_t i = 0; i < outputColsCount; i++) {
            rowSize += OperatorUtil::GetTypeSize(vecTypes[outputCols[i]]);
        }
        return rowSize;
    }

    static int32_t GetRowSize(const std::vector<omniruntime::vec::VecType> &vecTypes)
    {
        int32_t rowSize = 0;
        for (int32_t i = 0; i < vecTypes.size(); i++) {
            rowSize += OperatorUtil::GetTypeSize(vecTypes[i]);
        }
        return rowSize;
    }

    static int32_t GetMaxRowCount(int32_t rowSize)
    {
        return (MAX_VEC_BATCH_SIZE_IN_BYTES + rowSize - 1) / rowSize;
    }

    static int32_t GetMaxRowCount(const std::vector<omniruntime::vec::VecType> &vecTypes, const int32_t *outputCols,
        int32_t outputColsCount)
    {
        int32_t rowSize = GetOutputRowSize(vecTypes, outputCols, outputColsCount);
        return GetMaxRowCount(rowSize);
    }

    static int32_t GetVecBatchCount(int32_t positionCount, int32_t maxRowCount)
    {
        return ((positionCount + maxRowCount - 1) / maxRowCount);
    }

    static ALWAYS_INLINE int32_t CompareVectorAtPosition(int32_t colTypeId, omniruntime::vec::Vector *leftColumn,
        int32_t leftColumnPosition, omniruntime::vec::Vector *rightColumn, int32_t rightColumnPosition)
    {
        switch (colTypeId) {
            case omniruntime::vec::OMNI_VEC_TYPE_BOOLEAN:
                return (static_cast<omniruntime::vec::BooleanVector *>(leftColumn)->GetValue(leftColumnPosition) -
                    static_cast<omniruntime::vec::BooleanVector *>(rightColumn)->GetValue(rightColumnPosition));
            case omniruntime::vec::OMNI_VEC_TYPE_INT:
            case omniruntime::vec::OMNI_VEC_TYPE_DATE32:
                return (static_cast<omniruntime::vec::IntVector *>(leftColumn)->GetValue(leftColumnPosition) -
                    static_cast<omniruntime::vec::IntVector *>(rightColumn)->GetValue(rightColumnPosition));
            case omniruntime::vec::OMNI_VEC_TYPE_LONG:
            case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL64:
                return (static_cast<omniruntime::vec::LongVector *>(leftColumn)->GetValue(leftColumnPosition) -
                    static_cast<omniruntime::vec::LongVector *>(rightColumn)->GetValue(rightColumnPosition));
            case omniruntime::vec::OMNI_VEC_TYPE_DOUBLE:
                return CompareDouble(static_cast<omniruntime::vec::DoubleVector *>(leftColumn), leftColumnPosition,
                    static_cast<omniruntime::vec::DoubleVector *>(rightColumn), rightColumnPosition);
            case omniruntime::vec::OMNI_VEC_TYPE_VARCHAR:
                return CompareVarchar(static_cast<omniruntime::vec::VarcharVector *>(leftColumn), leftColumnPosition,
                    static_cast<omniruntime::vec::VarcharVector *>(rightColumn), rightColumnPosition);
            case omniruntime::vec::OMNI_VEC_TYPE_DECIMAL128:
                return CompareDecimal128(static_cast<omniruntime::vec::Decimal128Vector *>(leftColumn),
                    leftColumnPosition, static_cast<omniruntime::vec::Decimal128Vector *>(rightColumn),
                    rightColumnPosition);
            default:
                break;
        }
        return 0;
    }

    static ALWAYS_INLINE CompareStatus CompareNull(omniruntime::vec::Vector *leftColumn, int32_t leftPosition,
        omniruntime::vec::Vector *rightColumn, int32_t rightPosition, int32_t nullsFirst)
    {
        bool leftIsNull = leftColumn->IsValueNull(leftPosition);
        bool rightIsNull = rightColumn->IsValueNull(rightPosition);

        if (leftIsNull && rightIsNull) {
            return COMPARE_STATUS_EQUAL;
        }
        if (leftIsNull) {
            return nullsFirst ? COMPARE_STATUS_LESS_THAN : COMPARE_STATUS_GREATER_THAN;
        }
        if (rightIsNull) {
            return nullsFirst ? COMPARE_STATUS_GREATER_THAN : COMPARE_STATUS_LESS_THAN;
        }
        return COMPARE_STATUS_OTHER;
    }

private:
    static int32_t CompareDouble(omniruntime::vec::DoubleVector *leftColumn, int32_t leftColumnPosition,
        omniruntime::vec::DoubleVector *rightColumn, int32_t rightColumnPosition)
    {
        if (leftColumn->GetValue(leftColumnPosition) > rightColumn->GetValue(rightColumnPosition)) {
            return COMPARE_STATUS_GREATER_THAN;
        } else if (leftColumn->GetValue(leftColumnPosition) < rightColumn->GetValue(rightColumnPosition)) {
            return COMPARE_STATUS_LESS_THAN;
        } else {
            return COMPARE_STATUS_EQUAL;
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
            return (result > 0) ? COMPARE_STATUS_GREATER_THAN : COMPARE_STATUS_LESS_THAN;
        } else if (leftLength == rightLength) {
            return COMPARE_STATUS_EQUAL;
        } else {
            return (leftLength > rightLength) ? COMPARE_STATUS_GREATER_THAN : COMPARE_STATUS_LESS_THAN;
        }
    }

    static int32_t CompareDecimal128(omniruntime::vec::Decimal128Vector *leftColumn, int32_t leftColumnPosition,
        omniruntime::vec::Decimal128Vector *rightColumn, int32_t rightColumnPosition)
    {
        omniruntime::vec::Decimal128 leftValue = leftColumn->GetValue(leftColumnPosition);
        omniruntime::vec::Decimal128 rightValue = rightColumn->GetValue(rightColumnPosition);
        if (leftValue > rightValue) {
            return COMPARE_STATUS_GREATER_THAN;
        } else if (leftValue < rightValue) {
            return COMPARE_STATUS_LESS_THAN;
        } else {
            return COMPARE_STATUS_EQUAL;
        }
    }
};

#endif // OMNI_RUNTIME_OPERATOR_UTIL_H
