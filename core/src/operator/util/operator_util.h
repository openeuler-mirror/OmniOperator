/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#ifndef OMNI_RUNTIME_OPERATOR_UTIL_H
#define OMNI_RUNTIME_OPERATOR_UTIL_H

#include <memory>
#include "../../vector/vector_common.h"
#include "../projection/projection.h"

namespace omniruntime {
namespace op {
using namespace vec;
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

    static int32_t GetTypeSize(const VecType &vecType)
    {
        switch (vecType.GetId()) {
            case OMNI_VEC_TYPE_INT:
                return OperatorUtil::SIZE_OF_INT;
            case OMNI_VEC_TYPE_LONG:
                return OperatorUtil::SIZE_OF_LONG;
            case OMNI_VEC_TYPE_DOUBLE:
                return OperatorUtil::SIZE_OF_DOUBLE;
            case OMNI_VEC_TYPE_BOOLEAN:
                return OperatorUtil::SIZE_OF_BOOL;
            case OMNI_VEC_TYPE_SHORT:
                return OperatorUtil::SIZE_OF_SHORT;
            case OMNI_VEC_TYPE_DECIMAL64:
                return OperatorUtil::SIZE_OF_DECIMAL64;
            case OMNI_VEC_TYPE_DECIMAL128:
                return OperatorUtil::SIZE_OF_DECIMAL128;
            case OMNI_VEC_TYPE_DATE32:
                return OperatorUtil::SIZE_OF_DATE32;
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR:
                return ((VarcharVecType &)vecType).GetWidth();
            default:
                return 0;
        }
    }

    static int32_t GetOutputRowSize(const std::vector<VecType> &vecTypes, const int32_t *outputCols,
        int32_t outputColsCount)
    {
        int32_t rowSize = 0;
        for (int32_t i = 0; i < outputColsCount; i++) {
            rowSize += OperatorUtil::GetTypeSize(vecTypes[outputCols[i]]);
        }
        return rowSize;
    }

    static int32_t GetRowSize(const std::vector<VecType> &vecTypes)
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

    static int32_t GetMaxRowCount(const std::vector<VecType> &vecTypes, const int32_t *outputCols,
        int32_t outputColsCount)
    {
        int32_t rowSize = GetOutputRowSize(vecTypes, outputCols, outputColsCount);
        return GetMaxRowCount(rowSize);
    }

    static int32_t GetVecBatchCount(int32_t positionCount, int32_t maxRowCount)
    {
        return ((positionCount + maxRowCount - 1) / maxRowCount);
    }

    static int32_t CompareVectorAtPosition(int32_t colTypeId, Vector *leftColumn,
        int32_t leftColumnPosition, Vector *rightColumn, int32_t rightColumnPosition)
    {
        switch (colTypeId) {
            case OMNI_VEC_TYPE_BOOLEAN:
                return (static_cast<BooleanVector *>(leftColumn)->GetValue(leftColumnPosition) -
                    static_cast<BooleanVector *>(rightColumn)->GetValue(rightColumnPosition));
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
                return (static_cast<IntVector *>(leftColumn)->GetValue(leftColumnPosition) -
                    static_cast<IntVector *>(rightColumn)->GetValue(rightColumnPosition));
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                return (static_cast<LongVector *>(leftColumn)->GetValue(leftColumnPosition) -
                    static_cast<LongVector *>(rightColumn)->GetValue(rightColumnPosition));
            case OMNI_VEC_TYPE_DOUBLE:
                return CompareDouble(leftColumn, leftColumnPosition, rightColumn, rightColumnPosition);
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR:
                return CompareVarchar(leftColumn, leftColumnPosition, rightColumn, rightColumnPosition);
            case OMNI_VEC_TYPE_DECIMAL128:
                return CompareDecimal128(leftColumn, leftColumnPosition, rightColumn, rightColumnPosition);
            default:
                break;
        }
        return 0;
    }

    static ALWAYS_INLINE CompareStatus CompareNull(Vector *leftColumn, int32_t leftPosition, Vector *rightColumn,
        int32_t rightPosition, int32_t nullsFirst)
    {
        bool leftIsNull = leftColumn->IsValueNull(leftPosition);
        bool rightIsNull = rightColumn->IsValueNull(rightPosition);
        // we want to check the most likely comparison first
        if (!leftIsNull && !rightIsNull) {
            return COMPARE_STATUS_OTHER;
        }
        if (leftIsNull) {
            if (rightIsNull) {
                return COMPARE_STATUS_EQUAL;
            }
            return nullsFirst ? COMPARE_STATUS_LESS_THAN : COMPARE_STATUS_GREATER_THAN;
        }
        // we are left with right only
        return nullsFirst ? COMPARE_STATUS_GREATER_THAN : COMPARE_STATUS_LESS_THAN;
    }

    template <typename V>
    static ALWAYS_INLINE int32_t CompareTemplate(Vector *leftVector, int32_t leftPosition, Vector *rightVector,
        int32_t rightPosition)
    {
        return static_cast<V *>(leftVector)->GetValue(leftPosition) -
            static_cast<V *>(rightVector)->GetValue(rightPosition);
    }

    static ALWAYS_INLINE int32_t CompareDouble(Vector *leftColumn, int32_t leftColumnPosition, Vector *rightColumn,
        int32_t rightColumnPosition)
    {
        double leftDouble = static_cast<DoubleVector *>(leftColumn)->GetValue(leftColumnPosition);
        double rightDouble = static_cast<DoubleVector *>(rightColumn)->GetValue(rightColumnPosition);
        if (leftDouble > rightDouble) {
            return COMPARE_STATUS_GREATER_THAN;
        } else if (leftDouble < rightDouble) {
            return COMPARE_STATUS_LESS_THAN;
        } else {
            return COMPARE_STATUS_EQUAL;
        }
    }

    static ALWAYS_INLINE int32_t CompareVarchar(Vector *leftColumn, int32_t leftColumnPosition, Vector *rightColumn,
        int32_t rightColumnPosition)
    {
        auto leftVarCharColumn = static_cast<VarcharVector *>(leftColumn);
        auto rightVarCharColumn = static_cast<VarcharVector *>(rightColumn);
        uint8_t *leftValue = nullptr;
        int32_t leftLength = leftVarCharColumn->GetValue(leftColumnPosition, &leftValue);
        uint8_t *rightValue = nullptr;
        int32_t rightLength = rightVarCharColumn->GetValue(rightColumnPosition, &rightValue);
        int32_t result = memcmp(leftValue, rightValue, std::min(leftLength, rightLength));
        if (result != 0) {
            return (result > 0) ? COMPARE_STATUS_GREATER_THAN : COMPARE_STATUS_LESS_THAN;
        } else if (leftLength == rightLength) {
            return COMPARE_STATUS_EQUAL;
        } else {
            return (leftLength > rightLength) ? COMPARE_STATUS_GREATER_THAN : COMPARE_STATUS_LESS_THAN;
        }
    }

    static ALWAYS_INLINE int32_t CompareDecimal128(Vector *leftColumn, int32_t leftColumnPosition, Vector *rightColumn,
        int32_t rightColumnPosition)
    {
        auto leftDecimalColumn = static_cast<Decimal128Vector *>(leftColumn);
        auto rightDecimalColumn = static_cast<Decimal128Vector *>(rightColumn);
        Decimal128 leftValue = leftDecimalColumn->GetValue(leftColumnPosition);
        Decimal128 rightValue = rightDecimalColumn->GetValue(rightColumnPosition);
        if (leftValue > rightValue) {
            return COMPARE_STATUS_GREATER_THAN;
        } else if (leftValue < rightValue) {
            return COMPARE_STATUS_LESS_THAN;
        } else {
            return COMPARE_STATUS_EQUAL;
        }
    }

    static void CreateProjectFuncs(const VecTypes &intputTypes,
        std::vector<omniruntime::expressions::Expr *> projectKeys,
        int32_t projectKeysCount, std::vector<VecType> &newIntputTypes,
        std::vector<std::unique_ptr<RowProjection>> &rowProjections, std::vector<int32_t> &projectCols,
        std::vector<RowProjFunc> &projectFuncs);

    static void CreateRequiredProjectFuncs(const VecTypes &intputTypes, omniruntime::expressions::Expr *projectKeys[],
        int32_t projectKeysCount, std::vector<VecType> &newIntputTypes,
        std::vector<std::unique_ptr<RowProjection>> &rowProjections, std::vector<int32_t> &projectCols,
        std::vector<int32_t> &allCols, std::vector<RowProjFunc> &projectFuncs);

    static VectorBatch *ProjectVectors(VectorBatch *inputVecBatch, const VecTypes &inputTypes,
        const std::vector<RowProjFunc> &projectFuncs, const std::vector<int32_t> &projectCols);

    static VectorBatch *ProjectRequiredVectors(VectorBatch *inputVecBatch, const VecTypes &inputTypes,
        const std::vector<RowProjFunc> &projectFuncs, const std::vector<int32_t> &projectCols);

private:
    static void ProjectVectors(const VecTypes &newInputTypes, const std::vector<RowProjFunc> &projectFuncs,
        const std::vector<int32_t> &projectCols, int64_t *values, int64_t *valueNulls, int64_t *valueOffsets,
        int64_t *dictVectorAddrs, int32_t rowCount, VectorBatch *newVecBatch);

    static void ProjectRequiredVectors(const VecTypes &newInputTypes, const std::vector<RowProjFunc> &projectFuncs,
        const std::vector<int32_t> &projectCols, int64_t *values, int64_t *valueNulls, int64_t *valueOffsets,
        int64_t *dictVectorAddrs, int32_t rowCount, VectorBatch *newVecBatch);
};
}
}
#endif // OMNI_RUNTIME_OPERATOR_UTIL_H
