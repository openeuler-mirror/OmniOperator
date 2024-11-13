/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */
#ifndef OMNI_RUNTIME_OPERATOR_UTIL_H
#define OMNI_RUNTIME_OPERATOR_UTIL_H

#include <memory>
#include "operator/projection/projection.h"
#include "vector/vector.h"
#include "vector/unsafe_vector.h"
#include "type/data_type.h"
#include "type/decimal128.h"
#include "operator/omni_id_type_vector_traits.h"
#include "operator/execution_context.h"
#include "operator/filter/filter_and_project.h"

namespace omniruntime {
namespace op {
class OperatorUtil {
    template <typename T> using LargeStringContainer = vec::LargeStringContainer<T>;
    using VarcharVector = vec::Vector<LargeStringContainer<std::string_view>>;

public:
    using CompareStatus = enum CompareStatus {
        COMPARE_STATUS_LESS_THAN = -1,
        COMPARE_STATUS_EQUAL,
        COMPARE_STATUS_GREATER_THAN,
        COMPARE_STATUS_OTHER,
    };

    static constexpr int32_t DEFAULT_CHAR_LENGTH = 200;
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
    static const int32_t SIZE_OF_DATE64 = SIZE_OF_LONG;

    using CompareFunc = int32_t (*)(vec::BaseVector *leftVector, int32_t leftPosition, vec::BaseVector *rightVector,
        int32_t rightPosition);

    static int32_t GetTypeSize(const DataTypePtr dataTypePtr)
    {
        switch (dataTypePtr->GetId()) {
            case OMNI_INT:
                return OperatorUtil::SIZE_OF_INT;
            case OMNI_TIMESTAMP:
            case OMNI_LONG:
                return OperatorUtil::SIZE_OF_LONG;
            case OMNI_DOUBLE:
                return OperatorUtil::SIZE_OF_DOUBLE;
            case OMNI_BOOLEAN:
                return OperatorUtil::SIZE_OF_BOOL;
            case OMNI_SHORT:
                return OperatorUtil::SIZE_OF_SHORT;
            case OMNI_DECIMAL64:
                return OperatorUtil::SIZE_OF_DECIMAL64;
            case OMNI_DECIMAL128:
                return OperatorUtil::SIZE_OF_DECIMAL128;
            case OMNI_DATE32:
                return OperatorUtil::SIZE_OF_DATE32;
            case OMNI_DATE64:
                return OperatorUtil::SIZE_OF_DATE64;
            case OMNI_CHAR: {
                // if width is not set (which is the case when width=CHAR_MAX_WIDTH), we use 'DEFAULT_CHAR_LENGTH' width
                // otherwise, estimation of row bytes would be too large and could overflow and be
                const auto width = static_cast<VarcharDataType &>(*dataTypePtr).GetWidth();
                return width < CHAR_MAX_WIDTH ? width : DEFAULT_CHAR_LENGTH;
            }
            case OMNI_VARCHAR: {
                // if width is not set (which is the case when width=INT_MAX), we use default DEFAULT_CHAR_LENGTH width
                // otherwise, estimation of row bytes would be too large and could overflow and be
                const auto width = static_cast<VarcharDataType &>(*dataTypePtr).GetWidth();
                return width < INT_MAX ? width : DEFAULT_CHAR_LENGTH;
            }
            default:
                return 0;
        }
    }

    static int32_t GetOutputRowSize(const std::vector<DataTypePtr> &dataTypes, const int32_t *outputCols,
        int32_t outputColsCount)
    {
        int32_t rowSize = 0;
        for (int32_t i = 0; i < outputColsCount; i++) {
            rowSize += OperatorUtil::GetTypeSize(dataTypes[outputCols[i]]);
        }
        return rowSize;
    }

    static int32_t GetRowSize(const std::vector<DataTypePtr> &dataTypes)
    {
        int32_t rowSize = 0;
        for (const auto &dataType : dataTypes) {
            rowSize += OperatorUtil::GetTypeSize(dataType);
        }
        return rowSize;
    }

    static int32_t GetMaxRowCount(int32_t rowSize)
    {
        ASSERT(rowSize != 0);
        return (MAX_VEC_BATCH_SIZE_IN_BYTES + rowSize - 1) / rowSize;
    }

    static int32_t GetMaxRowCount(const std::vector<DataTypePtr> &dataTypes, const int32_t *outputCols,
        int32_t outputColsCount)
    {
        int32_t rowSize = GetOutputRowSize(dataTypes, outputCols, outputColsCount);
        return GetMaxRowCount(rowSize);
    }

    static int32_t GetVecBatchCount(int64_t positionCount, int64_t maxRowCount)
    {
        ASSERT(maxRowCount != 0);
        return static_cast<int32_t>((positionCount + maxRowCount - 1) / maxRowCount);
    }

    static int32_t CompareVectorAtPosition(int32_t colTypeId, vec::BaseVector *leftColumn, int32_t leftColumnPosition,
        vec::BaseVector *rightColumn, int32_t rightColumnPosition)
    {
        switch (colTypeId) {
            case OMNI_BOOLEAN:
                return CompareTemplate<bool>(leftColumn, leftColumnPosition, rightColumn, rightColumnPosition);
            case OMNI_INT:
            case OMNI_DATE32:
                return CompareTemplate<int32_t>(leftColumn, leftColumnPosition, rightColumn, rightColumnPosition);
            case OMNI_SHORT:
                return CompareTemplate<int16_t>(leftColumn, leftColumnPosition, rightColumn, rightColumnPosition);
            case OMNI_LONG:
            case OMNI_TIMESTAMP:
            case OMNI_DECIMAL64:
                return CompareTemplate<int64_t>(leftColumn, leftColumnPosition, rightColumn, rightColumnPosition);
            case OMNI_DOUBLE:
                return CompareDouble(leftColumn, leftColumnPosition, rightColumn, rightColumnPosition);
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                return CompareVarchar(leftColumn, leftColumnPosition, rightColumn, rightColumnPosition);
            case OMNI_DECIMAL128:
                return CompareTemplate<type::Decimal128>(leftColumn, leftColumnPosition, rightColumn,
                    rightColumnPosition);
            default:
                break;
        }
        return 0;
    }

    static ALWAYS_INLINE CompareStatus CompareNull(vec::BaseVector *leftColumn, int32_t leftPosition,
        vec::BaseVector *rightColumn, int32_t rightPosition, int32_t nullsFirst)
    {
        bool leftIsNull = leftColumn->IsNull(leftPosition);
        bool rightIsNull = rightColumn->IsNull(rightPosition);
        // we want to check the most likely comparison first
        if (!leftIsNull && !rightIsNull) {
            return COMPARE_STATUS_OTHER;
        }
        if (leftIsNull) {
            if (rightIsNull) {
                return COMPARE_STATUS_EQUAL;
            }
            return (nullsFirst != 0) ? COMPARE_STATUS_LESS_THAN : COMPARE_STATUS_GREATER_THAN;
        }
        // we are left with right only
        return (nullsFirst != 0) ? COMPARE_STATUS_GREATER_THAN : COMPARE_STATUS_LESS_THAN;
    }

    template <bool IsAscending, bool IsNullsFirst>
    static ALWAYS_INLINE int32_t CompareNull(bool isNullLeft, bool isNullRight)
    {
        if (isNullLeft && isNullRight) {
            return COMPARE_STATUS_EQUAL;
        }
        if (!isNullLeft && !isNullRight) {
            return COMPARE_STATUS_OTHER;
        }

        int32_t nullResult = 0;
        if (isNullLeft) {
            // left is null, but right is not null
            if constexpr (IsNullsFirst) {
                nullResult = COMPARE_STATUS_LESS_THAN;
            } else {
                nullResult = COMPARE_STATUS_GREATER_THAN;
            }
        } else {
            // left is not null, but right is null
            if constexpr (IsNullsFirst) {
                nullResult = COMPARE_STATUS_GREATER_THAN;
            } else {
                nullResult = COMPARE_STATUS_LESS_THAN;
            }
        }
        if constexpr (IsAscending) {
            return nullResult;
        } else {
            return -nullResult;
        }
    }

    template <typename T, bool IsAscending, bool NeedCompareNull, bool IsNullsFirst>
    static ALWAYS_INLINE int32_t CompareValue(vec::BaseVector *leftVector, int32_t leftPosition,
            vec::BaseVector *rightVector, int32_t rightPosition)
    {
        if constexpr (NeedCompareNull) {
            bool isNullLeft = leftVector->IsNull(leftPosition);
            bool isNullRight = rightVector->IsNull(rightPosition);
            auto nullResult = CompareNull<IsAscending, IsNullsFirst>(isNullLeft, isNullRight);
            if (nullResult != COMPARE_STATUS_OTHER) {
                return nullResult;
            }
        }

        int32_t result = 0;
        if constexpr (std::is_same_v<T, std::string_view>) {
            std::string_view leftValue = reinterpret_cast<VarcharVector *>(leftVector)->GetValue(leftPosition);
            std::string_view rightValue = reinterpret_cast<VarcharVector *>(rightVector)->GetValue(rightPosition);
            auto leftLength = leftValue.length();
            auto rightLength = rightValue.length();
            result = memcmp(leftValue.data(), rightValue.data(), std::min(leftLength, rightLength));
            if (result == 0) {
                if (leftLength > rightLength) {
                    result = COMPARE_STATUS_GREATER_THAN;
                } else if (leftLength < rightLength) {
                    result = COMPARE_STATUS_LESS_THAN;
                }
            }
        } else {
            T left;
            T right;
            if (leftVector->GetEncoding() == vec::OMNI_DICTIONARY) {
                left = static_cast<omniruntime::vec::Vector<vec::DictionaryContainer<T>> *>(leftVector)
                        ->GetValue(leftPosition);
            } else {
                left = static_cast<omniruntime::vec::Vector<T> *>(leftVector)->GetValue(leftPosition);
            }
            if (rightVector->GetEncoding() == vec::OMNI_DICTIONARY) {
                right = static_cast<omniruntime::vec::Vector<vec::DictionaryContainer<T>> *>(rightVector)
                        ->GetValue(rightPosition);
            } else {
                right = static_cast<omniruntime::vec::Vector<T> *>(rightVector)->GetValue(rightPosition);
            }
            result = left > right ? COMPARE_STATUS_GREATER_THAN :
                     (left < right ? COMPARE_STATUS_LESS_THAN : COMPARE_STATUS_EQUAL);
        }
        if constexpr (IsAscending) {
            return result;
        } else {
            return -result;
        }
    }

    template <typename T, bool IsAscending, bool NeedCompareNull, bool IsNullsFirst>
    static ALWAYS_INLINE int32_t CompareFlatTemplate(vec::BaseVector *leftVector, int32_t leftPosition,
        vec::BaseVector *rightVector, int32_t rightPosition)
    {
        if constexpr (NeedCompareNull) {
            bool isNullLeft = leftVector->IsNull(leftPosition);
            bool isNullRight = rightVector->IsNull(rightPosition);
            auto nullResult = CompareNull<IsAscending, IsNullsFirst>(isNullLeft, isNullRight);
            if (nullResult != COMPARE_STATUS_OTHER) {
                return nullResult;
            }
        }

        int32_t result = 0;
        if constexpr (std::is_same_v<T, std::string_view>) {
            std::string_view leftValue = reinterpret_cast<VarcharVector *>(leftVector)->GetValue(leftPosition);
            std::string_view rightValue = reinterpret_cast<VarcharVector *>(rightVector)->GetValue(rightPosition);
            auto leftLength = leftValue.length();
            auto rightLength = rightValue.length();
            result = memcmp(leftValue.data(), rightValue.data(), std::min(leftLength, rightLength));
            if (result == 0) {
                if (leftLength > rightLength) {
                    result = COMPARE_STATUS_GREATER_THAN;
                } else if (leftLength < rightLength) {
                    result = COMPARE_STATUS_LESS_THAN;
                }
            }
        } else {
            T left = static_cast<omniruntime::vec::Vector<T> *>(leftVector)->GetValue(leftPosition);
            T right = static_cast<omniruntime::vec::Vector<T> *>(rightVector)->GetValue(rightPosition);
            result = left > right ? COMPARE_STATUS_GREATER_THAN :
                (left < right ? COMPARE_STATUS_LESS_THAN : COMPARE_STATUS_EQUAL);
        }
        if constexpr (IsAscending) {
            return result;
        } else {
            return -result;
        }
    }

    template <typename T>
    static ALWAYS_INLINE int32_t CompareTemplate(vec::BaseVector *leftVector, int32_t leftPosition,
        vec::BaseVector *rightVector, int32_t rightPosition)
    {
        T left;
        T right;
        if (leftVector->GetEncoding() == vec::OMNI_DICTIONARY) {
            left = static_cast<omniruntime::vec::Vector<vec::DictionaryContainer<T>> *>(leftVector)
                       ->GetValue(leftPosition);
        } else {
            left = static_cast<omniruntime::vec::Vector<T> *>(leftVector)->GetValue(leftPosition);
        }
        if (rightVector->GetEncoding() == vec::OMNI_DICTIONARY) {
            right = static_cast<omniruntime::vec::Vector<vec::DictionaryContainer<T>> *>(rightVector)
                        ->GetValue(rightPosition);
        } else {
            right = static_cast<omniruntime::vec::Vector<T> *>(rightVector)->GetValue(rightPosition);
        }
        return left > right ? COMPARE_STATUS_GREATER_THAN :
                              left < right ? COMPARE_STATUS_LESS_THAN : COMPARE_STATUS_EQUAL;
    }

    static ALWAYS_INLINE int32_t CompareDouble(vec::BaseVector *leftColumn, int32_t leftColumnPosition,
        vec::BaseVector *rightColumn, int32_t rightColumnPosition)
    {
        double leftDouble;
        double rightDouble;
        if (leftColumn->GetEncoding() == vec::OMNI_DICTIONARY) {
            leftDouble = static_cast<omniruntime::vec::Vector<vec::DictionaryContainer<double>> *>(leftColumn)
                             ->GetValue(leftColumnPosition);
        } else {
            leftDouble = static_cast<omniruntime::vec::Vector<double> *>(leftColumn)->GetValue(leftColumnPosition);
        };
        if (rightColumn->GetEncoding() == vec::OMNI_DICTIONARY) {
            rightDouble = static_cast<omniruntime::vec::Vector<vec::DictionaryContainer<double>> *>(rightColumn)
                              ->GetValue(rightColumnPosition);
        } else {
            rightDouble = static_cast<omniruntime::vec::Vector<double> *>(rightColumn)->GetValue(rightColumnPosition);
        };

        if (leftDouble > rightDouble) {
            return COMPARE_STATUS_GREATER_THAN;
        } else if (leftDouble < rightDouble) {
            return COMPARE_STATUS_LESS_THAN;
        } else {
            return COMPARE_STATUS_EQUAL;
        }
    }

    static ALWAYS_INLINE int32_t CompareVarchar(vec::BaseVector *leftColumn, int32_t leftColumnPosition,
        vec::BaseVector *rightColumn, int32_t rightColumnPosition)
    {
        std::string_view leftValue;
        if (leftColumn->GetEncoding() == vec::OMNI_DICTIONARY) {
            leftValue =
                reinterpret_cast<vec::Vector<vec::DictionaryContainer<std::string_view, vec::LargeStringContainer>> *>(
                leftColumn)
                    ->GetValue(leftColumnPosition);
        } else {
            leftValue = reinterpret_cast<VarcharVector *>(leftColumn)->GetValue(leftColumnPosition);
        }

        std::string_view rightValue;
        if (rightColumn->GetEncoding() == vec::OMNI_DICTIONARY) {
            rightValue =
                reinterpret_cast<vec::Vector<vec::DictionaryContainer<std::string_view, vec::LargeStringContainer>> *>(
                rightColumn)
                    ->GetValue(rightColumnPosition);
        } else {
            rightValue = reinterpret_cast<VarcharVector *>(rightColumn)->GetValue(rightColumnPosition);
        }

        auto leftLength = leftValue.length();
        auto rightLength = rightValue.length();
        int32_t result = memcmp(leftValue.data(), rightValue.data(), std::min(leftLength, rightLength));
        if (result > 0) {
            return COMPARE_STATUS_GREATER_THAN;
        } else if (result < 0) {
            return COMPARE_STATUS_LESS_THAN;
        } else {
            if (leftLength == rightLength) {
                return COMPARE_STATUS_EQUAL;
            }
            return (leftLength > rightLength) ? COMPARE_STATUS_GREATER_THAN : COMPARE_STATUS_LESS_THAN;
        }
    }

    static ALWAYS_INLINE int32_t CompareDecimal128(vec::BaseVector *leftColumn, int32_t leftColumnPosition,
        vec::BaseVector *rightColumn, int32_t rightColumnPosition)
    {
        Decimal128 leftDecimalValue;
        Decimal128 rightDecimalValue;
        if (leftColumn->GetEncoding() == vec::OMNI_DICTIONARY) {
            leftDecimalValue = static_cast<omniruntime::vec::Vector<vec::DictionaryContainer<Decimal128>> *>(leftColumn)
                                   ->GetValue(leftColumnPosition);
        } else {
            leftDecimalValue =
                static_cast<omniruntime::vec::Vector<Decimal128> *>(leftColumn)->GetValue(leftColumnPosition);
        }

        if (rightColumn->GetEncoding() == vec::OMNI_DICTIONARY) {
            rightDecimalValue =
                static_cast<omniruntime::vec::Vector<vec::DictionaryContainer<Decimal128>> *>(rightColumn)
                    ->GetValue(rightColumnPosition);
        } else {
            rightDecimalValue =
                static_cast<omniruntime::vec::Vector<Decimal128> *>(rightColumn)->GetValue(rightColumnPosition);
        }

        if (leftDecimalValue > rightDecimalValue) {
            return COMPARE_STATUS_GREATER_THAN;
        } else if (leftDecimalValue < rightDecimalValue) {
            return COMPARE_STATUS_LESS_THAN;
        } else {
            return COMPARE_STATUS_EQUAL;
        }
    }

    // add the non-raw-field expression result based on the original input cols
    // if the raw field has Projection object, then the same raw field could not have Projection object
    static int32_t CreateProjections(const DataTypes &inputTypes,
        const std::vector<omniruntime::expressions::Expr *> &projectKeys,
        std::vector<omniruntime::type::DataTypePtr> &newInputTypes,
        std::vector<std::unique_ptr<Projection>> &projections, std::vector<int32_t> &projectCols,
        OverflowConfig *overflowConfig)
    {
        newInputTypes.insert(newInputTypes.end(), inputTypes.Get().begin(), inputTypes.Get().end());
        int32_t inputTypesCount = inputTypes.GetSize();
        int32_t projectOutIdx = 0;
        std::unordered_set<int32_t> colIdSet;
        auto projectKeysCount = projectKeys.size();
        for (size_t i = 0; i < projectKeysCount; i++) {
            auto projectKey = projectKeys[i];
            auto projectRetType = projectKey->GetReturnType();
            if (projectKey->GetType() == ExprType::FIELD_E) {
                auto projectCol = static_cast<const FieldExpr *>(projectKey)->colVal;
                projectCols.emplace_back(projectCol);
                if (colIdSet.find(projectCol) == colIdSet.end()) {
                    colIdSet.emplace(projectCol);
                    auto proj =
                        std::make_unique<Projection>(*projectKey, false, projectRetType, inputTypes, overflowConfig);
                    projections.emplace_back(std::move(proj));
                }
            } else {
                projectCols.emplace_back(inputTypesCount + projectOutIdx);
                newInputTypes.emplace_back(projectRetType);
                projectOutIdx++;
                auto proj =
                    std::make_unique<Projection>(*projectKey, false, projectRetType, inputTypes, overflowConfig);
                projections.emplace_back(std::move(proj));
            }
        }
        return projectOutIdx;
    }

    /* the projectKeys is organized by group by keys + agg input keys
     * this function will change the allCols according to group by cols, agg input cols
     * if the raw field has Projection object, then the same raw field could not have Projection object
     * for example:
     * intTypes char,char,char,int,int,int
     * input 0,1,2,3,4,5
     * group by 4,1,0,3,2,5, min(3),max(3),avg(cast(3 as long)),min(4),max(4),avg(cast(4 as
     * long)),min(5),max(5),avg(cast(5 as long))
     * the allCols could be 0,1,2,3,4,5  +  3,3,6,4,4,7,5,5,8 the
     * newInputTypes could be char,char,char,int,int,int,long,long,long */
    static void CreateRequiredProjections(const DataTypes &inputTypes,
        const std::vector<omniruntime::expressions::Expr *> &projectKeys, std::vector<DataTypePtr> &newInputTypes,
        std::vector<std::unique_ptr<Projection>> &projections, std::vector<int32_t> &allCols,
        OverflowConfig &overflowConfig)
    {
        auto &inputTypeVec = inputTypes.Get();
        int32_t newProjectCol = 0;
        std::unordered_map<int32_t, int32_t> colIdMap;
        auto projectKeysCount = projectKeys.size();
        for (size_t i = 0; i < projectKeysCount; i++) {
            auto projectKey = projectKeys[i];
            auto projectRetType = projectKey->GetReturnType();
            if (projectKey->GetType() == ExprType::FIELD_E) {
                auto projectCol = static_cast<const FieldExpr *>(projectKey)->colVal;
                if (colIdMap.find(projectCol) != colIdMap.end()) {
                    // already exists
                    allCols.push_back(colIdMap[projectCol]);
                } else {
                    allCols.push_back(newProjectCol);
                    colIdMap[projectCol] = newProjectCol++;
                    newInputTypes.push_back(inputTypeVec[projectCol]);
                    auto proj =
                        std::make_unique<Projection>(*projectKey, false, projectRetType, inputTypes, &overflowConfig);
                    projections.push_back(std::move(proj));
                }
            } else {
                // expr col
                allCols.push_back(newProjectCol++);
                newInputTypes.push_back(projectRetType);
                auto proj =
                    std::make_unique<Projection>(*projectKey, false, projectRetType, inputTypes, &overflowConfig);
                projections.push_back(std::move(proj));
            }
        }
    }

    static vec::VectorBatch *ProjectVectors(vec::VectorBatch *inputVecBatch, const DataTypes &inputTypes,
        const std::vector<std::unique_ptr<Projection>> &projections, ExecutionContext *executionContext)
    {
        int32_t vecCount = inputVecBatch->GetVectorCount();
        int32_t rowCount = inputVecBatch->GetRowCount();
        auto newInputVecBatch = std::make_unique<vec::VectorBatch>(rowCount);
        if (rowCount == 0) {
            vec::VectorHelper::AppendVectors(newInputVecBatch.get(), inputTypes, rowCount);
            return newInputVecBatch.release();
        }

        for (int32_t i = 0; i < vecCount; i++) {
            auto inputVector = inputVecBatch->Get(i);
            auto newInputVec = vec::VectorHelper::SliceVector(inputVector, 0, rowCount);
            newInputVecBatch->Append(newInputVec);
        }

        int64_t valueAddrs[vecCount];
        int64_t nullAddrs[vecCount];
        int64_t offsetAddrs[vecCount];
        int64_t dictionaryVectors[vecCount];
        bool hasSetAddr = false;

        auto projectionCount = projections.size();
        for (size_t i = 0; i < projectionCount; i++) {
            if (!projections[i]->IsColumnProjection()) {
                if (!hasSetAddr) {
                    GetAddr(*inputVecBatch, valueAddrs, nullAddrs, offsetAddrs, dictionaryVectors, inputTypes);
                    hasSetAddr = true;
                }
                auto projectVec = projections[i]->Project(inputVecBatch, valueAddrs, nullAddrs, offsetAddrs,
                    executionContext, dictionaryVectors, inputTypes.GetIds());
                if (executionContext->HasError()) {
                    executionContext->GetArena()->Reset();
                    std::string errorMessage = executionContext->GetError();
                    throw OmniException("OPERATOR_RUNTIME_ERROR", errorMessage);
                }
                newInputVecBatch->Append(projectVec);
            }
            // short-circuit logic for column projections
            // no need to go through codegen
        }
        return newInputVecBatch.release();
    }

    template <DataTypeId typeId>
    static int64_t GetDictionaryVectorValuePtrAndLength(vec::BaseVector *vector, int32_t rowIndex, int32_t *length)
    {
        using T = typename NativeType<typeId>::type;
        using DictionaryVarcharVector = vec::Vector<vec::DictionaryContainer<std::string_view, LargeStringContainer>>;
        using DictionaryFlatVector = vec::Vector<vec::DictionaryContainer<T>>;
        if constexpr (std::is_same_v<T, std::string_view>) {
            std::string_view value = reinterpret_cast<DictionaryVarcharVector *>(vector)->GetValue(rowIndex);
            *length = static_cast<int32_t>(value.length());
            return reinterpret_cast<int64_t>(value.data());
        } else {
            T *values =
                vec::unsafe::UnsafeDictionaryVector::GetDictionary(reinterpret_cast<DictionaryFlatVector *>(vector));
            int32_t *originalRowIndex =
                vec::unsafe::UnsafeDictionaryVector::GetIds(reinterpret_cast<DictionaryFlatVector *>(vector)) +
                rowIndex;
            return reinterpret_cast<int64_t>(values + *originalRowIndex);
        }
    }

    template <DataTypeId typeId>
    static int64_t GetFlatVectorValuePtrAndLength(vec::BaseVector *vector, int32_t rowIndex, int32_t *length)
    {
        using T = typename NativeType<typeId>::type;
        using FlatVector = vec::Vector<T>;
        if constexpr (std::is_same_v<T, std::string_view>) {
            std::string_view value = reinterpret_cast<VarcharVector *>(vector)->GetValue(rowIndex);
            *length = static_cast<int32_t>(value.length());
            return reinterpret_cast<int64_t>(value.data());
        } else {
            T *values = vec::unsafe::UnsafeVector::GetRawValues(reinterpret_cast<FlatVector *>(vector)) + rowIndex;
            return reinterpret_cast<int64_t>(values);
        }
    }

    static int64_t GetValuePtrAndLength(vec::BaseVector *vector, int32_t rowIndex, int32_t *length, int32_t typeId)
    {
        if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
            return DYNAMIC_TYPE_DISPATCH(GetFlatVectorValuePtrAndLength, typeId, vector, rowIndex, length);
        } else {
            return DYNAMIC_TYPE_DISPATCH(GetDictionaryVectorValuePtrAndLength, typeId, vector, rowIndex, length);
        }
    }

    static int64_t GetValuePtrAndLengthFromRawVector(vec::BaseVector *vector, int32_t rowIndex, int32_t *length,
        int32_t typeId)
    {
        return DYNAMIC_TYPE_DISPATCH(GetFlatVectorValuePtrAndLength, typeId, vector, rowIndex, length);
    }
};
}
}
#endif // OMNI_RUNTIME_OPERATOR_UTIL_H
