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

    using CompareFunc = int32_t (*)(vec::BaseVector *leftVector, int32_t leftPosition, vec::BaseVector *rightVector,
        int32_t rightPosition);

    static int32_t GetTypeSize(const DataTypePtr dataTypePtr)
    {
        switch (dataTypePtr->GetId()) {
            case OMNI_INT:
                return OperatorUtil::SIZE_OF_INT;
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
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                return static_cast<VarcharDataType &>(*dataTypePtr).GetWidth();
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

    static int32_t GetVecBatchCount(int32_t positionCount, int32_t maxRowCount)
    {
        ASSERT(maxRowCount != 0);
        return ((positionCount + maxRowCount - 1) / maxRowCount);
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

        int32_t result = memcmp(leftValue.data(), rightValue.data(), std::min(leftValue.length(), rightValue.length()));
        if (result != 0) {
            return (result > 0) ? COMPARE_STATUS_GREATER_THAN : COMPARE_STATUS_LESS_THAN;
        } else if (leftValue.length() == rightValue.length()) {
            return COMPARE_STATUS_EQUAL;
        } else {
            return (leftValue.length() > rightValue.length()) ? COMPARE_STATUS_GREATER_THAN : COMPARE_STATUS_LESS_THAN;
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

    static void CreateProjectFuncs(const DataTypes &inputTypes,
        std::vector<omniruntime::expressions::Expr *> projectKeys, int32_t projectKeysCount,
        std::vector<omniruntime::type::DataTypePtr> &newInputTypes,
        std::vector<std::unique_ptr<Projection>> &projections, std::vector<int32_t> &projectCols,
        std::vector<ProjFunc> &projectFuncs, OverflowConfig *overflowConfig)
    {
        newInputTypes.insert(newInputTypes.end(), inputTypes.Get().begin(), inputTypes.Get().end());
        int32_t inputTypesCount = inputTypes.GetSize();
        for (int32_t i = 0; i < projectKeysCount; i++) {
            std::unique_ptr<Projection> proj;
            if (projectKeys[i] && projectKeys[i]->GetType() == ExprType::FIELD_E) {
                projectCols.push_back(static_cast<const FieldExpr *>(projectKeys[i])->colVal);
            } else {
                proj = std::make_unique<Projection>(*(projectKeys[i]), false, projectKeys[i]->GetReturnType(),
                    inputTypes, overflowConfig);
                projectCols.emplace_back(inputTypesCount + projectFuncs.size());
                ProjFunc func = proj->GetProjector();
                projectFuncs.emplace_back(func);
                DataTypePtr returnType = projectKeys[i]->GetReturnType();
                newInputTypes.emplace_back(returnType);
            }
            projections.emplace_back(std::move(proj));
        }
    }

    static void CreateRequiredProjectFuncs(const DataTypes &inputTypes, omniruntime::expressions::Expr *projectKeys[],
        int32_t projectKeysCount, std::vector<DataTypePtr> &newInputTypes,
        std::vector<std::unique_ptr<Projection>> &projections, std::vector<int32_t> &projectCols,
        std::vector<int32_t> &allCols, std::vector<ProjFunc> &projectFuncs, OverflowConfig &overflowConfig)
    {
        auto &inputTypeVec = inputTypes.Get();
        int32_t newProjectCol = 0;
        std::map<int32_t, int32_t> colIdMap;
        for (int32_t i = 0; i < projectKeysCount; i++) {
            std::unique_ptr<Projection> proj;
            int32_t projectCol;
            if (projectKeys[i] && projectKeys[i]->GetType() == ExprType::FIELD_E) {
                projectCol = static_cast<const FieldExpr *>(projectKeys[i])->colVal;
            } else {
                projectCol = -1;
            }

            if (projectCol != -1) {
                if (colIdMap.find(projectCol) != colIdMap.end()) {
                    // already exists
                    allCols.push_back(colIdMap[projectCol]);
                } else {
                    projectCols.push_back(projectCol);
                    allCols.push_back(newProjectCol);
                    colIdMap[projectCol] = newProjectCol++;
                    newInputTypes.push_back(inputTypeVec[projectCol]);
                }
            } else {
                // expr col
                projectCols.push_back(projectCol);
                allCols.push_back(newProjectCol++);
                proj = std::make_unique<Projection>(*(projectKeys[i]), false, projectKeys[i]->GetReturnType(),
                    inputTypes, const_cast<OverflowConfig *>(&overflowConfig));
                ProjFunc func = proj->GetProjector();
                projectFuncs.push_back(func);
                DataTypePtr returnType = projectKeys[i]->GetReturnType();
                newInputTypes.push_back(returnType);
            }
            projections.push_back(std::move(proj));
        }
    }

    template <DataTypeId typeId> static int64_t GetRawAddr(BaseVector *vector)
    {
        using T = typename NativeType<typeId>::type;
        if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
            return reinterpret_cast<int64_t>(unsafe::UnsafeStringVector::GetValues(
                reinterpret_cast<vec::Vector<LargeStringContainer<std::string_view>> *>(vector)));
        } else {
            return reinterpret_cast<int64_t>(
                unsafe::UnsafeVector::GetRawValues(reinterpret_cast<vec::Vector<T> *>(vector)));
        }
    }

    template <DataTypeId typeId>
    static std::unique_ptr<BaseVector> ProjectVector(ProjFunc func, int64_t *valuesAddresses, int64_t *valueNulls,
        int64_t *valueOffsets, int64_t *dictVectorAddrs, int32_t rowCount)
    {
        using Type = typename omniruntime::op::NativeAndVectorType<typeId>::type;
        using Vector = typename omniruntime::op::NativeAndVectorType<typeId>::vector;

        auto result = std::make_unique<Vector>(rowCount);
        ExecutionContext context;
        int64_t outData = reinterpret_cast<int64_t>(vec::VectorHelper::UnsafeGetValues(result.get(), typeId));
        bool *outNull = unsafe::UnsafeBaseVector::GetNulls(result.get());
        int32_t *outOffset = reinterpret_cast<int32_t *>(vec::VectorHelper::UnsafeGetOffsetsAddr(result.get(), typeId));

        if constexpr (std::is_same_v<Type, std::string_view>) {
            func(valuesAddresses, rowCount, reinterpret_cast<int64_t>(result.get()), nullptr, rowCount, valueNulls,
                valueOffsets, outNull, outOffset, reinterpret_cast<int64_t>(&context), dictVectorAddrs);
        } else {
            func(valuesAddresses, rowCount, outData, nullptr, rowCount, valueNulls, valueOffsets, outNull, outOffset,
                reinterpret_cast<int64_t>(&context), dictVectorAddrs);
        }

        return std::move(result);
    }

    static void ProjectVectors(const DataTypes &newInputTypes, const std::vector<ProjFunc> &projectFuncs,
        const std::vector<int32_t> &projectCols, int64_t *values, int64_t *valueNulls, int64_t *valueOffsets,
        int64_t *dictVectorAddrs, int32_t rowCount, vec::VectorBatch *newVecBatch)
    {
        int32_t projectColsCount = projectCols.size();
        int32_t projectFuncsIndex = 0;
        for (int32_t i = 0; i < projectColsCount; i++) {
            int32_t projectCol = projectCols[i];
            // skip the project key which is not expression
            if (projectCol < newInputTypes.GetSize() - projectFuncs.size()) {
                continue;
            }
            newVecBatch->Append(DYNAMIC_TYPE_DISPATCH(ProjectVector, newInputTypes.GetType(projectCol)->GetId(),
                projectFuncs[projectFuncsIndex++], values, valueNulls, valueOffsets, dictVectorAddrs, rowCount)
                                    .release());
        }
    }

    static vec::VectorBatch *ProjectVectors(vec::VectorBatch *inputVecBatch, const DataTypes &inputTypes,
        const std::vector<ProjFunc> &projectFuncs, const std::vector<int32_t> &projectCols)
    {
        int32_t projectFuncsCount = projectFuncs.size();
        if (projectFuncsCount <= 0) {
            return nullptr;
        }

        int32_t vecCount = inputVecBatch->GetVectorCount();
        int32_t rowCount = inputVecBatch->GetRowCount();
        auto newInputVecBatch = new vec::VectorBatch(rowCount);
        // short-circuit logic for column projections
        // no need to go through codegen
        if (rowCount == 0) {
            vec::VectorHelper::AppendVectors(newInputVecBatch, inputTypes, rowCount);
            return newInputVecBatch;
        }

        int64_t valueAddresses[vecCount];
        int64_t valueNulls[vecCount];
        int64_t valueOffsets[vecCount];
        int64_t dictVectorAddrs[vecCount];

        for (int32_t i = 0; i < vecCount; i++) {
            auto inputVector = inputVecBatch->Get(i);
            std::unique_ptr<BaseVector> newInputVec =
                vec::VectorHelper::SliceVector(inputVector, inputTypes.GetIds()[i], 0, rowCount);

            if (newInputVec->GetEncoding() != vec::OMNI_DICTIONARY) {
                valueAddresses[i] = reinterpret_cast<int64_t>(
                    vec::VectorHelper::UnsafeGetValues(newInputVec.get(), inputTypes.GetIds()[i]));
                dictVectorAddrs[i] = 0;
            } else {
                valueAddresses[i] = 0;
                dictVectorAddrs[i] = reinterpret_cast<int64_t>(reinterpret_cast<void *>(newInputVec.get()));
            }
            valueNulls[i] = reinterpret_cast<int64_t>(unsafe::UnsafeBaseVector::GetNulls(newInputVec.get()));
            valueOffsets[i] = reinterpret_cast<int64_t>(
                vec::VectorHelper::UnsafeGetOffsetsAddr(newInputVec.get(), inputTypes.GetIds()[i]));
            newInputVecBatch->Append(newInputVec.release());
        }

        ProjectVectors(inputTypes, projectFuncs, projectCols, valueAddresses, valueNulls, valueOffsets, dictVectorAddrs,
            rowCount, newInputVecBatch);
        return newInputVecBatch;
    }

    template <DataTypeId typeId>
    static int64_t GetDictionaryVectorValuePtrAndLength(vec::BaseVector *vector, int32_t rowIndex, int32_t *length)
    {
        using T = typename NativeType<typeId>::type;
        using DictionaryVarcharVector = vec::Vector<vec::DictionaryContainer<std::string_view, LargeStringContainer>>;
        using DictionaryFlatVector = vec::Vector<vec::DictionaryContainer<T>>;
        if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
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
        if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
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
};
}
}
#endif // OMNI_RUNTIME_OPERATOR_UTIL_H
