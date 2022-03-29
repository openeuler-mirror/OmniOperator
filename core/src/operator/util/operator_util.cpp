/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

#include "operator_util.h"
#include <memory>
#include "vector/vector_helper.h"

using namespace omniruntime::vec;
using namespace omniruntime::expressions;

namespace omniruntime {
namespace op {
void OperatorUtil::CreateProjectFuncs(const omniruntime::type::DataTypes &inputTypes,
    std::vector<omniruntime::expressions::Expr *> projectKeys, int32_t projectKeysCount,
    std::vector<omniruntime::type::DataType> &newInputTypes,
    std::vector<std::unique_ptr<RowProjection>> &rowProjections, std::vector<int32_t> &projectCols,
    std::vector<omniruntime::op::RowProjFunc> &projectFuncs)
{
    newInputTypes.insert(newInputTypes.end(), inputTypes.Get().begin(), inputTypes.Get().end());
    int32_t inputTypesCount = inputTypes.GetSize();
    for (int32_t i = 0; i < projectKeysCount; i++) {
        auto rowProjection = std::make_unique<RowProjection>(*(projectKeys[i]));
        int32_t projectCol = rowProjection->GetIndexIfColumnProjection();
        if (projectCol != -1) {
            projectCols.push_back(projectCol);
        } else {
            projectCols.push_back(inputTypesCount + projectFuncs.size());
            RowProjFunc func = rowProjection->Create();
            projectFuncs.push_back(func);
            DataType returnType = rowProjection->GetReturnType();
            newInputTypes.push_back(returnType);
        }
        rowProjections.push_back(std::move(rowProjection));
    }
}

void OperatorUtil::CreateRequiredProjectFuncs(const DataTypes &inputTypes,
    omniruntime::expressions::Expr *projectKeys[], int32_t projectKeysCount, std::vector<DataType> &newInputTypes,
    std::vector<std::unique_ptr<RowProjection>> &rowProjections, std::vector<int32_t> &projectCols,
    std::vector<int32_t> &allCols, std::vector<RowProjFunc> &projectFuncs)
{
    std::vector<DataType> inputTypeVec = inputTypes.Get();
    int32_t newProjectCol = 0;
    std::map<int32_t, int32_t> colIdMap;
    for (int32_t i = 0; i < projectKeysCount; i++) {
        auto rowProjection = std::make_unique<RowProjection>(*(projectKeys[i]));
        int32_t projectCol = rowProjection->GetIndexIfColumnProjection();
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
            RowProjFunc func = rowProjection->Create();
            projectFuncs.push_back(func);
            DataType returnType = rowProjection->GetReturnType();
            newInputTypes.push_back(returnType);
        }
        rowProjections.push_back(std::move(rowProjection));
    }
}


template <typename T, typename V>
static T *ProjectVector(RowProjFunc func, int64_t *valuesAddresses, int64_t *valueNulls, int64_t *valueOffsets,
    int64_t *dictVectorAddrs, int32_t rowCount)
{
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator();
    T *result = new T(vecAllocator, rowCount);
    bool isNull = false;
    int32_t length = 0;
    ExecutionContext context;
    for (int32_t i = 0; i < rowCount; i++) {
        isNull = false;
        length = 0;
        void *valuePtr = func(valuesAddresses, valueNulls, valueOffsets, i, &length,
            reinterpret_cast<int64_t>(&context), dictVectorAddrs, &isNull);
        if (!isNull) {
            V value = *(static_cast<V *>(valuePtr));
            result->SetValue(i, value);
        } else {
            result->SetValueNull(i);
        }
    }
    return result;
}

static VarcharVector *ProjectVarcharVector(DataType &type, const RowProjFunc func, int64_t *valuesAddresses,
    int64_t *valueNulls, int64_t *valueOffsets, int64_t *dictVectorAddrs, int32_t rowCount)
{
    VectorAllocator *vectorAllocator = VectorAllocator::GetGlobalAllocator();
    VarcharDataType vecType = static_cast<VarcharDataType &>(type);
    VarcharVector *result = new VarcharVector(vectorAllocator, vecType.GetWidth() * rowCount, rowCount);

    bool isNull = false;
    int32_t length = 0;
    ExecutionContext context;
    for (int32_t i = 0; i < rowCount; i++) {
        isNull = false;
        length = 0;
        void *valuePtr = func(valuesAddresses, valueNulls, valueOffsets, i, &length,
            reinterpret_cast<int64_t>(&context), dictVectorAddrs, &isNull);
        if (!isNull) {
            uint8_t *value = *reinterpret_cast<uint8_t **>(reinterpret_cast<uintptr_t>(valuePtr));
            result->SetValue(i, value, length);
        } else {
            result->SetValueNull(i);
        }
    }
    return result;
}

void OperatorUtil::ProjectVectors(const DataTypes &newInputTypes, const std::vector<RowProjFunc> &projectFuncs,
    const std::vector<int32_t> &projectCols, int64_t *values, int64_t *valueNulls, int64_t *valueOffsets,
    int64_t *dictVectorAddrs, int32_t rowCount, VectorBatch *newVecBatch)
{
    int32_t originalVecCount = newInputTypes.GetSize() - projectFuncs.size();
    int32_t projectColsCount = projectCols.size();
    int32_t projectFuncsIndex = 0;
    const int32_t *typeIds = newInputTypes.GetIds();
    std::vector<DataType> dataTypes = newInputTypes.Get();
    for (int32_t i = 0; i < projectColsCount; i++) {
        int32_t projectCol = projectCols[i];
        // skip the project key which is not expression
        if (projectCol < originalVecCount) {
            continue;
        }

        switch (typeIds[projectCol]) {
            case OMNI_INT:
            case OMNI_DATE32:
                newVecBatch->SetVector(projectCol, ProjectVector<IntVector, int32_t>(projectFuncs[projectFuncsIndex],
                    values, valueNulls, valueOffsets, dictVectorAddrs, rowCount));
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                newVecBatch->SetVector(projectCol, ProjectVector<LongVector, int64_t>(projectFuncs[projectFuncsIndex],
                    values, valueNulls, valueOffsets, dictVectorAddrs, rowCount));
                break;
            case OMNI_DOUBLE:
                newVecBatch->SetVector(projectCol, ProjectVector<DoubleVector, double>(projectFuncs[projectFuncsIndex],
                    values, valueNulls, valueOffsets, dictVectorAddrs, rowCount));
                break;
            case OMNI_BOOLEAN:
                newVecBatch->SetVector(projectCol, ProjectVector<BooleanVector, bool>(projectFuncs[projectFuncsIndex],
                    values, valueNulls, valueOffsets, dictVectorAddrs, rowCount));
                break;
            case OMNI_VARCHAR:
                newVecBatch->SetVector(projectCol, ProjectVarcharVector(dataTypes[projectCol],
                    projectFuncs[projectFuncsIndex], values, valueNulls, valueOffsets, dictVectorAddrs, rowCount));
                break;
            case OMNI_DECIMAL128:
                break;
            default:
                break;
        }
        projectFuncsIndex++;
    }
}

void OperatorUtil::ProjectRequiredVectors(const DataTypes &newInputTypes, const std::vector<RowProjFunc> &projectFuncs,
    const std::vector<int32_t> &projectCols, int64_t *values, int64_t *valueNulls, int64_t *valueOffsets,
    int64_t *dictVectorAddrs, int32_t rowCount, VectorBatch *newVecBatch)
{
    int32_t projectColsCount = projectCols.size();
    int32_t projectFuncsIndex = 0;
    const int32_t *typeIds = newInputTypes.GetIds();
    std::vector<DataType> dataTypes = newInputTypes.Get();
    for (int32_t i = 0; i < projectColsCount; i++) {
        int32_t projectCol = projectCols[i];
        // skip the project key which is not expression
        if (projectCol != -1) {
            continue;
        }

        switch (typeIds[i]) {
            case OMNI_INT:
            case OMNI_DATE32:
                newVecBatch->SetVector(i, ProjectVector<IntVector, int32_t>(projectFuncs[projectFuncsIndex], values,
                    valueNulls, valueOffsets, dictVectorAddrs, rowCount));
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                newVecBatch->SetVector(i, ProjectVector<LongVector, int64_t>(projectFuncs[projectFuncsIndex], values,
                    valueNulls, valueOffsets, dictVectorAddrs, rowCount));
                break;
            case OMNI_DOUBLE:
                newVecBatch->SetVector(i, ProjectVector<DoubleVector, double>(projectFuncs[projectFuncsIndex], values,
                    valueNulls, valueOffsets, dictVectorAddrs, rowCount));
                break;
            case OMNI_BOOLEAN:
                newVecBatch->SetVector(i, ProjectVector<BooleanVector, bool>(projectFuncs[projectFuncsIndex], values,
                    valueNulls, valueOffsets, dictVectorAddrs, rowCount));
                break;
            case OMNI_VARCHAR:
                newVecBatch->SetVector(i, ProjectVarcharVector(dataTypes[i], projectFuncs[projectFuncsIndex], values,
                    valueNulls, valueOffsets, dictVectorAddrs, rowCount));
                break;
            case OMNI_DECIMAL128:
                break;
            default:
                break;
        }
        projectFuncsIndex++;
    }
}

VectorBatch *OperatorUtil::ProjectVectors(VectorBatch *inputVecBatch, const DataTypes &inputTypes,
    const std::vector<RowProjFunc> &projectFuncs, const std::vector<int32_t> &projectCols)
{
    int32_t projectFuncsCount = projectFuncs.size();
    if (projectFuncsCount <= 0) {
        return nullptr;
    }

    int32_t vecCount = inputVecBatch->GetVectorCount();
    int32_t rowCount = inputVecBatch->GetRowCount();
    VectorBatch *newInputVecBatch = new VectorBatch(vecCount + projectFuncsCount, rowCount);
    int64_t valueAddresses[vecCount];
    int64_t valueNulls[vecCount];
    int64_t valueOffsets[vecCount];
    int64_t dictVectorAddrs[vecCount];

    for (int32_t i = 0; i < vecCount; i++) {
        Vector *inputVector = inputVecBatch->GetVector(i);
        Vector *newInputVec = inputVector->Slice(0, rowCount);
        newInputVecBatch->SetVector(i, newInputVec);

        if (newInputVec->GetEncoding() != OMNI_VEC_ENCODING_DICTIONARY) {
            valueAddresses[i] = VectorHelper::GetValuesAddr(newInputVec);
            dictVectorAddrs[i] = 0;
        } else {
            valueAddresses[i] = 0;
            dictVectorAddrs[i] = reinterpret_cast<int64_t>(reinterpret_cast<void *>(newInputVec));
        }
        valueNulls[i] = VectorHelper::GetNullsAddr(newInputVec);
        valueOffsets[i] = VectorHelper::GetOffsetsAddr(newInputVec);
    }

    ProjectVectors(inputTypes, projectFuncs, projectCols, valueAddresses, valueNulls, valueOffsets, dictVectorAddrs,
        rowCount, newInputVecBatch);
    return newInputVecBatch;
}

VectorBatch *OperatorUtil::ProjectRequiredVectors(VectorBatch *inputVecBatch, const DataTypes &inputTypes,
    const std::vector<RowProjFunc> &projectFuncs, const std::vector<int32_t> &projectCols)
{
    int32_t vecCount = projectCols.size();
    int32_t rowCount = inputVecBatch->GetRowCount();
    VectorBatch *newInputVecBatch = new VectorBatch(vecCount, rowCount);

    for (int32_t i = 0; i < vecCount; i++) {
        int32_t sourceColId = projectCols[i];
        if (sourceColId >= 0) {
            // source col
            Vector *inputVector = inputVecBatch->GetVector(sourceColId);
            Vector *newInputVec = inputVector->Slice(0, rowCount);
            newInputVecBatch->SetVector(i, newInputVec);
        }
    }

    int32_t projectFuncsCount = projectFuncs.size();
    if (projectFuncsCount <= 0) {
        return newInputVecBatch;
    }

    int32_t originVecCount = inputVecBatch->GetVectorCount();
    int64_t valueAddresses[vecCount];
    int64_t valueNulls[vecCount];
    int64_t valueOffsets[vecCount];
    int64_t dictVectorAddrs[vecCount];
    for (int32_t i = 0; i < originVecCount; i++) {
        Vector *inputVector = inputVecBatch->GetVector(i);
        if (inputVector->GetEncoding() != OMNI_VEC_ENCODING_DICTIONARY) {
            valueAddresses[i] = VectorHelper::GetValuesAddr(inputVector);
            dictVectorAddrs[i] = 0;
        } else {
            valueAddresses[i] = 0;
            dictVectorAddrs[i] = reinterpret_cast<int64_t>(reinterpret_cast<void *>(inputVector));
        }
        valueNulls[i] = VectorHelper::GetNullsAddr(inputVector);
        valueOffsets[i] = VectorHelper::GetOffsetsAddr(inputVector);
    }

    ProjectRequiredVectors(inputTypes, projectFuncs, projectCols, valueAddresses, valueNulls, valueOffsets,
        dictVectorAddrs, rowCount, newInputVecBatch);
    return newInputVecBatch;
}
}
}
