/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include "vector_helper.h"

#include <iomanip>

#include "dictionary_vector.h"
#include "varchar_vector.h"
#include "boolean_vector.h"
#include "decimal128_vector.h"

namespace omniruntime {
namespace vec {
void VectorHelper::FreeVecBatch(VectorBatch *vecBatch)
{
    vecBatch->ReleaseAllVectors();
    delete vecBatch;
}

void VectorHelper::FreeVecBatches(VectorBatch **vecBatches, int32_t vecBatchCount)
{
    for (int i = 0; i < vecBatchCount; ++i) {
        vecBatches[i]->ReleaseAllVectors();
        delete vecBatches[i];
    }
    delete[] vecBatches;
}

void VectorHelper::FreeVecBatches(std::vector<VectorBatch *> &vecBatches)
{
    for (int i = 0; i < vecBatches.size(); ++i) {
        vecBatches[i]->ReleaseAllVectors();
        delete vecBatches[i];
    }
}

void VectorHelper::PrintVectorValue(Vector *vector, int32_t rowIndex)
{
    int32_t originalRowIndex;
    vector = ExpandVectorAndIndex(vector, rowIndex, originalRowIndex);
    auto vecTypeId = vector->GetTypeId();
    if (vector->IsValueNull(originalRowIndex)) {
        std::cout << "NULL"
                  << "\t";
        return;
    }
    switch (vecTypeId) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            std::cout << std::dec << static_cast<IntVector *>(vector)->GetValue(originalRowIndex) << "\t";
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            std::cout << std::dec << static_cast<LongVector *>(vector)->GetValue(originalRowIndex) << "\t";
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            std::cout << static_cast<DoubleVector *>(vector)->GetValue(originalRowIndex) << "\t";
            break;
        }
        case OMNI_VEC_TYPE_BOOLEAN: {
            std::cout << static_cast<BooleanVector *>(vector)->GetValue(originalRowIndex) << "\t";
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *value = nullptr;
            int32_t len = static_cast<VarcharVector *>(vector)->GetValue(originalRowIndex, &value);
            std::string valueString(value, value + len);
            std::cout << valueString << "\t";
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 result = static_cast<Decimal128Vector *>(vector)->GetValue(originalRowIndex);
            std::cout << result << "\t";
            break;
        }
        case OMNI_VEC_TYPE_CONTAINER: {
            std::cout << "to parse container vec"
                      << "\t";
            break;
        }
        default:
            LogError("Error vector type %d", vecTypeId);
    }
}

void VectorHelper::PrintVecBatch(VectorBatch *vecBatch)
{
    int32_t vectorCount = vecBatch->GetVectorCount();
    int32_t rowCount = vecBatch->GetRowCount();
    for (int32_t rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
        for (int32_t colIdx = 0; colIdx < vectorCount; ++colIdx) {
            auto vector = vecBatch->GetVector(colIdx);
            PrintVectorValue(vector, rowIdx);
        }
        std::cout << std::endl;
    }
}
} // namespace vec
} // namespace omniruntime