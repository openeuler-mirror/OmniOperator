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
    vector = GetDictionary(vector, rowIndex);
    auto vecType = vector->GetType();
    if (vector->IsValueNull(rowIndex)) {
        std::cout << "NULL" << "\t";
        return;
    }
    switch (vecType.GetId()) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32: {
            std::cout << std::dec << static_cast<IntVector *>(vector)->GetValue(rowIndex) << "\t";
            break;
        }
        case OMNI_VEC_TYPE_LONG:
        case OMNI_VEC_TYPE_DECIMAL64: {
            std::cout << std::dec << static_cast<LongVector *>(vector)->GetValue(rowIndex) << "\t";
            break;
        }
        case OMNI_VEC_TYPE_DOUBLE: {
            std::cout << static_cast<DoubleVector *>(vector)->GetValue(rowIndex) << "\t";
            break;
        }
        case OMNI_VEC_TYPE_BOOLEAN: {
            std::cout << static_cast<BooleanVector *>(vector)->GetValue(rowIndex) << "\t";
            break;
        }
        case OMNI_VEC_TYPE_VARCHAR: {
            uint8_t *value = nullptr;
            int32_t len = static_cast<VarcharVector *>(vector)->GetValue(rowIndex, &value);
            std::string valueString(value, value + len);
            std::cout << valueString << "\t";
            break;
        }
        case OMNI_VEC_TYPE_DECIMAL128: {
            Decimal128 result = static_cast<Decimal128Vector*>(vector)->GetValue(rowIndex);
            std::cout << result << "\t";
            break;
        }
        case OMNI_VEC_TYPE_CONTAINER: {
            std::cout << "to parse container vec" << "\t";
            break;
        }
        default:
            DebugError("Error vector type %d", vecType.GetId());
    }
}

void VectorHelper::PrintVecBatch(VectorBatch *vecBatch)
{
    int32_t vectorCount = vecBatch->GetVectorCount();
    for (int32_t rowIdx = 0; rowIdx < vecBatch->GetVector(0)->GetSize(); ++rowIdx) {
        for (int32_t colIdx = 0; colIdx < vectorCount; ++colIdx) {
            auto vector = vecBatch->GetVector(colIdx);
            PrintVectorValue(vector, rowIdx);
        }
        std::cout << std::endl;
    }
}
} // namespace vec
} // namespace omniruntime