/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_HELPER_H
#define OMNI_RUNTIME_VECTOR_HELPER_H

#include <vector>
#include "vector.h"
#include "vector_batch.h"
#include "int_vector.h"
#include "long_vector.h"
#include "double_vector.h"
#include "varchar_vector.h"

namespace omniruntime {
namespace vec {
#define STATIC_CAST_VEC_GET(CLASS, VEC, INDEX) static_cast<CLASS *>(VEC)->GetValue(INDEX)
#define STATIC_CAST_VEC_SET(CLASS, VEC, INDEX, VALUE) static_cast<CLASS *>(VEC)->SetValue(INDEX, VALUE)

#define INT32_VEC_GET(VEC, INDEX) STATIC_CAST_VEC_GET(IntVector, VEC, INDEX)
#define INT32_SET_SET(VEC, INDEX, VALUE) STATIC_CAST_VEC_SET(IntVector, VEC, INDEX, VALUE)

#define LONG_VEC_GET(VEC, INDEX) STATIC_CAST_VEC_GET(IntVector, VEC, INDEX)
#define LONG_SET_SET(VEC, INDEX, VALUE) STATIC_CAST_VEC_SET(IntVector, VEC, INDEX, VALUE)

#define DOUBLE_VEC_GET(VEC, INDEX) STATIC_CAST_VEC_GET(IntVector, VEC, INDEX)
#define DOUBLE_SET_SET(VEC, INDEX, VALUE) STATIC_CAST_VEC_SET(IntVector, VEC, INDEX, VALUE)

class VectorHelper {
public:
    static void SetValue(Vector *vector, int32_t index, void *value)
    {
        switch (vector->GetType().GetId()) {
            case OMNI_VEC_TYPE_INT:
                static_cast<IntVector *>(vector)->SetValue(index, *static_cast<int32_t *>(value));
                break;
            case OMNI_VEC_TYPE_LONG:
                static_cast<LongVector *>(vector)->SetValue(index, *static_cast<int64_t *>(value));
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                static_cast<DoubleVector *>(vector)->SetValue(index, *static_cast<double *>(value));
                break;
            default:
                break;
        }
    }

    static void SetValue(Vector *vector, int32_t index, void *value, int32_t length)
    {
        switch (vector->GetType().GetId()) {
            case OMNI_VEC_TYPE_VARCHAR:
                static_cast<VarcharVector *>(vector)->SetValue(index, static_cast<uint8_t *>(value), length);
                break;
            default:
                break;
        }
    }

    static int32_t GetValue(Vector *vector, int32_t index, void *value)
    {
        int32_t length = 0;
        switch (vector->GetType().GetId()) {
            case OMNI_VEC_TYPE_INT:
                *static_cast<int32_t *>(value) = static_cast<IntVector *>(vector)->GetValue(index);
                break;
            case OMNI_VEC_TYPE_LONG:
                *static_cast<int64_t *>(value) = static_cast<LongVector *>(vector)->GetValue(index);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                *static_cast<double *>(value) = static_cast<DoubleVector *>(vector)->GetValue(index);
                break;
            case OMNI_VEC_TYPE_VARCHAR:
                length = static_cast<VarcharVector *>(vector)->GetValue(index, static_cast<uint8_t **>(value));
                break;
            default:
                break;
        }
        return length;
    }

    static void FreeVecBatch(VectorBatch *vecBatch);

    static void FreeVecBatches(VectorBatch **vecBatches, int32_t vecBatchCount);

    static void FreeVecBatches(std::vector<VectorBatch *> &vecBatches);

    static void PrintVecBatch(VectorBatch *vecBatch);

private:
    static void PrintVectorValue(Vector *vector, int32_t rowIndex);
};
} // namespace vec
} // namespace omniruntime
#endif // OMNI_RUNTIME_VECTOR_HELPER_H
