/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_HELPER_H
#define OMNI_RUNTIME_VECTOR_HELPER_H

#include "vector.h"
#include "vector_batch.h"
#include "int_vector.h"
#include "long_vector.h"
#include "double_vector.h"
#include <vector>

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
                ((IntVector *)vector)->SetValue(index, *(int32_t *)value);
                break;
            case OMNI_VEC_TYPE_LONG:
                ((LongVector *)vector)->SetValue(index, *(int64_t *)value);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                ((DoubleVector *)vector)->SetValue(index, *(double *)value);
                break;
            default:
                break;
        }
    }

    static void GetValue(Vector *vector, int32_t index, void *value)
    {
        switch (vector->GetType().GetId()) {
            case OMNI_VEC_TYPE_INT:
                *(int32_t *)value = ((IntVector *)vector)->GetValue(index);
                break;
            case OMNI_VEC_TYPE_LONG:
                *(int64_t *)value = ((LongVector *)vector)->GetValue(index);
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                *(double *)value = ((DoubleVector *)vector)->GetValue(index);
                break;
            default:
                break;
        }
    }

    static void FreeVecBatch(VectorBatch *vecBatch);

    static void FreeVecBatches(VectorBatch **vecBatches, int32_t vecBatchCount);

    static void FreeVecBatches(std::vector<VectorBatch *> &vecBatches);
};
} // namespace vec
} // namespace omniruntime
#endif // OMNI_RUNTIME_VECTOR_HELPER_H
