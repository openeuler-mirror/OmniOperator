//
// Created by root on 6/28/21.
//
#ifndef OMNI_RUNTIME_CONTAINER_VECTOR_H
#define OMNI_RUNTIME_CONTAINER_VECTOR_H

#include <cstring>
#include <vector>

#include "fixed_width_vector.h"

/**
 * ContainerVector is for combining more than one vectors to one vector. Present as a vector
 * to the place where it is used. For instance, in two-stage aggregations, calculating average
 * in partial stage will produce two vectors. One is intermediate average value, the other is
 * intermediate count value.
 */
namespace omniruntime {
namespace vec {
class ContainerVector : public Vector {
    using T = typename NativeType<OMNI_VEC_TYPE_CONTAINER>::type;

public:
    ContainerVector(VectorAllocator *allocator, int32_t positionCount, Vector **fieldVectors, int32_t vectorCount,
        VecType types[]);

    ContainerVector(VectorAllocator *allocator, int32_t capacityInBytes, int32_t positionCount);

    // inline for high performance.
    int64_t ALWAYS_INLINE GetValue(int32_t index)
    {
        return reinterpret_cast<uintptr_t *>(valuesAddress)[index];
    };

    // inline for high performance.
    void ALWAYS_INLINE SetValue(int32_t index, int64_t value)
    {
        reinterpret_cast<int64_t *>(valuesAddress)[index] = value;
    }

    int32_t ALWAYS_INLINE GetPositionCount()
    {
        return positionCount;
    }

    int32_t ALWAYS_INLINE GetVectorCount()
    {
        return vectorCount;
    }

    std::vector<VecType> ALWAYS_INLINE &GetVecTypes()
    {
        return vecTypes;
    }

    ContainerVector *Slice(int positionOffset, int length) override;

    ContainerVector *CopyPositions(const int *positions, int offset, int length) override;

    ContainerVector *CopyRegion(int positionOffset, int length) override;

    void Append(Vector *other, int positionOffset, int length) override;

    ~ContainerVector() override {};

private:
    static const int BYTES = sizeof(T);
    std::vector<VecType> vecTypes;
    int32_t vectorCount;
    int32_t positionCount;

    ContainerVector(ContainerVector *vector, int32_t vectorCount, int32_t positionOffset, VecType types[])
        : Vector(vector, vectorCount, positionOffset)
    {
        for (int32_t i = 0; i < vectorCount; ++i) {
            this->vecTypes.push_back(types[i]);
        }
    }
};
} // namespace vec
} // namepsace omniruntime
#endif // OMNI_RUNTIME_CONTAINER_VECTOR_H
