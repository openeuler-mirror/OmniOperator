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
class ContainerVector : public FixedWidthVector<int64_t> {
public:
    ContainerVector(VectorAllocator* allocator, int32_t positionCount, Vector** fieldVectors, int32_t vectorCount, VecType types[]) :
    FixedWidthVector(allocator, vectorCount * BYTES, vectorCount, OMNI_VEC_TYPE_CONTAINER), vectorCount(vectorCount), positionCount(positionCount)
    {
        // ????? use setValues
        for(int32_t i = 0; i < vectorCount; ++i) {
            setValue(i, reinterpret_cast<int64_t>(fieldVectors[i]));
            this->vecTypes.push_back(types[i]);
        }
    }
    ContainerVector *Slice(int32_t positionOffset, int32_t length) override;
    ContainerVector *CopyPositions(const int32_t *positions, int32_t offset, int32_t length) override;
    ContainerVector *CopyRegion(int32_t positionOffset, int32_t length) override;
    void SetValues(int32_t startIndex, const int64_t *values, int32_t length) override;
    void Append(Vector *other, int positionOffset, int length) override;

    // inline for high performance.
    int64_t getValue(int32_t index) {
        return reinterpret_cast<uintptr_t*>(valuesAddress)[index];
    };

    // inline for high performance.
    void setValue(int32_t index, int64_t value) {
        reinterpret_cast<int64_t*>(valuesAddress)[index] = value;
    }

    int32_t getPositionCount()
    {
        return positionCount;
    }

    std::vector<VecType>& getVecTypes()
    {
        return vecTypes;
    }

    ~ContainerVector() {}

private:
    static const int32_t BYTES = sizeof(int64_t);
    std::vector<VecType> vecTypes;
    int32_t vectorCount;
    int32_t positionCount;
    ContainerVector(ContainerVector *vector, int32_t vectorCount, int32_t positionOffset, VecType types[]) :
    FixedWidthVector(vector, vectorCount, positionOffset) {
        for(int32_t i = 0; i < vectorCount; ++i) {
            this->vecTypes.push_back(types[i]);
        }
    }
};

#endif //OMNI_RUNTIME_CONTAINER_VECTOR_H
