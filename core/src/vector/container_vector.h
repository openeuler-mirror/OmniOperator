//
// Created by root on 6/28/21.
//
#ifndef OMNI_RUNTIME_CONTAINER_VECTOR_H
#define OMNI_RUNTIME_CONTAINER_VECTOR_H

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
using DataType = type::DataType;
class ContainerVector : public Vector {
    using T = typename type::NativeType<type::OMNI_CONTAINER>::type;

public:
    ContainerVector(VectorAllocator *allocator, int32_t positionCount, std::vector<uintptr_t> &fieldVectors,
        int32_t vectorCount, std::vector<DataTypeRawPtr> &dataTypes);

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

    std::vector<DataTypeRawPtr> ALWAYS_INLINE &GetDataTypes()
    {
        return dataTypes;
    }

    ContainerVector *Slice(int positionOffset, int length) override;

    ContainerVector *CopyPositions(const int *positions, int offset, int length) override;

    ContainerVector *CopyRegion(int positionOffset, int length) override;

    void Append(Vector *other, int positionOffset, int length) override;

    ~ContainerVector() override;

    VectorEncoding GetEncoding() override
    {
        return OMNI_VEC_ENCODING_CONTAINER;
    }

    void SetDataTypes(const std::vector<DataTypeRawPtr> &dataTypes)
    {
        this->dataTypes = dataTypes;
    }

private:
    static const int BYTES = sizeof(T);
    std::vector<DataTypeRawPtr> dataTypes;
    int32_t vectorCount;
    int32_t positionCount;

    std::vector<int32_t> GetFieldVecOffsets()
    {
        std::vector<int32_t> fieldVecOffsets(positionCount + 1);
        for (int position = 0; position < positionCount; position++) {
            fieldVecOffsets[position + 1] = fieldVecOffsets[position] + (IsValueNull(position) ? 0 : 1);
        }
        return fieldVecOffsets;
    }
};
} // namespace vec
} // namepsace omniruntime
#endif // OMNI_RUNTIME_CONTAINER_VECTOR_H
