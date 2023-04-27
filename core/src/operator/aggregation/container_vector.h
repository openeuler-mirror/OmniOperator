/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */
#ifndef OMNI_RUNTIME_CONTAINER_VECTOR_H
#define OMNI_RUNTIME_CONTAINER_VECTOR_H

#include <vector>
#include "vector/vector.h"
#include "util/error_code.h"
#include "util/omni_exception.h"
#include "type/data_type.h"

/**
 * ContainerVector is for combining more than one vectors to one vector. Present as a vector
 * to the place where it is used. For instance, in two-stage aggregations, calculating average
 * in partial stage will produce two vectors. One is intermediate average value, the other is
 * intermediate count value.
 */
namespace omniruntime {
namespace vec {
class ContainerVector : public Vector<int64_t> {
public:
    ContainerVector(int32_t positionCount, std::vector<int64_t> &fieldVectors,
        std::vector<omniruntime::type::DataTypePtr> &dataTypes)
        : Vector<int64_t>(positionCount, OMNI_ENCODING_CONTAINER), dataTypes(dataTypes)
    {
        values = std::shared_ptr<int64_t[]>(new int64_t[dataTypes.size()]);
        // init nulls
        for (int32_t rowIndex = 0; rowIndex < positionCount; rowIndex++) {
            bool isNull = true;
            for (int32_t j = 0; j < fieldVectors.size(); j++) {
                BaseVector *col = reinterpret_cast<BaseVector *>(fieldVectors[j]);
                isNull = isNull && col->IsNull(rowIndex); // each value is null in all column
            }
            this->nulls[rowIndex] = isNull;
        }

        // init value
        for (int32_t i = 0; i < this->dataTypes.size(); ++i) {
            SetValue(i, fieldVectors[i]);
        }

        // report memory usage
        int64_t vectorCapacity = sizeof(ContainerVector) + BaseVector::GetNullsCapacity(nulls.get()) +
            Vector<int64_t>::GetValuesCapacity(values.get());
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::AddVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
    }

    ContainerVector(int32_t vectorCount) : Vector<int64_t>(vectorCount, OMNI_ENCODING_CONTAINER)
    {
        values = std::shared_ptr<int64_t[]>(new int64_t[vectorCount]);

        // init vec is null in container
        for (int i = 0; i < vectorCount; i++) {
            SetValue(i, 0);
        }

        // report memory usage
        int64_t vectorCapacity = sizeof(ContainerVector) + BaseVector::GetNullsCapacity(nulls.get()) +
            Vector<int64_t>::GetValuesCapacity(values.get());
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::AddVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
    }

    // inline for high performance.
    int64_t ALWAYS_INLINE GetValue(int32_t index)
    {
        return values[index];
    }

    /* *
     * Set the value at the indicated index
     * @param index
     * @param value
     */
    void ALWAYS_INLINE SetValue(int index, int64_t value)
    {
        values[index] = value;
    }

    int32_t ALWAYS_INLINE GetVectorCount()
    {
        return dataTypes.size();
    }

    std::vector<type::DataTypePtr> ALWAYS_INLINE &GetDataTypes()
    {
        return dataTypes;
    }

    /* *
     * not support
     *
     * @param other the dst data from
     * @param positionOffset element position
     * @param length number of elements
     */
    void Append(BaseVector *other, int positionOffset, int length)
    {
        throw exception::OmniException(omniruntime::op::GetErrorCode(omniruntime::op::ErrorCode::UNSUPPORTED),
            "container vector not support append");
    }

    /* *
     * not support
     * @param positions
     * @param offset
     * @param length
     */
    std::unique_ptr<Vector<int64_t>> CopyPositions(const int *positions, int positionOffset, int length)
    {
        throw exception::OmniException(omniruntime::op::GetErrorCode(omniruntime::op::ErrorCode::UNSUPPORTED),
            "container vector not support CopyPositions");
    }

    /* *
     * not support
     * @param positionOffset
     * @param length
     * @param isCopy reserved parameters
     */
    std::unique_ptr<ContainerVector> Slice(int positionOffset, int length, bool isCopy = false)
    {
        throw exception::OmniException(omniruntime::op::GetErrorCode(omniruntime::op::ErrorCode::UNSUPPORTED),
            "container vector not support CopyPositions");
    }

    ~ContainerVector()
    {
        // release the vector in container vector
        for (int32_t i = 0; i < dataTypes.size(); i++) {
            auto *field = reinterpret_cast<BaseVector *>(GetValue(i));
            if (field != nullptr) {
                delete field;
                // set null vec
                SetValue(i, 0);
            }
        }

        int64_t vectorCapacity = sizeof(ContainerVector) + BaseVector::GetNullsCapacity(nulls.get()) +
            Vector<int64_t>::GetValuesCapacity(values.get());
        omniruntime::mem::ThreadMemoryManager::ReclaimMemory(vectorCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::SubVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
    }

    void SetDataTypes(const std::vector<type::DataTypePtr> &dataTypes)
    {
        this->dataTypes = dataTypes;
    }

private:
    std::vector<type::DataTypePtr> dataTypes;
};
} // namespace vector2
} // namepsace omniruntime
#endif // OMNI_RUNTIME_CONTAINER_VECTOR2_H
