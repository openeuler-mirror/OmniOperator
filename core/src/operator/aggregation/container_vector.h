/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
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
        dataTypeId = OMNI_CONTAINER;
        valuesBuffer = std::make_shared<AlignedBuffer<int64_t>>(dataTypes.size());
        values = valuesBuffer->GetBuffer();
        // init nulls
        for (int32_t rowIndex = 0; rowIndex < positionCount; rowIndex++) {
            bool isNull = true;
            for (int32_t j = 0; j < fieldVectors.size(); j++) {
                BaseVector *col = reinterpret_cast<BaseVector *>(fieldVectors[j]);
                isNull = isNull && col->IsNull(rowIndex); // each value is null in all column
            }
            this->nullsBuffer->SetNull(rowIndex, isNull);
        }

        // init value
        for (int32_t i = 0; i < this->dataTypes.size(); ++i) {
            SetValue(i, fieldVectors[i]);
        }

        // report memory usage
        int64_t vectorCapacity = sizeof(ContainerVector) + sizeof(NullsBuffer) + sizeof(AlignedBuffer<uint8_t>)
            + sizeof(AlignedBuffer<int64_t>);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
        omniruntime::mem::MemoryTrace::AddVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
    }

    ContainerVector(int32_t capacityInBytes, int32_t positionCount)
        : Vector<int64_t>(positionCount, OMNI_ENCODING_CONTAINER)
    {
        dataTypeId = OMNI_CONTAINER;
        using T = typename type::NativeType<type::OMNI_CONTAINER>::type;
        int32_t vectorCount = capacityInBytes / sizeof(T);
        valuesBuffer = std::make_shared<AlignedBuffer<int64_t>>(vectorCount);
        values = valuesBuffer->GetBuffer();
        // init vec is null in container
        for (int i = 0; i < vectorCount; i++) {
            SetValue(i, 0);
        }

        // report memory usage
        int64_t vectorCapacity = sizeof(ContainerVector) + sizeof(NullsBuffer) + sizeof(AlignedBuffer<uint8_t>)
            + sizeof(AlignedBuffer<int64_t>);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
        omniruntime::mem::MemoryTrace::AddVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
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

    static void AppendToVector(BaseVector *destVector, int32_t offset, BaseVector *srcVector, int32_t length,
        int32_t dataTypeId)
    {
        switch (dataTypeId) {
            case type::OMNI_LONG: {
                static_cast<Vector<int64_t> *>(destVector)->Append(srcVector, offset, length);
                break;
            }
            case type::OMNI_DOUBLE: {
                static_cast<Vector<double> *>(destVector)->Append(srcVector, offset, length);
                break;
            }
            default: {
                std::string omniExceptionInfo =
                    "In function AppendToVector, no such data type " + std::to_string(dataTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
            }
        }
    }

    /* *
     *
     * @param other the dst data from
     * @param positionOffset element position
     * @param length number of elements
     */
    void Append(BaseVector *other, int positionOffset, int length)
    {
        auto *otherContainer = static_cast<ContainerVector *>(other);
        if (otherContainer->GetVectorCount() != this->GetVectorCount()) {
            LogError("this vec count %d is not equal other vec count %d, container vec append failed.",
                this->GetVectorCount(), otherContainer->GetVectorCount());
            throw omniruntime::exception::OmniException("CONTAINER_VEC", "container vector append count error");
        }
        if (other->GetEncoding() != OMNI_ENCODING_CONTAINER) {
            LogError("this vec type %d is not equal other type %d, container vec append failed.",
                OMNI_ENCODING_CONTAINER, other->GetEncoding());
            throw omniruntime::exception::OmniException("CONTAINER_VEC", "container vector append encoding error");
        }

        for (int32_t i = 0; i < GetVectorCount(); i++) {
            auto *thisVector = reinterpret_cast<BaseVector *>(GetValue(i));
            auto *otherVector = reinterpret_cast<BaseVector *>(otherContainer->GetValue(i));
            AppendToVector(thisVector, positionOffset, otherVector, length, dataTypes[i]->GetId());
        }
        // set nulls
        SetNulls(positionOffset, otherContainer->nullsBuffer.get(), length);
    }

    /* *
     * not support
     * @param positions
     * @param offset
     * @param length
     */
    Vector<int64_t> *CopyPositions(const int *positions, int positionOffset, int length)
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
    ContainerVector *Slice(int positionOffset, int length, bool isCopy = false)
    {
        throw exception::OmniException(omniruntime::op::GetErrorCode(omniruntime::op::ErrorCode::UNSUPPORTED),
            "container vector not support Slice");
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

        int64_t vectorCapacity = sizeof(ContainerVector) + sizeof(NullsBuffer) + sizeof(AlignedBuffer<uint8_t>)
            + sizeof(AlignedBuffer<int64_t>);
        omniruntime::mem::ThreadMemoryManager::ReclaimMemory(vectorCapacity);
        omniruntime::mem::MemoryTrace::SubVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
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
