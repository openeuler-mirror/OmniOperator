/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef __VECTOR_H__
#define __VECTOR_H__

#include "util/debug.h"
#include "util/compiler_util.h"
#include "type/data_type.h"
#include "vector_reference.h"
#include "vector_allocator.h"
#include "vector_encoding.h"
#include "tracer/vector_tracer.h"
#include "util/bit_map.h"
#include "type/string_ref.h"
#include "memory/simple_arena_allocator.h"

namespace omniruntime {
namespace vec {
using DataTypeId = type::DataTypeId;
class Vector {
public:
    Vector(VectorAllocator *allocator, int capacityInBytes, int size, DataTypeId dataTypeId);

    virtual ~Vector();

    int GetSize() const
    {
        return size;
    }

    void SetSize(int newSize)
    {
        this->size = newSize;
    }

    int GetPositionOffset()
    {
        return positionOffset;
    }

    VectorReference *GetVectorReference()
    {
        return this->reference;
    }

    void SetVectorReference(VectorReference *vectorReference)
    {
        this->reference = vectorReference;
    }

    int64_t GetReference() const
    {
        return reference->GetRef();
    }

    VectorAllocator *GetAllocator() const
    {
        return allocator;
    }

    void *GetValues() const
    {
        return valuesAddress;
    }

    DataTypeId GetTypeId() const
    {
        return dataTypeId;
    }

    int GetCapacityInBytes() const
    {
        return capacityInBytes;
    }

    void *GetValueNulls() const
    {
        return valueNullsAddress;
    }

    void *GetValueOffsets() const
    {
        return valueOffsetsAddress;
    }

    bool IsValueNull(int index) const
    {
        return (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset];
    }

    void SetValueNull(int index)
    {
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = true;
        hasNull = true;
    }

    virtual void SetValueNull(int index, bool value)
    {
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = value;
        hasNull = value || hasNull;
    }

    void SetValueNotNull(int index)
    {
        (reinterpret_cast<bool *>(valueNullsAddress))[index + positionOffset] = false;
    }

    int GetValueOffset(int index) const
    {
        return static_cast<int32_t *>(valueOffsetsAddress)[index];
    }

    void SetValueOffset(int index, int valueOffset)
    {
        (static_cast<int32_t *>(valueOffsetsAddress))[index] = valueOffset;
    }

    virtual Vector *Slice(int positionOffset, int length) = 0;

    virtual Vector *CopyPositions(const int *positions, int offset, int length) = 0;

    virtual Vector *CopyRegion(int positionOffset, int length) = 0;

    virtual void Append(Vector *other, int positionOffset, int length) = 0;

    virtual int64_t ExpandDataCapacity(int32_t toCapacityInBytes)
    {
        LogInfo("%s", "unsupported expandDataCapacity");
        return 0;
    }

    void RecordStack(std::string &stack, VecOpType opType);

    void SetVectorTracer(VectorTracer *vectorTracer);

    VectorTracer *GetVectorTracer();

    virtual VectorEncoding GetEncoding()
    {
        return OMNI_VEC_ENCODING_FLAT;
    }

    virtual bool MayHaveNull() const
    {
        return hasNull;
    }

    void SetNullFlag(bool newHasNull)
    {
        hasNull = newHasNull;
    }

    virtual int32_t GetNullCount() const
    {
        return hasNull ?
            BitMap::ComputeBitCount(static_cast<const uint8_t *>(valueNullsAddress), positionOffset, size) :
            0;
    }
    /* *
     *
     * @param rowId   Serialize the rowId element of the current vector.
     * @param executionContext Allocate the memory for the serialization result through executionContent.
     * @param begin stores the serialization result of the previous vector.
     * If begin is nullptr, the rowId element of the current vector is the first element in the current row.
     * After the serialization is complete, the value of @begin changes to pos + the number of bytes serialized.
     * @return Serialized Results
     */
    virtual StringRef SerializeValue(size_t rowId, mem::SimpleArenaAllocator &executionContext,
        const uint8_t *&begin)
    {
        return {};
    }

    /* *
     *
     * @param rowId The deserialization result is stored in the rowId element of the vector.
     * @param pos Pointer to serialized data
     * @return Pointer to next serialized data
     */
    virtual const uint8_t *DeserializeValueIntoThis(size_t rowId, const uint8_t *pos)
    {
        return nullptr;
    }

protected:
    // this method is mainly used for vector slice
    Vector(Vector *vector, int size, int positionOffset);

    void SetValueNulls(int startIndex, bool *nulls, int length);

    void *valuesAddress = nullptr;
    void *valueNullsAddress = nullptr;
    void *valueOffsetsAddress = nullptr;
    int positionOffset = 0;
    int capacityInBytes = 0;
    int size = 0;
    DataTypeId dataTypeId;
    VectorReference *reference = nullptr;
    VectorTracer *tracer = nullptr;
    VectorAllocator *allocator = nullptr;
    bool hasNull;
};
} // namespace vec
} // namespace omniruntime
#endif // __VECTOR_H__
