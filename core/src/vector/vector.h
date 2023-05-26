/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: vector  implementation
 */
#ifndef OMNI_RUNTIME_VECTOR_H
#define OMNI_RUNTIME_VECTOR_H
#pragma once

#include <cstdlib>
#include <functional>
#include <type_traits>
#include <jemalloc/jemalloc.h>
#include <memory>
#include "type_utils.h"
#include "dictionary_container.h"
#include "util/debug.h"
#include "util/bit_map.h"
#include "util/compiler_util.h"
#include "large_string_container.h"
#include "memory/memory_trace.h"
#include "type/data_type.h"

namespace omniruntime::vec::unsafe {
class UnsafeBaseVector;
class UnsafeVector;
class UnsafeDictionaryVector;
class UnsafeStringVector;
}

namespace omniruntime::vec {
using namespace type;
enum Encoding {
    OMNI_FLAT = 0,       // ordinary vector, storing primitive data types, such as int, long, boolean
    OMNI_DICTIONARY = 1, // if dictionary needs to be combined with varchar, get encoding by enum 'StringEncoding'
    OMNI_ENCODING_CONTAINER = 2, // the temporarily added code is mainly used for the agg avg partial, and the vector
                                 // implementation
                                 // is also placed in the hash agg module
    OMNI_ENCODING_INVALID
};

class BaseVector {
public:
    BaseVector() = default;
    explicit BaseVector(int32_t size) : BaseVector(size, OMNI_FLAT /* * ordinary vector */) {}

    BaseVector(int32_t size, Encoding encoding, std::shared_ptr<bool[]> nulls = nullptr)
        : size(size), encoding(encoding), offset(0), nulls(nulls), hasNull(false), isSliced(false)
    {
        const int constexpr sizeB = sizeof(bool);
        if (this->nulls == nullptr) {
            // fixme: Initialization of nulls should replace new with make_shared, when the C++ version is upgraded
            // to 20.
            this->nulls = std::shared_ptr<bool[]>(new bool[size]);
            memset_sp(this->nulls.get(), sizeB * size, 0, sizeB * size);
        }
    }

    virtual ~BaseVector() = default;

    /* *
     * set the element at the index position to null
     * Attention: String vector has its own SetNull, need call corresponding SetNull when string vector
     * @param index
     *                */
    void ALWAYS_INLINE SetNull(int32_t index)
    {
        nulls[index] = true;
        hasNull = true;
    }

    void ALWAYS_INLINE SetNotNull(int32_t index)
    {
        nulls[index] = false;
    }

    void SetNulls(int startIndex, bool *nullsPtr, int length)
    {
        if (startIndex + length > this->size) {
            LogError("vector is out of range(needed size:%d, real size:%d).", startIndex + length, this->size);
            return;
        }

        bool *startAddr = reinterpret_cast<bool *>(this->nulls.get());
        errno_t ret = memcpy_s(startAddr + startIndex, length * sizeof(bool), nullsPtr, length * sizeof(bool));
        if (ret != EOK) {
            LogError("memory copy failed %d, vec size %d, length %d, startIndex %d startAddr %x.", ret, this->size,
                length, startIndex, startAddr);
        }
    }

    /* *
     * determine is the element at the index position is null
     * @param index
     * @return true is null, otherwise non-null
     */
    bool ALWAYS_INLINE IsNull(int32_t index)
    {
        return nulls[index + offset];
    }

    Encoding ALWAYS_INLINE GetEncoding()
    {
        return encoding;
    }

    StringEncoding ALWAYS_INLINE GetStringEncoding()
    {
        return stringEncoding;
    }

    int ALWAYS_INLINE GetSize()
    {
        return size;
    }

    void ALWAYS_INLINE SetNullFlag(bool newHasNull)
    {
        hasNull = newHasNull;
    }

    bool ALWAYS_INLINE HasNull()
    {
        if (hasNull) {
            return true;
        }

        const bool *p = nulls.get() + offset;
        // Check if first bool differs from expected.
        if (*p) {
            hasNull = true;
            return true;
        }

        uint32_t nearHalf;
        uint32_t farHalf;
        uint64_t bytesStream = sizeof(bool) * size;
        while (bytesStream > 1) {
            nearHalf = bytesStream / 2;
            farHalf = bytesStream - nearHalf;
            if (memcmp(p, p + farHalf, nearHalf)) {
                // first half and last half differ, so null exist.
                hasNull = true;
                return true;
            }
            bytesStream = farHalf; // for odd length, new size should be farHalf
        }
        return false;
    }

    int32_t GetNullCount() const
    {
        bool *nullsPtr = nulls.get();
        return hasNull ? BitMap::ComputeBitCount(reinterpret_cast<const uint8_t *>(nullsPtr), offset, size) : 0;
    }

    int32_t ALWAYS_INLINE GetOffset()
    {
        return offset;
    }

    DataTypeId ALWAYS_INLINE GetDataTypeId()
    {
        return dataTypeId;
    }

protected:
    int64_t ALWAYS_INLINE GetNullsCapacity(bool *nulls)
    {
#ifdef COVERAGE
        return size * sizeof(bool);
#else
        return malloc_usable_size(nulls);
#endif
    }

    friend class unsafe::UnsafeBaseVector;
    int32_t size;
    Encoding encoding;             // vector encoding, such as flat, dictionary
    StringEncoding stringEncoding; // varchar encoding, such as large string encoding, small string encoding
    int32_t offset;
    std::shared_ptr<bool[]> nulls; // whether the element is null
    bool hasNull;
    bool isSliced;
    DataTypeId dataTypeId;
};

/**
 * The partially specialized implementation for primitive types without encoding
 * @tparam RAW_DATA_TYPE
 */
template <typename RAW_DATA_TYPE> class Vector : public BaseVector {
public:
    /* *
     * Constructor for data types of arithematic types without encoding
     * fixme: Initialization of values should replace new with make_shared, when the C++ version is upgraded to 20.
     * @param vSize: size of array in variables nulls and values
     * @param dataTypeId: the dataTypeId of vector
     */
    // TODO: the dataTypeId will be used in operator in the future.
    Vector(int vSize, DataTypeId dataTypeId = TYPE_ID<RAW_DATA_TYPE>)
        : BaseVector(vSize), values(std::shared_ptr<RAW_DATA_TYPE[]>(new RAW_DATA_TYPE[vSize]))
    {
        this->dataTypeId = dataTypeId;
        // vector class, values total capacity and nulls total capacity
        int64_t vectorCapacity =
            sizeof(Vector<RAW_DATA_TYPE>) + GetValuesCapacity(values.get()) + BaseVector::GetNullsCapacity(nulls.get());
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::AddVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
    }

    virtual ~Vector() override
    {
        if ((encoding == OMNI_FLAT) && (values != nullptr)) {
            int64_t vectorCapacity = sizeof(Vector<RAW_DATA_TYPE>);
            if (!isSliced) {
                // vector class, nulls total capacity and values total capacity
                vectorCapacity += BaseVector::GetNullsCapacity(nulls.get()) + GetValuesCapacity(values.get());
            }
            omniruntime::mem::ThreadMemoryManager::ReclaimMemory(vectorCapacity);
#ifdef TRACE
            omniruntime::mem::MemoryTrace::SubVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
        }
    }

    // used for container vector, dictionary vector and varchar vector.
    Vector(int vSize, Encoding encoding, std::shared_ptr<bool[]> nulls = nullptr) : BaseVector(vSize, encoding, nulls)
    {}

    // used for vector slice, sliced vector use same values as parent vector
    Vector(int vSize, Encoding encoding, std::shared_ptr<bool[]> nulls, std::shared_ptr<RAW_DATA_TYPE[]> values,
        DataTypeId dataTypeId = TYPE_ID<RAW_DATA_TYPE>)
        : BaseVector(vSize, encoding, nulls)
    {
        this->dataTypeId = dataTypeId;
        this->values = values; // copy the data field shared_ptr
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<RAW_DATA_TYPE>);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::AddVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
    }

    /* *
     * Set the value at the indicated index
     * @param index
     * @param value
     */
    void ALWAYS_INLINE SetValue(int index, typename PARAM_TYPE<RAW_DATA_TYPE>::type value)
    {
        values[index] = value;
    }

    /* *
     * Set the values from start index
     * @param startIndex
     * @param values
     * @param length
     */
    void ALWAYS_INLINE SetValues(int startIndex, const void *values, int length)
    {
        if (startIndex + length > this->size) {
            LogError("vector is out of range(needed size:%d, real size:%d).", startIndex + length, this->size);
            return;
        }

        RAW_DATA_TYPE *startAddr = reinterpret_cast<RAW_DATA_TYPE *>(this->values.get());
        errno_t ret =
            memcpy_s(startAddr + startIndex, length * sizeof(RAW_DATA_TYPE), values, length * sizeof(RAW_DATA_TYPE));
        if (ret != EOK) {
            LogError("memory copy failed %d, vec size %d, length %d, startIndex %d startAddr %x.", ret, this->size,
                length, startIndex, startAddr);
        }
    }

    /* *
     * Gets the value of the vector at the indicated index
     * @param index
     * @return
     */
    ALWAYS_INLINE typename PARAM_TYPE<RAW_DATA_TYPE>::type GetValue(int index)
    {
        return values[index + offset];
    }

    /* *
     * append data to the current vector
     *
     * @param other the dst data from
     * @param positionOffset element position
     * @param length number of elements
     */
    void Append(BaseVector *other, int positionOffset, int length)
    {
        if (positionOffset + length > size) {
            LogError("append vector out of range(needed size:%d, real size:%d).", positionOffset + length, size);
            return;
        }

        if (other->GetEncoding() == OMNI_FLAT) {
            auto src = reinterpret_cast<Vector<RAW_DATA_TYPE> *>(other);
            SetNulls(positionOffset, src->nulls.get() + src->offset, length);
            SetValues(positionOffset, src->values.get() + src->offset, length);
        } else { // for dictionay
            auto src = reinterpret_cast<Vector<DictionaryContainer<RAW_DATA_TYPE>> *>(other);
            for (int32_t i = 0; i < length; i++) {
                auto index = i + positionOffset;
                nulls[index] = src->IsNull(i);
                SetValue(index, src->GetValue(i));
            }
        }
    }

    /* *
     * Copies the values of the vector at the indicated positions
     * @param positions
     * @param offset
     * @param length
     */
    Vector<RAW_DATA_TYPE> *CopyPositions(const int *positions, int positionOffset, int length)
    {
        if ((positions == nullptr) || (length < 0)) {
            LogError("positions is null or the input length is incorrect: %d.", length);
            return nullptr;
        }
        auto vector = new Vector<RAW_DATA_TYPE>(length);
        auto startPositions = positions + positionOffset;
        for (int32_t i = 0; i < length; i++) {
            int position = startPositions[i];
            vector->nulls[i] = IsNull(position);
            vector->SetValue(i, GetValue(position));
        }
        return vector;
    }

    /* *
     * Create a new vector based on a slice of the vector. The returned Vector is
     * a read-only vector which shares data memory with the original vector,
     * if the vector data is modified, the original vector data is also modified.
     *
     * @param positionOffset
     * @param length
     * @param isCopy reserved parameters
     */
    Vector<RAW_DATA_TYPE> *Slice(int positionOffset, int length, bool isCopy = false)
    {
        if (positionOffset + length > this->size) {
            LogError("slice vector out of range(needed size:%d, real size:%d).", positionOffset + length, this->size);
            return nullptr;
        }
        auto sliced = new Vector<RAW_DATA_TYPE>(length, this->encoding, nulls, values);
        sliced->offset = offset + positionOffset; // update offset
        sliced->isSliced = true;
        return sliced;
    }

protected:
    int64_t ALWAYS_INLINE GetValuesCapacity(RAW_DATA_TYPE *values)
    {
#ifdef COVERAGE
        return size * sizeof(RAW_DATA_TYPE);
#else
        return malloc_usable_size(values);
#endif
    }

    friend class unsafe::UnsafeVector;
    std::shared_ptr<RAW_DATA_TYPE[]> values; // for primitive types without encoding
};

/**
 * dictionary encoding implementation
 * @tparam RAW_DATA_TYPE
 */
template <typename RAW_DATA_TYPE, template <typename> typename CONTAINER>
class Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>> final : public Vector<RAW_DATA_TYPE> {
public:
    Vector(int size, std::shared_ptr<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>> container,
        std::shared_ptr<bool[]> nulls, DataTypeId dataTypeId = TYPE_ID<RAW_DATA_TYPE>)
        : Vector<RAW_DATA_TYPE>(size, OMNI_DICTIONARY /* * create enum for encodings */, nulls), container(container)
    {
        this->dataTypeId = dataTypeId;
        // vector class capacity, nulls total capacity and container total capacity.
        int64_t vectorCapacity = sizeof(Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>) +
            BaseVector::GetNullsCapacity(nulls.get()) + container->GetContainerCapacity();
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::AddVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
    }

    // used for vector slice, sliced vector use same container as parent vector
    Vector(int size, std::shared_ptr<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>> container,
        std::shared_ptr<bool[]> nulls, bool isSliced, DataTypeId dataTypeId = TYPE_ID<RAW_DATA_TYPE>)
        : Vector<RAW_DATA_TYPE>(size, OMNI_DICTIONARY /* * create enum for encodings */, nulls), container(container)
    {
        this->dataTypeId = dataTypeId;
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::AddVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
    }

    ~Vector() override
    {
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>);
        if (!this->isSliced) {
            vectorCapacity += BaseVector::GetNullsCapacity(this->nulls.get()) + container->GetContainerCapacity();
        }
        omniruntime::mem::ThreadMemoryManager::ReclaimMemory(vectorCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::SubVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
    }

    /* *
     * Set the value at the indicated index
     * @param index
     * @param value
     */
    void ALWAYS_INLINE SetValue(int index, typename PARAM_TYPE<RAW_DATA_TYPE>::type value)
    {
        container->SetValue(index, value);
    }

    /* *
     * Gets the value of the vector at the indicated index
     * @param index
     * @return
     */
    ALWAYS_INLINE typename PARAM_TYPE<RAW_DATA_TYPE>::type GetValue(int index)
    {
        return container->GetValue(index + this->offset);
    }

    /* *
     * Copies the values of the vector at the indicated positions
     * @param positions
     * @param offset
     * @param length
     */
    Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>> *CopyPositions(const int *positions,
        int positionOffset, int length)
    {
        if ((positions == nullptr) || (length < 0)) {
            LogError("positions is null or the input length is incorrect: %d.", length);
            return nullptr;
        }

        // new nulls
        // fixme: Initialization of nulls should replace new with make_shared, when the C++ version is upgraded
        // to 20.
        std::shared_ptr<bool[]> newNulls = std::shared_ptr<bool[]>(new bool[length]);
        // new positions
        std::vector<int32_t> newPositions(length);
        auto startPositions = positions + positionOffset;
        for (int32_t i = 0; i < length; i++) {
            auto position = startPositions[i];
            newNulls[i] = this->IsNull(position);
            newPositions[i] = position + this->offset;
        }
        // new container
        std::shared_ptr<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>> newContainer =
            container->CopyPositions(newPositions.data(), length);
        return new Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>(length, newContainer, newNulls,
            this->dataTypeId);
    }

    /* *
     * Create a new vector based on a slice of the vector. The returned Vector is
     * a read-only vector which shares data memory with the original vector,
     * if the vector data is modified, the original vector data is also modified.
     *
     * @param positionOffset
     * @param length
     * @param isCopy
     */
    Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>> *Slice(int positionOffset, int length, bool isCopy = false)
    {
        if (positionOffset + length > this->size) {
            LogError("slice vector out of range(needed size:%d, real size:%d).", positionOffset + length, this->size);
            return nullptr;
        }
        // copy the data field shared_ptr
        auto sliced = new Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>(length, this->container,
            this->nulls, true, this->dataTypeId);
        sliced->offset = this->offset + positionOffset; // update offset
        sliced->isSliced = true;
        return sliced;
    }

private:
    friend class unsafe::UnsafeDictionaryVector;
    std::shared_ptr<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>> container;
};

/**
 * The master template for the vector class supporting string encoding
 * @tparam string_view : meta of string
 */
template <typename RAW_DATA_TYPE>
class Vector<LargeStringContainer<RAW_DATA_TYPE>> final : public Vector<RAW_DATA_TYPE> {
public:
    explicit Vector(int size, int capacityInBytes = INITIAL_STRING_SIZE)
        : Vector<RAW_DATA_TYPE>(size, OMNI_FLAT /* * create enum for encodings */)
    {
        this->dataTypeId = OMNI_CHAR;
        this->stringEncoding = OMNI_LARGE_STRING;
        // default string_view vector use large string encoding
        this->container = std::make_shared<LargeStringContainer<std::string_view>>(size, capacityInBytes);
        // vector class capacity, nulls total capacity and values total capacity
        int64_t vectorCapacity = sizeof(Vector<LargeStringContainer<RAW_DATA_TYPE>>) +
            BaseVector::GetNullsCapacity(this->nulls.get()) + container->GetContainerCapacity();
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::AddVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
    }

    // used for vector slice, sliced vector use same container as parent vector
    explicit Vector(int size, std::shared_ptr<LargeStringContainer<std::string_view>> container,
        std::shared_ptr<bool[]> nulls, DataTypeId dataTypeId = OMNI_CHAR)
        : Vector<RAW_DATA_TYPE>(size, OMNI_FLAT /* * create enum for encodings */, nulls), container(container)
    {
        this->dataTypeId = dataTypeId;
        this->stringEncoding = OMNI_LARGE_STRING;
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<LargeStringContainer<RAW_DATA_TYPE>>);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::AddVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
    }

    ~Vector() override
    {
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<LargeStringContainer<RAW_DATA_TYPE>>);
        if (!this->isSliced) {
            vectorCapacity += BaseVector::GetNullsCapacity(this->nulls.get()) + container->GetContainerCapacity();
        }
        omniruntime::mem::ThreadMemoryManager::ReclaimMemory(vectorCapacity);
#ifdef TRACE
        omniruntime::mem::MemoryTrace::SubVectorMemory(reinterpret_cast<uintptr_t>(this), vectorCapacity);
#endif
    }

    /* *
     * set the element at the index position to null
     * @param index
     *             */
    void ALWAYS_INLINE SetNull(int32_t index)
    {
        BaseVector::SetNull(index);
        container->SetNull(index);
    }

    /* *
     * Set the value at the indicated index
     * @param index
     * @param value
     */
    void ALWAYS_INLINE SetValue(int index, std::string_view &value)
    {
        container->SetValue(index, value);
    }

    /* *
     * Gets the value of the vector at the indicated index
     * @param index
     * @return
     */
    ALWAYS_INLINE std::string_view GetValue(int index)
    {
        return container->GetValue(index + this->offset);
    }

    /* *
     * append data to the current vector
     *
     * @param other the dst data from
     * @param positionOffset element position
     * @param length number of elements
     */
    void Append(BaseVector *other, int positionOffset, int length)
    {
        if (positionOffset + length > this->size) {
            LogError("append vector out of range(needed size:%d, real size:%d).", positionOffset + length, this->size);
            return;
        }

        if (other->GetEncoding() == OMNI_FLAT) {
            auto src = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(other);
            for (int32_t i = 0; i < length; i++) {
                bool isNull = src->IsNull(i);
                auto index = i + positionOffset;
                if (!isNull) {
                    auto value = src->GetValue(i);
                    SetValue(index, value);
                } else {
                    SetNull(index);
                }
            }
        } else { // for dictionay
            auto src = reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(other);
            for (int32_t i = 0; i < length; i++) {
                bool isNull = src->IsNull(i);
                auto index = i + positionOffset;
                if (!isNull) {
                    auto value = src->GetValue(i);
                    SetValue(index, value);
                } else {
                    SetNull(index);
                }
            }
        }
    }

    /* *
     * Copies the values of the vector at the indicated positions
     * @param positions
     * @param offset
     * @param length
     */
    Vector<LargeStringContainer<std::string_view>> *CopyPositions(const int *positions, int offset, int length)
    {
        if ((positions == nullptr) || (length < 0)) {
            LogError("positions is null or the input length is incorrect: %d.", length);
            return nullptr;
        }

        auto vector = new Vector<LargeStringContainer<std::string_view>>(length);
        auto startPositions = positions + offset;
        for (int32_t i = 0; i < length; i++) {
            auto position = startPositions[i];
            if (this->IsNull(position)) {
                vector->SetNull(i);
            } else {
                auto value = GetValue(position);
                vector->SetValue(i, value);
            }
        }
        return vector;
    }

    /* *
     * Create a new vector based on a slice of the vector. The returned Vector is
     * a read-only vector which shares data memory with the original vector,
     * if the vector data is modified, the original vector data is also modified.
     *
     * @param positionOffset
     * @param length
     */
    Vector<LargeStringContainer<std::string_view>> *Slice(int positionOffset, int length, bool isCopy = false)
    {
        if (positionOffset + length > this->size) {
            LogError("slice vector out of range(needed size:%d, real size:%d).", positionOffset + length, this->size);
            return nullptr;
        }
        // copy the data field shared_ptr
        auto sliced = new Vector<LargeStringContainer<std::string_view>>(length, container, this->nulls);
        sliced->offset = this->offset + positionOffset; // update offset
        sliced->isSliced = true;
        return std::move(sliced);
    }

private:
    friend class unsafe::UnsafeStringVector;
    std::shared_ptr<LargeStringContainer<std::string_view>> container;
};
}

#endif // OMNI_RUNTIME_VECTOR_H
