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
#include "util/compiler_util.h"
#include "small_string_container.h"
#include "large_string_container.h"

namespace omniruntime::vec::unsafe {
class UnsafeBaseVector;
class UnsafeVector;
class UnsafeDictionaryVector;
class UnsafeStringVector;
}

namespace omniruntime::vec {
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
            memset(this->nulls.get(), 0, sizeB * size);
            omniruntime::mem::ThreadMemoryManager::ReportMemory(GetNullsCapacity(this->nulls));
        }
    }

    virtual ~BaseVector()
    {
        if (!isSliced) {
            omniruntime::mem::ThreadMemoryManager::ReclaimMemory(GetNullsCapacity(this->nulls));
        }
    }

    /* *
     * set the element at the index position to null
     * Attention: String vector has its own SetNull, need call corresponding SetNull when string vector
     * @param index
     *        */
    void ALWAYS_INLINE SetNull(int32_t index)
    {
        nulls[index] = true;
        hasNull = true;
    }

    void ALWAYS_INLINE SetNotNull(int32_t index)
    {
        nulls[index] = false;
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

    int32_t ALWAYS_INLINE GetOffset()
    {
        return offset;
    }

protected:
    int64_t ALWAYS_INLINE GetNullsCapacity(std::shared_ptr<bool[]> ptr)
    {
#ifdef COVERAGE
        return size * sizeof(bool);
#else
        return  malloc_usable_size(ptr.get());
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
     */
    explicit Vector(int vSize) : BaseVector(vSize), values(std::shared_ptr<RAW_DATA_TYPE[]>(new RAW_DATA_TYPE[vSize]))
    {
        // vector class, and values total capacity
        int64_t vectorCapacity = sizeof(Vector<RAW_DATA_TYPE>) + GetValuesCapacity(values);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
    }

    virtual ~Vector()
    {
        if ((encoding == OMNI_FLAT) && (values != nullptr)) {
            int64_t vectorCapacity = sizeof(Vector<RAW_DATA_TYPE>);
            if (!isSliced) {
                // vector class, and values total capacity
                vectorCapacity += GetValuesCapacity(values);
            }
            omniruntime::mem::ThreadMemoryManager::ReclaimMemory(vectorCapacity);
        }
    }

    // used for container vector, dictionary vector and varchar vector.
    Vector(int vSize, Encoding encoding, std::shared_ptr<bool[]> nulls = nullptr) : BaseVector(vSize, encoding, nulls)
    {}

    // used for vector slice, sliced vector use same values as parent vector
    Vector(int vSize, Encoding encoding, std::shared_ptr<bool[]> nulls, std::shared_ptr<RAW_DATA_TYPE[]> values)
        : BaseVector(vSize, encoding, nulls)
    {
        this->values = values;  // copy the data field shared_ptr
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<RAW_DATA_TYPE>);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
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
            for (int32_t i = 0; i < length; i++) {
                nulls[i + positionOffset] = src->IsNull(i);
                SetValue(i + positionOffset, src->GetValue(i));
            }
        } else { // for dictionay
            auto src = reinterpret_cast<Vector<DictionaryContainer<RAW_DATA_TYPE>> *>(other);
            for (int32_t i = 0; i < length; i++) {
                nulls[i + positionOffset] = src->IsNull(i);
                SetValue(i + positionOffset, src->GetValue(i));
            }
        }
    }

    /* *
     * Copies the values of the vector at the indicated positions
     * @param positions
     * @param offset
     * @param length
     */
    std::unique_ptr<Vector<RAW_DATA_TYPE>> CopyPositions(const int *positions, int positionOffset, int length)
    {
        if ((positions == nullptr) || (length < 0)) {
            LogError("positions is null or the input length is incorrect: %d.", length);
            return nullptr;
        }
        auto vector = std::make_unique<Vector<RAW_DATA_TYPE>>(length);
        for (int32_t i = 0; i < length; i++) {
            int position = positions[positionOffset + i];
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
    std::unique_ptr<Vector<RAW_DATA_TYPE>> Slice(int positionOffset, int length, bool isCopy = false)
    {
        if (positionOffset + length > this->size) {
            LogError("slice vector out of range(needed size:%d, real size:%d).", positionOffset + length, this->size);
            return nullptr;
        }
        auto sliced = std::make_unique<Vector<RAW_DATA_TYPE>>(length, this->encoding, nulls, values);
        sliced->offset = offset + positionOffset; // update offset
        sliced->isSliced = true;
        return std::move(sliced);
    }

protected:
    int64_t ALWAYS_INLINE GetValuesCapacity(std::shared_ptr<RAW_DATA_TYPE[]> ptr)
    {
#ifdef COVERAGE
        return size * sizeof(RAW_DATA_TYPE);
#else
        return  malloc_usable_size(ptr.get());
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
        std::shared_ptr<bool[]> nulls)
        : Vector<RAW_DATA_TYPE>(size, OMNI_DICTIONARY /* * create enum for encodings */, nulls), container(container)
    {
        // vector class capacity, size* sizeof(bool) is reclaimed in BaseVector destructor.
        int64_t vectorCapacity = sizeof(Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>) + BaseVector::GetNullsCapacity(nulls);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
    }

    // used for vector slice, sliced vector use same container as parent vector
    Vector(int size, std::shared_ptr<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>> container,
           std::shared_ptr<bool[]> nulls, bool isSliced)
            : Vector<RAW_DATA_TYPE>(size, OMNI_DICTIONARY /* * create enum for encodings */, nulls), container(container)
    {
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
    }

    ~Vector()
    {
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>);
        omniruntime::mem::ThreadMemoryManager::ReclaimMemory(vectorCapacity);

        // nulls reclaimed by baseVector
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
    std::unique_ptr<Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>> CopyPositions(const int *positions,
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
        for (int32_t i = 0; i < length; i++) {
            newNulls[i] = this->IsNull(positions[i + positionOffset]);
            newPositions[i] = positions[i + positionOffset] + this->offset;
        }
        // new container
        std::shared_ptr<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>> newContainer =
            container->CopyPosition(newPositions.data(), length);
        return std::make_unique<Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>>(length, newContainer, newNulls);
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
    std::unique_ptr<Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>> Slice(int positionOffset, int length,
        bool isCopy = false)
    {
        if (positionOffset + length > this->size) {
            LogError("slice vector out of range(needed size:%d, real size:%d).", positionOffset + length, this->size);
            return nullptr;
        }
        // copy the data field shared_ptr
        auto sliced = std::make_unique<Vector<DictionaryContainer<RAW_DATA_TYPE, CONTAINER>>>(length, this->container,
            this->nulls, true);
        sliced->offset = this->offset + positionOffset; // update offset
        sliced->isSliced = true;
        return std::move(sliced);
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
        this->stringEncoding = OMNI_LARGE_STRING;
        // default string_view vector use large string encoding
        this->container = std::make_shared<LargeStringContainer<std::string_view>>(size, capacityInBytes);
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<LargeStringContainer<RAW_DATA_TYPE>>);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
    }

    // used for vector slice, sliced vector use same container as parent vector
    explicit Vector(int size, std::shared_ptr<LargeStringContainer<std::string_view>> container,
        std::shared_ptr<bool[]> nulls)
        : Vector<RAW_DATA_TYPE>(size, OMNI_FLAT /* * create enum for encodings */, nulls), container(container)
    {
        this->stringEncoding = OMNI_LARGE_STRING;
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<LargeStringContainer<RAW_DATA_TYPE>>);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
    }

    ~Vector()
    {
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<LargeStringContainer<RAW_DATA_TYPE>>);
        omniruntime::mem::ThreadMemoryManager::ReclaimMemory(vectorCapacity);
    }

    /* *
     * set the element at the index position to null
     * @param index
     *      */
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
    void ALWAYS_INLINE SetValue(int index, std::string_view value)
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
                if (!isNull) {
                    SetValue(i + positionOffset, src->GetValue(i));
                } else {
                    SetNull(i + positionOffset);
                }
            }
        } else { // for dictionay
            auto src = reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(other);
            for (int32_t i = 0; i < length; i++) {
                bool isNull = src->IsNull(i);
                if (!isNull) {
                    SetValue(i + positionOffset, src->GetValue(i));
                } else {
                    SetNull(i + positionOffset);
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
    std::unique_ptr<Vector<LargeStringContainer<std::string_view>>> CopyPositions(const int *positions, int offset,
        int length)
    {
        if ((positions == nullptr) || (length < 0)) {
            LogError("positions is null or the input length is incorrect: %d.", length);
            return nullptr;
        }

        auto vector = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(length);
        for (int32_t i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (this->IsNull(position)) {
                vector->SetNull(i);
            } else {
                vector->SetValue(i, GetValue(position));
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
    std::unique_ptr<Vector<LargeStringContainer<std::string_view>>> Slice(int positionOffset, int length,
        bool isCopy = false)
    {
        if (positionOffset + length > this->size) {
            LogError("slice vector out of range(needed size:%d, real size:%d).", positionOffset + length, this->size);
            return nullptr;
        }
        // copy the data field shared_ptr
        auto sliced = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(length, container, this->nulls);
        sliced->offset = this->offset + positionOffset; // update offset
        sliced->isSliced = true;
        return std::move(sliced);
    }

private:
    friend class unsafe::UnsafeStringVector;
    std::shared_ptr<LargeStringContainer<std::string_view>> container;
};

/**
 * The master template for the vector class supporting string encoding
 * @tparam string_view : meta of string
 */
template <typename RAW_DATA_TYPE>
class Vector<SmallStringContainer<RAW_DATA_TYPE>> final : public Vector<RAW_DATA_TYPE> {
public:
    explicit Vector(int size) : Vector<RAW_DATA_TYPE>(size, OMNI_FLAT /* * create enum for encodings */)
    {
        this->stringEncoding = OMNI_SMALL_STRING;
        this->container = std::make_shared<SmallStringContainer<std::string_view>>(size);
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<SmallStringContainer<RAW_DATA_TYPE>>);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
    }

    // used for vector slice, sliced vector use same container as parent vector
    explicit Vector(int size, std::shared_ptr<SmallStringContainer<std::string_view>> container,
        std::shared_ptr<bool[]> nulls)
        : Vector<RAW_DATA_TYPE>(size, OMNI_FLAT /* * create enum for encodings */, nulls), container(container)
    {
        this->stringEncoding = OMNI_SMALL_STRING;
        // vector class capacity
        int64_t vectorCapacity = sizeof(Vector<SmallStringContainer<RAW_DATA_TYPE>>);
        omniruntime::mem::ThreadMemoryManager::ReportMemory(vectorCapacity);
    }

    ~Vector()
    {
        int64_t vectorCapacity = sizeof(Vector<SmallStringContainer<RAW_DATA_TYPE>>);
        omniruntime::mem::ThreadMemoryManager::ReclaimMemory(vectorCapacity);
    }

    /* *
     * Set the value at the indicated index
     * @param index
     * @param value
     */
    void ALWAYS_INLINE SetValue(int index, std::string_view value)
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
            auto src = reinterpret_cast<Vector<SmallStringContainer<std::string_view>> *>(other);
            for (int32_t i = 0; i < length; i++) {
                bool isNull = src->IsNull(i);
                this->nulls[i + positionOffset] = isNull;
                if (!isNull) {
                    SetValue(i + positionOffset, src->GetValue(i));
                }
            }
        } else { // for dictionay
            auto src = reinterpret_cast<Vector<DictionaryContainer<std::string_view, SmallStringContainer>> *>(other);
            for (int32_t i = 0; i < length; i++) {
                bool isNull = src->IsNull(i);
                this->nulls[i + positionOffset] = isNull;
                if (!isNull) {
                    SetValue(i + positionOffset, src->GetValue(i));
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
    std::unique_ptr<Vector<SmallStringContainer<std::string_view>>> CopyPositions(const int *positions, int offset,
        int length)
    {
        if ((positions == nullptr) || (length < 0)) {
            LogError("positions is null or the input length is incorrect: %d.", length);
            return nullptr;
        }

        auto vector = std::make_unique<Vector<SmallStringContainer<std::string_view>>>(length);
        for (int32_t i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (this->IsNull(position)) {
                vector->SetNull(i);
            } else {
                vector->SetValue(i, GetValue(position));
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
    std::unique_ptr<Vector<SmallStringContainer<std::string_view>>> Slice(int positionOffset, int length,
        bool isCopy = false)
    {
        if (positionOffset + length > this->size) {
            LogError("slice vector out of range(needed size:%d, real size:%d).", positionOffset + length, this->size);
            return nullptr;
        }
        // copy the data field shared_ptr
        auto sliced = std::make_unique<Vector<SmallStringContainer<std::string_view>>>(length, container, this->nulls);
        sliced->offset = this->offset + positionOffset; // update offset
        sliced->isSliced = true;
        return std::move(sliced);
    }

private:
    friend class unsafe::UnsafeStringVector;
    std::shared_ptr<SmallStringContainer<std::string_view>> container;
};
}

#endif // OMNI_RUNTIME_VECTOR_H
