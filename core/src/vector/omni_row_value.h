/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Row buffer Header
 */

#ifndef OMNI_RUNTIME_OMNI_ROW_VALUE_H
#define OMNI_RUNTIME_OMNI_ROW_VALUE_H

#include "vector.h"
#include "omni_row.h"
#include "array_vector.h"
#include "type/data_types.h"
#include "type/data_types.h"
#include "vector_batch.h"
#include "util/debug.h"
/*
 * row format:
 * whichType 000：fixType
 * whichType 001: varChar
 * whichType 010: array
 * 
 * | whichType(3bit 000) + isNull(1bit) + neg(1bit) + sizeLength(3bit) | fixValue |
 * | whichType(3bit 001) + isNull(1bit) + sizeLength(4bit) | realLength(n bytes) | varcharValue |
 * | whichType(3bit 010) + isNull(1bit) + real_num_ele_size(4bit) | real_num_ele(n bytes) | element1 | element2 | ... |
 *
 */
namespace omniruntime {
namespace vec {
class RowBuffer;

class BaseSerialize {
public:
    static constexpr int32_t PrefixLen = 1;
    static constexpr int32_t BIT_64 = 64;
    static constexpr int32_t BIT_8 = 8;
    static constexpr int32_t BIT_1 = 1;
    static constexpr int32_t BIT_16 = 16;
    static constexpr int8_t VARCHAR_BIT = 1 << 6;
    static constexpr int8_t ARRAY_BIT = 1 << 7;
    static constexpr int8_t FIX_BIT = 0;
    static constexpr int8_t NULL_POS = 5;
    static constexpr int8_t NEG_POS = 4;

    /* *
     * @brief Translates the specified row from the given BaseVector into a local storage.
     *
     * This method takes a specific row (identified by rowIndex) from the provided BaseVector
     * and converts or copies its content to the local storage structure of the derived class.
     * The specific way of translation or storage is dependent on the implementation in the derived class.
     *
     * @param baseVector Pointer to the BaseVector from which the row is to be translated.
     * @param rowIndex The index of the row in the BaseVector that needs to be translated.
     */
    virtual void TransValue(BaseVector *baseVector, int32_t rowIndex)
    {
        throw exception::OmniException("BaseSerialize", "not implement");
    }

    /* *
     * @brief Computes the size of the compressed data buffer, including metadata.
     *
     * This method calculates the total size required to store the locally converted data
     * into a compressed buffer. The size includes both the metadata and the actual compressed
     * data. The method returns the computed size as an integer.
     *
     * @return The total size of the buffer needed to store the compressed data and metadata.
     */
    virtual int32_t CompactLength()
    {
        throw exception::OmniException("BaseSerialize", "not implement");
    }

    /* *
     * @brief Writes the locally stored data into the specified memory buffer and returns the
     * pointer to the subsequent available buffer location for further data storage.
     *
     * This method compresses data and copies metadata and data into the provided memory buffer `writeBuffer`.
     * After writing the data, it returns a pointer to the subsequent available buffer location,
     * which can be used for further data storage.
     *
     * @param writeBuffer Pointer to the memory buffer where the data will be written.
     * @return A pointer to the subsequent available buffer location after the written data.
     */
    virtual uint8_t *WriteBuffer(uint8_t *writeBuffer)
    {
        throw exception::OmniException("BaseSerialize", "WriteBuffer not implement");
    }

    /* *
     * @brief just for debug print in ut or e2e test
     */
    virtual void Print()
    {
        throw exception::OmniException("BaseSerialize", "Print not implement");
    }

    void Reset()
    {
        isNull = false;
    }

    void SetNull()
    {
        isNull = true;
    }

    virtual ~BaseSerialize() = default;

protected:
    bool isNull = false;
};

template <typename T, Encoding encoding = OMNI_FLAT> class SerializedValue : public BaseSerialize {
public:
    // set value from vector row
    void TransValue(BaseVector *baseVector, int32_t rowIndex)
    {
        if (baseVector->IsNull(rowIndex)) {
            SetNull();
        } else {
            isNull = false;
            if constexpr (encoding == OMNI_FLAT) {
                value = reinterpret_cast<VectorType *>(baseVector)->GetValue(rowIndex);
            } else if constexpr (encoding == OMNI_DICTIONARY) {
                value = reinterpret_cast<DicVectorType *>(baseVector)->GetValue(rowIndex);
            } else if constexpr (encoding == OMNI_ENCODING_ARRAY) {
                value = reinterpret_cast<ArrayVector *>(baseVector)->GetValue(rowIndex);
            } else {
                // OMNI_ENCODING_CONTAINER is only used for the agg avg partial in olk. row shuffle is not supported.
                std::string message = encoding + "encoding type is not supported for omni row";
                throw omniruntime::exception::OmniException("Encoding Unsupported", message);
            }
        }
    }

    // set value from row
    void SetValue(T setValue)
    {
        isNull = false;
        value = setValue;
    }

    /* *
     * length is final length
     * @return
     */
    int32_t CompactLength()
    {
        if (isNull) {
            return PrefixLen;
        }

        if constexpr (std::is_same_v<T, std::string_view>) {
            // for varchar
            uint8_t rowLenSize = CalMetaSize();
            return PrefixLen + rowLenSize + value.length();
        } else if constexpr (std::is_same_v<T, BaseVector*>) {
            return CompactArrayLength();
        } else {
            return PrefixLen + CalMetaSize();
        }
    }

    /**
     * Compute the array final length
     */
    int32_t CompactArrayLength()
    {
        if (isNull) {
            return PrefixLen;
        }
        DataTypeId arrayDataTypeId = value->GetTypeId();
        switch (arrayDataTypeId) {
            case OMNI_BYTE:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_BYTE>();
                break;
            case OMNI_SHORT:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_SHORT>();
                break;
            case OMNI_INT:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_INT>();
                break;
            case OMNI_DATE32:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_DATE32>();
                break;
            case OMNI_LONG:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_LONG>();
                break;
            case OMNI_TIMESTAMP:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_TIMESTAMP>();
                break;
            case OMNI_DECIMAL64:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_DECIMAL64>();
                break;
            case OMNI_DECIMAL128:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_DECIMAL128>();
                break;
            case OMNI_CHAR:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_CHAR>();
                break;
            case OMNI_VARCHAR:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_VARCHAR>();
                break;
            case OMNI_DOUBLE:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_DOUBLE>();
                break;
            case OMNI_BOOLEAN:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_BOOLEAN>();
                break;
            case OMNI_ARRAY:
                return PrefixLen + CalMetaSize() + CalElementSize<OMNI_ARRAY>();
                break;
            default:
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "This type not supported yet");
        }
    }

    template <DataTypeId id>
    uint8_t CalElementSize()
    {
        int32_t elementSize = 0;
        int32_t arraySize = value->GetSize();
        using elementT = typename NativeType<id>::type;
        SerializedValue<elementT> serializedValue;
        auto elementVector = reinterpret_cast<Vector<elementT> *>(value);
        for (int32_t index = 0; index < arraySize; index++) {
            elementT elementValue = elementVector->GetValue(index);
            serializedValue.SetValue(elementValue);
            elementSize += serializedValue.CompactLength();
            if constexpr (id == OMNI_ARRAY) {
                delete elementValue;
            }
        }
        return elementSize;
    }

    /* *
     * 1. If the data is a string, the function checks whether the string is empty and sets a flag accordingly.
     * The function then takes the length of the string and stores the length in a buffer.
     * Finally, the function copies the contents of the string into the buffer.
     * 2. If the data is of Decimal128 type, floating point type,
     * or Boolean type, the function copies the data to the buffer.
     * 3.  If the data is an integer type, and the data is negative,
     * the function inverts the data and sets a flag accordingly.
     * The function then calculates the number of bytes required for the data and copies the data into the buffer.
     *
     * @return a pointer to the buffer, pointing to the next free position.
     */
    uint8_t CalMetaSize()
    {
        if constexpr (std::is_same_v<T, std::string_view>) {
            // for varchar
            return (BIT_64 - __builtin_clzll(value.length()) + BIT_8) / BIT_8;
        } else if constexpr (std::is_same_v<T, double>) {
            return BIT_8;
        } else if constexpr (std::is_same_v<T, type::Decimal128>) {
            // decimal is fix 16 bit
            return BIT_16;
        } else if constexpr (std::is_same_v<T, bool>) {
            // bool value is in meta data
            return 0;
        } else if constexpr (std::is_same_v<T, BaseVector*>) {
            int32_t arraySize = value->GetSize();
            return (BIT_64 - __builtin_clzll(arraySize) + BIT_8) / BIT_8;
        } else {
            auto tmp = value;
            if (value < 0) {
                tmp = ~value;
            }
            uint8_t rowLenSize = (BIT_64 - __builtin_clzll(tmp) + BIT_8) / BIT_8;
            return rowLenSize;
        }
    }

    /* *
     *
     * @param writeBuffer : start point
     * @return writeBuffer + CompactLength()
     */
    uint8_t *WriteBuffer(uint8_t *writeBuffer) override
    {
        if constexpr (std::is_same_v<T, std::string_view>) {
            return WriteVarcharBuffer(writeBuffer);
        } else if constexpr (std::is_same_v<T, BaseVector*>) {
            return WriteArrayBuffer(writeBuffer);
        } else {
            if (isNull) {
                *writeBuffer = (FIX_BIT | (0x1 << NULL_POS));
                ++writeBuffer;
                return writeBuffer;
            }
            if constexpr (std::is_same_v<T, type::Decimal128>) {
                // fix len for decimal
                *writeBuffer = (FIX_BIT | (0x0 << NULL_POS) | BIT_16);
                ++writeBuffer;
                std::copy(reinterpret_cast<uint8_t *>(&value), reinterpret_cast<uint8_t *>(&value) + BIT_16,
                    writeBuffer);
                return writeBuffer + BIT_16;
            } else if constexpr (std::is_same_v<T, double>) {
                // fix len for decimal
                *writeBuffer = (FIX_BIT | (0x0 << NULL_POS) | BIT_8);
                ++writeBuffer;
                std::copy(reinterpret_cast<uint8_t *>(&value), reinterpret_cast<uint8_t *>(&value) + BIT_8,
                    writeBuffer);
#ifdef DEBUG
                LogDebug("double offset is %d\n", 1 + BIT_8);
#endif
                return writeBuffer + BIT_8;
            } else if constexpr (std::is_same_v<T, bool>) {
                // bool is too simple, so we put value in last bit of meta value
                *writeBuffer = (FIX_BIT | (0x0 << NULL_POS) | (value & 0x01));
                return writeBuffer + BIT_1;
            } else {
                return WriteFixValueNoNullBuffer(writeBuffer);
            }
        }
    }

private:
    uint8_t *WriteFixValueNoNullBuffer(uint8_t *writeBuffer)
    {
        auto tmp = value;
        bool neg = false;
        if (value < 0) {
            tmp = ~value;
            neg = true;
        }
        uint8_t rowLenSize = (BIT_64 - __builtin_clzll(tmp) + BIT_8) / BIT_8;
        *writeBuffer = (FIX_BIT | (isNull << NULL_POS) | (neg << NEG_POS) | rowLenSize);
        ++writeBuffer;
        std::copy(reinterpret_cast<uint8_t *>(&tmp), reinterpret_cast<uint8_t *>(&tmp) + rowLenSize, writeBuffer);
#ifdef DEBUG
        LogDebug("fix value offset is %d\n", 1 + rowLenSize);
#endif
        return writeBuffer + rowLenSize;
    }

    uint8_t *WriteVarcharBuffer(uint8_t *writeBuffer)
    {
        if (isNull) {
            *writeBuffer = (VARCHAR_BIT | (0x1 << NULL_POS));
            ++writeBuffer;
            return writeBuffer;
        }
        int32_t len = value.length();
        uint8_t rowLenSize = CalMetaSize();
        *writeBuffer = (VARCHAR_BIT | (isNull << NULL_POS) | rowLenSize);
        ++writeBuffer;
        std::copy(reinterpret_cast<uint8_t *>(&len), reinterpret_cast<uint8_t *>(&len) + rowLenSize, writeBuffer);
        writeBuffer += rowLenSize;
        std::copy(reinterpret_cast<const uint8_t *>(value.data()),
            reinterpret_cast<const uint8_t *>(value.data()) + value.size(), writeBuffer);
#ifdef DEBUG
        LogDebug("row write str value: offset in writebuffer is %d\n", 1 + rowLenSize + value.size());
#endif
        return writeBuffer + value.size();
    }

    uint8_t *WriteArrayBuffer(uint8_t *writeBuffer)
    {
        if (isNull) {
            *writeBuffer = (ARRAY_BIT | (0x1 << NULL_POS));
            ++writeBuffer;
            return writeBuffer;
        }

        int32_t arraySize = value->GetSize();
        uint8_t rowArraySize = CalMetaSize();
        *writeBuffer = (ARRAY_BIT | (isNull << NULL_POS) | rowArraySize);
        ++writeBuffer;
        std::copy(reinterpret_cast<uint8_t *>(&arraySize), reinterpret_cast<uint8_t *>(&arraySize) + rowArraySize, writeBuffer);
        writeBuffer += rowArraySize;

        Encoding arrayEncoding = value->GetEncoding();
        DataTypeId arrayDataTypeId = value->GetTypeId();
        switch (arrayDataTypeId) {
            case OMNI_BYTE:
                return SerializeArrayElements<OMNI_BYTE>(writeBuffer);
                break;
            case OMNI_SHORT:
                return SerializeArrayElements<OMNI_SHORT>(writeBuffer);
                break;
            case OMNI_INT:
                return SerializeArrayElements<OMNI_INT>(writeBuffer);
                break;
            case OMNI_DATE32:
                return SerializeArrayElements<OMNI_DATE32>(writeBuffer);
                break;
            case OMNI_LONG:
                return SerializeArrayElements<OMNI_LONG>(writeBuffer);
                break;
            case OMNI_TIMESTAMP:
                return SerializeArrayElements<OMNI_TIMESTAMP>(writeBuffer);
                break;
            case OMNI_DECIMAL64:
                return SerializeArrayElements<OMNI_DECIMAL64>(writeBuffer);
                break;
            case OMNI_DECIMAL128:
                return SerializeArrayElements<OMNI_DECIMAL128>(writeBuffer);
                break;
            case OMNI_CHAR:
                return SerializeArrayElements<OMNI_CHAR>(writeBuffer);
                break;
            case OMNI_VARCHAR:
                return SerializeArrayElements<OMNI_VARCHAR>(writeBuffer);
                break;
            case OMNI_DOUBLE:
                return SerializeArrayElements<OMNI_DOUBLE>(writeBuffer);
                break;
            case OMNI_BOOLEAN:
                return SerializeArrayElements<OMNI_BOOLEAN>(writeBuffer);
                break;
            case OMNI_ARRAY:
                return SerializeArrayElements<OMNI_ARRAY>(writeBuffer);
                break;
            default:
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", "This type not supported yet");
        }
    }

    template <DataTypeId id>
    uint8_t* SerializeArrayElements(uint8_t* writeBuffer)
    {
        int32_t arraySize = value->GetSize();
        using elementT = typename NativeType<id>::type;
        SerializedValue<elementT> serializedValue;
        auto elementVector = reinterpret_cast<Vector<elementT> *>(value);
        for (int32_t index = 0; index < arraySize; index++) {
            elementT elementValue = elementVector->GetValue(index);
            serializedValue.SetValue(elementValue);
            writeBuffer = serializedValue.WriteBuffer(writeBuffer);
            if constexpr (id == OMNI_ARRAY) {
                delete elementValue;
            }
        }
        delete value;
        return writeBuffer;
    }

    using VectorType = std::conditional_t<std::is_same_v<T, std::string_view>,
        Vector<LargeStringContainer<std::string_view>>, Vector<T>>;
    using DicVectorType = Vector<DictionaryContainer<T>>;
    T value;
};
}
}
#endif // OMNI_RUNTIME_OMNI_ROW_VALUE_H
