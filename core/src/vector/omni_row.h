/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: Row buffer Header
 */

#ifndef OMNI_RUNTIME_OMNI_ROW_H
#define OMNI_RUNTIME_OMNI_ROW_H

/*
 * row :
 * | isVarchar(1bit 1) + isNull(1bit) + pad (3bit) + sizeLength(3bit) | realLength (n bytes)| varcharValue |
 * | isVarchar(1bit 0) + isNull(1bit) + neg(1bit) + pad (1bit) + sizeLength(4bit) | fixValue |
 *
 * for example
 * one row is
 * string("hello",5) + long(100) + long(null) + int(500) + string("world", 5) + string(null)
 * | 1 0 001 000 (b)| 5 | h e l l o  | + | 0 0 001 000| 100 |
 * + | 0 1 000 000 | + | 0 0 001 000 | 500|
 * + | 1 0 001 000 (b)| 5 | h e l l o  | + | 1 1 000 000 (b)|
 */
#include "vector.h"
#include "type/data_types.h"
#include "type/data_types.h"
#include "vector_batch.h"
#include "util/debug.h"

namespace omniruntime {
namespace vec {
class BaseSerialize {
protected:
    bool isNull = false;

public:
    static constexpr int32_t PrefixLen = 1;
    static constexpr int32_t BIT_64 = 64;
    static constexpr int32_t BIT_8 = 8;
    static constexpr int32_t BIT_1 = 1;
    static constexpr int32_t BIT_16 = 16;
    static constexpr int8_t VARCHAR_BIT = 1 << 7;
    static constexpr int8_t FIX_BIT = 0;
    static constexpr int8_t NULL_POS = 6;
    static constexpr int8_t NEG_POS = 5;

    virtual void TransValue(BaseVector *baseVector, int32_t rowIndex)
    {
        throw exception::OmniException("BaseSerialize", "not implement");
    }

    virtual int32_t CompactLength()
    {
        throw exception::OmniException("BaseSerialize", "not implement");
    }

    virtual uint8_t *WriteBuffer(uint8_t *writeBuffer)
    {
        throw exception::OmniException("BaseSerialize", "WriteBuffer not implement");
    }

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
};

template <typename T> class SerializedValue : public BaseSerialize {
private:
    T value;
    using VectorType = std::conditional_t<std::is_same_v<T, std::string_view>,
        Vector<LargeStringContainer<std::string_view>>, Vector<T>>;

public:
    // only 1 Bytes represent following coding :
    // 'isVarchar(1bit 1) + isNull(1bit) + sizeLength(3bit)  + pad (3bit)'
    // or 'isVarchar(1bit 0) + isNull(1bit) + sizeLength(3bit) '
    // set value from vector row
    void TransValue(BaseVector *baseVector, int32_t rowIndex)
    {
        if (baseVector->IsNull(rowIndex)) {
            SetNull();
        } else {
            isNull = false;
            value = reinterpret_cast<VectorType *>(baseVector)->GetValue(rowIndex);
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
        } else {
            return PrefixLen + CalMetaSize();
        }
    }

    /*
     * for varchar return compaction of value.length
     * for len return compaction of value
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
        // for varchar
        if constexpr (std::is_same_v<T, std::string_view>) {
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
        } else {
            if (isNull) {
                *writeBuffer = (FIX_BIT | (0x1 << NULL_POS));
                ++writeBuffer;
                return writeBuffer;
            }
            if constexpr (std::is_same_v<T, type::Decimal128>) {
                // fix len for decimal
                *writeBuffer = (FIX_BIT | (isNull << NULL_POS) | BIT_16);
                ++writeBuffer;
                std::copy(reinterpret_cast<uint8_t *>(&value), reinterpret_cast<uint8_t *>(&value) + BIT_16,
                    writeBuffer);
                return writeBuffer + BIT_16;
            } else if constexpr (std::is_same_v<T, double>) {
                // fix len for decimal
                *writeBuffer = (FIX_BIT | (isNull << NULL_POS) | BIT_8);
                ++writeBuffer;
                std::copy(reinterpret_cast<uint8_t *>(&value), reinterpret_cast<uint8_t *>(&value) + BIT_8,
                    writeBuffer);
#ifdef DEBUG
                LogDebug("double offset is %d\n", 1 + BIT_8);
#endif
                return writeBuffer + BIT_8;
            } else if constexpr (std::is_same_v<T, bool>) {
                *writeBuffer = (FIX_BIT | (isNull << NULL_POS) | BIT_1);
                ++writeBuffer;
                std::copy(reinterpret_cast<uint8_t *>(&value), reinterpret_cast<uint8_t *>(&value) + BIT_1,
                    writeBuffer);
                return writeBuffer + BIT_1;
            } else {
                auto tmp = value;
                bool neg = false;
                if (value < 0) {
                    tmp = ~value;
                    neg = true;
                }
                uint8_t rowLenSize = (BIT_64 - __builtin_clzll(tmp) + BIT_8) / BIT_8;
                *writeBuffer = (FIX_BIT | (isNull << NULL_POS) | (neg << NEG_POS) | rowLenSize);
                ++writeBuffer;
                std::copy(reinterpret_cast<uint8_t *>(&tmp), reinterpret_cast<uint8_t *>(&tmp) + rowLenSize,
                    writeBuffer);
#ifdef DEBUG
                LogDebug("fix value offset is %d\n", 1 + rowLenSize);
#endif
                return writeBuffer + rowLenSize;
            }
        }
    }
};

template <type::DataTypeId id> void TransToValue(BaseVector *vec, int32_t rowIndex, BaseSerialize *value)
{
    using T = typename NativeType<id>::type;
    reinterpret_cast<SerializedValue<T> *>(value)->TransValue(vec, rowIndex);
}

template <type::DataTypeId id> std::unique_ptr<BaseSerialize> GenerateSerValue()
{
    using T = typename NativeType<id>::type;
    return std::make_unique<SerializedValue<T>>();
}

template <typename IntLikeType>
static inline uint8_t *FixedRowSetVecValue(uint8_t *row, Vector<IntLikeType> *vec, int32_t rowIndex)
{
    IntLikeType value = 0;
    bool negValue = false;
    if (*row & (1 << BaseSerialize::NEG_POS)) {
        negValue = true;
    }
    auto valueLen = (*row) & (0x0F);
    // point to real value
    ++row;
    std::copy(row, row + valueLen, reinterpret_cast<uint8_t *>(&value));
    if (UNLIKELY(negValue)) {
        value = ~value;
    }
    vec->SetValue(rowIndex, value);
#ifdef DEBUG
    std::string out;
    out = "fix value parse value is " + std::to_string(value) + ".";
    LogDebug("Row Handle:%s\n", out.c_str());
#endif
    return row + valueLen;
}

template <typename IntLikeType> static inline uint8_t *FixedRowGetValue(uint8_t *row)
{
    IntLikeType value = 0;
    bool negValue = false;
    if (*row & (1 << BaseSerialize::NEG_POS)) {
        negValue = true;
    }
    auto valueLen = (*row) & (0x0F);
    // point to real value
    ++row;
#ifdef DEBUG
    std::copy(row, row + valueLen, reinterpret_cast<uint8_t *>(&value));
    std::string out;
    out = "fix value parse value is " + std::to_string(negValue ? -value : value) + ".";
    LogDebug("Row Handle:%s\n", out.c_str());
#endif
    return row + valueLen;
}

static inline uint8_t *DecimalRowSetVecValue(uint8_t *row, Vector<Decimal128> *vec, int32_t rowIndex)
{
    type::Decimal128 value;
    ++row;
    std::copy(row, row + BaseSerialize::BIT_16, reinterpret_cast<uint8_t *>(&value));
    vec->SetValue(rowIndex, value);
#ifdef DEBUG
    std::string out;
    out = "Decimal value parse highBits is " + std::to_string(value.HighBits()) + ", lowBits is " +
        std::to_string(value.LowBits());
    LogDebug("Row Handle:%s\n", out.c_str());
#endif
    return row + BaseSerialize::BIT_16;
}

static inline uint8_t *DecimalRowGetValue(uint8_t *row)
{
    type::Decimal128 value;
    ++row;
#ifdef DEBUG
    std::copy(row, row + BaseSerialize::BIT_16, reinterpret_cast<uint8_t *>(&value));
    std::string out;
    out = "Decimal value parse highBits is " + std::to_string(value.HighBits()) + ", lowBits is " +
        std::to_string(value.LowBits()) + ".";
    LogDebug("Row Handle:%s\n", out.c_str());
#endif
    return row + BaseSerialize::BIT_16;
}

static inline uint8_t *BoolRowSetVecValue(uint8_t *row, Vector<bool> *vec, int32_t rowIndex)
{
    bool value;
    ++row;
    std::copy(row, row + BaseSerialize::BIT_1, reinterpret_cast<uint8_t *>(&value));
    vec->SetValue(rowIndex, value);
    return row + BaseSerialize::BIT_1;
}

static inline uint8_t *DoubleRowSetVecValue(uint8_t *row, Vector<double> *vec, int32_t rowIndex)
{
    double value;
    ++row;
    std::copy(row, row + BaseSerialize::BIT_8, reinterpret_cast<uint8_t *>(&value));
    vec->SetValue(rowIndex, value);
#ifdef DEBUG
    std::string out;
    out = "double parse value is " + std::to_string(value);
    LogDebug("Row Handle:%s\n", out.c_str());
#endif
    return row + BaseSerialize::BIT_8;
}

static inline uint8_t *DoubleRowGetValue(uint8_t *row)
{
    double value;
    ++row;
#ifdef DEBUG
    std::copy(row, row + BaseSerialize::BIT_8, reinterpret_cast<uint8_t *>(&value));
    std::string out;
    out = "double parse value is " + std::to_string(value) + ".";
    LogDebug("Row Handle:%s\n", out.c_str());
#endif
    return row + BaseSerialize::BIT_8;
}

static inline uint8_t *StringRowSetVecValue(uint8_t *row, Vector<LargeStringContainer<std::string_view>> *vec,
    int32_t rowIndex)
{
    auto valueLen = (*row) & (0b00000111);
    // point to strLen value
    ++row;
    int32_t strLen = 0;
    std::copy(row, row + valueLen, &strLen);
    // point to str
    row += valueLen;
    // row will be copied to value 's string buffer
    std::string_view value{ (char *)(row), strLen };
    vec->SetValue(rowIndex, value);
#ifdef DEBUG
    std::string out;
    out = "str parse value is " + std::string(value.data(), value.size()) + ".";
    LogDebug("Row Handle:%s\n", out.c_str());
#endif
    return row + strLen;
}

static inline uint8_t *StringRowGetValue(uint8_t *row)
{
    auto valueLen = (*row) & (0b00000111);
    // point to strLen value
    ++row;
    int32_t strLen = 0;
    std::copy(row, row + valueLen, &strLen);
    // point to str
    row += valueLen;
#ifdef DEBUG
    // row will be copied to value 's string buffer
    std::string_view value{ (char *)(row), strLen };
    std::string out;
    out = "str parse value is " + std::string(value.data(), value.size()) + ".";
    LogDebug("Row Handle:%s\n", out.c_str());
#endif
    return row + strLen;
}

template <type::DataTypeId id> uint8_t *RowToVec(uint8_t *row, BaseVector *vec, int32_t rowIndex)
{
    using Type = typename NativeType<id>::type;
    using VectorType = std::conditional_t<std::is_same_v<Type, std::string_view>,
        Vector<LargeStringContainer<std::string_view>>, Vector<Type>>;
    auto *typeVector = reinterpret_cast<VectorType *>(vec);
    if (*row & (0x1 << BaseSerialize::NULL_POS)) {
        typeVector->SetNull(rowIndex);
        return row + BaseSerialize::PrefixLen;
    } else {
        if constexpr (std::is_same_v<std::string_view, Type>) {
            return StringRowSetVecValue(row, typeVector, rowIndex);
        } else if constexpr (std::is_same_v<Type, double>) {
            return DoubleRowSetVecValue(row, typeVector, rowIndex);
        } else if constexpr (std::is_same_v<Type, Decimal128>) {
            return DecimalRowSetVecValue(row, typeVector, rowIndex);
        } else if constexpr (std::is_same_v<Type, bool>) {
            return BoolRowSetVecValue(row, typeVector, rowIndex);
        } else {
            return FixedRowSetVecValue<Type>(row, typeVector, rowIndex);
        }
    }
}

template <type::DataTypeId id> uint8_t *PrintRow(uint8_t *row)
{
    using Type = typename NativeType<id>::type;
    if (*row & (0x1 << BaseSerialize::NULL_POS)) {
#ifdef DEBUG
        std::string out = "the value is null. \n";
        LogDebug("Row Handle:%s\n", out.c_str());
#endif
    } else {
        if constexpr (std::is_same_v<std::string_view, Type>) {
            return StringRowGetValue(row);
        } else if constexpr (std::is_same_v<Type, double>) {
            return DoubleRowGetValue(row);
        } else if constexpr (std::is_same_v<Type, Decimal128>) {
            return DecimalRowGetValue(row);
        } else {
            return FixedRowGetValue<Type>(row);
        }
    }
}

#define FUNC_CENTER_DEF(CENTER_NAME, FUNC_NAME, TMP_FUNC_PTR) \
    static std::vector<FUNC_NAME> CENTER_NAME = { nullptr,    \
        TMP_FUNC_PTR<type::OMNI_INT>,                         \
        TMP_FUNC_PTR<type::OMNI_LONG>,                        \
        TMP_FUNC_PTR<type::OMNI_DOUBLE>,                      \
        TMP_FUNC_PTR<type::OMNI_BOOLEAN>,                     \
        TMP_FUNC_PTR<type::OMNI_SHORT>,                       \
        TMP_FUNC_PTR<type::OMNI_LONG>,                        \
        TMP_FUNC_PTR<type::OMNI_DECIMAL128>,                  \
        TMP_FUNC_PTR<type::OMNI_INT>,                         \
        TMP_FUNC_PTR<type::OMNI_LONG>,                        \
        TMP_FUNC_PTR<type::OMNI_INT>,                         \
        TMP_FUNC_PTR<type::OMNI_LONG>,                        \
        nullptr,                                              \
        nullptr,                                              \
        nullptr,                                              \
        TMP_FUNC_PTR<type::OMNI_VARCHAR>,                     \
        TMP_FUNC_PTR<type::OMNI_VARCHAR>,                     \
        nullptr };

using TransFuncPtr = void (*)(BaseVector *vec, int32_t rowIndex, BaseSerialize *value);
using GenFuncPtr = std::unique_ptr<BaseSerialize> (*)();
using RowToVecFuncPtr = uint8_t *(*)(uint8_t *row, BaseVector *vec, int32_t rowIndex);
using PrintRowFuncPtr = uint8_t *(*)(uint8_t *row);

FUNC_CENTER_DEF(Row2VecFuncCenter, RowToVecFuncPtr, RowToVec)
FUNC_CENTER_DEF(PrintRowFuncCenter, PrintRowFuncPtr, PrintRow)
FUNC_CENTER_DEF(GenerateValueFuncCenter, GenFuncPtr, GenerateSerValue)
FUNC_CENTER_DEF(TransFuncCenter, TransFuncPtr, TransToValue)

/*
 * RowBuffer.TransValue();
 * RowBuffer.FillBuffer();
 * auto *buffer = RowBuffer.GetRowBuffer();
 * SerializedRowBatch.SetRow(index, buffer);
 */
class RowBuffer {
public:
    explicit RowBuffer(std::vector<DataTypePtr> &types, int32_t keyColumnSize = 0)
        : columnSize(types.size()), keyColumn(keyColumnSize)
    {
        oneRow.resize(columnSize);
        transFuns.resize(columnSize);
        for (int i = 0; i < types.size(); i++) {
            ConstructFuncById(i, types[i]->GetId());
        }
    }

    RowBuffer(std::vector<type::DataTypeId> types, int32_t keyColumnSize = 0)
        : columnSize(types.size()), keyColumn(keyColumnSize)
    {
        oneRow.resize(columnSize);
        transFuns.resize(columnSize);
        for (int i = 0; i < types.size(); i++) {
            ConstructFuncById(i, types[i]);
        }
    }

    ~RowBuffer()
    {
    }

    void TransValue(BaseVector **vecs, int32_t rowIndex)
    {
        for (int32_t i = 0; i < columnSize; ++i) {
            transFuns[i](vecs[i], rowIndex, oneRow[i].get());
        }
    }

    void TransValueFromVectorBatch(vec::VectorBatch *vb, int32_t index)
    {
        for (int32_t i = 0; i < columnSize; ++i) {
            transFuns[i](vb->Get(i), index, oneRow[i].get());
        }
    }

    int32_t CalculateCompactLength(int32_t endColumn)
    {
        int32_t ret = 0;
        for (int32_t i = 0; i < endColumn; ++i) {
            auto len = oneRow[i]->CompactLength();
            ret += len;
        }
        return ret;
    }

    int32_t FillBuffer()
    {
        oneRowLen = CalculateCompactLength(columnSize);
        originBuffer = (uint8_t *)(mem::Allocator::GetAllocator()->Alloc(oneRowLen));
        writeBuffer = originBuffer;
        for (int32_t i = 0; i < columnSize; ++i) {
            writeBuffer = oneRow[i]->WriteBuffer(writeBuffer);
        }
#ifdef DEBUG
        LogDebug("originBuffer + oneRowLen - writeBuffer = %d\n", originBuffer + oneRowLen - writeBuffer);
#endif
        return oneRowLen;
    }

    int32_t CalculateHashPos()
    {
        return CalculateCompactLength(keyColumn);
    }

    uint8_t *GetRowBuffer()
    {
        return originBuffer;
    }

    uint8_t *TakeRowBuffer()
    {
        auto *retBuffer = originBuffer;
        originBuffer = nullptr;
        return retBuffer;
    }

    BaseSerialize *GetOneOfRow(int32_t index)
    {
        return oneRow[index].get();
    }

    // this api can be called after FillBuffer()
    int32_t GetCurRowLen()
    {
        return oneRowLen;
    }

private:
    void ConstructFuncById(int32_t i, DataTypeId id)
    {
        oneRow[i] = GenerateValueFuncCenter[id]();
        transFuns[i] = TransFuncCenter[id];
    }

    std::vector<std::unique_ptr<BaseSerialize>> oneRow;
    std::vector<TransFuncPtr> transFuns;
    int32_t oneRowLen;
    // it is necessaries for shuffle when calculate partition id
    // key column is 0 -> keyColumn
    int32_t keyColumn;
    uint8_t *originBuffer;
    uint8_t *writeBuffer;
    int32_t columnSize;
};

class RowParser {
public:
    RowParser(const std::vector<DataTypeId> &types)
    {
        ResizeFuncs(types.size());
        for (int i = 0; i < types.size(); ++i) {
            PushBackFunc(types[i]);
        }
    }

    RowParser(DataTypeId *types, int32_t len)
    {
        ResizeFuncs(len);
        for (int i = 0; i < len; ++i) {
            PushBackFunc(types[i]);
        }
    }

    RowParser(std::vector<DataTypePtr> &typeIds)
    {
        ResizeFuncs(typeIds.size());
        for (int i = 0; i < typeIds.size(); ++i) {
            PushBackFunc(typeIds[i]->GetId());
        }
    }

    void ParseOneRow(uint8_t *row, BaseVector **vec, int32_t rowIndex)
    {
        for (int i = 0; i < typeParser.size(); ++i) {
            auto parseFunc = typeParser[i];
            row = parseFunc(row, vec[i], rowIndex);
        }
    }

    // friendly to jni
    void ParseOnRow(uint8_t *row, long *vecs, int32_t rowIndex)
    {
        for (int i = 0; i < typeParser.size(); ++i) {
            auto parseFunc = typeParser[i];
            row = parseFunc(row, (BaseVector *)(vecs[i]), rowIndex);
        }
    }

    void PrintOneRow(uint8_t *row)
    {
        for (int i = 0; i < typeParser.size(); ++i) {
            auto printFunc = printParser[i];
            row = printFunc(row);
        }
    }

    void ReserveBuffer(int32_t bufSize)
    {
        if (capacity >= bufSize) {
            return;
        } else {
            mem::Allocator::GetAllocator()->Free(reuseBuffer, capacity);
            reuseBuffer = reinterpret_cast<uint8_t*>(mem::Allocator::GetAllocator()->Alloc(capacity));
        }
    }

    uint8_t *GetBuffer()
    {
        return reuseBuffer;
    }

    ~RowParser() = default;

private:
    void ResizeFuncs(int32_t size)
    {
        typeParser.reserve(size);
        printParser.reserve(size);
    }

    void PushBackFunc(DataTypeId id)
    {
        typeParser.push_back(Row2VecFuncCenter[id]);
        printParser.push_back(PrintRowFuncCenter[id]);
    }

    std::vector<RowToVecFuncPtr> typeParser;
    std::vector<PrintRowFuncPtr> printParser;
    uint8_t *reuseBuffer = nullptr;
    int32_t capacity;
};

class RowInfo {
public:
    uint8_t *row;
    int32_t length;

    RowInfo(uint8_t *row, int32_t length) : row(row), length(length) {}

    RowInfo() = default;

    RowInfo(const RowInfo &rowInfo)
    {
        this->row = rowInfo.row;
        this->length = rowInfo.length;
    }

    ~RowInfo()
    {
        mem::Allocator::GetAllocator()->Free(row, length);
    }
};

// friend for shuffle
class RowBatch {
public:
    RowBatch(int32_t rowCount)
    {
        rowArray.resize(rowCount);
    }

    RowBatch(int32_t rowCount, const std::vector<DataTypeId> &typeIds)
    {
        rowArray.resize(rowCount);
        this->types.reserve(typeIds.size());
        for (int i = 0; i < typeIds.size(); ++i) {
            types[i] = typeIds[i];
        }
    }

    void Resize(int32_t rowCount)
    {
        rowArray.resize(rowCount);
    }

    int32_t GetRowCount()
    {
        return rowArray.size();
    }

    void SetTypes(std::vector<DataTypeId> typeIds)
    {
        types.reserve(typeIds.size());
        for (int i = 0; i < typeIds.size(); ++i) {
            types[i] = typeIds[i];
        }
    }

    void SetRow(int32_t index, uint8_t *buffer)
    {
        rowArray[index]->row = buffer;
    }

    void SetRow(int32_t index, RowInfo *info)
    {
        rowArray[index] = info;
    }

    /*
     * get one row
     */
    RowInfo *Get(int32_t index)
    {
        return rowArray[index];
    }

    ~RowBatch()
    {
        // we cant free rowBatch now
        // for adaptor, memory in rowArray will be reuse
    }

    void PrintRowBatch(std::vector<DataTypeId> types)
    {
        auto parser = std::make_unique<RowParser>(types);
        for (int i = 0; i < rowArray.size(); ++i) {
            std::string out;
            out = "--------row info--------. \n";
            LogDebug("Row Handle:%s\n", out.c_str());
            parser->PrintOneRow(rowArray[i]->row);
        }
    }

private:
    std::vector<RowInfo *> rowArray;
    std::vector<DataTypeId> types;
};
}
}
#endif // OMNI_RUNTIME_OMNI_ROW_H
