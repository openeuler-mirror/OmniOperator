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
    static constexpr int32_t BIT_16 = 16;
    static constexpr int8_t VARCHAR_BIT = 1 << 7;
    static constexpr int8_t FIX_BIT = 0;
    static constexpr int8_t NULL_POS = 6;
    static constexpr int8_t NEG_POS = 5;

    virtual void TransValue(BaseVector *baseVector, int32_t rowIndex) {
        throw exception::OmniException("BaseSerialize", "not implement");
    }

    virtual int32_t CompactLength() {
        throw exception::OmniException("BaseSerialize", "not implement");
    }

    virtual uint8_t *WriteBuffer(uint8_t *writeBuffer) {
        throw exception::OmniException("BaseSerialize", "WriteBuffer not implement");
    }

    virtual void Print() {
        throw exception::OmniException("BaseSerialize", "Print not implement");
    }

    void Reset() {
        isNull = false;
    }

    void SetNull() {
        isNull = true;
    }
};

template<typename T>
class SerializedValue : public BaseSerialize {
private:
    T value;
    using VectorType = std::conditional_t<std::is_same_v<T,std::string_view >,
            Vector<LargeStringContainer<std::string_view>>, Vector<T>>;
public:
    // only 1 Bytes represent following coding :
    // 'isVarchar(1bit 1) + isNull(1bit) + sizeLength(3bit)  + pad (3bit)'
    // or 'isVarchar(1bit 0) + isNull(1bit) + sizeLength(3bit) '


    // set value from vector row
    void TransValue(BaseVector *baseVector, int32_t rowIndex) {
        if (baseVector->IsNull(rowIndex)) {
            SetNull();
        } else {
            isNull = false;
            value = reinterpret_cast<VectorType *>(baseVector)->GetValue(rowIndex);
        }
    }

    // set value from row
    void SetValue(T setValue) {
        isNull = false;
        value = setValue;
    }

    /**
     * length is final length
     * @return
     */
    int32_t CompactLength() {
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
    uint8_t CalMetaSize() {
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
            if(value < 0) {
                tmp = ~value;
            }
            return (BIT_64 - __builtin_clzll(tmp) + BIT_8) / BIT_8;
        }
    }

    /**
     *
     * @param writeBuffer : start point
     * @return writeBuffer + CompactLength()
     */
    uint8_t *WriteBuffer(uint8_t *writeBuffer) override {
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
            memcpy(writeBuffer, &len, rowLenSize);
            writeBuffer += rowLenSize;
            memcpy(writeBuffer, value.data(), value.size());
            LogDebug("str offset is %d\n", 1 + rowLenSize + value.size());
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
                memcpy(writeBuffer, &value, BIT_16);
                return writeBuffer + BIT_16;
            } else if constexpr (std::is_same_v<T, double>) {
                // fix len for decimal
                *writeBuffer = (FIX_BIT | (isNull << NULL_POS) | BIT_8);
                ++writeBuffer;
                memcpy(writeBuffer, &value, BIT_8);
                LogDebug("double offset is %d\n", 1 + BIT_8);
                return writeBuffer + BIT_8;
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
                memcpy(writeBuffer, &tmp, rowLenSize);
                LogDebug("fix value offset is %d\n", 1 + rowLenSize);
                return writeBuffer + rowLenSize;
            }
        }
    }
};

template<type::DataTypeId id>
void TransToValue(BaseVector *vec, int32_t rowIndex, BaseSerialize *value) {
    using T = typename NativeType<id>::type;
    reinterpret_cast<SerializedValue<T> *>(value)->TransValue(vec, rowIndex);
}

template<type::DataTypeId id>
BaseSerialize *GenerateSerValue() {
    using T = typename NativeType<id>::type;
    return new SerializedValue<T>();
}

template<typename IntLikeType>
static inline uint8_t* FixedRowSetVecValue(uint8_t *row, Vector<IntLikeType> *vec, int32_t rowIndex) {
    IntLikeType value = 0;
    bool negValue = false;
    if(*row & (1<<BaseSerialize::NEG_POS)) {
        negValue = true;
    }
    auto valueLen = (*row) & (0x0F);
    // point to real value
    ++row;
    memcpy(&value, row, valueLen);
    if(UNLIKELY(negValue)) {
        vec->SetValue(rowIndex, -value);
    } else {
        vec->SetValue(rowIndex, value);
    }
    std::cout<<"fix value parse value is "<<(negValue?-value:value)<<", offset is "<< 1+ valueLen<<std::endl;
    return row + valueLen;
}

static inline uint8_t* DecimalRowSetVecValue(uint8_t *row, Vector<Decimal128> *vec, int32_t rowIndex) {
    type::Decimal128 value;
    ++row;
    memcpy(&value, row, BaseSerialize::BIT_16);
    vec->SetValue(rowIndex, value);
    return row + BaseSerialize::BIT_16;
}

static inline uint8_t* DoubleRowSetVecValue(uint8_t *row, Vector<double> *vec, int32_t rowIndex) {
    double value;
    ++row;
    memcpy(&value, row, BaseSerialize::BIT_8);
    vec->SetValue(rowIndex, value);
    std::cout<<"double parse value is "<<value<<", offset is 9"<<std::endl;
    return row + BaseSerialize::BIT_8;
}

static inline uint8_t* StringRowSetVecValue(uint8_t *row, Vector<LargeStringContainer<std::string_view>> *vec, int32_t rowIndex) {
    auto valueLen = (*row) & (0b00000111);
    // point to strLen value
    ++row;
    int32_t strLen = 0;
    memcpy(&strLen, row, valueLen);
    // point to str
    row += valueLen;
    // row will be copied to value 's string buffer
    std::string_view value{(char*)(row), strLen};
    vec->SetValue(rowIndex, value);
    std::cout<<"str parse len is "<<strLen<<", offset is "<< 1 + valueLen + strLen<<std::endl;
    return row + strLen;
}

template<type::DataTypeId id>
uint8_t *RowToVec(uint8_t *row, BaseVector *vec, int32_t rowIndex) {
    using Type = typename NativeType<id>::type;
    using VectorType = std::conditional_t<std::is_same_v<Type,std::string_view >,
            Vector<LargeStringContainer<std::string_view>>,
            Vector<Type>>;
    auto *typeVector = reinterpret_cast<VectorType *>(vec);
    if(*row & (0x1 << BaseSerialize::NULL_POS)) {
        typeVector->SetNull(rowIndex);
    } else {
        if constexpr (std::is_same_v<std::string_view, Type>) {
            return StringRowSetVecValue(row, typeVector, rowIndex);
        } else if constexpr (std::is_same_v<Type, double>) {
            return DoubleRowSetVecValue(row, typeVector, rowIndex);
        } else if constexpr (std::is_same_v<Type, Decimal128>){
            return DecimalRowSetVecValue(row, typeVector, rowIndex);
        } else {
            return FixedRowSetVecValue(row, typeVector, rowIndex);
        }
    }
}

using TransFuncPtr = void (*)(BaseVector *vec, int32_t rowIndex, BaseSerialize *value);
using GenFuncPtr = BaseSerialize *(*)();
using RowToVecFuncPtr = uint8_t* (*)(uint8_t *row, BaseVector *vec, int32_t rowIndex);

static std::vector<RowToVecFuncPtr> Row2VecFuncCenter = {
        nullptr,                                        // OMNI_NONE,
        RowToVec<type::OMNI_INT>,        // OMNI_INT
        RowToVec<type::OMNI_LONG>,       // OMNI_LONG
        RowToVec<type::OMNI_DOUBLE>,     // OMNI_DOUBLE
        RowToVec<type::OMNI_BOOLEAN>,    // OMNI_BOOLEAN
        RowToVec<type::OMNI_SHORT>,      // OMNI_SHORT
        RowToVec<type::OMNI_LONG>,       // OMNI_DECIMAL64,
        RowToVec<type::OMNI_DECIMAL128>, // OMNI_DECIMAL128
        RowToVec<type::OMNI_INT>,        // OMNI_DATE32
        RowToVec<type::OMNI_LONG>,       // OMNI_DATE64
        RowToVec<type::OMNI_INT>,        // OMNI_TIME32
        RowToVec<type::OMNI_LONG>,       // OMNI_TIME64
        nullptr,                                        // OMNI_TIMESTAMP
        nullptr,                                        // OMNI_INTERVAL_MONTHS
        nullptr,                                        // OMNI_INTERVAL_DAY_TIME
        RowToVec<type::OMNI_VARCHAR>,    // OMNI_VARCHAR
        RowToVec<type::OMNI_VARCHAR>,    // OMNI_CHAR,
        nullptr                                         // OMNI_CONTAINER,
};


static std::vector<GenFuncPtr> GenerateValueFuncCenter = {
        nullptr,                                        // OMNI_NONE,
        GenerateSerValue<type::OMNI_INT>,        // OMNI_INT
        GenerateSerValue<type::OMNI_LONG>,       // OMNI_LONG
        GenerateSerValue<type::OMNI_DOUBLE>,     // OMNI_DOUBLE
        GenerateSerValue<type::OMNI_BOOLEAN>,    // OMNI_BOOLEAN
        GenerateSerValue<type::OMNI_SHORT>,      // OMNI_SHORT
        GenerateSerValue<type::OMNI_LONG>,       // OMNI_DECIMAL64,
        GenerateSerValue<type::OMNI_DECIMAL128>, // OMNI_DECIMAL128
        GenerateSerValue<type::OMNI_INT>,        // OMNI_DATE32
        GenerateSerValue<type::OMNI_LONG>,       // OMNI_DATE64
        GenerateSerValue<type::OMNI_INT>,        // OMNI_TIME32
        GenerateSerValue<type::OMNI_LONG>,       // OMNI_TIME64
        nullptr,                                        // OMNI_TIMESTAMP
        nullptr,                                        // OMNI_INTERVAL_MONTHS
        nullptr,                                        // OMNI_INTERVAL_DAY_TIME
        GenerateSerValue<type::OMNI_VARCHAR>,    // OMNI_VARCHAR
        GenerateSerValue<type::OMNI_VARCHAR>,    // OMNI_CHAR,
        nullptr                                         // OMNI_CONTAINER,
};

static std::vector<TransFuncPtr> TransFuncCenter = {
        nullptr,                                        // OMNI_NONE,
        TransToValue<type::OMNI_INT>,        // OMNI_INT
        TransToValue<type::OMNI_LONG>,       // OMNI_LONG
        TransToValue<type::OMNI_DOUBLE>,     // OMNI_DOUBLE
        TransToValue<type::OMNI_BOOLEAN>,    // OMNI_BOOLEAN
        TransToValue<type::OMNI_SHORT>,      // OMNI_SHORT
        TransToValue<type::OMNI_LONG>,       // OMNI_DECIMAL64,
        TransToValue<type::OMNI_DECIMAL128>, // OMNI_DECIMAL128
        TransToValue<type::OMNI_INT>,        // OMNI_DATE32
        TransToValue<type::OMNI_LONG>,       // OMNI_DATE64
        TransToValue<type::OMNI_INT>,        // OMNI_TIME32
        TransToValue<type::OMNI_LONG>,       // OMNI_TIME64
        nullptr,                                        // OMNI_TIMESTAMP
        nullptr,                                        // OMNI_INTERVAL_MONTHS
        nullptr,                                        // OMNI_INTERVAL_DAY_TIME
        TransToValue<type::OMNI_VARCHAR>,    // OMNI_VARCHAR
        TransToValue<type::OMNI_VARCHAR>,    // OMNI_CHAR,
        nullptr                                         // OMNI_CONTAINER,
};

/*
 * RowBuffer.TransValue();
 * RowBuffer.FillBuffer();
 * auto *buffer = RowBuffer.GetRowBuffer();
 * SerializedRowBatch.SetRow(index, buffer);
 */
class RowBuffer {
public:
    explicit RowBuffer(std::vector<DataTypePtr> &types, int32_t keyColumnSize = 0) :
    columnSize(types.size()),
    keyColumn(keyColumnSize) {
        uint8_t *buffer;
        mem::GetMemoryPool()->Allocate(sizeof(intptr_t) * columnSize, &buffer);
        oneRow = reinterpret_cast<BaseSerialize **>(buffer);
        transFuns.reserve(columnSize);
        for (int i = 0; i < types.size(); i++) {
            ConstructFuncById(i, types[i]->GetId());
        }
    }

    RowBuffer(std::vector<type::DataTypeId> types, int32_t keyColumnSize = 0):
    columnSize(types.size()),
    keyColumn(keyColumnSize) {
        uint8_t *buffer;
        mem::GetMemoryPool()->Allocate(sizeof(intptr_t) * columnSize, &buffer);
        oneRow = reinterpret_cast<BaseSerialize **>(buffer);
        for (int i = 0; i < types.size(); i++) {
            ConstructFuncById(i, types[i]);
        }
    }

    ~RowBuffer() {
        for(int i=0;i<columnSize;++i) {
            delete oneRow[i];
        }
        mem::GetMemoryPool()->Release((uint8_t*)(oneRow));
    }

    void TransValue(BaseVector **vecs, int32_t rowIndex) {
        for (int32_t i = 0; i < columnSize; ++i) {
            transFuns[i](vecs[i], rowIndex, oneRow[i]);
        }
    }

    void TransValueFromVectorBatch(vec::VectorBatch *vb, int32_t index) {
        for (int32_t i = 0; i < columnSize; ++i) {
            transFuns[i](vb->Get(i), index, oneRow[i]);
        }
    }

    int32_t CalculateCompactLength(int32_t endColumn) {
        int32_t ret = 0;
        for (int32_t i = 0; i < endColumn; ++i) {
            auto len = oneRow[i]->CompactLength();
            ret += len;
        }
        std::cout<<"total len is "<<ret<<std::endl;
        return ret;
    }

    int32_t FillBuffer() {
        oneRowLen = CalculateCompactLength(columnSize);
        mem::GetMemoryPool()->Allocate(oneRowLen, &originBuffer);
        writeBuffer = originBuffer;
        for (int32_t i = 0; i < columnSize; ++i) {
            writeBuffer = oneRow[i]->WriteBuffer(writeBuffer);
        }
        LogDebug("originBuffer + oneRowLen - writeBuffer = %d\n", originBuffer + oneRowLen - writeBuffer);
        return oneRowLen;
    }

    int32_t CalculateHashPos() {
        return CalculateCompactLength(keyColumn);
    }

    uint8_t* GetRowBuffer() {
        return originBuffer;
    }

    uint8_t*  TakeRowBuffer() {
        auto *retBuffer = originBuffer;
        originBuffer = nullptr;
        return retBuffer;
    }

    BaseSerialize *GetOneOfRow(int32_t index){
        return oneRow[index];
    }

    // this api can be called after FillBuffer()
    int32_t GetCurRowLen() {
        return oneRowLen;
    }

    /**
     * just for debug
     */
    void Print() {

    }

private:
    void ConstructFuncById(int32_t i, DataTypeId id) {
        oneRow[i] = GenerateValueFuncCenter[id]();
        transFuns[i] = TransFuncCenter[id];
    }

    BaseSerialize **oneRow;
    std::vector<TransFuncPtr> transFuns;
    int32_t oneRowLen;
    // it is necessaries for shuffle when calculate partition id
    // key column is 0 -> keyColumn
    int32_t keyColumn;
    uint8_t *originBuffer;
    uint8_t *writeBuffer;
    int32_t columnSize;
};

struct RowInfo {
    uint8_t *row;
    int32_t hashPos;
    int32_t length;

    RowInfo(uint8_t *row, int32_t hashPos, int32_t length) : row(row), hashPos(hashPos), length(length) {}

    RowInfo() = default;

    RowInfo(const RowInfo &rowInfo) {
        this->row = rowInfo.row;
        this->hashPos = rowInfo.hashPos;
        this->length = rowInfo.length;
    }

    ~RowInfo() {
        mem::GetMemoryPool()->Release(row);
    }
};

// friend for shuffle
class RowBatch {
public:
    RowBatch(int32_t rowCount) {
        rowArray.resize(rowCount);
    }

    void Resize(int32_t rowCount) {
        rowArray.resize(rowCount);
    }

    int32_t GetRowCount() {
        return rowArray.size();
    }


    void SetRow(int32_t index, uint8_t *buffer) {
        rowArray[index].row = buffer;
    }

    void SetRow(int32_t index, const RowInfo &info) {
        rowArray[index] = info;
    }

    void SetHashPos(int32_t index, int32_t pos) {
        rowArray[index].hashPos = pos;
    }

    void FreeAllRow() {
        for(auto &row:rowArray) {
            mem::GetMemoryPool()->Release(row.row);
            row.row = nullptr;
        }
    }
    /*
     * get one row
     */
    RowInfo Get(int32_t index) {
        return rowArray[index];
    }

    ~RowBatch() {
        // we cant free rowBatch now
        // for adaptor , memory in rowArray will be reuse
    }

private:
    std::vector<RowInfo> rowArray;
};

class RowParser {
public:
    RowParser(std::vector<DataTypeId> types) {
        typeParser.reserve(types.size());
        for(auto type:types) {
            typeParser.push_back(Row2VecFuncCenter[type]);
        }
    }

    RowParser(DataTypeId* types, int32_t len) {
        typeParser.reserve(len);
        for(int i=0; i < len; ++i) {
            typeParser.push_back(Row2VecFuncCenter[types[i]]);
        }
    }

    RowParser(std::vector<DataTypePtr> &typeIds) {
        typeParser.reserve(typeIds.size());
        for(int i=0; i < typeIds.size(); ++i) {
            typeParser.push_back(Row2VecFuncCenter[typeIds[i]->GetId()]);
        }
    }

    void ParseOneRow(uint8_t *row, BaseVector** vec, int32_t rowIndex) {
        for (int i = 0; i< typeParser.size(); ++i) {
            auto parseFunc = typeParser[i];
            row = parseFunc(row, vec[i], rowIndex);
        }
    }

    // friendly to jni
    void ParseOnRow(uint8_t *row, long* vecs, int32_t rowIndex) {
        for (int i = 0; i< typeParser.size(); ++i) {
            auto parseFunc = typeParser[i];
            parseFunc(row, (BaseVector*)(vecs[i]), rowIndex);
        }
    }

    void ReserveBuffer(int32_t bufSize) {
        if(capacity >= bufSize) {
            return ;
        } else {
            mem::GetMemoryPool()->Release(reuseBuffer);
            mem::GetMemoryPool()->Allocate(bufSize, &reuseBuffer);
        }
    }

    uint8_t *GetBuffer() {
        return reuseBuffer;
    }

    ~RowParser() = default;

private:
    std::vector<RowToVecFuncPtr> typeParser;
    uint8_t *reuseBuffer = nullptr;
    int32_t capacity;
};


}
}
#endif //OMNI_RUNTIME_OMNI_ROW_H
