#pragma once

/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Inner supported aggregators header
 */

#include "aggregator.h"
#include "operations_hash_aggregator.h"
#include "operations_aggregator.h"

#include <memory>
#include <cmath>

#include "type/decimal_operations.h"

namespace omniruntime {
namespace op {
template <type::DataTypeId dataTypeId> struct NativeAndVectorType {};

template <> struct NativeAndVectorType<type::DataTypeId::OMNI_BOOLEAN> {
    using type = int8_t;
    using vector = vec::BooleanVector;
};

template <> struct NativeAndVectorType<type::DataTypeId::OMNI_SHORT> {
    using type = int16_t;
    using vector = vec::ShortVector;
};

template <> struct NativeAndVectorType<type::DataTypeId::OMNI_INT> {
    using type = int32_t;
    using vector = vec::IntVector;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_DATE32> {
    using type = int32_t;
    using vector = vec::IntVector;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_TIME32> {
    using type = int32_t;
    using vector = vec::IntVector;
};

template <> struct NativeAndVectorType<type::DataTypeId::OMNI_LONG> {
    using type = int64_t;
    using vector = vec::LongVector;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_DATE64> {
    using type = int64_t;
    using vector = vec::LongVector;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_TIME64> {
    using type = int64_t;
    using vector = vec::LongVector;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_TIMESTAMP> {
    using type = int64_t;
    using vector = vec::LongVector;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_DOUBLE> {
    using type = double;
    using vector = vec::DoubleVector;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_DECIMAL64> {
    using type = int64_t;
    using vector = vec::LongVector;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_DECIMAL128> {
    using type = Decimal128;
    using vector = vec::Decimal128Vector;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_VARCHAR> {
    using type = uint8_t;
    using vector = vec::VarcharVector;
};
template <> struct NativeAndVectorType<type::DataTypeId::OMNI_CHAR> {
    using type = uint8_t;
    // Reza:: what is proper type
    using vector = vec::VarcharVector;
};

template <> struct NativeAndVectorType<type::DataTypeId::OMNI_CONTAINER> {
    using type = int64_t;
    using vector = vec::ContainerVector;
};

template <typename T>
struct AggregatorBuffer {
    AggregatorBuffer() = default;

    AggregatorBuffer(BaseAllocator *allocator, const size_t length, const bool zeroFill)
    {
        Create(allocator, length, zeroFill);
    }

    void Create(BaseAllocator *allocator, const size_t length, const bool zeroFill)
    {
        Release();
        chunk = omniruntime::mem::Chunk::NewChunk(allocator, length * sizeof(T), zeroFill);
        data = reinterpret_cast<T *>(chunk->GetAddress());
    }

    void Release()
    {
        if (chunk != nullptr) {
            delete chunk;
        }
        chunk = nullptr;
        data = nullptr;
    }

    ~AggregatorBuffer()
    {
        Release();
    }

    omniruntime::mem::Chunk *chunk = nullptr;
    T *data = nullptr;
};

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW>
class TypedMaskColAggregator;

template <bool RAW_IN, bool PARTIAL_OUT, bool NULL_OVERFLOW>
class TypedAggregator : public Aggregator {
    friend class TypedMaskColAggregator<RAW_IN, PARTIAL_OUT, NULL_OVERFLOW>;

public:
    TypedAggregator(const FunctionType aggregateType, const DataTypesPtr inputTypes,
        const DataTypesPtr outputTypes, const std::vector<int32_t> &channels)
        : Aggregator(aggregateType, inputTypes, outputTypes, channels, RAW_IN, PARTIAL_OUT, NULL_OVERFLOW)
    {
        Validate();
    }


    ~TypedAggregator() override = default;

    // for no groupby aggregation
    virtual void ProcessGroup(AggregateState &state, VectorBatch *vectorBatch,
        const int32_t rowOffset, const int32_t rowCount) override
    {
        AggregatorBuffer<int32_t> indexMap;
        uint8_t *nullMap = nullptr;
        Vector *vector = GetVector(vectorBatch, rowOffset, rowCount, &nullMap, indexMap, 0);

        if constexpr (RAW_IN) {
            ProcessRawInput(state, vector, rowOffset, rowCount, nullMap, indexMap.data);
        } else {
            ProcessPartialInput(state, vector, rowOffset, rowCount, nullMap, indexMap.data);
        }
    }

    // for groupby hash aggregation
    virtual void ProcessGroup(std::vector<AggregateState *> &rowStates, const size_t aggIdx,
        VectorBatch *vectorBatch, const int32_t rowOffset) override
    {
        AggregatorBuffer<int32_t> indexMap;
        uint8_t *nullMap = nullptr;
        Vector *vector = GetVector(vectorBatch, rowOffset, rowStates.size(), &nullMap, indexMap, 0);

        // ProcessGroupUseIndex
        if constexpr (RAW_IN) {
            ProcessGroupRawInput(rowStates, aggIdx, vector, rowOffset, nullMap, indexMap.data);
        } else {
            ProcessGroupPartialInput(rowStates, aggIdx, vector, rowOffset, nullMap, indexMap.data);
        }
    }

    bool IsTypedAggregator() override
    {
        return true;
    }

protected:
    virtual void Validate()
    {}

    virtual ALWAYS_INLINE void ProcessRawInput(
        AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap) = 0;

    virtual ALWAYS_INLINE void ProcessPartialInput(
        AggregateState &state, Vector *vector, const int32_t rowOffset, const int32_t rowCount,
        const uint8_t *nullMap, const int32_t *indexMap)
    {
        ProcessRawInput(state, vector, rowOffset, rowCount, nullMap, indexMap);
    }

    virtual ALWAYS_INLINE void ProcessGroupRawInput(
        std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset, const uint8_t *nullMap, const int32_t *indexMap) = 0;

    virtual ALWAYS_INLINE void ProcessGroupPartialInput(
        std::vector<AggregateState *> &rowStates, const size_t aggIdx, Vector *vector,
        const int32_t rowOffset,  const uint8_t *nullMap, const int32_t *indexMap)
    {
        ProcessGroupRawInput(rowStates, aggIdx, vector, rowOffset, nullMap, indexMap);
    }

    // set vector value null or throw exception when overflow
    void SetNullOrThrowException(Vector *vector, const int index, const char *errorMsg)
    {
        if (!IsOverflowAsNull()) {
            throw OmniException("OPERATOR_RUNTIME_ERROR", errorMsg);
        }
        vector->SetValueNull(index);
    }

    ALWAYS_INLINE void SetDecimal128Value(const Int128 &value, Decimal128Vector *vector, const int32_t index)
    {
        __int128 v = static_cast<__int128>(value);
        vector->SetValue(index, Decimal128(v));
    }

    virtual ALWAYS_INLINE Vector *GetVector(VectorBatch *vectorBatch, const int32_t rowOffset, const int32_t rowCount,
        uint8_t **nullMap, AggregatorBuffer<int32_t> &indexMap, const size_t channelIdx)
    {
#ifdef DEBUG
        if (channelIdx < 0 || channelIdx >= channels.size()) {
            throw OmniException("Illegal Arguement",
                "Aggregator channel index" + std::to_string(channelIdx) + " out of range [0, "
                    + std::to_string(channels.size()) + ") for " + std::to_string(as_integer(type)));
        }
#endif

        auto channel = channels[channelIdx];
#ifdef DEBUG
        if (channel < 0 || channel >= vectorBatch->GetVectorCount()) {
            throw OmniException("Illegal Arguement",
                "Aggregator channel " + std::to_string(channel) + " out of range [0, "
                    + std::to_string(vectorBatch->GetVectorCount()) + ") for " + std::to_string(as_integer(type)));
        }
#endif

        auto vector = vectorBatch->GetVector(channel);
        *nullMap = vector->MayHaveNull() ? reinterpret_cast<uint8_t *>(vector->GetValueNulls()) : nullptr;
        if (*nullMap != nullptr) {
            *nullMap += vector->GetPositionOffset() + rowOffset;
        }

        if (vector->GetEncoding() == OMNI_VEC_ENCODING_DICTIONARY) {
            indexMap.Create(this->allocator, rowCount, false);
            return static_cast<DictionaryVector *>(vector)->ExtractDictionaryAndIds(rowOffset, rowCount, indexMap.data);
        } else {
            indexMap.Release();
            return vector;
        }
    }

    // template<DataTypeId IN_ID, DataTypeId OUT_ID, typename OutType = typename NativeAndVectorType<OUT_ID>::type, typename InType = typename NativeAndVectorType<IN_ID>::type>
    template<typename InType, typename OutType>
    OutType CastWithOverflow(const InType val, bool &overflow)
    {
        if (overflow) {
            return OutType {};
        }

        if constexpr (std::is_same_v<InType, Decimal128>) {
            return CastWithOverflowDecimalInput<OutType>(val, overflow);
        } else if constexpr (std::is_same_v<OutType, Decimal128>) {
            return CastWithOverflowDecimalOutput<InType>(val, overflow);
        } else {
            OutType res = static_cast<OutType>(val);
            if constexpr (std::is_floating_point_v<InType> || std::is_floating_point_v<OutType>) {
                return res;
            } else {
                auto inputType = inputTypes->GetType(0);
                auto outputType = outputTypes->GetType(0);
                int32_t scale = 0;
                if (inputType->GetId() == OMNI_DECIMAL64 && outputType->GetId() == OMNI_DECIMAL64) {
                    int32_t scale = static_cast<DecimalDataType *>(outputType.get())->GetScale()
                        - static_cast<DecimalDataType *>(inputType.get())->GetScale();
                }


                if (scale > 0) {
                    OutType scaleFactor = static_cast<OutType>(pow(10, abs(scale)));
                    res *= scaleFactor;
                    overflow = static_cast<InType>(res / scaleFactor) != val;
                } else if (scale < 0) {
                    OutType scaleFactor = static_cast<OutType>(pow(10, abs(scale)));
                    res /= scaleFactor;
                    overflow = static_cast<InType>(res) != (val / static_cast<InType>(scaleFactor));
                } else {
                    overflow = static_cast<InType>(res) != val;
                }
                return res;
            }
        }
    }

    BaseAllocator *allocator = BaseAllocator::GetRootAllocator()->NewChildAllocator("aggregator");

private:
    template<typename OutType>
    OutType CastWithOverflowDecimalInput(const Decimal128 val, bool &overflow)
    {
        if constexpr (std::is_same_v<OutType, Decimal128>) {
            return this->CastWithOverflowDecimaToDecimal(val, overflow);
        } else if constexpr (std::is_same_v<OutType, int64_t>) {
            return this->CastWithOverflowDecimalToLong(val, overflow);
        } else if constexpr (std::is_floating_point_v<OutType>) {
            return this->CastWithOverflowDecimalToFloatingPoint<OutType>(val, overflow);
        } else {
            auto inputType = inputTypes->GetType(0);
            OutType result {};
            Decimal128Wrapper val128(val);

            // so we shoud add decimal part separately if input is acutally decimal not varchar
            if (inputType->GetId() == OMNI_DECIMAL128) {
                int32_t scale = -static_cast<DecimalDataType *>(inputType.get())->GetScale();
                val128.ReScale(scale);
                if(val128.IsOverflow() != OpStatus::SUCCESS){
                    overflow = true;
                    return result;
                }
            }

            int64_t result64;
            try {
                result64 = static_cast<int64_t>(val128);
            } catch (std::overflow_error &e) {
                overflow = true;
                return result;
            }

            result = static_cast<OutType>(result64);
            overflow = (static_cast<int64_t>(result) != result64);
            return result;
        }
    }

    template<typename InType>
    Decimal128 CastWithOverflowDecimalOutput(const InType val, bool &overflow)
    {
        if constexpr (std::is_same_v<InType, Decimal128>) {
            throw OmniException("Invalid Arguement", "Unexpected decimal input for 'CastWithOverflowDecimalOutput'");
        } else if constexpr (std::is_same_v<InType, int64_t>) {
            return this->CastWithOverflowLongToDecimal(val, overflow);
        } else if constexpr (std::is_floating_point_v<InType>) {
            return this->CastWithOverflowFloatingPointDecimal<InType>(val, overflow);
        } else {
            Decimal128Wrapper result(static_cast<int64_t>(val));
            auto outputType = outputTypes->GetType(0);
            if (outputType->GetId() == OMNI_DECIMAL128) {
                int32_t scale = static_cast<DecimalDataType *>(outputType.get())->GetScale();
                result.ReScale(scale);
                if (result.IsOverflow() != OpStatus::SUCCESS) {
                    overflow = true;
                }
            }

            return result.ToDecimal128();
        }
    }

    Decimal128 CastWithOverflowDecimaToDecimal(const Decimal128 val, bool &overflow)
    {
        auto inputType = inputTypes->GetType(0);
        auto outputType = outputTypes->GetType(0);
        Decimal128Wrapper result(val);
        // following if condition makes sure that both input and output are acutally decimal,
        // not varchare (which has no scale)
        if ((inputType->GetId() == OMNI_DECIMAL64 || inputType->GetId() == OMNI_DECIMAL128) 
            && (outputType->GetId() == OMNI_DECIMAL64 || outputType->GetId() == OMNI_DECIMAL128)) {
            int32_t scale = static_cast<DecimalDataType *>(outputType.get())->GetScale()
                - static_cast<DecimalDataType *>(inputType.get())->GetScale();
            if (scale != 0) {
                overflow = static_cast<bool>(result.ReScale(scale).IsOverflow());
            }
        }

        return result.ToDecimal128();
    }

    int64_t CastWithOverflowDecimalToLong(const Decimal128 val, bool &overflow)
    {
        auto inputType = inputTypes->GetType(0);
        auto outputType = outputTypes->GetType(0);
        int64_t result {};
        Decimal128Wrapper resultDec(val);
        int32_t scale = 0;
        if (outputType->GetId() == OMNI_DECIMAL64) {
            // following if condition makes sure that input is acutally decimal,
            // not varchare (which has no scale)
            if (inputType->GetId() == OMNI_DECIMAL128 || inputType->GetId() == OMNI_DECIMAL64) {
                scale = static_cast<DecimalDataType *>(outputType.get())->GetScale()
                    - static_cast<DecimalDataType *>(inputType.get())->GetScale();
            }
        } else {
            // regular long output. so we shoud remove decimal part if input is acutally decimal not varchar
            if (inputType->GetId() == OMNI_DECIMAL128 || inputType->GetId() == OMNI_DECIMAL64) {
                scale = -static_cast<DecimalDataType *>(inputType.get())->GetScale();
            }
        }

        if (scale != 0) {
            resultDec.ReScale(scale);
            if (resultDec.IsOverflow() != OpStatus::SUCCESS) {
                overflow = true;
                return result;
            }
        } else {
            resultDec = val;
        }
        try {
            result = static_cast<int64_t>(resultDec.SetScale(0));
        } catch (std::overflow_error &e) {
            overflow = true;
        }
        return result;
    }

    template <typename OutType>
    OutType CastWithOverflowDecimalToFloatingPoint(const Decimal128 val, bool &overflow)
    {
        auto inputType = inputTypes->GetType(0);
        OutType result {};
        // so we shoud add decimal part separately if input is acutally decimal not varchar
        int32_t scale = (inputType->GetId() == OMNI_DECIMAL128 || inputType->GetId() == OMNI_DECIMAL64)
            ? static_cast<DecimalDataType *>(inputType.get())->GetScale()
            : 0;

        Decimal128Wrapper integralDec(val);
        Decimal128Wrapper fractionalDec(0);
        if (scale != 0) {
            Decimal128Wrapper scaleValue(TenOfScaleMultipliers[abs(scale)]);
            integralDec = integralDec.Divide(scaleValue, 0);
            fractionalDec = integralDec.Mod(scaleValue);
            if (integralDec.IsOverflow() != OpStatus::SUCCESS) {
                overflow = true;
                return result;
            }
        }
        int64_t result64;
        try {
            result = static_cast<int64_t>(integralDec);
        } catch (std::overflow_error &e) {
            overflow = true;
            return result;
        }

        result = static_cast<OutType>(result64);

        if (scale != 0) {
            try {
                result = static_cast<int64_t>(fractionalDec);
            } catch (std::overflow_error &e) {
                overflow = true;
                return result;
            }
            result += (static_cast<OutType>(result64) / pow(10.0, scale));
        }
        return result;
    }

    Decimal128 CastWithOverflowLongToDecimal(const int64_t val, bool &overflow)
    {
        auto inputType = inputTypes->GetType(0);
        auto outputType = outputTypes->GetType(0);
        Decimal128Wrapper result(val);
        int32_t scale = 0;
        if (inputType->GetId() == OMNI_DECIMAL64) {
            // following if condition makes sure that output is acutally decimal,
            // not varchare (which has no scale)
            if (outputType->GetId() == OMNI_DECIMAL128 || outputType->GetId() == OMNI_DECIMAL64) {
                scale = static_cast<DecimalDataType *>(outputType.get())->GetScale()
                    - static_cast<DecimalDataType *>(inputType.get())->GetScale();
            }
        } else {
            // regular long output. so we shoud remove decimal part if input is acutally decimal not varchar
            if (outputType->GetId() == OMNI_DECIMAL128 || outputType->GetId() == OMNI_DECIMAL64) {
                scale = static_cast<DecimalDataType *>(outputType.get())->GetScale();
            }
        }

        if (scale != 0) {
            result.ReScale(scale);
            if (result.IsOverflow() != OpStatus::SUCCESS) {
                overflow = true;
            }
        }

        return result.ToDecimal128();
    }

    template <typename InType>
    Decimal128 CastWithOverflowFloatingPointDecimal(const InType val, bool &overflow)
    {
        InType integral;
        InType fractional = std::modf(val, &integral);
        Decimal128Wrapper result(static_cast<int64_t>(integral));

        auto outputType = outputTypes->GetType(0);
        // so we shoud add decimal part separately if input is acutally decimal not varchar
        int32_t scale = (outputType->GetId() == OMNI_DECIMAL128 || outputType->GetId() == OMNI_DECIMAL64)
            ? static_cast<DecimalDataType *>(outputType.get())->GetScale()
            : 0;
        if (scale > 0) {
            result.ReScale(scale);
            if (result.IsOverflow() != OpStatus::SUCCESS) {
                overflow = true;
                return result.ToDecimal128();
            }

            fractional *= pow(10.0, scale);
            Decimal128Wrapper fractionalDec(static_cast<int64_t>(fractional));
            result=result.Add(fractionalDec);
            if (result.IsOverflow() != OpStatus::SUCCESS) {
                overflow = true;
            }
        }

        return result.ToDecimal128();
    }
};
} // end of namespace op
} // end of namespace omniruntime
