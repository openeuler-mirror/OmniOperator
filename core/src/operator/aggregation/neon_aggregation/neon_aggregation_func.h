/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: neon aggregation func
 */
#ifndef OMNI_RUNTIME_NEON_AGGREGATION_FUNC_H
#define OMNI_RUNTIME_NEON_AGGREGATION_FUNC_H

#include <algorithm>
#include "arm_neon.h"
#include "type/decimal128.h"

namespace omniruntime {
namespace simd {
enum BasicOp : int16_t {
    Sum,
    Max,
    Min,
};

template <typename IN, typename OUT> struct BinaryFunc {
    static OUT SumFunc(const IN *rawValue, uint32_t size)
    {
        OUT value = 0;
        for (int i = 0; i < size; ++i) {
            value += rawValue[i];
        }
        return value;
    }

    static OUT MaxFunc(const IN *rawValue, uint32_t size)
    {
        IN value = std::numeric_limits<IN>::lowest();
        for (int i = 0; i < size; ++i) {
            value = std::max(rawValue[i], value);
        }
        return value;
    }

    static OUT MinFunc(const IN *rawValue, uint32_t size)
    {
        IN value = std::numeric_limits<IN>::max();
        for (int i = 0; i < size; ++i) {
            value = std::min(rawValue[i], value);
        }
        return value;
    }
};

template <BasicOp op, typename IN, typename OUT> struct BinaryOperation;

template <typename IN, typename OUT> struct BinaryOperation<BasicOp::Sum, IN, OUT> {
    static OUT ArrayHandleFunc(const IN *rawValue, uint32_t size)
    {
        return BinaryFunc<IN, OUT>::SumFunc(rawValue, size);
    }

    static OUT BasicHandleFunc(OUT value1, OUT value2)
    {
        return value1 + value2;
    }

    static constexpr OUT InitValue()
    {
        if constexpr (std::is_floating_point_v<OUT>) {
            return 0.0f;
        } else if constexpr (std::is_integral_v<OUT>) {
            return 0;
        } else {
            throw std::out_of_range("unsupported type");
        }
    }
};

template <typename IN, typename OUT> struct BinaryOperation<BasicOp::Max, IN, OUT> {
    static OUT ArrayHandleFunc(const IN *rawValue, uint32_t size)
    {
        return BinaryFunc<IN, OUT>::MaxFunc(rawValue, size);
    }

    static OUT BasicHandleFunc(OUT value1, OUT value2)
    {
        return std::max(value1, value2);
    }

    static OUT InitValue()
    {
        return std::numeric_limits<OUT>::lowest();
    }
};

template <typename IN, typename OUT> struct BinaryOperation<BasicOp::Min, IN, OUT> {
    static OUT ArrayHandleFunc(const IN *rawValue, uint32_t size)
    {
        return BinaryFunc<IN, OUT>::MinFunc(rawValue, size);
    }

    static OUT BasicHandleFunc(OUT value1, OUT value2)
    {
        return std::min(value1, value2);
    }

    static OUT InitValue()
    {
        return std::numeric_limits<OUT>::max();
    }
};

#define BasicDefineForSimd                                   \
    static constexpr uint32_t RawSize = sizeof(RawType) * 8; \
    static constexpr uint32_t HandleNumOnce = NeonBitWidth / RawSize;

static constexpr uint32_t NeonBitWidth = 128;

template <typename T> struct NeonSimd {
    using RawType = T;
    static constexpr uint32_t RawSize = sizeof(T);
    static constexpr uint32_t HandleNumOnce = NeonBitWidth / sizeof(T);
};

template <typename T> using NeonSimdType = typename NeonSimd<T>::BasicType;

template <typename IN, typename OUT> static auto LoadDifferentType(const IN *rawValuePtr)
{
    // use output load function
    OUT outs[NeonSimd<OUT>::HandleNumOnce];
    for (int i = 0; i < NeonSimd<OUT>::HandleNumOnce; ++i) {
        outs[i] = rawValuePtr[i];
    }
    return NeonSimd<OUT>::LoadFunc(outs);
}

template <> struct NeonSimd<int8_t> {
    using RawType = int8_t;
    using SimdType = int8x16_t;
    BasicDefineForSimd

    static auto AddFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vaddq_s8(neonLeft, neonRight);
    }

    static auto MaxFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vmaxq_s8(neonLeft, neonRight);
    }

    static auto MinFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vminq_s8(neonLeft, neonRight);
    }

    static auto InitFunc(const RawType initValue)
    {
        return vdupq_n_s8(initValue);
    }

    template <typename OUT> static auto LoadFunc(const RawType *rawValuePtr)
    {
        // value ptr is same with output type
        if constexpr (std::is_same_v<RawType, OUT>) {
            return vld1q_s8(rawValuePtr);
        } else {
            // use output load function
            return LoadDifferentType<RawType, OUT>(rawValuePtr);
        }
    }

    template <BasicOp op> static RawType BasicConvert(const SimdType neonValue)
    {
        int8_t value[HandleNumOnce];
        vst1q_s8(value, neonValue);
        return BinaryOperation<op, RawType, RawType>::ArrayHandleFunc(value, HandleNumOnce);
    }
};

template <> struct NeonSimd<int16_t> {
    using RawType = int16_t;
    using SimdType = int16x8_t;
    BasicDefineForSimd

    static auto AddFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vaddq_s16(neonLeft, neonRight);
    }

    static auto MaxFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vmaxq_s16(neonLeft, neonRight);
    }

    static auto MinFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vminq_s16(neonLeft, neonRight);
    }

    static auto InitFunc(const RawType initValue)
    {
        return vdupq_n_s16(initValue);
    }

    template <typename OUT> static auto LoadFunc(const RawType *rawValuePtr)
    {
        // value ptr is same with output type
        if constexpr (std::is_same_v<RawType, OUT>) {
            return vld1q_s16(rawValuePtr);
        } else {
            // use output load function
            return LoadDifferentType<RawType, OUT>(rawValuePtr);
        }
    }

    template <BasicOp op> static RawType BasicConvert(const SimdType neonValue)
    {
        int16_t value[HandleNumOnce];
        vst1q_s16(value, neonValue);
        return BinaryOperation<op, RawType, RawType>::ArrayHandleFunc(value, HandleNumOnce);
    }
};


template <> struct NeonSimd<int32_t> {
    using RawType = int32_t;
    using SimdType = int32x4_t;
    BasicDefineForSimd

    static auto AddFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vaddq_s32(neonLeft, neonRight);
    }

    static auto MaxFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vmaxq_s32(neonLeft, neonRight);
    }

    static auto MinFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vminq_s32(neonLeft, neonRight);
    }

    static auto InitFunc(const RawType initValue)
    {
        return vdupq_n_s32(initValue);
    }

    template <typename OUT = RawType> static auto LoadFunc(const RawType *rawValuePtr)
    {
        // value ptr is same with output type
        if constexpr (std::is_same_v<RawType, OUT>) {
            return vld1q_s32(rawValuePtr);
        } else {
            // use output load function
            return LoadDifferentType<RawType, OUT>(rawValuePtr);
        }
    }

    template <BasicOp op> static RawType BasicConvert(const SimdType neonValue)
    {
        RawType value[HandleNumOnce];
        vst1q_s32(value, neonValue);
        return BinaryOperation<op, RawType, RawType>::ArrayHandleFunc(value, HandleNumOnce);
    }
};

template <> struct NeonSimd<uint32_t> {
    using RawType = uint32_t;
    using SimdType = uint32x4_t;

    BasicDefineForSimd

    static auto AddFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vaddq_u32(neonLeft, neonRight);
    }

    static auto MaxFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vmaxq_u32(neonLeft, neonRight);
    }

    static auto MinFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vminq_u32(neonLeft, neonRight);
    }

    static auto InitFunc(const RawType initValue)
    {
        return vdupq_n_u32(initValue);
    }

    template <typename OUT = RawType> static auto LoadFunc(const RawType *rawValuePtr)
    {
        // value ptr is same with output type
        if constexpr (std::is_same_v<RawType, OUT>) {
            return vld1q_u32(rawValuePtr);
        } else {
            // use output load function
            return LoadDifferentType<RawType, OUT>(rawValuePtr);
        }
    }

    template <BasicOp op> static RawType BasicConvert(const SimdType neonValue)
    {
        uint32_t value[HandleNumOnce];
        vst1q_u32(value, neonValue);
        return BinaryOperation<op, RawType, RawType>::ArrayHandleFunc(value, HandleNumOnce);
    }
};

template <> struct NeonSimd<int64_t> {
    using RawType = int64_t;
    using SimdType = int64x2_t;
    using EqualSimdType = float64x2_t;

    BasicDefineForSimd

    static auto AddFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vaddq_s64(neonLeft, neonRight);
    }

    static auto MaxFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        auto compare = vcltq_s64(neonLeft, neonRight);
        auto result = vbslq_s64(compare, neonRight, neonLeft);
        return result;
    }

    static auto MinFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        auto compare = vcgtq_s64(neonLeft, neonRight);
        auto result = vbslq_s64(compare, neonRight, neonLeft);
        return result;
    }

    static auto InitFunc(const RawType initValue)
    {
        return vdupq_n_s64(initValue);
    }

    template <typename OUT = RawType> static auto LoadFunc(const RawType *rawValuePtr)
    {
        // value ptr is same with output type
        if constexpr (std::is_same_v<RawType, OUT>) {
            return vld1q_s64(rawValuePtr);
        } else {
            // use output load function
            return LoadDifferentType<RawType, OUT>(rawValuePtr);
        }
    }

    template <BasicOp op> static RawType BasicConvert(const SimdType neonValue)
    {
        int64_t value[HandleNumOnce];
        vst1q_s64(value, neonValue);
        return BinaryOperation<op, RawType, RawType>::ArrayHandleFunc(value, HandleNumOnce);
    }
};


template <> struct NeonSimd<float64_t> {
    using RawType = float64_t;
    using SimdType = float64x2_t;

    static constexpr uint32_t RawSize = sizeof(RawType) * 8; // raw type bit width
    static constexpr uint32_t HandleNumOnce = NeonBitWidth / RawSize;

    static auto AddFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vaddq_f64(neonLeft, neonRight);
    }

    static auto MaxFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vmaxq_f64(neonLeft, neonRight);
    }

    static auto MinFunc(const SimdType neonLeft, const SimdType neonRight)
    {
        return vminq_f64(neonLeft, neonRight);
    }

    static auto InitFunc(const RawType initValue)
    {
        return vdupq_n_f64(initValue);
    }

    template <typename OUT = RawType> static auto LoadFunc(const RawType *rawValuePtr)
    {
        // value ptr is same with output type
        if constexpr (std::is_same_v<RawType, OUT>) {
            return vld1q_f64(rawValuePtr);
        } else {
            // use output load function
            return LoadDifferentType<RawType, OUT>(rawValuePtr);
        }
    }

    template <BasicOp op> static RawType BasicConvert(const SimdType neonValue)
    {
        float64_t value[HandleNumOnce];
        vst1q_f64(value, neonValue);
        return BinaryOperation<op, RawType, RawType>::ArrayHandleFunc(value, HandleNumOnce);
    }
};

template <BasicOp op, typename RawType> struct BinarySimdFunc {};

template <typename RawType> struct BinarySimdFunc<BasicOp::Sum, RawType> {
    static auto CalcSimd(const typename NeonSimd<RawType>::SimdType &value1,
        const typename NeonSimd<RawType>::SimdType &value2)
    {
        return NeonSimd<RawType>::AddFunc(value1, value2);
    }
};

template <typename RawType> struct BinarySimdFunc<BasicOp::Max, RawType> {
    static auto CalcSimd(const typename NeonSimd<RawType>::SimdType value1,
        const typename NeonSimd<RawType>::SimdType value2)
    {
        return NeonSimd<RawType>::MaxFunc(value1, value2);
    }
};

template <typename RawType> struct BinarySimdFunc<BasicOp::Min, RawType> {
    static auto CalcSimd(const typename NeonSimd<RawType>::SimdType value1,
        const typename NeonSimd<RawType>::SimdType value2)
    {
        return NeonSimd<RawType>::MinFunc(value1, value2);
    }
};

template <typename...> struct CheckTypesContainsDecimal128 {
    static constexpr bool value = false;
};

template <typename RawType> struct CheckTypesContainsDecimal128<RawType> {
    static constexpr bool value = std::is_same_v<RawType, omniruntime::type::Decimal128>;
};

template <typename oneType, typename... RemainTypes> struct CheckTypesContainsDecimal128<oneType, RemainTypes...> {
    static constexpr bool value =
        CheckTypesContainsDecimal128<oneType>::value || CheckTypesContainsDecimal128<RemainTypes...>::value;
};
}
}

#endif // OMNI_RUNTIME_NEON_AGGREGATION_FUNC_H
