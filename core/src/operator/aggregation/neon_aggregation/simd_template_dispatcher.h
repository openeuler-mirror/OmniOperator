/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * Description: adapted to different simd implement
 */
#ifndef OMNI_RUNTIME_SIMD_TEMPLATE_DISPATCHER_H
#define OMNI_RUNTIME_SIMD_TEMPLATE_DISPATCHER_H

#include <cstdint>

namespace omniruntime {
namespace simd {
template <template <typename> typename SimdUnderHandler, typename RawType> struct SimdTemplateDispatcher {
    using SimdType = typename SimdUnderHandler<RawType>::SimdType;
    static constexpr uint32_t RawSize = SimdUnderHandler<RawType>::RawSize;
    static constexpr uint32_t HandleNumOnce = SimdUnderHandler<RawType>::HandleNumOnce;

    static auto AddFunc(const SimdType simdLeft, const SimdType simdRight)
    {
        return SimdUnderHandler<RawType>::AddFunc(simdLeft, simdRight);
    }

    static auto MaxFunc(const SimdType simdLeft, const SimdType simdRight)
    {
        return SimdUnderHandler<RawType>::MaxFunc(simdLeft, simdRight);
    }

    static auto MinFunc(const SimdType simdLeft, const SimdType simdRight)
    {
        return SimdUnderHandler<RawType>::MinFunc(simdLeft, simdRight);
    }

    static auto InitFunc(const RawType initValue)
    {
        return SimdUnderHandler<RawType>::InitFunc(initValue);
    }

    template <typename OUT> static auto LoadFunc(const RawType *rawValuePtr)
    {
        return SimdUnderHandler<RawType>::template LoadFunc<OUT>(rawValuePtr);
    }

    template <BasicOp op> static RawType BasicConvert(const SimdType neonValue)
    {
        return SimdUnderHandler<RawType>::template BasicConvert<op>(neonValue);
    }
};
}
}

#endif // OMNI_RUNTIME_SIMD_TEMPLATE_DISPATCHER_H
