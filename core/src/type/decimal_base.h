/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_DECIMAL_BASE_H
#define OMNI_RUNTIME_DECIMAL_BASE_H

#include <type_traits>

namespace omniruntime {
namespace type {
class BasicDecimal {
public:
    BasicDecimal() = default;
    ~BasicDecimal() = default;

    template <typename SignedInt> SignedInt SafeSignedAdd(SignedInt u, SignedInt v)
    {
        using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
        return static_cast<SignedInt>(static_cast<UnsignedInt>(u) + static_cast<UnsignedInt>(v));
    }
};
}
}
#endif // OMNI_RUNTIME_DECIMAL_BASE_H
