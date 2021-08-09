/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#ifndef OMNI_RUNTIME_OP_UTIL_INTERNAL_H
#define OMNI_RUNTIME_OP_UTIL_INTERNAL_H

#include <type_traits>

namespace omniruntime {
namespace vec {
template <typename SignedInt> SignedInt SafeSignedAdd(SignedInt u, SignedInt v)
{
    using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
    return static_cast<SignedInt>(static_cast<UnsignedInt>(u) + static_cast<UnsignedInt>(v));
}
}
}
#endif // OMNI_RUNTIME_OP_UTIL_INTERNAL_H
