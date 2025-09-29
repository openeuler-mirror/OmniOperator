/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
