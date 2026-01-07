/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once

#include <string>
#include "vectorization/SimpleFunction.h"
#include "SimpleFunctionRegistry.h"

namespace omniruntime::vectorization {
class RegisterFunctions {
public:
    static void RegisterAllFunctions(const std::string &prefix = "");

    static int Register();

    inline static int state_ = Register();
};

void link_register_functions();
}
