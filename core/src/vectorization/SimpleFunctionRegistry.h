/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once

#include "VectorFunction.h"
#include "util/compiler_util.h"

namespace omniruntime::vectorization {
using SignatureMap = std::unordered_map<codegen::FunctionSignature, std::unique_ptr<const VectorFunction>>;

class SimpleFunctionRegistry {
};
}
