/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include <string>
#include "../functions/Bitwise.h"
#include "RegistrationHelpers.h"
#include "SimpleFunctionRegistry.h"

namespace omniruntime::vectorization {
void RegisterBitwiseFunctions(const std::string &prefix)
{
    RegisterBinaryIntegral<BitwiseAndFunction>({prefix + "bitwise_and"});
    RegisterBinaryIntegral<BitwiseOrFunction>({prefix + "bitwise_or"});
    RegisterBinaryIntegral<BitwiseXorFunction>({prefix + "bitwise_xor"});
    RegisterFunction<ShiftLeftFunction, int32_t, int32_t, int32_t>(
            prefix + "shiftleft", {OMNI_INT, OMNI_INT}, OMNI_INT);
    RegisterFunction<ShiftLeftFunction, int64_t, int64_t, int32_t>(
            prefix + "shiftleft", {OMNI_LONG, OMNI_INT}, OMNI_LONG);
    RegisterFunction<ShiftRightFunction, int32_t, int32_t, int32_t>(
            prefix + "shiftright", {OMNI_INT, OMNI_INT}, OMNI_INT);
    RegisterFunction<ShiftRightFunction, int64_t, int64_t, int32_t>(
            prefix + "shiftright", {OMNI_LONG, OMNI_INT}, OMNI_LONG);
}
}
