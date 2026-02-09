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
    RegisterUnaryIntegralSameType<BitwiseNotFunction>({prefix + "bitwise_not"});
    RegisterFunction<ShiftLeftFunction, int32_t, int32_t, int32_t>(
            prefix + "shiftleft", {OMNI_INT, OMNI_INT}, OMNI_INT);
    RegisterFunction<ShiftLeftFunction, int64_t, int64_t, int32_t>(
            prefix + "shiftleft", {OMNI_LONG, OMNI_INT}, OMNI_LONG);
    RegisterFunction<ShiftRightFunction, int32_t, int32_t, int32_t>(
            prefix + "shiftright", {OMNI_INT, OMNI_INT}, OMNI_INT);
    RegisterFunction<ShiftRightFunction, int64_t, int64_t, int32_t>(
            prefix + "shiftright", {OMNI_LONG, OMNI_INT}, OMNI_LONG);
    
    // BitGet function: returns int8_t (0 or 1) for the bit at specified position
    RegisterFunction<BitGetFunction, int8_t, int8_t, int32_t>(
            prefix + "bit_get", {OMNI_BYTE, OMNI_INT}, OMNI_BYTE);
    RegisterFunction<BitGetFunction, int8_t, int16_t, int32_t>(
            prefix + "bit_get", {OMNI_SHORT, OMNI_INT}, OMNI_BYTE);
    RegisterFunction<BitGetFunction, int8_t, int32_t, int32_t>(
            prefix + "bit_get", {OMNI_INT, OMNI_INT}, OMNI_BYTE);
    RegisterFunction<BitGetFunction, int8_t, int64_t, int32_t>(
            prefix + "bit_get", {OMNI_LONG, OMNI_INT}, OMNI_BYTE);

    // BitCount function: returns int32_t representing the number of 1-bits
    // Supports: bool, byte, short, int, long
    RegisterFunction<BitCountFunction, int32_t, bool>(
            prefix + "bit_count", {OMNI_BOOLEAN}, OMNI_INT);
    RegisterFunction<BitCountFunction, int32_t, int8_t>(
            prefix + "bit_count", {OMNI_BYTE}, OMNI_INT);
    RegisterFunction<BitCountFunction, int32_t, int16_t>(
            prefix + "bit_count", {OMNI_SHORT}, OMNI_INT);
    RegisterFunction<BitCountFunction, int32_t, int32_t>(
            prefix + "bit_count", {OMNI_INT}, OMNI_INT);
    RegisterFunction<BitCountFunction, int32_t, int64_t>(
            prefix + "bit_count", {OMNI_LONG}, OMNI_INT);
}
}
