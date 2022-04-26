/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: Fuzz test file
 */

#ifndef OMNI_RUNTIME_FUZZ_WRAPPER_H
#define OMNI_RUNTIME_FUZZ_WRAPPER_H
#include <iostream>

int GlobalFuzz(int32_t intValue, int64_t longValue, bool boolValue, double doubleValue, int64_t highBits,
    uint64_t lowBits, std::string stringValue, int32_t loopCount, int32_t charSize);

#endif // OMNI_RUNTIME_FUZZ_WRAPPER_H
