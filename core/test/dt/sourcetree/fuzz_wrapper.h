/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 * Description: Fuzz test file
 */

#ifndef OMNI_RUNTIME_FUZZ_WRAPPER_H
#define OMNI_RUNTIME_FUZZ_WRAPPER_H
#include <iostream>

struct FuzzData {
    int16_t shortValue;
    int32_t intValue;
    int64_t longValue;
    bool boolValue;
    double doubleValue;
    int64_t highBits;
    uint64_t lowBits;
    std::string strValue;
};

int GlobalFuzz(struct FuzzData fzd, uint16_t loopCount, std::string filterExpr, int32_t opCnt, uint16_t chooseFunc);

#endif // OMNI_RUNTIME_FUZZ_WRAPPER_H
