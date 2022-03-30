/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include <cstdio>
#include <vector>

namespace omniruntime {
namespace jit {
__attribute__((noinline)) void Process(std::vector<int> vector)
{
    double sum = 0;
    printf("processing vector size: %d\n", vector.size());
    for (int i = 0; i < vector.size(); i++) {
        printf("vector[%d]=%d\n", i, vector[i]);
    }
}
}
}
