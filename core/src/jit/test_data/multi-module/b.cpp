/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include <cstdio>

namespace omniruntime {
namespace jit {
int Callee()
{
    printf("I am callee");
    return 1234; // 1234
}
}
}
