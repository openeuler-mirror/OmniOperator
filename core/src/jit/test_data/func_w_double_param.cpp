/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include <cstdio>

namespace omniruntime {
namespace jit {
__attribute__((noinline)) double Process(const double rowData[], const int y[], int z)
{
    double sum = 0;
    printf("hello %d\n", z);
    for (int i = 0; i < z; i++) {
        printf("row_data[%d]: %f, y[%d]: %d\n", i, row_data[i], i, y[i]);
        if (y[i] == 1) {
            sum = sum + rowData[i];
        }
        if (y[i] == 2) { // 2
            sum = sum + rowData[i];
        }
        printf("sum=%f\n", sum);
    }
    return sum;
}
}
}
