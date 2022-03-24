/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include <stdio.h>
#include <stdlib.h>
#include <vector>

__attribute__((noinline)) double Process(void **columns, const int y[], int z, int rowCount)
{
    double sum = 0;
    for (int i = 0; i < z; i++) {
        if (y[i] == 1) {
            sum = sum + ((int *)columns[i])[0];
        }
        if (y[i] == 2) { // 2
            sum = sum + ((int *)columns[i])[0];
        }
    }
    return sum;
}
