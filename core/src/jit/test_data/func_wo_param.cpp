/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include <stdio.h>
#include <stdlib.h>
#include <vector>

__attribute__((noinline)) int Process()
{
    int rowData[] = {1, 2, 3};
    int y[] = {1, 2, 3};
    int z = 3;

    int sum = 0;
    printf("hello %d\n", z);
    for (int i = 0; i < z; i++) {
        printf("y[%d]: %d", i, y[i]);
        if (y[i] == 1) {
            sum += rowData[i];
        } else if (y[i] == 2) { // 20
            sum += rowData[i];
        }
    }
    return sum;
}