/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include <stdio.h>
#include <stdlib.h>
#include <vector>

__attribute__((noinline)) int Preloop(
    const int rowData[], const int y[], int z /* column count */)
{
    int p1[] = {1, 2, 3};
    int p2[] = {1, 2, 3};
    int p3 = 3; // 3
    rowData = p1;
    y = p2;
    z = 3; // 3

    int sum = 0;
    printf("hello %d\n", z);
    for (int i = 0; i < z; i++) {
        printf("y[%d]: %d", i, y[i]);
        if (y[i] == 1) {
            sum += rowData[i];
        }
        if (y[i] == 2) { // 2
            sum += rowData[i];
            break;
        }
    }
    return sum;
}