/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include <cstdio>

namespace omniruntime {
namespace jit {
// we expect the call to this function to be:
// 1. hardened
// 2. inlined to its caller
int ProcessColumn(int &rowData, const int &y, int colIdx)
{
    switch (y[colIdx]) {
        case 1:
            /* code */
            return rowData[colIdx] + 1;
            break;
        case 2: // 2
            /* code */
            return rowData[colIdx] + 1;
            break;
        default:
            return 0;
            break;
    }
}

__attribute__((noinline)) int Process(const int rowData[], const int y[], int z)
{
    int sum = 0;
    printf("hello %d\n", z);
    for (int i = 0; i < z; i++) {
        printf("row_data[%d]: %d, y[%d]: %d\n", i, row_data[i], i, y[i]);
        if (y[i] == 1) {
            sum = sum + ProcessColumn(rowData, y, i);
        }
        if (y[i] == 2) { // 2
            sum = sum + ProcessColumn(rowData, y, i);
        }
        printf("sum=%d\n", sum);
    }
    return sum;
}
}
}
