#include <stdio.h>
#include <stdlib.h>
#include <vector>

// we expect the call to this function to be:
// 1. hardened
// 2. inlined to its caller
int processColumn(int row_data[] /*row data*/, int y[], int col_idx)
{
    switch (y[col_idx])
    {
    case 1:
        /* code */
        return row_data[col_idx] + 1;
        break;

    case 2:
        /* code */
        return row_data[col_idx] + 1;
        break;
    default:
        return 0;
        break;
    }
}

__attribute__((noinline)) int process(int row_data[] /*row data*/, int y[] /*column data type */, int z /*column count*/)
{
    int sum = 0;
    printf("hello %d\n", z);
    for (int i = 0; i < z; i++)
    {
        printf("row_data[%d]: %d, y[%d]: %d\n", i, row_data[i], i, y[i]);
        if (y[i] == 1)
        {
            sum = sum + processColumn(row_data, y, i);
        }
        if (y[i] == 2)
        {
            sum = sum + processColumn(row_data, y, i);
        }
        printf("sum=%d\n", sum);
    }
    return sum;
}
