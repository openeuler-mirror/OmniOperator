#include <stdio.h>
#include <stdlib.h>
#include <vector>

__attribute__((noinline)) int process(int row_data[] /*row data*/, int y[] /*column data type */, int z /*column count*/)
{
    int sum = 0;
    printf("hello %d\n", z);
    for (int i = 0; i < z; i++)
    {
        printf("row_data[%d]: %d, y[%d]: %d\n", i, row_data[i], i, y[i]);
        if (y[i] == 1)
        {
            sum = sum + row_data[i];
        }
        if (y[i] == 2)
        {
            sum = sum + row_data[i];
        }
        printf("sum=%d\n", sum);
    }
    return sum;
}