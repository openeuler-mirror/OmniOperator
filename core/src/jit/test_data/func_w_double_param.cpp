#include <stdio.h>
#include <stdlib.h>
#include <vector>

__attribute__((noinline)) double process(double row_data[] /*row data*/, int y[] /*column data type */, int z /*column count*/)
{
    double sum = 0;
    printf("hello %d\n", z);
    for (int i = 0; i < z; i++)
    {
        printf("row_data[%d]: %f, y[%d]: %d\n", i, row_data[i], i, y[i]);
        if (y[i] == 1)
        {
            sum = sum + row_data[i];
        }
        if (y[i] == 2)
        {
            sum = sum + row_data[i];
        }
        printf("sum=%f\n", sum);
    }
    return sum;
}