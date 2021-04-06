#include <stdio.h>
#include <stdlib.h>
#include <vector>

__attribute__((noinline)) double process(void **columns /*row data*/, int y[] /*column data type */, int z /*column count*/, int row_count)
{
    double sum = 0;
    for (int i = 0; i < z; i++)
    {
        if (y[i] == 1)
        {
            sum = sum + ((int *)columns[i])[0];
        }
        if (y[i] == 2)
        {
            sum = sum + ((int *)columns[i])[0];

        }
    }
    return sum;
}
