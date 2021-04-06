#include <stdio.h>
#include <stdlib.h>
#include <vector>

__attribute__((noinline)) int preloop(int row_data[] /*row data*/, int y[] /*column data type */, int z /*column count*/)
{
    int p1[] = {1, 2, 3};
    int p2[] = {1, 2, 3};
    int p3 = 3;
    row_data = p1;
    y = p2;
    z = 3;

    int sum = 0;
    printf("hello %d\n", z);
    for (int i = 0; i < z; i++)
    {
        printf("y[%d]: %d", i, y[i]);
        if (y[i] == 1)
        {
            sum += row_data[i];
        }
        if (y[i] == 2)
        {
            sum += row_data[i];
            break;
        }
    }
    return sum;
}