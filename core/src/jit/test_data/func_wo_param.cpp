#include <stdio.h>
#include <stdlib.h>
#include <vector>

__attribute__((noinline)) int process()
{
    int row_data[] = {1, 2, 3};
    int y[] = {1, 2, 3};
    int z = 3;

    int sum = 0;
    printf("hello %d\n", z);
    for (int i = 0; i < z; i++)
    {
        printf("y[%d]: %d", i, y[i]);
        if (y[i] == 1)
        {
            sum += row_data[i];
        }
        else if (y[i] == 2)
        {
            sum += row_data[i];
        }
    }
    return sum;
}