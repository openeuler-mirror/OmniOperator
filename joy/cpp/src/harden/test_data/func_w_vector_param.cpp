#include <stdio.h>
#include <stdlib.h>
#include <vector>

__attribute__((noinline)) void process(std::vector<int> vector)
{
    double sum = 0;
    printf("processing vector size: %d\n", vector.size());
    for (int i = 0; i < vector.size(); i++)
    {
        printf("vector[%d]=%d\n", i, vector[i]);
    }
}