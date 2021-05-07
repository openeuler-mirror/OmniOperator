#include <stdio.h>
#include <vector>
#include <iostream>
#include <chrono>
#include <stdlib.h>
#include <math.h>

int agg(int column[], int size) {
    int sum = 0;

    for (int i = 0; i < size; ++i) {
        sum += column[i] + rand() + sqrt((10 * rand()));
    }
    return sum;
}

double agg(double column[], int size) {
    double sum = 0;
    for (int i = 0; i < size; ++i) {
        sum += column[i] + rand();
    }
    return sum;
}

__attribute__((noinline)) double
process(void **columns /*row data*/, int y[] /*column data type */, int z /*column count*/, int row_count) {
    double sum = 0;
    for (int i = 0; i < z; i++) {
        if (y[i] == 1) {
            sum = sum + agg((int *) columns[i], row_count);
        }
        if (y[i] == 2) {
            sum = sum + agg((double *) columns[i], row_count);
        }
    }
    return sum;
}

int main(int argc, char *argv[]) {
    int count = atoi(argv[1]);
    int v1[count]; //value of a column
    int v2[count]; //value of a column
    int v3[count]; //value of a column

    for (int i = 0; i < count; i++) {
        v1[i] = i;
        v2[i] = i * 2;
        v3[i] = i * 3;
    }

    void *columns[] = {v1, v2, v3};

    int column_type[] = {1, 2, 1};      //type of each column, should be 1, or 2 for testing now
    int column_count = 3;
    int row_count = count;


    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::milliseconds ms;
    typedef std::chrono::duration<float> fsec;

    auto t1 = Time::now();

    double result = 0;
    for (int i = 0; i < 10000; i++) {
        result = process((void **) columns, column_type, column_count, row_count);
    }

    auto t0 = Time::now();
    fsec fs = t0 - t1;
    ms d = std::chrono::duration_cast<ms>(fs);
    std::cout << " duration time: " << d.count() << "ms\n";
    printf("result: %f\n", result);
}