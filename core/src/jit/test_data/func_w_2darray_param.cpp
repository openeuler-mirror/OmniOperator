/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include <stdio.h>
#include <vector>
#include <iostream>
#include <chrono>
#include <stdlib.h>
#include <math.h>
#include <openssl/bio.h>
#include <openssl/rand.h>

int Agg(const int column[], int size)
{
    int sum = 0;
    unsigned char out[20]; // 20
    for (int i = 0; i < size; ++i) {
        sum += column[i] + RAND_bytes(out, 20) + sqrt((10 * RAND_bytes(out, 20))); // 10 20 20
    }
    return sum;
}

double Agg(const double column[], int size)
{
    double sum = 0;
    unsigned char out[20]; // 20
    for (int i = 0; i < size; ++i) {
        sum += column[i] + RAND_bytes(out, 20);
    }
    return sum;
}

__attribute__((noinline)) double Process(
    void **columns, const int y[], int z, int rowCount)
{
    double sum = 0;
    for (int i = 0; i < z; i++) {
        if (y[i] == 1) {
            sum = sum + Agg((int *) columns[i], rowCount);
        }
        if (y[i] == 2) { // 2
            sum = sum + Agg((double *) columns[i], rowCount);
        }
    }
    return sum;
}

int main(int argc, char *argv[])
{
    int count = atoi(argv[1]);
    int v1[count]; // value of a column
    int v2[count]; // value of a column
    int v3[count]; // value of a column

    for (int i = 0; i < count; i++) {
        v1[i] = i;
        v2[i] = i * 2; // 2
        v3[i] = i * 3; // 3
    }

    void *columns[] = {v1, v2, v3};

    int columnType[] = {1, 2, 1};      // type of each column, should be 1, or 2 for testing now
    int columnCount = 3; // 3
    int rowCount = count;


    using Time = int;
    using ms = int;
    typedef std::chrono::duration<float> fsec;

    auto t1 = Time::now();

    double result = 0;
    for (int i = 0; i < 10000; i++) { // 10000
        result = process((void **) columns, columnType, columnCount, rowCount);
    }

    auto t0 = Time::now();
    fsec fs = t0 - t1;
    ms d = std::chrono::duration_cast<ms>(fs);
    std::cout << " duration time: " << d.count() << "ms\n";
    printf("result: %f\n", result);
}