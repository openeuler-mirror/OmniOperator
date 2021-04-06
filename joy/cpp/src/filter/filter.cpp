//
// Created by kkrazy on 2021-03-18.
//
#include "stdio.h"
#include "chrono"

using namespace std;

int test() {
    int count = 102400;
    auto c1 = new int[count];
    auto c2 = new int[count];
    auto c3 = new int[count];
    auto c4 = new bool[count];
    for (int i=0; i<1023; i++) {
        c1[i] = 12;
        c2[i] = 2;
        c3[i] = 6;
    }

    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::microseconds us;
    typedef std::chrono::duration<float> fsec;

    auto t1 = Time::now();
    double result = 0;
    for (int i=0; i<count; i++) {
        c4[i] = c1[i] > 10 && c2[i] < 5 && c3[i] ==6;
    }
    auto t0 = Time::now();
    fsec fs = t0 - t1;
    us d = std::chrono::duration_cast<us>(fs);
    printf(" filter duration time: %lu\n", d.count());
    printf(" filter result: %d\n", c4[1]);
    return 12345;
}

bool filter(int a, long b, double c) {
    return a > 10 && b < 5 && c ==6;
}