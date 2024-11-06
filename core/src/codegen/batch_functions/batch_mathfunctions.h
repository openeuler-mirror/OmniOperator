/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch math functions implementation
 */
#ifndef OMNI_RUNTIME_BATCH_MATHFUNCTIONS_H
#define OMNI_RUNTIME_BATCH_MATHFUNCTIONS_H

#include <iostream>
#include <cmath>

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

namespace omniruntime::codegen::function {
template <typename T> extern DLLEXPORT void BatchAbs(T *x, bool *resIsNull, T *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = std::abs(x[i]);
    }
}

extern "C" DLLEXPORT void BatchCastInt32ToDouble(int32_t *x, bool *resIsNull, double *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastInt64ToDouble(int64_t *x, bool *resIsNull, double *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastInt32ToInt64(int32_t *x, bool *resIsNull, int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastInt64ToInt32(int64_t *x, bool *resIsNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDoubleToInt32HalfUp(double *x, bool *resIsNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDoubleToInt64HalfUp(double *x, bool *resIsNull, int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDoubleToInt32Down(double *x, bool *resIsNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchCastDoubleToInt64Down(double *x, bool *resIsNull, int64_t *output, int32_t rowCnt);

// double functions
extern "C" DLLEXPORT void BatchAddDouble(double *left, double *right, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubtractDouble(double *left, double *right, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMultiplyDouble(double *left, double *right, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivideDouble(double *left, double *right, int32_t rowCnt);

extern "C" DLLEXPORT void BatchModulusDouble(double *left, double *right, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLessThanDouble(double *left, double *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLessThanEqualDouble(double *left, double *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanDouble(double *left, double *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanEqualDouble(double *left, double *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchEqualDouble(double *left, double *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchNotEqualDouble(double *left, double *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchNormalizeNaNAndZero(double *input, bool *isAnyNull, double *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchPowerDouble(double *base, double *exponent, double *output, int32_t rowCnt);

// long functions
extern "C" DLLEXPORT void BatchAddInt64(int64_t *left, int64_t *right, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubtractInt64(int64_t *left, int64_t *right, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMultiplyInt64(int64_t *left, int64_t *right, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivideInt64(int64_t contextPtr, int64_t *left, int64_t *right, int32_t rowCnt,
    bool *isNull);

extern "C" DLLEXPORT void BatchModulusInt64(int64_t contextPtr, int64_t *left, int64_t *right, int32_t rowCnt,
    bool *isNull);

extern "C" DLLEXPORT void BatchLessThanInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLessThanEqualInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanEqualInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchEqualInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchNotEqualInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt);

// int functions
extern "C" DLLEXPORT void BatchAddInt32(int32_t *left, int32_t *right, int32_t rowCnt);

extern "C" DLLEXPORT void BatchSubtractInt32(int32_t *left, int32_t *right, int32_t rowCnt);

extern "C" DLLEXPORT void BatchMultiplyInt32(int32_t *left, int32_t *right, int32_t rowCnt);

extern "C" DLLEXPORT void BatchDivideInt32(int64_t contextPtr, int32_t *left, int32_t *right, int32_t rowCnt,
    bool *isNull);

extern "C" DLLEXPORT void BatchModulusInt32(int64_t contextPtr, int32_t *left, int32_t *right, int32_t rowCnt,
    bool *isNull);

extern "C" DLLEXPORT void BatchLessThanInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchLessThanEqualInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchGreaterThanEqualInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchEqualInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchNotEqualInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchEqualBool(bool *left, bool toCmp, int32_t rowCnt);

extern "C" DLLEXPORT void BatchPmod(int32_t *x, int32_t *y, bool *isAnyNull, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void BatchRoundLong(int64_t *num, int32_t *decimals, bool *isAnyNull, int64_t *output,
    int32_t rowCnt);

template <typename T>
extern DLLEXPORT void BatchRound(T *num, int32_t *decimals, bool *isAnyNull, T *output, int32_t rowCnt)
{
    int32_t tenthPower = 10;
    double factor = std::pow(tenthPower, decimals[0]);

    for (int i = 0; i < rowCnt; ++i) {
        if (std::isnan(num[i]) || std::isinf(num[i])) {
            output[i] = num[i];
            continue;
        }

        if (num[i] < 0) {
            output[i] = -(std::round(-num[i] * factor) / factor);
            continue;
        }

        output[i] = std::round(num[i] * factor) / factor;
    }
}

template <typename T>
extern DLLEXPORT void BatchGreatest(T *xValue, bool *xIsNull, T *yValue, bool *yIsNull, bool *retIsNull, T *output,
    int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (xIsNull[i] && yIsNull[i]) {
            retIsNull[i] = true;
            output[i] = xValue[i];
            continue;
        }
        if (xIsNull[i] || (!yIsNull[i] && yValue[i] > xValue[i])) {
            output[i] = yValue[i];
            continue;
        }
        output[i] = xValue[i];
    }
}
}
#endif // OMNI_RUNTIME_BATCH_MATHFUNCTIONS_H
