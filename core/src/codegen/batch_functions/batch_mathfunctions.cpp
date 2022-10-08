/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch math functions implementation
 */
#include "batch_mathfunctions.h"
#include <iostream>
#include <cfloat>
#include "util/engine.h"
#include "../functions/context_helper.h"
#include "../functions/mathfunctions.h"


#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

using namespace omniruntime::codegen;

extern "C" DLLEXPORT void BatchCastInt32ToInt64(int32_t *x, bool *resIsNull, int64_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = static_cast<int64_t>(x[i]);
    }
}

extern "C" DLLEXPORT void BatchCastInt64ToInt32(int64_t *x, bool *resIsNull, int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = static_cast<int32_t>(x[i]);
    }
}

extern "C" DLLEXPORT void BatchCastInt32ToDouble(int32_t *x, bool *resIsNull, double *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = static_cast<double>(x[i]);
    }
}

extern "C" DLLEXPORT void BatchCastInt64ToDouble(int64_t *x, bool *resIsNull, double *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = static_cast<double>(x[i]);
    }
}

extern "C" DLLEXPORT void BatchCastDoubleToInt32(double *x, bool *resIsNull, int32_t *output, int32_t rowCnt)
{
    EngineType engineType = EngineUtil::GetInstance().GetEngineType();
    if (engineType == EngineType::Spark) {
        for (int i = 0; i < rowCnt; i++) {
            output[i] = static_cast<int32_t>(x[i]);
        }
    } else {
        for (int i = 0; i < rowCnt; i++) {
            output[i] = static_cast<int32_t>(Round(x[i], 0));
        }
    }
}

extern "C" DLLEXPORT void BatchCastDoubleToInt64(double *x, bool *resIsNull, int64_t *output, int32_t rowCnt)
{
    EngineType engineType = EngineUtil::GetInstance().GetEngineType();
    if (engineType == EngineType::Spark) {
        for (int i = 0; i < rowCnt; i++) {
            output[i] = static_cast<int64_t>(x[i]);
        }
    } else {
        for (int i = 0; i < rowCnt; i++) {
            output[i] = static_cast<int64_t>(Round(x[i], 0));
        }
    }
}

extern "C" DLLEXPORT void BatchAddDouble(double *left, double *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] + right[i];
    }
}

extern "C" DLLEXPORT void BatchSubtractDouble(double *left, double *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] - right[i];
    }
}

extern "C" DLLEXPORT void BatchMultiplyDouble(double *left, double *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] * right[i];
    }
}

extern "C" DLLEXPORT void BatchDivideDouble(double *left, double *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] / right[i];
    }
}

extern "C" DLLEXPORT void BatchModulusDouble(double *left, double *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = std::fmod(left[i], right[i]);
    }
}

extern "C" DLLEXPORT void BatchLessThanDouble(double *left, double *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (left[i] < right[i]);
    }
}

extern "C" DLLEXPORT void BatchLessThanEqualDouble(double *left, double *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (left[i] <= right[i]);
    }
}

extern "C" DLLEXPORT void BatchGreaterThanDouble(double *left, double *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (left[i] > right[i]);
    }
}

extern "C" DLLEXPORT void BatchGreaterThanEqualDouble(double *left, double *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (left[i] >= right[i]);
    }
}

extern "C" DLLEXPORT void BatchEqualDouble(double *left, double *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (std::fabs(left[i] - right[i]) < DBL_EPSILON);
    }
}

extern "C" DLLEXPORT void BatchNotEqualDouble(double *left, double *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = (left[i] != right[i]);
    }
}

extern "C" DLLEXPORT void BatchAddInt64(int64_t *left, int64_t *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] + right[i];
    }
}

extern "C" DLLEXPORT void BatchSubtractInt64(int64_t *left, int64_t *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] - right[i];
    }
}

extern "C" DLLEXPORT void BatchMultiplyInt64(int64_t *left, int64_t *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] * right[i];
    }
}

extern "C" DLLEXPORT void BatchDivideInt64(int64_t contextPtr, int64_t *left, int64_t *right, int32_t rowCnt,
    bool *isNull)
{
    for (int i = 0; i < rowCnt; i++) {
        if (isNull[i]) {
            continue;
        }
        if (right[i] == 0) {
            char message[] = "Divided by zero error!";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            return;
        }
        left[i] = left[i] / right[i];
    }
}

extern "C" DLLEXPORT void BatchModulusInt64(int64_t contextPtr, int64_t *left, int64_t *right, int32_t rowCnt,
    bool *isNull)
{
    for (int i = 0; i < rowCnt; i++) {
        if (isNull[i]) {
            continue;
        }
        if (right[i] == 0) {
            char message[] = "Divided by zero error!";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            return;
        }
        left[i] = std::fmod(left[i], right[i]);
    }
}

extern "C" DLLEXPORT void BatchLessThanInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] < right[i];
    }
}

extern "C" DLLEXPORT void BatchLessThanEqualInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] <= right[i];
    }
}

extern "C" DLLEXPORT void BatchGreaterThanInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] > right[i];
    }
}

extern "C" DLLEXPORT void BatchGreaterThanEqualInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] >= right[i];
    }
}

extern "C" DLLEXPORT void BatchEqualInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] == right[i];
    }
}

extern "C" DLLEXPORT void BatchNotEqualInt64(int64_t *left, int64_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] != right[i];
    }
}

extern "C" DLLEXPORT void BatchAddInt32(int32_t *left, int32_t *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] + right[i];
    }
}

extern "C" DLLEXPORT void BatchSubtractInt32(int32_t *left, int32_t *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] - right[i];
    }
}

extern "C" DLLEXPORT void BatchMultiplyInt32(int32_t *left, int32_t *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] * right[i];
    }
}

extern "C" DLLEXPORT void BatchDivideInt32(int64_t contextPtr, int32_t *left, int32_t *right, int32_t rowCnt,
    bool *isNull)
{
    for (int i = 0; i < rowCnt; i++) {
        if (isNull[i]) {
            continue;
        }
        if (right[i] == 0) {
            char message[] = "Divided by zero error!";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            return;
        }
        left[i] = left[i] / right[i];
    }
}

extern "C" DLLEXPORT void BatchModulusInt32(int64_t contextPtr, int32_t *left, int32_t *right, int32_t rowCnt,
    bool *isNull)
{
    for (int i = 0; i < rowCnt; i++) {
        if (isNull[i]) {
            continue;
        }
        if (right[i] == 0) {
            char message[] = "Divided by zero error!";
            SetError(contextPtr, message, sizeof(message) / sizeof(char));
            return;
        }
        left[i] = std::fmod(left[i], right[i]);
    }
}

extern "C" DLLEXPORT void BatchLessThanInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] < right[i];
    }
}

extern "C" DLLEXPORT void BatchLessThanEqualInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] <= right[i];
    }
}

extern "C" DLLEXPORT void BatchGreaterThanInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] > right[i];
    }
}

extern "C" DLLEXPORT void BatchGreaterThanEqualInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] >= right[i];
    }
}

extern "C" DLLEXPORT void BatchEqualInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] == right[i];
    }
}

extern "C" DLLEXPORT void BatchNotEqualInt32(int32_t *left, int32_t *right, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        output[i] = left[i] != right[i];
    }
}

extern "C" DLLEXPORT void BatchEqualBool(bool *left, bool toCmp, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = (left[i] == toCmp);
    }
}

extern "C" DLLEXPORT void BatchPmod(int32_t *x, int32_t *y, bool *isAnyNull, int32_t *output, int32_t rowCnt)
{
    int32_t r;
    for (int i = 0; i < rowCnt; i++) {
        if (y[i] == 0) {
            output[i] = 0;
            continue;
        }
        r = x[i] % y[i];
        if (r < 0) {
            output[i] = (r + y[i]) % y[i];
        } else {
            output[i] = r;
        }
    }
}