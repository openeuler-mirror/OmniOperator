/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: batch util functions implementation
 */

#ifndef OMNI_RUNTIME_BATCH_UTILFUNCTIONS_H
#define OMNI_RUNTIME_BATCH_UTILFUNCTIONS_H
#include <iostream>
#include <cmath>
#include <vector>
#include "type/decimal128.h"
#include <huawei_secure_c/include/securec.h>

using namespace omniruntime::type;

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

// make an array like {0, 1, 2, 3, ..., rowCnt - 1}
extern "C" DLLEXPORT void fillRowIndexArray(int32_t *dataArray, int32_t rowCnt);

// convert literal to an array
extern "C" DLLEXPORT void fillBool(bool *nullArray, bool isNull, int32_t rowCnt);

extern "C" DLLEXPORT void fillInt32(int32_t *dataArray, bool *nullArray, int32_t literal, bool isNull, int32_t rowCnt);

extern "C" DLLEXPORT void fillInt64(int64_t *dataArray, bool *nullArray, int64_t literal, bool isNull, int32_t rowCnt);

extern "C" DLLEXPORT void fillDouble(double *dataArray, bool *nullArray, double literal, bool isNull, int32_t rowCnt);

extern "C" DLLEXPORT void fillDecimal128(Decimal128 *dataArray, bool *nullArray, __int128_t literal, bool isNull,
    int32_t rowCnt);

extern "C" DLLEXPORT void fillString(int64_t contextPtr, uint8_t **dataArray, bool *nullArray, int32_t *lengthArray,
    uint8_t *literal, bool isNull, int32_t length, int32_t rowCnt);

// fill varchar length array according to offset array
extern "C" DLLEXPORT void fillLength(int32_t *offsets, int32_t *rowIdxArray, int32_t *lengthArray, int32_t rowCnt);

// logical operations for boolean
extern "C" DLLEXPORT int32_t createAndNot(bool *dataArray, bool *nullArray, int32_t *rowIdxArray, int32_t rowCnt);

extern DLLEXPORT void createNot(bool *val, int32_t rowCnt);

extern DLLEXPORT void createOr(bool *left, bool *right, int32_t rowCnt);

extern DLLEXPORT void createAnd(bool *left, bool *right, int32_t rowCnt);

extern "C" DLLEXPORT void createAndNotBool(bool *dataArray, bool *nullArray, int32_t rowCnt);

// process AND/OR expression
extern DLLEXPORT void createOrExpr(bool *left, bool *leftNull, bool *right, bool *rightNull, int32_t rowCnt);

extern DLLEXPORT void createAndExpr(bool *left, bool *leftNull, bool *right, bool *rightNull, int32_t rowCnt);

// copy result to output vector
extern "C" DLLEXPORT void copyBoolean(bool *dataArray, bool *output, int32_t rowCnt);

extern "C" DLLEXPORT void copyInt32(int32_t *dataArray, int32_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void copyInt64(int64_t *dataArray, int64_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void copyDouble(double *dataArray, double *output, int32_t rowCnt);

extern "C" DLLEXPORT void copyDecimal128(__int128_t *dataArray, __int128_t *output, int32_t rowCnt);

extern "C" DLLEXPORT void copyString(uint8_t **dataArray, uint8_t **output, int32_t rowCnt);


template <typename T>
extern DLLEXPORT void coalesce(T *lArray, bool *lIsNull, T *rArray, bool *rIsNulll, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (lIsNull[i] == true) {
            lArray[i] = rArray[i];
            lIsNull[i] = rIsNulll[i];
        }
    }
}

extern "C" DLLEXPORT void coalesceString(uint8_t **lArray, bool *lIsNull, int32_t *lLength, uint8_t **rArray,
    bool *rIsNull, int32_t *rLength, int32_t rowCnt);

extern "C" DLLEXPORT void coalesceDecimal64(int64_t *lArray, bool *lIsNull, int32_t lPrecision, int32_t lScale,
                                            int64_t *rArray, bool *rIsNull, int32_t rPrecision, int32_t rScale, int32_t rowCnt);

extern "C" DLLEXPORT void coalesceDecimal128(Decimal128 *lArray, bool *lIsNull, int32_t lPrecision, int32_t lScale,
    Decimal128 *rArray, bool *rIsNull, int32_t rPrecision, int32_t rScale, int32_t rowCnt);

template <typename T>
extern DLLEXPORT void ifExpr(bool *ifCond, bool *ifNull, T *trueValue, bool *trueNull, T *falseValue, bool *falseNull,
    int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (!(ifCond[i] && !ifNull[i])) {
            trueValue[i] = falseValue[i];
            trueNull[i] = falseNull[i];
        }
    }
}

template <typename T>
extern DLLEXPORT void ifExprDecimal(bool *ifCond, bool *ifNull, T *trueValue, bool *trueNull, int32_t truePrecision,
    int32_t trueScale, T *falseValue, bool *falseNull, int32_t falsePrecision, int32_t falseScale,
    int32_t outputPrecision, int32_t outputScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (!(ifCond[i] && !ifNull[i])) {
            trueValue[i] = falseValue[i];
            trueNull[i] = falseNull[i];
        }
    }
}

extern "C" DLLEXPORT void ifExprStr(bool *ifCond, bool *ifNull, uint8_t **trueValue, bool *trueNull,
    int32_t *trueLength, uint8_t **falseValue, bool *falseNull, int32_t *falseLength, int32_t rowCnt);

template <typename T>
extern DLLEXPORT void switchExpr(int32_t whenCnt, int64_t *whenClauses, int64_t *whenBools, int64_t *resultValues,
    int64_t *resultNulls, T *elseValue, bool *elseNull, T *finalResult, bool *finalNull, int32_t rowCnt)
{
    std::vector<bool *> whenValues;
    std::vector<bool *> whenNulls;

    std::vector<T *> resValues;
    std::vector<bool *> resNulls;

    for (int i = 0; i < whenCnt; ++i) {
        whenValues.push_back((bool *)whenClauses[i]);
        whenNulls.push_back((bool *)whenBools[i]);
        resValues.push_back((T *)resultValues[i]);
        resNulls.push_back((bool *)resultNulls[i]);
    }

    for (int i = 0; i < rowCnt; ++i) {
        bool hasSet = false;
        for (int j = 0; j < whenCnt; ++j) {
            if (whenValues[j][i] && !whenNulls[j][i]) {
                finalResult[i] = resValues[j][i];
                finalNull[i] = resNulls[j][i];
                hasSet = true;
                break;
            }
        }
        if (!hasSet) {
            finalResult[i] = elseValue[i];
            finalNull[i] = elseNull[i];
        }
    }
}

template <typename T>
extern DLLEXPORT void switchExprDecimal(int32_t whenCnt, int64_t *whenClauses, int64_t *whenBools,
    int64_t *resultValues, int64_t *resultNulls, T *elseValue, bool *elseNull, T *finalResult, bool *finalNull,
    int32_t rowCnt)
{
    std::vector<bool *> whenValues;
    std::vector<bool *> whenNulls;

    std::vector<T *> resValues;
    std::vector<bool *> resNulls;

    for (int i = 0; i < whenCnt; ++i) {
        whenValues.push_back((bool *)whenClauses[i]);
        whenNulls.push_back((bool *)whenBools[i]);
        resValues.push_back((T *)resultValues[i]);
        resNulls.push_back((bool *)resultNulls[i]);
    }

    for (int i = 0; i < rowCnt; ++i) {
        bool hasSet = false;
        for (int j = 0; j < whenCnt; ++j) {
            if (whenValues[j][i] && !whenNulls[j][i]) {
                finalResult[i] = resValues[j][i];
                finalNull[i] = resNulls[j][i];
                hasSet = true;
                break;
            }
        }
        if (!hasSet) {
            finalResult[i] = elseValue[i];
            finalNull[i] = elseNull[i];
        }
    }
}

extern DLLEXPORT void switchExprString(int32_t whenCnt, int64_t *whenClauses, int64_t *whenBools, int64_t *resultValues,
    int64_t *resultNulls, int64_t *resultLengths, uint8_t **elseValue, bool *elseNull, int32_t *elseLength,
    uint8_t **finalResult, bool *finalNull, int32_t *finalLength, int32_t rowCnt);

#endif // OMNI_RUNTIME_BATCH_UTILFUNCTIONS_H
