/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Description: batch util functions implementation
 */

#include "batch_utilfunctions.h"
#include "type/decimal_operations.h"
#include "../functions/context_helper.h"

using namespace omniruntime::codegen;

extern "C" DLLEXPORT void fillRowIndexArray(int32_t *dataArray, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dataArray[i] = i;
    }
}

extern "C" DLLEXPORT void fillBool(bool *nullArray, bool isNull, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        nullArray[i] = isNull;
    }
}

extern "C" DLLEXPORT void fillInt32(int32_t *dataArray, bool *nullArray, int32_t literal, bool isNull, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dataArray[i] = literal;
        nullArray[i] = isNull;
    }
}

extern "C" DLLEXPORT void fillInt64(int64_t *dataArray, bool *nullArray, int64_t literal, bool isNull, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dataArray[i] = literal;
        nullArray[i] = isNull;
    }
}

extern "C" DLLEXPORT void fillDouble(double *dataArray, bool *nullArray, double literal, bool isNull, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dataArray[i] = literal;
        nullArray[i] = isNull;
    }
}

extern "C" DLLEXPORT void fillDecimal128(Decimal128 *dataArray, bool *nullArray, __int128_t literal, bool isNull,
    int32_t rowCnt)
{
    bool isNegative = literal < 0;
    std::abs(literal);
    uint64_t lowBits = ((__uint128_t)literal) & 0xFFFF'FFFF'FFFF'FFFF;
    int64_t highBits = ((__uint128_t)literal) >> 64;
    if (isNegative) {
        highBits = highBits | 0x8000'0000'0000'0000;
    }

    for (int i = 0; i < rowCnt; i++) {
        dataArray[i].SetValue(highBits, lowBits);
        nullArray[i] = isNull;
    }
}

extern "C" DLLEXPORT void fillString(int64_t contextPtr, uint8_t **dataArray, bool *nullArray, int32_t *lengthArray,
    uint8_t *literal, bool isNull, int32_t length, int32_t rowCnt)
{
    errno_t err;
    char *ret;
    for (int i = 0; i < rowCnt; i++) {
        ret = ArenaAllocatorMalloc(contextPtr, length);
        err = memcpy_s(ret, length, literal, length);
        dataArray[i] = reinterpret_cast<uint8_t *>(ret);
        nullArray[i] = isNull;
        lengthArray[i] = length;
    }
}

extern "C" DLLEXPORT void fillLength(int32_t *offsets, int32_t *rowIdxArray, int32_t *lengthArray, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        lengthArray[i] = offsets[rowIdxArray[i] + 1] - offsets[i];
    }
}

extern DLLEXPORT void createNot(bool *val, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        val[i] = !val[i];
    }
}

extern DLLEXPORT void createOr(bool *left, bool *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] || right[i];
    }
}

extern DLLEXPORT void createAnd(bool *left, bool *right, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        left[i] = left[i] && right[i];
    }
}

extern "C" DLLEXPORT void createAndNotBool(bool *dataArray, bool *nullArray, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dataArray[i] = dataArray[i] && (!nullArray[i]);
    }
}

extern "C" DLLEXPORT int32_t createAndNot(bool *dataArray, bool *nullArray, int32_t *rowIdxArray, int32_t rowCnt)
{
    int selectedCnt = 0;
    for (int i = 0; i < rowCnt; i++) {
        if (dataArray[i] && (!nullArray[i])) {
            rowIdxArray[selectedCnt] = i;
            selectedCnt++;
        }
    }
    return selectedCnt;
}

extern DLLEXPORT void createOrExpr(bool *left, bool *leftNull, bool *right, bool *rightNull, int32_t rowCnt)
{
    bool res;
    for (int i = 0; i < rowCnt; ++i) {
        res = left[i] || right[i];
        leftNull[i] = (leftNull[i] && rightNull[i]) || (leftNull[i] && !right[i]) || (rightNull[i] && !left[i]);
        left[i] = res;
    }
}

extern DLLEXPORT void createAndExpr(bool *left, bool *leftNull, bool *right, bool *rightNull, int32_t rowCnt)
{
    bool tmpVal;
    for (int i = 0; i < rowCnt; i++) {
        tmpVal = left[i] && right[i];
        leftNull[i] = (leftNull[i] && rightNull[i]) || (leftNull[i] && right[i]) || (left[i] && rightNull[i]);
        left[i] = tmpVal;
    }
}

extern "C" DLLEXPORT void BatchInInt32(int32_t *left, bool *leftNull, int32_t *right, bool *rightNull, int32_t *output,
    bool *outNull, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        outNull[i] = (leftNull[i] || rightNull[i]);
        output[i] = ((left[i] == right[i]) && (!outNull[i]));
    }
}

extern "C" DLLEXPORT void copyBoolean(bool *dataArray, bool *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dataArray[i] = output[i];
    }
}

extern "C" DLLEXPORT void copyInt32(int32_t *dataArray, int32_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dataArray[i] = output[i];
    }
}

extern "C" DLLEXPORT void copyInt64(int64_t *dataArray, int64_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dataArray[i] = output[i];
    }
}

extern "C" DLLEXPORT void copyDouble(double *dataArray, double *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dataArray[i] = output[i];
    }
}

extern "C" DLLEXPORT void copyDecimal128(__int128_t *dataArray, __int128_t *output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dataArray[i] = output[i];
    }
}

extern "C" DLLEXPORT void copyString(uint8_t **dataArray, uint8_t **output, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        dataArray[i] = output[i];
    }
}

extern "C" DLLEXPORT void ifExprStr(bool *ifCond, bool *ifNull, uint8_t **trueValue, bool *trueNull,
    int32_t *trueLength, uint8_t **falseValue, bool *falseNull, int32_t *falseLength, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (!(ifCond[i] && !ifNull[i])) {
            trueValue[i] = falseValue[i];
            trueNull[i] = falseNull[i];
            trueLength[i] = falseLength[i];
        }
    }
}

extern DLLEXPORT void switchExprString(int32_t whenCnt, int64_t *whenClauses, int64_t *whenBools, int64_t *resultValues,
    int64_t *resultNulls, int64_t *resultLengths, uint8_t **elseValue, bool *elseNull, int32_t *elseLength,
    uint8_t **finalResult, bool *finalNull, int32_t *finalLength, int32_t rowCnt)
{
    std::vector<bool *> whenValues;
    std::vector<bool *> whenNulls;

    std::vector<uint8_t **> resValues;
    std::vector<bool *> resNulls;
    std::vector<int32_t *> resLengths;

    for (int i = 0; i < whenCnt; ++i) {
        whenValues.push_back((bool *)whenClauses[i]);
        whenNulls.push_back((bool *)whenBools[i]);
        resValues.push_back((uint8_t **)resultValues[i]);
        resNulls.push_back((bool *)resultNulls[i]);
        resLengths.push_back((int32_t *)resultLengths[i]);
    }

    for (int i = 0; i < rowCnt; ++i) {
        bool hasSet = false;
        for (int j = 0; j < whenCnt; ++j) {
            if (whenValues[j][i] && !whenNulls[j][i]) {
                finalResult[i] = resValues[j][i];
                finalNull[i] = resNulls[j][i];
                finalLength[i] = resLengths[j][i];
                hasSet = true;
                break;
            }
        }
        if (!hasSet) {
            finalResult[i] = elseValue[i];
            finalNull[i] = elseNull[i];
            finalLength[i] = elseLength[i];
        }
    }
}

extern "C" DLLEXPORT void coalesceString(uint8_t **lArray, bool *lIsNull, int32_t *lLength, uint8_t **rArray,
    bool *rIsNull, int32_t *rLength, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        if (lIsNull[i] == true) {
            lArray[i] = rArray[i];
            lIsNull[i] = rIsNull[i];
            lLength[i] = rLength[i];
        }
    }
}

extern "C" DLLEXPORT void coalesceDecimal64(int64_t *lArray, bool *lIsNull, int32_t lPrecision, int32_t lScale,
                                             int64_t *rArray, bool *rIsNull, int32_t rPrecision, int32_t rScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        if (lIsNull[i] == true) {
            lArray[i] = rArray[i];
            lIsNull[i] = rIsNull[i];
            lPrecision = rPrecision;
            lScale = rScale;
        }
    }
}

extern "C" DLLEXPORT void coalesceDecimal128(Decimal128 *lArray, bool *lIsNull, int32_t lPrecision, int32_t lScale,
    Decimal128 *rArray, bool *rIsNull, int32_t rPrecision, int32_t rScale, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; i++) {
        if (lIsNull[i] == true) {
            lArray[i] = rArray[i];
            lIsNull[i] = rIsNull[i];
            lPrecision = rPrecision;
            lScale = rScale;
        }
    }
}
