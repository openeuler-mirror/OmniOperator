/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch function test
 */

#include "gtest/gtest.h"
#include "codegen/batch_functions/batch_mathfunctions.h"
#include "codegen/batch_functions/batch_murmur3_hash.h"
#include "operator/execution_context.h"

using namespace omniruntime::op;
using namespace omniruntime::codegen;
using namespace std;

template <typename T> bool CmpArray(T *x, T *y, int32_t rowCnt)
{
    for (int i = 0; i < rowCnt; ++i) {
        if (x[i] != y[i]) {
            return false;
        }
    }
    return true;
}

TEST(BatchFunctionTest, CastBasicTypes)
{
    int32_t rowCnt = 2;
    int32_t col1[2] = {-10, 10};
    int64_t col2[2] = {-20L, 20L};
    double col3[2] = {-30.0, 30.0};
    bool resIsNull[2] = {false, false};

    double outDouble[2];
    BatchCastInt32ToDouble(col1, resIsNull, outDouble, rowCnt);
    double expectDouble1[2] = {-10.0, 10.0};
    EXPECT_TRUE(CmpArray<double>(outDouble, expectDouble1, rowCnt));

    BatchCastInt64ToDouble(col2, resIsNull, outDouble, rowCnt);
    double expectDouble2[2] = {-20.0, 20.0};
    EXPECT_TRUE(CmpArray<double>(outDouble, expectDouble2, rowCnt));

    int32_t outInt[2];
    BatchCastInt64ToInt32(col2, resIsNull, outInt, rowCnt);
    int32_t expectInt1[2] = {-20, 20};
    EXPECT_TRUE(CmpArray<int32_t>(outInt, expectInt1, rowCnt));

    BatchCastDoubleToInt32(col3, resIsNull, outInt, rowCnt);
    int32_t expectInt2[2] = {-30, 30};
    EXPECT_TRUE(CmpArray<int32_t>(outInt, expectInt2, rowCnt));

    int64_t outLong[2];
    BatchCastInt32ToInt64(col1, resIsNull, outLong, rowCnt);
    int64_t expectLong1[2] = {-10L, 10L};
    EXPECT_TRUE(CmpArray<int64_t>(outLong, expectLong1, rowCnt));

    BatchCastDoubleToInt64(col3, resIsNull, outLong, rowCnt);
    int64_t expectLong2[2] = {-30L, 30L};
    EXPECT_TRUE(CmpArray<int64_t>(outLong, expectLong2, rowCnt));
}

TEST(BatchFunctionTest, Abs)
{
    int32_t rowCnt = 2;
    int32_t col1[2] = {-10, 10};
    int64_t col2[2] = {-20L, 20L};
    double col3[2] = {-30.0, 30.0};
    bool resIsNull[2] = {false, false};

    BatchAbs<int32_t>(col1, resIsNull, col1, rowCnt);
    BatchAbs<int64_t>(col2, resIsNull, col2, rowCnt);
    BatchAbs<double>(col3, resIsNull, col3, rowCnt);

    int32_t expectCol1[2] = {10, 10};
    int64_t expectCol2[2] = {20L, 20L};
    double expectCol3[2] = {30.0, 30.0};

    EXPECT_TRUE(CmpArray<int32_t>(col1, expectCol1, rowCnt));
    EXPECT_TRUE(CmpArray<int64_t>(col2, expectCol2, rowCnt));
    EXPECT_TRUE(CmpArray<double>(col3, expectCol3, rowCnt));
}

TEST(BatchFunctionTest, IntCmp)
{
    int32_t rowCnt = 2;
    int32_t left[2] = {-10, 20};
    int32_t right[2] = {-20, 10};
    bool output[2];
    bool expect[2];

    BatchLessThanInt32(left, right, output, rowCnt);
    expect[0] = false;
    expect[1] = false;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchLessThanEqualInt32(left, right, output, rowCnt);
    expect[0] = false;
    expect[1] = false;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchGreaterThanInt32(left, right, output, rowCnt);
    expect[0] = true;
    expect[1] = true;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchGreaterThanEqualInt32(left, right, output, rowCnt);
    expect[0] = true;
    expect[1] = true;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchEqualInt32(left, right, output, rowCnt);
    expect[0] = false;
    expect[1] = false;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchNotEqualInt32(left, right, output, rowCnt);
    expect[0] = true;
    expect[1] = true;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));
}

TEST(BatchFunctionTest, IntArith)
{
    int32_t rowCnt = 2;
    int32_t left[2] = {-10, 10};
    int32_t right[2] = {-20, 20};
    int32_t expect[2];
    bool isNull[2] = {false, false};
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);

    expect[0] = -30;
    expect[1] = 30;
    BatchAddInt32(left, right, rowCnt);
    EXPECT_TRUE(CmpArray<int32_t>(left, expect, rowCnt));

    expect[0] = -10;
    expect[1] = 10;
    BatchSubtractInt32(left, right, rowCnt);
    EXPECT_TRUE(CmpArray<int32_t>(left, expect, rowCnt));

    expect[0] = 200;
    expect[1] = 200;
    BatchMultiplyInt32(left, right, rowCnt);
    EXPECT_TRUE(CmpArray<int32_t>(left, expect, rowCnt));

    expect[0] = -10;
    expect[1] = 10;
    BatchDivideInt32(contextPtr, left, right, rowCnt, isNull);
    EXPECT_TRUE(CmpArray<int32_t>(left, expect, rowCnt));

    expect[0] = -10;
    expect[1] = 10;
    BatchModulusInt32(contextPtr, left, right, rowCnt, isNull);
    EXPECT_TRUE(CmpArray<int32_t>(left, expect, rowCnt));

    delete context;
}

TEST(BatchFunctionTest, LongCmp)
{
    int32_t rowCnt = 2;
    int64_t left[2] = {-10L, 20L};
    int64_t right[2] = {-20L, 10L};
    bool output[2];
    bool expect[2];

    BatchLessThanInt64(left, right, output, rowCnt);
    expect[0] = false;
    expect[1] = false;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchLessThanEqualInt64(left, right, output, rowCnt);
    expect[0] = false;
    expect[1] = false;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchGreaterThanInt64(left, right, output, rowCnt);
    expect[0] = true;
    expect[1] = true;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchGreaterThanEqualInt64(left, right, output, rowCnt);
    expect[0] = true;
    expect[1] = true;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchEqualInt64(left, right, output, rowCnt);
    expect[0] = false;
    expect[1] = false;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchNotEqualInt64(left, right, output, rowCnt);
    expect[0] = true;
    expect[1] = true;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));
}

TEST(BatchFunctionTest, LongArith)
{
    int32_t rowCnt = 2;
    int64_t left[2] = {-10L, 10L};
    int64_t right[2] = {-20L, 20L};
    int64_t expect[2];
    bool isNull[2] = {false, false};
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);

    expect[0] = -30L;
    expect[1] = 30L;
    BatchAddInt64(left, right, rowCnt);
    EXPECT_TRUE(CmpArray<int64_t>(left, expect, rowCnt));

    expect[0] = -10L;
    expect[1] = 10L;
    BatchSubtractInt64(left, right, rowCnt);
    EXPECT_TRUE(CmpArray<int64_t>(left, expect, rowCnt));

    expect[0] = 200L;
    expect[1] = 200L;
    BatchMultiplyInt64(left, right, rowCnt);
    EXPECT_TRUE(CmpArray<int64_t>(left, expect, rowCnt));

    expect[0] = -10L;
    expect[1] = 10L;
    BatchDivideInt64(contextPtr, left, right, rowCnt, isNull);
    EXPECT_TRUE(CmpArray<int64_t>(left, expect, rowCnt));

    expect[0] = -10L;
    expect[1] = 10L;
    BatchModulusInt64(contextPtr, left, right, rowCnt, isNull);
    EXPECT_TRUE(CmpArray<int64_t>(left, expect, rowCnt));

    delete context;
}

TEST(BatchFunctionTest, DoubleCmp)
{
    int32_t rowCnt = 2;
    double left[2] = {-10.0, 20.0};
    double right[2] = {-20.0, 10.0};
    bool output[2];
    bool expect[2];

    BatchLessThanDouble(left, right, output, rowCnt);
    expect[0] = false;
    expect[1] = false;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchLessThanEqualDouble(left, right, output, rowCnt);
    expect[0] = false;
    expect[1] = false;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchGreaterThanDouble(left, right, output, rowCnt);
    expect[0] = true;
    expect[1] = true;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchGreaterThanEqualDouble(left, right, output, rowCnt);
    expect[0] = true;
    expect[1] = true;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchEqualDouble(left, right, output, rowCnt);
    expect[0] = false;
    expect[1] = false;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));

    BatchNotEqualDouble(left, right, output, rowCnt);
    expect[0] = true;
    expect[1] = true;
    EXPECT_TRUE(CmpArray<bool>(output, expect, rowCnt));
}

TEST(BatchFunctionTest, DoubleArith)
{
    int32_t rowCnt = 2;
    double left[2] = {-10.0, 10.0};
    double right[2] = {-20.0, 20.0};
    double expect[2];

    expect[0] = -30.0;
    expect[1] = 30.0;
    BatchAddDouble(left, right, rowCnt);
    EXPECT_TRUE(CmpArray<double>(left, expect, rowCnt));

    expect[0] = -10.0;
    expect[1] = 10.0;
    BatchSubtractDouble(left, right, rowCnt);
    EXPECT_TRUE(CmpArray<double>(left, expect, rowCnt));

    expect[0] = 200.0;
    expect[1] = 200.0;
    BatchMultiplyDouble(left, right, rowCnt);
    EXPECT_TRUE(CmpArray<double>(left, expect, rowCnt));

    expect[0] = -10.0;
    expect[1] = 10.0;
    BatchDivideDouble(left, right, rowCnt);
    EXPECT_TRUE(CmpArray<double>(left, expect, rowCnt));

    expect[0] = -10.0;
    expect[1] = 10.0;
    BatchModulusDouble(left, right, rowCnt);
    EXPECT_TRUE(CmpArray<double>(left, expect, rowCnt));
}

TEST(BatchFunctionTest, Mm3Hash)
{
    int32_t rowCnt = 1;
    int32_t intVal[1] = {-2147483648};
    int64_t longVal[1] = {-2147483648L};
    double doubleVal[1] = {123.456};
    int64_t decimal64Val[1] = {-2147483648L};
    uint8_t *strVal[1];
    strVal[0] = (uint8_t *) "hello world";
    int32_t strLen[1] = {11};
    Decimal128 decimal128Val[1];
    decimal128Val[0].SetValue(0, 4000);

    int32_t seed[1] = {42};
    bool isValNull[1] = {false};
    bool isSeedNull[1] = {false};
    bool resIsNull[1] = {false};
    int32_t output[1];

    BatchMm3Int32(intVal, isValNull, seed, isSeedNull, resIsNull, output, rowCnt);
    EXPECT_EQ(output[0], 723455942);

    BatchMm3Int64(longVal, isValNull, seed, isSeedNull, resIsNull, output, rowCnt);
    EXPECT_EQ(output[0], -1889108749);

    BatchMm3Double(doubleVal, isValNull, seed, isSeedNull, resIsNull, output, rowCnt);
    EXPECT_EQ(output[0], -39269148);

    BatchMm3String(strVal, strLen, isValNull, seed, isSeedNull, resIsNull, output, rowCnt);
    EXPECT_EQ(output[0], -1528836094);

    BatchMm3Decimal64(decimal64Val, 8, 2, isValNull, seed, isSeedNull, resIsNull, output, rowCnt);
    EXPECT_EQ(output[0], -1889108749);

    BatchMm3Decimal128(decimal128Val, 38, 20, isValNull, seed, isSeedNull, resIsNull, output, rowCnt);
    EXPECT_EQ(output[0], 308064329);
}
