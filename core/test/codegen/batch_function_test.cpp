/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch function test
 */
#include <string>
#include <vector>
#include "gtest/gtest.h"
#include "codegen/batch_functions/batch_mathfunctions.h"
#include "codegen/batch_functions/batch_murmur3_hash.h"
#include "codegen/batch_functions/batch_decimalfunctions.h"
#include "operator/execution_context.h"
#include "engine.h"
#include "codegen/batch_functions/batch_stringfunctions.h"
#include "../util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::codegen;
using namespace std;
using namespace TestUtil;

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
    string str = "hello world";
    strVal[0] = reinterpret_cast<uint8_t *>(const_cast<char *>(str.c_str()));
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


TEST(BatchFunctionTest, Decimal64Cmp)
{
    int32_t rowCnt = 2;
    int64_t col1[2] = {12345678L, 123456L};
    int64_t col2[2] = {12345678L, 1234567890L};
    bool output[2];

    BatchLessThanDecimal64(col1, 8, 2, col2, 9, 2, output, rowCnt);
    EXPECT_EQ(output[0], false);
    EXPECT_EQ(output[1], true);

    BatchLessThanEqualDecimal64(col1, 8, 3, col2, 9, 2, output, rowCnt);
    EXPECT_EQ(output[0], true);
    EXPECT_EQ(output[1], true);

    BatchGreaterThanDecimal64(col1, 8, 6, col2, 9, 2, output, rowCnt);
    EXPECT_EQ(output[0], false);
    EXPECT_EQ(output[1], false);

    BatchGreaterThanEqualDecimal64(col1, 8, 2, col2, 9, 2, output, rowCnt);
    EXPECT_EQ(output[0], true);
    EXPECT_EQ(output[1], false);

    BatchEqualDecimal64(col1, 8, 2, col2, 9, 2, output, rowCnt);
    EXPECT_EQ(output[0], true);
    EXPECT_EQ(output[1], false);

    BatchNotEqualDecimal64(col1, 8, 2, col2, 9, 2, output, rowCnt);
    EXPECT_EQ(output[0], false);
    EXPECT_EQ(output[1], true);
}

TEST(BatchFunctionTest, Decimal128Cmp)
{
    int32_t rowCnt = 2;
    Decimal128 col1[2];
    col1[0].SetValue(1234567L, UINT64_MAX);
    col1[1].SetValue(123456L, UINT64_MAX);
    Decimal128 col2[2];
    col2[0].SetValue(-1234567L, UINT64_MAX);
    col2[1].SetValue(1234567L, UINT64_MAX);
    bool output[2];

    BatchLessThanDecimal128(col1, 38, 2, col2, 38, 2, output, rowCnt);
    EXPECT_EQ(output[0], false);
    EXPECT_EQ(output[1], true);

    BatchLessThanEqualDecimal128(col1, 38, 4, col2, 38, 3, output, rowCnt);
    EXPECT_EQ(output[0], false);
    EXPECT_EQ(output[1], true);

    BatchGreaterThanDecimal128(col1, 38, 6, col2, 38, 2, output, rowCnt);
    EXPECT_EQ(output[0], true);
    EXPECT_EQ(output[1], false);

    BatchGreaterThanEqualDecimal128(col1, 38, 2, col2, 38, 4, output, rowCnt);
    EXPECT_EQ(output[0], true);
    EXPECT_EQ(output[1], true);

    BatchEqualDecimal128(col1, 38, 2, col2, 38, 2, output, rowCnt);
    EXPECT_EQ(output[0], false);
    EXPECT_EQ(output[1], false);

    BatchNotEqualDecimal128(col1, 38, 2, col2, 38, 2, output, rowCnt);
    EXPECT_EQ(output[0], true);
    EXPECT_EQ(output[1], true);
}

TEST(BatchFunctionTest, CastDecimalToDecimal)
{
    int32_t rowCnt = 2;
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);

    int64_t decimal64Val[2] = {1234567L, INT64_MAX};
    Decimal128 decimal128Val[2];
    decimal128Val[0].SetValue(0, 1234567);
    decimal128Val[1].SetValue(1LL << 63, 123456);
    bool isAnyNull[2] = {false, false};
    bool overflowNull[2] = {false, false};
    int64_t output64[2];
    Decimal128 output128[2];

    BatchCastDecimal64To64(contextPtr, decimal64Val, 7, 2, isAnyNull, output64, 10, 3, rowCnt);
    EXPECT_EQ(output64[0], 12345670L);
    EXPECT_TRUE(context->HasError());

    BatchCastDecimal64To64RetNull(overflowNull, decimal64Val, 7, 2, output64, 10, 3, rowCnt);
    EXPECT_EQ(output64[0], 12345670L);
    EXPECT_FALSE(overflowNull[0]);
    EXPECT_TRUE(overflowNull[1]);

    BatchCastDecimal128To128(contextPtr, decimal128Val, 20, 10, isAnyNull, output128, 38, 18, rowCnt);
    EXPECT_EQ(output128[0].HighBits(), 0);
    EXPECT_EQ(output128[0].LowBits(), 123456700000000L);
    EXPECT_EQ(output128[1].HighBits(), 1LL << 63);
    EXPECT_EQ(output128[1].LowBits(), 12345600000000L);

    overflowNull[0] = false;
    overflowNull[1] = false;
    BatchCastDecimal128To128RetNull(overflowNull, decimal128Val, 20, 10, output128, 38, 18, rowCnt);
    EXPECT_FALSE(overflowNull[0]);
    EXPECT_FALSE(overflowNull[1]);

    BatchCastDecimal64To128(contextPtr, decimal64Val, 18, 18, isAnyNull, output128, 38, 20, rowCnt);
    EXPECT_EQ(output128[0].HighBits(), 0);
    EXPECT_EQ(output128[0].LowBits(), 123456700L);
    EXPECT_EQ(output128[1].HighBits(), 49);
    EXPECT_EQ(output128[1].LowBits(), 18446744073709551516UL);

    BatchCastDecimal64To128RetNull(overflowNull, decimal64Val, 18, 18, output128, 38, 15, rowCnt);
    EXPECT_EQ(output128[0].HighBits(), 0);
    EXPECT_EQ(output128[0].LowBits(), 1235);
    EXPECT_EQ(output128[1].HighBits(), 0);
    EXPECT_EQ(output128[1].LowBits(), std::round(INT64_MAX / 1000));

    BatchCastDecimal128To64(contextPtr, decimal128Val, 38, 10, isAnyNull, output64, 18, 9, rowCnt);
    EXPECT_EQ(output64[0], 123457);
    EXPECT_EQ(output64[1], -12346);

    overflowNull[0] = false;
    overflowNull[1] = false;
    BatchCastDecimal128To64RetNull(overflowNull, decimal128Val, 38, 3, output64, 5, 2, rowCnt);
    EXPECT_TRUE(overflowNull[0]);
    EXPECT_EQ(output64[1], -12346);

    delete context;
}

TEST(BatchFunctionTest, CastBasicTypeToDecimal)
{
    int32_t rowCnt = 1;
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int32_t intVal[1] = {1234567};
    int64_t longVal[1] = {INT32_MAX};
    double doubleVal[1] = {99999999.99};
    bool isAnyNull[1] = {false};
    bool overflowNull[1] = {false};
    int64_t output64[1];
    Decimal128 output128[1];

    BatchCastIntToDecimal64(contextPtr, intVal, isAnyNull, output64, 9, 2, rowCnt);
    EXPECT_EQ(output64[0], 123456700);
    BatchCastIntToDecimal128(contextPtr, intVal, isAnyNull, output128, 38, 9, rowCnt);
    EXPECT_EQ(output128[0].HighBits(), 0);
    EXPECT_EQ(output128[0].LowBits(), 1'234'567'000'000'000);
    BatchCastIntToDecimal64RetNull(overflowNull, intVal, output64, 6, 0, rowCnt);
    EXPECT_TRUE(overflowNull[0]);
    overflowNull[0] = false;
    BatchCastIntToDecimal128RetNull(overflowNull, intVal, output128, 18, 12, rowCnt);
    EXPECT_TRUE(overflowNull[0]);

    overflowNull[0] = false;
    BatchCastLongToDecimal64(contextPtr, longVal, isAnyNull, output64, 13, 2, rowCnt);
    EXPECT_EQ(output64[0], INT_MAX * 100L);
    BatchCastLongToDecimal128(contextPtr, longVal, isAnyNull, output128, 38, 5, rowCnt);
    EXPECT_EQ(output128[0].HighBits(), 0);
    EXPECT_EQ(output128[0].LowBits(), INT_MAX * 100'000L);
    BatchCastLongToDecimal64RetNull(overflowNull, longVal, output64, 9, 0, rowCnt);
    EXPECT_TRUE(overflowNull[0]);
    BatchCastLongToDecimal128RetNull(overflowNull, longVal, output128, 38, 6, rowCnt);
    EXPECT_EQ(output128[0].HighBits(), 0);
    EXPECT_EQ(output128[0].LowBits(), INT_MAX * 1000'000L);

    overflowNull[0] = false;
    BatchCastDoubleToDecimal64(contextPtr, doubleVal, isAnyNull, output64, 13, 2, rowCnt);
    EXPECT_EQ(output64[0], 9'999'999'999L);
    BatchCastDoubleToDecimal128(contextPtr, doubleVal, isAnyNull, output128, 38, 5, rowCnt);
    EXPECT_EQ(output128[0].HighBits(), 0);
    EXPECT_EQ(output128[0].LowBits(), 9'999'999'999L * 1000L);
    BatchCastDoubleToDecimal64RetNull(overflowNull, doubleVal, output64, 12, 0, rowCnt);
    EXPECT_EQ(output64[0], 100'000'000L);
    BatchCastDoubleToDecimal128RetNull(overflowNull, doubleVal, output128, 19, 18, rowCnt);
    EXPECT_TRUE(overflowNull[0]);

    delete context;
}

TEST(BatchFunctionTest, CastDecimalToBasicType)
{
    int32_t rowCnt = 1;
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int64_t decimal64Val[1] = {1234567};
    Decimal128 decimal128Val[1];
    decimal128Val[0].SetValue(0, INT64_MAX);
    int32_t outputInt[1];
    int64_t outputLong[1];
    double outputDouble[1];
    bool isAnyNull[1] = {false};
    bool overflowNull[1] = {false};

    BatchCastDecimal64ToInt(contextPtr, decimal64Val, 7, 2, isAnyNull, outputInt, rowCnt);
    EXPECT_EQ(outputInt[0], 12346);
    BatchCastDecimal64ToLong(decimal64Val, 7, 2, isAnyNull, outputLong, rowCnt);
    EXPECT_EQ(outputLong[0], 12346L);
    BatchCastDecimal64ToDouble(decimal64Val, 7, 2, isAnyNull, outputDouble, rowCnt);
    EXPECT_EQ(outputDouble[0], 12345.67);

    BatchCastDecimal128ToInt(contextPtr, decimal128Val, 19, 2, isAnyNull, outputInt, rowCnt);
    EXPECT_TRUE(context->HasError());
    BatchCastDecimal128ToLong(contextPtr, decimal128Val, 19, 2, isAnyNull, outputLong, rowCnt);
    EXPECT_EQ(outputLong[0], INT64_MAX / 100L);
    BatchCastDecimal128ToDouble(decimal128Val, 19, 2, isAnyNull, outputDouble, rowCnt);
    EXPECT_EQ(outputDouble[0], INT64_MAX / (double)100);

    BatchCastDecimal64ToIntRetNull(overflowNull, decimal64Val, 7, 2, outputInt, rowCnt);
    EXPECT_EQ(outputInt[0], 12346);
    BatchCastDecimal64ToLongRetNull(overflowNull, decimal64Val, 7, 2, outputLong, rowCnt);
    EXPECT_EQ(outputLong[0], 12346L);
    BatchCastDecimal64ToDoubleRetNull(overflowNull, decimal64Val, 7, 2, outputDouble, rowCnt);
    EXPECT_EQ(outputDouble[0], 12345.67);

    BatchCastDecimal128ToIntRetNull(overflowNull, decimal128Val, 7, 2, outputInt, rowCnt);
    EXPECT_TRUE(overflowNull[0]);
    BatchCastDecimal128ToLongRetNull(overflowNull, decimal128Val, 7, 2, outputLong, rowCnt);
    EXPECT_EQ(outputLong[0], INT64_MAX / 100L);
    BatchCastDecimal128ToDoubleRetNull(overflowNull, decimal128Val, 7, 2, outputDouble, rowCnt);
    EXPECT_EQ(outputDouble[0], INT64_MAX / (double)100);

    delete context;
}

TEST(BatchFunctionTest, MakeDecimal)
{
    int32_t rowCnt = 1;
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int64_t col[1] = {12345678L};
    int64_t output[1];
    bool isAnyNull[1] = {false};
    bool overflowNull[1] = {false};

    BatchUnscaledValue64(col, 7, 2, isAnyNull, output, rowCnt);
    EXPECT_EQ(output[0], 12345678L);

    BatchMakeDecimal64(contextPtr, col, isAnyNull, output, 7, 2, rowCnt);
    EXPECT_TRUE(context->HasError());

    BatchMakeDecimal64RetNull(overflowNull, col, output, 7, 2, rowCnt);
    EXPECT_TRUE(overflowNull[0]);

    delete context;
}

TEST(BatchFunctionTest, SubstrZh)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    auto strLen = static_cast<int32_t>(str.length());

    std::vector<std::string> inputStr(8, str);
    std::vector<int32_t> inputLen(8, strLen);
    std::vector<int32_t> startIndexs { 1, 1, 10, -5, 0, 37, -38, -37 };
    std::vector<int32_t> length { 37, 5, 10, 7, 0, strLen + 5, 10, 37 };
    int32_t rowCnt = inputStr.size();
    std::vector<int32_t> outLen(rowCnt);
    std::vector<uint8_t *> outResult(rowCnt);
    std::vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }
    bool isAnyNull[] = {false, false, false, false, false, false, false, false};
    BatchSubstr(contextPtr, strAddr.data(), inputLen.data(), startIndexs.data(), length.data(), isAnyNull,
        outResult.data(), outLen.data(), rowCnt);

    std::vector<std::string> expected { str, "时欧基乌斯", "hello! 回复哦", "色的圣诞袜", "", "袜", "", str };
    AssertStringEquals(expected, outResult, outLen);

    delete context;
}

TEST(BatchFunctionTest, SubstrCharZh)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t width = 37;
    int32_t strLen = str.length();

    std::vector<std::string> inputStr(8, str);
    std::vector<int32_t> inputLen(8, strLen);
    std::vector<int32_t> startIndexs { 1, 1, 10, -5, 0, 37, -38, -37 };
    std::vector<int32_t> length { 37, 5, 10, 7, 0, strLen + 5, 10, 37 };
    int32_t rowCnt = inputStr.size();
    std::vector<int32_t> outLen(rowCnt);
    std::vector<uint8_t *> outResult(rowCnt);
    std::vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false, false, false, false, false, false, false};
    BatchSubstrChar(contextPtr, strAddr.data(), width, inputLen.data(), startIndexs.data(), length.data(), isAnyNull,
        outResult.data(), outLen.data(), rowCnt);

    std::vector<std::string> expected { str, "时欧基乌斯", "hello! 回复哦", "色的圣诞袜", "", "袜", "", str };
    AssertStringEquals(expected, outResult, outLen);

    delete context;
}

TEST(BatchFunctionTest, SubstrWithStartZh)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    auto strLen = static_cast<int32_t>(str.length());

    std::vector<std::string> inputStr(7, str);
    std::vector<int32_t> inputLen(7, strLen);
    std::vector<int32_t> startIndexs { 1, 9, -3, 0, 37, -38, -37 };
    int32_t rowCnt = inputStr.size();
    std::vector<int32_t> outLen(rowCnt);
    std::vector<uint8_t *> outResult(rowCnt);
    std::vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false, false, false, false, false, false};
    BatchSubstrWithStart(contextPtr, strAddr.data(), inputLen.data(), startIndexs.data(), isAnyNull, outResult.data(),
        outLen.data(), rowCnt);

    std::vector<std::string> expected { str, " hello! 回复哦黑色的and magic粉色的圣诞袜", "圣诞袜", "", "袜", "", str };
    AssertStringEquals(expected, outResult, outLen);

    delete context;
}

TEST(BatchFunctionTest, SubstrWithStartZhForSpark)
{
    std::string engineType("Spark");
    EngineUtil::GetInstance().SetEngineType(const_cast<char *>(engineType.c_str()));
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::string str = "时欧基乌斯侧后解 h";
    auto strLen = static_cast<int32_t>(str.length());

    std::vector<std::string> inputStr(1, str);
    std::vector<int32_t> inputLen(1, strLen);
    std::vector<int32_t> outLen(inputStr.size());
    std::vector<uint8_t *> outResult(inputStr.size());
    std::vector<int32_t> startIndexs { -15 };
    int32_t rowCnt = inputStr.size();
    std::vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull[] = {false};
    BatchSubstrWithStart(contextPtr, strAddr.data(), inputLen.data(), startIndexs.data(), isAnyNull, outResult.data(),
        outLen.data(), rowCnt);

    std::vector<std::string> expected(1, str);
    AssertStringEquals(expected, outResult, outLen);

    engineType = "OLK";
    EngineUtil::GetInstance().SetEngineType(const_cast<char *>(engineType.c_str()));
    delete context;
}

TEST(BatchFunctionTest, SubstrWithZhForSpark)
{
    std::string engineType("Spark");
    EngineUtil::GetInstance().SetEngineType(const_cast<char *>(engineType.c_str()));
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::string str = "时欧基乌斯侧后解 h";
    auto strLen = static_cast<int32_t>(str.length());

    std::vector<std::string> inputStr(4, str);
    int32_t rowCnt = inputStr.size();
    std::vector<int32_t> inputLen(4, strLen);
    std::vector<int32_t> outLen(inputStr.size());
    std::vector<uint8_t *> outResult(inputStr.size());
    std::vector<int32_t> startIndexs { -15, -15, -15, -15 };
    std::vector<int32_t> length { 5, 6, 14, 20 };
    std::vector<uint8_t *> strAddr(rowCnt);

    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull1[] = {false, false, false, false};
    BatchSubstr(contextPtr, strAddr.data(), inputLen.data(), startIndexs.data(), length.data(), isAnyNull1,
        outResult.data(), outLen.data(), rowCnt);
    std::vector<std::string> expected { "", "时", "时欧基乌斯侧后解 ", "时欧基乌斯侧后解 h" };
    AssertStringEquals(expected, outResult, outLen);

    engineType = "OLK";
    EngineUtil::GetInstance().SetEngineType(const_cast<char *>(engineType.c_str()));
    delete context;
}

TEST(BatchFunctionTest, SubstrCharWithStartZh)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    auto width = static_cast<int32_t>(str.length());
    int32_t strLen = str.length();

    std::vector<std::string> inputStr(7, str);
    std::vector<int32_t> inputLen(7, strLen);
    std::vector<int32_t> outLen(inputStr.size());
    std::vector<uint8_t *> outResult(inputStr.size());
    std::vector<int32_t> startIndexs { 1, 9, -3, 0, 37, -38, -37 };
    int32_t rowCnt = inputStr.size();
    std::vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false, false, false, false, false, false};
    BatchSubstrCharWithStart(contextPtr, strAddr.data(), width, inputLen.data(), startIndexs.data(), isAnyNull,
        outResult.data(), outLen.data(), rowCnt);

    std::vector<std::string> expected { str, " hello! 回复哦黑色的and magic粉色的圣诞袜", "圣诞袜", "", "袜", "", str };
    AssertStringEquals(expected, outResult, outLen);

    delete context;
}

TEST(BatchFunctionTest, LengthStrZh)
{
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t strLen = str.length();

    std::vector<std::string> inputStr { str, "解 hello! 回复哦黑色的" };
    std::vector<int32_t> inputLen { strLen, 29 };
    std::vector<int64_t> outLen(inputStr.size());
    int32_t rowCnt = inputStr.size();
    std::vector<uint8_t *> strAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false};
    BatchLengthStr(strAddr.data(), inputLen.data(), isAnyNull, outLen.data(), rowCnt);
    std::vector<int64_t> expected { 37, 15 };
    AssertLongEquals(expected, outLen);
}


TEST(BatchFunctionTest, LikeStrZh)
{
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t strLen = str.length();

    std::vector<std::string> inputStr(4, str);
    std::vector<int32_t> inputLen(4, strLen);
    std::vector<std::string> patternStr { "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞.$",
        "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞..$",
        "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣.*$",
        "^欧时基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞.*$" };
    int32_t rowCnt = inputStr.size();
    bool output[rowCnt];
    std::vector<uint8_t *> strAddr(rowCnt);
    std::vector<uint8_t *> patternAddr(rowCnt);
    std::vector<int32_t> patternLen(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str()));
        patternAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(patternStr[i].c_str()));
        patternLen[i] = static_cast<int32_t>(patternStr[i].length());
    }

    bool isAnyNull[] = {false, false, false, false};
    BatchLikeStr(strAddr.data(), inputLen.data(), patternAddr.data(), patternLen.data(), isAnyNull, output, rowCnt);

    std::vector<bool> expected { true, false, true, false };
    AssertBoolEquals(expected, output);
}

TEST(BatchFunctionTest, LikeCharZh)
{
    std::vector<std::string> inputStr { "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜", "时欧基乌",
        "时欧基乌", "时欧基乌" };
    std::vector<std::string> patternStr { "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞.$", "^时欧基乌..$",
        "^时欧基乌.$", "^时欧基乌.*$" };
    int32_t batch = 4;
    std::vector<bool> expected { true, true, false, true };
    std::vector<int32_t> width { 37, 6, 6, 6 };
    for (int32_t i = 0; i < batch; i++) {
        int32_t rowCnt = 1;
        std::vector<int32_t> inputLen { static_cast<int32_t>(inputStr[i].length()) };
        bool output[rowCnt];
        std::vector<uint8_t *> strAddr { reinterpret_cast<uint8_t *>(const_cast<char *>(inputStr[i].c_str())) };
        std::vector<uint8_t *> patternAddr { reinterpret_cast<uint8_t *>(const_cast<char *>(patternStr[i].c_str())) };
        std::vector<int32_t> patternLen { static_cast<int32_t>(patternStr[i].length()) };

        bool isAnyNull[] = {false};
        BatchLikeChar(strAddr.data(), width[i], inputLen.data(), patternAddr.data(), patternLen.data(), isAnyNull,
            output, rowCnt);
        EXPECT_EQ(output[0], expected[i]);
    }
}

TEST(BatchFunctionTest, ReplaceStrStrStrZh)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> str { "apple", "粉色的圣诞袜", "粉色de圣诞袜", "粉色de圣诞袜", "粉色de圣诞袜",
        "",      "粉色de圣诞袜" };
    std::vector<int32_t> strLen { 5, 18, 17, 17, 17, 0, 17 };
    std::vector<std::string> searchStr { "", "", "", "", "de圣", "", "" };
    std::vector<int32_t> searchLen { 0, 0, 0, 0, 5, 0, 0 };
    std::vector<std::string> replaceStr { "*w*", "*w*", "*w*", "*的*", "*的*", "", "" };
    std::vector<int32_t> replaceLen { 3, 3, 3, 5, 5, 0, 0 };
    int32_t rowCnt = str.size();
    std::vector<uint8_t *> output(rowCnt);
    std::vector<int32_t> outLen(rowCnt);
    std::vector<uint8_t *> strAddr(rowCnt);
    std::vector<uint8_t *> searchStrAddr(rowCnt);
    std::vector<uint8_t *> replaceStrAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(str[i].c_str()));
        searchStrAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(searchStr[i].c_str()));
        replaceStrAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(replaceStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false, false, false, false, false, false };
    BatchReplaceStrStrStrWithRep(contextPtr, strAddr.data(), strLen.data(), searchStrAddr.data(), searchLen.data(),
        replaceStrAddr.data(), replaceLen.data(), isAnyNull, output.data(), outLen.data(), rowCnt);
    std::vector<std::string> expected { "*w*a*w*p*w*p*w*l*w*e*w*",
        "*w*粉*w*色*w*的*w*圣*w*诞*w*袜*w*",
        "*w*粉*w*色*w*d*w*e*w*圣*w*诞*w*袜*w*",
        "*的*粉*的*色*的*d*的*e*的*圣*的*诞*的*袜*的*",
        "粉色*的*诞袜",
        "",
        "粉色de圣诞袜" };
    AssertStringEquals(expected, output, outLen);
    delete context;
}

TEST(BatchFunctionTest, ReplaceWithoutRepZh)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> str { "apple", "apple", "粉色的圣诞袜", "粉色的圣诞袜", "粉色de圣诞袜", "粉色de圣诞袜" };
    std::vector<int32_t> strLen { 5, 5, 18, 18, 17, 17 };
    std::vector<std::string> searchStr { "", "pp", "", "圣诞", "", "色de" };
    std::vector<int32_t> searchLen { 0, 2, 0, 6, 0, 5 };
    int32_t rowCnt = str.size();
    std::vector<uint8_t *> output(rowCnt);
    std::vector<int32_t> outLen(rowCnt);
    std::vector<uint8_t *> strAddr(rowCnt);
    std::vector<uint8_t *> searchStrAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        strAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(str[i].c_str()));
        searchStrAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(searchStr[i].c_str()));
    }

    bool isAnyNull[] = {false, false, false, false, false, false};
    BatchReplaceStrStrWithoutRep(contextPtr, strAddr.data(), strLen.data(), searchStrAddr.data(), searchLen.data(),
        isAnyNull, output.data(), outLen.data(), rowCnt);
    std::vector<std::string> expected {
        "apple", "ale", "粉色的圣诞袜", "粉色的袜", "粉色de圣诞袜", "粉圣诞袜",
    };
    AssertStringEquals(expected, output, outLen);
    delete context;
}

TEST(BatchFunctionTest, ConcatStrStrZh)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> ap { "你是Chinese?", "", "pink圣诞袜" };
    std::vector<int32_t> apLen { 14, 0, 13 };
    std::vector<std::string> bp { "Yes我是", "粉色de圣诞袜", "" };
    std::vector<int32_t> bpLen { 9, 17, 0 };
    int32_t rowCnt = ap.size();
    std::vector<uint8_t *> output(rowCnt);
    std::vector<int32_t> outLen(rowCnt);
    std::vector<uint8_t *> apAddr(rowCnt);
    std::vector<uint8_t *> bpAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        apAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[i].c_str()));
        bpAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[i].c_str()));
    }
    bool isAnyNull[] = {false, false, false};
    BatchConcatStrStr(contextPtr, apAddr.data(), apLen.data(), bpAddr.data(), bpLen.data(), isAnyNull, output.data(),
        outLen.data(), rowCnt);
    std::vector<std::string> expected { "你是Chinese?Yes我是", "粉色de圣诞袜", "pink圣诞袜" };
    AssertStringEquals(expected, output, outLen);
    delete context;
}

TEST(BatchFunctionTest, ConcatCharCharZh)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> ap { "粉色de圣诞袜", "*黑色*",      "Hei你好吗",    "Oh我很好",
        "Hei你好吗   ", "Oh我很好  ",  "   Hei你好吗", "   Oh我很好",
        "Hei   你好吗", "Oh   我很好", "   ",          "Oh我很好",
        "Hei你好吗",    "   ",         "Hei你好吗",    "" };
    std::vector<int32_t> aWidth { 7, 8, 10, 12, 12, 5, 8, 8 };
    std::vector<std::string> bp { "*黑色*",      "粉色de",      "Oh我很好",     "Hei你好吗",   "Oh我很好  ",
        "Hei你好吗  ", "   Oh我很好", "   Hei你好吗", "Oh   我很好", "Hei   你好",
        "Oh我很好   ", "   ",         "   ",          "Hei你好吗",   "",
        "Hei你好" };
    std::vector<int32_t> bWidth { 4, 8, 8, 12, 8, 12, 5, 5 };
    std::vector<std::string> expected { "粉色de圣诞袜*黑色*", "*黑色*   粉色de",
        "Hei你好吗  Oh我很好", "Oh我很好   Hei你好吗",
        "Hei你好吗    Oh我很好  ", "Oh我很好     Hei你好吗  ",
        "   Hei你好吗      Oh我很好", "   Oh我很好       Hei你好吗",
        "Hei   你好吗   Oh   我很好", "Oh   我很好    Hei   你好",
        "     Oh我很好   ", "Oh我很好   ",
        "Hei你好吗     ", "        Hei你好吗",
        "Hei你好吗", "        Hei你好" };
    int32_t batch = 8;
    int32_t rowCnt = 2;
    for (int32_t i = 0; i < batch; i++) {
        std::vector<uint8_t *> output(rowCnt);
        std::vector<int32_t> outLen(rowCnt);
        std::vector<int32_t> apLen(rowCnt);
        std::vector<int32_t> bpLen(rowCnt);
        std::vector<uint8_t *> apAddr(rowCnt);
        std::vector<uint8_t *> bpAddr(rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            int32_t index = i * rowCnt + row;
            apLen[row] = static_cast<int32_t>(ap[index].length());
            bpLen[row] = static_cast<int32_t>(bp[index].length());
            apAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[index].c_str()));
            bpAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[index].c_str()));
        }
        bool isAnyNull[] = {false, false};
        BatchConcatCharChar(contextPtr, apAddr.data(), aWidth[i], apLen.data(), bpAddr.data(), bWidth[i], bpLen.data(),
            isAnyNull, output.data(), outLen.data(), rowCnt);
        AssertStringEquals(expected, i * rowCnt, rowCnt, output, outLen);
    }
    delete context;
}

TEST(BatchFunctionTest, ConcatCharStrZh)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> ap { "*你是谁呢*", "我很OK", "*你是谁呢*", "我很OK", "*你是谁呢*", "" };
    std::vector<int32_t> aWidth { 6, 10, 10 };
    std::vector<std::string> bp { "我很OK", "*你是谁呢*", "我很OK", "*你是谁呢*", "", "*你是谁呢*" };
    std::vector<std::string> expected { "*你是谁呢*我很OK",       "我很OK  *你是谁呢*", "*你是谁呢*    我很OK",
        "我很OK      *你是谁呢*", "*你是谁呢*",         "          *你是谁呢*" };
    int32_t batch = 3;
    int32_t rowCnt = 2;
    for (int32_t i = 0; i < batch; i++) {
        std::vector<uint8_t *> output(rowCnt);
        std::vector<int32_t> outLen(rowCnt);
        std::vector<int32_t> apLen(rowCnt);
        std::vector<int32_t> bpLen(rowCnt);
        std::vector<uint8_t *> apAddr(rowCnt);
        std::vector<uint8_t *> bpAddr(rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            int32_t index = i * rowCnt + row;
            apLen[row] = static_cast<int32_t>(ap[index].length());
            bpLen[row] = static_cast<int32_t>(bp[index].length());
            apAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[index].c_str()));
            bpAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[index].c_str()));
        }

        bool isAnyNull[] = {false, false};
        BatchConcatCharStr(contextPtr, apAddr.data(), aWidth[i], apLen.data(), bpAddr.data(), bpLen.data(), isAnyNull,
            output.data(), outLen.data(), rowCnt);
        AssertStringEquals(expected, i * rowCnt, rowCnt, output, outLen);
    }
    delete context;
}

TEST(BatchFunctionTest, ConcatStrCharZh)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> ap { "粉色de圣诞袜", "*黑色*", "粉色de圣诞袜", "*黑色*", "粉色de圣诞袜   ", "" };
    std::vector<std::string> bp { "*黑色*", "粉色de圣诞袜", "*黑色*", "粉色de圣诞袜", "", "粉色de圣诞袜   " };
    std::vector<int32_t> bWidth { 4, 6, 5 };
    std::vector<std::string> expected { "粉色de圣诞袜*黑色*", "*黑色*粉色de圣诞袜", "粉色de圣诞袜*黑色*",
        "*黑色*粉色de圣诞袜", "粉色de圣诞袜   ",    "粉色de圣诞袜   " };
    int32_t batch = 3;
    int32_t rowCnt = 2;
    for (int32_t i = 0; i < batch; i++) {
        std::vector<uint8_t *> output(rowCnt);
        std::vector<int32_t> outLen(rowCnt);
        std::vector<int32_t> apLen(rowCnt);
        std::vector<int32_t> bpLen(rowCnt);
        std::vector<uint8_t *> apAddr(rowCnt);
        std::vector<uint8_t *> bpAddr(rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            int32_t index = i * rowCnt + row;
            apLen[row] = static_cast<int32_t>(ap[index].length());
            bpLen[row] = static_cast<int32_t>(bp[index].length());
            apAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[index].c_str()));
            bpAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[index].c_str()));
        }

        bool isAnyNull[] = {false, false};
        BatchConcatStrChar(contextPtr, apAddr.data(), apLen.data(), bpAddr.data(), bWidth[i], bpLen.data(), isAnyNull,
            output.data(), outLen.data(), rowCnt);
        AssertStringEquals(expected, i * rowCnt, rowCnt, output, outLen);
    }
    delete context;
}

TEST(BatchFunctionTest, ConcatStrStrRetNull)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> ap { "你是Chinese?", "", "pink圣诞袜" };
    std::vector<int32_t> apLen { 14, 0, 13 };
    std::vector<std::string> bp { "Yes我是", "粉色de圣诞袜", "" };
    std::vector<int32_t> bpLen { 9, 17, 0 };
    int32_t rowCnt = ap.size();
    std::vector<uint8_t *> output(rowCnt);
    std::vector<int32_t> outLen(rowCnt);
    std::vector<uint8_t *> apAddr(rowCnt);
    std::vector<uint8_t *> bpAddr(rowCnt);
    for (int32_t i = 0; i < rowCnt; i++) {
        apAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[i].c_str()));
        bpAddr[i] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[i].c_str()));
    }
    bool isNull[] = {false, false, false};
    BatchConcatStrStrRetNull(isNull, contextPtr, apAddr.data(), apLen.data(), bpAddr.data(), bpLen.data(),
        output.data(), outLen.data(), rowCnt);
    std::vector<std::string> expected { "你是Chinese?Yes我是", "粉色de圣诞袜", "pink圣诞袜" };
    AssertStringEquals(expected, output, outLen);
    delete context;
}

TEST(BatchFunctionTest, ConcatCharCharRetNull)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> ap { "粉色de圣诞袜", "*黑色*",      "Hei你好吗",    "Oh我很好",
        "Hei你好吗   ", "Oh我很好  ",  "   Hei你好吗", "   Oh我很好",
        "Hei   你好吗", "Oh   我很好", "   ",          "Oh我很好",
        "Hei你好吗",    "   ",         "Hei你好吗",    "" };
    std::vector<int32_t> aWidth { 7, 8, 10, 12, 12, 5, 8, 8 };
    std::vector<std::string> bp { "*黑色*",      "粉色de",      "Oh我很好",     "Hei你好吗",   "Oh我很好  ",
        "Hei你好吗  ", "   Oh我很好", "   Hei你好吗", "Oh   我很好", "Hei   你好",
        "Oh我很好   ", "   ",         "   ",          "Hei你好吗",   "",
        "Hei你好" };
    std::vector<int32_t> bWidth { 4, 8, 8, 12, 8, 12, 5, 5 };
    std::vector<std::string> expected { "粉色de圣诞袜*黑色*", "*黑色*   粉色de",
        "Hei你好吗  Oh我很好", "Oh我很好   Hei你好吗",
        "Hei你好吗    Oh我很好  ", "Oh我很好     Hei你好吗  ",
        "   Hei你好吗      Oh我很好", "   Oh我很好       Hei你好吗",
        "Hei   你好吗   Oh   我很好", "Oh   我很好    Hei   你好",
        "     Oh我很好   ", "Oh我很好   ",
        "Hei你好吗     ", "        Hei你好吗",
        "Hei你好吗", "        Hei你好" };
    int32_t batch = 8;
    int32_t rowCnt = 2;
    for (int32_t i = 0; i < batch; i++) {
        std::vector<uint8_t *> output(rowCnt);
        std::vector<int32_t> outLen(rowCnt);
        std::vector<int32_t> apLen(rowCnt);
        std::vector<int32_t> bpLen(rowCnt);
        std::vector<uint8_t *> apAddr(rowCnt);
        std::vector<uint8_t *> bpAddr(rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            int32_t index = i * rowCnt + row;
            apLen[row] = static_cast<int32_t>(ap[index].length());
            bpLen[row] = static_cast<int32_t>(bp[index].length());
            apAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[index].c_str()));
            bpAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[index].c_str()));
        }
        bool isNull[] = {false, false};
        BatchConcatCharCharRetNull(isNull, contextPtr, apAddr.data(), aWidth[i], apLen.data(), bpAddr.data(), bWidth[i],
            bpLen.data(), output.data(), outLen.data(), rowCnt);
        AssertStringEquals(expected, i * rowCnt, rowCnt, output, outLen);
    }
    delete context;
}

TEST(BatchFunctionTest, ConcatCharStrRetNull)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> ap { "*你是谁呢*", "我很OK", "*你是谁呢*", "我很OK", "*你是谁呢*", "" };
    std::vector<int32_t> aWidth { 6, 10, 10 };
    std::vector<std::string> bp { "我很OK", "*你是谁呢*", "我很OK", "*你是谁呢*", "", "*你是谁呢*" };
    std::vector<std::string> expected { "*你是谁呢*我很OK",       "我很OK  *你是谁呢*", "*你是谁呢*    我很OK",
        "我很OK      *你是谁呢*", "*你是谁呢*",         "          *你是谁呢*" };
    int32_t batch = 3;
    int32_t rowCnt = 2;
    for (int32_t i = 0; i < batch; i++) {
        std::vector<uint8_t *> output(rowCnt);
        std::vector<int32_t> outLen(rowCnt);
        std::vector<int32_t> apLen(rowCnt);
        std::vector<int32_t> bpLen(rowCnt);
        std::vector<uint8_t *> apAddr(rowCnt);
        std::vector<uint8_t *> bpAddr(rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            int32_t index = i * rowCnt + row;
            apLen[row] = static_cast<int32_t>(ap[index].length());
            bpLen[row] = static_cast<int32_t>(bp[index].length());
            apAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[index].c_str()));
            bpAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[index].c_str()));
        }

        bool isNull[] = {false, false};
        BatchConcatCharStrRetNull(isNull, contextPtr, apAddr.data(), aWidth[i], apLen.data(), bpAddr.data(),
            bpLen.data(), output.data(), outLen.data(), rowCnt);
        AssertStringEquals(expected, i * rowCnt, rowCnt, output, outLen);
    }
    delete context;
}

TEST(BatchFunctionTest, ConcatStrCharRetNull)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> ap { "粉色de圣诞袜", "*黑色*", "粉色de圣诞袜", "*黑色*", "粉色de圣诞袜   ", "" };
    std::vector<std::string> bp { "*黑色*", "粉色de圣诞袜", "*黑色*", "粉色de圣诞袜", "", "粉色de圣诞袜   " };
    std::vector<int32_t> bWidth { 4, 6, 5 };
    std::vector<std::string> expected { "粉色de圣诞袜*黑色*", "*黑色*粉色de圣诞袜", "粉色de圣诞袜*黑色*",
        "*黑色*粉色de圣诞袜", "粉色de圣诞袜   ",    "粉色de圣诞袜   " };
    int32_t batch = 3;
    int32_t rowCnt = 2;
    for (int32_t i = 0; i < batch; i++) {
        std::vector<uint8_t *> output(rowCnt);
        std::vector<int32_t> outLen(rowCnt);
        std::vector<int32_t> apLen(rowCnt);
        std::vector<int32_t> bpLen(rowCnt);
        std::vector<uint8_t *> apAddr(rowCnt);
        std::vector<uint8_t *> bpAddr(rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            int32_t index = i * rowCnt + row;
            apLen[row] = static_cast<int32_t>(ap[index].length());
            bpLen[row] = static_cast<int32_t>(bp[index].length());
            apAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(ap[index].c_str()));
            bpAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(bp[index].c_str()));
        }

        bool isNull[] = {false, false};
        BatchConcatStrCharRetNull(isNull, contextPtr, apAddr.data(), apLen.data(), bpAddr.data(), bWidth[i],
            bpLen.data(), output.data(), outLen.data(), rowCnt);
        AssertStringEquals(expected, i * rowCnt, rowCnt, output, outLen);
    }
    delete context;
}

TEST(BatchFunctionTest, CastStrWithDiffWidths)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> srcStr { "cast乌s斯侧后解", "cast乌s斯解", "cast乌s斯侧后解", "cast乌s" };
    std::vector<int32_t> srcWidth { 18, 18 };
    std::vector<int32_t> dstWidth { 1024, 7 };
    std::vector<std::string> expected { "cast乌s斯侧后解", "cast乌s斯解", "cast乌s斯", "cast乌s" };
    int32_t batch = 2;
    int32_t rowCnt = 2;
    for (int32_t i = 0; i < batch; i++) {
        std::vector<uint8_t *> output(rowCnt);
        std::vector<int32_t> outLen(rowCnt);
        std::vector<int32_t> strLen(rowCnt);
        std::vector<uint8_t *> srcAddr(rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            int32_t index = i * rowCnt + row;
            strLen[row] = static_cast<int32_t>(srcStr[index].length());
            srcAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(srcStr[index].c_str()));
        }
        bool isAnyNull[] = {false, false};
        BatchCastStrWithDiffWidths(contextPtr, srcAddr.data(), srcWidth[i], strLen.data(), isAnyNull, output.data(),
            outLen.data(), dstWidth[i], rowCnt);
        AssertStringEquals(expected, i * rowCnt, rowCnt, output, outLen);
    }
    delete context;
}

TEST(BatchFunctionTest, CastStrWithDiffWidthsRetNull)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::vector<std::string> srcStr { "cast乌s斯侧后解", "cast乌s斯解", "cast乌s斯侧后解", "cast乌s" };
    std::vector<int32_t> srcWidth { 18, 18 };
    std::vector<int32_t> dstWidth { 1024, 7 };
    std::vector<std::string> expected { "cast乌s斯侧后解", "cast乌s斯解", "cast乌s斯", "cast乌s" };
    int32_t batch = 2;
    int32_t rowCnt = 2;
    for (int32_t i = 0; i < batch; i++) {
        std::vector<uint8_t *> output(rowCnt);
        std::vector<int32_t> outLen(rowCnt);
        std::vector<int32_t> strLen(rowCnt);
        std::vector<uint8_t *> srcAddr(rowCnt);
        for (int32_t row = 0; row < rowCnt; row++) {
            int32_t index = i * rowCnt + row;
            strLen[row] = static_cast<int32_t>(srcStr[index].length());
            srcAddr[row] = reinterpret_cast<uint8_t *>(const_cast<char *>(srcStr[index].c_str()));
        }
        bool isNull[] = {false, false};
        BatchCastStrWithDiffWidthsRetNull(isNull, contextPtr, srcAddr.data(), srcWidth[i], strLen.data(), output.data(),
            outLen.data(), dstWidth[i], rowCnt);
        AssertStringEquals(expected, i * rowCnt, rowCnt, output, outLen);
    }
    delete context;
}