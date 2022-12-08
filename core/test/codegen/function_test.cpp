// /*
// * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
// * Description: function test
// */
#include <string>
#include <vector>
#include <chrono>
#include "gtest/gtest.h"
#include "expression/expressions.h"
#include "codegen/functions/stringfunctions.h"
#include "codegen/functions/decimalfunctions.h"
#include "codegen/functions/mathfunctions.h"
#include "codegen/functions/murmur3_hash.h"
#include "codegen/functions/dictionaryfunctions.h"
#include "codegen/functions/varcharVectorfunctions.h"
#include "codegen/functions/udffunctions.h"
#include "jni_mock.h"
#include "udf/cplusplus/jni_util.h"

namespace omniruntime {
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::codegen;
using namespace std;

/*
 * Dictionary funtion tests
 */
TEST(FunctionTest, GetIntFromDictionaryVector)
{
    int size = 5;
    auto vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_GetIntFromDictionaryVector");
    auto intVector = new IntVector(vecAllocator, size * 2);
    for (int i = 0; i < intVector->GetSize(); i++) {
        intVector->SetValue(i, i * 10);
    }
    int32_t ids[] = {0, 2, 4, 6, 8};
    auto dict = new DictionaryVector(intVector, ids, size);
    int64_t dictptr = reinterpret_cast<int64_t>(dict);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(GetIntFromDictionaryVector(dictptr, i), intVector->GetValue(ids[i]));
    }
    delete intVector;
    delete dict;
    delete vecAllocator;
}

TEST(FunctionTest, GetLongFromDictionaryVector)
{
    int size = 5;
    auto vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("LongVector_GetLongFromDictionaryVector");
    auto longVector = new LongVector(vecAllocator, size * 2);
    for (int i = 0; i < longVector->GetSize(); i++) {
        longVector->SetValue(i, static_cast<int64_t>(i * 10));
    }
    int32_t ids[] = {0, 2, 4, 6, 8};
    auto dict = new DictionaryVector(longVector, ids, size);
    int64_t dictptr = reinterpret_cast<int64_t>(dict);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(GetLongFromDictionaryVector(dictptr, i), longVector->GetValue(ids[i]));
    }
    delete longVector;
    delete dict;
    delete vecAllocator;
}

TEST(FunctionTest, GetDoubleFromDictionaryVector)
{
    int size = 5;
    auto vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("DoubleVector_GetDoubleFromDictionaryVector");
    auto doubleVector = new DoubleVector(vecAllocator, size * 2);
    for (int i = 0; i < doubleVector->GetSize(); i++) {
        doubleVector->SetValue(i, 3.14159 * i);
    }
    int32_t ids[] = {0, 2, 4, 6, 8};
    auto dict = new DictionaryVector(doubleVector, ids, size);
    int64_t dictptr = reinterpret_cast<int64_t>(dict);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(GetDoubleFromDictionaryVector(dictptr, i), doubleVector->GetValue(ids[i]));
    }
    delete doubleVector;
    delete dict;
    delete vecAllocator;
}

TEST(FunctionTest, GetBooleanFromDictionaryVector)
{
    int size = 5;
    auto vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("BooleanVector_GetBooleanFromDictionaryVector");
    auto booleanVector = new BooleanVector(vecAllocator, size * 2);
    for (int i = 0; i < booleanVector->GetSize(); i++) {
        booleanVector->SetValue(i, i % 2 == 0);
    }
    int32_t ids[] = {0, 2, 4, 6, 8};
    auto dict = new DictionaryVector(booleanVector, ids, size);
    int64_t dictptr = reinterpret_cast<int64_t>(dict);
    for (int i = 0; i < size; i++) {
        EXPECT_EQ(GetBooleanFromDictionaryVector(dictptr, i), booleanVector->GetValue(ids[i]));
    }
    delete booleanVector;
    delete dict;
    delete vecAllocator;
}

TEST(FunctionTest, GetVarcharFromDictionaryVector)
{
    int size = 5;
    auto vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_GetVarcharFromDictionaryVector");
    auto varcharVector = new VarcharVector(vecAllocator, size * 2);
    for (int i = 0; i < varcharVector->GetSize(); i++) {
        varcharVector->SetValue(i, reinterpret_cast<const unsigned char *>(string(i, 'a').c_str()), i);
    }
    int32_t ids[] = {0, 2, 4, 6, 8};
    auto dict = new DictionaryVector(varcharVector, ids, size);
    int32_t length = 0;
    int64_t dictptr = reinterpret_cast<int64_t>(dict);
    unsigned char *actual = nullptr;
    unsigned char *expected = nullptr;
    for (int i = 0; i < size; i++) {
        varcharVector->GetValue(ids[i], &actual);
        expected = GetVarcharFromDictionaryVector(dictptr, i, &length);
        EXPECT_EQ(expected, actual);
    }
    delete varcharVector;
    delete dict;
    delete vecAllocator;
}

TEST(FunctionTest, GetDecimalFromDictionaryVector)
{
    int size = 5;
    auto vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("DecimalVector_GetDecimalFromDictionaryVector");
    auto decimalVector = new Decimal128Vector(vecAllocator, size * 2);
    for (int i = 0; i < decimalVector->GetSize(); i++) {
        Decimal128 x(i * 11, i * 13);
        decimalVector->SetValue(i, x);
    }
    int32_t ids[] = {0, 2, 4, 6, 8};
    auto dict = new DictionaryVector(decimalVector, ids, size);
    int64_t outHigh = 0;
    uint64_t outLow = 0;
    int64_t dictptr = reinterpret_cast<int64_t>(dict);
    for (int i = 0; i < size; i++) {
        GetDecimalFromDictionaryVector(dictptr, i, 38, 0, &outHigh, &outLow);
        Decimal128 expected = decimalVector->GetValue(ids[i]);
        EXPECT_EQ(expected.HighBits(), outHigh);
        EXPECT_EQ(expected.LowBits(), outLow);
    }
    delete decimalVector;
    delete dict;
    delete vecAllocator;
}

/*
 * varcharVector tests
 */
TEST(FunctionTest, WrapVarcharVector)
{
    auto vecAllocator =
        VectorAllocator::GetGlobalAllocator()->NewChildAllocator("WrapVarcharVector_GetWrapVarcharVector");
    auto varcharVector = new VarcharVector(vecAllocator, 1024, 5);
    int64_t vecptr = reinterpret_cast<int64_t>(varcharVector);
    WrapVarcharVector(vecptr, 0, (uint8_t *)"hello", 5);
    WrapVarcharVector(vecptr, 3, (uint8_t *)"world", 5);
    uint8_t *temp = nullptr;
    int len = varcharVector->GetValue(0, &temp);
    string result(reinterpret_cast<char *>(temp), len);
    EXPECT_EQ(result, "hello");
    len = varcharVector->GetValue(3, &temp);
    string result2(reinterpret_cast<char *>(temp), len);
    EXPECT_EQ(result2, "world");
    delete varcharVector;
    delete vecAllocator;
}

/*
 * context helper tests
 */
TEST(FunctionTest, ArenaAllocatorMalloc)
{
    auto execContext = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(execContext);
    char *ptr;
    for (int i = 1; i <= execContext->GetArena()->TotalBytes() / 256; ++i) {
        ptr = ArenaAllocatorMalloc(contextptr, 256);
        EXPECT_NE(ptr, nullptr);
        // check 256 is allocated each call;
        EXPECT_EQ(256 * i, execContext->GetArena()->TotalBytes() - execContext->GetArena()->AvailBytes());
    }
    delete execContext;
}

TEST(FunctionTest, ArenaAllocatorReset)
{
    auto execContext = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(execContext);

    ArenaAllocatorMalloc(contextptr, 1);
    EXPECT_EQ(execContext->GetArena()->AvailBytes(), execContext->GetArena()->TotalBytes() - 1);
    ArenaAllocatorReset(contextptr);
    EXPECT_EQ(execContext->GetArena()->AvailBytes(), execContext->GetArena()->TotalBytes());

    ArenaAllocatorMalloc(contextptr, execContext->GetArena()->TotalBytes());
    EXPECT_EQ(execContext->GetArena()->AvailBytes(), 0);
    ArenaAllocatorReset(contextptr);
    EXPECT_EQ(execContext->GetArena()->AvailBytes(), execContext->GetArena()->TotalBytes());
    delete execContext;
}

/*
 * Murmur3 hash tests
 */
TEST(FunctionTest, Mm3Int32)
{
    EXPECT_EQ(Mm3Int32(-2147483648, false, 42, false), 723455942);
}

TEST(FunctionTest, Mm3Int64)
{
    EXPECT_EQ(Mm3Int64(static_cast<int64_t>(-2147483648), false, 42, false), -1889108749);
}

TEST(FunctionTest, Mm3Double)
{
    EXPECT_EQ(Mm3Double(123.456, false, 42, false), -39269148);
}

TEST(FunctionTest, Mm3String)
{
    EXPECT_EQ(Mm3String("hello world", 11, false, 42, false), -1528836094);
}

TEST(FunctionTest, Mm3Boolean)
{
    EXPECT_EQ(Mm3Boolean(true, false, 42, false), -559580957);
}

/*
 * Math functions:
 */
TEST(FunctionTest, Abs)
{
    EXPECT_EQ(10, Abs<int32_t>(10));
    EXPECT_EQ(24, Abs<int32_t>(-24));
    EXPECT_EQ(0, Abs<int32_t>(0));
    EXPECT_EQ(10, Abs<int64_t>(10));
    EXPECT_EQ(24, Abs<int64_t>(-24));
    EXPECT_EQ(0, Abs<int64_t>(0));
}

TEST(FunctionTest, CastInt32ToInt64)
{
    int32_t test1 = 10;
    int32_t test2 = -24;
    int32_t test3 = 0;
    int32_t test4 = std::numeric_limits<int32_t>::min();
    int32_t test5 = std::numeric_limits<int32_t>::max();
    int64_t baseline = 1;
    auto result = CastInt32ToInt64(test1);
    bool isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10, CastInt32ToInt64(result));

    result = CastInt32ToInt64(test2);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24, result);

    result = CastInt32ToInt64(test3);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0, CastInt32ToInt64(result));

    result = CastInt32ToInt64(test4);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(std::numeric_limits<int32_t>::min(), result);

    result = CastInt32ToInt64(test5);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(std::numeric_limits<int32_t>::max(), result);
}

TEST(FunctionTest, CastInt64ToInt32)
{
    int64_t test1 = 10;
    int64_t test2 = -24;
    int64_t test3 = 0;
    int64_t test4 = std::numeric_limits<int64_t>::min();
    int64_t test5 = std::numeric_limits<int64_t>::max();
    int32_t baseline = 1;
    auto result = CastInt64ToInt32(test1);
    bool isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10, result);

    result = CastInt64ToInt32(test2);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24, result);

    result = CastInt64ToInt32(test3);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0, CastInt64ToInt32(result));

    result = CastInt64ToInt32(test4);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0, result);

    result = CastInt64ToInt32(test5);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-1, result);
}

TEST(FunctionTest, CastInt32ToDouble)
{
    int32_t test1 = 10;
    int32_t test2 = -24;
    int32_t test3 = 0;
    int32_t test4 = std::numeric_limits<int32_t>::min();
    int32_t test5 = std::numeric_limits<int32_t>::max();
    double baseline = 11.13;
    auto result = CastInt32ToDouble(test1);
    bool isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10.00, result);

    result = CastInt32ToDouble(test2);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24.00, result);

    result = CastInt32ToDouble(test3);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0.00, result);

    result = CastInt32ToDouble(test4);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(static_cast<double>(std::numeric_limits<int32_t>::min()), result);

    result = CastInt32ToDouble(test5);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(static_cast<double>(std::numeric_limits<int32_t>::max()), result);
}

TEST(FunctionTest, CastInt64ToDouble)
{
    int64_t test1 = 10;
    int64_t test2 = -24;
    int64_t test3 = 0;
    int64_t test4 = std::numeric_limits<int64_t>::min();
    int64_t test5 = std::numeric_limits<int64_t>::max();
    double baseline = 11.13;
    auto result = CastInt64ToDouble(test1);
    bool isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10.00, result);

    result = CastInt64ToDouble(test2);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24.00, result);

    result = CastInt64ToDouble(test3);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0.00, result);

    result = CastInt64ToDouble(test4);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(static_cast<double>(std::numeric_limits<int64_t>::min()), result);

    result = CastInt64ToDouble(test5);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(static_cast<double>(std::numeric_limits<int64_t>::max()), result);
}

TEST(FunctionTest, CastDoubleToInt32)
{
    double test1 = 10.00;
    double test2 = -24.00;
    double test3 = 0.0;
    double test4 = 113.1313;
    double test5 = -2000.989;
    int32_t baseline = 1;
    auto result = CastDoubleToInt32(test1);
    bool isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10, result);

    result = CastDoubleToInt32(test2);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24, result);

    result = CastDoubleToInt32(test3);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0, result);

    result = CastDoubleToInt32(test4);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(113, result);

    result = CastDoubleToInt32(test5);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-2001, result);
}

TEST(FunctionTest, CastDoubleToInt64)
{
    double test1 = 10.00;
    double test2 = -24.00;
    double test3 = 0.0;
    double test4 = 113.1313;
    double test5 = -2000.989;
    int64_t baseline = 1;
    auto result = CastDoubleToInt64(test1);
    bool isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10, result);

    result = CastDoubleToInt64(test2);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24, result);

    result = CastDoubleToInt64(test3);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0, result);

    result = CastDoubleToInt64(test4);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(113, result);

    result = CastDoubleToInt64(test5);
    isSameType = is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-2001, result);
}

TEST(FunctionTest, Pmod)
{
    EXPECT_EQ(0, Pmod(4589732, 0));
    int n = 100;
    for (int i = 1; i <= n; ++i) {
        EXPECT_EQ(n % i, Pmod(n, i));
    }
    n = -100;
    for (int i = -1; i >= n; --i) {
        EXPECT_EQ(n - (n / i) * i, Pmod(n, i));
    }
}

TEST(FunctionTest, Round)
{
    EXPECT_EQ(10, Round<int32_t>(10, 0));
    EXPECT_EQ(10, Round<int32_t>(10, 5));
    EXPECT_EQ(-10, Round<int32_t>(-10, 0));
    EXPECT_EQ(-10, Round<int32_t>(-10, 5));

    EXPECT_EQ(10, Round<int64_t>(10, 0));
    EXPECT_EQ(10, Round<int64_t>(10, 5));
    EXPECT_EQ(-10, Round<int64_t>(-10, 0));
    EXPECT_EQ(-10, Round<int64_t>(-10, 5));

    EXPECT_EQ(10.00, Round<double>(10.12345, 0));
    EXPECT_EQ(10.12, Round<double>(10.12345, 2));
    EXPECT_EQ(10.13, Round<double>(10.12945, 2));
    EXPECT_EQ(-10.00, Round<double>(-10.12345, 0));
    EXPECT_EQ(-10.12, Round<double>(-10.12345, 2));
    EXPECT_EQ(-10.13, Round<double>(-10.12945, 2));
}

TEST(FunctionTest, CombineHash)
{
    EXPECT_EQ(342, CombineHash(10, false, 32, false));
}

/*
 * Decimal Functions:
 */
TEST(FunctionTest, Decimal128Compare)
{
    Decimal128 op1;
    Decimal128 op2;

    op1 = DecimalOperations::UnscaledDecimal(0);
    op2 = DecimalOperations::UnscaledDecimal(0);
    EXPECT_EQ(0, Decimal128Compare(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, false));

    op1 = DecimalOperations::UnscaledDecimal(1);
    op2 = DecimalOperations::UnscaledDecimal(5);
    EXPECT_EQ(-1, Decimal128Compare(op1.HighBits(), op1.LowBits(), 38, 4, op2.HighBits(), op2.LowBits(), 38, 4, false));

    op1 = DecimalOperations::UnscaledDecimal(6);
    op2 = DecimalOperations::UnscaledDecimal(-8);
    EXPECT_EQ(1,
        Decimal128Compare(op1.HighBits(), op1.LowBits(), 38, 17, op2.HighBits(), op2.LowBits(), 38, 17, false));
}

TEST(FunctionTest, AddDec128)
{
    Decimal128 op1;
    Decimal128 op2;
    Decimal128 expected;
    int64_t zHigh = 0;
    uint64_t zLow = 0;
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);

    op1 = DecimalOperations::UnscaledDecimal(0);
    op2 = DecimalOperations::UnscaledDecimal(0);
    expected = DecimalOperations::UnscaledDecimal(0);
    AddDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 9, op2.HighBits(), op2.LowBits(), 38, 9, 38, 9,
        &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(5);
    op2 = DecimalOperations::UnscaledDecimal(10);
    expected = DecimalOperations::UnscaledDecimal(15);
    AddDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 12, op2.HighBits(), op2.LowBits(), 38, 12, 38,
        12, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-1);
    op2 = DecimalOperations::UnscaledDecimal(1);
    expected = DecimalOperations::UnscaledDecimal(0);
    AddDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0,
        &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-3);
    op2 = DecimalOperations::UnscaledDecimal(-4);
    expected = DecimalOperations::UnscaledDecimal(-7);
    AddDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0,
        &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    delete context;
}

TEST(FunctionTest, SubDec128)
{
    Decimal128 op1;
    Decimal128 op2;
    Decimal128 expected;
    int64_t zHigh = 0;
    uint64_t zLow = 0;

    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);

    op1 = DecimalOperations::UnscaledDecimal(0);
    op2 = DecimalOperations::UnscaledDecimal(0);
    expected = DecimalOperations::UnscaledDecimal(0);
    SubDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0,
        &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(10);
    op2 = DecimalOperations::UnscaledDecimal(5);
    expected = DecimalOperations::UnscaledDecimal(5);
    SubDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0,
        &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(5);
    op2 = DecimalOperations::UnscaledDecimal(10);
    expected = DecimalOperations::UnscaledDecimal(-5);
    SubDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0,
        &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-3);
    op2 = DecimalOperations::UnscaledDecimal(-4);
    expected = DecimalOperations::UnscaledDecimal(1);
    SubDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0,
        &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    delete context;
}

TEST(FunctionTest, MulDec128)
{
    Decimal128 op1;
    Decimal128 op2;
    Decimal128 expected;
    int64_t zHigh = 0;
    uint64_t zLow = 0;

    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    op1 = DecimalOperations::UnscaledDecimal(0);
    op2 = DecimalOperations::UnscaledDecimal(500);
    expected = DecimalOperations::UnscaledDecimal(0);
    MulDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 7, op2.HighBits(), op2.LowBits(), 38, 7, 38,
        14, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(1);
    op2 = DecimalOperations::UnscaledDecimal(500);
    expected = DecimalOperations::UnscaledDecimal(500);
    MulDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 1, op2.HighBits(), op2.LowBits(), 38, 1, 38, 2,
        &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(3);
    op2 = DecimalOperations::UnscaledDecimal(5);
    expected = DecimalOperations::UnscaledDecimal(15);
    MulDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0,
        &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-3);
    op2 = DecimalOperations::UnscaledDecimal(-4);
    expected = DecimalOperations::UnscaledDecimal(12);
    MulDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0,
        &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-3);
    op2 = DecimalOperations::UnscaledDecimal(4);
    expected = DecimalOperations::UnscaledDecimal(-12);
    MulDec128Dec128Dec128(contextPtr, op1.HighBits(), op1.LowBits(), 38, 3, op2.HighBits(), op2.LowBits(), 38, 3, 38, 6,
        &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    delete context;
}

TEST(FunctionTest, DivDec128)
{
    Decimal128 op1;
    Decimal128 op2;
    Decimal128 expected;
    int64_t zHigh = 0;
    uint64_t zLow = 0;

    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    // for simplicity using precision = 0 scale = 0
    int32_t precision = 0;
    int32_t scale = 0;

    op1 = DecimalOperations::UnscaledDecimal(10);
    op2 = DecimalOperations::UnscaledDecimal(2);
    expected = DecimalOperations::UnscaledDecimal(5);
    DivDec128Dec128Dec128(contextptr, op1.HighBits(), op1.LowBits(), precision, scale, op2.HighBits(), op2.LowBits(),
        precision, scale, precision, scale, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-10);
    op2 = DecimalOperations::UnscaledDecimal(2);
    expected = DecimalOperations::UnscaledDecimal(-5);
    DivDec128Dec128Dec128(contextptr, op1.HighBits(), op1.LowBits(), precision, scale, op2.HighBits(), op2.LowBits(),
        precision, scale, precision, scale, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-10);
    op2 = DecimalOperations::UnscaledDecimal(-2);
    expected = DecimalOperations::UnscaledDecimal(5);
    DivDec128Dec128Dec128(contextptr, op1.HighBits(), op1.LowBits(), precision, scale, op2.HighBits(), op2.LowBits(),
        precision, scale, precision, scale, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(7);
    op2 = DecimalOperations::UnscaledDecimal(3);
    expected = DecimalOperations::UnscaledDecimal(2);
    DivDec128Dec128Dec128(contextptr, op1.HighBits(), op1.LowBits(), precision, scale, op2.HighBits(), op2.LowBits(),
        precision, scale, precision, scale, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(8);
    op2 = DecimalOperations::UnscaledDecimal(3);
    expected = DecimalOperations::UnscaledDecimal(3);
    DivDec128Dec128Dec128(contextptr, op1.HighBits(), op1.LowBits(), precision, scale, op2.HighBits(), op2.LowBits(),
        precision, scale, precision, scale, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    delete context;
}

TEST(FunctionTest, AbsDecimal128)
{
    int64_t outHigh = 0;
    uint64_t outLow = 0;

    Decimal128 test;
    Decimal128 expected;

    test = DecimalOperations::UnscaledDecimal(3);
    AbsDecimal128(test.HighBits(), test.LowBits(), 38, 9, false, 38, 9, &outHigh, &outLow);
    EXPECT_EQ(outHigh, test.HighBits());
    EXPECT_EQ(outLow, test.LowBits());

    test = DecimalOperations::UnscaledDecimal(-1);
    expected = DecimalOperations::UnscaledDecimal(1);
    AbsDecimal128(test.HighBits(), test.LowBits(), 38, 0, false, 38, 0, &outHigh, &outLow);
    EXPECT_EQ(outHigh, expected.HighBits());
    EXPECT_EQ(outLow, expected.LowBits());

    test = DecimalOperations::UnscaledDecimal(0);
    expected = DecimalOperations::UnscaledDecimal(0);
    AbsDecimal128(test.HighBits(), test.LowBits(), 38, 0, false, 38, 0, &outHigh, &outLow);
    EXPECT_EQ(outHigh, expected.HighBits());
    EXPECT_EQ(outLow, expected.LowBits());
}

/*
 * String functions:
 */
TEST(FunctionTest, ConcatCharChar)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    int outlen = 0;
    const char *result;
    string actual;

    result = ConcatCharChar(contextptr, "hello", 5, 5, "world", 5, 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatCharChar(contextptr, "hello", 5, 5, "world", 10, 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatCharChar(contextptr, "hello", 10, 5, "world", 5, 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharChar(contextptr, "hello", 10, 5, "world", 5, 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharChar(contextptr, "", 0, 0, "", 0, 0, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    delete context;
}

TEST(FunctionTest, ConcatStrChar)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    int outlen = 0;
    const char *result;
    string actual;

    result = ConcatStrChar(contextptr, "hello", 5, "world", 5, 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatStrChar(contextptr, "hello", 5, "world", 10, 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatStrChar(contextptr, "hello", 5, "world     ", 10, 10, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld     ");
    EXPECT_EQ(outlen, 15);

    result = ConcatStrChar(contextptr, "", 0, "", 0, 0, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = ConcatStrChar(contextptr, "hello", 5, "     ", 5, 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "hello     ");
    EXPECT_EQ(outlen, 10);
    delete context;
}

TEST(FunctionTest, ConcatCharStr)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    int outlen = 0;
    const char *result;
    string actual;

    result = ConcatCharStr(contextptr, "hello", 5, 5, "world", 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatCharStr(contextptr, "hello", 10, 5, "world", 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharStr(contextptr, "hello     ", 10, 10, "world", 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharStr(contextptr, "", 0, 0, "", 0, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = ConcatCharStr(contextptr, "", 5, 0, "world", 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "     world");
    EXPECT_EQ(outlen, 10);
    delete context;
}

TEST(FunctionTest, Substr)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    string str = "Magic Johnson 123@#$";
    int32_t strlen = static_cast<int32_t>(str.length());
    int32_t outlen = 0;
    const char *result;
    string actual;

    result = Substr(contextptr, str.c_str(), strlen, 1, strlen, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outlen, strlen);

    result = Substr(contextptr, str.c_str(), strlen, 1, 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "Magic");
    EXPECT_EQ(outlen, 5);

    result = Substr(contextptr, str.c_str(), strlen, 10, 10, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "nson 123@#");
    EXPECT_EQ(outlen, 10);

    result = Substr(contextptr, str.c_str(), strlen, -5, 7, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "23@#$");
    EXPECT_EQ(outlen, 5);

    result = Substr(contextptr, str.c_str(), strlen, 0, 0, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = Substr(contextptr, str.c_str(), strlen, strlen, strlen + 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "$");
    EXPECT_EQ(outlen, 1);
    delete context;
}

TEST(FunctionTest, SubstrZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t strLen = static_cast<int32_t>(str.length());
    int32_t outLen = 0;
    const char *result;
    string actual;

    result = Substr(contextPtr, str.c_str(), strLen, 1, 37, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    result = Substr(contextPtr, str.c_str(), strLen, 1, 5, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "时欧基乌斯");
    EXPECT_EQ(outLen, 15);

    result = Substr(contextPtr, str.c_str(), strLen, 10, 10, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "hello! 回复哦");
    EXPECT_EQ(outLen, 16);

    result = Substr(contextPtr, str.c_str(), strLen, -5, 7, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "色的圣诞袜");
    EXPECT_EQ(outLen, 15);

    result = Substr(contextPtr, str.c_str(), strLen, 0, 0, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = Substr(contextPtr, str.c_str(), strLen, 37, strLen + 5, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "袜");
    EXPECT_EQ(outLen, 3);

    result = Substr(contextPtr, str.c_str(), strLen, -38, 10, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = Substr(contextPtr, str.c_str(), strLen, -37, 37, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    delete context;
}

TEST(FunctionTest, SubstrChar)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    string str = "Magic Johnson 123@#$       ";
    int32_t width = static_cast<int32_t>(str.length());
    int32_t strlen = width - 7;
    int32_t outlen = 0;
    const char *result;
    string actual;

    result = SubstrChar(contextptr, str.c_str(), width, strlen, 1, strlen, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "Magic Johnson 123@#$");
    EXPECT_EQ(outlen, strlen);

    result = SubstrChar(contextptr, str.c_str(), width, strlen, 1, 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "Magic");
    EXPECT_EQ(outlen, 5);

    result = SubstrChar(contextptr, str.c_str(), width, strlen, 10, 10, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "nson 123@#");
    EXPECT_EQ(outlen, 10);

    result = SubstrChar(contextptr, str.c_str(), width, strlen, -5, 7, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "23@#$");
    EXPECT_EQ(outlen, 5);

    result = SubstrChar(contextptr, str.c_str(), width, strlen, 0, 0, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = SubstrChar(contextptr, str.c_str(), width, strlen, strlen, strlen + 5, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "$");
    EXPECT_EQ(outlen, 1);

    delete context;
}

TEST(FunctionTest, SubstrCharZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t width = 37;
    int32_t strLen = str.length();
    int32_t outLen = 0;
    const char *result;
    string actual;

    result = SubstrChar(contextPtr, str.c_str(), width, strLen, 1, 37, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    result = SubstrChar(contextPtr, str.c_str(), width, strLen, 1, 5, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "时欧基乌斯");
    EXPECT_EQ(outLen, 15);

    result = SubstrChar(contextPtr, str.c_str(), width, strLen, 10, 10, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "hello! 回复哦");
    EXPECT_EQ(outLen, 16);

    result = SubstrChar(contextPtr, str.c_str(), width, strLen, -5, 7, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "色的圣诞袜");
    EXPECT_EQ(outLen, 15);

    result = SubstrChar(contextPtr, str.c_str(), width, strLen, 0, 0, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrChar(contextPtr, str.c_str(), width, strLen, 37, strLen + 5, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "袜");
    EXPECT_EQ(outLen, 3);

    result = SubstrChar(contextPtr, str.c_str(), width, strLen, -38, 10, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrChar(contextPtr, str.c_str(), width, strLen, -37, 37, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    delete context;
}

TEST(FunctionTest, SubstrWithStart)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    string str = "ABC efg 123 $%^";
    int32_t strlen = static_cast<int32_t>(str.length());
    int32_t outlen = 0;
    const char *result;
    string actual;

    result = SubstrWithStart(contextptr, str.c_str(), strlen, 1, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outlen, strlen);

    result = SubstrWithStart(contextptr, str.c_str(), strlen, 9, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "123 $%^");
    EXPECT_EQ(outlen, 7);

    result = SubstrWithStart(contextptr, str.c_str(), strlen, -3, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "$%^");
    EXPECT_EQ(outlen, 3);

    result = SubstrWithStart(contextptr, str.c_str(), strlen, 0, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    delete context;
}

TEST(FunctionTest, SubstrWithZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t strLen = static_cast<int32_t>(str.length());
    int32_t outLen = 0;
    const char *result;
    string actual;

    result = SubstrWithStart(contextPtr, str.c_str(), strLen, 1, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    result = SubstrWithStart(contextPtr, str.c_str(), strLen, 9, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, " hello! 回复哦黑色的and magic粉色的圣诞袜");
    EXPECT_EQ(outLen, 53);

    result = SubstrWithStart(contextPtr, str.c_str(), strLen, -3, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "圣诞袜");
    EXPECT_EQ(outLen, 9);

    result = SubstrWithStart(contextPtr, str.c_str(), strLen, 0, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrWithStart(contextPtr, str.c_str(), strLen, 37, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "袜");
    EXPECT_EQ(outLen, 3);

    result = SubstrWithStart(contextPtr, str.c_str(), strLen, -38, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrWithStart(contextPtr, str.c_str(), strLen, -37, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    delete context;
}

TEST(FunctionTest, SubstrWithZhForSpark)
{
    ConfigUtil::SetNegativeStartIndexOutOfBoundsRule(NegativeStartIndexOutOfBoundsRule::INTERCEPT_FROM_BEYOND);
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string str = "时欧基乌斯侧后解 h";
    int32_t strLen = static_cast<int32_t>(str.length());
    int32_t outLen = 0;
    const char *result;
    string actual;

    result = SubstrWithStart(contextPtr, str.c_str(), strLen, -15, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    result = Substr(contextPtr, str.c_str(), strLen, -15, 5, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = Substr(contextPtr, str.c_str(), strLen, -15, 6, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "时");
    EXPECT_EQ(outLen, 3);

    result = Substr(contextPtr, str.c_str(), strLen, -15, 14, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "时欧基乌斯侧后解 ");
    EXPECT_EQ(outLen, 25);

    result = Substr(contextPtr, str.c_str(), strLen, -15, 20, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "时欧基乌斯侧后解 h");
    EXPECT_EQ(outLen, 26);

    string strEn = "apple";
    result = Substr(contextPtr, strEn.c_str(), static_cast<int32_t>(strEn.length()), -7, 3, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "a");

    result = SubstrWithStart(contextPtr, strEn.c_str(), static_cast<int32_t>(strEn.length()), -7, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "apple");

    ConfigUtil::SetNegativeStartIndexOutOfBoundsRule(NegativeStartIndexOutOfBoundsRule::EMPTY_STRING);
    delete context;
}

TEST(FunctionTest, SubstrCharWithStart)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    string str = "ABC efg 123 $%^        ";
    int32_t width = static_cast<int32_t>(str.length());
    int32_t outlen = 0;
    int32_t strlen = width - 8;
    const char *result;
    string actual;

    result = SubstrCharWithStart(contextptr, str.c_str(), width, strlen, 1, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "ABC efg 123 $%^");
    EXPECT_EQ(outlen, strlen);

    result = SubstrCharWithStart(contextptr, str.c_str(), width, strlen, 9, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "123 $%^");
    EXPECT_EQ(outlen, 7);

    result = SubstrCharWithStart(contextptr, str.c_str(), width, strlen, -3, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "$%^");
    EXPECT_EQ(outlen, 3);

    result = SubstrCharWithStart(contextptr, str.c_str(), width, strlen, 0, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);
    delete context;
}

TEST(FunctionTest, SubstrCharWithStartZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t width = static_cast<int32_t>(str.length());
    int32_t outLen = 0;
    int32_t strLen = width;
    const char *result;
    string actual;

    result = SubstrCharWithStart(contextPtr, str.c_str(), width, strLen, 1, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    result = SubstrCharWithStart(contextPtr, str.c_str(), width, strLen, 9, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, " hello! 回复哦黑色的and magic粉色的圣诞袜");
    EXPECT_EQ(outLen, 53);

    result = SubstrCharWithStart(contextPtr, str.c_str(), width, strLen, -3, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "圣诞袜");
    EXPECT_EQ(outLen, 9);

    result = SubstrCharWithStart(contextPtr, str.c_str(), width, strLen, 0, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrCharWithStart(contextPtr, str.c_str(), width, strLen, 37, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "袜");
    EXPECT_EQ(outLen, 3);

    result = SubstrCharWithStart(contextPtr, str.c_str(), width, strLen, -38, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrCharWithStart(contextPtr, str.c_str(), width, strLen, -37, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    delete context;
}

TEST(FunctionTest, ToUpperStr)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    string test = "[\\]^_abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    string expected = "[\\]^_ABCDEFGHIJKLMNOPQRSTUVWXYZ ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    int32_t outLen = 0;
    const char *result = ToUpperStr(contextptr, test.c_str(), static_cast<int32_t>(test.length()), false, &outLen);
    string actual = string(result, outLen);
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(outLen, 62);
    delete context;
}

TEST(FunctionTest, ToUpperChar)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    string test = "[\\]^_abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    string expected = "[\\]^_ABCDEFGHIJKLMNOPQRSTUVWXYZ ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    int32_t width = 100;
    int32_t outLen = 0;
    const char *result =
        ToUpperChar(contextptr, test.c_str(), width, static_cast<int32_t>(test.length()), false, &outLen);
    string actual = string(result, outLen);
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(outLen, 62);
    delete context;
}

TEST(FunctionTest, ToLowerStr)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string test = "[\\]^_abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    string expected = "[\\]^_abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz{|}.";
    int32_t outLen = 0;
    const char *result = ToLowerStr(contextPtr, test.c_str(), static_cast<int32_t>(test.length()), false, &outLen);
    string actual = string(result, outLen);
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(outLen, 62);
    delete context;
}

TEST(FunctionTest, ToLowerChar)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    string test = "[\\]^_abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    string expected = "[\\]^_abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz{|}.";
    int32_t width = 100;
    int32_t outLen = 0;
    const char *result =
        ToLowerChar(contextPtr, test.c_str(), width, static_cast<int32_t>(test.length()), false, &outLen);
    string actual = string(result, outLen);
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(outLen, 62);
    delete context;
}

TEST(FunctionTest, StrCompare)
{
    int result = StrCompare("abcd EFGH 123 $%^", 17, "abcd EFGH 123 $%^", 17);
    EXPECT_EQ(result, string("abcd EFGH 123 $%^").compare(string("abcd EFGH 123 $%^")));

    result = StrCompare("five", 4, "four", 4);
    EXPECT_EQ(result, string("five").compare(string("four")));

    result = StrCompare("five", 4, "FIVE", 4);
    EXPECT_EQ(result, string("five").compare(string("FIVE")));

    result = StrCompare("test", 4, "testing", 7);
    EXPECT_EQ(result, string("test").compare(string("testing")));

    result = StrCompare("racecar", 7, "race", 4);
    EXPECT_EQ(result, string("racecar").compare(string("race")));
}

TEST(FunctionTest, LikeStr)
{
    bool result = LikeStr("hello", 5, "hello", 5, false);
    EXPECT_TRUE(result);

    result = LikeStr("regex", 5, "rege(x(es)?|xps?)", 17, false);
    EXPECT_TRUE(result);

    result = LikeStr("20500", 5, "\\d{5}(-\\d{4})?", 14, false);
    EXPECT_TRUE(result);
}

TEST(FunctionTest, ConcatStrStr)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    int outlen = 0;
    const char *result;
    string actual;

    result = ConcatStrStr(contextptr, "abc", 3, "defghi", 6, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "abcdefghi");
    EXPECT_EQ(outlen, 9);

    result = ConcatStrStr(contextptr, "hello", 5, "", 0, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "hello");
    EXPECT_EQ(outlen, 5);

    result = ConcatStrStr(contextptr, "", 0, "", 0, false, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);
    delete context;
}

// Cast
TEST(FunctionTest, CastStringToDate)
{
    ConfigUtil::SetStringToDateFormatRule(StringToDateFormatRule::ALLOW_REDUCED_PRECISION);
    // year-month-day
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int32_t result = CastStringToDate(contextPtr, "1970-01-03", 10, false);
    EXPECT_EQ(result, 2);
    result = CastStringToDate(contextPtr, "1969-12-31", 10, false);
    EXPECT_EQ(result, -1);
    result = CastStringToDate(contextPtr, "1980-01-01", 10, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDate(contextPtr, "1980-01-01 12345", 16, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDate(contextPtr, "1980-1-1", 8, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDate(contextPtr, "1980-1-01", 9, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDate(contextPtr, "1980-1-1 123", 12, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDate(contextPtr, "1980-01-1", 9, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDate(contextPtr, "1980-01", 7, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDate(contextPtr, "1980", 4, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDate(contextPtr, "1453-05-29", 10, false);
    EXPECT_EQ(result, -188682);
    result = CastStringToDate(contextPtr, "   1453-05-29   ", 16, false);
    EXPECT_EQ(result, -188682);
    result = CastStringToDate(contextPtr, "   1 453-05-29   ", 16, false);
    EXPECT_EQ(result, -1);

    result = CastStringToDate(contextPtr, "1996-09  ", 9, false);
    EXPECT_EQ(result, 9740);
    result = CastStringToDate(contextPtr, "1996-09-30", 10, false);
    EXPECT_EQ(result, 9769);

    bool isNull = false;
    result = CastStringToDateRetNull(&isNull, "   1453- 05-29    ", 16);
    EXPECT_EQ(result, -1);
    result = CastStringToDateRetNull(&isNull, "1453-05-29", 10);
    EXPECT_EQ(result, -188682);
    result = CastStringToDateRetNull(&isNull, "   1453-05-29   ", 16);
    EXPECT_EQ(result, -188682);
    ConfigUtil::SetStringToDateFormatRule(StringToDateFormatRule::NOT_ALLOW_REDUCED_PRECISION);
    delete context;
}

TEST(FunctionTest, LengthChar)
{
    string test = "abcd";
    int32_t width = 10;
    auto len = LengthChar(test.c_str(), width, test.length(), false);
    EXPECT_EQ(len, 10);
}

TEST(FunctionTest, LengthStr)
{
    string test = "abcd";
    auto len = LengthStr(test.c_str(), test.length(), false);
    EXPECT_EQ(len, 4);
}

TEST(FunctionTest, LengthStrZh)
{
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    auto len = LengthStr(str.c_str(), str.length(), false);
    EXPECT_EQ(len, 37);
}

TEST(FunctionTest, ReplaceStrStrStrWithRep)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int32_t outLen = 0;

    string str = "operator1";
    string searchStr = "o";
    string replaceStr = "**";
    auto result = ReplaceStrStrStrWithRep(contextPtr, str.c_str(), str.length(), searchStr.c_str(), searchStr.length(),
        replaceStr.c_str(), replaceStr.length(), false, &outLen);
    string expected = "**perat**r1";
    EXPECT_EQ(outLen, 11);
    EXPECT_EQ(string(result, outLen), expected);

    str = "operator2";
    searchStr = "";
    replaceStr = "*";
    result = ReplaceStrStrStrWithRep(contextPtr, str.c_str(), str.length(), searchStr.c_str(), searchStr.length(),
        replaceStr.c_str(), replaceStr.length(), false, &outLen);
    expected = "*o*p*e*r*a*t*o*r*2*";
    EXPECT_EQ(outLen, 19);
    EXPECT_EQ(string(result, outLen), expected);

    str = "operator3";
    searchStr = "era";
    replaceStr = "ER";
    result = ReplaceStrStrStrWithRep(contextPtr, str.c_str(), str.length(), searchStr.c_str(), searchStr.length(),
        replaceStr.c_str(), replaceStr.length(), false, &outLen);
    expected = "opERtor3";
    EXPECT_EQ(outLen, 8);
    EXPECT_EQ(string(result, outLen), expected);
    delete context;
}

TEST(FunctionTest, ReplaceStrStrWithoutRep)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int32_t outLen = 0;

    string str = "operator1";
    string searchStr = "o";
    auto result = ReplaceStrStrWithoutRep(contextPtr, str.c_str(), str.length(), searchStr.c_str(), searchStr.length(),
        false, &outLen);
    string expected = "peratr1";
    EXPECT_EQ(outLen, 7);
    EXPECT_EQ(string(result, outLen), expected);

    str = "operator2";
    searchStr = "";
    result = ReplaceStrStrWithoutRep(contextPtr, str.c_str(), str.length(), searchStr.c_str(), searchStr.length(),
        false, &outLen);
    expected = "operator2";
    EXPECT_EQ(outLen, 9);
    EXPECT_EQ(string(result, outLen), expected);

    str = "operator3";
    searchStr = "era";
    result = ReplaceStrStrWithoutRep(contextPtr, str.c_str(), str.length(), searchStr.c_str(), searchStr.length(),
        false, &outLen);
    expected = "optor3";
    EXPECT_EQ(outLen, 6);
    EXPECT_EQ(string(result, outLen), expected);
    delete context;
}

TEST(FunctionTest, ReplaceStrCharStr)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int32_t outLen = 0;

    string str[] = {"", " varchar2", "", "varchar4", "varchar5", "varchar6", "varchar7"};
    string searchStr[] =
            {"          ", " char200  ", "char300   ", "char400   ", "char500   ", "char600   ", "char700   "};
    string replaceStr[] = {"", " varchar2", "", "varchar4", "varchar5", "varchar6", "varchar7"};
    int32_t resultLen[] = {0, 9, 0,  8, 8, 8, 8};
    string expected[] = {"", " varchar2", "", "varchar4", "varchar5", "varchar6", "varchar7"};

    for (int32_t i = 0; i < 7; i++) {
        auto result = ReplaceStrStrStrWithRep(contextPtr, str[i].c_str(), str[i].length(), searchStr[i].c_str(),
            searchStr[i].length(), replaceStr[i].c_str(), replaceStr[i].length(), false, &outLen);
        EXPECT_EQ(outLen, resultLen[i]);
        EXPECT_EQ(string(result, outLen), expected[i]);
    }
    delete context;
}

TEST(FunctionTest, ReplaceCharCharChar)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int32_t outLen = 0;

    string str[] = {"          ", " char200  ", "          ", "char400   ", "char500   ", "char600   ", "char700   "};
    string searchStr[] =
            {"cha1     ", " char2     ", "char3     ", "char4     ", "char5     ", "char6     ", "char7     "};
    string replaceStr[] =
            {"varchar100", "varchar200", "varchar300", "varchar400", "varchar500", "varchar600", "varchar700"};
    int32_t resultLen[] = {10, 10, 10, 10, 10, 10, 10};
    string expected[] =
            {"          ", " char200  ", "          ", "char400   ", "char500   ", "char600   ", "char700   "};

    for (int32_t i = 0; i < 7; i++) {
        auto result = ReplaceStrStrStrWithRep(contextPtr, str[i].c_str(), str[i].length(), searchStr[i].c_str(),
            searchStr[i].length(), replaceStr[i].c_str(), replaceStr[i].length(), false, &outLen);
        EXPECT_EQ(outLen, resultLen[i]);
        EXPECT_EQ(string(result, outLen), expected[i]);
    }
    delete context;
}

TEST(FunctionTest, ReplaceStrStrStrZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int32_t outLen = 0;

    std::vector<string> str { "", "粉色的圣诞袜", "apple", "粉色de圣诞袜" };
    std::vector<string> searchStr { "", "粉色", "pp", "de圣" };
    std::vector<string> replaceStr { "", "黑色", "*w*", "*的*" };

    auto result1 = ReplaceStrStrStrWithRep(contextPtr, str[2].c_str(), str[2].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[2].c_str(), replaceStr[2].length(), false, &outLen);
    string expected = "*w*a*w*p*w*p*w*l*w*e*w*";
    EXPECT_EQ(outLen, 23);
    EXPECT_EQ(string(result1, outLen), expected);

    auto result2 = ReplaceStrStrStrWithRep(contextPtr, str[1].c_str(), str[1].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[2].c_str(), replaceStr[2].length(), false, &outLen);
    expected = "*w*粉*w*色*w*的*w*圣*w*诞*w*袜*w*";
    EXPECT_EQ(outLen, 39);
    EXPECT_EQ(string(result2, outLen), expected);

    auto result3 = ReplaceStrStrStrWithRep(contextPtr, str[3].c_str(), str[3].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[2].c_str(), replaceStr[2].length(), false, &outLen);
    expected = "*w*粉*w*色*w*d*w*e*w*圣*w*诞*w*袜*w*";
    EXPECT_EQ(outLen, 41);
    EXPECT_EQ(string(result3, outLen), expected);

    auto result4 = ReplaceStrStrStrWithRep(contextPtr, str[3].c_str(), str[3].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[3].c_str(), replaceStr[3].length(), false, &outLen);
    expected = "*的*粉*的*色*的*d*的*e*的*圣*的*诞*的*袜*的*";
    EXPECT_EQ(outLen, 57);
    EXPECT_EQ(string(result4, outLen), expected);

    auto result5 = ReplaceStrStrStrWithRep(contextPtr, str[3].c_str(), str[3].length(), searchStr[3].c_str(),
        searchStr[3].length(), replaceStr[3].c_str(), replaceStr[3].length(), false, &outLen);
    expected = "粉色*的*诞袜";
    EXPECT_EQ(outLen, 17);
    EXPECT_EQ(string(result5, outLen), expected);

    auto result6 = ReplaceStrStrStrWithRep(contextPtr, str[0].c_str(), str[0].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[0].c_str(), replaceStr[0].length(), false, &outLen);
    expected = "";
    EXPECT_EQ(outLen, 0);
    EXPECT_EQ(string(result6, outLen), expected);

    auto result7 = ReplaceStrStrStrWithRep(contextPtr, str[3].c_str(), str[3].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[0].c_str(), replaceStr[0].length(), false, &outLen);
    expected = "粉色de圣诞袜";
    EXPECT_EQ(outLen, 17);
    EXPECT_EQ(string(result7, outLen), expected);
    delete context;
}

TEST(FunctionTest, ConcatStrStrZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int outLen = 0;
    const char *result;
    string actual;

    result = ConcatStrStr(contextPtr, "你是Chinese?", 14, "Yes我是", 9, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "你是Chinese?Yes我是");
    EXPECT_EQ(outLen, 23);
    delete context;
}

TEST(FunctionTest, ConcatCharCharZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int32_t outLen = 0;
    const char *result;
    string actual;

    result = ConcatCharChar(contextPtr, "粉色de圣诞袜", 7, 17, "*黑色*", 4, 8, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "粉色de圣诞袜*黑色*");
    EXPECT_EQ(outLen, 25);

    result = ConcatCharChar(contextPtr, "Hei你好吗", 8, 12, "Oh我很好", 8, 11, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "Hei你好吗  Oh我很好");
    EXPECT_EQ(outLen, 25);

    result = ConcatCharChar(contextPtr, "Hei你好吗   ", 10, 15, "Oh我很好  ", 8, 13, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "Hei你好吗    Oh我很好  ");
    EXPECT_EQ(outLen, 29);

    result = ConcatCharChar(contextPtr, "   Hei你好吗", 12, 15, "   Oh我很好", 12, 14, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "   Hei你好吗      Oh我很好");
    EXPECT_EQ(outLen, 32);

    result = ConcatCharChar(contextPtr, "Hei   你好吗", 12, 15, "Oh   我很好", 8, 14, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "Hei   你好吗   Oh   我很好");
    EXPECT_EQ(outLen, 32);

    result = ConcatCharChar(contextPtr, "   ", 5, 3, "Oh我很好   ", 12, 14, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "     Oh我很好   ");
    EXPECT_EQ(outLen, 19);

    result = ConcatCharChar(contextPtr, "Hei你好吗", 8, 12, "   ", 5, 3, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "Hei你好吗     ");
    EXPECT_EQ(outLen, 17);

    result = ConcatCharChar(contextPtr, "Hei你好吗", 8, 12, "", 5, 0, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "Hei你好吗");
    EXPECT_EQ(outLen, 12);
    delete context;
}

TEST(FunctionTest, ConcatCharStrZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int outLen = 0;
    const char *result;
    string actual;

    result = ConcatCharStr(contextPtr, "*你是谁呢*", 6, 14, "我很OK", 8, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "*你是谁呢*我很OK");
    EXPECT_EQ(outLen, 22);

    result = ConcatCharStr(contextPtr, "*你是谁呢*", 10, 14, "我很OK", 8, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "*你是谁呢*    我很OK");
    EXPECT_EQ(outLen, 26);
    delete context;
}

TEST(FunctionTest, ConcatStrCharZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int outLen = 0;
    const char *result;
    string actual;

    result = ConcatStrChar(contextPtr, "粉色de圣诞袜", 17, "*黑色*", 4, 8, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "粉色de圣诞袜*黑色*");
    EXPECT_EQ(outLen, 25);

    result = ConcatStrChar(contextPtr, "粉色de圣诞袜", 17, "*黑色*", 6, 8, false, &outLen);
    actual = string(result, outLen);
    EXPECT_EQ(actual, "粉色de圣诞袜*黑色*");
    EXPECT_EQ(outLen, 25);
    delete context;
}

TEST(FunctionTest, LikeStrZh)
{
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    // like "xxx_"
    string pattern = "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞.$";
    bool isMatch = LikeStr(str.c_str(), str.length(), pattern.c_str(), pattern.length(), false);
    EXPECT_TRUE(isMatch);
    pattern = "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞..$";
    isMatch = LikeStr(str.c_str(), str.length(), pattern.c_str(), pattern.length(), false);
    EXPECT_FALSE(isMatch);

    // like "xxx%"
    pattern = "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣.*$";
    isMatch = LikeStr(str.c_str(), str.length(), pattern.c_str(), pattern.length(), false);
    EXPECT_TRUE(isMatch);
    pattern = "^欧时基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞.*$";
    isMatch = LikeStr(str.c_str(), str.length(), pattern.c_str(), pattern.length(), false);
    EXPECT_FALSE(isMatch);
}

TEST(FunctionTest, LikeCharZh)
{
    // like "xxx_"
    string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    string pattern = "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞.$";
    bool isMatch = LikeChar(str.c_str(), 37, str.length(), pattern.c_str(), pattern.length(), false);
    EXPECT_TRUE(isMatch);

    pattern = "^时欧基乌..$";
    str = "时欧基乌";
    isMatch = LikeChar(str.c_str(), 6, str.length(), pattern.c_str(), pattern.length(), false);
    EXPECT_TRUE(isMatch);

    pattern = "^时欧基乌.$";
    str = "时欧基乌";
    isMatch = LikeChar(str.c_str(), 6, str.length(), pattern.c_str(), pattern.length(), false);
    EXPECT_FALSE(isMatch);

    // like "xxx%"
    pattern = "^时欧基乌.*$";
    str = "时欧基乌";
    isMatch = LikeChar(str.c_str(), 6, str.length(), pattern.c_str(), pattern.length(), false);
    EXPECT_TRUE(isMatch);
}

TEST(FunctionTest, CastDecimal64To64)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int64_t input = 12345;
    int64_t result = CastDecimal64To64(contextPtr, input, 18, 0, false, 18, 3);
    EXPECT_EQ(result, 12345000);
    input = -19501040780019L;
    result = CastDecimal64To64(contextPtr, input, 17, 2, false, 18, 6);
    EXPECT_EQ(result, -195010407800190000L);
    input = 12395;
    result = CastDecimal64To64(contextPtr, input, 18, 2, false, 18, 0);
    EXPECT_EQ(result, 124);
    input = 12395;
    result = CastDecimal64To64(contextPtr, input, 18, 2, false, 5, 4);
    string message = context->GetError();
    EXPECT_EQ(message, "Cannot cast DECIMAL(18, 2) '123.95' to DECIMAL(5, 4)");
    delete context;
}

TEST(FunctionTest, CastDecimal128To128)
{
    int64_t high = 0;
    uint64_t low = 0;
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    CastDecimal128To128(contextPtr, 0, 123, 38, 0, false, 20, 0, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 123);
    CastDecimal128To128(contextPtr, 0, 129, 38, 2, false, 20, 1, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 13);
    CastDecimal128To128(contextPtr, 0, 129234454, 38, 0, false, 38, 30, &high, &low);
    string message = context->GetError();
    EXPECT_EQ(message, "Cannot cast DECIMAL(38, 0) '129234454' to DECIMAL(38, 30)");
    delete context;
}

TEST(FunctionTest, CastDecimal64To128)
{
    int64_t high = 0;
    uint64_t low = 0;
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    CastDecimal64To128(contextPtr, 123123, 17, 3, false, 38, 4, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 1231230);
    CastDecimal64To128(contextPtr, -123123, 17, 3, false, 38, 4, &high, &low);
    EXPECT_EQ(high, 1L << 63);
    EXPECT_EQ(low, 1231230);
    CastDecimal64To128(contextPtr, 123123, 17, 3, false, 38, 37, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 0);
    CastDecimal64To128(contextPtr, 123125, 17, 3, false, 38, 2, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 12313);
    delete context;
}

TEST(FunctionTest, CastDecimal128To64)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int64_t result = CastDecimal128To64(contextPtr, 0, 123, 38, 1, false, 18, 3);
    EXPECT_EQ(result, 12300);
    result = CastDecimal128To64(contextPtr, 1, 123, 38, 1, false, 18, 3);
    EXPECT_EQ(result, 0);
    result = CastDecimal128To64(contextPtr, 0, 123, 38, 1, false, 4, 3);
    EXPECT_EQ(result, 0);
    result = CastDecimal128To64(contextPtr, 1L << 63, 123, 38, 1, false, 18, 3);
    EXPECT_EQ(result, -12300);
    result = CastDecimal128To64(contextPtr, 0, 12366, 38, 2, false, 18, 1);
    EXPECT_EQ(result, 1237);
    Decimal128 x("12345120000000000000000");
    result = CastDecimal128To64(contextPtr, x.HighBits(), x.LowBits(), 38, 18, false, 7, 2);
    EXPECT_EQ(result, 1234512);
    delete context;
}

TEST(FunctionTest, CastIntToDecimal64)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int32_t s = 9123;
    int64_t result = CastIntToDecimal64(contextPtr, s, false, 17, 0);
    EXPECT_EQ(result, 9123);
    s = -45594;
    result = CastIntToDecimal64(contextPtr, s, false, 17, 3);
    EXPECT_EQ(result, -45594000);
    s = 0;
    result = CastIntToDecimal64(contextPtr, s, false, 17, 0);
    EXPECT_EQ(result, 0);
    s = 21'4748'3647;
    result = CastIntToDecimal64(contextPtr, s, false, 18, 0);
    EXPECT_EQ(result, 21'4748'3647);
    s = 21'4748'3647;
    result = CastIntToDecimal64(contextPtr, s, false, 5, 4);
    EXPECT_EQ(result, 0);
    s = 123;
    result = CastIntToDecimal64(contextPtr, s, false, 18, 19);
    EXPECT_EQ(result, 0);
    delete context;
}

TEST(FunctionTest, CastLongToDecimal64)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int64_t s = 9123;
    int64_t result = CastLongToDecimal64(contextPtr, s, false, 17, 0);
    EXPECT_EQ(result, 9123);
    s = -45594;
    result = CastLongToDecimal64(contextPtr, s, false, 17, 3);
    EXPECT_EQ(result, -45594000);
    s = 0;
    result = CastLongToDecimal64(contextPtr, s, false, 17, 0);
    EXPECT_EQ(result, 0);
    s = 123;
    result = CastLongToDecimal64(contextPtr, s, false, 18, 3);
    EXPECT_EQ(result, 123000);
    s = 922'3372'0368'5477'5807;
    result = CastLongToDecimal64(contextPtr, s, false, 18, 3);
    EXPECT_EQ(result, 0);
    s = 9223372036854775807;
    result = CastLongToDecimal64(contextPtr, s, false, 18, 0);
    EXPECT_EQ(result, 0);
    s = INT64_MIN;
    result = CastLongToDecimal64(contextPtr, s, false, 18, 0);
    EXPECT_EQ(result, 0);
    delete context;
}

TEST(FunctionTest, CastDoubleToDecimal64)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    double s = 9123;
    int64_t result = CastDoubleToDecimal64(contextPtr, s, false, 18, 0);
    EXPECT_EQ(result, 9123);
    s = -9123.973;
    result = CastDoubleToDecimal64(contextPtr, s, false, 18, 3);
    EXPECT_EQ(result, -9123973);
    s = 0;
    result = CastDoubleToDecimal64(contextPtr, s, false, 18, 3);
    EXPECT_EQ(result, 0);
    s = 1.11E100;
    result = CastDoubleToDecimal64(contextPtr, s, false, 18, 3);
    EXPECT_EQ(result, 0);
    s = -45.7;
    result = CastDoubleToDecimal64(contextPtr, s, false, 18, 0);
    EXPECT_EQ(result, -46);
    s = 1.17549E-38;
    result = CastDoubleToDecimal64(contextPtr, s, false, 18, 0);
    EXPECT_EQ(result, 0.00);
    result = CastDoubleToDecimal64(contextPtr, 0, false, 17, 16);
    EXPECT_EQ(result, 0);
    delete context;
}

TEST(FunctionTest, CastIntToDecimal128)
{
    int64_t high = 0;
    uint64_t low = 0;

    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int32_t s = 9123;
    CastIntToDecimal128(contextPtr, s, false, 38, 0, &high, &low);
    EXPECT_EQ(low, 9123);
    s = -45594;
    CastIntToDecimal128(contextPtr, s, false, 38, 3, &high, &low);
    EXPECT_EQ(low, 4559'4000);
    s = 0;
    CastIntToDecimal128(contextPtr, s, false, 38, 0, &high, &low);
    EXPECT_EQ(low, 0);
    s = 21'4748'3647;
    CastIntToDecimal128(contextPtr, s, false, 38, 0, &high, &low);
    EXPECT_EQ(low, 21'4748'3647);
    delete context;
}

TEST(FunctionTest, CastLongToDecimal128)
{
    int64_t outHigh = 0;
    uint64_t outLow = 0;
    Decimal128 expected;

    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);

    int64_t x = 999;
    CastLongToDecimal128(contextPtr, x, false, 2, 0, &outHigh, &outLow);
    EXPECT_TRUE(context->HasError());
    x = 10;
    CastLongToDecimal128(contextPtr, x, false, 2, 0, &outHigh, &outLow);
    EXPECT_EQ(outLow, 10);

    delete context;
}

TEST(FunctionTest, CastDoubleToDecimal128)
{
    int64_t high = 0;
    uint64_t low = 0;

    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    double s = 9123;
    CastDoubleToDecimal128(contextPtr, s, false, 18, 0, &high, &low);
    EXPECT_EQ(low, 9123);
    s = -9123.973;
    CastDoubleToDecimal128(contextPtr, s, false, 18, 3, &high, &low);
    EXPECT_EQ(low, 9123973);
    s = 0;
    CastDoubleToDecimal128(contextPtr, s, false, 18, 0, &high, &low);
    EXPECT_EQ(low, 0);
    s = 123.1119;
    CastDoubleToDecimal128(contextPtr, s, false, 18, 3, &high, &low);
    EXPECT_EQ(low, 123112);
    s = 1.11E100;
    CastDoubleToDecimal128(contextPtr, s, false, 18, 0, &high, &low);
    EXPECT_TRUE(context->HasError());
    delete context;
}

TEST(FunctionTest, CastDecimal64ToInt)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int32_t result = CastDecimal64ToInt(contextPtr, 100, 38, 0, false);
    EXPECT_EQ(result, 100);
    result = CastDecimal64ToInt(contextPtr, 99, 38, 1, false);
    EXPECT_EQ(result, 10);
    result = CastDecimal64ToInt(contextPtr, 100, 38, 0, false);
    EXPECT_EQ(result, 100);
    result = CastDecimal64ToInt(contextPtr, 8888, 38, 2, false);
    EXPECT_EQ(result, 89);
    result = CastDecimal64ToInt(contextPtr, -1736879480, 15, 0, false);
    EXPECT_EQ(result, -1736879480);
    delete context;
}

TEST(FunctionTest, CastDecimal64ToLong)
{
    int64_t result = CastDecimal64ToLong(100, 38, 0, false);
    EXPECT_EQ(result, 100);
    result = CastDecimal64ToLong(99, 38, 1, false);
    EXPECT_EQ(result, 10);
    result = CastDecimal64ToLong(100, 38, 0, false);
    EXPECT_EQ(result, 100);
    result = CastDecimal64ToLong(-8888, 38, 2, false);
    EXPECT_EQ(result, -89);
    result = CastDecimal64ToLong(INT64_MIN, 22, 0, false);
    EXPECT_EQ(result, INT64_MIN);
}

TEST(FunctionTest, CastDecimal64ToDouble)
{
    double result = CastDecimal64ToDouble(100, 38, 0, false);
    EXPECT_EQ(result, 100);
    result = CastDecimal64ToDouble(99, 38, 1, false);
    EXPECT_EQ(result, 9.9);
    result = CastDecimal64ToDouble(100, 38, 0, false);
    EXPECT_EQ(result, 100);
    result = CastDecimal64ToDouble(-8888, 38, 2, false);
    EXPECT_EQ(result, -88.88);
}

TEST(FunctionTest, CastDecimal128ToInt)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int32_t result = CastDecimal128ToInt(contextPtr, 0, 100, 38, 0, false);
    EXPECT_EQ(result, 100);
    result = CastDecimal128ToInt(contextPtr, 0, 99, 38, 1, false);
    EXPECT_EQ(result, 10);
    result = CastDecimal128ToInt(contextPtr, 1, 100, 38, 0, false);
    EXPECT_EQ(result, 0);
    result = CastDecimal128ToInt(contextPtr, 0, 8888, 38, 2, false);
    EXPECT_EQ(result, 89);
    delete context;
}

TEST(FunctionTest, CastDecimal128ToLong)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int64_t result = CastDecimal128ToLong(contextPtr, 0, 100, 38, 0, false);
    EXPECT_EQ(result, 100);
    result = CastDecimal128ToLong(contextPtr, 0, 99, 38, 0, false);
    EXPECT_EQ(result, 99);
    result = CastDecimal128ToLong(contextPtr, 1, 100, 38, 0, false);
    EXPECT_EQ(result, 0);
    result = CastDecimal128ToLong(contextPtr, 0, 8888, 38, 2, false);
    EXPECT_EQ(result, 89);
    result = CastDecimal128ToLong(contextPtr, 1, 1, 38, 20, false);
    EXPECT_EQ(result, 0);
    result = CastDecimal128ToLong(contextPtr, 1L << 63, 9223372036854775808UL, 22, 0, false);
    EXPECT_EQ(result, INT64_MIN);
    delete context;
}

TEST(FunctionTest, CastDecimal128ToDouble)
{
    double result = CastDecimal128ToDouble(0, 100, 38, 0, false);
    EXPECT_EQ(result, 100);
    result = CastDecimal128ToDouble(0, 534, 38, 3, false);
    EXPECT_EQ(result, 0.534);
    result = CastDecimal128ToDouble(0, 123, 38, 5, false);
    EXPECT_EQ(result, 0.00123);
    result = CastDecimal128ToDouble(0, 1234, 38, 2, false);
    EXPECT_EQ(result, 12.34);
    result = CastDecimal128ToDouble(0, 1234, 38, 2, false);
    EXPECT_EQ(result, 12.34);
}

TEST(FunctionTest, CastStringToLong)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    string s = "23423";
    int64_t result = CastStringToLong(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);

    EXPECT_EQ(result, 23423);
    s = "100123";
    result = CastStringToLong(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 100123);
    s = "-10078";
    result = CastStringToLong(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, -10078);
    s = "123.123";
    result = CastStringToLong(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 0);
    s = "9223372036854775807";
    result = CastStringToLong(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 922'3372'0368'5477'5807);
    s = "9223372036854775808";
    result = CastStringToLong(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 0);
    s = "-9223372036854775808";
    result = CastStringToLong(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, INT64_MIN);
    delete context;
}

TEST(FunctionTest, CastStringToInt)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);

    string s = "23423";
    int64_t result = CastStringToInt(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 23423);
    s = "100123";
    result = CastStringToInt(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 100123);
    s = "123.123";
    result = CastStringToInt(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 0);
    s = "2147483648";
    result = CastStringToInt(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 0);
    s = "2a147483648";
    result = CastStringToInt(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_TRUE(context->HasError());
    context->SetError();
    s = "-10078";
    result = CastStringToInt(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, -10078);
    EXPECT_FALSE(context->HasError());
    s = "2123123123147483648";
    result = CastStringToInt(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_TRUE(context->HasError());
    context->SetError();
    s = "-2123123123147483648";
    result = CastStringToInt(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_TRUE(context->HasError());
    context->SetError();
    s = "-2123123123147-483648";
    result = CastStringToInt(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_TRUE(context->HasError());
    delete context;
}

TEST(FunctionTest, CastStringToDouble)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    string s = "23423";
    double result = CastStringToInt(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 23423);
    s = "100123";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 100123);
    s = "-10078";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, -10078);
    s = "123.123";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 123.123);
    s = "-10.11";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, -10.11);
    s = "999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 1e+108);
    s = "1.111e202";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 1.111e202);
    s = "1.111e-202";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 1.111e-202);
    s = "1.7976931348623157E308";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 1.7976931348623157E308);

    s = "0";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    EXPECT_EQ(result, 0);

    s = "1.7976931348623157E2000";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    string message = context->GetError();
    EXPECT_EQ(message, "Cannot cast '1.7976931348623157E2000' to DOUBLE. Value too large.");

    s = "1.7976931348623157E-2000";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    message = context->GetError();
    EXPECT_EQ(message, "Cannot cast '1.7976931348623157E-2000' to DOUBLE. Value too large.");

    s = "1.7976931348623157E-200.0";
    result = CastStringToDouble(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false);
    message = context->GetError();
    EXPECT_EQ(message, "Cannot cast '1.7976931348623157E-200.0' to DOUBLE. Value is not a number.");

    delete context;
}

TEST(FunctionTest, CastStringToDecimal64)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    string s = "23423";
    int64_t result = CastStringToDecimal64(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 17, 0);
    EXPECT_EQ(result, 23423);
    s = "9223372036854775807";
    result = CastStringToDecimal64(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 17, 2);
    EXPECT_EQ(result, 0);
    s = "-10078";
    result = CastStringToDecimal64(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 17, 3);
    EXPECT_EQ(result, -10078000);
    s = "123.129";
    result = CastStringToDecimal64(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 17, 2);
    EXPECT_EQ(result, 12313);
    s = "-10.11";
    result = CastStringToDecimal64(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 16, 2);
    EXPECT_EQ(result, -1011);
    s = "123";
    result = CastStringToDecimal64(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 5, 4);
    EXPECT_EQ(result, 0);
    s = "999999999999999999";
    result = CastStringToDecimal64(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 18, 0);
    EXPECT_EQ(result, 99'9999'9999'9999'9999);
    s = "9999999999999999999";
    result = CastStringToDecimal64(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 18, 0);
    EXPECT_TRUE(context->HasError());
    context->SetError();
    s = "123a";
    result = CastStringToDecimal64(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 18, 0);
    EXPECT_TRUE(context->HasError());
    context->SetError();
    delete context;
}

TEST(FunctionTest, CastStringToDecimal128)
{
    int64_t high = 0;
    uint64_t low = 0;
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    bool *isNull = new bool(false);
    string s = "23423";
    CastStringToDecimal128(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 17, 0, &high, &low);
    EXPECT_EQ(low, 23423);
    EXPECT_EQ(high, 0);
    s = "-36893488147419103230";
    CastStringToDecimal128(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 37, 0, &high, &low);
    EXPECT_EQ(low, 1844'6744'0737'0955'1614UL);
    EXPECT_EQ(high, -922'3372'0368'5477'5807L);
    s = "-10078";
    CastStringToDecimal128(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 17, 0, &high, &low);
    EXPECT_EQ(low, 10078);
    EXPECT_EQ(high, 1L << 63);
    s = "123.999";
    CastStringToDecimal128(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 17, 0, &high, &low);
    EXPECT_EQ(low, 124);
    EXPECT_EQ(high, 0);
    s = "-10.11";
    CastStringToDecimal128(contextPtr, s.c_str(), static_cast<int32_t>(s.size()), false, 17, 0, &high, &low);
    EXPECT_EQ(low, 10);
    EXPECT_EQ(high, 1L << 63);
    s = "-10.1a1";
    CastStringToDecimal128RetNull(isNull, s.c_str(), static_cast<int32_t>(s.size()), 17, 0, &high, &low);
    EXPECT_TRUE(isNull);
    s = "9999999999999999999999999999999999999999";
    CastStringToDecimal128RetNull(isNull, s.c_str(), static_cast<int32_t>(s.size()), 17, 0, &high, &low);
    EXPECT_TRUE(isNull);
    delete context;
    delete isNull;
}

TEST(FunctionTest, CastDecimal64ToString)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int64_t input = 123;
    int32_t outLen = 0;
    const char *s = CastDecimal64ToString(contextPtr, input, 3, 0, false, &outLen);
    EXPECT_EQ(string(s, outLen), "123");
    input = 123423;
    s = CastDecimal64ToString(contextPtr, input, 6, 3, false, &outLen);
    EXPECT_EQ(string(s, outLen), "123.423");
    input = 1;
    s = CastDecimal64ToString(contextPtr, input, 10, 6, false, &outLen);
    EXPECT_EQ(string(s, outLen), "0.000001");
    delete context;
}

TEST(FunctionTest, CastDecimal128ToString)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);

    int32_t outLen = 0;
    const char *s = CastDecimal128ToString(contextPtr, 0, 123, 3, 0, false, &outLen);
    EXPECT_EQ(string(s, outLen), "123");
    s = CastDecimal128ToString(contextPtr, 0, 123423, 6, 3, false, &outLen);
    EXPECT_EQ(string(s, outLen), "123.423");
    s = CastDecimal128ToString(contextPtr, 0, 1, 10, 6, false, &outLen);
    EXPECT_EQ(string(s, outLen), "0.000001");
    s = CastDecimal128ToString(contextPtr, 1, 0, 38, 0, false, &outLen);
    EXPECT_EQ(string(s, outLen), "18446744073709551616");
    delete context;
}

// Decimal Operation
TEST(FunctionTest, DecimalAddOpeartion)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int64_t high;
    uint64_t low;

    // dec64 add dec64 return dec64
    int64_t result = AddDec64Dec64Dec64(contextPtr, 123, 3, 2, 321, 3, 1, 4, 2);
    EXPECT_EQ(result, 3333);
    result = AddDec64Dec64Dec64(contextPtr, 123, 3, 2, 321, 3, 1, 5, 3);
    EXPECT_EQ(result, 33330);
    // dec64 add dec128 return dec128
    AddDec64Dec128Dec128(contextPtr, 123, 3, 2, 0, 321, 3, 1, 4, 2, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 3333);
    AddDec64Dec128Dec128(contextPtr, 123, 3, 2, 0, 321, 3, 1, 5, 3, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 33330);
    // dec128 add dec64 return dec128
    AddDec128Dec64Dec128(contextPtr, 0, 123, 3, 2, 321, 3, 1, 4, 2, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 3333);
    AddDec128Dec64Dec128(contextPtr, 0, 123, 3, 2, 321, 3, 1, 5, 3, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 33330);
    // dec64 add dec64 return dec128
    AddDec64Dec128Dec128(contextPtr, 123, 3, 2, 0, 321, 3, 1, 4, 2, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 3333);
    AddDec64Dec128Dec128(contextPtr, 123, 3, 2, 0, 321, 3, 1, 5, 3, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 33330);
    // dec128 add dec128 return dec128
    AddDec128Dec128Dec128(contextPtr, 0, 123, 3, 2, 0, 321, 3, 1, 4, 2, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 3333);
    AddDec128Dec128Dec128(contextPtr, 0, 123, 3, 2, 0, 321, 3, 1, 5, 3, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 33330);
    Decimal128 right = Decimal128("99999999999999999999980000000000000000");
    AddDec128Dec64Dec128(contextPtr, right.HighBits(), right.LowBits(), 38, 6, 999999999999999999L, 18, 6, 38, 6, &high,
        &low);
    EXPECT_TRUE(context->HasError());
    delete context;
}

TEST(FunctionTest, DecimalMulOpeartion)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int64_t high;
    uint64_t low;

    // dec64 mul dec64 return dec64
    int64_t result = MulDec64Dec64Dec64(contextPtr, 123, 3, 2, 321, 3, 1, 5, 3);
    EXPECT_EQ(result, 39483);
    result = MulDec64Dec64Dec64(contextPtr, 123, 3, 2, 321, 3, 1, 6, 4);
    EXPECT_EQ(result, 394830);
    // dec64 mul dec64 return dec128
    MulDec64Dec64Dec128(contextPtr, 123, 3, 2, 321, 3, 1, 5, 3, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 39483);
    MulDec64Dec64Dec128(contextPtr, 123, 3, 2, 321, 3, 1, 6, 4, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 394830);
    // dec128 mul dec128 return dec128
    Decimal128 l = Decimal128("123456789123456456789");
    Decimal128 r = Decimal128("1234567891234567567891");
    MulDec128Dec128Dec128(contextPtr, l.HighBits(), l.LowBits(), 22, 6, r.HighBits(), r.LowBits(), 22, 6, 38, 6, &high,
        &low);
    EXPECT_EQ(Decimal128(high, low).ToString(), "152415787806736335252649604807436065");

    l = Decimal128("12345678912345623");
    r = Decimal128("1234567891234567567891");
    MulDec128Dec128Dec128(contextPtr, l.HighBits(), l.LowBits(), 17, 2, r.HighBits(), r.LowBits(), 22, 6, 38, 6, &high,
        &low);
    EXPECT_EQ(Decimal128(high, low).ToString(), "152415787806736055266232119611091911");

    MulDec128Dec128Dec128(contextPtr, 0, 123, 3, 2, 0, 321, 3, 1, 5, 3, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 39483);
    // dec64 mul dec128 return dec128
    MulDec64Dec128Dec128(contextPtr, 123, 3, 2, 0, 321, 3, 1, 5, 3, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 39483);
    MulDec64Dec128Dec128(contextPtr, 123, 3, 2, 0, 321, 3, 1, 6, 4, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 394830);
    // dec128 mul dec64 return dec128
    MulDec128Dec64Dec128(contextPtr, 0, 123, 3, 2, 321, 3, 1, 5, 3, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 39483);
    MulDec128Dec64Dec128(contextPtr, 0, 123, 3, 2, 321, 3, 1, 6, 4, &high, &low);
    EXPECT_EQ(high, 0);
    EXPECT_EQ(low, 394830);
    delete context;
}


TEST(FunctionTest, DecimalModOpeartion)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    // dec64 mod dec64 return dec64
    int64_t result = ModDec64Dec64Dec64(contextPtr, -1234500, 7, 2, 1234512, 7, 2, 7, 2);
    EXPECT_EQ(result, -1234500);
    int64_t high;
    uint64_t low;
    ModDec64Dec128Dec128(contextPtr, -1234500, 7, 2, 0, 1234512, 7, 2, 7, 2, &high, &low);
    EXPECT_EQ(high, 1L << 63);
    EXPECT_EQ(low, 1234500);
    Decimal128 right = Decimal128("250009700094102345239493000152399025");
    ModDec128Dec64Dec128(contextPtr, right.HighBits(), right.LowBits(), 36, 36, 7, 10, 0, 36, 36, &high, &low);
    EXPECT_EQ(Decimal128(high, low).ToString(), "250009700094102345239493000152399025");
    delete context;
}

TEST(FunctionTest, DecimalDivOpeartion)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    Decimal128 right = Decimal128("9999999999999999999999");
    Decimal128 left = Decimal128("99999999999999999999999999999999999999");
    int64_t high = 0;
    uint64_t low = 0;
    // dec128 mul dec128 return dec128
    DivDec128Dec128Dec128(contextPtr, left.HighBits(), left.LowBits(), 38, 16, right.HighBits(), right.LowBits(), 22, 6,
        38, 16, &high, &low);
    EXPECT_EQ(Decimal128(high, low).ToString(), "10000000000000000000001");
    DivDec64Dec64Dec128(contextPtr, -111, 7, 2, 50'0009'7000'0001'2345, 18, 18, 19, 18, &high, &low);
    EXPECT_EQ(Decimal128(high, low).ToString(), "-2219956932835448182");
    DivDec64Dec64Dec128(contextPtr, 1111111, 7, 2, 50'0009'7000'0001'2345, 18, 18, 19, 18, &high, &low);
    EXPECT_EQ(Decimal128(high, low).ToString(), "22221788897294843824062");
    DivDec64Dec64Dec128(contextPtr, 50'0009'7000'0001'2345, 18, 18, 1111111, 7, 2, 19, 18, &high, &low);
    EXPECT_EQ(Decimal128(high, low).ToString(), "45000877500089");
    DivDec128Dec128Dec128(contextPtr, 0, 100, 38, 0, 0, 3, 38, 0, 38, 3, &high, &low);
    right = Decimal128("-99999999999999999999999999999999999999");
    DivDec128Dec128Dec128(contextPtr, right.HighBits(), right.LowBits(), 38, 16, 0, 999999, 22, 6, 38, 16, &high, &low);
    EXPECT_TRUE(context->HasError());
    delete context;
}

TEST(FunctionTest, CastStringToNumericalType)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);

    // String To Int
    string s = "123";
    int32_t intResult = CastStringToInt(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(intResult, 123);

    s = "-123";
    intResult = CastStringToInt(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(intResult, -123);

    s = "0";
    intResult = CastStringToInt(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(intResult, 0);

    s = "-2147483648";
    intResult = CastStringToInt(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(intResult, -2147483648);

    s = "2147483647";
    intResult = CastStringToInt(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(intResult, 2147483647);

    // String To Long
    s = "123";
    int64_t longResult = CastStringToLong(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(longResult, 123);

    s = "-123";
    longResult = CastStringToLong(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(longResult, -123);

    s = "0";
    longResult = CastStringToLong(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(longResult, 0);

    s = "-9223372036854775808";
    longResult = CastStringToLong(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(longResult, INT64_MIN);

    s = "9223372036854775807";
    longResult = CastStringToLong(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(longResult, 9223372036854775807LL);

    // String To Long
    s = "123.123";
    double doubleResult = CastStringToDouble(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(doubleResult, 123.123);

    s = "-123.123";
    doubleResult = CastStringToDouble(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(doubleResult, -123.123);

    s = "0.0";
    doubleResult = CastStringToDouble(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(doubleResult, 0);

    s = "123e-123";
    doubleResult = CastStringToDouble(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(doubleResult, 123e-123);

    s = "-123e123";
    doubleResult = CastStringToDouble(contextPtr, s.c_str(), s.size(), false);
    EXPECT_EQ(doubleResult, -123e123);

    // Overflow
    s = "2147483648";
    intResult = CastStringToInt(contextPtr, s.c_str(), s.length(), false);
    EXPECT_EQ(context->GetError(), "Cannot cast '2147483648' to INTEGER. Value too large.");

    s = "-2147483649";
    intResult = CastStringToInt(contextPtr, s.c_str(), s.length(), false);
    EXPECT_EQ(context->GetError(), "Cannot cast '-2147483649' to INTEGER. Value too large.");

    s = "-9223372036854775809";
    longResult = CastStringToLong(contextPtr, s.c_str(), s.length(), false);
    EXPECT_EQ(context->GetError(), "Cannot cast '-9223372036854775809' to BIGINT. Value too large.");

    s = "9223372036854775808";
    longResult = CastStringToLong(contextPtr, s.c_str(), s.length(), false);
    EXPECT_EQ(context->GetError(), "Cannot cast '9223372036854775808' to BIGINT. Value too large.");

    delete context;
}

TEST(FunctionTest, CastDecimal64ToDoubleRetNull)
{
    bool isNull = false;
    double result = CastDecimal64ToDoubleRetNull(&isNull, 100, 38, 0);
    EXPECT_EQ(result, 100);
    result = CastDecimal64ToDoubleRetNull(&isNull, 99, 38, 1);
    EXPECT_EQ(result, 9.9);
    result = CastDecimal64ToDoubleRetNull(&isNull, 100, 38, 0);
    EXPECT_EQ(result, 100);
    result = CastDecimal64ToDoubleRetNull(&isNull, -8888, 38, 2);
    EXPECT_EQ(result, -88.88);
    result = CastDecimal64ToDoubleRetNull(&isNull, 12'3456'7890'1234'5612L, 18, 2);
    EXPECT_EQ(result, 1.234567890123456E15);
}

TEST(FunctionTest, CastDecimal128ToDoubleRetNull)
{
    bool isNull = false;
    double result = CastDecimal128ToDoubleRetNull(&isNull, 0, 100, 38, 0);
    EXPECT_EQ(result, 100);
    result = CastDecimal128ToDoubleRetNull(&isNull, 0, 534, 38, 3);
    EXPECT_EQ(result, 0.534);
    result = CastDecimal128ToDoubleRetNull(&isNull, 0, 123, 38, 5);
    EXPECT_EQ(result, 0.00123);
    result = CastDecimal128ToDoubleRetNull(&isNull, 0, 1234, 38, 2);
    EXPECT_EQ(result, 12.34);
    result = CastDecimal128ToDoubleRetNull(&isNull, 0, 1234, 38, 2);
    EXPECT_EQ(result, 12.34);
}

TEST(FunctionTest, CastDecimal64ToLongRetNull)
{
    bool isNull = false;
    int64_t result = CastDecimal64ToLongRetNull(&isNull, 100, 38, 0);
    EXPECT_EQ(result, 100);
    result = CastDecimal64ToLongRetNull(&isNull, 123999, 38, 3);
    EXPECT_EQ(result, 124);
    result = CastDecimal64ToLongRetNull(&isNull, 123111, 38, 3);
    EXPECT_EQ(result, 123);
}

TEST(FunctionTest, EvaluateHiveUdfSingle)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int32_t inputTypes[] = {OMNI_INT, OMNI_INT};
    int32_t retType = OMNI_INT;
    int32_t vecCount = 2;

    int32_t inputValue[2] = {3, 5};
    uint8_t inputNull[2] = {0, 0};
    int64_t inputLength = 0;
    int32_t outputValue;
    int8_t outputNull;
    int32_t outputLength;

    using namespace omniruntime::mock;
    using namespace testing::internal;
    using testing::_;
    using testing::Assign;
    using testing::DoAll;
    using testing::Return;
    JNIEnvMock *env = CreateJNIEnvMock();
    JniUtil::SetEnv(env);

    // for InitHiveUdf
    EXPECT_CALL(*env, FindClass(_)).WillRepeatedly(Return(nullptr));
    EXPECT_CALL(*env, ExceptionCheck()).WillRepeatedly(Return(false));
    EXPECT_CALL(*env, NewGlobalRef(_)).WillRepeatedly(Return(nullptr));
    EXPECT_CALL(*env, DeleteLocalRef(_)).WillRepeatedly(Return());
    EXPECT_CALL(*env, GetStaticMethodID(_, _, _)).WillRepeatedly(Return(nullptr));
    EXPECT_CALL(*env, GetStaticFieldID(_, _, _)).WillRepeatedly(Return(nullptr));

    // for EvaluateHiveUdfSingle
    EXPECT_CALL(*env, NewStringUTF(_)).WillOnce(Return(jstring("AddIntUDF")));
    EXPECT_CALL(*env, NewObjectArray(vecCount, _, _)).WillOnce(Return(nullptr));
    EXPECT_CALL(*env, GetStaticObjectField(_, _)).WillRepeatedly(Return(nullptr));
    EXPECT_CALL(*env, SetObjectArrayElement(_, _, _)).WillRepeatedly(Return());

    InAndOutputInfos infos {};
    EXPECT_CALL(*env, CallStaticVoidMethodV(_, _, infos))
        .WillOnce(DoAll(Assign((int32_t *)(&outputValue), 8), Assign((int8_t *)(&outputNull), 0)))
        .WillRepeatedly(Return());

    EvaluateHiveUdfSingle(contextPtr, "omniruntime.udf.AddIntUDF", inputTypes, retType, vecCount,
        reinterpret_cast<int64_t>(inputValue), reinterpret_cast<int64_t>(inputNull), inputLength,
        reinterpret_cast<int64_t>(&outputValue), reinterpret_cast<int64_t>(&outputNull),
        reinterpret_cast<int64_t>(&outputLength));

    ASSERT_EQ(outputValue, 8);
    ASSERT_EQ(outputNull, 0);

    delete context;
    DestroyJNIEnvMock(env);
}

TEST(FunctionTest, CastDecimal64ToInt_normal_when_engine_is_spark)
{
    ConfigUtil::SetRoundingRule(RoundingRule::DOWN);

    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    auto result = CastDecimal64ToInt(contextPtr, 123456, 4, 2, false);
    ASSERT_EQ(result, 1234);
    ASSERT_FALSE(context->HasError());

    ConfigUtil::SetRoundingRule(RoundingRule::HALF_UP);
    delete context;
}

TEST(FunctionTest, CastDecimal64ToInt_positive_overflow_when_engine_is_spark)
{
    ConfigUtil::SetRoundingRule(RoundingRule::DOWN);

    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    auto result = CastDecimal64ToInt(contextPtr, 1232147483667, 11, 2, false);
    ASSERT_EQ(result, 0);
    ASSERT_TRUE(context->HasError());

    ConfigUtil::SetRoundingRule(RoundingRule::HALF_UP);
    delete context;
}

TEST(FunctionTest, CastDecimal64ToInt_negative_overflow_when_engine_is_spark)
{
    ConfigUtil::SetRoundingRule(RoundingRule::DOWN);

    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    auto result = CastDecimal64ToInt(contextPtr, -1232147483667, 11, 2, false);
    ASSERT_EQ(result, 0);
    ASSERT_TRUE(context->HasError());

    ConfigUtil::SetRoundingRule(RoundingRule::HALF_UP);
    delete context;
}

TEST(FunctionTest, CastDecimal64ToLong_normal_when_engine_is_spark)
{
    ConfigUtil::SetRoundingRule(RoundingRule::DOWN);

    auto result = CastDecimal64ToLong(123456, 4, 2, false);
    ASSERT_EQ(result, 1234);

    ConfigUtil::SetRoundingRule(RoundingRule::HALF_UP);
}

TEST(FunctionTest, CastDecimal128ToInt_normal_when_engine_is_spark)
{
    ConfigUtil::SetRoundingRule(RoundingRule::DOWN);

    Decimal128 deci;
    int32_t precision = 0;
    int32_t scale = 0;
    DecimalOperations::StringToDecimal128("1234.56", deci, scale, precision);
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    auto result = CastDecimal128ToInt(contextPtr, deci.HighBits(), deci.LowBits(), precision, scale, false);
    ASSERT_EQ(result, 1234);
    ASSERT_FALSE(context->HasError());

    ConfigUtil::SetRoundingRule(RoundingRule::HALF_UP);
    delete context;
}

TEST(FunctionTest, CastDecimal128ToInt_positive_overflow_when_engine_is_spark)
{
    ConfigUtil::SetRoundingRule(RoundingRule::DOWN);

    Decimal128 deci;
    int32_t precision = 0;
    int32_t scale = 0;
    DecimalOperations::StringToDecimal128("12321474836.67", deci, scale, precision);
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    auto result = CastDecimal128ToInt(contextPtr, deci.HighBits(), deci.LowBits(), precision, scale, false);
    ASSERT_EQ(result, 0);
    ASSERT_TRUE(context->HasError());

    ConfigUtil::SetRoundingRule(RoundingRule::HALF_UP);
    delete context;
}

TEST(FunctionTest, CastDecimal128ToInt_negative_overflow_when_engine_is_spark)
{
    ConfigUtil::SetRoundingRule(RoundingRule::DOWN);

    Decimal128 deci;
    int32_t precision = 0;
    int32_t scale = 0;
    DecimalOperations::StringToDecimal128("-12321474836.67", deci, scale, precision);
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    auto result = CastDecimal128ToInt(contextPtr, deci.HighBits(), deci.LowBits(), precision, scale, false);
    ASSERT_EQ(result, 0);
    ASSERT_TRUE(context->HasError());

    ConfigUtil::SetRoundingRule(RoundingRule::HALF_UP);
    delete context;
}

TEST(FunctionTest, CastDecimal128ToLong_normal_when_engine_is_spark)
{
    ConfigUtil::SetRoundingRule(RoundingRule::DOWN);

    Decimal128 deci;
    int32_t precision = 0;
    int32_t scale = 0;
    DecimalOperations::StringToDecimal128("12321474836.67", deci, scale, precision);
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    auto result = CastDecimal128ToLong(contextPtr, deci.HighBits(), deci.LowBits(), precision, scale, false);
    ASSERT_EQ(result, 12321474836);
    ASSERT_FALSE(context->HasError());

    ConfigUtil::SetRoundingRule(RoundingRule::HALF_UP);
    delete context;
}

TEST(FunctionTest, CastDecimal128ToLong_positive_overflow_when_engine_is_spark)
{
    ConfigUtil::SetRoundingRule(RoundingRule::DOWN);

    Decimal128 deci;
    int32_t precision = 0;
    int32_t scale = 0;
    DecimalOperations::StringToDecimal128("12345678912345678912345.67", deci, scale, precision);
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    auto result = CastDecimal128ToLong(contextPtr, deci.HighBits(), deci.LowBits(), precision, scale, false);
    ASSERT_EQ(result, 0);
    ASSERT_TRUE(context->HasError());

    ConfigUtil::SetRoundingRule(RoundingRule::HALF_UP);
    delete context;
}

TEST(FunctionTest, CastDecimal128ToLong_negative_overflow_when_engine_is_spark)
{
    ConfigUtil::SetRoundingRule(RoundingRule::DOWN);

    Decimal128 deci;
    int32_t precision = 0;
    int32_t scale = 0;
    DecimalOperations::StringToDecimal128("-12345678912345678912345.67", deci, scale, precision);
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    auto result = CastDecimal128ToLong(contextPtr, deci.HighBits(), deci.LowBits(), precision, scale, false);
    ASSERT_EQ(result, 0);
    ASSERT_TRUE(context->HasError());

    ConfigUtil::SetRoundingRule(RoundingRule::HALF_UP);
    delete context;
}
}