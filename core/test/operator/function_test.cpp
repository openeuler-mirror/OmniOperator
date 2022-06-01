// /*
// * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
// * Description: ...
// */
#include "gtest/gtest.h"
#include "../util/test_util.h"
#include "../../src/operator/projection/projection.h"
#include "../../src/vector/vector_helper.h"
#include "../../src/codegen/functions/stringfunctions.h"
#include "../../src/codegen/functions/decimalfunctions.h"
#include "../../src/codegen/functions/mathfunctions.h"
#include "../../src/codegen/functions/murmur3_hash.h"
#include "../../src/codegen/functions/dictionaryfunctions.h"
#include "../../src/codegen/functions/context_helper.h"
#include "../../src/codegen/functions/varcharVectorfunctions.h"
#include "../../src/type/decimal_operations.h"
#include "../../src/type/decimal128.h"
#include <string>
#include <vector>
#include <chrono>
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
    string result(reinterpret_cast<char *>(temp), 0, len);
    EXPECT_EQ(result, "hello");
    len = varcharVector->GetValue(3, &temp);
    string result2(reinterpret_cast<char *>(temp), 0, len);
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
    EXPECT_EQ(0, Decimal128Compare(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0));

    op1 = DecimalOperations::UnscaledDecimal(1);
    op2 = DecimalOperations::UnscaledDecimal(5);
    EXPECT_EQ(-1, Decimal128Compare(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0));

    op1 = DecimalOperations::UnscaledDecimal(6);
    op2 = DecimalOperations::UnscaledDecimal(-8);
    EXPECT_EQ(1, Decimal128Compare(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0));
}

TEST(FunctionTest, AddDec128)
{
    Decimal128 op1;
    Decimal128 op2;
    Decimal128 expected;
    int64_t zHigh = 0;
    uint64_t zLow = 0;

    op1 = DecimalOperations::UnscaledDecimal(0);
    op2 = DecimalOperations::UnscaledDecimal(0);
    expected = DecimalOperations::UnscaledDecimal(0);
    AddDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(5);
    op2 = DecimalOperations::UnscaledDecimal(10);
    expected = DecimalOperations::UnscaledDecimal(15);
    AddDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-1);
    op2 = DecimalOperations::UnscaledDecimal(1);
    expected = DecimalOperations::UnscaledDecimal(0);
    AddDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-3);
    op2 = DecimalOperations::UnscaledDecimal(-4);
    expected = DecimalOperations::UnscaledDecimal(-7);
    AddDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());
}

TEST(FunctionTest, SubDec128)
{
    Decimal128 op1;
    Decimal128 op2;
    Decimal128 expected;
    int64_t zHigh = 0;
    uint64_t zLow = 0;

    op1 = DecimalOperations::UnscaledDecimal(0);
    op2 = DecimalOperations::UnscaledDecimal(0);
    expected = DecimalOperations::UnscaledDecimal(0);
    SubDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(10);
    op2 = DecimalOperations::UnscaledDecimal(5);
    expected = DecimalOperations::UnscaledDecimal(5);
    SubDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(5);
    op2 = DecimalOperations::UnscaledDecimal(10);
    expected = DecimalOperations::UnscaledDecimal(-5);
    SubDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-3);
    op2 = DecimalOperations::UnscaledDecimal(-4);
    expected = DecimalOperations::UnscaledDecimal(1);
    SubDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());
}

TEST(FunctionTest, MulDec128)
{
    Decimal128 op1;
    Decimal128 op2;
    Decimal128 expected;
    int64_t zHigh = 0;
    uint64_t zLow = 0;

    op1 = DecimalOperations::UnscaledDecimal(0);
    op2 = DecimalOperations::UnscaledDecimal(500);
    expected = DecimalOperations::UnscaledDecimal(0);
    MulDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(1);
    op2 = DecimalOperations::UnscaledDecimal(500);
    expected = DecimalOperations::UnscaledDecimal(500);
    MulDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(3);
    op2 = DecimalOperations::UnscaledDecimal(5);
    expected = DecimalOperations::UnscaledDecimal(15);
    MulDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-3);
    op2 = DecimalOperations::UnscaledDecimal(-4);
    expected = DecimalOperations::UnscaledDecimal(12);
    MulDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-3);
    op2 = DecimalOperations::UnscaledDecimal(4);
    expected = DecimalOperations::UnscaledDecimal(-12);
    MulDec128(op1.HighBits(), op1.LowBits(), 38, 0, op2.HighBits(), op2.LowBits(), 38, 0, 38, 0, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());
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
    DivDec128(contextptr, op1.HighBits(), op1.LowBits(), precision, scale, op2.HighBits(), op2.LowBits(), precision,
        scale, precision, scale, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-10);
    op2 = DecimalOperations::UnscaledDecimal(2);
    expected = DecimalOperations::UnscaledDecimal(-5);
    DivDec128(contextptr, op1.HighBits(), op1.LowBits(), precision, scale, op2.HighBits(), op2.LowBits(), precision,
        scale, precision, scale, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(-10);
    op2 = DecimalOperations::UnscaledDecimal(-2);
    expected = DecimalOperations::UnscaledDecimal(5);
    DivDec128(contextptr, op1.HighBits(), op1.LowBits(), precision, scale, op2.HighBits(), op2.LowBits(), precision,
        scale, precision, scale, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(7);
    op2 = DecimalOperations::UnscaledDecimal(3);
    expected = DecimalOperations::UnscaledDecimal(2);
    DivDec128(contextptr, op1.HighBits(), op1.LowBits(), precision, scale, op2.HighBits(), op2.LowBits(), precision,
        scale, precision, scale, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    op1 = DecimalOperations::UnscaledDecimal(8);
    op2 = DecimalOperations::UnscaledDecimal(3);
    expected = DecimalOperations::UnscaledDecimal(3);
    DivDec128(contextptr, op1.HighBits(), op1.LowBits(), precision, scale, op2.HighBits(), op2.LowBits(), precision,
        scale, precision, scale, &zHigh, &zLow);
    EXPECT_EQ(zHigh, expected.HighBits());
    EXPECT_EQ(zLow, expected.LowBits());

    delete context;
}

TEST(FunctionTest, CastInt64ToDecimal128)
{
    int64_t outHigh = 0;
    uint64_t outLow = 0;
    Decimal128 expected;

    for (int64_t x = -500; x <= 550; ++x) {
        expected = DecimalOperations::UnscaledDecimal(x);
        CastInt64ToDecimal128(x, 38, 0, &outHigh, &outLow);
        EXPECT_EQ(outHigh, expected.HighBits());
        EXPECT_EQ(outLow, expected.LowBits());
    }
}

TEST(FunctionTest, AbsDecimal128)
{
    int64_t outHigh = 0;
    uint64_t outLow = 0;
    Decimal128 test;
    Decimal128 expected;

    test = DecimalOperations::UnscaledDecimal(3);
    AbsDecimal128(test.HighBits(), test.LowBits(), 38, 0, 38, 0, &outHigh, &outLow);
    EXPECT_EQ(outHigh, test.HighBits());
    EXPECT_EQ(outLow, test.LowBits());

    test = DecimalOperations::UnscaledDecimal(-1);
    expected = DecimalOperations::UnscaledDecimal(1);
    AbsDecimal128(test.HighBits(), test.LowBits(), 38, 0, 38, 0, &outHigh, &outLow);
    EXPECT_EQ(outHigh, expected.HighBits());
    EXPECT_EQ(outLow, expected.LowBits());

    test = DecimalOperations::UnscaledDecimal(0);
    expected = DecimalOperations::UnscaledDecimal(0);
    AbsDecimal128(test.HighBits(), test.LowBits(), 38, 0, 38, 0, &outHigh, &outLow);
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

    result = ConcatCharChar(contextptr, "hello", 5, 5, "world", 5, 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatCharChar(contextptr, "hello", 5, 5, "world", 10, 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld     ");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharChar(contextptr, "hello", 10, 5, "world", 5, 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharChar(contextptr, "hello", 10, 5, "world", 5, 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharChar(contextptr, "", 0, 0, "", 0, 0, &outlen);
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

    result = ConcatStrChar(contextptr, "hello", 5, "world", 5, 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatStrChar(contextptr, "hello", 5, "world", 10, 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld     ");
    EXPECT_EQ(outlen, 15);

    result = ConcatStrChar(contextptr, "hello", 5, "world     ", 10, 10, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld     ");
    EXPECT_EQ(outlen, 15);

    result = ConcatStrChar(contextptr, "", 0, "", 0, 0, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = ConcatStrChar(contextptr, "hello", 5, "     ", 5, 0, &outlen);
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

    result = ConcatCharStr(contextptr, "hello", 5, 5, "world", 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatCharStr(contextptr, "hello", 10, 5, "world", 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharStr(contextptr, "hello     ", 10, 10, "world", 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharStr(contextptr, "", 0, 0, "", 0, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = ConcatCharStr(contextptr, "", 5, 0, "world", 5, &outlen);
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

    result = Substr(contextptr, str.c_str(), strlen, 1, strlen, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outlen, strlen);

    result = Substr(contextptr, str.c_str(), strlen, 1, 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "Magic");
    EXPECT_EQ(outlen, 5);

    result = Substr(contextptr, str.c_str(), strlen, 10, 10, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "nson 123@#");
    EXPECT_EQ(outlen, 10);

    result = Substr(contextptr, str.c_str(), strlen, -5, 7, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "23@#$");
    EXPECT_EQ(outlen, 5);

    result = Substr(contextptr, str.c_str(), strlen, 0, 0, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = Substr(contextptr, str.c_str(), strlen, strlen, strlen + 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "$");
    EXPECT_EQ(outlen, 1);
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

    result = SubstrChar(contextptr, str.c_str(), width, strlen, 1, strlen, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "Magic Johnson 123@#$");
    EXPECT_EQ(outlen, strlen);

    result = SubstrChar(contextptr, str.c_str(), width, strlen, 1, 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "Magic");
    EXPECT_EQ(outlen, 5);

    result = SubstrChar(contextptr, str.c_str(), width, strlen, 10, 10, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "nson 123@#");
    EXPECT_EQ(outlen, 10);

    result = SubstrChar(contextptr, str.c_str(), width, strlen, -5, 7, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "23@#$");
    EXPECT_EQ(outlen, 5);

    result = SubstrChar(contextptr, str.c_str(), width, strlen, 0, 0, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = SubstrChar(contextptr, str.c_str(), width, strlen, strlen, strlen + 5, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "$");
    EXPECT_EQ(outlen, 1);

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

    result = SubstrWithStart(contextptr, str.c_str(), strlen, 1, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outlen, strlen);

    result = SubstrWithStart(contextptr, str.c_str(), strlen, 9, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "123 $%^");
    EXPECT_EQ(outlen, 7);

    result = SubstrWithStart(contextptr, str.c_str(), strlen, -3, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "$%^");
    EXPECT_EQ(outlen, 3);

    result = SubstrWithStart(contextptr, str.c_str(), strlen, 0, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

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

    result = SubstrCharWithStart(contextptr, str.c_str(), width, strlen, 1, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "ABC efg 123 $%^");
    EXPECT_EQ(outlen, strlen);

    result = SubstrCharWithStart(contextptr, str.c_str(), width, strlen, 9, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "123 $%^");
    EXPECT_EQ(outlen, 7);

    result = SubstrCharWithStart(contextptr, str.c_str(), width, strlen, -3, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "$%^");
    EXPECT_EQ(outlen, 3);

    result = SubstrCharWithStart(contextptr, str.c_str(), width, strlen, 0, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);
    delete context;
}

TEST(FunctionTest, ToUpper)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    string test = "[\\]^_abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    string expected = "[\\]^_ABCDEFGHIJKLMNOPQRSTUVWXYZ ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    int32_t outLen = 0;
    const char *result = ToUpper(contextptr, test.c_str(), static_cast<int32_t>(test.length()), &outLen);
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
    const char *result = ToUpperChar(contextptr, test.c_str(), width, static_cast<int32_t>(test.length()), &outLen);
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

TEST(FunctionTest, Like)
{
    bool result = Like("hello", 5, "hello", 5);
    EXPECT_TRUE(result);

    result = Like("regex", 5, "rege(x(es)?|xps?)", 17);
    EXPECT_TRUE(result);

    result = Like("20500", 5, "\\d{5}(-\\d{4})?", 14);
    EXPECT_TRUE(result);
}

TEST(FunctionTest, ConcatStrStr)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    int outlen = 0;
    const char *result;
    string actual;

    result = ConcatStrStr(contextptr, "abc", 3, "defghi", 6, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "abcdefghi");
    EXPECT_EQ(outlen, 9);

    result = ConcatStrStr(contextptr, "hello", 5, "", 0, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "hello");
    EXPECT_EQ(outlen, 5);

    result = ConcatStrStr(contextptr, "", 0, "", 0, &outlen);
    actual = string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);
    delete context;
}

TEST(FunctionTest, CastString)
{
    // year-month-day
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    int32_t result = CastString(contextptr, "1970-01-03", 10);
    EXPECT_EQ(result, 2);
    result = CastString(contextptr, "1969-12-31", 10);
    EXPECT_EQ(result, -1);
    result = CastString(contextptr, "1980-01-01", 10);
    EXPECT_EQ(result, 3652);
    result = CastString(contextptr, "1453-05-29", 10);
    EXPECT_EQ(result, -188682);
    delete context;
}
}