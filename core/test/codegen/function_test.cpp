/*
* Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
* Description: function test
*/
#include <string>
#include <vector>
#include <chrono>
#include "gtest/gtest.h"
#include "expression/expressions.h"
#include "codegen/functions/stringfunctions.h"
#include "codegen/functions/mathfunctions.h"
#include "codegen/functions/murmur3_hash.h"
#include "codegen/functions/dictionaryfunctions.h"
#include "codegen/functions/varcharVectorfunctions.h"
#include "codegen/functions/udffunctions.h"
#include "vector/vector_helper.h"
#include "jni_mock.h"
#include "udf/cplusplus/jni_util.h"

namespace omniruntime {
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::expressions;
using namespace omniruntime::codegen::function;

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
    int32_t ids[] = { 0, 2, 4, 6, 8 };
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
    int32_t ids[] = { 0, 2, 4, 6, 8 };
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
    int32_t ids[] = { 0, 2, 4, 6, 8 };
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
    int32_t ids[] = { 0, 2, 4, 6, 8 };
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
        varcharVector->SetValue(i, reinterpret_cast<const unsigned char *>(std::string(i, 'a').c_str()), i);
    }
    int32_t ids[] = { 0, 2, 4, 6, 8 };
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
    std::string result(reinterpret_cast<char *>(temp), len);
    EXPECT_EQ(result, "hello");
    len = varcharVector->GetValue(3, &temp);
    std::string result2(reinterpret_cast<char *>(temp), len);
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
    bool isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10, CastInt32ToInt64(result));

    result = CastInt32ToInt64(test2);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24, result);

    result = CastInt32ToInt64(test3);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0, CastInt32ToInt64(result));

    result = CastInt32ToInt64(test4);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(std::numeric_limits<int32_t>::min(), result);

    result = CastInt32ToInt64(test5);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
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
    bool isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10, result);

    result = CastInt64ToInt32(test2);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24, result);

    result = CastInt64ToInt32(test3);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0, CastInt64ToInt32(result));

    result = CastInt64ToInt32(test4);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0, result);

    result = CastInt64ToInt32(test5);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
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
    bool isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10.00, result);

    result = CastInt32ToDouble(test2);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24.00, result);

    result = CastInt32ToDouble(test3);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0.00, result);

    result = CastInt32ToDouble(test4);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(static_cast<double>(std::numeric_limits<int32_t>::min()), result);

    result = CastInt32ToDouble(test5);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
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
    bool isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10.00, result);

    result = CastInt64ToDouble(test2);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24.00, result);

    result = CastInt64ToDouble(test3);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0.00, result);

    result = CastInt64ToDouble(test4);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(static_cast<double>(std::numeric_limits<int64_t>::min()), result);

    result = CastInt64ToDouble(test5);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
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
    auto result = CastDoubleToInt32HalfUp(test1);
    bool isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10, result);

    result = CastDoubleToInt32HalfUp(test2);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24, result);

    result = CastDoubleToInt32HalfUp(test3);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0, result);

    result = CastDoubleToInt32HalfUp(test4);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(113, result);

    result = CastDoubleToInt32HalfUp(test5);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
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
    auto result = CastDoubleToInt64HalfUp(test1);
    bool isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(10, result);

    result = CastDoubleToInt64HalfUp(test2);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(-24, result);

    result = CastDoubleToInt64HalfUp(test3);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(0, result);

    result = CastDoubleToInt64HalfUp(test4);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
    EXPECT_TRUE(isSameType);
    EXPECT_EQ(113, result);

    result = CastDoubleToInt64HalfUp(test5);
    isSameType = std::is_same<decltype(baseline), decltype(result)>::value;
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
 * std::string functions:
 */
TEST(FunctionTest, ConcatCharChar)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    int outlen = 0;
    const char *result;
    std::string actual;

    result = ConcatCharChar(contextptr, "hello", 5, 5, "world", 5, 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatCharChar(contextptr, "hello", 5, 5, "world", 10, 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatCharChar(contextptr, "hello", 10, 5, "world", 5, 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharChar(contextptr, "hello", 10, 5, "world", 5, 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharChar(contextptr, "", 0, 0, "", 0, 0, false, &outlen);
    actual = std::string(result, outlen);
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
    std::string actual;

    result = ConcatStrChar(contextptr, "hello", 5, "world", 5, 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatStrChar(contextptr, "hello", 5, "world", 10, 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatStrChar(contextptr, "hello", 5, "world     ", 10, 10, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "helloworld     ");
    EXPECT_EQ(outlen, 15);

    result = ConcatStrChar(contextptr, "", 0, "", 0, 0, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = ConcatStrChar(contextptr, "hello", 5, "     ", 5, 5, false, &outlen);
    actual = std::string(result, outlen);
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
    std::string actual;

    result = ConcatCharStr(contextptr, "hello", 5, 5, "world", 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "helloworld");
    EXPECT_EQ(outlen, 10);

    result = ConcatCharStr(contextptr, "hello", 10, 5, "world", 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharStr(contextptr, "hello     ", 10, 10, "world", 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "hello     world");
    EXPECT_EQ(outlen, 15);

    result = ConcatCharStr(contextptr, "", 0, 0, "", 0, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = ConcatCharStr(contextptr, "", 5, 0, "world", 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "     world");
    EXPECT_EQ(outlen, 10);
    delete context;
}

TEST(FunctionTest, Substr)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    std::string str = "Magic Johnson 123@#$";
    int32_t strlen = static_cast<int32_t>(str.length());
    int32_t outlen = 0;
    const char *result;
    std::string actual;

    result = SubstrEmptyString(contextptr, str.c_str(), strlen, 1, strlen, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outlen, strlen);

    result = SubstrEmptyString(contextptr, str.c_str(), strlen, 1, 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "Magic");
    EXPECT_EQ(outlen, 5);

    result = SubstrEmptyString(contextptr, str.c_str(), strlen, 10, 10, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "nson 123@#");
    EXPECT_EQ(outlen, 10);

    result = SubstrEmptyString(contextptr, str.c_str(), strlen, -5, 7, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "23@#$");
    EXPECT_EQ(outlen, 5);

    result = SubstrEmptyString(contextptr, str.c_str(), strlen, 0, 0, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = SubstrEmptyString(contextptr, str.c_str(), strlen, strlen, strlen + 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "$");
    EXPECT_EQ(outlen, 1);
    delete context;
}

TEST(FunctionTest, SubstrZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t strLen = static_cast<int32_t>(str.length());
    int32_t outLen = 0;
    const char *result;
    std::string actual;

    result = SubstrEmptyString(contextPtr, str.c_str(), strLen, 1, 37, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    result = SubstrEmptyString(contextPtr, str.c_str(), strLen, 1, 5, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "时欧基乌斯");
    EXPECT_EQ(outLen, 15);

    result = SubstrEmptyString(contextPtr, str.c_str(), strLen, 10, 10, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "hello! 回复哦");
    EXPECT_EQ(outLen, 16);

    result = SubstrEmptyString(contextPtr, str.c_str(), strLen, -5, 7, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "色的圣诞袜");
    EXPECT_EQ(outLen, 15);

    result = SubstrEmptyString(contextPtr, str.c_str(), strLen, 0, 0, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrEmptyString(contextPtr, str.c_str(), strLen, 37, strLen + 5, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "袜");
    EXPECT_EQ(outLen, 3);

    result = SubstrEmptyString(contextPtr, str.c_str(), strLen, -38, 10, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrEmptyString(contextPtr, str.c_str(), strLen, -37, 37, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    delete context;
}

TEST(FunctionTest, SubstrChar)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    std::string str = "Magic Johnson 123@#$       ";
    int32_t width = static_cast<int32_t>(str.length());
    int32_t strlen = width - 7;
    int32_t outlen = 0;
    const char *result;
    std::string actual;

    result = SubstrCharEmptyString(contextptr, str.c_str(), width, strlen, 1, strlen, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "Magic Johnson 123@#$");
    EXPECT_EQ(outlen, strlen);

    result = SubstrCharEmptyString(contextptr, str.c_str(), width, strlen, 1, 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "Magic");
    EXPECT_EQ(outlen, 5);

    result = SubstrCharEmptyString(contextptr, str.c_str(), width, strlen, 10, 10, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "nson 123@#");
    EXPECT_EQ(outlen, 10);

    result = SubstrCharEmptyString(contextptr, str.c_str(), width, strlen, -5, 7, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "23@#$");
    EXPECT_EQ(outlen, 5);

    result = SubstrCharEmptyString(contextptr, str.c_str(), width, strlen, 0, 0, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    result = SubstrCharEmptyString(contextptr, str.c_str(), width, strlen, strlen, strlen + 5, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "$");
    EXPECT_EQ(outlen, 1);

    delete context;
}

TEST(FunctionTest, SubstrCharZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t width = 37;
    int32_t strLen = str.length();
    int32_t outLen = 0;
    const char *result;
    std::string actual;

    result = SubstrCharEmptyString(contextPtr, str.c_str(), width, strLen, 1, 37, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    result = SubstrCharEmptyString(contextPtr, str.c_str(), width, strLen, 1, 5, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "时欧基乌斯");
    EXPECT_EQ(outLen, 15);

    result = SubstrCharEmptyString(contextPtr, str.c_str(), width, strLen, 10, 10, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "hello! 回复哦");
    EXPECT_EQ(outLen, 16);

    result = SubstrCharEmptyString(contextPtr, str.c_str(), width, strLen, -5, 7, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "色的圣诞袜");
    EXPECT_EQ(outLen, 15);

    result = SubstrCharEmptyString(contextPtr, str.c_str(), width, strLen, 0, 0, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrCharEmptyString(contextPtr, str.c_str(), width, strLen, 37, strLen + 5, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "袜");
    EXPECT_EQ(outLen, 3);

    result = SubstrCharEmptyString(contextPtr, str.c_str(), width, strLen, -38, 10, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrCharEmptyString(contextPtr, str.c_str(), width, strLen, -37, 37, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    delete context;
}

TEST(FunctionTest, SubstrWithStart)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    std::string str = "ABC efg 123 $%^";
    int32_t strlen = static_cast<int32_t>(str.length());
    int32_t outlen = 0;
    const char *result;
    std::string actual;

    result = SubstrWithStartEmptyString(contextptr, str.c_str(), strlen, 1, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outlen, strlen);

    result = SubstrWithStartEmptyString(contextptr, str.c_str(), strlen, 9, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "123 $%^");
    EXPECT_EQ(outlen, 7);

    result = SubstrWithStartEmptyString(contextptr, str.c_str(), strlen, -3, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "$%^");
    EXPECT_EQ(outlen, 3);

    result = SubstrWithStartEmptyString(contextptr, str.c_str(), strlen, 0, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);

    delete context;
}

TEST(FunctionTest, SubstrWithZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t strLen = static_cast<int32_t>(str.length());
    int32_t outLen = 0;
    const char *result;
    std::string actual;

    result = SubstrWithStartEmptyString(contextPtr, str.c_str(), strLen, 1, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    result = SubstrWithStartEmptyString(contextPtr, str.c_str(), strLen, 9, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, " hello! 回复哦黑色的and magic粉色的圣诞袜");
    EXPECT_EQ(outLen, 53);

    result = SubstrWithStartEmptyString(contextPtr, str.c_str(), strLen, -3, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "圣诞袜");
    EXPECT_EQ(outLen, 9);

    result = SubstrWithStartEmptyString(contextPtr, str.c_str(), strLen, 0, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrWithStartEmptyString(contextPtr, str.c_str(), strLen, 37, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "袜");
    EXPECT_EQ(outLen, 3);

    result = SubstrWithStartEmptyString(contextPtr, str.c_str(), strLen, -38, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrWithStartEmptyString(contextPtr, str.c_str(), strLen, -37, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    delete context;
}

TEST(FunctionTest, SubstrWithZhForSpark)
{
    ConfigUtil::SetNegativeStartIndexOutOfBoundsRule(NegativeStartIndexOutOfBoundsRule::INTERCEPT_FROM_BEYOND);
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    std::string str = "时欧基乌斯侧后解 h";
    int32_t strLen = static_cast<int32_t>(str.length());
    int32_t outLen = 0;
    const char *result;
    std::string actual;

    result = SubstrWithStartInterceptFromBeyond(contextPtr, str.c_str(), strLen, -15, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    result = SubstrInterceptFromBeyond(contextPtr, str.c_str(), strLen, -15, 5, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrInterceptFromBeyond(contextPtr, str.c_str(), strLen, -15, 6, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "时");
    EXPECT_EQ(outLen, 3);

    result = SubstrInterceptFromBeyond(contextPtr, str.c_str(), strLen, -15, 14, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "时欧基乌斯侧后解 ");
    EXPECT_EQ(outLen, 25);

    result = SubstrInterceptFromBeyond(contextPtr, str.c_str(), strLen, -15, 20, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "时欧基乌斯侧后解 h");
    EXPECT_EQ(outLen, 26);

    std::string strEn = "apple";
    result = SubstrInterceptFromBeyond(contextPtr, strEn.c_str(), static_cast<int32_t>(strEn.length()), -7, 3, false,
        &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "a");

    result = SubstrWithStartInterceptFromBeyond(contextPtr, strEn.c_str(), static_cast<int32_t>(strEn.length()), -7,
        false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "apple");

    ConfigUtil::SetNegativeStartIndexOutOfBoundsRule(NegativeStartIndexOutOfBoundsRule::EMPTY_STRING);
    delete context;
}

TEST(FunctionTest, SubstrCharWithStart)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    std::string str = "ABC efg 123 $%^        ";
    int32_t width = static_cast<int32_t>(str.length());
    int32_t outlen = 0;
    int32_t strlen = width - 8;
    const char *result;
    std::string actual;

    result = SubstrCharWithStartEmptyString(contextptr, str.c_str(), width, strlen, 1, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "ABC efg 123 $%^");
    EXPECT_EQ(outlen, strlen);

    result = SubstrCharWithStartEmptyString(contextptr, str.c_str(), width, strlen, 9, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "123 $%^");
    EXPECT_EQ(outlen, 7);

    result = SubstrCharWithStartEmptyString(contextptr, str.c_str(), width, strlen, -3, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "$%^");
    EXPECT_EQ(outlen, 3);

    result = SubstrCharWithStartEmptyString(contextptr, str.c_str(), width, strlen, 0, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outlen, 0);
    delete context;
}

TEST(FunctionTest, SubstrCharWithStartZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    int32_t width = static_cast<int32_t>(str.length());
    int32_t outLen = 0;
    int32_t strLen = width;
    const char *result;
    std::string actual;

    result = SubstrCharWithStartEmptyString(contextPtr, str.c_str(), width, strLen, 1, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    result = SubstrCharWithStartEmptyString(contextPtr, str.c_str(), width, strLen, 9, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, " hello! 回复哦黑色的and magic粉色的圣诞袜");
    EXPECT_EQ(outLen, 53);

    result = SubstrCharWithStartEmptyString(contextPtr, str.c_str(), width, strLen, -3, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "圣诞袜");
    EXPECT_EQ(outLen, 9);

    result = SubstrCharWithStartEmptyString(contextPtr, str.c_str(), width, strLen, 0, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrCharWithStartEmptyString(contextPtr, str.c_str(), width, strLen, 37, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "袜");
    EXPECT_EQ(outLen, 3);

    result = SubstrCharWithStartEmptyString(contextPtr, str.c_str(), width, strLen, -38, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "");
    EXPECT_EQ(outLen, 0);

    result = SubstrCharWithStartEmptyString(contextPtr, str.c_str(), width, strLen, -37, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, str);
    EXPECT_EQ(outLen, strLen);

    delete context;
}

TEST(FunctionTest, ToUpperStr)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    std::string test = "[\\]^_abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    std::string expected = "[\\]^_ABCDEFGHIJKLMNOPQRSTUVWXYZ ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    int32_t outLen = 0;
    const char *result = ToUpperStr(contextptr, test.c_str(), static_cast<int32_t>(test.length()), false, &outLen);
    std::string actual = std::string(result, outLen);
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(outLen, 62);
    delete context;
}

TEST(FunctionTest, ToUpperChar)
{
    auto context = new ExecutionContext();
    int64_t contextptr = reinterpret_cast<int64_t>(context);
    std::string test = "[\\]^_abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    std::string expected = "[\\]^_ABCDEFGHIJKLMNOPQRSTUVWXYZ ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    int32_t width = 100;
    int32_t outLen = 0;
    const char *result =
        ToUpperChar(contextptr, test.c_str(), width, static_cast<int32_t>(test.length()), false, &outLen);
    std::string actual = std::string(result, outLen);
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(outLen, 62);
    delete context;
}

TEST(FunctionTest, ToLowerStr)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    std::string test = "[\\]^_abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    std::string expected = "[\\]^_abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz{|}.";
    int32_t outLen = 0;
    const char *result = ToLowerStr(contextPtr, test.c_str(), static_cast<int32_t>(test.length()), false, &outLen);
    std::string actual = std::string(result, outLen);
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(outLen, 62);
    delete context;
}

TEST(FunctionTest, ToLowerChar)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    std::string test = "[\\]^_abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}.";
    std::string expected = "[\\]^_abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz{|}.";
    int32_t width = 100;
    int32_t outLen = 0;
    const char *result =
        ToLowerChar(contextPtr, test.c_str(), width, static_cast<int32_t>(test.length()), false, &outLen);
    std::string actual = std::string(result, outLen);
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(outLen, 62);
    delete context;
}

TEST(FunctionTest, StrCompare)
{
    int result = StrCompare("abcd EFGH 123 $%^", 17, "abcd EFGH 123 $%^", 17);
    EXPECT_EQ(result, std::string("abcd EFGH 123 $%^").compare(std::string("abcd EFGH 123 $%^")));

    result = StrCompare("five", 4, "four", 4);
    EXPECT_EQ(result, std::string("five").compare(std::string("four")));

    result = StrCompare("five", 4, "FIVE", 4);
    EXPECT_EQ(result, std::string("five").compare(std::string("FIVE")));

    result = StrCompare("test", 4, "testing", 7);
    EXPECT_EQ(result, std::string("test").compare(std::string("testing")));

    result = StrCompare("racecar", 7, "race", 4);
    EXPECT_EQ(result, std::string("racecar").compare(std::string("race")));
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
    std::string actual;

    result = ConcatStrStr(contextptr, "abc", 3, "defghi", 6, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "abcdefghi");
    EXPECT_EQ(outlen, 9);

    result = ConcatStrStr(contextptr, "hello", 5, "", 0, false, &outlen);
    actual = std::string(result, outlen);
    EXPECT_EQ(actual, "hello");
    EXPECT_EQ(outlen, 5);

    result = ConcatStrStr(contextptr, "", 0, "", 0, false, &outlen);
    actual = std::string(result, outlen);
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
    int32_t result = CastStringToDateAllowReducePrecison(contextPtr, "1970-01-03", 10, false);
    EXPECT_EQ(result, 2);
    result = CastStringToDateAllowReducePrecison(contextPtr, "1969-12-31", 10, false);
    EXPECT_EQ(result, -1);
    result = CastStringToDateAllowReducePrecison(contextPtr, "1980-01-01", 10, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDateAllowReducePrecison(contextPtr, "1980-01-01 12345", 16, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDateAllowReducePrecison(contextPtr, "1980-1-1", 8, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDateAllowReducePrecison(contextPtr, "1980-1-01", 9, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDateAllowReducePrecison(contextPtr, "1980-1-1 123", 12, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDateAllowReducePrecison(contextPtr, "1980-01-1", 9, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDateAllowReducePrecison(contextPtr, "1980-01", 7, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDateAllowReducePrecison(contextPtr, "1980", 4, false);
    EXPECT_EQ(result, 3652);
    result = CastStringToDateAllowReducePrecison(contextPtr, "1453-05-29", 10, false);
    EXPECT_EQ(result, -188682);
    result = CastStringToDateAllowReducePrecison(contextPtr, "   1453-05-29   ", 16, false);
    EXPECT_EQ(result, -188682);
    result = CastStringToDateAllowReducePrecison(contextPtr, "   1 453-05-29   ", 16, false);
    EXPECT_EQ(result, -1);

    result = CastStringToDateAllowReducePrecison(contextPtr, "1996-09  ", 9, false);
    EXPECT_EQ(result, 9740);
    result = CastStringToDateAllowReducePrecison(contextPtr, "1996-09-30", 10, false);
    EXPECT_EQ(result, 9769);

    bool isNull = false;
    result = CastStringToDateRetNullAllowReducePrecison(&isNull, "   1453- 05-29    ", 16);
    EXPECT_EQ(result, -1);
    result = CastStringToDateRetNullAllowReducePrecison(&isNull, "1453-05-29", 10);
    EXPECT_EQ(result, -188682);
    result = CastStringToDateRetNullAllowReducePrecison(&isNull, "   1453-05-29   ", 16);
    EXPECT_EQ(result, -188682);
    ConfigUtil::SetStringToDateFormatRule(StringToDateFormatRule::NOT_ALLOW_REDUCED_PRECISION);
    delete context;
}

TEST(FunctionTest, LengthChar)
{
    std::string test = "abcd";
    int32_t width = 10;
    auto len = LengthChar(test.c_str(), width, test.length(), false);
    EXPECT_EQ(len, 10);
}

TEST(FunctionTest, LengthStr)
{
    std::string test = "abcd";
    auto len = LengthStr(test.c_str(), test.length(), false);
    EXPECT_EQ(len, 4);
}

TEST(FunctionTest, LengthStrZh)
{
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    auto len = LengthStr(str.c_str(), str.length(), false);
    EXPECT_EQ(len, 37);
}

TEST(FunctionTest, ReplaceStrStrStrWithRep)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int32_t outLen = 0;

    std::string str = "operator1";
    std::string searchStr = "o";
    std::string replaceStr = "**";
    auto result = ReplaceStrStrStrWithRepReplace(contextPtr, str.c_str(), str.length(), searchStr.c_str(),
        searchStr.length(),
        replaceStr.c_str(), replaceStr.length(), false, &outLen);
    std::string expected = "**perat**r1";
    EXPECT_EQ(outLen, 11);
    EXPECT_EQ(std::string(result, outLen), expected);

    str = "operator2";
    searchStr = "";
    replaceStr = "*";
    result = ReplaceStrStrStrWithRepReplace(contextPtr, str.c_str(), str.length(), searchStr.c_str(),
        searchStr.length(),
        replaceStr.c_str(), replaceStr.length(), false, &outLen);
    expected = "*o*p*e*r*a*t*o*r*2*";
    EXPECT_EQ(outLen, 19);
    EXPECT_EQ(std::string(result, outLen), expected);

    str = "operator3";
    searchStr = "era";
    replaceStr = "ER";
    result = ReplaceStrStrStrWithRepReplace(contextPtr, str.c_str(), str.length(), searchStr.c_str(),
        searchStr.length(),
        replaceStr.c_str(), replaceStr.length(), false, &outLen);
    expected = "opERtor3";
    EXPECT_EQ(outLen, 8);
    EXPECT_EQ(std::string(result, outLen), expected);
    delete context;
}

TEST(FunctionTest, ReplaceStrStrWithoutRep)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int32_t outLen = 0;

    std::string str = "operator1";
    std::string searchStr = "o";
    auto result = ReplaceStrStrWithoutRepReplace(contextPtr, str.c_str(), str.length(), searchStr.c_str(),
        searchStr.length(),
        false, &outLen);
    std::string expected = "peratr1";
    EXPECT_EQ(outLen, 7);
    EXPECT_EQ(std::string(result, outLen), expected);

    str = "operator2";
    searchStr = "";
    result = ReplaceStrStrWithoutRepReplace(contextPtr, str.c_str(), str.length(), searchStr.c_str(),
        searchStr.length(),
        false, &outLen);
    expected = "operator2";
    EXPECT_EQ(outLen, 9);
    EXPECT_EQ(std::string(result, outLen), expected);

    str = "operator3";
    searchStr = "era";
    result = ReplaceStrStrWithoutRepReplace(contextPtr, str.c_str(), str.length(), searchStr.c_str(),
        searchStr.length(),
        false, &outLen);
    expected = "optor3";
    EXPECT_EQ(outLen, 6);
    EXPECT_EQ(std::string(result, outLen), expected);
    delete context;
}

TEST(FunctionTest, ReplaceStrCharStr)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int32_t outLen = 0;

    std::string str[] = { "", " varchar2", "", "varchar4", "varchar5", "varchar6", "varchar7" };
    std::string searchStr[] =
        { "          ", " char200  ", "char300   ", "char400   ", "char500   ", "char600   ", "char700   " };
    std::string replaceStr[] = { "", " varchar2", "", "varchar4", "varchar5", "varchar6", "varchar7" };
    int32_t resultLen[] = { 0, 9, 0, 8, 8, 8, 8 };
    std::string expected[] = { "", " varchar2", "", "varchar4", "varchar5", "varchar6", "varchar7" };

    for (int32_t i = 0; i < 7; i++) {
        auto result = ReplaceStrStrStrWithRepReplace(contextPtr, str[i].c_str(), str[i].length(), searchStr[i].c_str(),
            searchStr[i].length(), replaceStr[i].c_str(), replaceStr[i].length(), false, &outLen);
        EXPECT_EQ(outLen, resultLen[i]);
        EXPECT_EQ(std::string(result, outLen), expected[i]);
    }
    delete context;
}

TEST(FunctionTest, ReplaceCharCharChar)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int32_t outLen = 0;

    std::string str[] = { "          ", " char200  ", "          ", "char400   ", "char500   ", "char600   ",
        "char700   " };
    std::string searchStr[] =
        { "cha1     ", " char2     ", "char3     ", "char4     ", "char5     ", "char6     ", "char7     " };
    std::string replaceStr[] =
        { "varchar100", "varchar200", "varchar300", "varchar400", "varchar500", "varchar600", "varchar700" };
    int32_t resultLen[] = { 10, 10, 10, 10, 10, 10, 10 };
    std::string expected[] =
        { "          ", " char200  ", "          ", "char400   ", "char500   ", "char600   ", "char700   " };

    for (int32_t i = 0; i < 7; i++) {
        auto result = ReplaceStrStrStrWithRepReplace(contextPtr, str[i].c_str(), str[i].length(), searchStr[i].c_str(),
            searchStr[i].length(), replaceStr[i].c_str(), replaceStr[i].length(), false, &outLen);
        EXPECT_EQ(outLen, resultLen[i]);
        EXPECT_EQ(std::string(result, outLen), expected[i]);
    }
    delete context;
}

TEST(FunctionTest, ReplaceStrStrStrZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int32_t outLen = 0;

    std::vector<std::string> str { "", "粉色的圣诞袜", "apple", "粉色de圣诞袜" };
    std::vector<std::string> searchStr { "", "粉色", "pp", "de圣" };
    std::vector<std::string> replaceStr { "", "黑色", "*w*", "*的*" };

    auto result1 = ReplaceStrStrStrWithRepReplace(contextPtr, str[2].c_str(), str[2].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[2].c_str(), replaceStr[2].length(), false, &outLen);
    std::string expected = "*w*a*w*p*w*p*w*l*w*e*w*";
    EXPECT_EQ(outLen, 23);
    EXPECT_EQ(std::string(result1, outLen), expected);

    auto result2 = ReplaceStrStrStrWithRepReplace(contextPtr, str[1].c_str(), str[1].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[2].c_str(), replaceStr[2].length(), false, &outLen);
    expected = "*w*粉*w*色*w*的*w*圣*w*诞*w*袜*w*";
    EXPECT_EQ(outLen, 39);
    EXPECT_EQ(std::string(result2, outLen), expected);

    auto result3 = ReplaceStrStrStrWithRepReplace(contextPtr, str[3].c_str(), str[3].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[2].c_str(), replaceStr[2].length(), false, &outLen);
    expected = "*w*粉*w*色*w*d*w*e*w*圣*w*诞*w*袜*w*";
    EXPECT_EQ(outLen, 41);
    EXPECT_EQ(std::string(result3, outLen), expected);

    auto result4 = ReplaceStrStrStrWithRepReplace(contextPtr, str[3].c_str(), str[3].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[3].c_str(), replaceStr[3].length(), false, &outLen);
    expected = "*的*粉*的*色*的*d*的*e*的*圣*的*诞*的*袜*的*";
    EXPECT_EQ(outLen, 57);
    EXPECT_EQ(std::string(result4, outLen), expected);

    auto result5 = ReplaceStrStrStrWithRepReplace(contextPtr, str[3].c_str(), str[3].length(), searchStr[3].c_str(),
        searchStr[3].length(), replaceStr[3].c_str(), replaceStr[3].length(), false, &outLen);
    expected = "粉色*的*诞袜";
    EXPECT_EQ(outLen, 17);
    EXPECT_EQ(std::string(result5, outLen), expected);

    auto result6 = ReplaceStrStrStrWithRepReplace(contextPtr, str[0].c_str(), str[0].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[0].c_str(), replaceStr[0].length(), false, &outLen);
    expected = "";
    EXPECT_EQ(outLen, 0);
    EXPECT_EQ(std::string(result6, outLen), expected);

    auto result7 = ReplaceStrStrStrWithRepReplace(contextPtr, str[3].c_str(), str[3].length(), searchStr[0].c_str(),
        searchStr[0].length(), replaceStr[0].c_str(), replaceStr[0].length(), false, &outLen);
    expected = "粉色de圣诞袜";
    EXPECT_EQ(outLen, 17);
    EXPECT_EQ(std::string(result7, outLen), expected);
    delete context;
}

TEST(FunctionTest, ConcatStrStrZh)
{
    auto context = new ExecutionContext();
    int64_t contextPtr = reinterpret_cast<int64_t>(context);
    int outLen = 0;
    const char *result;
    std::string actual;

    result = ConcatStrStr(contextPtr, "你是Chinese?", 14, "Yes我是", 9, false, &outLen);
    actual = std::string(result, outLen);
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
    std::string actual;

    result = ConcatCharChar(contextPtr, "粉色de圣诞袜", 7, 17, "*黑色*", 4, 8, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "粉色de圣诞袜*黑色*");
    EXPECT_EQ(outLen, 25);

    result = ConcatCharChar(contextPtr, "Hei你好吗", 8, 12, "Oh我很好", 8, 11, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "Hei你好吗  Oh我很好");
    EXPECT_EQ(outLen, 25);

    result = ConcatCharChar(contextPtr, "Hei你好吗   ", 10, 15, "Oh我很好  ", 8, 13, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "Hei你好吗    Oh我很好  ");
    EXPECT_EQ(outLen, 29);

    result = ConcatCharChar(contextPtr, "   Hei你好吗", 12, 15, "   Oh我很好", 12, 14, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "   Hei你好吗      Oh我很好");
    EXPECT_EQ(outLen, 32);

    result = ConcatCharChar(contextPtr, "Hei   你好吗", 12, 15, "Oh   我很好", 8, 14, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "Hei   你好吗   Oh   我很好");
    EXPECT_EQ(outLen, 32);

    result = ConcatCharChar(contextPtr, "   ", 5, 3, "Oh我很好   ", 12, 14, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "     Oh我很好   ");
    EXPECT_EQ(outLen, 19);

    result = ConcatCharChar(contextPtr, "Hei你好吗", 8, 12, "   ", 5, 3, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "Hei你好吗     ");
    EXPECT_EQ(outLen, 17);

    result = ConcatCharChar(contextPtr, "Hei你好吗", 8, 12, "", 5, 0, false, &outLen);
    actual = std::string(result, outLen);
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
    std::string actual;

    result = ConcatCharStr(contextPtr, "*你是谁呢*", 6, 14, "我很OK", 8, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "*你是谁呢*我很OK");
    EXPECT_EQ(outLen, 22);

    result = ConcatCharStr(contextPtr, "*你是谁呢*", 10, 14, "我很OK", 8, false, &outLen);
    actual = std::string(result, outLen);
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
    std::string actual;

    result = ConcatStrChar(contextPtr, "粉色de圣诞袜", 17, "*黑色*", 4, 8, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "粉色de圣诞袜*黑色*");
    EXPECT_EQ(outLen, 25);

    result = ConcatStrChar(contextPtr, "粉色de圣诞袜", 17, "*黑色*", 6, 8, false, &outLen);
    actual = std::string(result, outLen);
    EXPECT_EQ(actual, "粉色de圣诞袜*黑色*");
    EXPECT_EQ(outLen, 25);
    delete context;
}

TEST(FunctionTest, LikeStrZh)
{
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    // like "xxx_"
    std::string pattern = "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞.$";
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
    std::string str = "时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞袜";
    std::string pattern = "^时欧基乌斯侧后解 hello! 回复哦黑色的and magic粉色的圣诞.$";
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

TEST(FunctionTest, CastStringToLong)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    std::string s = "23423";
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

    std::string s = "23423";
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
    std::string s = "23423";
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
    delete context;
}

TEST(FunctionTest, EvaluateHiveUdfSingle)
{
    auto context = new ExecutionContext();
    auto contextPtr = reinterpret_cast<int64_t>(context);
    int32_t inputTypes[] = { OMNI_INT, OMNI_INT };
    int32_t retType = OMNI_INT;
    int32_t vecCount = 2;

    int32_t inputValue[2] = { 3, 5 };
    uint8_t inputNull[2] = { 0, 0 };
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

}