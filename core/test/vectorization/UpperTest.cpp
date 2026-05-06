/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: upper function unit tests.
 *   upper(string) -> string. Converts string to uppercase.
 */

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <stack>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/VectorFunction.h"
#include "codegen/func_signature.h"
#include "vector/vector_helper.h"
#include "vector/vector.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace omniruntime::codegen;
using namespace omniruntime::TestUtil;

class UpperTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const upper_test_env = ::testing::AddGlobalTestEnvironment(new UpperTestEnvironment);

class UpperFunctionTestHelper {
public:
    static void ValidateStringResult(BaseVector* result, const std::vector<std::string>& expected, int rowSize)
    {
        auto* resultVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector type mismatch";
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                continue;
            }
            std::string_view actualSv = resultVec->GetValue(i);
            std::string actual(actualSv);
            std::string exp = expected[i];
            EXPECT_EQ(actual, exp) << "Row " << i << " expected=\"" << exp << "\" actual=\"" << actual << "\"";
        }
    }

    static BaseVector* CreateStringVector(const std::vector<std::string>& values)
    {
        BaseVector* vec = VectorHelper::CreateStringVector(values.size());
        vec->SetIsField(true);
        auto* typed = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(vec);
        EXPECT_NE(typed, nullptr);
        for (size_t i = 0; i < values.size(); ++i) {
            std::string_view sv(values[i]);
            typed->SetValue(i, sv);
        }
        return vec;
    }

    static void ExecuteUpperWithInputType(BaseVector* stringVec, DataTypeId inputTypeId, BaseVector*& result)
    {
        std::vector<DataTypeId> inputTypeIds = {inputTypeId};
        auto sig = std::make_shared<FunctionSignature>("upper", inputTypeIds, OMNI_VARCHAR);
        auto fn = VectorFunction::Find(sig);
        ASSERT_NE(fn, nullptr) << "upper not found";
        auto outputType = std::make_shared<DataType>(OMNI_VARCHAR);
        ExecutionContext ctx;
        ctx.SetResultRowSize(stringVec->GetSize());
        std::stack<BaseVector*> args;
        args.push(stringVec);
        ASSERT_NO_THROW(fn->Apply(args, outputType, result, &ctx));
    }

    static void ExecuteUpper(BaseVector* stringVec, BaseVector*& result)
    {
        ExecuteUpperWithInputType(stringVec, OMNI_VARCHAR, result);
    }
};

TEST(UpperTest, BasicAscii)
{
    std::vector<std::string> strings = {"abcdefg", "hello", "world"};
    std::vector<std::string> expected = {"ABCDEFG", "HELLO", "WORLD"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 3);
    delete strVec;
    delete result;
}

TEST(UpperTest, AlreadyUpper)
{
    std::vector<std::string> strings = {"ABCDEFG", "HELLO", "WORLD"};
    std::vector<std::string> expected = {"ABCDEFG", "HELLO", "WORLD"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 3);
    delete strVec;
    delete result;
}

TEST(UpperTest, MixedCase)
{
    std::vector<std::string> strings = {"a B c D e F g", "Hello World", "AbCdEf"};
    std::vector<std::string> expected = {"A B C D E F G", "HELLO WORLD", "ABCDEF"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 3);
    delete strVec;
    delete result;
}

TEST(UpperTest, EmptyString)
{
    std::vector<std::string> strings = {"", "abc"};
    std::vector<std::string> expected = {"", "ABC"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 2);
    delete strVec;
    delete result;
}

TEST(UpperTest, NullPropagation)
{
    std::vector<std::string> strings = {"abc", "xyz", "Hi", "bye"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    strVec->SetNull(1);
    strVec->SetNull(3);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    auto* resultVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(result);
    ASSERT_NE(resultVec, nullptr);
    EXPECT_EQ(resultVec->GetValue(0), "ABC");
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL";
    EXPECT_EQ(resultVec->GetValue(2), "HI");
    EXPECT_TRUE(resultVec->IsNull(3)) << "Row 3 should be NULL";
    delete strVec;
    delete result;
}

TEST(UpperTest, NonLetterBytesUnchanged)
{
    std::vector<std::string> strings = {"123 abc 456", "!@#xyz$%"};
    std::vector<std::string> expected = {"123 ABC 456", "!@#XYZ$%"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 2);
    delete strVec;
    delete result;
}

TEST(UpperTest, GreekUnicode)
{
    std::vector<std::string> strings = {
        u8"\u03C0",
        u8"\u03C0\u03B1\u03C2",
        u8"\u03C0\u03B1\u03C3\u03B1",
        u8"a\u03C0b",
        u8"hello \u03C3",
        u8"hello\u03C2",
        u8"a\u0301\u03C2"};
    std::vector<std::string> expected = {
        u8"\u03A0",
        u8"\u03A0\u0391\u03A3",
        u8"\u03A0\u0391\u03A3\u0391",
        u8"A\u03A0B",
        u8"HELLO \u03A3",
        u8"HELLO\u03A3",
        u8"A\u0301\u03A3"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 7);
    delete strVec;
    delete result;
}

TEST(UpperTest, Boundary)
{
    std::vector<std::string> strings = {"", "A", "a"};
    std::vector<std::string> expected = {"", "A", "A"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 3);
    delete strVec;
    delete result;
}

TEST(UpperTest, UnicodeLatinAndCyrillic)
{
    std::vector<std::string> strings = {
        u8"\u00E0\u00E1\u00E2\u00E3\u00E4\u00E5\u00E6\u00E7\u00E8\u00E9\u00EA\u00EB\u00EC\u00ED\u00EE\u00EF\u00F0\u00F1\u00F2\u00F3\u00F4\u00F5\u00F6\u00F8\u00F9\u00FA\u00FB\u00FC\u00FD\u00FE",
        u8"\u0430\u0431\u0432\u0433\u0434\u0435\u0436\u0437\u0438\u0439\u043A\u043B\u043C\u043D\u043E\u043F\u0440\u0441\u0442\u0443\u0444\u0445\u0446\u0447\u0448\u0449\u044A\u044B\u044C\u044D\u044E\u044F"};
    std::vector<std::string> expected = {
        u8"\u00C0\u00C1\u00C2\u00C3\u00C4\u00C5\u00C6\u00C7\u00C8\u00C9\u00CA\u00CB\u00CC\u00CD\u00CE\u00CF\u00D0\u00D1\u00D2\u00D3\u00D4\u00D5\u00D6\u00D8\u00D9\u00DA\u00DB\u00DC\u00DD\u00DE",
        u8"\u0410\u0411\u0412\u0413\u0414\u0415\u0416\u0417\u0418\u0419\u041A\u041B\u041C\u041D\u041E\u041F\u0420\u0421\u0422\u0423\u0424\u0425\u0426\u0427\u0428\u0429\u042A\u042B\u042C\u042D\u042E\u042F"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 2);
    delete strVec;
    delete result;
}

TEST(UpperTest, UnicodeSpecialMappings)
{
    std::vector<std::string> strings = {
        u8"i\u0307",
        u8"i\u0307",
        u8"\u010B\u0117\u0121\u017C\u0227\u022F\u1E03\u1E0B\u1E1F\u1E23\u1E41\u1E45\u1E57\u1E59\u1E61\u1E65\u1E67\u1E69\u1E6B\u1E87\u1E8B\u1E8F"};
    std::vector<std::string> expected = {
        u8"I\u0307",
        u8"I\u0307",
        u8"\u010A\u0116\u0120\u017B\u0226\u022E\u1E02\u1E0A\u1E1E\u1E22\u1E40\u1E44\u1E56\u1E58\u1E60\u1E64\u1E66\u1E68\u1E6A\u1E86\u1E8A\u1E8E"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 3);
    delete strVec;
    delete result;
}

TEST(UpperTest, GreekSigmaRoundTripLowerCases)
{
    std::vector<std::string> strings = {
        u8"\u03C0\u03B1\u03C2 ",
        u8"\u03C0\u03B1\u03C2.",
        u8"\u03C0\u03B1\u03C2   a",
        u8"hello \u03C3",
        u8"hello \u03C3 world",
        u8"   \u03C3",
        u8"\u03C3",
        u8"ab\u03C2\u4E2D",
        u8"ab\u4E2D\u03C3\u4E2D",
        u8"\u03C3\u03C3\u03C2",
        u8"a\u03C3b\u03C2"};
    std::vector<std::string> expected = {
        u8"\u03A0\u0391\u03A3 ",
        u8"\u03A0\u0391\u03A3.",
        u8"\u03A0\u0391\u03A3   A",
        u8"HELLO \u03A3",
        u8"HELLO \u03A3 WORLD",
        u8"   \u03A3",
        u8"\u03A3",
        u8"AB\u03A3\u4E2D",
        u8"AB\u4E2D\u03A3\u4E2D",
        u8"\u03A3\u03A3\u03A3",
        u8"A\u03A3B\u03A3"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 11);
    delete strVec;
    delete result;
}

TEST(UpperTest, VeloxUpperGreekWords)
{
    std::vector<std::string> strings = {
        u8"\u03C0\u03B1\u03C3",
        u8"\u03C0\u03B1\u03C3 ",
        u8"\u03C0\u03B1\u03C3a",
        u8"\u03C0\u03B1\u03C2",
        u8"\u03C0\u03B1\u03C2 ",
        u8"\u03C0\u03B1\u03C2a"};
    std::vector<std::string> expected = {
        u8"\u03A0\u0391\u03A3",
        u8"\u03A0\u0391\u03A3 ",
        u8"\u03A0\u0391\u03A3A",
        u8"\u03A0\u0391\u03A3",
        u8"\u03A0\u0391\u03A3 ",
        u8"\u03A0\u0391\u03A3A"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 6);
    delete strVec;
    delete result;
}

TEST(UpperTest, VeloxFullGreekAlphabetLowerToUpper)
{
    std::vector<std::string> strings = {
        u8"\u03B1\u03B2\u03B3\u03B4\u03B5\u03B6\u03B7\u03B8\u03B9\u03BA\u03BB\u03BC\u03BD\u03BE\u03BF\u03C0\u03C1\u03C2\u03C3\u03C4\u03C5\u03C6\u03C7\u03C8"};
    std::vector<std::string> expected = {
        u8"\u0391\u0392\u0393\u0394\u0395\u0396\u0397\u0398\u0399\u039A\u039B\u039C\u039D\u039E\u039F\u03A0\u03A1\u03A3\u03A3\u03A4\u03A5\u03A6\u03A7\u03A8"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 1);
    delete strVec;
    delete result;
}

TEST(UpperTest, CharInputSignature)
{
    std::vector<std::string> strings = {"abc", u8"i\u0307", u8"\u03C0\u03B1\u03C2"};
    std::vector<std::string> expected = {"ABC", u8"I\u0307", u8"\u03A0\u0391\u03A3"};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpperWithInputType(strVec, OMNI_CHAR, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 3);
    delete strVec;
    delete result;
}

TEST(UpperTest, InvalidUtf8PreservesRemainingBytes)
{
    std::string invalid = u8"\u03C8";
    invalid.append("\xFF\xFF", 2);
    invalid.append(u8"\u03A3\u0393\u0394A");
    std::string expectedInvalid = u8"\u03A8";
    expectedInvalid.append("\xFF\xFF", 2);
    expectedInvalid.append(u8"\u03A3\u0393\u0394A");
    std::vector<std::string> strings = {invalid};
    std::vector<std::string> expected = {expectedInvalid};
    BaseVector* strVec = UpperFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    UpperFunctionTestHelper::ExecuteUpper(strVec, result);
    UpperFunctionTestHelper::ValidateStringResult(result, expected, 1);
    delete strVec;
    delete result;
}
