/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: lower function unit tests.
 *   lower(string) -> string. Converts string to lowercase (ASCII A-Z -> a-z).
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

class LowerTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const lower_test_env =
    ::testing::AddGlobalTestEnvironment(new LowerTestEnvironment);

class LowerFunctionTestHelper {
public:
    static void ValidateStringResult(BaseVector* result,
                                     const std::vector<std::string>& expected,
                                     int rowSize) {
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

    static BaseVector* CreateStringVector(const std::vector<std::string>& values) {
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

    static void ExecuteLower(BaseVector* stringVec, BaseVector*& result) {
        std::vector<DataTypeId> inputTypeIds = {OMNI_VARCHAR};
        auto sig = std::make_shared<FunctionSignature>("lower", inputTypeIds, OMNI_VARCHAR);
        auto fn = VectorFunction::Find(sig);
        ASSERT_NE(fn, nullptr) << "lower not found";
        auto outputType = std::make_shared<DataType>(OMNI_VARCHAR);
        ExecutionContext ctx;
        ctx.SetResultRowSize(stringVec->GetSize());
        std::stack<BaseVector*> args;
        args.push(stringVec);
        ASSERT_NO_THROW(fn->Apply(args, outputType, result, &ctx));
    }
};

TEST(LowerTest, BasicAscii) {
    std::vector<std::string> strings = {"ABCDEFG", "HELLO", "WORLD"};
    std::vector<std::string> expected = {"abcdefg", "hello", "world"};
    BaseVector* strVec = LowerFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    LowerFunctionTestHelper::ExecuteLower(strVec, result);
    LowerFunctionTestHelper::ValidateStringResult(result, expected, 3);
    delete strVec;
    delete result;
}

TEST(LowerTest, AlreadyLower) {
    std::vector<std::string> strings = {"abcdefg", "hello", "world"};
    std::vector<std::string> expected = {"abcdefg", "hello", "world"};
    BaseVector* strVec = LowerFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    LowerFunctionTestHelper::ExecuteLower(strVec, result);
    LowerFunctionTestHelper::ValidateStringResult(result, expected, 3);
    delete strVec;
    delete result;
}

TEST(LowerTest, MixedCase) {
    std::vector<std::string> strings = {"a B c D e F g", "Hello World", "AbCdEf"};
    std::vector<std::string> expected = {"a b c d e f g", "hello world", "abcdef"};
    BaseVector* strVec = LowerFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    LowerFunctionTestHelper::ExecuteLower(strVec, result);
    LowerFunctionTestHelper::ValidateStringResult(result, expected, 3);
    delete strVec;
    delete result;
}

TEST(LowerTest, EmptyString) {
    std::vector<std::string> strings = {"", "ABC"};
    std::vector<std::string> expected = {"", "abc"};
    BaseVector* strVec = LowerFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    LowerFunctionTestHelper::ExecuteLower(strVec, result);
    LowerFunctionTestHelper::ValidateStringResult(result, expected, 2);
    delete strVec;
    delete result;
}

TEST(LowerTest, NullPropagation) {
    std::vector<std::string> strings = {"ABC", "xyz", "Hi", "BYE"};
    BaseVector* strVec = LowerFunctionTestHelper::CreateStringVector(strings);
    strVec->SetNull(1);
    strVec->SetNull(3);
    BaseVector* result = nullptr;
    LowerFunctionTestHelper::ExecuteLower(strVec, result);
    auto* resultVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(result);
    ASSERT_NE(resultVec, nullptr);
    EXPECT_EQ(resultVec->GetValue(0), "abc");
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL";
    EXPECT_EQ(resultVec->GetValue(2), "hi");
    EXPECT_TRUE(resultVec->IsNull(3)) << "Row 3 should be NULL";
    delete strVec;
    delete result;
}

TEST(LowerTest, NonLetterBytesUnchanged) {
    std::vector<std::string> strings = {"123 ABC 456", "!@#XYZ$%"};
    std::vector<std::string> expected = {"123 abc 456", "!@#xyz$%"};
    BaseVector* strVec = LowerFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    LowerFunctionTestHelper::ExecuteLower(strVec, result);
    LowerFunctionTestHelper::ValidateStringResult(result, expected, 2);
    delete strVec;
    delete result;
}

TEST(LowerTest, Boundary) {
    std::vector<std::string> strings = {"", "A", "a"};
    std::vector<std::string> expected = {"", "a", "a"};
    BaseVector* strVec = LowerFunctionTestHelper::CreateStringVector(strings);
    BaseVector* result = nullptr;
    LowerFunctionTestHelper::ExecuteLower(strVec, result);
    LowerFunctionTestHelper::ValidateStringResult(result, expected, 3);
    delete strVec;
    delete result;
}
