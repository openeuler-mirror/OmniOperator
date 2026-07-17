/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Unit tests for left / right vectorized string functions
 *   left(string, length)  -> varchar  : leftmost `length` characters
 *   right(string, length) -> varchar  : rightmost `length` characters
 *   length <= 0 -> empty string; length >= char count -> whole string;
 *   Unicode-aware (counts characters, not bytes); NULL args propagate to NULL.
 */

#include <gtest/gtest.h>
#include <string>
#include <vector>

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

class LeftRightTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const left_right_test_env =
    ::testing::AddGlobalTestEnvironment(new LeftRightTestEnvironment);

class LeftRightTestHelper {
public:
    using StringVector = Vector<LargeStringContainer<std::string_view>>;

    static void ValidateStringResult(BaseVector *result, const std::vector<std::string> &expected, int rowSize)
    {
        auto *resultVec = dynamic_cast<StringVector *>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector type mismatch";
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                continue;
            }
            std::string actual(resultVec->GetValue(i));
            EXPECT_EQ(actual, expected[i]) << "Row " << i << " expected=\"" << expected[i] << "\" actual=\"" << actual
                                           << "\"";
        }
    }

    static BaseVector *CreateStringVector(const std::vector<std::string> &values)
    {
        BaseVector *vec = VectorHelper::CreateStringVector(values.size());
        vec->SetIsField(true);
        auto *typed = dynamic_cast<StringVector *>(vec);
        EXPECT_NE(typed, nullptr);
        for (size_t i = 0; i < values.size(); ++i) {
            std::string_view sv(values[i]);
            typed->SetValue(i, sv);
        }
        return vec;
    }

    static BaseVector *CreateInt32Vector(const std::vector<int32_t> &values)
    {
        BaseVector *vec = VectorHelper::CreateFlatVector(OMNI_INT, values.size());
        vec->SetIsField(true);
        auto *typed = static_cast<Vector<int32_t> *>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            typed->SetValue(i, values[i]);
        }
        return vec;
    }

    static BaseVector *CreateInt64Vector(const std::vector<int64_t> &values)
    {
        BaseVector *vec = VectorHelper::CreateFlatVector(OMNI_LONG, values.size());
        vec->SetIsField(true);
        auto *typed = static_cast<Vector<int64_t> *>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            typed->SetValue(i, values[i]);
        }
        return vec;
    }

    // Generic executor: works for "left"/"right" with INT or LONG length arg.
    static void Execute(const std::string &funcName, DataTypeId lengthType,
        BaseVector *stringVec, BaseVector *lengthVec, BaseVector *&result)
    {
        std::vector<DataTypeId> inputTypeIds = {OMNI_VARCHAR, lengthType};
        auto sig = std::make_shared<FunctionSignature>(funcName, inputTypeIds, OMNI_VARCHAR);
        auto fn = VectorFunction::Find(sig);
        ASSERT_NE(fn, nullptr) << funcName << "(varchar, " << (lengthType == OMNI_INT ? "int32" : "int64") << ") not found";
        auto outputType = std::make_shared<DataType>(OMNI_VARCHAR);
        ExecutionContext ctx;
        ctx.SetResultRowSize(stringVec->GetSize());
        std::stack<BaseVector *> args;
        args.push(stringVec);
        args.push(lengthVec);
        ASSERT_NO_THROW(fn->Apply(args, outputType, result, &ctx));
    }
};

// ============================================================================
// left(string, length) tests
// ============================================================================

TEST(LeftTest, BasicInt64)
{
    std::vector<std::string> strings = {"hello", "world", "abcdef"};
    std::vector<int64_t> lengths = {2, 3, 10};
    std::vector<std::string> expected = {"he", "wor", "abcdef"}; // 10 > len -> whole string

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt64Vector(lengths);
    BaseVector *result = nullptr;

    LeftRightTestHelper::Execute("left", OMNI_LONG, strVec, lenVec, result);
    LeftRightTestHelper::ValidateStringResult(result, expected, 3);

    delete strVec;
    delete lenVec;
    delete result;
}

TEST(LeftTest, BoundaryZeroAndNegative)
{
    std::vector<std::string> strings = {"hello", "hello", "hello"};
    std::vector<int64_t> lengths = {0, -1, -100};
    std::vector<std::string> expected = {"", "", ""}; // length <= 0 -> empty

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt64Vector(lengths);
    BaseVector *result = nullptr;

    LeftRightTestHelper::Execute("left", OMNI_LONG, strVec, lenVec, result);
    LeftRightTestHelper::ValidateStringResult(result, expected, 3);

    delete strVec;
    delete lenVec;
    delete result;
}

TEST(LeftTest, ExactLengthAndEmptyString)
{
    std::vector<std::string> strings = {"hello", "", "abc"};
    std::vector<int64_t> lengths = {5, 3, 3};
    std::vector<std::string> expected = {"hello", "", "abc"};

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt64Vector(lengths);
    BaseVector *result = nullptr;

    LeftRightTestHelper::Execute("left", OMNI_LONG, strVec, lenVec, result);
    LeftRightTestHelper::ValidateStringResult(result, expected, 3);

    delete strVec;
    delete lenVec;
    delete result;
}

TEST(LeftTest, Unicode)
{
    // "你好world" has 7 characters: 你 好 w o r l d
    std::vector<std::string> strings = {"你好world", "你好world", "你好world"};
    std::vector<int64_t> lengths = {3, 2, 7};
    std::vector<std::string> expected = {"你好w", "你好", "你好world"};

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt64Vector(lengths);
    BaseVector *result = nullptr;

    LeftRightTestHelper::Execute("left", OMNI_LONG, strVec, lenVec, result);
    LeftRightTestHelper::ValidateStringResult(result, expected, 3);

    delete strVec;
    delete lenVec;
    delete result;
}

TEST(LeftTest, Int32LengthParameter)
{
    std::vector<std::string> strings = {"hello", "world"};
    std::vector<int32_t> lengths = {2, 4};
    std::vector<std::string> expected = {"he", "worl"};

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt32Vector(lengths);
    BaseVector *result = nullptr;

    LeftRightTestHelper::Execute("left", OMNI_INT, strVec, lenVec, result);
    LeftRightTestHelper::ValidateStringResult(result, expected, 2);

    delete strVec;
    delete lenVec;
    delete result;
}

TEST(LeftTest, NullPropagation)
{
    std::vector<std::string> strings = {"hello", "world", "test"};
    std::vector<int64_t> lengths = {2, 3, 2};

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt64Vector(lengths);

    strVec->SetNull(0);  // string null at row 0 -> null
    lenVec->SetNull(2);  // length null at row 2 -> null

    BaseVector *result = nullptr;
    LeftRightTestHelper::Execute("left", OMNI_LONG, strVec, lenVec, result);

    EXPECT_TRUE(result->IsNull(0));   // string null
    EXPECT_FALSE(result->IsNull(1));  // valid
    EXPECT_TRUE(result->IsNull(2));   // length null

    auto *resultVec = dynamic_cast<LeftRightTestHelper::StringVector *>(result);
    ASSERT_NE(resultVec, nullptr);
    EXPECT_EQ(std::string(resultVec->GetValue(1)), "wor"); // left("world", 3)

    delete strVec;
    delete lenVec;
    delete result;
}

// ============================================================================
// right(string, length) tests
// ============================================================================

TEST(RightTest, BasicInt64)
{
    std::vector<std::string> strings = {"hello", "world", "abcdef"};
    std::vector<int64_t> lengths = {2, 3, 10};
    std::vector<std::string> expected = {"lo", "rld", "abcdef"}; // 10 > len -> whole string

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt64Vector(lengths);
    BaseVector *result = nullptr;

    LeftRightTestHelper::Execute("right", OMNI_LONG, strVec, lenVec, result);
    LeftRightTestHelper::ValidateStringResult(result, expected, 3);

    delete strVec;
    delete lenVec;
    delete result;
}

TEST(RightTest, BoundaryZeroAndNegative)
{
    std::vector<std::string> strings = {"hello", "hello", "hello"};
    std::vector<int64_t> lengths = {0, -1, -100};
    std::vector<std::string> expected = {"", "", ""}; // length <= 0 -> empty

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt64Vector(lengths);
    BaseVector *result = nullptr;

    LeftRightTestHelper::Execute("right", OMNI_LONG, strVec, lenVec, result);
    LeftRightTestHelper::ValidateStringResult(result, expected, 3);

    delete strVec;
    delete lenVec;
    delete result;
}

TEST(RightTest, ExactLengthAndEmptyString)
{
    std::vector<std::string> strings = {"hello", "", "abc"};
    std::vector<int64_t> lengths = {5, 3, 3};
    std::vector<std::string> expected = {"hello", "", "abc"};

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt64Vector(lengths);
    BaseVector *result = nullptr;

    LeftRightTestHelper::Execute("right", OMNI_LONG, strVec, lenVec, result);
    LeftRightTestHelper::ValidateStringResult(result, expected, 3);

    delete strVec;
    delete lenVec;
    delete result;
}

TEST(RightTest, Unicode)
{
    // "你好world" has 7 characters: 你 好 w o r l d
    std::vector<std::string> strings = {"你好world", "你好world", "你好world"};
    std::vector<int64_t> lengths = {3, 2, 7};
    std::vector<std::string> expected = {"rld", "ld", "你好world"};

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt64Vector(lengths);
    BaseVector *result = nullptr;

    LeftRightTestHelper::Execute("right", OMNI_LONG, strVec, lenVec, result);
    LeftRightTestHelper::ValidateStringResult(result, expected, 3);

    delete strVec;
    delete lenVec;
    delete result;
}

TEST(RightTest, Int32LengthParameter)
{
    std::vector<std::string> strings = {"hello", "world"};
    std::vector<int32_t> lengths = {2, 4};
    std::vector<std::string> expected = {"lo", "orld"};

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt32Vector(lengths);
    BaseVector *result = nullptr;

    LeftRightTestHelper::Execute("right", OMNI_INT, strVec, lenVec, result);
    LeftRightTestHelper::ValidateStringResult(result, expected, 2);

    delete strVec;
    delete lenVec;
    delete result;
}

TEST(RightTest, NullPropagation)
{
    std::vector<std::string> strings = {"hello", "world", "test"};
    std::vector<int64_t> lengths = {2, 3, 2};

    BaseVector *strVec = LeftRightTestHelper::CreateStringVector(strings);
    BaseVector *lenVec = LeftRightTestHelper::CreateInt64Vector(lengths);

    strVec->SetNull(0);  // string null at row 0 -> null
    lenVec->SetNull(2);  // length null at row 2 -> null

    BaseVector *result = nullptr;
    LeftRightTestHelper::Execute("right", OMNI_LONG, strVec, lenVec, result);

    EXPECT_TRUE(result->IsNull(0));   // string null
    EXPECT_FALSE(result->IsNull(1));  // valid
    EXPECT_TRUE(result->IsNull(2));   // length null

    auto *resultVec = dynamic_cast<LeftRightTestHelper::StringVector *>(result);
    ASSERT_NE(resultVec, nullptr);
    EXPECT_EQ(std::string(resultVec->GetValue(1)), "rld"); // right("world", 3)

    delete strVec;
    delete lenVec;
    delete result;
}
