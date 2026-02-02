/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: LIKE function unit tests.
 *   LIKE(string, pattern) -> boolean.
 *   Pattern: % = zero or more chars, _ = one char, \ = escape.
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

class LikeTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const like_test_env =
    ::testing::AddGlobalTestEnvironment(new LikeTestEnvironment);

class LikeFunctionTestHelper {
public:
    static void ValidateBooleanResult(BaseVector* result,
                                     const std::vector<bool>& expected,
                                     int rowSize) {
        auto* resultVec = dynamic_cast<Vector<bool>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector is not boolean type";
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                continue;
            }
            bool actualValue = resultVec->GetValue(i);
            bool expectedValue = expected[i];
            EXPECT_EQ(actualValue, expectedValue)
                << "Row " << i << " expected=" << (expectedValue ? "true" : "false")
                << " actual=" << (actualValue ? "true" : "false");
        }
    }

    static BaseVector* CreateStringVector(const std::vector<std::string>& values) {
        BaseVector* vec = VectorHelper::CreateStringVector(values.size());
        vec->SetIsField(true);
        auto* typedVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(vec);
        EXPECT_NE(typedVec, nullptr);
        for (size_t i = 0; i < values.size(); ++i) {
            std::string_view sv(values[i]);
            typedVec->SetValue(i, sv);
        }
        return vec;
    }

    static void ExecuteLike(BaseVector* strVec, BaseVector* patternVec, BaseVector*& result) {
        auto signature = std::make_shared<FunctionSignature>("LIKE",
            std::vector<DataTypeId>{OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_BOOLEAN);
        auto function = VectorFunction::Find(signature);
        ASSERT_NE(function, nullptr) << "LIKE function not found for signature";

        auto outputType = std::make_shared<DataType>(OMNI_BOOLEAN);
        ExecutionContext context;
        context.SetResultRowSize(strVec->GetSize());
        std::stack<BaseVector*> args;
        // Apply pops last arg first: push (str, pattern) -> pop order (pattern, str)
        args.push(strVec);
        args.push(patternVec);

        ASSERT_NO_THROW(function->Apply(args, outputType, result, &context));
    }
};

TEST(LikeTest, BasicMatch) {
    std::vector<std::string> strValues = {"hello", "world", "test"};
    std::vector<std::string> patterns = {"hello", "world", "test"};
    std::vector<bool> expected = {true, true, true};

    BaseVector* strVec = LikeFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = LikeFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;

    LikeFunctionTestHelper::ExecuteLike(strVec, patternVec, resultVec);
    LikeFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());

    delete strVec;
    delete patternVec;
    delete resultVec;
}

TEST(LikeTest, NoMatch) {
    std::vector<std::string> strValues = {"hello", "world", "test"};
    std::vector<std::string> patterns = {"xyz", "abc", "def"};
    std::vector<bool> expected = {false, false, false};

    BaseVector* strVec = LikeFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = LikeFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;

    LikeFunctionTestHelper::ExecuteLike(strVec, patternVec, resultVec);
    LikeFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());

    delete strVec;
    delete patternVec;
    delete resultVec;
}

TEST(LikeTest, PercentSuffix) {
    std::vector<std::string> strValues = {"hello", "heat", "hex"};
    std::vector<std::string> patterns = {"he%", "he%", "he%"};
    std::vector<bool> expected = {true, true, true};

    BaseVector* strVec = LikeFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = LikeFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;

    LikeFunctionTestHelper::ExecuteLike(strVec, patternVec, resultVec);
    LikeFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());

    delete strVec;
    delete patternVec;
    delete resultVec;
}

TEST(LikeTest, PercentPrefix) {
    std::vector<std::string> strValues = {"hello", "world", "cold"};
    std::vector<std::string> patterns = {"%lo", "%ld", "%old"};
    std::vector<bool> expected = {true, true, true};

    BaseVector* strVec = LikeFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = LikeFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;

    LikeFunctionTestHelper::ExecuteLike(strVec, patternVec, resultVec);
    LikeFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());

    delete strVec;
    delete patternVec;
    delete resultVec;
}

TEST(LikeTest, PercentBoth) {
    std::vector<std::string> strValues = {"hello", "world", "abc"};
    std::vector<std::string> patterns = {"%el%", "%or%", "%b%"};
    std::vector<bool> expected = {true, true, true};

    BaseVector* strVec = LikeFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = LikeFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;

    LikeFunctionTestHelper::ExecuteLike(strVec, patternVec, resultVec);
    LikeFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());

    delete strVec;
    delete patternVec;
    delete resultVec;
}

TEST(LikeTest, UnderscoreWildcard) {
    std::vector<std::string> strValues = {"a", "ab", "abc", "x"};
    std::vector<std::string> patterns = {"_", "__", "___", "_"};
    std::vector<bool> expected = {true, true, true, true};

    BaseVector* strVec = LikeFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = LikeFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;

    LikeFunctionTestHelper::ExecuteLike(strVec, patternVec, resultVec);
    LikeFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());

    delete strVec;
    delete patternVec;
    delete resultVec;
}

TEST(LikeTest, EscapePercent) {
    std::vector<std::string> strValues = {"100%", "50%"};
    std::vector<std::string> patterns = {"100\\%", "50\\%"};
    std::vector<bool> expected = {true, true};

    BaseVector* strVec = LikeFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = LikeFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;

    LikeFunctionTestHelper::ExecuteLike(strVec, patternVec, resultVec);
    LikeFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());

    delete strVec;
    delete patternVec;
    delete resultVec;
}

TEST(LikeTest, EmptyString) {
    std::vector<std::string> strValues = {"", "", "a"};
    std::vector<std::string> patterns = {"%", "x", "%"};
    std::vector<bool> expected = {true, false, true};

    BaseVector* strVec = LikeFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = LikeFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;

    LikeFunctionTestHelper::ExecuteLike(strVec, patternVec, resultVec);
    LikeFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());

    delete strVec;
    delete patternVec;
    delete resultVec;
}

TEST(LikeTest, NullString) {
    std::vector<std::string> strValues = {"hello", "world", "test"};
    std::vector<std::string> patterns = {"he%", "w%", "t%"};

    BaseVector* strVec = LikeFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = LikeFunctionTestHelper::CreateStringVector(patterns);
    strVec->SetNull(1);

    BaseVector* resultVec = nullptr;
    LikeFunctionTestHelper::ExecuteLike(strVec, patternVec, resultVec);

    EXPECT_FALSE(resultVec->IsNull(0)) << "Row 0 should not be NULL";
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL (string is NULL)";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";

    delete strVec;
    delete patternVec;
    delete resultVec;
}

TEST(LikeTest, NullPattern) {
    std::vector<std::string> strValues = {"hello", "world", "test"};
    std::vector<std::string> patterns = {"he%", "w%", "t%"};

    BaseVector* strVec = LikeFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = LikeFunctionTestHelper::CreateStringVector(patterns);
    patternVec->SetNull(1);

    BaseVector* resultVec = nullptr;
    LikeFunctionTestHelper::ExecuteLike(strVec, patternVec, resultVec);

    EXPECT_FALSE(resultVec->IsNull(0)) << "Row 0 should not be NULL";
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL (pattern is NULL)";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";

    delete strVec;
    delete patternVec;
    delete resultVec;
}
