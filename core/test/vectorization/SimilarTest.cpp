/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Description: SIMILAR TO vectorized function test (SQL regex full-match)
 */

#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>
#include <limits>
#include <cmath>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/functions/Similar.h"
#include "vectorization/registration/SimpleFunctionRegistry.h"
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

// Initialize function registration before running tests (registers similar_to via RegisterRegexpFunctions).
class SimilarTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const similar_test_env = ::testing::AddGlobalTestEnvironment(new SimilarTestEnvironment);

class SimilarFunctionTestHelper {
public:
    static void ValidateBooleanResult(BaseVector* result, const std::vector<bool>& expected, int rowSize) {
        auto* resultVec = dynamic_cast<Vector<bool>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector is not boolean type";
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                std::cout << "Row " << i << ": NULL" << std::endl;
                continue;
            }
            bool actualValue = resultVec->GetValue(i);
            bool expectedValue = expected[i];
            std::cout << "Row " << i << ": Expected=" << (expectedValue ? "true" : "false")
                    << ", Actual=" << (actualValue ? "true" : "false") << std::endl;
            EXPECT_EQ(actualValue, expectedValue) << "Row " << i << " value mismatch";
        }
    }

    static BaseVector* CreateStringVector(const std::vector<std::string>& values) {
        BaseVector* vec = VectorHelper::CreateStringVector(values.size());
        auto* typedVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            std::string_view sv(values[i]);
            typedVec->SetValue(i, sv);
        }
        return vec;
    }

    // ExprEval pushes value first then pattern (LIFO), mirroring the vectorized Visit(SimilarExpr).
    static void ExecuteSimilar(BaseVector* strVec, BaseVector* patternVec, BaseVector*& result) {
        auto signature = std::make_shared<FunctionSignature>("similar_to",
            std::vector<DataTypeId>{OMNI_VARCHAR, OMNI_VARCHAR}, OMNI_BOOLEAN);
        auto function = VectorFunction::Find(signature);
        ASSERT_NE(function, nullptr) << "similar_to function not found for signature";

        auto outputType = std::make_shared<DataType>(OMNI_BOOLEAN);
        ExecutionContext context;
        context.SetResultRowSize(strVec->GetSize());
        std::stack<BaseVector*> args;
        args.push(strVec);
        args.push(patternVec);
        function->Apply(args, outputType, result, &context);
    }
};

// Test: exact full match
TEST(SimilarTest, SimilarExactMatch) {
    std::cout << "=== Test: exact full match ===" << std::endl;
    std::vector<std::string> strValues = {"hello", "world", "test"};
    std::vector<std::string> patterns = {"hello", "world", "test"};
    std::vector<bool> expected = {true, true, true};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: % wildcard (any chars, including empty), SIMILAR TO is full match
TEST(SimilarTest, SimilarPercentWildcard) {
    std::cout << "=== Test: % wildcard ===" << std::endl;
    std::vector<std::string> strValues = {"hello", "world", "h"};
    std::vector<std::string> patterns = {"h%", "h%", "h%"};
    std::vector<bool> expected = {true, false, true};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: _ wildcard (exactly one char)
TEST(SimilarTest, SimilarUnderscoreWildcard) {
    std::cout << "=== Test: _ wildcard ===" << std::endl;
    std::vector<std::string> strValues = {"hello", "helo", "hxllo"};
    std::vector<std::string> patterns = {"h_llo", "h_llo", "h_llo"};
    std::vector<bool> expected = {true, false, true};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: * quantifier (zero or more)
TEST(SimilarTest, SimilarStarQuantifier) {
    std::cout << "=== Test: * quantifier ===" << std::endl;
    std::vector<std::string> strValues = {"aaa", "b", ""};
    std::vector<std::string> patterns = {"a*", "a*", "a*"};
    std::vector<bool> expected = {true, false, true};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: + quantifier (one or more)
TEST(SimilarTest, SimilarPlusQuantifier) {
    std::cout << "=== Test: + quantifier ===" << std::endl;
    std::vector<std::string> strValues = {"aaa", ""};
    std::vector<std::string> patterns = {"a+", "a+"};
    std::vector<bool> expected = {true, false};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: [] character class
TEST(SimilarTest, SimilarCharClass) {
    std::cout << "=== Test: [] character class ===" << std::endl;
    std::vector<std::string> strValues = {"hello", "ello"};
    std::vector<std::string> patterns = {"[h]ello", "[h]ello"};
    std::vector<bool> expected = {true, false};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: | alternation
TEST(SimilarTest, SimilarAlternation) {
    std::cout << "=== Test: | alternation ===" << std::endl;
    std::vector<std::string> strValues = {"cat", "bird"};
    std::vector<std::string> patterns = {"cat|dog", "cat|dog"};
    std::vector<bool> expected = {true, false};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: SIMILAR TO is full match (substring does not match)
TEST(SimilarTest, SimilarFullMatch) {
    std::cout << "=== Test: full match (substring must not match) ===" << std::endl;
    std::vector<std::string> strValues = {"hello", "ell"};
    std::vector<std::string> patterns = {"ell", "ell"};
    std::vector<bool> expected = {false, true};  // 'hello' SIMILAR 'ell' false; 'ell' SIMILAR 'ell' true
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: % matches empty too
TEST(SimilarTest, SimilarPercentAll) {
    std::cout << "=== Test: % matches all incl empty ===" << std::endl;
    std::vector<std::string> strValues = {"hello", ""};
    std::vector<std::string> patterns = {"%", "%"};
    std::vector<bool> expected = {true, true};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: backslash is literal in SQL SIMILAR (C++ "a\\b" is the string a\b)
TEST(SimilarTest, SimilarBackslashLiteral) {
    std::cout << "=== Test: backslash literal ===" << std::endl;
    std::vector<std::string> strValues = {"a\\b", "ab"};
    std::vector<std::string> patterns = {"a\\b", "a\\b"};
    std::vector<bool> expected = {true, false};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: dollar is literal in SQL SIMILAR
TEST(SimilarTest, SimilarDollarLiteral) {
    std::cout << "=== Test: dollar literal ===" << std::endl;
    std::vector<std::string> strValues = {"a$b", "ab"};
    std::vector<std::string> patterns = {"a$b", "a$b"};
    std::vector<bool> expected = {true, false};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: NULL string -> NULL
TEST(SimilarTest, SimilarNullString) {
    std::cout << "=== Test: NULL string ===" << std::endl;
    std::vector<std::string> strValues = {"hello", "world", "test"};
    std::vector<std::string> patterns = {"hello", "world", "test"};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    strVec->SetNull(1);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    EXPECT_FALSE(resultVec->IsNull(0));
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL (string is NULL)";
    EXPECT_FALSE(resultVec->IsNull(2));
    delete resultVec;
}

// Test: NULL pattern -> NULL
TEST(SimilarTest, SimilarNullPattern) {
    std::cout << "=== Test: NULL pattern ===" << std::endl;
    std::vector<std::string> strValues = {"hello", "world", "test"};
    std::vector<std::string> patterns = {"hello", "world", "test"};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    patternVec->SetNull(1);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    EXPECT_FALSE(resultVec->IsNull(0));
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL (pattern is NULL)";
    EXPECT_FALSE(resultVec->IsNull(2));
    delete resultVec;
}

// Test: empty pattern matches only empty string
TEST(SimilarTest, SimilarEmpty) {
    std::cout << "=== Test: empty pattern ===" << std::endl;
    std::vector<std::string> strValues = {"", "a"};
    std::vector<std::string> patterns = {"", ""};
    std::vector<bool> expected = {true, false};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: invalid regex pattern throws
TEST(SimilarTest, SimilarInvalidPattern) {
    std::cout << "=== Test: invalid pattern ===" << std::endl;
    std::vector<std::string> strValues = {"hello"};
    std::vector<std::string> patterns = {"["};  // invalid (unclosed char class)
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    ASSERT_THROW({SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);},
        omniruntime::exception::OmniException) << "SIMILAR TO should throw on invalid regex";
    // Apply threw before its internal deletes, so clean up args here.
    delete strVec;
    delete patternVec;
    delete resultVec;
}

// Test: '.' passes through as a regex wildcard (matches Flink default branch; NOT escaped).
// Regression: escaping '.' would diverge from Flink ('axb' SIMILAR 'a.b' would become false).
TEST(SimilarTest, SimilarDotPassthrough) {
    std::cout << "=== Test: dot passthrough (Flink-compatible) ===" << std::endl;
    std::vector<std::string> strValues = {"axb", "a.b", "ab"};
    std::vector<std::string> patterns = {"a.b", "a.b", "a.b"};
    std::vector<bool> expected = {true, true, false};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: '^' passes through as a regex anchor (matches Flink default branch; NOT escaped).
// Regression: escaping '^' would diverge from Flink ('abc' SIMILAR '^abc' would become false).
TEST(SimilarTest, SimilarCaretPassthrough) {
    std::cout << "=== Test: caret passthrough (Flink-compatible) ===" << std::endl;
    std::vector<std::string> strValues = {"abc", "^abc"};
    std::vector<std::string> patterns = {"^abc", "^abc"};
    std::vector<bool> expected = {true, false};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);
    SimilarFunctionTestHelper::ValidateBooleanResult(resultVec, expected, strValues.size());
    delete resultVec;
}

// Test: % or _ inside [...] is invalid -> throw (matches Flink invalidRegularExpression).
TEST(SimilarTest, SimilarCharClassWildcardInvalid) {
    std::cout << "=== Test: wildcard inside char class throws ===" << std::endl;
    std::vector<std::string> strValues = {"a"};
    std::vector<std::string> patterns = {"[%_]"};
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    ASSERT_THROW({SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);},
        omniruntime::exception::OmniException);
    // Apply threw before its internal deletes, so clean up args here.
    delete strVec;
    delete patternVec;
    delete resultVec;
}

// Regression: an invalid pattern must throw on EVERY call, not silently return false from a
// stale cache after the first throw. Covers the cache-on-failure bug (build-then-cache fix).
TEST(SimilarTest, SimilarInvalidPatternCacheCoherence) {
    std::cout << "=== Test: invalid pattern cache coherence ===" << std::endl;
    std::vector<std::string> strValues = {"hello"};
    std::vector<std::string> patterns = {"["};  // invalid (unclosed char class)
    BaseVector* strVec = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec = nullptr;
    ASSERT_THROW({SimilarFunctionTestHelper::ExecuteSimilar(strVec, patternVec, resultVec);},
        omniruntime::exception::OmniException);
    delete strVec;
    delete patternVec;
    delete resultVec;
    // Second call with the same invalid pattern must also throw.
    BaseVector* strVec2 = SimilarFunctionTestHelper::CreateStringVector(strValues);
    BaseVector* patternVec2 = SimilarFunctionTestHelper::CreateStringVector(patterns);
    BaseVector* resultVec2 = nullptr;
    ASSERT_THROW({SimilarFunctionTestHelper::ExecuteSimilar(strVec2, patternVec2, resultVec2);},
        omniruntime::exception::OmniException);
    delete strVec2;
    delete patternVec2;
    delete resultVec2;
}
