/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Trim function unit tests
 *   Trim(string) -> string  (trim leading/trailing space only, per TrimFunction)
 *   Trim(trimStr, string) -> string  (trim leading/trailing chars in trimStr, per TrimWithCharsFunction)
 */

#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <stdexcept>

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

class TrimTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const trim_test_env =
    ::testing::AddGlobalTestEnvironment(new TrimTestEnvironment);

class TrimFunctionTestHelper {
public:
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

    static void ValidateStringResult(BaseVector* result,
                                    const std::vector<std::string>& expected,
                                    int rowSize) {
        auto* resultVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector type mismatch";
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                EXPECT_TRUE(expected[static_cast<size_t>(i)].empty() || expected[static_cast<size_t>(i)] == "<null>")
                    << "Row " << i << " result is NULL";
                continue;
            }
            std::string_view actual = resultVec->GetValue(i);
            std::string actualStr(actual);
            const std::string& exp = expected[static_cast<size_t>(i)];
            EXPECT_EQ(actualStr, exp) << "Row " << i << " expected=\"" << exp << "\" actual=\"" << actualStr << "\"";
        }
    }

    // Use OMNI_VARCHAR in signature: Trim is registered for OMNI_VARCHAR; CreateStringVector may return OMNI_CHAR.
    static void ExecuteTrimOneArg(BaseVector* strVec, BaseVector*& result) {
        std::vector<DataTypeId> inputTypeIds = { OMNI_VARCHAR };
        auto sig = std::make_shared<FunctionSignature>("Trim", inputTypeIds, OMNI_VARCHAR);
        auto fn = VectorFunction::Find(sig);
        ASSERT_NE(fn, nullptr) << "Trim(string) function not found";
        auto outputType = std::make_shared<DataType>(OMNI_VARCHAR);
        ExecutionContext ctx;
        ctx.SetResultRowSize(strVec->GetSize());
        std::stack<BaseVector*> args;
        args.push(strVec);
        ASSERT_NO_THROW(fn->Apply(args, outputType, result, &ctx));
    }

    // TrimWithCharsFunction(trimStr, str): first arg = trimStr, second = str. Use OMNI_VARCHAR for lookup.
    static void ExecuteTrimTwoArgs(BaseVector* strVec, BaseVector* trimStrVec, BaseVector*& result) {
        std::vector<DataTypeId> inputTypeIds = { OMNI_VARCHAR, OMNI_VARCHAR };
        auto sig = std::make_shared<FunctionSignature>("Trim", inputTypeIds, OMNI_VARCHAR);
        auto fn = VectorFunction::Find(sig);
        ASSERT_NE(fn, nullptr) << "Trim(trimStr, string) function not found";
        auto outputType = std::make_shared<DataType>(OMNI_VARCHAR);
        ExecutionContext ctx;
        ctx.SetResultRowSize(strVec->GetSize());
        std::stack<BaseVector*> args;
        args.push(trimStrVec);
        args.push(strVec);
        ASSERT_NO_THROW(fn->Apply(args, outputType, result, &ctx));
    }
};

// --- TrimFunction (single arg: trim space only) ---
// String.h TrimFunction uses find_first_not_of(" ") / find_last_not_of(" "), so only space is trimmed.

TEST(TrimTest, SingleArgTrimSpace) {
    std::vector<std::string> strings = {
        "  hello world  ",
        "   ",
        "no_spaces",
        "",
        " \t leading and trailing "
    };
    std::vector<std::string> expected = {
        "hello world",
        "",
        "no_spaces",
        "",
        "\t leading and trailing"   // only trailing space trimmed; leading \t not trimmed
    };
    std::unique_ptr<BaseVector> strVec(TrimFunctionTestHelper::CreateStringVector(strings));
    BaseVector* result = nullptr;
    TrimFunctionTestHelper::ExecuteTrimOneArg(strVec.get(), result);
    std::unique_ptr<BaseVector> resultHolder(result);
    TrimFunctionTestHelper::ValidateStringResult(resultHolder.get(), expected, static_cast<int>(strings.size()));
}

TEST(TrimTest, SingleArgEdgeCases) {
    std::vector<std::string> strings = {
        "  leading only",
        "trailing only  ",
        "  both  ",
        "single_char",
        "\t\ntest\n\t"   // TrimFunction only trims space, so tab/newline remain
    };
    std::vector<std::string> expected = {
        "leading only",
        "trailing only",
        "both",
        "single_char",
        "\t\ntest\n\t"
    };
    std::unique_ptr<BaseVector> strVec(TrimFunctionTestHelper::CreateStringVector(strings));
    BaseVector* result = nullptr;
    TrimFunctionTestHelper::ExecuteTrimOneArg(strVec.get(), result);
    std::unique_ptr<BaseVector> resultHolder(result);
    TrimFunctionTestHelper::ValidateStringResult(resultHolder.get(), expected, static_cast<int>(strings.size()));
}

TEST(TrimTest, SingleArgConstVector) {
    int rowSize = 3;
    std::string_view constVal("  constant value  ");
    std::unique_ptr<BaseVector> inputVec(new ConstVector<std::string_view>(constVal, OMNI_VARCHAR, rowSize));
    std::vector<std::string> expected(rowSize, "constant value");
    BaseVector* result = nullptr;
    TrimFunctionTestHelper::ExecuteTrimOneArg(inputVec.get(), result);
    std::unique_ptr<BaseVector> resultHolder(result);
    TrimFunctionTestHelper::ValidateStringResult(resultHolder.get(), expected, rowSize);
}

// --- TrimWithCharsFunction (two args: trim chars in trimStr from string) ---

TEST(TrimTest, TwoArgsTrimChars) {
    std::vector<std::string> strings = {
        "xxxyyhelloyyxx",
        "ababab",
        "xyz",
        "",
        "abcd"
    };
    std::vector<std::string> trimStrs = {
        "xy",
        "ab",
        "xyz",
        "a",
        "abcd"
    };
    std::vector<std::string> expected = {
        "hello",
        "",
        "",
        "",
        ""
    };
    std::unique_ptr<BaseVector> strVec(TrimFunctionTestHelper::CreateStringVector(strings));
    std::unique_ptr<BaseVector> trimStrVec(TrimFunctionTestHelper::CreateStringVector(trimStrs));
    BaseVector* result = nullptr;
    TrimFunctionTestHelper::ExecuteTrimTwoArgs(strVec.get(), trimStrVec.get(), result);
    std::unique_ptr<BaseVector> resultHolder(result);
    TrimFunctionTestHelper::ValidateStringResult(resultHolder.get(), expected, static_cast<int>(strings.size()));
}

TEST(TrimTest, TwoArgsEmptyTrimStr) {
    // Empty trimStr: find_first_not_of("") returns 0, so full string returned.
    std::vector<std::string> strings = { "  hello  ", "data", "" };
    std::vector<std::string> trimStrs = { "", "", "" };
    std::vector<std::string> expected = { "  hello  ", "data", "" };
    std::unique_ptr<BaseVector> strVec(TrimFunctionTestHelper::CreateStringVector(strings));
    std::unique_ptr<BaseVector> trimStrVec(TrimFunctionTestHelper::CreateStringVector(trimStrs));
    BaseVector* result = nullptr;
    TrimFunctionTestHelper::ExecuteTrimTwoArgs(strVec.get(), trimStrVec.get(), result);
    std::unique_ptr<BaseVector> resultHolder(result);
    TrimFunctionTestHelper::ValidateStringResult(resultHolder.get(), expected, 3);
}

TEST(TrimTest, TwoArgsSingleCharTrim) {
    std::vector<std::string> strings = { "zzhellozz", "aaa", "bbb" };
    std::vector<std::string> trimStrs = { "z", "a", "b" };
    std::vector<std::string> expected = { "hello", "", "" };
    std::unique_ptr<BaseVector> strVec(TrimFunctionTestHelper::CreateStringVector(strings));
    std::unique_ptr<BaseVector> trimStrVec(TrimFunctionTestHelper::CreateStringVector(trimStrs));
    BaseVector* result = nullptr;
    TrimFunctionTestHelper::ExecuteTrimTwoArgs(strVec.get(), trimStrVec.get(), result);
    std::unique_ptr<BaseVector> resultHolder(result);
    TrimFunctionTestHelper::ValidateStringResult(resultHolder.get(), expected, 3);
}

TEST(TrimTest, TwoArgsTrimSpace) {
    // trimStr = " " should behave like single-arg trim for space
    std::vector<std::string> strings = { "  both  ", " x ", "a b" };
    std::vector<std::string> trimStrs = { " ", " ", " " };
    std::vector<std::string> expected = { "both", "x", "a b" };
    std::unique_ptr<BaseVector> strVec(TrimFunctionTestHelper::CreateStringVector(strings));
    std::unique_ptr<BaseVector> trimStrVec(TrimFunctionTestHelper::CreateStringVector(trimStrs));
    BaseVector* result = nullptr;
    TrimFunctionTestHelper::ExecuteTrimTwoArgs(strVec.get(), trimStrVec.get(), result);
    std::unique_ptr<BaseVector> resultHolder(result);
    TrimFunctionTestHelper::ValidateStringResult(resultHolder.get(), expected, 3);
}
