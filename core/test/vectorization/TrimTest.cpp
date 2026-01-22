/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Trim function test


#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>
#include <stdexcept>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/ExprEval.h"
#include "expression/expressions.h"
#include "type/decimal_operations.h"
#include "vectorization/registration/SimpleFunctionRegistry.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace omniruntime::expressions;
using namespace omniruntime::TestUtil;

class TrimFunctionTestHelper {
public:
    static void SetupTestVector(int rowSize,
                                const std::vector<std::string>& inputStrs,
                                vec::BaseVector*& inputVec)
    {
        inputVec = VectorHelper::CreateStringVector(rowSize);
        auto *inputVector = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(inputVec);
        if (!inputVector) {
            FAIL() << "Type conversion failed for input vector";
            return;
        }
        for (int i = 0; i < rowSize; i++) {
            std::string_view inputStr(inputStrs[i]);
            inputVector->SetValue(i, inputStr);
        }
    }

    static void ValidateResult(Vector<LargeStringContainer<std::string_view>>* resultVector, 
                               const std::vector<std::string>& expectedStrs, 
                               int rowSize)
    {
        if (!resultVector) {
            FAIL() << "Result vector is null";
            return;
        }

        ASSERT_EQ(resultVector->GetSize(), rowSize) 
            << "Result vector size does not match expected row size";

        for (int row = 0; row < rowSize; ++row) {
            if (resultVector->IsNull(row)) {
                std::cout << "Row " << row << ": Result is NULL" << std::endl;
                continue;
            }

            std::string_view resultValue = resultVector->GetValue(row);
            std::string resultStr(resultValue);
            std::string expectedStr = expectedStrs[row];

            std::cout << "Row " << row << ": Input=\"" << resultStr 
                      << "\", Expected=\"" << expectedStr << "\"" << std::endl;

            EXPECT_EQ(resultStr, expectedStr) 
                << "Row " << row << " result does not match expected value";
        }
    }

    static void ExecuteTrimFunction(vec::BaseVector* inputVec, int rowSize)
    {
        auto signature = std::make_shared<FunctionSignature>("trim",
            std::vector<omniruntime::type::DataTypeId>{OMNI_VARCHAR},
            OMNI_VARCHAR);
        auto function = VectorFunction::Find(signature);
        if (!function) {
            FAIL() << "Trim function not found";
            return;
        }

        vec::BaseVector* resultVector = nullptr;
        auto varcharType = std::make_shared<DataType>(OMNI_VARCHAR);

        op::ExecutionContext context;
        context.SetResultRowSize(rowSize);
        std::stack<vec::BaseVector*> args;
        args.push(inputVec);

        ASSERT_NO_THROW(function->Apply(args, varcharType, resultVector, &context))
            << "TrimFunction.apply() threw an unexpected exception";

        auto* resultStrVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(resultVector);
        if (!resultStrVec) {
            FAIL() << "Result vector type conversion failed";
            delete resultVector;
            return;
        }

        // Store results for validation
        std::vector<std::string> results;
        for (int i = 0; i < rowSize; ++i) {
            if (resultStrVec->IsNull(i)) {
                results.push_back("");
            } else {
                std::string_view sv = resultStrVec->GetValue(i);
                results.push_back(std::string(sv));
            }
        }

        delete resultVector;
    }
};

TEST(VectorizationTest, TrimFunctionBasicTest) {
    std::cout << "=== Trim Function Basic Test ===" << std::endl;

    int rowSize = 5;
    vec::BaseVector *inputVec;

    std::vector<std::string> inputStrs = {
        "  hello world  ",
        "\t\ntest\n\t",
        "no_spaces",
        "   ",
        ""
    };

    std::vector<std::string> expectedStrs = {
        "hello world",
        "test",
        "no_spaces",
        "",
        ""
    };

    TrimFunctionTestHelper::SetupTestVector(rowSize, inputStrs, inputVec);

    auto signature = std::make_shared<FunctionSignature>("trim",
        std::vector<omniruntime::type::DataTypeId>{OMNI_VARCHAR},
        OMNI_VARCHAR);
    auto function = VectorFunction::Find(signature);
    ASSERT_NE(function, nullptr) << "Trim function not found";

    vec::BaseVector* resultVector = nullptr;
    auto varcharType = std::make_shared<DataType>(OMNI_VARCHAR);

    op::ExecutionContext context;
    context.SetResultRowSize(rowSize);
    std::stack<vec::BaseVector*> args;
    args.push(inputVec);

    ASSERT_NO_THROW(function->Apply(args, varcharType, resultVector, &context))
        << "TrimFunction.apply() threw an unexpected exception";

    auto* resultStrVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(resultVector);
    ASSERT_NE(resultStrVec, nullptr) << "Result vector type conversion failed";

    // Validate results
    for (int row = 0; row < rowSize; ++row) {
        if (resultStrVec->IsNull(row)) {
            EXPECT_TRUE(expectedStrs[row].empty()) 
                << "Row " << row << " result is NULL but expected non-empty";
            continue;
        }

        std::string_view resultValue = resultStrVec->GetValue(row);
        std::string resultStr(resultValue);
        std::string expectedStr = expectedStrs[row];

        EXPECT_EQ(resultStr, expectedStr) 
            << "Row " << row << " result does not match expected value. "
            << "Got: \"" << resultStr << "\", Expected: \"" << expectedStr << "\"";
    }

    delete resultVector;
    delete inputVec;

    std::cout << "=== Basic Test Completed ===" << std::endl;
}

TEST(VectorizationTest, TrimFunctionEdgeCasesTest) {
    std::cout << "=== Trim Function Edge Cases Test ===" << std::endl;

    int rowSize = 4;
    vec::BaseVector *inputVec;

    std::vector<std::string> inputStrs = {
        "  leading only",
        "trailing only  ",
        "\r\nmixed\t whitespace\r\n",
        "single_char"
    };

    std::vector<std::string> expectedStrs = {
        "leading only",
        "trailing only",
        "mixed\t whitespace",
        "single_char"
    };

    TrimFunctionTestHelper::SetupTestVector(rowSize, inputStrs, inputVec);

    auto signature = std::make_shared<FunctionSignature>("trim",
        std::vector<omniruntime::type::DataTypeId>{OMNI_VARCHAR},
        OMNI_VARCHAR);
    auto function = VectorFunction::Find(signature);
    ASSERT_NE(function, nullptr) << "Trim function not found";

    vec::BaseVector* resultVector = nullptr;
    auto varcharType = std::make_shared<DataType>(OMNI_VARCHAR);

    op::ExecutionContext context;
    context.SetResultRowSize(rowSize);
    std::stack<vec::BaseVector*> args;
    args.push(inputVec);

    ASSERT_NO_THROW(function->Apply(args, varcharType, resultVector, &context))
        << "TrimFunction.apply() threw an unexpected exception";

    auto* resultStrVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(resultVector);
    ASSERT_NE(resultStrVec, nullptr) << "Result vector type conversion failed";

    // Validate results
    for (int row = 0; row < rowSize; ++row) {
        std::string_view resultValue = resultStrVec->GetValue(row);
        std::string resultStr(resultValue);
        std::string expectedStr = expectedStrs[row];

        EXPECT_EQ(resultStr, expectedStr) 
            << "Row " << row << " result does not match expected value. "
            << "Got: \"" << resultStr << "\", Expected: \"" << expectedStr << "\"";
    }

    delete resultVector;
    delete inputVec;

    std::cout << "=== Edge Cases Test Completed ===" << std::endl;
}

TEST(VectorizationTest, TrimFunctionConstVectorTest) {
    std::cout << "=== Trim Function Const Vector Test ===" << std::endl;

    int rowSize = 3;
    vec::BaseVector *inputVec = new ConstVector<std::string>("  constant value  ", OMNI_VARCHAR, rowSize);

    auto signature = std::make_shared<FunctionSignature>("trim",
        std::vector<omniruntime::type::DataTypeId>{OMNI_VARCHAR},
        OMNI_VARCHAR);
    auto function = VectorFunction::Find(signature);
    ASSERT_NE(function, nullptr) << "Trim function not found";

    vec::BaseVector* resultVector = nullptr;
    auto varcharType = std::make_shared<DataType>(OMNI_VARCHAR);

    op::ExecutionContext context;
    context.SetResultRowSize(rowSize);
    std::stack<vec::BaseVector*> args;
    args.push(inputVec);

    ASSERT_NO_THROW(function->Apply(args, varcharType, resultVector, &context))
        << "TrimFunction.apply() threw an unexpected exception";

    auto* resultStrVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(resultVector);
    ASSERT_NE(resultStrVec, nullptr) << "Result vector type conversion failed";

    // All rows should have the same trimmed value
    std::string expectedStr = "constant value";
    for (int row = 0; row < rowSize; ++row) {
        std::string_view resultValue = resultStrVec->GetValue(row);
        std::string resultStr(resultValue);

        EXPECT_EQ(resultStr, expectedStr) 
            << "Row " << row << " result does not match expected value. "
            << "Got: \"" << resultStr << "\", Expected: \"" << expectedStr << "\"";
    }

    delete resultVector;
    delete inputVec;

    std::cout << "=== Const Vector Test Completed ===" << std::endl;
}
*/