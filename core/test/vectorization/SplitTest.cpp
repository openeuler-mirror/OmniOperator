/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: codegen test
 */

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

class SplitFunctionTestHelper {
public:
    static void SetupTestVectors(int rowSize,
                                const std::vector<std::string>& inputStrs,
                                const std::string& delimiter,
                                int32_t limit,
                                vec::BaseVector*& inputVec,
                                vec::BaseVector*& delimiterVec,
                                vec::BaseVector*& limitVec)
    {
        inputVec = VectorHelper::CreateStringVector(rowSize);
        auto *inputVector = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(inputVec);
        if (!inputVector) {
            FAIL() << "Type conversion failed for input vectors";
            return;
        }
        for (int i = 0; i < rowSize; i++) {
            std::string_view inputStr(inputStrs[i]);
            inputVector->SetValue(i, inputStr);
        }

        delimiterVec = new ConstVector<std::string>(delimiter, OMNI_VARCHAR, rowSize);
        limitVec = new ConstVector<int32_t>(limit, OMNI_INT, rowSize);
    }

    static void ValidateResult(ArrayVector* resultVector, int rowSize)
    {
        auto elementVector = resultVector->GetElementVector();
        if (!elementVector) {
            FAIL() << "Element vector of result is null";
            return;
        }
        auto* strElementVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(elementVector.get());
        if (!strElementVec) {
            FAIL() << "Element vector is not string type";
            return;
        }

        for (int row = 0; row < rowSize; ++row) {
            std::cout << "Row " << row << ": ";
            if (resultVector->IsNull(row)) {
                std::cout << "Result is NULL" << std::endl;
                continue;
            }

            int64_t arraySize = resultVector->GetSize(row);
            int64_t startIdx = resultVector->GetOffset(row);
            std::cout << "Split elements: [";
            for (int64_t elemIdx = 0; elemIdx < arraySize; ++elemIdx) {
                int64_t globalElemIdx = startIdx + elemIdx;
                if (globalElemIdx >= strElementVec->GetSize()) {
                    std::cout << "out_of_bounds";
                    continue;
                }
                if (strElementVec->IsNull(globalElemIdx)) {
                    std::cout << "null";
                } else {
                    std::string_view elemValue = strElementVec->GetValue(globalElemIdx);
                    std::cout << "\"" << elemValue << "\"";
                }
                if (elemIdx != arraySize - 1) {
                    std::cout << ", ";
                }
            }
            std::cout << "]" << std::endl;
        }
    }

    static void ExecuteSplitFunction(vec::BaseVector* inputVec,
                                   vec::BaseVector* delimiterVec,
                                   vec::BaseVector* limitVec,
                                   int rowSize)
    {
        auto signature = std::make_shared<FunctionSignature>("split",
            std::vector<omniruntime::type::DataTypeId>{OMNI_VARCHAR, OMNI_VARCHAR, OMNI_INT},
            OMNI_ARRAY);
        auto function = VectorFunction::Find(signature);
        vec::BaseVector* resultVector = nullptr;
        auto arrayType = std::make_shared<DataType>(OMNI_ARRAY);

        op::ExecutionContext context;
        context.SetResultRowSize(rowSize);
        std::stack<vec::BaseVector*> args;
        args.push(inputVec);
        args.push(delimiterVec);
        args.push(limitVec);

        ASSERT_NO_THROW(function->Apply(args, arrayType, resultVector, &context))
            << "SplitFunction.apply() threw an unexpected exception";

        ValidateResult(resultVector, rowSize);
        delete resultVector;
    }
};

TEST(VectorizationTest, SplitFunctionTest) {
    std::cout << "=== Split Function Direct Test ===" << std::endl;

    int rowSize = 3;
    vec::BaseVector *inputVec, *delimiterVec, *limitVec;

    SplitFunctionTestHelper::SetupTestVectors(
        rowSize,
        {"Washington 6th", "Dogwood Washington", "Laurel"}, " ", -1,
        inputVec, delimiterVec, limitVec
    );

    SplitFunctionTestHelper::ExecuteSplitFunction(inputVec, delimiterVec, limitVec, rowSize);

    delete inputVec;
    delete delimiterVec;

    std::cout << "=== Direct Test Completed ===" << std::endl;
}

TEST(VectorizationTest, SplitFunctionLimitTest) {
    std::cout << "=== Split Function Multi-Row Test ===" << std::endl;

    int rowSize = 3;
    vec::BaseVector *inputVec, *delimiterVec, *limitVec;

    SplitFunctionTestHelper::SetupTestVectors(
        rowSize,
        {"1,2,3,4", "a,b,c", "Omni,Operator"}, ",", 2,
        inputVec, delimiterVec, limitVec
    );

    SplitFunctionTestHelper::ExecuteSplitFunction(inputVec, delimiterVec, limitVec, rowSize);

    delete inputVec;
    delete delimiterVec;

    std::cout << "=== Multi-Row Test Completed ===" << std::endl;
}
