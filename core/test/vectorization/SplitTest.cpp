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
                                const std::vector<std::string>& delimiters,
                                const std::vector<int32_t>& limits,
                                vec::BaseVector*& inputVec,
                                vec::BaseVector*& delimiterVec,
                                vec::BaseVector*& limitVec)
    {
        inputVec = VectorHelper::CreateStringVector(rowSize);
        delimiterVec = VectorHelper::CreateStringVector(rowSize);
        limitVec = VectorHelper::CreateFlatVector(OMNI_INT, rowSize);
        limitVec->SetIsField(true);
        
        auto *inputVector = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(inputVec);
        auto *delimiterVector = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(delimiterVec);
        auto *limitVector = dynamic_cast<Vector<int32_t>*>(limitVec);

        if (!inputVector || !delimiterVector || !limitVector) {
            FAIL() << "Type conversion failed for input vectors";
            return;
        }

        for (int i = 0; i < rowSize; i++) {
            std::string_view inputStr(inputStrs[i]);
            std::string_view delimiterStr(delimiters[i]);
            inputVector->SetValue(i, inputStr);
            delimiterVector->SetValue(i, delimiterStr);
            limitVector->SetValue(i, limits[i]);
        }
    }

    static void ValidateResult(ArrayVector* resultVector, int rowSize)
    {
        for (int row = 0; row < rowSize; ++row) {
            if (resultVector->IsNull(row)) {
                std::cout << "Row " << row << ": Result is NULL" << std::endl;
                continue;
            }

            int64_t arraySize = resultVector->GetSize(row);
            std::cout << "Row " << row << ": Result array size = " << arraySize << std::endl;
            
            auto elementVector = resultVector->GetElementVector();
            if (!elementVector) {
                std::cout << "Row " << row << ": Element vector is NULL" << std::endl;
                continue;
            }

            auto* strElementVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(elementVector.get());
            if (!strElementVec) {
                continue;
            }

            int64_t startIdx = resultVector->GetOffset(row);
            for (int64_t elemIdx = 0; elemIdx < arraySize; ++elemIdx) {
                int64_t globalElemIdx = startIdx + elemIdx;
                if (globalElemIdx < strElementVec->GetSize()) {
                    std::string_view elemValue = strElementVec->GetValue(globalElemIdx);
                    std::cout << "Row " << row << " Element " << elemIdx << ": " << elemValue << std::endl;
                }
            }
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
        auto resultVector = new ArrayVector(rowSize);
        auto arrayType = std::make_shared<DataType>(OMNI_ARRAY);
        
        op::ExecutionContext context;
        context.SetResultRowSize(rowSize);
        
        std::stack<vec::BaseVector*> args;
        args.push(inputVec);
        args.push(delimiterVec);
        args.push(limitVec);
        
        ASSERT_NO_THROW(function->apply(args, arrayType, resultVector, &context))
            << "SplitFunction.apply() threw an unexpected exception";
        
        ValidateResult(resultVector, rowSize);
        delete resultVector;
    }
};
 
TEST(VectorizationTest, SplitFunctionDirectTest) {
    std::cout << "=== Split Function Direct Test ===" << std::endl;
    
    int rowSize = 1;
    vec::BaseVector *inputVec, *delimiterVec, *limitVec;

    SplitFunctionTestHelper::SetupTestVectors(rowSize,
        {"a,b,c"}, {","}, {2}, inputVec, delimiterVec, limitVec);

    SplitFunctionTestHelper::ExecuteSplitFunction(inputVec, delimiterVec, limitVec, rowSize);
    
    delete inputVec;
    delete delimiterVec;
    delete limitVec;
    
    std::cout << "=== Direct Test Completed ===" << std::endl;
}
 
TEST(VectorizationTest, SplitFunctionMultiRowTest) {
    std::cout << "=== Split Function Multi-Row Test ===" << std::endl;
    
    int rowSize = 3;
    vec::BaseVector *inputVec, *delimiterVec, *limitVec;

    SplitFunctionTestHelper::SetupTestVectors(rowSize,
        {"a,b,c", "hello world", "x-y-z"},
        {",", " ", "-"},
        {2, -1, 3},
        inputVec, delimiterVec, limitVec);

    SplitFunctionTestHelper::ExecuteSplitFunction(inputVec, delimiterVec, limitVec, rowSize);
    
    delete inputVec;
    delete delimiterVec;
    delete limitVec;
    
    std::cout << "=== Multi-Row Test Completed ===" << std::endl;
}

class SplitFunctionWithNullsTestHelper {
public:
    static void SetupTestVectorsWithNulls(int rowSize,
                                         vec::BaseVector*& inputVec,
                                         vec::BaseVector*& delimiterVec,
                                         vec::BaseVector*& limitVec)
    {
        inputVec = VectorHelper::CreateStringVector(rowSize);
        delimiterVec = VectorHelper::CreateStringVector(rowSize);
        limitVec = VectorHelper::CreateFlatVector(OMNI_INT, rowSize);
        limitVec->SetIsField(true);
        
        auto *inputVector = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(inputVec);
        auto *delimiterVector = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(delimiterVec);
        auto *limitVector = dynamic_cast<Vector<int32_t>*>(limitVec);

        std::string inputStrings[rowSize] = {
            "a,b,c", "", "hello world", "x-y-z", "null,test", "a-b-c,d-e-f"
        };
        std::string delimiters[rowSize] = {",", ",", "", "-", ",", ""};
        int32_t limits[rowSize] = {2, -1, 5, -1, 3, -1};

        for (int i = 0; i < rowSize; i++) {
            if (i == 4) {
                inputVector->SetNull(i);
            } else {
                std::string_view inputStr(inputStrings[i]);
                inputVector->SetValue(i, inputStr);
            }

            std::string_view delimiterStr(delimiters[i]);
            delimiterVector->SetValue(i, delimiterStr);

            if (i == 3 || i == 5) {
                limitVector->SetNull(i);
            } else {
                limitVector->SetValue(i, limits[i]);
            }
        }
    }
};
 
TEST(VectorizationTest, SplitFunctionMultiRowWithNullsTest) {
    std::cout << "=== Split Function Multi-Row With Nulls Test ===" << std::endl;
    
    int rowSize = 6;
    vec::BaseVector *inputVec, *delimiterVec, *limitVec;

    SplitFunctionWithNullsTestHelper::SetupTestVectorsWithNulls(rowSize, inputVec, delimiterVec, limitVec);
    SplitFunctionTestHelper::ExecuteSplitFunction(inputVec, delimiterVec, limitVec, rowSize);
    
    delete inputVec;
    delete delimiterVec;
    delete limitVec;
    
    std::cout << "=== Multi-Row With Nulls Test Completed ===" << std::endl;
}