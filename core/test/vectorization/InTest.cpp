///*
//* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
// * Description: codegen test
// */
//
//#include <gtest/gtest.h>
//#include <iostream>
//#include <string>
//#include <vector>
//
//#include "test/util/test_util.h"
//#include "vectorization/registration/Register.h"
//#include "vectorization/ExprEval.h"
//#include "expression/expressions.h"
//#include "type/decimal_operations.h"
//#include "vectorization/registration/SimpleFunctionRegistry.h"
//#include "expression/parserhelper.h"
//
//using namespace omniruntime;
//using namespace omniruntime::vec;
//using namespace omniruntime::vectorization;
//using namespace omniruntime::mem;
//using namespace omniruntime::op;
//using namespace omniruntime::expressions;
//using namespace omniruntime::TestUtil;
//
//VectorBatch* CreateIntVectorBatchWithNulls(int32_t rowSize, const int32_t* data, const std::vector<bool>& isNull)
//{
//    VectorBatch* batch = new VectorBatch(static_cast<size_t>(rowSize));
//    BaseVector* intVec = VectorHelper::CreateFlatVector(OMNI_INT, rowSize);
//    auto* vec = static_cast<Vector<int32_t>*>(intVec);
//
//    for (int i = 0; i < rowSize; ++i) {
//        vec->SetValue(i, data[i]);
//    }
//
//    for (int i = 0; i < rowSize; ++i) {
//        if (isNull[i]) {
//            vec->SetNull(i);
//        } else {
//            vec->SetNotNull(i);
//        }
//    }
//
//    batch->ResizeVectorCount(1);
//    batch->SetVector(0, vec);
//
//    return batch;
//}
//
//VectorBatch* CreateVarcharVectorBatchWithNulls(int32_t rowSize, const std::vector<std::string>& data, const std::vector<bool>& isNull)
//{
//    VectorBatch* batch = new VectorBatch(static_cast<size_t>(rowSize));
//    BaseVector* varcharVec = VectorHelper::CreateFlatVector(OMNI_VARCHAR, rowSize);
//    auto* vec = static_cast<Vector<LargeStringContainer<std::string_view>>*>(varcharVec);
//
//    for (int i = 0; i < rowSize; ++i) {
//        std::string_view sv(data[i]);
//        vec->SetValue(i, sv);
//    }
//
//    for (int i = 0; i < rowSize; ++i) {
//        if (isNull[i]) {
//            vec->SetNull(i);
//        } else {
//            vec->SetNotNull(i);
//        }
//    }
//
//    batch->ResizeVectorCount(1);
//    batch->SetVector(0, vec);
//
//    return batch;
//}
//
//TEST(VectorizationTest, InExprInt32Test)
//{
//    int rowSize = 5;
//    auto intType = std::make_shared<DataType>(OMNI_INT);
//
//    expressions::InExpr* inExpr = new expressions::InExpr({
//        new expressions::FieldExpr(0, intType),
//        new expressions::LiteralExpr(2, intType),
//        new expressions::LiteralExpr(4, intType),
//        new expressions::LiteralExpr(6, intType)
//    });
//
//    int32_t col0[rowSize] = {1, 2, 3, 4, 5};
//    std::vector<DataTypePtr> vecOfTypes = {intType};
//    DataTypes inputTypes(vecOfTypes);
//    VectorBatch* inputBatch = CreateVectorBatch(inputTypes, rowSize, col0);
//
//    ExecutionContext* context = new ExecutionContext();
//    context->SetResultRowSize(rowSize);
//
//    ExprEval exprEval(inputBatch, context);
//    exprEval.VisitExpr(*inExpr);
//    BaseVector* resultVec = exprEval.GetResult();
//
//    auto* boolResult = static_cast<Vector<bool>*>(resultVec);
//    std::vector<bool> expected = {false, true, false, true, false};
//    for (int i = 0; i < rowSize; i++) {
//        bool actual = boolResult->GetValue(i);
//        bool exp = expected[i];
//        std::cout << "Int Value:" << col0[i] << ", Actual Result=" << actual << ", Expected Result=" << exp << std::endl;
//        ASSERT_EQ(actual, exp) << "Int Value In Expression Line " << i << " is Wrong";
//    }
//
//    delete inputBatch;
//    delete inExpr;
//    delete context;
//}
//
//TEST(VectorizationTest, InExprStringViewTest)
//{
//    int rowSize = 5;
//    auto varcharType = std::make_shared<DataType>(OMNI_VARCHAR);
//
//    expressions::InExpr* inExpr = new expressions::InExpr({
//        new expressions::FieldExpr(0, varcharType),
//        new expressions::LiteralExpr(new std::string("apple"), varcharType),
//        new expressions::LiteralExpr(new std::string("banana"), varcharType),
//        new expressions::LiteralExpr(new std::string("cherry"), varcharType)
//    });
//
//    std::vector<std::string> col0 = {"apple", "orange", "banana", "grape", ""};
//    std::vector<bool> expected = {true, false, true, false, false};
//
//    std::vector<DataTypePtr> vecOfTypes = {varcharType};
//    DataTypes inputTypes(vecOfTypes);
//    VectorBatch* inputBatch = CreateVectorBatch(inputTypes, rowSize, col0.data());
//
//    ExecutionContext* context = new ExecutionContext();
//    context->SetResultRowSize(rowSize);
//
//    ExprEval exprEval(inputBatch, context);
//    exprEval.VisitExpr(*inExpr);
//    BaseVector* resultVec = exprEval.GetResult();
//
//    auto* boolResult = static_cast<Vector<bool>*>(resultVec);
//    for (int i = 0; i < rowSize; i++) {
//        bool actual = boolResult->GetValue(i);
//        bool exp = expected[i];
//        std::cout << "String Value:\"" << col0[i] << "\", Actual Result=" << actual << ", Expected Result=" << exp << std::endl;
//        ASSERT_EQ(actual, exp) << "String Value In Expression Line " << i << " is Wrong";
//    }
//
//    delete inputBatch;
//    delete inExpr;
//    delete context;
//}
//
//TEST(VectorizationTest, InExprInt32NullTest)
//{
//    const int rowSize = 5;
//    auto intType = std::make_shared<DataType>(OMNI_INT);
//
//    InExpr* inExpr = new InExpr({
//        new FieldExpr(0, intType),
//        new LiteralExpr(2, intType),
//        new LiteralExpr(4, intType),
//        new LiteralExpr(6, intType)
//    });
//
//    int32_t col0[rowSize] = {1, 2, 3, 4, 5};
//    std::vector<bool> fieldIsNull = {false, false, true, false, true};
//    VectorBatch* inputBatch = CreateIntVectorBatchWithNulls(rowSize, col0, fieldIsNull);
//
//    ExecutionContext* context = new ExecutionContext();
//    context->SetResultRowSize(rowSize);
//    ExprEval exprEval(inputBatch, context);
//    exprEval.VisitExpr(*inExpr);
//    BaseVector* resultVec = exprEval.GetResult();
//
//    auto* boolResult = static_cast<Vector<bool>*>(resultVec);
//    for (int i = 0; i < rowSize; ++i) {
//        bool actual = boolResult->GetValue(i);
//        std::string fieldValStr = fieldIsNull[i] ? "NULL" : std::to_string(col0[i]);
//        std::string resultStr = boolResult->IsNull(i) ? "NULL" : (actual ? "true" : "false");
//        std::string expectedResultStr = fieldIsNull[i] ? "NULL" : (
//                (col0[i] == 2 || col0[i] == 4 || col0[i] == 6) ? "true" : "false"
//        );
//        std::cout << "Int Value:" << fieldValStr << ", Actual Result=" << resultStr << ", Expected Result=" << expectedResultStr << std::endl;
//
//        if (fieldIsNull[i]) {
//            ASSERT_TRUE(boolResult->IsNull(i)) << "Int Null Field In Expression Line " << i << " Result Is Not NULL";
//        } else {
//            bool expected = (col0[i] == 2 || col0[i] == 4 || col0[i] == 6);
//            ASSERT_FALSE(boolResult->IsNull(i)) << "Int Non-Null Field In Expression Line " << i << " Result Is NULL";
//            ASSERT_EQ(actual, expected) << "Int Null Field In Expression Line " << i << " is Wrong";
//        }
//    }
//
//    delete inputBatch;
//    delete inExpr;
//    delete context;
//}
//
//TEST(VectorizationTest, InExprStringViewNullTest)
//{
//    const int rowSize = 5;
//    auto varcharType = std::make_shared<DataType>(OMNI_VARCHAR);
//
//    InExpr* inExpr = new InExpr({
//        new FieldExpr(0, varcharType),
//        new LiteralExpr(new std::string("apple"), varcharType),
//        new LiteralExpr(new std::string("banana"), varcharType),
//        new LiteralExpr(new std::string("cherry"), varcharType)
//    });
//
//    std::vector<std::string> col0 = {"apple", "orange", "banana", "grape", "cherry"};
//    std::vector<bool> fieldIsNull = {false, false, true, false, true};
//    VectorBatch* inputBatch = CreateVarcharVectorBatchWithNulls(rowSize, col0, fieldIsNull);
//
//    ExecutionContext* context = new ExecutionContext();
//    context->SetResultRowSize(rowSize);
//    ExprEval exprEval(inputBatch, context);
//    exprEval.VisitExpr(*inExpr);
//    BaseVector* resultVec = exprEval.GetResult();
//
//    auto* boolResult = static_cast<Vector<bool>*>(resultVec);
//    for (int i = 0; i < rowSize; ++i) {
//        bool actual = boolResult->GetValue(i);
//        std::string fieldValStr = fieldIsNull[i] ? "NULL" : ("\"" + col0[i] + "\"");
//        std::string resultStr = boolResult->IsNull(i) ? "NULL" : (actual ? "true" : "false");
//        std::string expectedResultStr = fieldIsNull[i] ? "NULL" : (
//                (col0[i] == "apple" || col0[i] == "banana" || col0[i] == "cherry") ? "true" : "false"
//        );
//        std::cout << "String Value:" << fieldValStr << ", Actual Result=" << resultStr << ", Expected Result=" << expectedResultStr << std::endl;
//
//        if (fieldIsNull[i]) {
//            ASSERT_TRUE(boolResult->IsNull(i)) << "String Null Field In Expression Line " << i << " Result Is Not NULL";
//        } else {
//            bool expected = (col0[i] == "apple" || col0[i] == "banana" || col0[i] == "cherry");
//            ASSERT_FALSE(boolResult->IsNull(i)) << "String Non-Null Field In Expression Line " << i << " Result Is NULL";
//            ASSERT_EQ(actual, expected) << "String Null Field In Expression Line " << i << " is Wrong";
//        }
//    }
//
//    delete inputBatch;
//    delete inExpr;
//    delete context;
//}
//
//TEST(VectorizationTest, InExprInt32NullCandidateAtStartTest) {
//    const int rowSize = 5;
//    auto intType = std::make_shared<DataType>(OMNI_INT);
//
//    InExpr* inExpr = new InExpr({
//        new FieldExpr(0, intType),
//        []() -> LiteralExpr* {
//            auto nullLiteral = ParserHelper::GetDefaultValueForType(OMNI_INT);
//            nullLiteral->isNull = true;
//            return nullLiteral;
//        }(),
//        new LiteralExpr(2, intType),
//        new LiteralExpr(4, intType)
//    });
//
//    int32_t col0[rowSize] = {0, 2, 3, 4, 5};
//    std::vector<bool> fieldIsNull = {false, false, false, false, false};
//    VectorBatch* inputBatch = CreateIntVectorBatchWithNulls(rowSize, col0, fieldIsNull);
//
//    ExecutionContext* context = new ExecutionContext();
//    context->SetResultRowSize(rowSize);
//    ExprEval exprEval(inputBatch, context);
//    exprEval.VisitExpr(*inExpr);
//    BaseVector* resultVec = exprEval.GetResult();
//
//    auto* boolResult = static_cast<Vector<bool>*>(resultVec);
//    for (int i = 0; i < rowSize; ++i) {
//        std::string fieldValStr = std::to_string(col0[i]);
//        bool expected = (col0[i] == 2 || col0[i] == 4);
//        std::string resultStr = boolResult->IsNull(i) ? "NULL" : (boolResult->GetValue(i) ? "true" : "false");
//        std::string expectedStr = expected ? "true" : "false";
//
//        std::cout << "Int Value:" << fieldValStr
//                  << ", Actual Result=" << resultStr
//                  << ", Expected Result=" << expectedStr << std::endl;
//
//        ASSERT_FALSE(boolResult->IsNull(i)) << "Line " << i << ": Non-NULL field should not return NULL";
//        ASSERT_EQ(boolResult->GetValue(i), expected) << "Line " << i << ": Match result error";
//    }
//
//    delete inputBatch;
//    delete inExpr;
//    delete context;
//}
//
//TEST(VectorizationTest, InExprInt32NullCandidateInMiddleTest) {
//    const int rowSize = 5;
//    auto intType = std::make_shared<DataType>(OMNI_INT);
//
//    InExpr* inExpr = new InExpr({
//        new FieldExpr(0, intType),
//        new LiteralExpr(2, intType),
//        []() -> LiteralExpr* {
//            auto nullLiteral = ParserHelper::GetDefaultValueForType(OMNI_INT);
//            nullLiteral->isNull = true;
//            return nullLiteral;
//        }(),
//        new LiteralExpr(4, intType)
//    });
//
//    int32_t col0[rowSize] = {0, 2, 3, 4, 5};
//    std::vector<bool> fieldIsNull = {false, false, false, false, false};
//    VectorBatch* inputBatch = CreateIntVectorBatchWithNulls(rowSize, col0, fieldIsNull);
//
//    ExecutionContext* context = new ExecutionContext();
//    context->SetResultRowSize(rowSize);
//    ExprEval exprEval(inputBatch, context);
//    exprEval.VisitExpr(*inExpr);
//    BaseVector* resultVec = exprEval.GetResult();
//
//    auto* boolResult = static_cast<Vector<bool>*>(resultVec);
//    for (int i = 0; i < rowSize; ++i) {
//        std::string fieldValStr = std::to_string(col0[i]);
//        bool expected = (col0[i] == 2 || col0[i] == 4);
//        std::string resultStr = boolResult->IsNull(i) ? "NULL" : (boolResult->GetValue(i) ? "true" : "false");
//        std::string expectedStr = expected ? "true" : "false";
//
//        std::cout << "Int Value:" << fieldValStr
//                  << ", Actual Result=" << resultStr
//                  << ", Expected Result=" << expectedStr << std::endl;
//
//        ASSERT_FALSE(boolResult->IsNull(i)) << "Line " << i << ": Non-NULL field should not return NULL";
//        ASSERT_EQ(boolResult->GetValue(i), expected) << "Line " << i << ": Match result error";
//    }
//
//    delete inputBatch;
//    delete inExpr;
//    delete context;
//}
//
//TEST(VectorizationTest, InExprInt32NullCandidateAtEndTest) {
//    const int rowSize = 5;
//    auto intType = std::make_shared<DataType>(OMNI_INT);
//
//    InExpr* inExpr = new InExpr({
//        new FieldExpr(0, intType),
//        new LiteralExpr(2, intType),
//        new LiteralExpr(4, intType),
//        []() -> LiteralExpr* {
//            auto nullLiteral = ParserHelper::GetDefaultValueForType(OMNI_INT);
//            nullLiteral->isNull = true;
//            return nullLiteral;
//        }()
//    });
//
//    int32_t col0[rowSize] = {0, 2, 3, 4, 5};
//    std::vector<bool> fieldIsNull = {false, false, true, false, false};
//    VectorBatch* inputBatch = CreateIntVectorBatchWithNulls(rowSize, col0, fieldIsNull);
//
//    ExecutionContext* context = new ExecutionContext();
//    context->SetResultRowSize(rowSize);
//    ExprEval exprEval(inputBatch, context);
//    exprEval.VisitExpr(*inExpr);
//    BaseVector* resultVec = exprEval.GetResult();
//
//    auto* boolResult = static_cast<Vector<bool>*>(resultVec);
//    for (int i = 0; i < rowSize; ++i) {
//        std::string fieldValStr = fieldIsNull[i] ? "NULL" : std::to_string(col0[i]);
//        bool expectedMatch = (!fieldIsNull[i]) && (col0[i] == 2 || col0[i] == 4);
//        bool expectedNull = fieldIsNull[i];
//
//        std::string resultStr = boolResult->IsNull(i) ? "NULL" : (boolResult->GetValue(i) ? "true" : "false");
//        std::string expectedStr = expectedNull ? "NULL" : (expectedMatch ? "true" : "false");
//
//        std::cout << "Int Value:" << fieldValStr
//                  << ", Actual Result=" << resultStr
//                  << ", Expected Result=" << expectedStr << std::endl;
//
//        if (expectedNull) {
//            ASSERT_TRUE(boolResult->IsNull(i)) << "Line " << i << ": NULL field should return NULL";
//        } else {
//            ASSERT_FALSE(boolResult->IsNull(i)) << "Line " << i << ": Non-NULL field should not return NULL";
//            ASSERT_EQ(boolResult->GetValue(i), expectedMatch) << "Line " << i << ": Match result error";
//        }
//    }
//
//    delete inputBatch;
//    delete inExpr;
//    delete context;
//}
//
//TEST(VectorizationTest, InExprStringNullCandidateInMiddleTest) {
//    const int rowSize = 5;
//    auto varcharType = std::make_shared<DataType>(OMNI_VARCHAR);
//
//    InExpr* inExpr = new InExpr({
//        new FieldExpr(0, varcharType),
//        new LiteralExpr(new std::string("apple"), varcharType),
//        []() -> LiteralExpr* {
//            auto nullLiteral = ParserHelper::GetDefaultValueForType(OMNI_VARCHAR);
//            nullLiteral->isNull = true;
//            return nullLiteral;
//        }(),
//        new LiteralExpr(new std::string("orange"), varcharType)
//    });
//
//    std::vector<std::string> col0 = {"banana", "apple", "", "orange", "grape"};
//    std::vector<bool> fieldIsNull = {false, false, false, false, false};
//    VectorBatch* inputBatch = CreateVarcharVectorBatchWithNulls(rowSize, col0, fieldIsNull);
//
//    ExecutionContext* context = new ExecutionContext();
//    context->SetResultRowSize(rowSize);
//    ExprEval exprEval(inputBatch, context);
//    exprEval.VisitExpr(*inExpr);
//    BaseVector* resultVec = exprEval.GetResult();
//
//    auto* boolResult = static_cast<Vector<bool>*>(resultVec);
//    for (int i = 0; i < rowSize; ++i) {
//        std::string fieldValStr = "\"" + col0[i] + "\"";
//        bool expected = (col0[i] == "apple" || col0[i] == "orange");
//        std::string resultStr = boolResult->IsNull(i) ? "NULL" : (boolResult->GetValue(i) ? "true" : "false");
//        std::string expectedStr = expected ? "true" : "false";
//
//        std::cout << "String Value:" << fieldValStr
//                  << ", Actual Result=" << resultStr
//                  << ", Expected Result=" << expectedStr << std::endl;
//
//        ASSERT_FALSE(boolResult->IsNull(i)) << "Line " << i << ": Non-NULL field should not return NULL";
//        ASSERT_EQ(boolResult->GetValue(i), expected) << "Line " << i << ": Match result error";
//    }
//
//    delete inputBatch;
//    delete inExpr;
//    delete context;
//}