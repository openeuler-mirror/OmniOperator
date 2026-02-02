/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Substr function unit tests
 *   substr(string, start) -> varchar
 *   substr(string, start, length) -> varchar
 *   Spark + Gluten: INT for start/length; Spark semantics (start=0 -> first char).
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
 
 class SubstrTestEnvironment : public ::testing::Environment {
 public:
     void SetUp() override {
         RegisterFunctions::RegisterAllFunctions("");
     }
 };
 
 ::testing::Environment* const substr_test_env =
     ::testing::AddGlobalTestEnvironment(new SubstrTestEnvironment);
 
 class SubstrFunctionTestHelper {
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
 
     static BaseVector* CreateInt32Vector(const std::vector<int32_t>& values) {
         BaseVector* vec = VectorHelper::CreateFlatVector(OMNI_INT, values.size());
         vec->SetIsField(true);
         auto* typed = static_cast<Vector<int32_t>*>(vec);
         for (size_t i = 0; i < values.size(); ++i) {
             typed->SetValue(i, values[i]);
         }
         return vec;
     }
 
     static void ExecuteSubstrTwoArg(BaseVector* stringVec, BaseVector* startVec,
                                     BaseVector*& result) {
         // Lookup by OMNI_VARCHAR (Spark/Gluten actual type); CreateStringVector returns OMNI_CHAR
         std::vector<DataTypeId> inputTypeIds = { OMNI_VARCHAR, startVec->GetTypeId() };
         auto sig = std::make_shared<FunctionSignature>("substr", inputTypeIds, OMNI_VARCHAR);
         auto fn = VectorFunction::Find(sig);
         ASSERT_NE(fn, nullptr) << "substr(string, start) not found";
         auto outputType = std::make_shared<DataType>(OMNI_VARCHAR);
         ExecutionContext ctx;
         ctx.SetResultRowSize(stringVec->GetSize());
         // SimpleFunction pops last arg first: push (string, start) so pop order is (start, string)
         std::stack<BaseVector*> args;
         args.push(stringVec);
         args.push(startVec);
         ASSERT_NO_THROW(fn->Apply(args, outputType, result, &ctx));
     }
 
     static void ExecuteSubstrThreeArg(BaseVector* stringVec, BaseVector* startVec,
                                      BaseVector* lengthVec, BaseVector*& result) {
         // Lookup by OMNI_VARCHAR (Spark/Gluten actual type); CreateStringVector returns OMNI_CHAR
         std::vector<DataTypeId> inputTypeIds = {
             OMNI_VARCHAR, startVec->GetTypeId(), lengthVec->GetTypeId()
         };
         auto sig = std::make_shared<FunctionSignature>("substr", inputTypeIds, OMNI_VARCHAR);
         auto fn = VectorFunction::Find(sig);
         ASSERT_NE(fn, nullptr) << "substr(string, start, length) not found";
         auto outputType = std::make_shared<DataType>(OMNI_VARCHAR);
         ExecutionContext ctx;
         ctx.SetResultRowSize(stringVec->GetSize());
         // SimpleFunction pops last arg first: push (string, start, length) so pop order is (length, start, string)
         std::stack<BaseVector*> args;
         args.push(stringVec);
         args.push(startVec);
         args.push(lengthVec);
         ASSERT_NO_THROW(fn->Apply(args, outputType, result, &ctx));
     }
 };
 
 TEST(SubstrTest, BasicSubstrTwoArg) {
     std::vector<std::string> strings = {"hello", "world", "apple"};
     std::vector<int32_t> starts = {1, 2, 3};
     std::vector<std::string> expected = {"hello", "orld", "ple"};
     BaseVector* strVec = SubstrFunctionTestHelper::CreateStringVector(strings);
     BaseVector* startVec = SubstrFunctionTestHelper::CreateInt32Vector(starts);
     BaseVector* result = nullptr;
     SubstrFunctionTestHelper::ExecuteSubstrTwoArg(strVec, startVec, result);
     SubstrFunctionTestHelper::ValidateStringResult(result, expected, 3);
     delete strVec;
     delete startVec;
     delete result;
 }
 
 TEST(SubstrTest, BasicSubstrThreeArg) {
     std::vector<std::string> strings = {"hello", "world", "apple"};
     std::vector<int32_t> starts = {1, 1, 2};
     std::vector<int32_t> lengths = {2, 5, 3};
     std::vector<std::string> expected = {"he", "world", "ppl"};
     BaseVector* strVec = SubstrFunctionTestHelper::CreateStringVector(strings);
     BaseVector* startVec = SubstrFunctionTestHelper::CreateInt32Vector(starts);
     BaseVector* lengthVec = SubstrFunctionTestHelper::CreateInt32Vector(lengths);
     BaseVector* result = nullptr;
     SubstrFunctionTestHelper::ExecuteSubstrThreeArg(strVec, startVec, lengthVec, result);
     SubstrFunctionTestHelper::ValidateStringResult(result, expected, 3);
     delete strVec;
     delete startVec;
     delete lengthVec;
     delete result;
 }
 
 TEST(SubstrTest, NegativeStart) {
     std::vector<std::string> strings = {"my string here", "my string here", "my string here"};
     std::vector<int32_t> starts = {-3, -1, -10};
     std::vector<int32_t> lengths = {3, 3, 100};
     std::vector<std::string> expected = {"ere", "e", "tring here"};
     BaseVector* strVec = SubstrFunctionTestHelper::CreateStringVector(strings);
     BaseVector* startVec = SubstrFunctionTestHelper::CreateInt32Vector(starts);
     BaseVector* lengthVec = SubstrFunctionTestHelper::CreateInt32Vector(lengths);
     BaseVector* result = nullptr;
     SubstrFunctionTestHelper::ExecuteSubstrThreeArg(strVec, startVec, lengthVec, result);
     SubstrFunctionTestHelper::ValidateStringResult(result, expected, 3);
     delete strVec;
     delete startVec;
     delete lengthVec;
     delete result;
 }
 
 TEST(SubstrTest, NegativeStartTwoArg) {
     std::vector<std::string> strings = {"my string here"};
     std::vector<int32_t> starts = {-3};
     std::vector<std::string> expected = {"ere"};
     BaseVector* strVec = SubstrFunctionTestHelper::CreateStringVector(strings);
     BaseVector* startVec = SubstrFunctionTestHelper::CreateInt32Vector(starts);
     BaseVector* result = nullptr;
     SubstrFunctionTestHelper::ExecuteSubstrTwoArg(strVec, startVec, result);
     SubstrFunctionTestHelper::ValidateStringResult(result, expected, 1);
     delete strVec;
     delete startVec;
     delete result;
 }
 
 TEST(SubstrTest, SparkStartZero) {
     std::vector<std::string> strings = {"example", "apple"};
     std::vector<int32_t> starts = {0, 0};
     std::vector<int32_t> lengths = {2, 3};
     std::vector<std::string> expected = {"ex", "app"};
     BaseVector* strVec = SubstrFunctionTestHelper::CreateStringVector(strings);
     BaseVector* startVec = SubstrFunctionTestHelper::CreateInt32Vector(starts);
     BaseVector* lengthVec = SubstrFunctionTestHelper::CreateInt32Vector(lengths);
     BaseVector* result = nullptr;
     SubstrFunctionTestHelper::ExecuteSubstrThreeArg(strVec, startVec, lengthVec, result);
     SubstrFunctionTestHelper::ValidateStringResult(result, expected, 2);
     delete strVec;
     delete startVec;
     delete lengthVec;
     delete result;
 }
 
 TEST(SubstrTest, EmptyResult) {
     std::vector<std::string> strings = {"apple", "apple"};
     std::vector<int32_t> starts = {10, 1};
     std::vector<int32_t> lengths = {2, 0};
     std::vector<std::string> expected = {"", ""};
     BaseVector* strVec = SubstrFunctionTestHelper::CreateStringVector(strings);
     BaseVector* startVec = SubstrFunctionTestHelper::CreateInt32Vector(starts);
     BaseVector* lengthVec = SubstrFunctionTestHelper::CreateInt32Vector(lengths);
     BaseVector* result = nullptr;
     SubstrFunctionTestHelper::ExecuteSubstrThreeArg(strVec, startVec, lengthVec, result);
     SubstrFunctionTestHelper::ValidateStringResult(result, expected, 2);
     delete strVec;
     delete startVec;
     delete lengthVec;
     delete result;
 }
 
 TEST(SubstrTest, LengthClampedToEnd) {
     std::vector<std::string> strings = {"hello"};
     std::vector<int32_t> starts = {2};
     std::vector<int32_t> lengths = {100};
     std::vector<std::string> expected = {"ello"};
     BaseVector* strVec = SubstrFunctionTestHelper::CreateStringVector(strings);
     BaseVector* startVec = SubstrFunctionTestHelper::CreateInt32Vector(starts);
     BaseVector* lengthVec = SubstrFunctionTestHelper::CreateInt32Vector(lengths);
     BaseVector* result = nullptr;
     SubstrFunctionTestHelper::ExecuteSubstrThreeArg(strVec, startVec, lengthVec, result);
     SubstrFunctionTestHelper::ValidateStringResult(result, expected, 1);
     delete strVec;
     delete startVec;
     delete lengthVec;
     delete result;
 }
 
 TEST(SubstrTest, Boundary) {
     // 空串、start 超出长度、单字符
     std::vector<std::string> strings = {"", "ab", "x"};
     std::vector<int32_t> starts = {1, 10, 1};
     std::vector<int32_t> lengths = {5, 5, 1};
     std::vector<std::string> expected = {"", "", "x"};
     BaseVector* strVec = SubstrFunctionTestHelper::CreateStringVector(strings);
     BaseVector* startVec = SubstrFunctionTestHelper::CreateInt32Vector(starts);
     BaseVector* lengthVec = SubstrFunctionTestHelper::CreateInt32Vector(lengths);
     BaseVector* result = nullptr;
     SubstrFunctionTestHelper::ExecuteSubstrThreeArg(strVec, startVec, lengthVec, result);
     SubstrFunctionTestHelper::ValidateStringResult(result, expected, 3);
     delete strVec;
     delete startVec;
     delete lengthVec;
     delete result;
 }