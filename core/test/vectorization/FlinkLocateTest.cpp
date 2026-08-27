/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Flink locate function unit tests
 *   flink_locate(substring, string, start) -> int32
 *   Returns 1-based position; 0 if not found or invalid start.
 *   Differs from Spark locate only in that start == 0 is treated as start == 1.
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

class FlinkLocateTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const flink_locate_test_env =
    ::testing::AddGlobalTestEnvironment(new FlinkLocateTestEnvironment);

class FlinkLocateFunctionTestHelper {
public:
    static void ValidateNumericResult(BaseVector* result,
                                      const std::vector<int32_t>& expected,
                                      int rowSize) {
        auto* resultVec = dynamic_cast<Vector<int32_t>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector type mismatch";
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                continue;
            }
            int32_t actual = resultVec->GetValue(i);
            int32_t exp = expected[i];
            EXPECT_EQ(actual, exp) << "Row " << i << " expected=" << exp << " actual=" << actual;
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

    template <typename T>
    static BaseVector* CreateNumericVector(const std::vector<T>& values, DataTypeId typeId) {
        BaseVector* vec = VectorHelper::CreateFlatVector(typeId, values.size());
        vec->SetIsField(true);
        auto* typed = static_cast<Vector<T>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            typed->SetValue(i, values[i]);
        }
        return vec;
    }

    static void ExecuteFlinkLocate(BaseVector* subStringVec, BaseVector* stringVec,
                                   BaseVector* startVec, DataTypeId outputTypeId,
                                   BaseVector*& result) {
        std::vector<DataTypeId> inputTypeIds = {
            subStringVec->GetTypeId(),
            stringVec->GetTypeId(),
            startVec->GetTypeId()
        };
        auto sig = std::make_shared<FunctionSignature>("flink_locate", inputTypeIds, outputTypeId);
        auto fn = VectorFunction::Find(sig);
        ASSERT_NE(fn, nullptr) << "Flink locate function not found";
        auto outputType = std::make_shared<DataType>(outputTypeId);
        ExecutionContext ctx;
        ctx.SetResultRowSize(subStringVec->GetSize());
        std::stack<BaseVector*> args;
        args.push(subStringVec);
        args.push(stringVec);
        args.push(startVec);
        ASSERT_NO_THROW(fn->Apply(args, outputType, result, &ctx));
    }
};

TEST(FlinkLocateTest, BasicFlinkLocate) {
    std::vector<std::string> subStrings = {"aa", "aa", "xyz"};
    std::vector<std::string> strings = {"aaads", "aaads", "aaads"};
    std::vector<int32_t> starts = {1, 2, 1};
    std::vector<int32_t> expected = {1, 2, 0};
    BaseVector* subVec = FlinkLocateFunctionTestHelper::CreateStringVector(subStrings);
    BaseVector* strVec = FlinkLocateFunctionTestHelper::CreateStringVector(strings);
    BaseVector* startVec = FlinkLocateFunctionTestHelper::CreateNumericVector(starts, OMNI_INT);
    BaseVector* result = nullptr;
    FlinkLocateFunctionTestHelper::ExecuteFlinkLocate(subVec, strVec, startVec, OMNI_INT, result);
    FlinkLocateFunctionTestHelper::ValidateNumericResult(result, expected, 3);
    delete subVec;
    delete strVec;
    delete startVec;
    delete result;
}

// Flink semantics: start == 0 is treated as start == 1.
// Spark locate would return 0 for start == 0; flink_locate returns the same as start == 1.
TEST(FlinkLocateTest, ZeroStartEqualsOne) {
    std::vector<std::string> subStrings = {"aa", "aa", "aa", "d"};
    std::vector<std::string> strings = {"aaads", "aaads", "aaads", "aaads"};
    std::vector<int32_t> starts = {0, 2, 3, 0};
    std::vector<int32_t> expected = {1, 2, 0, 4};
    BaseVector* subVec = FlinkLocateFunctionTestHelper::CreateStringVector(subStrings);
    BaseVector* strVec = FlinkLocateFunctionTestHelper::CreateStringVector(strings);
    BaseVector* startVec = FlinkLocateFunctionTestHelper::CreateNumericVector(starts, OMNI_INT);
    BaseVector* result = nullptr;
    FlinkLocateFunctionTestHelper::ExecuteFlinkLocate(subVec, strVec, startVec, OMNI_INT, result);
    FlinkLocateFunctionTestHelper::ValidateNumericResult(result, expected, 4);
    delete subVec;
    delete strVec;
    delete startVec;
    delete result;
}

// Combined: start == 0 behaves like 1, negative start still yields 0, start beyond length yields 0.
// Contrast with Spark locate, where all three rows would return 0.
TEST(FlinkLocateTest, ZeroNegativeAndOverflowStart) {
    std::vector<std::string> subStrings = {"aa", "aa", "aa"};
    std::vector<std::string> strings = {"aaads", "aaads", "aaads"};
    std::vector<int32_t> starts = {0, -1, 10};
    std::vector<int32_t> expected = {1, 0, 0};
    BaseVector* subVec = FlinkLocateFunctionTestHelper::CreateStringVector(subStrings);
    BaseVector* strVec = FlinkLocateFunctionTestHelper::CreateStringVector(strings);
    BaseVector* startVec = FlinkLocateFunctionTestHelper::CreateNumericVector(starts, OMNI_INT);
    BaseVector* result = nullptr;
    FlinkLocateFunctionTestHelper::ExecuteFlinkLocate(subVec, strVec, startVec, OMNI_INT, result);
    FlinkLocateFunctionTestHelper::ValidateNumericResult(result, expected, 3);
    delete subVec;
    delete strVec;
    delete startVec;
    delete result;
}

TEST(FlinkLocateTest, EmptySubstringReturnsOne) {
    std::vector<std::string> subStrings = {"", "", ""};
    std::vector<std::string> strings = {"aaads", "test", "hello"};
    std::vector<int32_t> starts = {1, 1, 1};
    std::vector<int32_t> expected = {1, 1, 1};
    BaseVector* subVec = FlinkLocateFunctionTestHelper::CreateStringVector(subStrings);
    BaseVector* strVec = FlinkLocateFunctionTestHelper::CreateStringVector(strings);
    BaseVector* startVec = FlinkLocateFunctionTestHelper::CreateNumericVector(starts, OMNI_INT);
    BaseVector* result = nullptr;
    FlinkLocateFunctionTestHelper::ExecuteFlinkLocate(subVec, strVec, startVec, OMNI_INT, result);
    FlinkLocateFunctionTestHelper::ValidateNumericResult(result, expected, 3);
    delete subVec;
    delete strVec;
    delete startVec;
    delete result;
}

// Empty substring with start == 0 still yields 1 (start 0 ≡ 1, empty substring → 1).
TEST(FlinkLocateTest, EmptySubstringWithZeroStartReturnsOne) {
    std::vector<std::string> subStrings = {"", ""};
    std::vector<std::string> strings = {"aaads", "test"};
    std::vector<int32_t> starts = {0, 0};
    std::vector<int32_t> expected = {1, 1};
    BaseVector* subVec = FlinkLocateFunctionTestHelper::CreateStringVector(subStrings);
    BaseVector* strVec = FlinkLocateFunctionTestHelper::CreateStringVector(strings);
    BaseVector* startVec = FlinkLocateFunctionTestHelper::CreateNumericVector(starts, OMNI_INT);
    BaseVector* result = nullptr;
    FlinkLocateFunctionTestHelper::ExecuteFlinkLocate(subVec, strVec, startVec, OMNI_INT, result);
    FlinkLocateFunctionTestHelper::ValidateNumericResult(result, expected, 2);
    delete subVec;
    delete strVec;
    delete startVec;
    delete result;
}

TEST(FlinkLocateTest, EmptyStringReturnsZero) {
    std::vector<std::string> subStrings = {"aa", "x"};
    std::vector<std::string> strings = {"", ""};
    std::vector<int32_t> starts = {1, 1};
    std::vector<int32_t> expected = {0, 0};
    BaseVector* subVec = FlinkLocateFunctionTestHelper::CreateStringVector(subStrings);
    BaseVector* strVec = FlinkLocateFunctionTestHelper::CreateStringVector(strings);
    BaseVector* startVec = FlinkLocateFunctionTestHelper::CreateNumericVector(starts, OMNI_INT);
    BaseVector* result = nullptr;
    FlinkLocateFunctionTestHelper::ExecuteFlinkLocate(subVec, strVec, startVec, OMNI_INT, result);
    FlinkLocateFunctionTestHelper::ValidateNumericResult(result, expected, 2);
    delete subVec;
    delete strVec;
    delete startVec;
    delete result;
}

TEST(FlinkLocateTest, SubstringLongerThanStringReturnsZero) {
    std::vector<std::string> subStrings = {"aaadsxyz", "longer"};
    std::vector<std::string> strings = {"aaads", "hi"};
    std::vector<int32_t> starts = {1, 1};
    std::vector<int32_t> expected = {0, 0};
    BaseVector* subVec = FlinkLocateFunctionTestHelper::CreateStringVector(subStrings);
    BaseVector* strVec = FlinkLocateFunctionTestHelper::CreateStringVector(strings);
    BaseVector* startVec = FlinkLocateFunctionTestHelper::CreateNumericVector(starts, OMNI_INT);
    BaseVector* result = nullptr;
    FlinkLocateFunctionTestHelper::ExecuteFlinkLocate(subVec, strVec, startVec, OMNI_INT, result);
    FlinkLocateFunctionTestHelper::ValidateNumericResult(result, expected, 2);
    delete subVec;
    delete strVec;
    delete startVec;
    delete result;
}

// 空值传播：在 SimpleFunction 框架下，任一参数为 NULL 的列会从计算中排除，
// 该列结果被标记为 NULL。因此当 sub/str/start 中任意一个为 NULL 时，该列结果均为 NULL。
TEST(FlinkLocateTest, NullHandling) {
    std::vector<std::string> subStrings = {"aa", "aa", "aa"};
    std::vector<std::string> strings = {"aaads", "aaads", "aaads"};
    std::vector<int32_t> starts = {1, 1, 1};
    BaseVector* subVec = FlinkLocateFunctionTestHelper::CreateStringVector(subStrings);
    BaseVector* strVec = FlinkLocateFunctionTestHelper::CreateStringVector(strings);
    BaseVector* startVec = FlinkLocateFunctionTestHelper::CreateNumericVector(starts, OMNI_INT);
    subVec->SetNull(0);
    strVec->SetNull(1);
    startVec->SetNull(2);
    BaseVector* result = nullptr;
    FlinkLocateFunctionTestHelper::ExecuteFlinkLocate(subVec, strVec, startVec, OMNI_INT, result);
    EXPECT_TRUE(result->IsNull(0));  // substring NULL → result NULL
    EXPECT_TRUE(result->IsNull(1));  // string NULL → result NULL
    EXPECT_TRUE(result->IsNull(2));  // start NULL → result NULL
    delete subVec;
    delete strVec;
    delete startVec;
    delete result;
}

TEST(FlinkLocateTest, UnicodeSupport) {
    std::vector<std::string> subStrings = {"万丈", "万丈", "xyz"};
    std::vector<std::string> strings = {"一丁丂七丄丅丆万丈三上下", "一丁丂七丄丅丆万丈三上下", "一丁丂七丄丅丆万丈三上下"};
    std::vector<int32_t> starts = {1, 8, 1};
    std::vector<int32_t> expected = {8, 8, 0};
    BaseVector* subVec = FlinkLocateFunctionTestHelper::CreateStringVector(subStrings);
    BaseVector* strVec = FlinkLocateFunctionTestHelper::CreateStringVector(strings);
    BaseVector* startVec = FlinkLocateFunctionTestHelper::CreateNumericVector(starts, OMNI_INT);
    BaseVector* result = nullptr;
    FlinkLocateFunctionTestHelper::ExecuteFlinkLocate(subVec, strVec, startVec, OMNI_INT, result);
    FlinkLocateFunctionTestHelper::ValidateNumericResult(result, expected, 3);
    delete subVec;
    delete strVec;
    delete startVec;
    delete result;
}

// Unicode with start == 0: equivalent to start == 1.
TEST(FlinkLocateTest, UnicodeZeroStartEqualsOne) {
    std::vector<std::string> subStrings = {"万丈", "下"};
    std::vector<std::string> strings = {"一丁丂七丄丅丆万丈三上下", "一丁丂七丄丅丆万丈三上下"};
    std::vector<int32_t> starts = {0, 0};
    std::vector<int32_t> expected = {8, 12};
    BaseVector* subVec = FlinkLocateFunctionTestHelper::CreateStringVector(subStrings);
    BaseVector* strVec = FlinkLocateFunctionTestHelper::CreateStringVector(strings);
    BaseVector* startVec = FlinkLocateFunctionTestHelper::CreateNumericVector(starts, OMNI_INT);
    BaseVector* result = nullptr;
    FlinkLocateFunctionTestHelper::ExecuteFlinkLocate(subVec, strVec, startVec, OMNI_INT, result);
    FlinkLocateFunctionTestHelper::ValidateNumericResult(result, expected, 2);
    delete subVec;
    delete strVec;
    delete startVec;
    delete result;
}

TEST(FlinkLocateTest, SingleRow) {
    std::vector<std::string> subStrings = {"o"};
    std::vector<std::string> strings = {"hello"};
    std::vector<int32_t> starts = {1};
    std::vector<int32_t> expected = {5};
    BaseVector* subVec = FlinkLocateFunctionTestHelper::CreateStringVector(subStrings);
    BaseVector* strVec = FlinkLocateFunctionTestHelper::CreateStringVector(strings);
    BaseVector* startVec = FlinkLocateFunctionTestHelper::CreateNumericVector(starts, OMNI_INT);
    BaseVector* result = nullptr;
    FlinkLocateFunctionTestHelper::ExecuteFlinkLocate(subVec, strVec, startVec, OMNI_INT, result);
    FlinkLocateFunctionTestHelper::ValidateNumericResult(result, expected, 1);
    delete subVec;
    delete strVec;
    delete startVec;
    delete result;
}
