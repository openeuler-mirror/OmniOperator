/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Unit tests for the BIN function (bin(int)/bin(bigint) -> varchar).
 */

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <limits>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/VectorFunction.h"
#include "vectorization/functions/BinFunction.h"
#include "type/data_type.h"
#include "vector/vector.h"
#include "vector/vector_helper.h"
#include "codegen/func_registry.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::op;
using namespace omniruntime::TestUtil;
using namespace omniruntime::codegen;
using namespace omniruntime::type;

class BinFunctionTest : public ::testing::Test {
protected:
    void SetUp() override {
        RegisterFunctions::Register();
    }
};

namespace {
using VarcharVector = Vector<LargeStringContainer<std::string_view>>;

// Run bin() over one flat input column of type TypeId. nullRows marks NULL inputs.
// Returns the result vector (caller owns and must delete it).
template <typename CppT, DataTypeId TypeId>
BaseVector *RunBin(const std::vector<CppT> &data, const std::vector<bool> &nullRows) {
    const int32_t rowSize = static_cast<int32_t>(data.size());
    BaseVector *inVec = VectorHelper::CreateFlatVector(TypeId, rowSize);
    auto *typedIn = static_cast<Vector<CppT> *>(inVec);
    for (int32_t i = 0; i < rowSize; ++i) {
        typedIn->SetValue(i, data[i]);
        if (nullRows[i]) {
            typedIn->SetNull(i);
        } else {
            typedIn->SetNotNull(i);
        }
    }

    std::vector<DataTypeId> argTypes = {TypeId};
    auto signature = std::make_shared<FunctionSignature>("bin", argTypes, OMNI_VARCHAR);
    auto vectorFunction = VectorFunction::Find(signature);
    EXPECT_NE(vectorFunction, nullptr);
    if (vectorFunction == nullptr) {
        delete inVec;
        return nullptr;
    }

    ExecutionContext context;
    context.SetResultRowSize(rowSize);
    std::stack<BaseVector *> args;
    args.push(inVec);

    BaseVector *rawResult = nullptr;
    auto resultType = std::make_shared<DataType>(OMNI_VARCHAR);
    vectorFunction->Apply(args, resultType, rawResult, &context);
    return rawResult;
}
}  // namespace

// ----------------------------------------------------------------------------
// bin(int)
// ----------------------------------------------------------------------------

TEST_F(BinFunctionTest, BinIntBasic) {
    std::vector<int32_t> data = {0, 1, 4, 12, 255, 1024};
    std::vector<std::string> expected = {"0", "1", "100", "1100", "11111111", "10000000000"};
    std::vector<bool> nullRows(data.size(), false);

    BaseVector *rawResult = RunBin<int32_t, OMNI_INT>(data, nullRows);
    ASSERT_NE(rawResult, nullptr);
    auto *resultVector = static_cast<VarcharVector *>(rawResult);
    for (size_t i = 0; i < data.size(); ++i) {
        ASSERT_FALSE(resultVector->IsNull(static_cast<int32_t>(i))) << "row " << i;
        EXPECT_EQ(std::string(resultVector->GetValue(static_cast<int32_t>(i))), expected[i]) << "row " << i;
    }
    delete rawResult;
}

// Negative INT is sign-extended to 64 bits (Flink int->long widening).
TEST_F(BinFunctionTest, BinIntNegative) {
    const int32_t intMin = std::numeric_limits<int32_t>::min();  // -2147483648
    std::vector<int32_t> data = {-1, intMin};
    // -1  -> 64 ones
    // INT_MIN -> (long)0xFFFFFFFF80000000 -> 33 ones followed by 31 zeros
    std::vector<std::string> expected = {
        std::string(64, '1'),
        std::string(33, '1') + std::string(31, '0')
    };
    std::vector<bool> nullRows(data.size(), false);

    BaseVector *rawResult = RunBin<int32_t, OMNI_INT>(data, nullRows);
    ASSERT_NE(rawResult, nullptr);
    auto *resultVector = static_cast<VarcharVector *>(rawResult);
    for (size_t i = 0; i < data.size(); ++i) {
        ASSERT_FALSE(resultVector->IsNull(static_cast<int32_t>(i))) << "row " << i;
        EXPECT_EQ(std::string(resultVector->GetValue(static_cast<int32_t>(i))), expected[i]) << "row " << i;
    }
    delete rawResult;
}

TEST_F(BinFunctionTest, BinIntNull) {
    std::vector<int32_t> data = {5, 0, 8};
    std::vector<bool> nullRows = {false, true, false};

    BaseVector *rawResult = RunBin<int32_t, OMNI_INT>(data, nullRows);
    ASSERT_NE(rawResult, nullptr);
    auto *resultVector = static_cast<VarcharVector *>(rawResult);
    EXPECT_FALSE(resultVector->IsNull(0));
    EXPECT_EQ(std::string(resultVector->GetValue(0)), "101");
    EXPECT_TRUE(resultVector->IsNull(1)) << "NULL input -> NULL output";
    EXPECT_FALSE(resultVector->IsNull(2));
    EXPECT_EQ(std::string(resultVector->GetValue(2)), "1000");
    delete rawResult;
}

// ----------------------------------------------------------------------------
// bin(bigint)
// ----------------------------------------------------------------------------

TEST_F(BinFunctionTest, BinBigintBasic) {
    std::vector<int64_t> data = {0LL, 12LL, 1024LL, 4294967296LL};  // last is 2^32
    std::vector<std::string> expected = {
        "0",
        "1100",
        "10000000000",
        std::string("1") + std::string(32, '0')  // 2^32 -> 1 followed by 32 zeros
    };
    std::vector<bool> nullRows(data.size(), false);

    BaseVector *rawResult = RunBin<int64_t, OMNI_LONG>(data, nullRows);
    ASSERT_NE(rawResult, nullptr);
    auto *resultVector = static_cast<VarcharVector *>(rawResult);
    for (size_t i = 0; i < data.size(); ++i) {
        ASSERT_FALSE(resultVector->IsNull(static_cast<int32_t>(i))) << "row " << i;
        EXPECT_EQ(std::string(resultVector->GetValue(static_cast<int32_t>(i))), expected[i]) << "row " << i;
    }
    delete rawResult;
}

TEST_F(BinFunctionTest, BinBigintNegative) {
    const int64_t longMin = std::numeric_limits<int64_t>::min();
    std::vector<int64_t> data = {-1LL, longMin};
    std::vector<std::string> expected = {
        std::string(64, '1'),                       // -1 -> 64 ones
        std::string("1") + std::string(63, '0')     // LONG_MIN -> 1 followed by 63 zeros
    };
    std::vector<bool> nullRows(data.size(), false);

    BaseVector *rawResult = RunBin<int64_t, OMNI_LONG>(data, nullRows);
    ASSERT_NE(rawResult, nullptr);
    auto *resultVector = static_cast<VarcharVector *>(rawResult);
    for (size_t i = 0; i < data.size(); ++i) {
        ASSERT_FALSE(resultVector->IsNull(static_cast<int32_t>(i))) << "row " << i;
        EXPECT_EQ(std::string(resultVector->GetValue(static_cast<int32_t>(i))), expected[i]) << "row " << i;
    }
    delete rawResult;
}
