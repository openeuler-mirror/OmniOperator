/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: If function unit tests
 */

#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <limits>
#include <cmath>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/functions/If.h"
#include "vectorization/registration/SimpleFunctionRegistry.h"
#include "vectorization/VectorFunction.h"
#include "codegen/func_signature.h"
#include "vector/vector_helper.h"
#include "vector/vector.h"
#include "vector/array_vector.h"
#include "vector/map_vector.h"
#include "vector/row_vector.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::mem;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace omniruntime::codegen;
using namespace omniruntime::TestUtil;

class IfTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const if_test_env = ::testing::AddGlobalTestEnvironment(new IfTestEnvironment);

class IfFunctionTestHelper {
public:
    template<typename T>
    static void ValidateNumericResult(BaseVector* result, const std::vector<T>& expected, int rowSize) {
        auto* resultVec = dynamic_cast<Vector<T>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector type mismatch";

        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                continue;
            }
            T actualValue = resultVec->GetValue(i);
            T expectedValue = expected[i];

            if constexpr (std::is_floating_point_v<T>) {
                EXPECT_NEAR(actualValue, expectedValue, 1e-6)
                    << "Row " << i << " value mismatch";
            } else {
                EXPECT_EQ(actualValue, expectedValue)
                    << "Row " << i << " value mismatch";
            }
        }
    }

    static void ValidateStringResult(BaseVector* result, const std::vector<std::string>& expected, int rowSize) {
        auto* resultVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector is not string type";

        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                continue;
            }
            std::string_view actualValue = resultVec->GetValue(i);
            std::string expectedValue = expected[i];
            EXPECT_EQ(actualValue, expectedValue) << "Row " << i << " value mismatch";
        }
    }

    static void ValidateBooleanResult(BaseVector* result, const std::vector<bool>& expected, int rowSize) {
        auto* resultVec = dynamic_cast<Vector<bool>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector is not boolean type";

        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                continue;
            }
            bool actualValue = resultVec->GetValue(i);
            bool expectedValue = expected[i];
            EXPECT_EQ(actualValue, expectedValue) << "Row " << i << " value mismatch";
        }
    }

    template<typename T>
    static BaseVector* CreateNumericVector(const std::vector<T>& values, DataTypeId typeId) {
        BaseVector* vec = VectorHelper::CreateFlatVector(typeId, values.size());
        auto* typedVec = static_cast<Vector<T>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            typedVec->SetValue(i, values[i]);
        }
        return vec;
    }

    static BaseVector* CreateBooleanVector(const std::vector<bool>& values) {
        BaseVector* vec = VectorHelper::CreateFlatVector(OMNI_BOOLEAN, values.size());
        auto* typedVec = static_cast<Vector<bool>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            typedVec->SetValue(i, values[i]);
        }
        return vec;
    }

    static BaseVector* CreateStringVector(const std::vector<std::string>& values, DataTypeId typeId = OMNI_VARCHAR) {
        BaseVector* vec = VectorHelper::CreateFlatVector(typeId, values.size());
        auto* typedVec = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            std::string_view sv(values[i]);
            typedVec->SetValue(i, sv);
        }
        return vec;
    }

    static BaseVector* CreateArrayVector(const std::vector<std::vector<int32_t>>& arrays) {
        int32_t totalElements = 0;
        for (const auto& arr : arrays) {
            totalElements += arr.size();
        }
        auto elemVec = std::shared_ptr<BaseVector>(
            VectorHelper::CreateFlatVector(OMNI_INT, totalElements));
        auto* arrayVec = new ArrayVector(arrays.size(), elemVec);

        for (size_t row = 0; row < arrays.size(); ++row) {
            const auto& arr = arrays[row];
            auto* tempElem = VectorHelper::CreateFlatVector(OMNI_INT, arr.size());
            auto* typedElem = static_cast<Vector<int32_t>*>(tempElem);
            for (size_t i = 0; i < arr.size(); ++i) {
                typedElem->SetValue(i, arr[i]);
            }
            arrayVec->SetValue(row, tempElem);
            delete tempElem;
        }
        return arrayVec;
    }

    static BaseVector* CreateMapVector(const std::vector<std::vector<std::pair<std::string, int32_t>>>& maps) {
        int32_t totalEntries = 0;
        for (const auto& m : maps) {
            totalEntries += m.size();
        }

        auto keyVec = std::shared_ptr<BaseVector>(
            VectorHelper::CreateFlatVector(OMNI_VARCHAR, totalEntries));
        auto valVec = std::shared_ptr<BaseVector>(
            VectorHelper::CreateFlatVector(OMNI_INT, totalEntries));
        auto* mapVec = new MapVector(maps.size(), keyVec, valVec);

        int32_t offset = 0;
        for (size_t row = 0; row < maps.size(); ++row) {
            const auto& m = maps[row];
            int32_t mapSize = m.size();
            for (size_t i = 0; i < m.size(); ++i) {
                auto* typedKeys = static_cast<Vector<LargeStringContainer<std::string_view>>*>(keyVec.get());
                auto* typedVals = static_cast<Vector<int32_t>*>(valVec.get());
                typedKeys->Expand(offset + i + 1);
                typedVals->Expand(offset + i + 1);
                std::string_view keySv(m[i].first);
                typedKeys->SetValue(offset + i, keySv);
                typedVals->SetValue(offset + i, m[i].second);
            }
            mapVec->SetSize(row, mapSize);
            offset += mapSize;
        }
        return mapVec;
    }

    static BaseVector* CreateRowVector(const std::vector<std::pair<std::string, int32_t>>& rows) {
        int32_t size = rows.size();
        auto* rowVec = new RowVector(size);

        auto* strChild = VectorHelper::CreateFlatVector(OMNI_VARCHAR, size);
        auto* intChild = VectorHelper::CreateFlatVector(OMNI_INT, size);
        auto* typedStr = static_cast<Vector<LargeStringContainer<std::string_view>>*>(strChild);
        auto* typedInt = static_cast<Vector<int32_t>*>(intChild);

        for (size_t i = 0; i < rows.size(); ++i) {
            std::string_view sv(rows[i].first);
            typedStr->SetValue(i, sv);
            typedInt->SetValue(i, rows[i].second);
        }

        rowVec->AddChild(strChild);
        rowVec->AddChild(intChild);
        return rowVec;
    }

    static void ExecuteIf(BaseVector* condVec, BaseVector* trueVec, BaseVector* falseVec,
                          DataTypeId outputTypeId, BaseVector*& result) {
        auto signature = std::make_shared<FunctionSignature>("if",
            std::vector<DataTypeId>{OMNI_BOOLEAN, outputTypeId, outputTypeId}, outputTypeId);
        auto function = VectorFunction::Find(signature);
        ASSERT_NE(function, nullptr) << "If function not found for type " << outputTypeId;

        auto outputType = std::make_shared<DataType>(outputTypeId);
        ExecutionContext context;
        context.SetResultRowSize(condVec->GetSize());
        std::stack<BaseVector*> args;

        args.push(condVec);
        args.push(trueVec);
        args.push(falseVec);

        ASSERT_NO_THROW(function->Apply(args, outputType, result, &context))
            << "If function threw an exception";
    }
};

TEST(IfTest, BooleanIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<bool> trueValues = {true, true, false};
    std::vector<bool> falseValues = {false, false, true};
    std::vector<bool> expected = {true, false, false};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateBooleanVector(trueValues);
    BaseVector* falseVec = IfFunctionTestHelper::CreateBooleanVector(falseValues);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_BOOLEAN, resultVec);
    IfFunctionTestHelper::ValidateBooleanResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, BooleanIfNullCondition) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<bool> trueValues = {true, true, false};
    std::vector<bool> falseValues = {false, false, true};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateBooleanVector(trueValues);
    BaseVector* falseVec = IfFunctionTestHelper::CreateBooleanVector(falseValues);

    condVec->SetNull(1);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_BOOLEAN, resultVec);

    EXPECT_FALSE(resultVec->IsNull(0)) << "Row 0 should not be NULL";
    EXPECT_FALSE(resultVec->IsNull(1)) << "Row 1 should not be NULL (NULL cond -> false branch)";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";

    auto* boolResult = dynamic_cast<Vector<bool>*>(resultVec);
    EXPECT_EQ(boolResult->GetValue(0), true);
    EXPECT_EQ(boolResult->GetValue(1), false);  
    EXPECT_EQ(boolResult->GetValue(2), false);

    delete resultVec;
}

TEST(IfTest, ByteIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<int8_t> trueValues = {10, 20, 30};
    std::vector<int8_t> falseValues = {40, 50, 60};
    std::vector<int8_t> expected = {10, 50, 30};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_BYTE);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_BYTE);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_BYTE, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, ShortIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<int16_t> trueValues = {1000, 2000, 3000};
    std::vector<int16_t> falseValues = {4000, 5000, 6000};
    std::vector<int16_t> expected = {1000, 5000, 3000};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_SHORT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_SHORT);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_SHORT, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, IntIfTrue) {

    std::vector<bool> condValues = {true, true, true};
    std::vector<int32_t> trueValues = {100, 200, 300};
    std::vector<int32_t> falseValues = {400, 500, 600};
    std::vector<int32_t> expected = {100, 200, 300};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, IntIfFalse) {

    std::vector<bool> condValues = {false, false, false};
    std::vector<int32_t> trueValues = {100, 200, 300};
    std::vector<int32_t> falseValues = {400, 500, 600};
    std::vector<int32_t> expected = {400, 500, 600};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, IntIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<int32_t> trueValues = {100, 200, 300};
    std::vector<int32_t> falseValues = {400, 500, 600};
    std::vector<int32_t> expected = {100, 500, 300};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, IntIfNullCondition) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<int32_t> trueValues = {100, 200, 300};
    std::vector<int32_t> falseValues = {400, 500, 600};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);

    condVec->SetNull(1);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);

    EXPECT_FALSE(resultVec->IsNull(0));
    EXPECT_FALSE(resultVec->IsNull(1));  
    EXPECT_FALSE(resultVec->IsNull(2));

    auto* intResult = dynamic_cast<Vector<int32_t>*>(resultVec);
    EXPECT_EQ(intResult->GetValue(0), 100);
    EXPECT_EQ(intResult->GetValue(1), 500);  
    EXPECT_EQ(intResult->GetValue(2), 300);

    delete resultVec;
}

TEST(IfTest, IntIfNullTrueValue) {

    std::vector<bool> condValues = {true, true, false};
    std::vector<int32_t> trueValues = {100, 200, 300};
    std::vector<int32_t> falseValues = {400, 500, 600};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);

    trueVec->SetNull(0);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);

    EXPECT_TRUE(resultVec->IsNull(0)) << "Row 0 should be NULL (true value is NULL)";
    EXPECT_FALSE(resultVec->IsNull(1)) << "Row 1 should not be NULL";
    EXPECT_FALSE(resultVec->IsNull(2)) << "Row 2 should not be NULL";

    delete resultVec;
}

TEST(IfTest, LongIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<int64_t> trueValues = {100LL, 200LL, 300LL};
    std::vector<int64_t> falseValues = {400LL, 500LL, 600LL};
    std::vector<int64_t> expected = {100LL, 500LL, 300LL};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_LONG);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_LONG);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_LONG, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, FloatIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<float> trueValues = {100.5f, 200.7f, 300.9f};
    std::vector<float> falseValues = {400.1f, 500.3f, 600.5f};
    std::vector<float> expected = {100.5f, 500.3f, 300.9f};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_FLOAT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_FLOAT);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_FLOAT, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, DoubleIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<double> trueValues = {100.5, 200.7, 300.9};
    std::vector<double> falseValues = {400.1, 500.3, 600.5};
    std::vector<double> expected = {100.5, 500.3, 300.9};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_DOUBLE);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_DOUBLE);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_DOUBLE, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, StringIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<std::string> trueValues = {"hello", "world", "test"};
    std::vector<std::string> falseValues = {"foo", "bar", "baz"};
    std::vector<std::string> expected = {"hello", "bar", "test"};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateStringVector(trueValues);
    BaseVector* falseVec = IfFunctionTestHelper::CreateStringVector(falseValues);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_VARCHAR, resultVec);
    IfFunctionTestHelper::ValidateStringResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, StringIfNullCondition) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<std::string> trueValues = {"hello", "world", "test"};
    std::vector<std::string> falseValues = {"foo", "bar", "baz"};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateStringVector(trueValues);
    BaseVector* falseVec = IfFunctionTestHelper::CreateStringVector(falseValues);

    condVec->SetNull(1);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_VARCHAR, resultVec);

    EXPECT_FALSE(resultVec->IsNull(0));
    EXPECT_FALSE(resultVec->IsNull(1));  
    EXPECT_FALSE(resultVec->IsNull(2));

    delete resultVec;
}

TEST(IfTest, TimestampIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<int64_t> trueValues = {1706522730000000LL, 1706609130000000LL, 1706695530000000LL};
    std::vector<int64_t> falseValues = {0LL, 86400000000LL, 172800000000LL};
    std::vector<int64_t> expected = {1706522730000000LL, 86400000000LL, 1706695530000000LL};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_TIMESTAMP);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_TIMESTAMP);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_TIMESTAMP, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, TimestampIfNullCondition) {

    std::vector<bool> condValues = {true, false, false};
    std::vector<int64_t> trueValues = {1000000LL, 2000000LL, 3000000LL};
    std::vector<int64_t> falseValues = {4000000LL, 5000000LL, 6000000LL};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_TIMESTAMP);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_TIMESTAMP);

    condVec->SetNull(0);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_TIMESTAMP, resultVec);

    EXPECT_FALSE(resultVec->IsNull(0));
    auto* tsResult = dynamic_cast<Vector<int64_t>*>(resultVec);
    EXPECT_EQ(tsResult->GetValue(0), 4000000LL);

    delete resultVec;
}

TEST(IfTest, BinaryIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<std::string> trueValues = {"binary_a", "binary_b", "binary_c"};
    std::vector<std::string> falseValues = {"bin_x", "bin_y", "bin_z"};
    std::vector<std::string> expected = {"binary_a", "bin_y", "binary_c"};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateStringVector(trueValues, OMNI_VARBINARY);
    BaseVector* falseVec = IfFunctionTestHelper::CreateStringVector(falseValues, OMNI_VARBINARY);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_VARBINARY, resultVec);
    IfFunctionTestHelper::ValidateStringResult(resultVec, expected, condValues.size());

    delete resultVec;
}

TEST(IfTest, BinaryIfNullCondition) {

    std::vector<bool> condValues = {true, true, false};
    std::vector<std::string> trueValues = {"aaa", "bbb", "ccc"};
    std::vector<std::string> falseValues = {"xxx", "yyy", "zzz"};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateStringVector(trueValues, OMNI_VARBINARY);
    BaseVector* falseVec = IfFunctionTestHelper::CreateStringVector(falseValues, OMNI_VARBINARY);

    condVec->SetNull(2);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_VARBINARY, resultVec);

    EXPECT_FALSE(resultVec->IsNull(0));
    EXPECT_FALSE(resultVec->IsNull(1));
    EXPECT_FALSE(resultVec->IsNull(2));  

    auto* binResult = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(resultVec);
    EXPECT_EQ(binResult->GetValue(0), "aaa");
    EXPECT_EQ(binResult->GetValue(1), "bbb");
    EXPECT_EQ(binResult->GetValue(2), "zzz");  

    delete resultVec;
}

TEST(IfTest, BinaryIfNullValue) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<std::string> trueValues = {"data1", "data2", "data3"};
    std::vector<std::string> falseValues = {"alt1", "alt2", "alt3"};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateStringVector(trueValues, OMNI_VARBINARY);
    BaseVector* falseVec = IfFunctionTestHelper::CreateStringVector(falseValues, OMNI_VARBINARY);

    trueVec->SetNull(0);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_VARBINARY, resultVec);

    EXPECT_TRUE(resultVec->IsNull(0)) << "Row 0 should be NULL (true value is NULL and cond is true)";
    EXPECT_FALSE(resultVec->IsNull(1));
    EXPECT_FALSE(resultVec->IsNull(2));

    delete resultVec;
}

TEST(IfTest, ArrayIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<std::vector<int32_t>> trueArrays = {{1, 2, 3}, {4, 5}, {6}};
    std::vector<std::vector<int32_t>> falseArrays = {{10, 20}, {30, 40, 50}, {60, 70}};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateArrayVector(trueArrays);
    BaseVector* falseVec = IfFunctionTestHelper::CreateArrayVector(falseArrays);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_ARRAY, resultVec);

    ASSERT_NE(resultVec, nullptr);
    auto* resultArray = dynamic_cast<ArrayVector*>(resultVec);
    ASSERT_NE(resultArray, nullptr);

    EXPECT_EQ(resultArray->GetSize(static_cast<int64_t>(0)), 3);
    EXPECT_EQ(resultArray->GetSize(static_cast<int64_t>(1)), 3);
    EXPECT_EQ(resultArray->GetSize(static_cast<int64_t>(2)), 1);

    BaseVector* row0Slice = resultArray->GetValue(0);
    auto* row0Vec = static_cast<Vector<int32_t>*>(row0Slice);
    EXPECT_EQ(row0Vec->GetValue(0), 1);
    EXPECT_EQ(row0Vec->GetValue(1), 2);
    EXPECT_EQ(row0Vec->GetValue(2), 3);
    delete row0Slice;

    BaseVector* row1Slice = resultArray->GetValue(1);
    auto* row1Vec = static_cast<Vector<int32_t>*>(row1Slice);
    EXPECT_EQ(row1Vec->GetValue(0), 30);
    EXPECT_EQ(row1Vec->GetValue(1), 40);
    EXPECT_EQ(row1Vec->GetValue(2), 50);
    delete row1Slice;

    delete resultVec;
}

TEST(IfTest, ArrayIfNullCondition) {

    std::vector<bool> condValues = {true, true, false};
    std::vector<std::vector<int32_t>> trueArrays = {{1, 2}, {3, 4}, {5, 6}};
    std::vector<std::vector<int32_t>> falseArrays = {{10, 20}, {30, 40}, {50, 60}};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateArrayVector(trueArrays);
    BaseVector* falseVec = IfFunctionTestHelper::CreateArrayVector(falseArrays);

    condVec->SetNull(0);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_ARRAY, resultVec);

    ASSERT_NE(resultVec, nullptr);
    auto* resultArray = dynamic_cast<ArrayVector*>(resultVec);

    EXPECT_EQ(resultArray->GetSize(static_cast<int64_t>(0)), 2);
    BaseVector* row0Slice = resultArray->GetValue(0);
    auto* row0Vec = static_cast<Vector<int32_t>*>(row0Slice);
    EXPECT_EQ(row0Vec->GetValue(0), 10);
    EXPECT_EQ(row0Vec->GetValue(1), 20);
    delete row0Slice;

    delete resultVec;
}

TEST(IfTest, ArrayIfNullValue) {

    std::vector<bool> condValues = {true, false};
    std::vector<std::vector<int32_t>> trueArrays = {{1, 2}, {3, 4}};
    std::vector<std::vector<int32_t>> falseArrays = {{10, 20}, {30, 40}};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateArrayVector(trueArrays);
    BaseVector* falseVec = IfFunctionTestHelper::CreateArrayVector(falseArrays);

    trueVec->SetNull(0);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_ARRAY, resultVec);

    ASSERT_NE(resultVec, nullptr);
    EXPECT_TRUE(resultVec->IsNull(0)) << "Row 0 should be NULL (true array is NULL and cond is true)";
    EXPECT_FALSE(resultVec->IsNull(1));

    delete resultVec;
}


TEST(IfTest, MapIfMixed) {

    std::vector<bool> condValues = {true, false};
    std::vector<std::vector<std::pair<std::string, int32_t>>> trueMaps = {
        {{"k1", 1}, {"k2", 2}},
        {{"k3", 3}}
    };
    std::vector<std::vector<std::pair<std::string, int32_t>>> falseMaps = {
        {{"a1", 10}},
        {{"a2", 20}, {"a3", 30}}
    };

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateMapVector(trueMaps);
    BaseVector* falseVec = IfFunctionTestHelper::CreateMapVector(falseMaps);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_MAP, resultVec);

    ASSERT_NE(resultVec, nullptr);
    auto* resultMap = dynamic_cast<MapVector*>(resultVec);
    ASSERT_NE(resultMap, nullptr);

    EXPECT_EQ(resultMap->GetSize(static_cast<int64_t>(0)), 2);
    EXPECT_EQ(resultMap->GetSize(static_cast<int64_t>(1)), 2);

    delete resultVec;
}

TEST(IfTest, MapIfNullCondition) {

    std::vector<bool> condValues = {true, false};
    std::vector<std::vector<std::pair<std::string, int32_t>>> trueMaps = {
        {{"k1", 1}},
        {{"k2", 2}}
    };
    std::vector<std::vector<std::pair<std::string, int32_t>>> falseMaps = {
        {{"a1", 10}, {"a2", 20}},
        {{"a3", 30}}
    };

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateMapVector(trueMaps);
    BaseVector* falseVec = IfFunctionTestHelper::CreateMapVector(falseMaps);

    condVec->SetNull(0);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_MAP, resultVec);

    ASSERT_NE(resultVec, nullptr);
    auto* resultMap = dynamic_cast<MapVector*>(resultVec);

    EXPECT_EQ(resultMap->GetSize(static_cast<int64_t>(0)), 2);

    delete resultVec;
}

TEST(IfTest, RowIfMixed) {

    std::vector<bool> condValues = {true, false, true};
    std::vector<std::pair<std::string, int32_t>> trueRows = {
        {"alice", 25}, {"bob", 30}, {"charlie", 35}
    };
    std::vector<std::pair<std::string, int32_t>> falseRows = {
        {"dave", 40}, {"eve", 45}, {"frank", 50}
    };

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateRowVector(trueRows);
    BaseVector* falseVec = IfFunctionTestHelper::CreateRowVector(falseRows);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_ROW, resultVec);

    ASSERT_NE(resultVec, nullptr);
    auto* resultRow = dynamic_cast<RowVector*>(resultVec);
    ASSERT_NE(resultRow, nullptr);
    ASSERT_EQ(resultRow->ChildSize(), 2);

    auto* strChild = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(resultRow->ChildAt(0).get());
    ASSERT_NE(strChild, nullptr);
    EXPECT_EQ(strChild->GetValue(0), "alice");   
    EXPECT_EQ(strChild->GetValue(1), "eve");      
    EXPECT_EQ(strChild->GetValue(2), "charlie"); 

    auto* intChild = dynamic_cast<Vector<int32_t>*>(resultRow->ChildAt(1).get());
    ASSERT_NE(intChild, nullptr);
    EXPECT_EQ(intChild->GetValue(0), 25);   
    EXPECT_EQ(intChild->GetValue(1), 45);   
    EXPECT_EQ(intChild->GetValue(2), 35);   

    delete resultVec;
}

TEST(IfTest, RowIfNullCondition) {

    std::vector<bool> condValues = {true, false};
    std::vector<std::pair<std::string, int32_t>> trueRows = {
        {"alice", 25}, {"bob", 30}
    };
    std::vector<std::pair<std::string, int32_t>> falseRows = {
        {"dave", 40}, {"eve", 45}
    };

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateRowVector(trueRows);
    BaseVector* falseVec = IfFunctionTestHelper::CreateRowVector(falseRows);

    condVec->SetNull(0);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_ROW, resultVec);

    ASSERT_NE(resultVec, nullptr);
    auto* resultRow = dynamic_cast<RowVector*>(resultVec);
    auto* strChild = dynamic_cast<Vector<LargeStringContainer<std::string_view>>*>(resultRow->ChildAt(0).get());
    auto* intChild = dynamic_cast<Vector<int32_t>*>(resultRow->ChildAt(1).get());
    EXPECT_EQ(strChild->GetValue(0), "dave");
    EXPECT_EQ(intChild->GetValue(0), 40);

    delete resultVec;
}

TEST(IfTest, RowIfNullValue) {

    std::vector<bool> condValues = {true, false};
    std::vector<std::pair<std::string, int32_t>> trueRows = {
        {"alice", 25}, {"bob", 30}
    };
    std::vector<std::pair<std::string, int32_t>> falseRows = {
        {"dave", 40}, {"eve", 45}
    };

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateRowVector(trueRows);
    BaseVector* falseVec = IfFunctionTestHelper::CreateRowVector(falseRows);

    trueVec->SetNull(0);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_ROW, resultVec);

    ASSERT_NE(resultVec, nullptr);
    EXPECT_TRUE(resultVec->IsNull(0)) << "Row 0 should be NULL (true row is NULL)";
    EXPECT_FALSE(resultVec->IsNull(1));

    delete resultVec;
}

TEST(IfTest, AllNullConditions) {

    std::vector<bool> condValues = {false, false, false};
    std::vector<int32_t> trueValues = {100, 200, 300};
    std::vector<int32_t> falseValues = {400, 500, 600};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);

    condVec->SetNull(0);
    condVec->SetNull(1);
    condVec->SetNull(2);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);

    auto* intResult = dynamic_cast<Vector<int32_t>*>(resultVec);
    for (int i = 0; i < 3; ++i) {
        EXPECT_FALSE(resultVec->IsNull(i));
        EXPECT_EQ(intResult->GetValue(i), falseValues[i]);
    }

    delete resultVec;
}

TEST(IfTest, AllNullValues) {

    std::vector<bool> condValues = {true, false};
    std::vector<int32_t> trueValues = {100, 200};
    std::vector<int32_t> falseValues = {300, 400};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);

    trueVec->SetNull(0);
    trueVec->SetNull(1);
    falseVec->SetNull(0);
    falseVec->SetNull(1);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);

    EXPECT_TRUE(resultVec->IsNull(0)) << "Row 0 should be NULL";
    EXPECT_TRUE(resultVec->IsNull(1)) << "Row 1 should be NULL";

    delete resultVec;
}

TEST(IfTest, SingleRow) {

    std::vector<bool> condValues = {true};
    std::vector<int32_t> trueValues = {42};
    std::vector<int32_t> falseValues = {99};
    std::vector<int32_t> expected = {42};

    BaseVector* condVec = IfFunctionTestHelper::CreateBooleanVector(condValues);
    BaseVector* trueVec = IfFunctionTestHelper::CreateNumericVector(trueValues, OMNI_INT);
    BaseVector* falseVec = IfFunctionTestHelper::CreateNumericVector(falseValues, OMNI_INT);

    BaseVector* resultVec = nullptr;
    IfFunctionTestHelper::ExecuteIf(condVec, trueVec, falseVec, OMNI_INT, resultVec);
    IfFunctionTestHelper::ValidateNumericResult(resultVec, expected, condValues.size());

    delete resultVec;
}
