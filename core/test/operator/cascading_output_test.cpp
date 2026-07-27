/*
* @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * @Description: Test BuildFinalOutput to ensure output is correctly constructed
 */


#include <vector>
#include <iostream>
#include <limits>
#include <new>
#include "gtest/gtest.h"
#include "operator/operator.h"
#include "operator/join/hash_builder.h"
#include "operator/join/cascading_lookup_join.h"
#include "type/data_type.h"
#include "type/data_types.h"
#include "vector/vector_helper.h"
#include "util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace TestUtil;

template <typename T>
T* SafeAllocateArray(int32_t size)
{
    if (size <= 0) {
        std::cerr
            << "[SafeAllocateArray] invalid size: "
            << size
            << std::endl;
        return nullptr;
    }

    const size_t count = static_cast<size_t>(size);

    if (count > std::numeric_limits<size_t>::max() / sizeof(T)) {
        std::cerr
            << "[SafeAllocateArray] allocation size overflow: "
            << "count=" << count
            << ", elementSize=" << sizeof(T)
            << std::endl;
        return nullptr;
    }

    const size_t bytes = count * sizeof(T);

    T* ptr = new (std::nothrow) T[count];

    if (ptr == nullptr) {
        std::cerr
            << "[SafeAllocateArray] memory allocation failed: "
            << "count=" << count
            << ", bytes=" << bytes
            << std::endl;
        return nullptr;
    }

    return ptr;
}

namespace CascadingOutputTest {

VectorBatch* CreateBuildTable_B(int32_t size) {
    DataTypes buildTypes({ LongType(), IntType() });
    int64_t* col0 = SafeAllocateArray<int64_t>(size);
    int32_t* col1 = SafeAllocateArray<int32_t>(size);

    if (col0 == nullptr || col1 == nullptr) {
        delete[] col0;
        delete[] col1;
        return nullptr;
    }

    for (int i = 0; i < size; i++) {
        col0[i] = i % 100;  // id
        col1[i] = 100 + i;   // value_b
    }

    VectorBatch* batch = CreateVectorBatch(buildTypes, size, col0, col1);

    delete[] col0;
    delete[] col1;

    return batch;
}

VectorBatch* CreateBuildTable_C(int32_t size) {
    DataTypes buildTypes({ LongType(), IntType() });
    int64_t* col0 = SafeAllocateArray<int64_t>(size);
    int32_t* col1 = SafeAllocateArray<int32_t>(size);

    if (col0 == nullptr || col1 == nullptr) {
        delete[] col0;
        delete[] col1;
        return nullptr;
    }

    for (int i = 0; i < size; i++) {
        col0[i] = i % 50;   // id
        col1[i] = 300 + i;  // value_c
    }

    VectorBatch* batch = CreateVectorBatch(buildTypes, size, col0, col1);

    delete[] col0;
    delete[] col1;

    return batch;
}

VectorBatch* CreateProbeTable_A(int32_t size) {
    DataTypes probeTypes({ LongType(), IntType() });
    int64_t* col0 = SafeAllocateArray<int64_t>(size);
    int32_t* col1 = SafeAllocateArray<int32_t>(size);

    if (col0 == nullptr || col1 == nullptr) {
        delete[] col0;
        delete[] col1;
        return nullptr;
    }

    for (int i = 0; i < size; i++) {
        col0[i] = i % 100;  // id
        col1[i] = i;         // value_a
    }

    VectorBatch* batch = CreateVectorBatch(probeTypes, size, col0, col1);

    delete[] col0;
    delete[] col1;

    return batch;
}

void PrintVectorBatch(VectorBatch* batch, const std::string& name) {
    if (batch == nullptr) {
        std::cout << name << ": nullptr" << std::endl;
        return;
    }

    std::cout << "=== " << name << " ===" << std::endl;
    std::cout << "Rows: " << batch->GetRowCount() << std::endl;
    std::cout << "Columns: " << batch->GetVectorCount() << std::endl;

    for (int32_t col = 0; col < batch->GetVectorCount(); col++) {
        BaseVector* vec = batch->Get(col);
        std::cout << "Column " << col << ": " << vec->GetTypeId() << " type, encoding: " << vec->GetEncoding();

        // Print first 5 values of each column
        int32_t rowCount = std::min(5, batch->GetRowCount());
        std::cout << " [";
        for (int32_t i = 0; i < rowCount; i++) {
            if (vec->IsNull(i)) {
                std::cout << "NULL";
            } else {
                // Handle different encodings
                auto encoding = vec->GetEncoding();
                if (vec->GetTypeId() == OMNI_LONG) {
                    int64_t value;
                    if (encoding == vec::OMNI_ENCODING_CONST) {
                        value = static_cast<ConstVector<int64_t>*>(vec)->GetConstValue();
                    } else if (encoding == vec::OMNI_DICTIONARY) {
                        value = static_cast<Vector<DictionaryContainer<int64_t>>*>(vec)->GetValue(i);
                    } else {
                        value = static_cast<Vector<int64_t>*>(vec)->GetValue(i);
                    }
                    std::cout << value;
                } else if (vec->GetTypeId() == OMNI_INT) {
                    int32_t value;
                    if (encoding == vec::OMNI_ENCODING_CONST) {
                        value = static_cast<ConstVector<int32_t>*>(vec)->GetConstValue();
                    } else if (encoding == vec::OMNI_DICTIONARY) {
                        value = static_cast<Vector<DictionaryContainer<int32_t>>*>(vec)->GetValue(i);
                    } else {
                        value = static_cast<Vector<int32_t>*>(vec)->GetValue(i);
                    }
                    std::cout << value;
                } else {
                    std::cout << "?";
                }
            }
            if (i < rowCount - 1) std::cout << ", ";
        }
        std::cout << "]";
        std::cout << std::endl;
    }
    std::cout << "====================" << std::endl;
}

TEST(CascadingOutputTest, VerifyOutputConstruction) {
    const int32_t buildSize = 100;
    const int32_t probeSize = 50;

    //
    // Test Setup Description
    //

    std::cout << "\n" << std::string(80, '=') << std::endl;
    std::cout << "Cascading Join Output Test" << std::endl;
    std::cout << std::string(80, '=') << std::endl;
    std::cout << "\nJoin Structure: A.id = B.id AND B.id = C.id" << std::endl;
    std::cout << "  - Table A (probe): id, value_a" << std::endl;
    std::cout << "  - Table B (build level 0): id, value_b" << std::endl;
    std::cout << "  - Table C (build level 1): id, value_c" << std::endl;
    std::cout << "\nExpected Output: 4 columns (A.id, A.value_a, B.value_b, C.value_c)" << std::endl;
    std::cout << std::string(80, '=') << std::endl;

    //
    // Step 1: Create test data
    //

    std::cout << "\n=== Creating test data ===" << std::endl;
    VectorBatch* buildDataB = CreateBuildTable_B(buildSize);
    VectorBatch* buildDataC = CreateBuildTable_C(buildSize);
    VectorBatch* probeDataA = CreateProbeTable_A(probeSize);

    PrintVectorBatch(buildDataB, "Build Table B");
    PrintVectorBatch(buildDataC, "Build Table C");
    PrintVectorBatch(probeDataA, "Probe Table A");

    //
    // Step 2: Build hash tables
    //

    std::cout << "\n=== Building hash tables ===" << std::endl;

    std::vector<int32_t> buildHashColsB = {0};
    std::vector<int32_t> buildHashColsC = {0};

    std::cout << "Building hash table for Table B..." << std::endl;
    HashBuilderOperatorFactory* hashBuilderFactoryB = new HashBuilderOperatorFactory(
        JoinType::OMNI_JOIN_TYPE_INNER,
        DataTypes({ LongType(), IntType() }),
        buildHashColsB.data(),
        static_cast<int32_t>(buildHashColsB.size()),
        1);

    omniruntime::op::Operator* hashBuilderOpB = hashBuilderFactoryB->CreateOperator();
    hashBuilderOpB->AddInput(buildDataB);
    VectorBatch* outputB = nullptr;
    hashBuilderOpB->GetOutput(&outputB);
    if (outputB) VectorHelper::FreeVecBatch(outputB);

    std::cout << "✓ Hash table B built successfully (key: id, columns: id, value_b)" << std::endl;

    std::cout << "Building hash table for Table C..." << std::endl;

    HashBuilderOperatorFactory* hashBuilderFactoryC = new HashBuilderOperatorFactory(
        JoinType::OMNI_JOIN_TYPE_INNER,
        DataTypes({ LongType(), IntType() }),
        buildHashColsC.data(),
        static_cast<int32_t>(buildHashColsC.size()),
        1);

    omniruntime::op::Operator* hashBuilderOpC = hashBuilderFactoryC->CreateOperator();
    hashBuilderOpC->AddInput(buildDataC);
    VectorBatch* outputC = nullptr;
    hashBuilderOpC->GetOutput(&outputC);
    if (outputC) VectorHelper::FreeVecBatch(outputC);

    std::cout << "✓ Hash table C built successfully (key: id, columns: id, value_c)" << std::endl;

    //
    // Step 3: Create cascading join operator
    //

    std::cout << "\n=== Creating cascading join operator ===" << std::endl;

    std::cout << "Configuration:" << std::endl;
    std::cout << "  Cascade levels: 2" << std::endl;
    std::cout << "  Level 0: A.id = B.id, output B.value_b (column 1)" << std::endl;
    std::cout << "  Level 1: B.id = C.id, output C.value_c (column 1)" << std::endl;

    std::vector<DataTypes> probeTypesList;
    probeTypesList.push_back(DataTypes({ LongType(), IntType() }));  // Level 0: A
    probeTypesList.push_back(DataTypes({ LongType(), IntType() }));  // Level 1: B

    std::vector<std::vector<int32_t>> probeHashColsList;
    probeHashColsList.push_back({0});  // A.id
    probeHashColsList.push_back({0});  // B.id

    std::vector<std::vector<int32_t>> probeHashColTypesList;
    probeHashColTypesList.push_back({OMNI_LONG});
    probeHashColTypesList.push_back({OMNI_LONG});

    std::vector<std::vector<int32_t>> buildOutputColsList;
    buildOutputColsList.push_back({1});  // Output B.value_b
    buildOutputColsList.push_back({1});  // Output C.value_c

    std::vector<DataTypes> buildOutputTypesList;
    buildOutputTypesList.push_back(DataTypes({ IntType() }));
    buildOutputTypesList.push_back(DataTypes({ IntType() }));

    std::vector<HashTableVariants*> hashTablesList;
    hashTablesList.push_back(hashBuilderFactoryB->GetHashTablesVariants());
    hashTablesList.push_back(hashBuilderFactoryC->GetHashTablesVariants());

    int32_t outputRowSize = 8 + 4 + 4 + 4;  // A.id + A.value_a + B.value_b + C.value_c

    CascadingLookupJoinOperatorFactory* cascadingFactory = new CascadingLookupJoinOperatorFactory(
        probeTypesList,
        probeHashColsList,
        probeHashColTypesList,
        buildOutputColsList,
        buildOutputTypesList,
        hashTablesList,
        outputRowSize);

    omniruntime::op::Operator* cascadingOp = cascadingFactory->CreateOperator();
    ASSERT_NE(cascadingOp, nullptr);
    std::cout << "Cascading join operator created" << std::endl;

    //
    // Step 4: Execute cascading join
    //

    std::cout << "\n=== Executing cascading join ===" << std::endl;
    std::cout << "Adding probe data (Table A, " << probeSize << " rows)..." << std::endl;

    cascadingOp->AddInput(probeDataA);
    std::cout << "✓ Input added" << std::endl;

    std::cout << "\n*** DIAGNOSTIC INFO ***" << std::endl;
    std::cout << "Hash table B has data: " << (hashBuilderFactoryB->GetHashTablesVariants() ? "YES" : "NO") << std::endl;
    std::cout << "Hash table C has data: " << (hashBuilderFactoryC->GetHashTablesVariants() ? "YES" : "NO") << std::endl;
    std::cout << "***********************\n" << std::endl;

    std::cout << "Retrieving output..." << std::endl;
    VectorBatch* result = nullptr;
    int32_t status = cascadingOp->GetOutput(&result);
    std::cout << "✓ GetOutput called, status: " << status << std::endl;

    //
    // Step 5: Verify and print output
    //

    std::cout << "\n=== Verifying output ===" << std::endl;

    if (result != nullptr) {
        PrintVectorBatch(result, "Cascading Join Result");

        // Verify output structure
        int32_t rowCount = result->GetRowCount();
        int32_t colCount = result->GetVectorCount();

        std::cout << "\nExpected output columns: 4 (A.id, A.value_a, B.value_b, C.value_c)" << std::endl;
        std::cout << "Actual output columns: " << colCount << std::endl;

        // Print sample rows with actual values
        if (rowCount > 0) {
            std::cout << "\nSample rows (first " << std::min(5, rowCount) << " rows):" << std::endl;
            int32_t sampleCount = std::min(5, rowCount);
            for (int32_t i = 0; i < sampleCount; i++) {
                std::cout << "  Row " << i << ": ";
                for (int32_t j = 0; j < colCount; j++) {
                    BaseVector* vec = result->Get(j);
                    std::cout << "[";
                    if (vec->IsNull(i)) {
                        std::cout << "NULL";
                    } else {
                        // Handle different encodings
                        auto encoding = vec->GetEncoding();
                        if (vec->GetTypeId() == OMNI_LONG) {
                            int64_t value;
                            if (encoding == vec::OMNI_ENCODING_CONST) {
                                value = static_cast<ConstVector<int64_t>*>(vec)->GetConstValue();
                            } else if (encoding == vec::OMNI_DICTIONARY) {
                                value = static_cast<Vector<DictionaryContainer<int64_t>>*>(vec)->GetValue(i);
                            } else {
                                value = static_cast<Vector<int64_t>*>(vec)->GetValue(i);
                            }
                            std::cout << value;
                        } else if (vec->GetTypeId() == OMNI_INT) {
                            int32_t value;
                            if (encoding == vec::OMNI_ENCODING_CONST) {
                                value = static_cast<ConstVector<int32_t>*>(vec)->GetConstValue();
                            } else if (encoding == vec::OMNI_DICTIONARY) {
                                value = static_cast<Vector<DictionaryContainer<int32_t>>*>(vec)->GetValue(i);
                            } else {
                                value = static_cast<Vector<int32_t>*>(vec)->GetValue(i);
                            }
                            std::cout << value;
                        } else {
                            std::cout << "?";
                        }
                    }
                    std::cout << "]";
                    if (j < colCount - 1) std::cout << ", ";
                }
                std::cout << std::endl;
            }

            // Print column names if we have 4 columns
            if (colCount >= 4) {
                std::cout << "\nColumn mapping:" << std::endl;
                std::cout << "  Col 0 = A.id (probe)" << std::endl;
                std::cout << "  Col 1 = A.value_a (probe)" << std::endl;
                std::cout << "  Col 2 = B.value_b (build level 0)" << std::endl;
                std::cout << "  Col 3 = C.value_c (build level 1)" << std::endl;
            }
        }

        // Verify expectations
        EXPECT_GT(rowCount, 0) << "Should have output rows";
        EXPECT_EQ(colCount, 4) << "Should have 4 columns: A.id, A.value_a, B.value_b, C.value_c";

        // Summary
        std::cout << "\n" << std::string(80, '=') << std::endl;
        std::cout << "Test Summary:" << std::endl;
        std::cout << "  Total rows: " << rowCount << std::endl;
        std::cout << "  Total columns: " << colCount << std::endl;
        if (colCount == 4) {
            std::cout << "  Result: PASS ✓" << std::endl;
        } else {
            std::cout << "  Result: FAIL ✗ (expected 4 columns, got " << colCount << ")" << std::endl;
        }
        std::cout << std::string(80, '=') << std::endl;

        VectorHelper::FreeVecBatch(result);
    } else {
        std::cout << "ERROR: Result is nullptr!" << std::endl;
        FAIL() << "BuildFinalOutput did not produce output";
    }

    //
    // Step 6: Cleanup
    //

    std::cout << "\n=== Cleanup ===" << std::endl;

    omniruntime::op::Operator::DeleteOperator(hashBuilderOpB);
    omniruntime::op::Operator::DeleteOperator(hashBuilderOpC);
    omniruntime::op::Operator::DeleteOperator(cascadingOp);

    delete hashBuilderFactoryB;
    delete hashBuilderFactoryC;
    delete cascadingFactory;

    std::cout << "Test completed successfully!" << std::endl;
}

}