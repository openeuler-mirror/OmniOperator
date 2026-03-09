/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * @Description: Lead and Lag window function unit tests
 */
#include <vector>
#include <iostream>
#include <chrono>

#include "gtest/gtest.h"
#include "operator/window/window.h"
#include "util/test_util.h"
#include "vector/vector_helper.h"
#include "vector/unsafe_vector.h"

using namespace std;
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace TestUtil;

namespace LeadLagTest {

const int32_t DATA_SIZE = 6;

/**
 * Test LAG function with INT type
 * LAG returns the value at offset rows before the current row
 * Input:  [0, 1, 2, 3, 4, 5] with offset=1
 * Output: [NULL, 0, 1, 2, 3, 4]
 */
TEST(LeadLagWindowTest, testLagIntType)
{
    std::cout << "[LeadLagWindowTest::testLagIntType] Starting test..." << std::endl;
    
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({IntType(), LongType()}));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    int64_t data1[DATA_SIZE] = {10, 20, 30, 40, 50, 60};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LAG};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    // Output type includes source columns plus LAG result (same type as input column 0)
    DataTypes allTypes(std::vector<DataTypePtr>({IntType(), LongType(), IntType()}));
    int32_t argumentChannels[1] = {0};  // LAG operates on column 0

    // create operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // Verify output
    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLagIntType] Output VectorBatch received" << std::endl;
    
    // Print output for debugging
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLagIntType] Test completed successfully" << std::endl;
}

/**
 * Test LEAD function with INT type
 * LEAD returns the value at offset rows after the current row
 * Input:  [0, 1, 2, 3, 4, 5] with offset=1
 * Output: [1, 2, 3, 4, 5, NULL]
 */
TEST(LeadLagWindowTest, testLeadIntType)
{
    std::cout << "[LeadLagWindowTest::testLeadIntType] Starting test..." << std::endl;
    
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({IntType(), LongType()}));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    int64_t data1[DATA_SIZE] = {10, 20, 30, 40, 50, 60};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LEAD};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({IntType(), LongType(), IntType()}));
    int32_t argumentChannels[1] = {0};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLeadIntType] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLeadIntType] Test completed successfully" << std::endl;
}

/**
 * Test LAG function with LONG type
 */
TEST(LeadLagWindowTest, testLagLongType)
{
    std::cout << "[LeadLagWindowTest::testLagLongType] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({LongType(), IntType()}));
    int64_t data0[DATA_SIZE] = {100, 200, 300, 400, 500, 600};
    int32_t data1[DATA_SIZE] = {1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LAG};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({LongType(), IntType(), LongType()}));
    int32_t argumentChannels[1] = {0};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLagLongType] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLagLongType] Test completed successfully" << std::endl;
}

/**
 * Test LEAD function with LONG type
 */
TEST(LeadLagWindowTest, testLeadLongType)
{
    std::cout << "[LeadLagWindowTest::testLeadLongType] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({LongType(), IntType()}));
    int64_t data0[DATA_SIZE] = {100, 200, 300, 400, 500, 600};
    int32_t data1[DATA_SIZE] = {1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LEAD};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({LongType(), IntType(), LongType()}));
    int32_t argumentChannels[1] = {0};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLeadLongType] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLeadLongType] Test completed successfully" << std::endl;
}

/**
 * Test LAG function with DOUBLE type
 */
TEST(LeadLagWindowTest, testLagDoubleType)
{
    std::cout << "[LeadLagWindowTest::testLagDoubleType] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({DoubleType(), IntType()}));
    double data0[DATA_SIZE] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int32_t data1[DATA_SIZE] = {1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LAG};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({DoubleType(), IntType(), DoubleType()}));
    int32_t argumentChannels[1] = {0};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLagDoubleType] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLagDoubleType] Test completed successfully" << std::endl;
}

/**
 * Test LEAD function with DOUBLE type
 */
TEST(LeadLagWindowTest, testLeadDoubleType)
{
    std::cout << "[LeadLagWindowTest::testLeadDoubleType] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({DoubleType(), IntType()}));
    double data0[DATA_SIZE] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int32_t data1[DATA_SIZE] = {1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LEAD};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({DoubleType(), IntType(), DoubleType()}));
    int32_t argumentChannels[1] = {0};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLeadDoubleType] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLeadDoubleType] Test completed successfully" << std::endl;
}

/**
 * Test LAG function with SHORT type
 */
TEST(LeadLagWindowTest, testLagShortType)
{
    std::cout << "[LeadLagWindowTest::testLagShortType] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({ShortType(), IntType()}));
    int16_t data0[DATA_SIZE] = {10, 20, 30, 40, 50, 60};
    int32_t data1[DATA_SIZE] = {1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LAG};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ShortType(), IntType(), ShortType()}));
    int32_t argumentChannels[1] = {0};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLagShortType] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLagShortType] Test completed successfully" << std::endl;
}

/**
 * Test LEAD function with SHORT type
 */
TEST(LeadLagWindowTest, testLeadShortType)
{
    std::cout << "[LeadLagWindowTest::testLeadShortType] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({ShortType(), IntType()}));
    int16_t data0[DATA_SIZE] = {10, 20, 30, 40, 50, 60};
    int32_t data1[DATA_SIZE] = {1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LEAD};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ShortType(), IntType(), ShortType()}));
    int32_t argumentChannels[1] = {0};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLeadShortType] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLeadShortType] Test completed successfully" << std::endl;
}

/**
 * Test LAG function with partition
 * Data is partitioned by column 0
 */
TEST(LeadLagWindowTest, testLagWithPartition)
{
    std::cout << "[LeadLagWindowTest::testLagWithPartition] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({IntType(), LongType()}));
    // Partition key: 0, 0, 0, 1, 1, 1
    int32_t data0[DATA_SIZE] = {0, 0, 0, 1, 1, 1};
    int64_t data1[DATA_SIZE] = {10, 20, 30, 100, 200, 300};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LAG};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[1] = {0};  // Partition by column 0
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({IntType(), LongType(), LongType()}));
    int32_t argumentChannels[1] = {1};  // LAG operates on column 1

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLagWithPartition] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLagWithPartition] Test completed successfully" << std::endl;
}

/**
 * Test LEAD function with partition
 */
TEST(LeadLagWindowTest, testLeadWithPartition)
{
    std::cout << "[LeadLagWindowTest::testLeadWithPartition] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({IntType(), LongType()}));
    int32_t data0[DATA_SIZE] = {0, 0, 0, 1, 1, 1};
    int64_t data1[DATA_SIZE] = {10, 20, 30, 100, 200, 300};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LEAD};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({IntType(), LongType(), LongType()}));
    int32_t argumentChannels[1] = {1};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLeadWithPartition] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLeadWithPartition] Test completed successfully" << std::endl;
}

/**
 * Test LAG and LEAD together with other window functions
 */
TEST(LeadLagWindowTest, testLeadLagWithOtherWindowFunctions)
{
    std::cout << "[LeadLagWindowTest::testLeadLagWithOtherWindowFunctions] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({IntType(), LongType()}));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    int64_t data1[DATA_SIZE] = {10, 20, 30, 40, 50, 60};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    // Multiple window functions: ROW_NUMBER, LAG, LEAD
    int32_t windowFunctionTypes[3] = {OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_LAG, OMNI_WINDOW_TYPE_LEAD};
    int32_t windowFrameTypes[3] = {OMNI_FRAME_TYPE_ROWS, OMNI_FRAME_TYPE_ROWS, OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[3] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[3] = {-1, -1, -1};
    int32_t windowFrameEndTypes[3] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[3] = {-1, -1, -1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    // Output: source columns + row_number(long) + lag(int) + lead(int)
    DataTypes allTypes(std::vector<DataTypePtr>({IntType(), LongType(), LongType(), IntType(), IntType()}));
    int32_t argumentChannels[3] = {-1, 0, 0};  // ROW_NUMBER has no argument, LAG and LEAD operate on column 0

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 3, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 3, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLeadLagWithOtherWindowFunctions] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLeadLagWithOtherWindowFunctions] Test completed successfully" << std::endl;
}

/**
 * Test LAG function with FLOAT type
 */
TEST(LeadLagWindowTest, testLagFloatType)
{
    std::cout << "[LeadLagWindowTest::testLagFloatType] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({FloatType(), IntType()}));
    float data0[DATA_SIZE] = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f, 6.6f};
    int32_t data1[DATA_SIZE] = {1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LAG};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({FloatType(), IntType(), FloatType()}));
    int32_t argumentChannels[1] = {0};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLagFloatType] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLagFloatType] Test completed successfully" << std::endl;
}

/**
 * Test LEAD function with FLOAT type
 */
TEST(LeadLagWindowTest, testLeadFloatType)
{
    std::cout << "[LeadLagWindowTest::testLeadFloatType] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({FloatType(), IntType()}));
    float data0[DATA_SIZE] = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f, 6.6f};
    int32_t data1[DATA_SIZE] = {1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LEAD};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({FloatType(), IntType(), FloatType()}));
    int32_t argumentChannels[1] = {0};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLeadFloatType] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLeadFloatType] Test completed successfully" << std::endl;
}

/**
 * Test LAG function with BOOLEAN type
 */
TEST(LeadLagWindowTest, testLagBooleanType)
{
    std::cout << "[LeadLagWindowTest::testLagBooleanType] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({BooleanType(), IntType()}));
    bool data0[DATA_SIZE] = {true, false, true, false, true, false};
    int32_t data1[DATA_SIZE] = {1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LAG};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({BooleanType(), IntType(), BooleanType()}));
    int32_t argumentChannels[1] = {0};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLagBooleanType] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLagBooleanType] Test completed successfully" << std::endl;
}

/**
 * Test LEAD function with BOOLEAN type
 */
TEST(LeadLagWindowTest, testLeadBooleanType)
{
    std::cout << "[LeadLagWindowTest::testLeadBooleanType] Starting test..." << std::endl;
    
    DataTypes sourceTypes(std::vector<DataTypePtr>({BooleanType(), IntType()}));
    bool data0[DATA_SIZE] = {true, false, true, false, true, false};
    int32_t data1[DATA_SIZE] = {1, 2, 3, 4, 5, 6};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1);

    int32_t outputCols[2] = {0, 1};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_LEAD};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({BooleanType(), IntType(), BooleanType()}));
    int32_t argumentChannels[1] = {0};

    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        2, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    windowOperator->noMoreInput();
    
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    ASSERT_NE(outputVecBatch, nullptr);
    std::cout << "[LeadLagWindowTest::testLeadBooleanType] Output VectorBatch received" << std::endl;
    VectorHelper::PrintVecBatch(outputVecBatch);

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(outputVecBatch);
    
    std::cout << "[LeadLagWindowTest::testLeadBooleanType] Test completed successfully" << std::endl;
}

} // namespace LeadLagTest
