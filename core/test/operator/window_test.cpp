/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: window operator implementations
 */
#include <vector>
#include <iostream>
#include <chrono>

#include "gtest/gtest.h"
#include "operator/window/window.h"
#include "util/test_util.h"
#include "vector/vector_helper.h"
#include "vector/unsafe_vector.h"
#include "perf_util.h"
#include "util/config_util.h"

using namespace std;
using namespace omniruntime::vec;
using namespace omniruntime::op;
using namespace TestUtil;

namespace WindowTest {
const int32_t DATA_SIZE = 6;
const int32_t VEC_BATCH_NUM = 10;
const int32_t ROW_PER_VEC_BATCH = 100;
const uint64_t MAX_SPILL_BYTES = (5L << 20);

BaseVector *BuildVectorInput(const DataTypePtr sourceType, int32_t rowPerVecBatch)
{
    switch (sourceType->GetId()) {
        case OMNI_NONE: {
            auto col = new vec::Vector<int64_t>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetNull(j);
            }
            return col;
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            auto col = new vec::Vector<int32_t>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_SHORT: {
            auto col = new vec::Vector<int16_t>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64: {
            auto col = new vec::Vector<int64_t>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_DOUBLE: {
            auto col = new vec::Vector<double>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, 1);
            }
            return col;
        }
        case OMNI_BOOLEAN: {
            auto col = new vec::Vector<bool>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                col->SetValue(j, true);
            }
            return col;
        }
        case OMNI_DECIMAL128: {
            auto col = new vec::Vector<Decimal128>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                Decimal128 decimal128(0, 1);
                col->SetValue(j, decimal128);
            }
            return col;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR: {
            auto col = new vec::Vector<LargeStringContainer<std::string_view>>(rowPerVecBatch);
            for (int32_t j = 0; j < rowPerVecBatch; ++j) {
                std::string str = std::to_string(j);
                auto str_view = std::string_view(str.data(), str.size());
                col->SetValue(j, str_view);
            }
            return col;
        }
        default: {
            LogError("No such %d type support", sourceType->GetId());
            return nullptr;
        }
    }
}

VectorBatch **BuildWindowInput(int32_t vecBatchNum, int32_t rowPerVecBatch, int32_t windowFunctionNum,
    const std::vector<DataTypePtr> &sourceTypes)
{
    VectorBatch **input = new VectorBatch *[vecBatchNum];
    for (int32_t i = 0; i < vecBatchNum; ++i) {
        VectorBatch *vecBatch = new VectorBatch(windowFunctionNum);
        for (int32_t index = 0; index < windowFunctionNum; index++) {
            auto vec = std::unique_ptr<BaseVector>(BuildVectorInput(sourceTypes[index], rowPerVecBatch));
            vecBatch->Append(vec.release());
        }
        input[i] = vecBatch;
    }
    return input;
}

TEST(NativeOmniWindowOperatorTest, testRowNumberPartition)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    int64_t expectData2[DATA_SIZE] = {3, 0, 4, 1, 5, 2};
    double expectData3[DATA_SIZE] = {3.3, 6.6, 2.2, 5.5, 1.1, 4.4};
    int16_t expectData4[DATA_SIZE] = {2, 5, 1, 4, 0, 3};
    int64_t expectData5[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4, expectData5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testRowNumber)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    int32_t outputCols[3] = {2, 1, 3};
    int32_t sortCols[0] = {};
    int32_t ascendings[0] = {};
    int32_t nullFirsts[0] = {};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[1] = {2};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 0,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *test = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));
    WindowOperator *windowOperator = test;
    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ DoubleType(), LongType(), ShortType(), LongType() }));
    double expectData1[DATA_SIZE] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int64_t expectData2[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    int16_t expectData3[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    int64_t expectData4[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testRankPartition)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_RANK};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 1, 5, 2};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 5.5, 1.1, 4.4};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 4, 0, 3};
    int64_t expectData5[DATA_SIZE] = {1, 1, 1, 2, 1, 2};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4, expectData5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testRank)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    int32_t outputCols[4] = {1, 2, 0, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_RANK};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), DoubleType(), IntType(), ShortType(), LongType() }));
    int64_t expectData1[DATA_SIZE] = {8, 8, 5, 4, 2, 1};
    double expectData2[DATA_SIZE] = {6.6, 3.3, 1.1, 2.2, 4.4, 5.5};
    int32_t expectData3[DATA_SIZE] = {0, 0, 2, 1, 2, 1};
    int16_t expectData4[DATA_SIZE] = {5, 2, 0, 1, 3, 4};
    int64_t expectData5[DATA_SIZE] = {1, 1, 3, 4, 5, 6};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4, expectData5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testRowNumberAndRankPartition)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[2] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[2] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[2] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[2] = {-1, -1};
    int32_t windowFrameEndTypes[2] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[2] = {-1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t argumentChannels[2] = {-1, -1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 1, 5, 2};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 5.5, 1.1, 4.4};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 4, 0, 3};
    int64_t expectData5[DATA_SIZE] = {1, 1, 1, 2, 1, 2};
    int64_t expectData6[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testRowNumberAndRankPartitionWithNull)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);
    vecBatch->Get(0)->SetNull(1);
    vecBatch->Get(0)->SetNull(5);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[2] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[2] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[2] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[2] = {-1, -1};
    int32_t windowFrameEndTypes[2] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[2] = {-1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t argumentChannels[2] = {-1, -1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 2, 1};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 2, 5, 1};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 1.1, 5.5};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 3, 0, 4};
    int64_t expectData5[DATA_SIZE] = {1, 1, 1, 1, 1, 2};
    int64_t expectData6[DATA_SIZE] = {1, 2, 1, 1, 1, 2};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);
    expectVecBatch->Get(0)->SetNull(4);
    expectVecBatch->Get(0)->SetNull(5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testRowNumberAndRankPartitionWithNullWithoutSort)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);
    vecBatch->Get(0)->SetNull(1);
    vecBatch->Get(0)->SetNull(5);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[0] = {};
    int32_t ascendings[0] = {};
    int32_t nullFirsts[0] = {};
    int32_t windowFunctionTypes[2] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[2] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[2] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[2] = {-1, -1};
    int32_t windowFrameEndTypes[2] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[2] = {-1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t argumentChannels[2] = {-1, -1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 0,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 2, 1};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 2, 5, 1};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 1.1, 5.5};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 3, 0, 4};
    int64_t expectData5[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData6[DATA_SIZE] = {1, 2, 1, 1, 1, 2};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);
    expectVecBatch->Get(0)->SetNull(4);
    expectVecBatch->Get(0)->SetNull(5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testAggregationPartitionWithNull)
{
    // construct input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    vecBatch->Get(0)->SetNull(1);
    vecBatch->Get(0)->SetNull(5);

    vecBatch->Get(1)->SetNull(3);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[5] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MIN};
    int32_t windowFrameTypes[5] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[5] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[5] = {-1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[5] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[5] = {-1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(),
        LongType(), DoubleType(), DoubleType(), LongType() }));

    int32_t argumentChannels[5] = {1, 1, 1, 2, 1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 5, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 5, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(),
        LongType(), DoubleType(), DoubleType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 1, 1};
    int64_t expectData2[DATA_SIZE] = {8, 4, 4, 2, 5, 1};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 1.1, 5.5};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 3, 0, 4};
    int64_t expectData5[DATA_SIZE] = {8, 8, 4, 2, 5, 6};
    int64_t expectData6[DATA_SIZE] = {1, 1, 1, 1, 1, 2};
    double expectData7[DATA_SIZE] = {8, 8, 4, 2, 5, 3};
    double expectData8[DATA_SIZE] = {6.6, 6.6, 2.2, 4.4, 1.1, 5.5};
    int64_t expectData9[DATA_SIZE] = {8, 8, 4, 2, 5, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6, expectData7, expectData8, expectData9);
    expectVecBatch->Get(0)->SetNull(4);
    expectVecBatch->Get(0)->SetNull(5);
    expectVecBatch->Get(1)->SetNull(1);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testAggregationPartitionWithNullWithoutSort)
{
    // construct input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    vecBatch->Get(0)->SetNull(1);
    vecBatch->Get(0)->SetNull(5);
    vecBatch->Get(1)->SetNull(3);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[0] = {};
    int32_t ascendings[0] = {};
    int32_t nullFirsts[0] = {};
    int32_t windowFunctionTypes[5] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MIN};
    int32_t windowFrameTypes[5] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[5] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[5] = {-1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[5] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[5] = {-1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(),
        LongType(), DoubleType(), DoubleType(), LongType() }));

    int32_t argumentChannels[5] = {1, 1, 1, 2, 1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 5, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 0,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 5, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);

    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(),
        LongType(), DoubleType(), DoubleType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 1, 1};
    int64_t expectData2[DATA_SIZE] = {8, 4, 4, 2, 5, 1};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 1.1, 5.5};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 3, 0, 4};
    int64_t expectData5[DATA_SIZE] = {8, 8, 4, 2, 6, 6};
    int64_t expectData6[DATA_SIZE] = {1, 1, 1, 1, 2, 2};
    double expectData7[DATA_SIZE] = {8, 8, 4, 2, 3, 3};
    double expectData8[DATA_SIZE] = {6.6, 6.6, 2.2, 4.4, 5.5, 5.5};
    int64_t expectData9[DATA_SIZE] = {8, 8, 4, 2, 1, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6, expectData7, expectData8, expectData9);
    expectVecBatch->Get(0)->SetNull(4);
    expectVecBatch->Get(0)->SetNull(5);
    expectVecBatch->Get(1)->SetNull(1);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testRankWithAllDataTypes)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[colCount] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK,
        OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK,
        OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK};
    int32_t windowFrameTypes[colCount] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[colCount] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[colCount] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[colCount] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[colCount] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, colCount, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts,
        1, preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t expectData3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t expectData4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double expectData5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int64_t expectData11[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData12[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData13[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData14[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData15[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData16[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData17[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData18[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData19[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData20[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData21[DATA_SIZE] = {1, 1, 1, 1, 1, 1};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18,
        expectData19, expectData20, expectData21);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testRowNumberkWithAllDataTypes)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[colCount] = {OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER,
        OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER,
        OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER,
        OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[colCount] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[colCount] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[colCount] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[colCount] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[colCount] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t argumentChannels[0] = {};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, colCount, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts,
        1, preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t expectData3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t expectData4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double expectData5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int64_t expectData11[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData12[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData13[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData14[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData15[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData16[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData17[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData18[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData19[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData20[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData21[DATA_SIZE] = {1, 2, 1, 2, 1, 2};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18,
        expectData19, expectData20, expectData21);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));
    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testSumWithAllDataTypes)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);

    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(5, 0), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(2, 2), Decimal128(2, 2),
        Decimal128(3, 3), Decimal128(3, 3)};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[8] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    int32_t windowFrameTypes[8] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[8] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[8] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY), Date32Type(omniruntime::type::MILLI),
        LongType(), Decimal64Type(5, 0), DoubleType(), BooleanType(), VarcharType(3), Decimal128Type(2, 2), CharType(3),
        ShortType(), IntType(), Date32Type(omniruntime::type::DAY), Date32Type(omniruntime::type::MILLI), LongType(),
        Decimal128Type(10, 0), DoubleType(), Decimal128Type(2, 2), ShortType() }));
    int32_t argumentChannels[8] = {0, 1, 2, 3, 4, 5, 8, 10};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 8, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 8, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY), Date32Type(omniruntime::type::MILLI),
        LongType(), Decimal64Type(5, 0), DoubleType(), BooleanType(), VarcharType(3), Decimal128Type(2, 2), CharType(3),
        ShortType(), IntType(), Date32Type(omniruntime::type::DAY), Date32Type(omniruntime::type::MILLI), LongType(),
        Decimal128Type(10, 0), DoubleType(), Decimal128Type(2, 2), ShortType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t expectData3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t expectData4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double expectData5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(2, 2), Decimal128(2, 2),
        Decimal128(3, 3), Decimal128(3, 3)};
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData11[DATA_SIZE] = {2, 2, 4, 4, 6, 6};
    int32_t expectData12[DATA_SIZE] = {22, 22, 44, 44, 66, 66};
    int32_t expectData13[DATA_SIZE] = {222, 222, 444, 444, 666, 666};
    int64_t expectData14[DATA_SIZE] = {2222, 2222, 4444, 4444, 6666, 6666};
    Decimal128 expectData15[DATA_SIZE] = {Decimal128(22222), Decimal128(22222), Decimal128(44444), Decimal128(44444),
        Decimal128(66666), Decimal128(66666)};
    double expectData16[DATA_SIZE] = {2.2, 2.2, 4.4, 4.4, 6.6, 6.6};
    Decimal128 expectData17[DATA_SIZE] = {Decimal128(2, 2), Decimal128(2, 2), Decimal128(4, 4), Decimal128(4, 4),
        Decimal128(6, 6), Decimal128(6, 6)};
    int16_t expectData18[DATA_SIZE] = {22, 22, 44, 44, 66, 66};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testAvgWithAllDataTypes)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);

    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(5, 2), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0),
        Decimal128(0, 0), Decimal128(0, 0)};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t data10[DATA_SIZE] = {11, 33, 33, 55, 55, 77};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[8] = {OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_AVG};
    int32_t windowFrameTypes[8] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[8] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[8] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(5, 2), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType(), DoubleType(), DoubleType(), DoubleType(),
        DoubleType(), Decimal64Type(10, 4), DoubleType(), Decimal128Type(4, 4), DoubleType() }));
    int32_t argumentChannels[8] = {0, 1, 2, 3, 4, 5, 8, 10};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 8, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 8, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(5, 2), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType(), DoubleType(), DoubleType(), DoubleType(),
        DoubleType(), Decimal64Type(10, 4), DoubleType(), Decimal128Type(4, 4), DoubleType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0),
        Decimal128(0, 0), Decimal128(0, 0)};
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t expectData10[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    double expectData11[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    double expectData12[DATA_SIZE] = {22, 22, 44, 44, 66, 66};
    double expectData13[DATA_SIZE] = {222, 222, 444, 444, 666, 666};
    double expectData14[DATA_SIZE] = {2222, 2222, 4444, 4444, 6666, 6666};
    int64_t expectData15[DATA_SIZE] = {2222200, 2222200, 4444400, 4444400, 6666600, 6666600};
    double expectData16[DATA_SIZE] = {2.2, 2.2, 4.4, 4.4, 6.6, 6.6};
    Decimal128 expectData17[DATA_SIZE] = {Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0),
                                          Decimal128(0, 0), Decimal128(0, 0)};
    double expectData18[DATA_SIZE] = {22, 22, 44, 44, 66, 66};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testMaxWithAllDataTypes)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string data9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[10] = {OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX};
    int32_t windowFrameTypes[10] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[10] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[10] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType() }));
    int32_t argumentChannels[10] = {0, 1, 2, 3, 4, 5, 7, 8, 9, 10};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 10, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 10, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string expectData9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData11[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData12[DATA_SIZE] = {33, 33, 55, 55, 77, 77};
    int32_t expectData13[DATA_SIZE] = {333, 333, 555, 555, 777, 777};
    int64_t expectData14[DATA_SIZE] = {3333, 3333, 5555, 5555, 7777, 7777};
    int64_t expectData15[DATA_SIZE] = {33333, 33333, 55555, 55555, 77777, 77777};
    double expectData16[DATA_SIZE] = {3.3, 3.3, 5.5, 5.5, 7.7, 7.7};
    std::string expectData17[DATA_SIZE] = {"s3", "s3", "s5", "s5", "s7", "s7"};
    Decimal128 expectData18[DATA_SIZE] = {Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5),
        Decimal128(7, 7), Decimal128(7, 7)};
    std::string expectData19[DATA_SIZE] = {"c3", "c3", "c5", "c5", "c7", "c7"};
    int16_t expectData20[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2, expectData3, expectData4,
        expectData5, expectData6, expectData7, expectData8, expectData9, expectData10, expectData11, expectData12,
        expectData13, expectData14, expectData15, expectData16, expectData17, expectData18, expectData19, expectData20);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testMinWithAllDataTypes)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string data9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[10] = {OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN};
    int32_t windowFrameTypes[10] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[10] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[10] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType() }));
    int32_t argumentChannels[10] = {0, 1, 2, 3, 4, 5, 7, 8, 9, 10};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 10, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 10, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string expectData9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData11[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData12[DATA_SIZE] = {11, 11, 33, 33, 55, 55};
    int32_t expectData13[DATA_SIZE] = {111, 111, 333, 333, 555, 555};
    int64_t expectData14[DATA_SIZE] = {1111, 1111, 3333, 3333, 5555, 5555};
    int64_t expectData15[DATA_SIZE] = {11111, 11111, 33333, 33333, 55555, 55555};
    double expectData16[DATA_SIZE] = {1.1, 1.1, 3.3, 3.3, 5.5, 5.5};
    std::string expectData17[DATA_SIZE] = {"s1", "s1", "s3", "s3", "s5", "s5"};
    Decimal128 expectData18[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3),
        Decimal128(5, 5), Decimal128(5, 5)};
    std::string expectData19[DATA_SIZE] = {"c1", "c1", "c3", "c3", "c5", "c5"};
    int16_t expectData20[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2, expectData3, expectData4,
        expectData5, expectData6, expectData7, expectData8, expectData9, expectData10, expectData11, expectData12,
        expectData13, expectData14, expectData15, expectData16, expectData17, expectData18, expectData19, expectData20);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testCountWithAllDataTypes)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string data9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[10] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
    int32_t windowFrameTypes[10] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[10] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[10] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t argumentChannels[10] = {0, 1, 2, 3, 4, 5, 7, 8, 9, 10};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 10, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 10, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string expectData9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int64_t expectData11[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData12[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData13[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData14[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData15[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData16[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData17[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData18[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData19[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData20[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2, expectData3, expectData4,
        expectData5, expectData6, expectData7, expectData8, expectData9, expectData10, expectData11, expectData12,
        expectData13, expectData14, expectData15, expectData16, expectData17, expectData18, expectData19, expectData20);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testCountRowsWithNullWithSort)
{
    // construct the input data
    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int64_t data1[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data2[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data3[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data4[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    int16_t data5[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5);

    vecBatch->Get(1)->SetNull(1);
    vecBatch->Get(1)->SetNull(2);
    vecBatch->Get(2)->SetNull(2);
    vecBatch->Get(2)->SetNull(3);
    vecBatch->Get(2)->SetNull(4);
    vecBatch->Get(3)->SetNull(0);
    vecBatch->Get(4)->SetNull(1);
    vecBatch->Get(4)->SetNull(5);
    vecBatch->Get(5)->SetNull(1);
    vecBatch->Get(5)->SetNull(2);

    const int32_t colCount = 6;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5 };
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[11] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
    int32_t windowFrameTypes[11] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[11] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[11] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[11] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[11] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3),
        ShortType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(),
        LongType(), LongType(), LongType() }));
    int32_t argumentChannels[11] = {0, -1, 1, -1, 2, -1, 3, -1, 4, -1, 5};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 11, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 11, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3),
        ShortType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(),
        LongType(), LongType(), LongType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int64_t expectData1[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData2[DATA_SIZE] = {1.1, 3.3, 5.5, 5.5, 5.5, 7.7};
    bool expectData3[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData4[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    int16_t expectData5[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int64_t expectData6[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData7[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData8[DATA_SIZE] = {1, 1, 1, 1, 2, 2};
    int64_t expectData9[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData10[DATA_SIZE] = {2, 2, 0, 0, 1, 1};
    int64_t expectData11[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData12[DATA_SIZE] = {1, 1, 2, 2, 2, 2};
    int64_t expectData13[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData14[DATA_SIZE] = {1, 1, 2, 2, 1, 1};
    int64_t expectData15[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData16[DATA_SIZE] = {1, 1, 1, 1, 2, 2};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16);

    expectVecBatch->Get(1)->SetNull(1);
    expectVecBatch->Get(1)->SetNull(2);
    expectVecBatch->Get(2)->SetNull(2);
    expectVecBatch->Get(2)->SetNull(3);
    expectVecBatch->Get(2)->SetNull(4);
    expectVecBatch->Get(3)->SetNull(0);
    expectVecBatch->Get(4)->SetNull(1);
    expectVecBatch->Get(4)->SetNull(5);
    expectVecBatch->Get(5)->SetNull(1);
    expectVecBatch->Get(5)->SetNull(2);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testCountRowsWithNullWithoutSort)
{
    // construct the input data
    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int64_t data1[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data2[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data3[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data4[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4);

    vecBatch->Get(1)->SetNull(1);
    vecBatch->Get(1)->SetNull(2);
    vecBatch->Get(2)->SetNull(2);
    vecBatch->Get(2)->SetNull(3);
    vecBatch->Get(2)->SetNull(4);
    vecBatch->Get(3)->SetNull(0);
    vecBatch->Get(4)->SetNull(1);
    vecBatch->Get(4)->SetNull(5);

    const int32_t colCount = 5;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4 };
    int32_t sortCols[1] = {};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[10] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL};
    int32_t windowFrameTypes[10] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[10] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[10] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3), LongType(),
        LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType() }));
    int32_t argumentChannels[10] = {0, -1, 1, -1, 2, -1, 3, -1, 4, -1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 10, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 10, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3), LongType(),
        LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int64_t expectData1[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData2[DATA_SIZE] = {1.1, 3.3, 5.5, 5.5, 5.5, 7.7};
    bool expectData3[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData4[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    int64_t expectData5[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData6[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData7[DATA_SIZE] = {1, 1, 1, 1, 2, 2};
    int64_t expectData8[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData9[DATA_SIZE] = {2, 2, 0, 0, 1, 1};
    int64_t expectData10[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData11[DATA_SIZE] = {1, 1, 2, 2, 2, 2};
    int64_t expectData12[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData13[DATA_SIZE] = {1, 1, 2, 2, 1, 1};
    int64_t expectData14[DATA_SIZE] = {2, 2, 2, 2, 2, 2};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14);

    expectVecBatch->Get(1)->SetNull(1);
    expectVecBatch->Get(1)->SetNull(2);
    expectVecBatch->Get(2)->SetNull(2);
    expectVecBatch->Get(2)->SetNull(3);
    expectVecBatch->Get(2)->SetNull(4);
    expectVecBatch->Get(3)->SetNull(0);
    expectVecBatch->Get(4)->SetNull(1);
    expectVecBatch->Get(4)->SetNull(5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));
    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testDictionaryVector)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    int16_t data9[DATA_SIZE] = {11, 33, 33, 55, 55, 77};

    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    auto *vecBatch = new vec::VectorBatch(DATA_SIZE);
    void *datas[10] = {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9};
    for (int32_t i = 0; i < sourceTypes.GetSize(); i++) {
        DataTypePtr dataType = sourceTypes.GetType(i);
        vecBatch->Append(CreateDictionaryVector(*dataType, DATA_SIZE, ids, DATA_SIZE, datas[i]));
    }

    int32_t outputCols[10] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[8] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_MIN, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[8] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[8] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[8] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), ShortType(), LongType(), LongType(), LongType(), LongType(), DoubleType(),
        VarcharType(3), Decimal128Type(2, 2), ShortType() }));
    int32_t argumentChannels[8] = {0, 1, 3, 4, 5, 7, 8, 9};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        10, windowFunctionTypes, 8, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 8, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), ShortType(), LongType(), LongType(), LongType(), LongType(), DoubleType(),
        VarcharType(3), Decimal128Type(2, 2), ShortType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    int16_t expectData9[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int64_t expectData10[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData11[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData12[DATA_SIZE] = {4444, 4444, 8888, 8888, 13332, 13332};
    int64_t expectData13[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    double expectData14[DATA_SIZE] = {2.2, 2.2, 4.4, 4.4, 6.6, 6.6};
    std::string expectData15[DATA_SIZE] = {"s3", "s3", "s5", "s5", "s7", "s7"};
    Decimal128 expectData16[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3),
        Decimal128(5, 5), Decimal128(5, 5)};
    int16_t expectData17[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowPerf)
{
    DataTypes sourceTypes(std::vector<DataTypePtr>(
        { IntType(),    LongType(),           DoubleType(), VarcharType(20),       CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),           DoubleType(), VarcharType(20),       CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),           DoubleType(), VarcharType(20),       CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),           DoubleType(), Decimal128Type(22, 5), IntType(),    LongType(),
        DoubleType(), Decimal128Type(22, 5) }));
    int32_t outputCols[26] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
                              14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
    int32_t windowFunctionTypes[26] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
                                       OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
                                       OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
                                       OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
                                       OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
                                       OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
                                       OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
                                       OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
                                       OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
                                       OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
                                       OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
                                       OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG,
                                       OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG};
    int32_t windowFrameTypes[26] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[26] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[26] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  -1, -1,
                                            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[26] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[26] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  -1,
                                          -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;
    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),    LongType(),
        DoubleType(), VarcharType(20),
        CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), VarcharType(20),
        CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), VarcharType(20),
        CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), Decimal128Type(22, 5),
        LongType(),   LongType(),
        LongType(),   LongType(),
        LongType(),   LongType(),
        IntType(),    LongType(),
        DoubleType(), VarcharType(20),
        CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), VarcharType(20),
        CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), Decimal128Type(22, 5),
        DoubleType(), DoubleType(),
        DoubleType(), Decimal128Type(22, 5) }));
    int32_t argumentChannels[26] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                    13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        26, windowFunctionTypes, 26, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 26, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    operatorFactory->Init();
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));
    VectorBatch **input = BuildWindowInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, 26, sourceTypes.Get());

    Timer timer;
    timer.SetStart();

    ASSERT(!(input == nullptr));

    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        auto errNo = windowOperator->AddInput(input[pageIndex]);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }

    VectorBatch *result = nullptr;
    while (windowOperator->GetStatus() == OMNI_STATUS_NORMAL) {
        windowOperator->GetOutput(&result);
    }

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse();
    double cpuElapsed = timer.GetCpuElapse();
    std::cout << "Window with Omni, wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;

    delete[] input;
    VectorHelper::FreeVecBatch(result);
}

TEST(NativeOmniWindowOperatorTest, testWindowComparePerf)
{
    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType() }));
    int32_t outputCols[2] = {0, 1};
    int32_t windowFunctionTypes[2] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL};
    int32_t windowFrameTypes[2] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[2] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[2] = {-1, -1};
    int32_t windowFrameEndTypes[2] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[2] = {-1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;
    DataTypes allTypes(std::vector<DataTypePtr>({ LongType(), LongType(), LongType(), LongType() }));
    int32_t argumentChannels[2] = {1, -1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactoryWithJit = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes,
        outputCols, 2, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 2, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    operatorFactoryWithJit->Init();
    WindowOperator *windowOperatorWithJit = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactoryWithJit));
    VectorBatch **input1 = BuildWindowInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, 2, sourceTypes.Get());

    Timer timer;
    timer.SetStart();

    if (input1 == nullptr) {
        std::cerr << "Building input1 data failed!" << std::endl;
    }

    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        auto errNo = windowOperatorWithJit->AddInput(input1[pageIndex]);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }

    auto *perfUtil = new PerfUtil();
    perfUtil->Init();
    perfUtil->Reset();
    perfUtil->Start();
    perfUtil->Stop();
    long instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("Window with OmniJit, used %lld instructions\n", perfUtil->GetData());
    }

    std::vector<VectorBatch *> resultWithJit;
    while (windowOperatorWithJit->GetStatus() != OMNI_STATUS_FINISHED) {
        VectorBatch *outputVecBatchWithJit = nullptr;
        windowOperatorWithJit->GetOutput(&outputVecBatchWithJit);
        resultWithJit.push_back(outputVecBatchWithJit);
    }

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse();
    double cpuElapsed = timer.GetCpuElapse();
    std::cout << "Window with OmniJit, wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;

    WindowOperatorFactory *operatorFactoryWithoutJit = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes,
        outputCols, 2, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 2, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    operatorFactoryWithoutJit->Init();
    std::cout << "after create factory" << std::endl;
    WindowOperator *windowOperatorWithoutJit =
        dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactoryWithoutJit));
    VectorBatch **input2 = BuildWindowInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, 2, sourceTypes.Get());

    timer.Reset();

    if (input2 == nullptr) {
        std::cerr << "Building input2 data failed!" << std::endl;
    }

    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        auto errNo = windowOperatorWithoutJit->AddInput(input2[pageIndex]);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }

    perfUtil->Reset();
    perfUtil->Start();
    perfUtil->Stop();
    instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("Window without OmniJit, used %lld instructions\n", perfUtil->GetData());
    }

    std::vector<VectorBatch *> resultWithoutJit;
    while (windowOperatorWithoutJit->GetStatus() != OMNI_STATUS_FINISHED) {
        VectorBatch *outputVecBatchWithoutJit = nullptr;
        windowOperatorWithoutJit->GetOutput(&outputVecBatchWithoutJit);
        resultWithoutJit.push_back(outputVecBatchWithoutJit);
    }

    delete perfUtil;

    timer.CalculateElapse();
    wallElapsed = timer.GetWallElapse();
    cpuElapsed = timer.GetCpuElapse();

    std::cout << "Window without OmniJit, wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;

    op::Operator::DeleteOperator(windowOperatorWithJit);
    op::Operator::DeleteOperator(windowOperatorWithoutJit);
    delete operatorFactoryWithJit;
    delete operatorFactoryWithoutJit;

    EXPECT_EQ(resultWithJit.size(), resultWithoutJit.size());
    for (uint32_t i = 0; i < resultWithJit.size(); ++i) {
        EXPECT_TRUE(VecBatchMatchIgnoreOrder(resultWithJit[i], resultWithoutJit[i]));
    }

    delete[] input1;
    delete[] input2;
    VectorHelper::FreeVecBatches(resultWithJit);
    VectorHelper::FreeVecBatches(resultWithoutJit);
}

TEST(NativeOmniWindowOperatorTest, testFrameBound)
{
    // construct the input data
    const int MY_DATA_SIZE = DATA_SIZE + DATA_SIZE;
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType() }));
    std::string data0[MY_DATA_SIZE] = {"banana", "apple", "banana", "apple", "banana", "banana", "banana", "banana",
                                   "apple", "orange", "banana", "apple"};
    std::string data1[MY_DATA_SIZE] = {"2020-11-01", "2020-12-01", "2020-10-01", "2020-11-01",
                                   "2020-12-01", "2020-12-02", "2020-12-07", "2020-12-04", "2021-01-01", "2021-01-01",
                                   "2021-01-01", "2021-02-01"};
    int64_t data2[MY_DATA_SIZE] = {7400, 8000, 7800, 7000, 7500, 6500, 4500, 8500, 9000, 8000, 8500, 9500};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, MY_DATA_SIZE, data0, data1, data2);

    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t argumentChannels[1] = {2};
    int32_t outputCols[3] = {0, 1, 2};
    int32_t windowFunctionTypes[1] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), LongType() }));

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), LongType() }));
    std::string expectData1[MY_DATA_SIZE] = {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana",
                                         "banana", "banana", "banana", "orange"};
    std::string expectData2[MY_DATA_SIZE] = {"2020-11-01", "2020-12-01", "2021-01-01", "2021-02-01", "2020-10-01",
                                         "2020-11-01", "2020-12-01", "2020-12-02", "2020-12-04", "2020-12-07",
                                         "2021-01-01", "2021-01-01"};
    int64_t expectData3[MY_DATA_SIZE] = {7000, 8000, 9000, 9500, 7800, 7400, 7500, 6500, 8500, 4500, 8500, 8000};
    int64_t expectData4[MY_DATA_SIZE] = {1, 2, 3, 4, 1, 2, 3, 4, 5, 6, 7, 1};

    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, MY_DATA_SIZE, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testFrameBoundedN)
{
    // construct the input data
    const int MY_DATA_SIZE = DATA_SIZE + DATA_SIZE;
    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), IntType(), IntType() }));
    std::string data0[MY_DATA_SIZE] = {"banana", "apple", "banana", "apple", "banana", "banana", "banana", "banana",
                                   "apple", "orange", "banana", "apple"};
    std::string data1[MY_DATA_SIZE] = {"2020-11-01", "2020-12-01", "2020-10-01", "2020-11-01", "2020-12-01",
                                       "2020-12-02", "2020-12-07", "2020-12-04", "2021-01-01", "2021-01-01",
                                       "2021-01-01", "2021-02-01"};
    int64_t data2[MY_DATA_SIZE] = {7400, 8000, 7800, 7000, 7500, 6500, 4500, 8500, 9000, 8000, 8500, 9500};
    int32_t data3[MY_DATA_SIZE] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; // frame start col
    int32_t data4[MY_DATA_SIZE] = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // frane end col
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, MY_DATA_SIZE, data0, data1, data2, data3, data4);

    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t argumentChannels[1] = {2};
    int32_t outputCols[5] = {0, 1, 2, 3, 4};
    int32_t windowFunctionTypes[1] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_PRECEDING};
    int32_t windowFrameStartChannels[1] = {3};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_FOLLOWING};
    int32_t windowFrameEndChannels[1] = {4};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), IntType(), IntType(), LongType() }));

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        5, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), IntType(), IntType(), LongType() }));
    std::string expectData1[MY_DATA_SIZE] = {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana",
                                         "banana", "banana", "banana", "orange"};
    std::string expectData2[MY_DATA_SIZE] = {"2020-11-01", "2020-12-01", "2021-01-01", "2021-02-01", "2020-10-01",
                                         "2020-11-01", "2020-12-01", "2020-12-02", "2020-12-04", "2020-12-07",
                                         "2021-01-01", "2021-01-01"};
    int64_t expectData3[MY_DATA_SIZE] = {7000, 8000, 9000, 9500, 7800, 7400, 7500, 6500, 8500, 4500, 8500, 8000};
    int32_t expectData4[MY_DATA_SIZE] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; // frame start col
    int32_t expectData5[MY_DATA_SIZE] = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // frane end col
    int64_t expectData6[MY_DATA_SIZE] = {3, 4, 3, 2, 3, 4, 4, 4, 4, 3, 2, 1};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, MY_DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testFrameUnBounded)
{
    // construct the input data
    const int MY_DATA_SIZE = DATA_SIZE + DATA_SIZE;
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType() }));
    std::string data0[MY_DATA_SIZE] = {"banana", "apple", "banana", "apple", "banana", "banana", "banana", "banana",
                                   "apple", "orange", "banana", "apple"};
    std::string data1[MY_DATA_SIZE] = {"2020-11-01", "2020-12-01", "2020-10-01", "2020-11-01", "2020-12-01",
                                       "2020-12-02", "2020-12-07", "2020-12-04", "2021-01-01", "2021-01-01",
                                       "2021-01-01", "2021-02-01"};
    int64_t data2[MY_DATA_SIZE] = {7400, 8000, 7800, 7000, 7500, 6500, 4500, 8500, 9000, 8000, 8500, 9500};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, MY_DATA_SIZE, data0, data1, data2);

    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t argumentChannels[1] = {2};
    int32_t outputCols[5] = {0, 1, 2};
    int32_t windowFunctionTypes[1] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING};
    int32_t windowFrameEndChannels[1] = {-1};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), LongType() }));

    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), LongType() }));
    std::string expectData1[MY_DATA_SIZE] = {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana",
                                         "banana", "banana", "banana", "orange"};
    std::string expectData2[MY_DATA_SIZE] = {"2020-11-01", "2020-12-01", "2021-01-01", "2021-02-01", "2020-10-01",
                                         "2020-11-01", "2020-12-01", "2020-12-02", "2020-12-04", "2020-12-07",
                                         "2021-01-01", "2021-01-01"};
    int64_t expectData3[MY_DATA_SIZE] = {7000, 8000, 9000, 9500, 7800, 7400, 7500, 6500, 8500, 4500, 8500, 8000};
    int64_t expectData4[MY_DATA_SIZE] = {4, 4, 4, 4, 7, 7, 7, 7, 7, 7, 7, 1};

    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, MY_DATA_SIZE, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithInvalidConfig)
{
    const int32_t dataSize = 6;
    int32_t data1[dataSize] = {5, 2, 4, 1, 3, 0};
    int64_t data2[dataSize] = {11, 44, 22, 55, 33, 66};
    double data3[dataSize] = {2.3, 5.666, 98.001, 23.0, 0.2, -0.2323};
    int16_t data4[dataSize] = {0, 2, -2, 3, 9, 6};

    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t argumentChannels[0] = {};

    SparkSpillConfig spillConfig1("", UINT64_MAX, 5);
    OperatorConfig operatorConfig1(spillConfig1);
    auto operatorFactory1 = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols, 4,
        windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig1);
    auto operator1 = operatorFactory1->CreateOperator();
    auto vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);
    EXPECT_THROW(operator1->AddInput(vecBatch1), omniruntime::exception::OmniException);
    omniruntime::op::Operator::DeleteOperator(operator1);
    delete operatorFactory1;

    SparkSpillConfig spillConfig2("/opt/+-ab23", UINT64_MAX, 5);
    OperatorConfig operatorConfig2(spillConfig2);
    auto operatorFactory2 = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols, 4,
        windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig2);
    auto operator2 = operatorFactory2->CreateOperator();
    auto vecBatch2 = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);
    EXPECT_THROW(operator2->AddInput(vecBatch2), omniruntime::exception::OmniException);
    omniruntime::op::Operator::DeleteOperator(operator2);
    delete operatorFactory2;
    rmdir("/opt/+-ab23");

    SparkSpillConfig spillConfig3("/", UINT64_MAX, 5);
    OperatorConfig operatorConfig3(spillConfig3);
    auto operatorFactory3 = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols, 4,
        windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig3);
    auto operator3 = operatorFactory3->CreateOperator();
    auto vecBatch3 = CreateVectorBatch(sourceTypes, dataSize, data1, data2, data3, data4);
    EXPECT_THROW(operator3->AddInput(vecBatch3), omniruntime::exception::OmniException);
    omniruntime::op::Operator::DeleteOperator(operator3);
    delete operatorFactory3;
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithRowNumberPartition)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t argumentChannels[0] = {};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    int64_t expectData2[DATA_SIZE] = {3, 0, 4, 1, 5, 2};
    double expectData3[DATA_SIZE] = {3.3, 6.6, 2.2, 5.5, 1.1, 4.4};
    int16_t expectData4[DATA_SIZE] = {2, 5, 1, 4, 0, 3};
    int64_t expectData5[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4, expectData5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillRowNumber)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    int32_t outputCols[3] = {2, 1, 3};
    int32_t sortCols[0] = {};
    int32_t ascendings[0] = {};
    int32_t nullFirsts[0] = {};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[1] = {2};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t argumentChannels[0] = {};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 0,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *test = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));
    WindowOperator *windowOperator = test;
    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ DoubleType(), LongType(), ShortType(), LongType() }));
    double expectData1[DATA_SIZE] = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
    int64_t expectData2[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    int16_t expectData3[DATA_SIZE] = {0, 1, 2, 3, 4, 5};
    int64_t expectData4[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithRankPartition)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_RANK};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t argumentChannels[0] = {};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 1, 5, 2};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 5.5, 1.1, 4.4};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 4, 0, 3};
    int64_t expectData5[DATA_SIZE] = {1, 1, 1, 2, 1, 2};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4, expectData5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithRank)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    int32_t outputCols[4] = {1, 2, 0, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[1] = {OMNI_WINDOW_TYPE_RANK};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};
    int32_t partitionCols[0] = {};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType() }));
    int32_t argumentChannels[0] = {};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 1, partitionCols, 0, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ LongType(), DoubleType(), IntType(), ShortType(), LongType() }));
    int64_t expectData1[DATA_SIZE] = {8, 8, 5, 4, 2, 1};
    double expectData2[DATA_SIZE] = {6.6, 3.3, 1.1, 2.2, 4.4, 5.5};
    int32_t expectData3[DATA_SIZE] = {0, 0, 2, 1, 2, 1};
    int16_t expectData4[DATA_SIZE] = {5, 2, 0, 1, 3, 4};
    int64_t expectData5[DATA_SIZE] = {1, 1, 3, 4, 5, 6};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3, expectData4, expectData5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithAggregationPartitionWithNull)
{
    // construct input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    vecBatch->Get(0)->SetNull(1);
    vecBatch->Get(0)->SetNull(5);

    vecBatch->Get(1)->SetNull(3);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[5] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MIN};
    int32_t windowFrameTypes[5] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[5] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[5] = {-1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[5] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[5] = {-1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(),
        LongType(), DoubleType(), DoubleType(), LongType() }));

    int32_t argumentChannels[5] = {1, 1, 1, 2, 1};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 5, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 5, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(),
        LongType(), DoubleType(), DoubleType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 1, 1};
    int64_t expectData2[DATA_SIZE] = {8, 4, 4, 2, 5, 1};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 1.1, 5.5};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 3, 0, 4};
    int64_t expectData5[DATA_SIZE] = {8, 8, 4, 2, 5, 6};
    int64_t expectData6[DATA_SIZE] = {1, 1, 1, 1, 1, 2};
    double expectData7[DATA_SIZE] = {8, 8, 4, 2, 5, 3};
    double expectData8[DATA_SIZE] = {6.6, 6.6, 2.2, 4.4, 1.1, 5.5};
    int64_t expectData9[DATA_SIZE] = {8, 8, 4, 2, 5, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6, expectData7, expectData8, expectData9);
    expectVecBatch->Get(0)->SetNull(4);
    expectVecBatch->Get(0)->SetNull(5);
    expectVecBatch->Get(1)->SetNull(1);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithRowNumberAndRankPartition)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[2] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[2] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[2] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[2] = {-1, -1};
    int32_t windowFrameEndTypes[2] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[2] = {-1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t argumentChannels[2] = {-1, -1};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 1, 2, 2};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 1, 5, 2};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 5.5, 1.1, 4.4};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 4, 0, 3};
    int64_t expectData5[DATA_SIZE] = {1, 1, 1, 2, 1, 2};
    int64_t expectData6[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithRowNumberAndRankPartitionWithNull)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);
    vecBatch->Get(0)->SetNull(1);
    vecBatch->Get(0)->SetNull(5);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[2] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[2] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[2] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[2] = {-1, -1};
    int32_t windowFrameEndTypes[2] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[2] = {-1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t argumentChannels[2] = {-1, -1};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 2, 1};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 2, 5, 1};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 1.1, 5.5};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 3, 0, 4};
    int64_t expectData5[DATA_SIZE] = {1, 1, 1, 1, 1, 2};
    int64_t expectData6[DATA_SIZE] = {1, 2, 1, 1, 1, 2};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);
    expectVecBatch->Get(0)->SetNull(4);
    expectVecBatch->Get(0)->SetNull(5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithRowNumberAndRankPartitionWithNullWithoutSort)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);
    vecBatch->Get(0)->SetNull(1);
    vecBatch->Get(0)->SetNull(5);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[0] = {};
    int32_t ascendings[0] = {};
    int32_t nullFirsts[0] = {};
    int32_t windowFunctionTypes[2] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[2] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[2] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[2] = {-1, -1};
    int32_t windowFrameEndTypes[2] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[2] = {-1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t argumentChannels[2] = {-1, -1};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 0,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 2, 1};
    int64_t expectData2[DATA_SIZE] = {8, 8, 4, 2, 5, 1};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 1.1, 5.5};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 3, 0, 4};
    int64_t expectData5[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData6[DATA_SIZE] = {1, 2, 1, 1, 1, 2};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);
    expectVecBatch->Get(0)->SetNull(4);
    expectVecBatch->Get(0)->SetNull(5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithAggregationPartitionWithNullWithoutSort)
{
    // construct input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType() }));
    int32_t data0[DATA_SIZE] = {0, 1, 2, 0, 1, 2};
    int64_t data1[DATA_SIZE] = {8, 1, 2, 8, 4, 5};
    double data2[DATA_SIZE] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int16_t data3[DATA_SIZE] = {5, 4, 3, 2, 1, 0};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3);

    vecBatch->Get(0)->SetNull(1);
    vecBatch->Get(0)->SetNull(5);
    vecBatch->Get(1)->SetNull(3);

    int32_t outputCols[4] = {0, 1, 2, 3};
    int32_t sortCols[0] = {};
    int32_t ascendings[0] = {};
    int32_t nullFirsts[0] = {};
    int32_t windowFunctionTypes[5] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MIN};
    int32_t windowFrameTypes[5] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[5] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[5] = {-1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[5] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[5] = {-1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(),
        LongType(), DoubleType(), DoubleType(), LongType() }));

    int32_t argumentChannels[5] = {1, 1, 1, 2, 1};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        4, windowFunctionTypes, 5, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 0,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 5, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);

    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), ShortType(), LongType(),
        LongType(), DoubleType(), DoubleType(), LongType() }));
    int32_t expectData1[DATA_SIZE] = {0, 0, 1, 2, 1, 1};
    int64_t expectData2[DATA_SIZE] = {8, 4, 4, 2, 5, 1};
    double expectData3[DATA_SIZE] = {6.6, 3.3, 2.2, 4.4, 1.1, 5.5};
    int16_t expectData4[DATA_SIZE] = {5, 2, 1, 3, 0, 4};
    int64_t expectData5[DATA_SIZE] = {8, 8, 4, 2, 6, 6};
    int64_t expectData6[DATA_SIZE] = {1, 1, 1, 1, 2, 2};
    double expectData7[DATA_SIZE] = {8, 8, 4, 2, 3, 3};
    double expectData8[DATA_SIZE] = {6.6, 6.6, 2.2, 4.4, 5.5, 5.5};
    int64_t expectData9[DATA_SIZE] = {8, 8, 4, 2, 1, 1};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6, expectData7, expectData8, expectData9);
    expectVecBatch->Get(0)->SetNull(4);
    expectVecBatch->Get(0)->SetNull(5);
    expectVecBatch->Get(1)->SetNull(1);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithRankWithAllDataTypes)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[colCount] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK,
        OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK,
        OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_RANK};
    int32_t windowFrameTypes[colCount] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[colCount] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[colCount] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[colCount] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[colCount] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t argumentChannels[0] = {};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, colCount, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts,
        1, preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t expectData3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t expectData4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double expectData5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int64_t expectData11[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData12[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData13[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData14[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData15[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData16[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData17[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData18[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData19[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData20[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData21[DATA_SIZE] = {1, 1, 1, 1, 1, 1};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18,
        expectData19, expectData20, expectData21);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithRowNumberkWithAllDataTypes)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[colCount] = {OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER,
        OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER,
        OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER,
        OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[colCount] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                      OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[colCount] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                               OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[colCount] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[colCount] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                         OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[colCount] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t argumentChannels[0] = {};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, colCount, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts,
        1, preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 0, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t expectData3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t expectData4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double expectData5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {111111, 111111, 222222, 222222, 333333, 333333};
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int64_t expectData11[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData12[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData13[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData14[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData15[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData16[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData17[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData18[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData19[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData20[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData21[DATA_SIZE] = {1, 2, 1, 2, 1, 2};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18,
        expectData19, expectData20, expectData21);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));
    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithSumWithAllDataTypes)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);

    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(5, 0), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t data2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t data3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t data4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double data5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(2, 2), Decimal128(2, 2),
        Decimal128(3, 3), Decimal128(3, 3)};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[8] = {OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM};
    int32_t windowFrameTypes[8] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[8] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[8] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY), Date32Type(omniruntime::type::MILLI),
        LongType(), Decimal64Type(5, 0), DoubleType(), BooleanType(), VarcharType(3), Decimal128Type(2, 2), CharType(3),
        ShortType(), IntType(), Date32Type(omniruntime::type::DAY), Date32Type(omniruntime::type::MILLI), LongType(),
        Decimal128Type(10, 0), DoubleType(), Decimal128Type(2, 2), ShortType() }));
    int32_t argumentChannels[8] = {0, 1, 2, 3, 4, 5, 8, 10};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 8, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 8, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY), Date32Type(omniruntime::type::MILLI),
        LongType(), Decimal64Type(5, 0), DoubleType(), BooleanType(), VarcharType(3), Decimal128Type(2, 2), CharType(3),
        ShortType(), IntType(), Date32Type(omniruntime::type::DAY), Date32Type(omniruntime::type::MILLI), LongType(),
        Decimal128Type(10, 0), DoubleType(), Decimal128Type(2, 2), ShortType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData2[DATA_SIZE] = {111, 111, 222, 222, 333, 333};
    int64_t expectData3[DATA_SIZE] = {1111, 1111, 2222, 2222, 3333, 3333};
    int64_t expectData4[DATA_SIZE] = {11111, 11111, 22222, 22222, 33333, 33333};
    double expectData5[DATA_SIZE] = {1.1, 1.1, 2.2, 2.2, 3.3, 3.3};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(2, 2), Decimal128(2, 2),
        Decimal128(3, 3), Decimal128(3, 3)};
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData11[DATA_SIZE] = {2, 2, 4, 4, 6, 6};
    int32_t expectData12[DATA_SIZE] = {22, 22, 44, 44, 66, 66};
    int32_t expectData13[DATA_SIZE] = {222, 222, 444, 444, 666, 666};
    int64_t expectData14[DATA_SIZE] = {2222, 2222, 4444, 4444, 6666, 6666};
    Decimal128 expectData15[DATA_SIZE] = {Decimal128(22222), Decimal128(22222), Decimal128(44444), Decimal128(44444),
        Decimal128(66666), Decimal128(66666)};
    double expectData16[DATA_SIZE] = {2.2, 2.2, 4.4, 4.4, 6.6, 6.6};
    Decimal128 expectData17[DATA_SIZE] = {Decimal128(2, 2), Decimal128(2, 2), Decimal128(4, 4), Decimal128(4, 4),
        Decimal128(6, 6), Decimal128(6, 6)};
    int16_t expectData18[DATA_SIZE] = {22, 22, 44, 44, 66, 66};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithAvgWithAllDataTypes)
{
    ConfigUtil::SetSupportContainerVecRule(SupportContainerVecRule::SUPPORT);

    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(5, 2), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0),
        Decimal128(0, 0), Decimal128(0, 0)};
    std::string data9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t data10[DATA_SIZE] = {11, 33, 33, 55, 55, 77};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[8] = {OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG,
        OMNI_AGGREGATION_TYPE_AVG};
    int32_t windowFrameTypes[8] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[8] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[8] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(5, 2), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType(), DoubleType(), DoubleType(), DoubleType(),
        DoubleType(), Decimal64Type(10, 4), DoubleType(), Decimal128Type(4, 4), DoubleType() }));
    int32_t argumentChannels[8] = {0, 1, 2, 3, 4, 5, 8, 10};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 8, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 8, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(5, 2), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType(), DoubleType(), DoubleType(), DoubleType(),
        DoubleType(), Decimal64Type(10, 4), DoubleType(), Decimal128Type(4, 4), DoubleType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s1", "s2", "s2", "s3", "s3"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0),
        Decimal128(0, 0), Decimal128(0, 0)};
    std::string expectData9[DATA_SIZE] = {"c1", "c1", "c2", "c2", "c3", "c3"};
    int16_t expectData10[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    double expectData11[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    double expectData12[DATA_SIZE] = {22, 22, 44, 44, 66, 66};
    double expectData13[DATA_SIZE] = {222, 222, 444, 444, 666, 666};
    double expectData14[DATA_SIZE] = {2222, 2222, 4444, 4444, 6666, 6666};
    int64_t expectData15[DATA_SIZE] = {2222200, 2222200, 4444400, 4444400, 6666600, 6666600};
    double expectData16[DATA_SIZE] = {2.2, 2.2, 4.4, 4.4, 6.6, 6.6};
    Decimal128 expectData17[DATA_SIZE] = {Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0), Decimal128(0, 0),
                                          Decimal128(0, 0), Decimal128(0, 0)};
    double expectData18[DATA_SIZE] = {22, 22, 44, 44, 66, 66};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17, expectData18);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithMaxWithAllDataTypes)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string data9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[10] = {OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX};
    int32_t windowFrameTypes[10] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[10] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[10] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType() }));
    int32_t argumentChannels[10] = {0, 1, 2, 3, 4, 5, 7, 8, 9, 10};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 10, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 10, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string expectData9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData11[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData12[DATA_SIZE] = {33, 33, 55, 55, 77, 77};
    int32_t expectData13[DATA_SIZE] = {333, 333, 555, 555, 777, 777};
    int64_t expectData14[DATA_SIZE] = {3333, 3333, 5555, 5555, 7777, 7777};
    int64_t expectData15[DATA_SIZE] = {33333, 33333, 55555, 55555, 77777, 77777};
    double expectData16[DATA_SIZE] = {3.3, 3.3, 5.5, 5.5, 7.7, 7.7};
    std::string expectData17[DATA_SIZE] = {"s3", "s3", "s5", "s5", "s7", "s7"};
    Decimal128 expectData18[DATA_SIZE] = {Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5), Decimal128(5, 5),
        Decimal128(7, 7), Decimal128(7, 7)};
    std::string expectData19[DATA_SIZE] = {"c3", "c3", "c5", "c5", "c7", "c7"};
    int16_t expectData20[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2, expectData3, expectData4,
        expectData5, expectData6, expectData7, expectData8, expectData9, expectData10, expectData11, expectData12,
        expectData13, expectData14, expectData15, expectData16, expectData17, expectData18, expectData19, expectData20);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithMinWithAllDataTypes)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string data9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[10] = {OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
        OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN};
    int32_t windowFrameTypes[10] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[10] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[10] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType() }));
    int32_t argumentChannels[10] = {0, 1, 2, 3, 4, 5, 7, 8, 9, 10};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 10, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 10, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string expectData9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int32_t expectData11[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData12[DATA_SIZE] = {11, 11, 33, 33, 55, 55};
    int32_t expectData13[DATA_SIZE] = {111, 111, 333, 333, 555, 555};
    int64_t expectData14[DATA_SIZE] = {1111, 1111, 3333, 3333, 5555, 5555};
    int64_t expectData15[DATA_SIZE] = {11111, 11111, 33333, 33333, 55555, 55555};
    double expectData16[DATA_SIZE] = {1.1, 1.1, 3.3, 3.3, 5.5, 5.5};
    std::string expectData17[DATA_SIZE] = {"s1", "s1", "s3", "s3", "s5", "s5"};
    Decimal128 expectData18[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3),
        Decimal128(5, 5), Decimal128(5, 5)};
    std::string expectData19[DATA_SIZE] = {"c1", "c1", "c3", "c3", "c5", "c5"};
    int16_t expectData20[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2, expectData3, expectData4,
        expectData5, expectData6, expectData7, expectData8, expectData9, expectData10, expectData11, expectData12,
        expectData13, expectData14, expectData15, expectData16, expectData17, expectData18, expectData19, expectData20);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithCountWithAllDataTypes)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), CharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string data9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t data10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5, data6,
        data7, data8, data9, data10);

    const int32_t colCount = 11;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[10] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
    int32_t windowFrameTypes[10] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
        OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[10] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[10] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t argumentChannels[10] = {0, 1, 2, 3, 4, 5, 7, 8, 9, 10};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 10, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 10, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(),
        Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI),
        LongType(),
        Decimal64Type(1, 1),
        DoubleType(),
        BooleanType(),
        VarcharType(3),
        Decimal128Type(2, 2),
        CharType(3),
        ShortType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType(),
        LongType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    std::string expectData9[DATA_SIZE] = {"c1", "c3", "c3", "c5", "c5", "c7"};
    int16_t expectData10[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int64_t expectData11[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData12[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData13[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData14[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData15[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData16[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData17[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData18[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData19[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData20[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2, expectData3, expectData4,
        expectData5, expectData6, expectData7, expectData8, expectData9, expectData10, expectData11, expectData12,
        expectData13, expectData14, expectData15, expectData16, expectData17, expectData18, expectData19, expectData20);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithCountRowsWithNullWithSort)
{
    // construct the input data
    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int64_t data1[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data2[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data3[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data4[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    int16_t data5[DATA_SIZE] = {11, 11, 22, 22, 33, 33};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4, data5);

    vecBatch->Get(1)->SetNull(1);
    vecBatch->Get(1)->SetNull(2);
    vecBatch->Get(2)->SetNull(2);
    vecBatch->Get(2)->SetNull(3);
    vecBatch->Get(2)->SetNull(4);
    vecBatch->Get(3)->SetNull(0);
    vecBatch->Get(4)->SetNull(1);
    vecBatch->Get(4)->SetNull(5);
    vecBatch->Get(5)->SetNull(1);
    vecBatch->Get(5)->SetNull(2);

    const int32_t colCount = 6;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4, 5 };
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[11] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
    int32_t windowFrameTypes[11] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[11] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[11] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[11] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[11] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3),
        ShortType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(),
        LongType(), LongType(), LongType() }));
    int32_t argumentChannels[11] = {0, -1, 1, -1, 2, -1, 3, -1, 4, -1, 5};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 11, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 11, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3),
        ShortType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(),
        LongType(), LongType(), LongType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int64_t expectData1[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData2[DATA_SIZE] = {1.1, 3.3, 5.5, 5.5, 5.5, 7.7};
    bool expectData3[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData4[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    int16_t expectData5[DATA_SIZE] = {11, 11, 22, 22, 33, 33};
    int64_t expectData6[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData7[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData8[DATA_SIZE] = {1, 1, 1, 1, 2, 2};
    int64_t expectData9[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData10[DATA_SIZE] = {2, 2, 0, 0, 1, 1};
    int64_t expectData11[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData12[DATA_SIZE] = {1, 1, 2, 2, 2, 2};
    int64_t expectData13[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData14[DATA_SIZE] = {1, 1, 2, 2, 1, 1};
    int64_t expectData15[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData16[DATA_SIZE] = {1, 1, 1, 1, 2, 2};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16);

    expectVecBatch->Get(1)->SetNull(1);
    expectVecBatch->Get(1)->SetNull(2);
    expectVecBatch->Get(2)->SetNull(2);
    expectVecBatch->Get(2)->SetNull(3);
    expectVecBatch->Get(2)->SetNull(4);
    expectVecBatch->Get(3)->SetNull(0);
    expectVecBatch->Get(4)->SetNull(1);
    expectVecBatch->Get(4)->SetNull(5);
    expectVecBatch->Get(5)->SetNull(1);
    expectVecBatch->Get(5)->SetNull(2);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithCountRowsWithNullWithoutSort)
{
    // construct the input data
    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3) }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int64_t data1[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data2[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data3[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data4[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};

    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, DATA_SIZE, data0, data1, data2, data3, data4);

    vecBatch->Get(1)->SetNull(1);
    vecBatch->Get(1)->SetNull(2);
    vecBatch->Get(2)->SetNull(2);
    vecBatch->Get(2)->SetNull(3);
    vecBatch->Get(2)->SetNull(4);
    vecBatch->Get(3)->SetNull(0);
    vecBatch->Get(4)->SetNull(1);
    vecBatch->Get(4)->SetNull(5);

    const int32_t colCount = 5;
    int32_t outputCols[colCount] = {0, 1, 2, 3, 4 };
    int32_t sortCols[1] = {};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[10] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
        OMNI_AGGREGATION_TYPE_COUNT_ALL, OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL};
    int32_t windowFrameTypes[10] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[10] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                     OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[10] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                   OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[10] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3), LongType(),
        LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType() }));
    int32_t argumentChannels[10] = {0, -1, 1, -1, 2, -1, 3, -1, 4, -1};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        colCount, windowFunctionTypes, 10, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 10, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ IntType(), LongType(), DoubleType(), BooleanType(), VarcharType(3), LongType(),
        LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType(), LongType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int64_t expectData1[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData2[DATA_SIZE] = {1.1, 3.3, 5.5, 5.5, 5.5, 7.7};
    bool expectData3[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData4[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    int64_t expectData5[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData6[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData7[DATA_SIZE] = {1, 1, 1, 1, 2, 2};
    int64_t expectData8[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData9[DATA_SIZE] = {2, 2, 0, 0, 1, 1};
    int64_t expectData10[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData11[DATA_SIZE] = {1, 1, 2, 2, 2, 2};
    int64_t expectData12[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    int64_t expectData13[DATA_SIZE] = {1, 1, 2, 2, 1, 1};
    int64_t expectData14[DATA_SIZE] = {2, 2, 2, 2, 2, 2};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14);

    expectVecBatch->Get(1)->SetNull(1);
    expectVecBatch->Get(1)->SetNull(2);
    expectVecBatch->Get(2)->SetNull(2);
    expectVecBatch->Get(2)->SetNull(3);
    expectVecBatch->Get(2)->SetNull(4);
    expectVecBatch->Get(3)->SetNull(0);
    expectVecBatch->Get(4)->SetNull(1);
    expectVecBatch->Get(4)->SetNull(5);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));
    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithDictionaryVector)
{
    // construct the input data
    DataTypes sourceTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), ShortType() }));
    int32_t data0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t data1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t data2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t data3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t data4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double data5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool data6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string data7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 data8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    int16_t data9[DATA_SIZE] = {11, 33, 33, 55, 55, 77};

    int32_t ids[] = {0, 1, 2, 3, 4, 5};
    auto *vecBatch = new vec::VectorBatch(DATA_SIZE);
    void *datas[10] = {data0, data1, data2, data3, data4, data5, data6, data7, data8, data9};
    for (int32_t i = 0; i < sourceTypes.GetSize(); i++) {
        DataTypePtr dataType = sourceTypes.GetType(i);
        vecBatch->Append(CreateDictionaryVector(*dataType, DATA_SIZE, ids, DATA_SIZE, datas[i]));
    }

    int32_t outputCols[10] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int32_t sortCols[1] = {0};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t windowFunctionTypes[8] = {OMNI_WINDOW_TYPE_RANK, OMNI_WINDOW_TYPE_ROW_NUMBER, OMNI_AGGREGATION_TYPE_SUM,
        OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_MAX,
        OMNI_AGGREGATION_TYPE_MIN, OMNI_WINDOW_TYPE_ROW_NUMBER};
    int32_t windowFrameTypes[8] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                               OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[8] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                    OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[8] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                  OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[8] = {-1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), ShortType(), LongType(), LongType(), LongType(), LongType(), DoubleType(),
        VarcharType(3), Decimal128Type(2, 2), ShortType() }));
    int32_t argumentChannels[8] = {0, 1, 3, 4, 5, 7, 8, 9};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        10, windowFunctionTypes, 8, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 8, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ IntType(), Date32Type(omniruntime::type::DAY),
        Date32Type(omniruntime::type::MILLI), LongType(), Decimal64Type(1, 1), DoubleType(), BooleanType(),
        VarcharType(3), Decimal128Type(2, 2), ShortType(), LongType(), LongType(), LongType(), LongType(), DoubleType(),
        VarcharType(3), Decimal128Type(2, 2), ShortType() }));
    int32_t expectData0[DATA_SIZE] = {1, 1, 2, 2, 3, 3};
    int32_t expectData1[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int32_t expectData2[DATA_SIZE] = {111, 333, 333, 555, 555, 777};
    int64_t expectData3[DATA_SIZE] = {1111, 3333, 3333, 5555, 5555, 7777};
    int64_t expectData4[DATA_SIZE] = {11111, 33333, 33333, 55555, 55555, 77777};
    double expectData5[DATA_SIZE] = {1.1, 3.3, 3.3, 5.5, 5.5, 7.7};
    bool expectData6[DATA_SIZE] = {false, false, true, true, false, false};
    std::string expectData7[DATA_SIZE] = {"s1", "s3", "s3", "s5", "s5", "s7"};
    Decimal128 expectData8[DATA_SIZE] = {Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3), Decimal128(5, 5),
        Decimal128(5, 5), Decimal128(7, 7)};
    int16_t expectData9[DATA_SIZE] = {11, 33, 33, 55, 55, 77};
    int64_t expectData10[DATA_SIZE] = {1, 1, 1, 1, 1, 1};
    int64_t expectData11[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    int64_t expectData12[DATA_SIZE] = {4444, 4444, 8888, 8888, 13332, 13332};
    int64_t expectData13[DATA_SIZE] = {2, 2, 2, 2, 2, 2};
    double expectData14[DATA_SIZE] = {2.2, 2.2, 4.4, 4.4, 6.6, 6.6};
    std::string expectData15[DATA_SIZE] = {"s3", "s3", "s5", "s5", "s7", "s7"};
    Decimal128 expectData16[DATA_SIZE] = {Decimal128(1, 1), Decimal128(1, 1), Decimal128(3, 3), Decimal128(3, 3),
        Decimal128(5, 5), Decimal128(5, 5)};
    int16_t expectData17[DATA_SIZE] = {1, 2, 1, 2, 1, 2};
    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, DATA_SIZE, expectData0, expectData1, expectData2,
        expectData3, expectData4, expectData5, expectData6, expectData7, expectData8, expectData9, expectData10,
        expectData11, expectData12, expectData13, expectData14, expectData15, expectData16, expectData17);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithWindowPerf)
{
    DataTypes sourceTypes(std::vector<DataTypePtr>(
        { IntType(),    LongType(),           DoubleType(), VarcharType(20),       CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),           DoubleType(), VarcharType(20),       CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),           DoubleType(), VarcharType(20),       CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),           DoubleType(), Decimal128Type(22, 5), IntType(),    LongType(),
        DoubleType(), Decimal128Type(22, 5) }));
    int32_t outputCols[26] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
                              14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};
    int32_t windowFunctionTypes[26] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
                                       OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
                                       OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_COLUMN,
                                       OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
                                       OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
                                       OMNI_AGGREGATION_TYPE_MIN, OMNI_AGGREGATION_TYPE_MIN,
                                       OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
                                       OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
                                       OMNI_AGGREGATION_TYPE_MAX, OMNI_AGGREGATION_TYPE_MAX,
                                       OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
                                       OMNI_AGGREGATION_TYPE_SUM, OMNI_AGGREGATION_TYPE_SUM,
                                       OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG,
                                       OMNI_AGGREGATION_TYPE_AVG, OMNI_AGGREGATION_TYPE_AVG};
    int32_t windowFrameTypes[26] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE,
                                    OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[26] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING,
                                         OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[26] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  -1, -1,
                                            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t windowFrameEndTypes[26] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW,
                                       OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[26] = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  -1,
                                          -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;
    DataTypes allTypes(std::vector<DataTypePtr>({ IntType(),    LongType(),
        DoubleType(), VarcharType(20),
        CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), VarcharType(20),
        CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), VarcharType(20),
        CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), Decimal128Type(22, 5),
        LongType(),   LongType(),
        LongType(),   LongType(),
        LongType(),   LongType(),
        IntType(),    LongType(),
        DoubleType(), VarcharType(20),
        CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), VarcharType(20),
        CharType(20), Decimal128Type(22, 5),
        IntType(),    LongType(),
        DoubleType(), Decimal128Type(22, 5),
        DoubleType(), DoubleType(),
        DoubleType(), Decimal128Type(22, 5) }));
    int32_t argumentChannels[26] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                                    13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25};

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        26, windowFunctionTypes, 26, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 26, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    operatorFactory->Init();
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));
    VectorBatch **input = BuildWindowInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, 26, sourceTypes.Get());

    Timer timer;
    timer.SetStart();

    ASSERT(!(input == nullptr));

    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        auto errNo = windowOperator->AddInput(input[pageIndex]);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }

    VectorBatch *result = nullptr;
    while (windowOperator->GetStatus() == OMNI_STATUS_NORMAL) {
        windowOperator->GetOutput(&result);
    }

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse();
    double cpuElapsed = timer.GetCpuElapse();
    std::cout << "Window with Omni, wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;

    delete[] input;
    VectorHelper::FreeVecBatch(result);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithWindowComparePerf)
{
    DataTypes sourceTypes(std::vector<DataTypePtr>({ LongType(), LongType() }));
    int32_t outputCols[2] = {0, 1};
    int32_t windowFunctionTypes[2] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN, OMNI_AGGREGATION_TYPE_COUNT_ALL};
    int32_t windowFrameTypes[2] = {OMNI_FRAME_TYPE_RANGE, OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[2] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING, OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[2] = {-1, -1};
    int32_t windowFrameEndTypes[2] = {OMNI_FRAME_BOUND_CURRENT_ROW, OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[2] = {-1, -1};
    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {false};
    int32_t nullFirsts[1] = {false};
    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;
    DataTypes allTypes(std::vector<DataTypePtr>({ LongType(), LongType(), LongType(), LongType() }));
    int32_t argumentChannels[2] = {1, -1};

    // dealing data with the operator
    WindowOperatorFactory *operatorFactoryWithJit = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes,
        outputCols, 2, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 2, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels);
    operatorFactoryWithJit->Init();
    WindowOperator *windowOperatorWithJit = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactoryWithJit));
    VectorBatch **input1 = BuildWindowInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, 2, sourceTypes.Get());

    Timer timer;
    timer.SetStart();

    if (input1 == nullptr) {
        std::cerr << "Building input1 data failed!" << std::endl;
    }

    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        auto errNo = windowOperatorWithJit->AddInput(input1[pageIndex]);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }

    auto *perfUtil = new PerfUtil();
    perfUtil->Init();
    perfUtil->Reset();
    perfUtil->Start();
    perfUtil->Stop();
    long instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("Window with OmniJit, used %lld instructions\n", perfUtil->GetData());
    }

    std::vector<VectorBatch *> resultWithJit;
    while (windowOperatorWithJit->GetStatus() != OMNI_STATUS_FINISHED) {
        VectorBatch *outputVecBatchWithJit = nullptr;
        windowOperatorWithJit->GetOutput(&outputVecBatchWithJit);
        resultWithJit.push_back(outputVecBatchWithJit);
    }

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse();
    double cpuElapsed = timer.GetCpuElapse();
    std::cout << "Window with OmniJit, wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    WindowOperatorFactory *operatorFactoryWithoutJit = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes,
        outputCols, 2, windowFunctionTypes, 2, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 2, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    operatorFactoryWithoutJit->Init();
    std::cout << "after create factory" << std::endl;
    WindowOperator *windowOperatorWithoutJit =
        dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactoryWithoutJit));
    VectorBatch **input2 = BuildWindowInput(VEC_BATCH_NUM, ROW_PER_VEC_BATCH, 2, sourceTypes.Get());

    timer.Reset();

    if (input2 == nullptr) {
        std::cerr << "Building input2 data failed!" << std::endl;
    }

    for (int pageIndex = 0; pageIndex < VEC_BATCH_NUM; ++pageIndex) {
        auto errNo = windowOperatorWithoutJit->AddInput(input2[pageIndex]);
        EXPECT_EQ(errNo, OMNI_STATUS_NORMAL);
    }

    perfUtil->Reset();
    perfUtil->Start();
    perfUtil->Stop();
    instCount = perfUtil->GetData();
    if (instCount != -1) {
        printf("Window without OmniJit, used %lld instructions\n", perfUtil->GetData());
    }

    std::vector<VectorBatch *> resultWithoutJit;
    while (windowOperatorWithoutJit->GetStatus() != OMNI_STATUS_FINISHED) {
        VectorBatch *outputVecBatchWithoutJit = nullptr;
        windowOperatorWithoutJit->GetOutput(&outputVecBatchWithoutJit);
        resultWithoutJit.push_back(outputVecBatchWithoutJit);
    }

    delete perfUtil;

    timer.CalculateElapse();
    wallElapsed = timer.GetWallElapse();
    cpuElapsed = timer.GetCpuElapse();

    std::cout << "Window without OmniJit, wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;

    op::Operator::DeleteOperator(windowOperatorWithJit);
    op::Operator::DeleteOperator(windowOperatorWithoutJit);
    delete operatorFactoryWithJit;
    delete operatorFactoryWithoutJit;

    EXPECT_EQ(resultWithJit.size(), resultWithoutJit.size());
    for (uint32_t i = 0; i < resultWithJit.size(); ++i) {
        EXPECT_TRUE(VecBatchMatchIgnoreOrder(resultWithJit[i], resultWithoutJit[i]));
    }

    delete[] input1;
    delete[] input2;
    VectorHelper::FreeVecBatches(resultWithJit);
    VectorHelper::FreeVecBatches(resultWithoutJit);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithFrameBound)
{
    // construct the input data
    const int myDataSize = DATA_SIZE + DATA_SIZE;
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType() }));
    std::string data0[myDataSize] = {"banana", "apple", "banana", "apple", "banana", "banana", "banana", "banana",
                                     "apple", "orange", "banana", "apple"};
    std::string data1[myDataSize] = {"2020-11-01", "2020-12-01", "2020-10-01", "2020-11-01",
                                     "2020-12-01", "2020-12-02", "2020-12-07", "2020-12-04", "2021-01-01", "2021-01-01",
                                     "2021-01-01", "2021-02-01"};
    int64_t data2[myDataSize] = {7400, 8000, 7800, 7000, 7500, 6500, 4500, 8500, 9000, 8000, 8500, 9500};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, myDataSize, data0, data1, data2);

    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t argumentChannels[1] = {2};
    int32_t outputCols[3] = {0, 1, 2};
    int32_t windowFunctionTypes[1] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_RANGE};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_CURRENT_ROW};
    int32_t windowFrameEndChannels[1] = {-1};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), LongType() }));

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), LongType() }));
    std::string expectData1[myDataSize] = {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana",
                                           "banana", "banana", "banana", "orange"};
    std::string expectData2[myDataSize] = {"2020-11-01", "2020-12-01", "2021-01-01", "2021-02-01", "2020-10-01",
                                           "2020-11-01", "2020-12-01", "2020-12-02", "2020-12-04", "2020-12-07",
                                           "2021-01-01", "2021-01-01"};
    int64_t expectData3[myDataSize] = {7000, 8000, 9000, 9500, 7800, 7400, 7500, 6500, 8500, 4500, 8500, 8000};
    int64_t expectData4[myDataSize] = {1, 2, 3, 4, 1, 2, 3, 4, 5, 6, 7, 1};

    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, myDataSize, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithFrameBoundedN)
{
    // construct the input data
    const int myDataSize = DATA_SIZE + DATA_SIZE;
    DataTypes sourceTypes(
        std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), IntType(), IntType() }));
    std::string data0[myDataSize] = {"banana", "apple", "banana", "apple", "banana", "banana", "banana", "banana",
                                     "apple", "orange", "banana", "apple"};
    std::string data1[myDataSize] = {"2020-11-01", "2020-12-01", "2020-10-01", "2020-11-01", "2020-12-01",
                                     "2020-12-02", "2020-12-07", "2020-12-04", "2021-01-01", "2021-01-01",
                                     "2021-01-01", "2021-02-01"};
    int64_t data2[myDataSize] = {7400, 8000, 7800, 7000, 7500, 6500, 4500, 8500, 9000, 8000, 8500, 9500};
    int32_t data3[myDataSize] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; // frame start col
    int32_t data4[myDataSize] = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // frane end col
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, myDataSize, data0, data1, data2, data3, data4);

    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t argumentChannels[1] = {2};
    int32_t outputCols[5] = {0, 1, 2, 3, 4};
    int32_t windowFunctionTypes[1] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_PRECEDING};
    int32_t windowFrameStartChannels[1] = {3};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_FOLLOWING};
    int32_t windowFrameEndChannels[1] = {4};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(
        std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), IntType(), IntType(), LongType() }));

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        5, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(
        std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), IntType(), IntType(), LongType() }));
    std::string expectData1[myDataSize] = {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana",
                                           "banana", "banana", "banana", "orange"};
    std::string expectData2[myDataSize] = {"2020-11-01", "2020-12-01", "2021-01-01", "2021-02-01", "2020-10-01",
                                           "2020-11-01", "2020-12-01", "2020-12-02", "2020-12-04", "2020-12-07",
                                           "2021-01-01", "2021-01-01"};
    int64_t expectData3[myDataSize] = {7000, 8000, 9000, 9500, 7800, 7400, 7500, 6500, 8500, 4500, 8500, 8000};
    int32_t expectData4[myDataSize] = {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}; // frame start col
    int32_t expectData5[myDataSize] = {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}; // frane end col
    int64_t expectData6[myDataSize] = {3, 4, 3, 2, 3, 4, 4, 4, 4, 3, 2, 1};

    VectorBatch *expectVecBatch = CreateVectorBatch(expectTypes, myDataSize, expectData1, expectData2, expectData3,
        expectData4, expectData5, expectData6);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}

TEST(NativeOmniWindowOperatorTest, testWindowSpillWithFrameUnBounded)
{
    // construct the input data
    const int myDataSize = DATA_SIZE + DATA_SIZE;
    DataTypes sourceTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType() }));
    std::string data0[myDataSize] = {"banana", "apple", "banana", "apple", "banana", "banana", "banana", "banana",
                                     "apple", "orange", "banana", "apple"};
    std::string data1[myDataSize] = {"2020-11-01", "2020-12-01", "2020-10-01", "2020-11-01", "2020-12-01",
                                     "2020-12-02", "2020-12-07", "2020-12-04", "2021-01-01", "2021-01-01",
                                     "2021-01-01", "2021-02-01"};
    int64_t data2[myDataSize] = {7400, 8000, 7800, 7000, 7500, 6500, 4500, 8500, 9000, 8000, 8500, 9500};
    VectorBatch *vecBatch = CreateVectorBatch(sourceTypes, myDataSize, data0, data1, data2);

    int32_t partitionCols[1] = {0};
    int32_t preGroupedCols[0] = {};
    int32_t sortCols[1] = {1};
    int32_t ascendings[1] = {true};
    int32_t nullFirsts[1] = {false};
    int32_t argumentChannels[1] = {2};
    int32_t outputCols[5] = {0, 1, 2};
    int32_t windowFunctionTypes[1] = {OMNI_AGGREGATION_TYPE_COUNT_COLUMN};
    int32_t windowFrameTypes[1] = {OMNI_FRAME_TYPE_ROWS};
    int32_t windowFrameStartTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_PRECEDING};
    int32_t windowFrameStartChannels[1] = {-1};
    int32_t windowFrameEndTypes[1] = {OMNI_FRAME_BOUND_UNBOUNDED_FOLLOWING};
    int32_t windowFrameEndChannels[1] = {-1};

    int32_t preSortedChannelPrefix = 0;
    int32_t expectedPositions = 10000;

    DataTypes allTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), LongType() }));

    SparkSpillConfig spillConfig(GenerateSpillPath(), MAX_SPILL_BYTES, 5);
    OperatorConfig operatorConfig(spillConfig);
    // dealing data with the operator
    WindowOperatorFactory *operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(sourceTypes, outputCols,
        3, windowFunctionTypes, 1, partitionCols, 1, preGroupedCols, 0, sortCols, ascendings, nullFirsts, 1,
        preSortedChannelPrefix, expectedPositions, allTypes, argumentChannels, 1, windowFrameTypes,
        windowFrameStartTypes, windowFrameStartChannels, windowFrameEndTypes, windowFrameEndChannels, operatorConfig);
    WindowOperator *windowOperator = dynamic_cast<WindowOperator *>(CreateTestOperator(operatorFactory));

    windowOperator->AddInput(vecBatch);
    VectorBatch *outputVecBatch = nullptr;
    windowOperator->GetOutput(&outputVecBatch);

    // construct the output data
    DataTypes expectTypes(std::vector<DataTypePtr>({ VarcharType(20), VarcharType(20), LongType(), LongType() }));
    std::string expectData1[myDataSize] = {"apple", "apple", "apple", "apple", "banana", "banana", "banana", "banana",
                                           "banana", "banana", "banana", "orange"};
    std::string expectData2[myDataSize] = {"2020-11-01", "2020-12-01", "2021-01-01", "2021-02-01", "2020-10-01",
                                           "2020-11-01", "2020-12-01", "2020-12-02", "2020-12-04", "2020-12-07",
                                           "2021-01-01", "2021-01-01"};
    int64_t expectData3[myDataSize] = {7000, 8000, 9000, 9500, 7800, 7400, 7500, 6500, 8500, 4500, 8500, 8000};
    int64_t expectData4[myDataSize] = {4, 4, 4, 4, 7, 7, 7, 7, 7, 7, 7, 1};

    VectorBatch *expectVecBatch =
        CreateVectorBatch(expectTypes, myDataSize, expectData1, expectData2, expectData3, expectData4);

    EXPECT_TRUE(VecBatchMatchIgnoreOrder(outputVecBatch, expectVecBatch));

    omniruntime::op::Operator::DeleteOperator(windowOperator);
    delete operatorFactory;
    VectorHelper::FreeVecBatch(expectVecBatch);
    VectorHelper::FreeVecBatch(outputVecBatch);
}
}
