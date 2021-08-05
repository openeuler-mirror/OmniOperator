/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2018. All rights reserved.
 */
#include "gtest/gtest.h"
#include "../../src/operator/partitionedoutput/partitionedoutput.h"
#include "../util/test_util.h"
#include "../../src/vector/vector_common.h"
#include <vector>
#include <chrono>

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;

TEST (PartitionedOutputOperatorTest, TestOnePartitionedOutput) {
    const int32_t DATA_SIZE = 100;
    int32_t *data1 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data1[i] = i;
    }

    int32_t *data2 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data2[i] = i;
    }
    int32_t *data0 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data0[i] = i;
    }
    VectorBatch *vecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    IntVector *column1 = new IntVector(nullptr, DATA_SIZE);
    IntVector *column2 = new IntVector(nullptr, DATA_SIZE);
    column0->SetValues(0, data0, DATA_SIZE);
    column1->SetValues(0, data1, DATA_SIZE);
    column2->SetValues(0, data2, DATA_SIZE);
    vecBatch->SetVector(0, column1);
    vecBatch->SetVector(1, column2);
    vecBatch->SetVector(2, column0);

    int32_t sourceTypes[3] = {1, 1, 1};
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 1;
    int32_t bucketToPartition[1] = {0};
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
            PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(
                    sourceTypes, 3, replicatesAnyRow, nullChannel, partitionChannels, 1, partitionCount,
                    bucketToPartition, 1);
    PartitionedOutputOperator *partitionedOperator = (PartitionedOutputOperator *) partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);

    EXPECT_EQ(partitionedOperator->getVectorBatches().size(), 1);
    EXPECT_EQ(partitionedOperator->getVectorBatches()[0]->GetRowCount(), 100); // 111 row

    delete[]data0;
    delete[]data1;
    delete[]data2;
    delete partitionedOperator;
    delete partitionedOutputOperatorFactory;
    vecBatch->ReleaseAllVectors();
    delete vecBatch;
}

TEST (PartitionedOutputOperatorTest, TestMultiPartitionedOutput) {
    const int32_t DATA_SIZE = 101;
    int32_t *data1 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data1[i] = i;
    }

    int32_t *data2 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data2[i] = i;
    }
    int32_t *data0 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data0[i] = i;
    }
    VectorBatch *vecBatch = new VectorBatch(3);
    IntVector *column0 = new IntVector(nullptr, DATA_SIZE);
    IntVector *column1 = new IntVector(nullptr, DATA_SIZE);
    IntVector *column2 = new IntVector(nullptr, DATA_SIZE);
    column0->SetValues(0, data0, DATA_SIZE);
    column1->SetValues(0, data1, DATA_SIZE);
    column2->SetValues(0, data2, DATA_SIZE);
    vecBatch->SetVector(0, column1);
    vecBatch->SetVector(1, column2);
    vecBatch->SetVector(2, column0);

    int32_t sourceTypes[3] = {1, 1, 1};
    bool replicatesAnyRow = false;
    int32_t nullChannel = -1;
    int32_t partitionChannels[1] = {0};
    int32_t partitionCount = 2;
    int32_t bucketToPartition[2] = {0, 1};
    PartitionedOutputOperatorFactory *partitionedOutputOperatorFactory =
            PartitionedOutputOperatorFactory::CreatePartitionedOutputOperatorFactory(
                    sourceTypes, 3, replicatesAnyRow, nullChannel, partitionChannels, 1, partitionCount,
                    bucketToPartition,
                    2);
    PartitionedOutputOperator *partitionedOperator = (PartitionedOutputOperator *) partitionedOutputOperatorFactory->CreateOperator();
    partitionedOperator->AddInput(vecBatch);

    EXPECT_EQ(partitionedOperator->getVectorBatches().size(), 2);
    EXPECT_EQ(partitionedOperator->getVectorBatches()[0]->GetRowCount(), 51); // 111 row
    EXPECT_EQ(partitionedOperator->getVectorBatches()[1]->GetRowCount(), 50); // 111 row

    delete[]data0;
    delete[]data1;
    delete[]data2;
    delete partitionedOutputOperatorFactory;
    delete partitionedOperator;
    vecBatch->ReleaseAllVectors();
    delete vecBatch;
}