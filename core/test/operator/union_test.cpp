/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#include <vector>
#include "gtest/gtest.h"
#include "operator/union/union.h"
#include "vector/vector_helper.h"
#include "../util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;
using namespace TestUtil;

namespace UnionTest {
TEST(NativeOmniUnionOperator, TestUnionByTwoColum)
{
    // construct input data
    const int32_t dataSize = 6;
    // table1
    int32_t data1[dataSize] = {0, 1, 2, 0, 1, 2};
    double data2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    // table2
    int32_t data3[dataSize] = {10, 11, 12, 10, 11, 12};
    double data4[dataSize] = {16.6, 15.5, 14.4, 13.3, 12.2, 11.1};

    std::vector<DataTypePtr> types = { IntType(), DoubleType() };
    DataTypes sourceTypes(types);
    VectorBatch *vecBatch1 = CreateVectorBatch(sourceTypes, dataSize, data1, data2);
    VectorBatch *vecBatch2 = CreateVectorBatch(sourceTypes, dataSize, data3, data4);

    UnionOperatorFactory *operatorFactory =
        UnionOperatorFactory::CreateUnionOperatorFactory(sourceTypes, sourceTypes.GetSize(), false);
    UnionOperator *unionOperator = dynamic_cast<UnionOperator *>(CreateTestOperator(operatorFactory));
    unionOperator->AddInput(vecBatch1);
    unionOperator->AddInput(vecBatch2);
    std::vector<VectorBatch *> outputVecBatches;
    unionOperator->GetOutput(outputVecBatches);

    int32_t expData1[dataSize] = {0, 1, 2, 0, 1, 2};
    double expData2[dataSize] = {6.6, 5.5, 4.4, 3.3, 2.2, 1.1};
    int32_t expData3[dataSize] = {10, 11, 12, 10, 11, 12};
    double expData4[dataSize] = {16.6, 15.5, 14.4, 13.3, 12.2, 11.1};
    VectorBatch *expVecBatch1 = CreateVectorBatch(sourceTypes, dataSize, expData1, expData2);
    VectorBatch *expVecBatch2 = CreateVectorBatch(sourceTypes, dataSize, expData3, expData4);

    EXPECT_TRUE(VecBatchMatch(outputVecBatches[0], expVecBatch1));
    EXPECT_TRUE(VecBatchMatch(outputVecBatches[1], expVecBatch2));

    VectorHelper::FreeVecBatch(expVecBatch1);
    VectorHelper::FreeVecBatch(expVecBatch2);
    VectorHelper::FreeVecBatches(outputVecBatches);
    Operator::DeleteOperator(unionOperator);
    DeleteOperatorFactory(operatorFactory);
}
}
