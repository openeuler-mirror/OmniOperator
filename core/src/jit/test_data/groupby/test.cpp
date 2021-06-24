//
// Created by kkrazy on 2021-03-09.
//
#include "../../../operator/group_aggregation.h"

int test_group_by() {

    // create 10 vecBatches
    const int VEC_BATCH_NUM = 10;
    VectorBatch* input[VEC_BATCH_NUM];
    const int DATA_SIZE = 10;
    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        VectorBatch* vecBatch = new VectorBatch(DATA_SIZE, 2);
        int32_t* data1 = new int32_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data1[i] = i % 3;
        }
        Vector* col1 = new Vector(data1, INT32, DATA_SIZE);
        int64_t* data2 = new int64_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data2[i] = i % 3;
        }
        Vector* col2 = new Vector(data2, OMNI_VEC_TYPE_LONG, DATA_SIZE);
        vecBatch->setColumn(col1, INT32);
        vecBatch->setColumn(col2, OMNI_VEC_TYPE_LONG);
        input[i] = vecBatch;
    }

    ColumnIndex c0 = {0, INT32};
    ColumnIndex c1 = {1, OMNI_VEC_TYPE_LONG};
    std::vector<ColumnIndex> v1 = {c0, c1};
    std::vector<ColumnIndex> v2 = {c0, c1};
    std::vector<Aggregator*> aggs;
    SumAggregator<int32_t>* sum1 = new SumAggregator<int32_t>();
    SumAggregator<int64_t>* sum2 = new SumAggregator<int64_t>();
    aggs.push_back(sum1);
    aggs.push_back(sum2);
    HashGroupBy* groupBy = new HashGroupBy(v1, v2, aggs);

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        groupBy->process(input[i], DATA_SIZE);
    }

    VectorBatch* result = groupBy->getResult();

    for (int32_t i = 0; i < result->getColumnNumber(); ++i) {
        Vector* col = result->getColumn(i);
        col->printColumn();
    }

    for (int32_t i = 0; i < VEC_BATCH_NUM; ++i) {
        delete[] reinterpret_cast<int32_t*>(input[i]->getColumn(0)->getData());
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(1)->getData());
        delete input[i];
    }

    for (int32_t i = 0; i < result->getColumnNumber(); ++i) {
        Vector* col = result->getColumn(i);
        delete col->getData();
        delete col;
    }
    delete result;
}