//
// Created by kkrazy on 2021-03-09.
//
#include "../../../operator/group_aggregation.h"

int test_group_by() {

    // create 10 pages
    const int PAGE_NUM = 10;
    Table* input[PAGE_NUM];
    const int DATA_SIZE = 10;
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        Table* table = new Table(DATA_SIZE, 2);
        int32_t* data1 = new int32_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data1[i] = i % 3;
        }
        Column* col1 = new Column(data1, INT32, DATA_SIZE);
        int64_t* data2 = new int64_t[DATA_SIZE];
        for (int32_t i = 0; i < DATA_SIZE; ++i) {
            data2[i] = i % 3;
        }
        Column* col2 = new Column(data2, INT64, DATA_SIZE);
        table->setColumn(col1, INT32);
        table->setColumn(col2, INT64);
        input[i] = table;
    }

    ColumnIndex c0 = {0, INT32};
    ColumnIndex c1 = {1, INT64};
    std::vector<ColumnIndex> v1 = {c0, c1};
    std::vector<ColumnIndex> v2 = {c0, c1};
    std::vector<Aggregator*> aggs;
    SumAggregator<int32_t>* sum1 = new SumAggregator<int32_t>();
    SumAggregator<int64_t>* sum2 = new SumAggregator<int64_t>();
    aggs.push_back(sum1);
    aggs.push_back(sum2);
    HashGroupBy* groupBy = new HashGroupBy(v1, v2, aggs);

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        groupBy->process(input[i], DATA_SIZE);
    }

    Table* result = groupBy->getResult();

    for (int32_t i = 0; i < result->getColumnNumber(); ++i) {
        Column* col = result->getColumn(i);
        col->printColumn();
    }

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        delete[] reinterpret_cast<int32_t*>(input[i]->getColumn(0)->getData());
        delete[] reinterpret_cast<int64_t*>(input[i]->getColumn(1)->getData());
        delete input[i];
    }

    for (int32_t i = 0; i < result->getColumnNumber(); ++i) {
        Column* col = result->getColumn(i);
        delete col->getData();
        delete col;
    }
    delete result;
}