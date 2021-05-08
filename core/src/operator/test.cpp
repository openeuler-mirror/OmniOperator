//
// Created by kkrazy on 2021-03-09.
//
#include "chrono"
#include "aggregator/hash_groupby.h"
#include "../jni/sort_api.h"
#include <time.h>
#include <vector>
#include <iostream>
#include <string.h>

Table **buildData(int PAGE_NUM, int DATA_SIZE, int *data_type, int column_count) {
    Table **input = new Table *[PAGE_NUM];
    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        Table *table = new Table(DATA_SIZE, 2);

        for (int j = 0; j < column_count; j++) {
            if (data_type[j] == 1) //INT32
            {
                int32_t *data1 = new int32_t[DATA_SIZE];
                for (int32_t i = 0; i < DATA_SIZE; ++i) {
                    data1[i] = i % 3;
                }

                Column *col1 = new Column(data1, INT32, DATA_SIZE);
                table->setColumn(col1, INT32);
            }
            if (data_type[j] == 2) //INT64
            {
                int64_t *data1 = new int64_t[DATA_SIZE];
                for (int64_t i = 0; i < DATA_SIZE; ++i) {
                    data1[i] = i % 3;
                }

                Column *col1 = new Column(data1, INT64, DATA_SIZE);
                table->setColumn(col1, INT64);
            }
        }
        input[i] = table;
    }
    return input;
}

HashGroupBy *createGroupBy() {
    ColumnIndex c0 = {0, INT32};
    ColumnIndex c1 = {1, INT64};
    std::vector<ColumnIndex> v1 = {c0, c1};
    std::vector<ColumnIndex> v2 = {c0, c1};
    std::vector<Aggregator *> aggs;
    auto *sum1 = new SumAggregator(1);
    auto *sum2 = new SumAggregator(2);
    aggs.push_back(sum1);
    aggs.push_back(sum2);
    auto groupby = new HashGroupBy(v1, v2, aggs);
    return groupby;
}

int test_group_by(int page_count, int row_count, int *data_type, int column_count) {

    // create 10 pages
    const int PAGE_NUM = page_count;
    Table **input = buildData(page_count, row_count, data_type, column_count);
    const int DATA_SIZE = row_count;

    auto *groupBy = createGroupBy();

    typedef std::chrono::high_resolution_clock Time;
    typedef std::chrono::milliseconds ms;
    typedef std::chrono::duration<float> fsec;

    auto t1 = Time::now();

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        groupBy->process(input[i], DATA_SIZE);
    }

    Table *result = groupBy->getResult();

    auto t0 = Time::now();
    fsec fs = t0 - t1;
    ms d = std::chrono::duration_cast<ms>(fs);
    std::cout << " in agg duration time: " << d.count() << "ms\n";

    for (int32_t i = 0; i < result->getColumnNumber(); ++i) {
        Column *col = result->getColumn(i);
        col->printColumn();
    }

    std::cout << "finished groupby page count: " << PAGE_NUM << " page size: " << DATA_SIZE << "\n";

    for (int32_t i = 0; i < PAGE_NUM; ++i) {
        delete[] reinterpret_cast<int32_t *>(input[i]->getColumn(0)->getData());
        delete[] reinterpret_cast<int64_t *>(input[i]->getColumn(1)->getData());
        delete input[i];
    }

    for (int32_t i = 0; i < result->getColumnNumber(); ++i) {
        Column *col = result->getColumn(i);
        delete col->getData();
        delete col;
    }
    delete result;
    return 1234;
}