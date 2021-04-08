//
// Created by kkrazy on 2021-03-09.
//
#include "chrono"
#include "hash_groupby.h"
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


int test_sort() {
    printf("test_sort called\n");
    // construct input data
    const int32_t DATA_SIZE = 10000000;
    int sourceTypes[2] = {1, 1};
    int outputCols[2] = {0, 1};
    int sortCols[2] = {0, 1};
    int ascendings[2] = {true, true};
    int nullFirsts[2] = {true, true};

    long contextAddress = sortPrepare(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    long sortAddress = sortCreateOperator(contextAddress, sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    int32_t *data1 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data1[i] = i;
    }
    int *nulls1 = new int[DATA_SIZE];
    memset(nulls1, false, DATA_SIZE);

    int32_t *data2 = new int32_t[DATA_SIZE];
    for (int32_t i = 0; i < DATA_SIZE; ++i) {
        data2[i] = i;
    }
    int *nulls2 = new int[DATA_SIZE];
    memset(nulls2, false, DATA_SIZE);

    long datas[2] = {(long)data1, (long)data2};
    long nulls[2] = {(long)nulls1, (long)nulls2};
    long rowCounts[1] = {DATA_SIZE};

    sortAddInput(contextAddress, sortAddress, datas, nulls, 1, rowCounts, DATA_SIZE);
    clock_t start = clock();
    sortExecute(contextAddress, sortAddress);
    std::cout << "sort elapsed end time: " << (double) (std::clock() - start) / 1000 << " ms" << std::endl;

    Table * output = sortGetOutput(contextAddress, sortAddress);

    // free memory
    delete output;
    delete[]nulls2;
    delete[]nulls1;
    delete[]data2;
    delete[]data1;

    return 1235;
}

void buildSortData(int tableCount, int distinctValueCount, int repeatCount, long *datas, long *nulls)
{
    uint32_t positionCount = distinctValueCount * repeatCount;
    long *data1;
    long *data2;
    long *null1;
    long *null2;
    uint32_t size = positionCount * sizeof(long);
    uint32_t idx = 0;

    for (int i = 0; i < tableCount; i++) {
        data1 = (long *)malloc(size);
        null1 = (long *)malloc(size);
        data2 = (long *)malloc(size);
        null2 = (long *)malloc(size);

        idx = 0;
        for (int j = 0; j < distinctValueCount; j++) {
            for (int k = 0; k < repeatCount; k++) {
                data1[idx] = j;
                data2[idx] = j;
                null1[idx] = 0;
                null2[idx] = 0;
                idx++;
            }
        }

        datas[i * 2 + 0] = (long)data1;
        datas[i * 2 + 1] = (long)data2;
        nulls[i * 2 + 0] = (long)null1;
        nulls[i * 2 + 1] = (long)null2;
    }
}

int test_sort_one()
{
    printf("test_sort_one called\n");

    int tableCount = 10;
    int distinctValue = 4;
    int repeatCount = 250000;
    uint32_t rowNum = distinctValue * repeatCount;

    long *datas = (long *)malloc(tableCount * 2 * sizeof(long));
    long *nulls = (long *)malloc(tableCount * 2 * sizeof(long));
    buildSortData(tableCount, distinctValue, repeatCount, datas, nulls);
    std::cout<<"finish build sort data" << endl;

    long rowCounts[tableCount];
    for (int i = 0; i < tableCount; i++) {
        rowCounts[i] = rowNum;
    }
    
    int sourceTypes[] = {1, 1};
    int outputCols[] = {0, 1};
    int sortCols[] = {0, 1};
    int ascendings[] = {1, 1};
    int nullFirsts[] = {0, 0};

    long contextAddress = sortPrepare(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    long sortAddress = sortCreateOperator(contextAddress, sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    sortAddInput(contextAddress, sortAddress, datas, nulls, tableCount, rowCounts, tableCount * rowNum);

    clock_t start = clock();
    sortExecute(contextAddress, sortAddress);
    std::cout << "sort elapsed end time: " << (double) (std::clock() - start) / 1000 << " ms" << std::endl;

    Table * output = sortGetOutput(contextAddress, sortAddress);
    return 1236;
}
