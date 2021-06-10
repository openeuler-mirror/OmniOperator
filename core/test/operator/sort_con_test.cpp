#include "../../src/operator/sort/sort.h"
#include <time.h>
#include <unistd.h>
#include <vector>
#include <iostream>
#include <thread>
#include <atomic>

using namespace omniruntime::op;
int g_tableCount;
int g_distinctValue;
int g_repeatCount;
SortOperatorFactory *g_factory = NULL;
Table **g_inputTables;

typedef std::chrono::high_resolution_clock Time;
typedef std::chrono::milliseconds ms;
typedef std::chrono::duration<float> fsec;
std::atomic_long g_time;

void buildSortTestData(int tableCount, int distinctValueCount, int repeatCount, Table **inputTables)
{
    uint32_t positionCount = distinctValueCount * repeatCount;
    long *data1;
    long *data2;
    uint32_t size = positionCount * sizeof(long);
    uint32_t idx = 0;

    auto t0 = Time::now();
    for (int i = 0; i < tableCount; i++) {
        Table *table = new Table(positionCount, 2);

        data1 = (long *)malloc(size);
        data2 = (long *)malloc(size);

        idx = 0;
        for (int j = 0; j < distinctValueCount; j++) {
            for (int k = 0; k < repeatCount; k++) {
                data1[idx] = j;
                data2[idx] = j;
                idx++;
            }
        }

        Column *column1 = new Column(data1, INT64, positionCount);
        Column *column2 = new Column(data2, INT64, positionCount);
        table->setColumn(column1, INT64);
        table->setColumn(column2, INT64);
        inputTables[i] = table;
    }
    auto t1 = Time::now();
    fsec fs = t1 - t0;
    ms d = std::chrono::duration_cast<ms>(fs);
    std::cout << "buildSortTestData finished elapsed end time: " << (double)d.count() << " ms" << std::endl;
}

void sortProcess()
{
    //std::cout << "current thread on CPU " << sched_getcpu() << endl;
    uint32_t rowNum = g_distinctValue * g_repeatCount;
    int32_t rowCounts[g_tableCount];
    for (int i = 0; i < g_tableCount; i++) {
        rowCounts[i] = rowNum;
    }

    int sourceTypes[] = {2, 2};
    int outputCols[] = {0, 1};
    int sortCols[] = {0, 1};
    int ascendings[] = {1, 1};
    int nullFirsts[] = {0, 0};

    //auto t0 = Time::now();
    SortOperator *sortOperator = (SortOperator *)g_factory->createOperator();
    sortOperator->addInput(g_inputTables, rowCounts, g_tableCount);
    std::vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);

    //auto t1 = Time::now();
    //fsec fs = t1 - t0;
    //ms d = std::chrono::duration_cast<ms>(fs);
    //g_time = g_time + (long)d.count();
    //std::cout << "THREAD sortProcess finished elapsed end time: " << (double)d.count() << " ms" << std::endl;

    freeDataInColumn(&outputTables[0], outputTables.size());
    freeOutputTable(outputTables);
    delete sortOperator;
}

int main1(int argc, char **argv) {
    if (argc < 4) {
        std::cout << "Usage: program tablecount distinctvalue repeatcount" << std::endl;
        return 0;
    }
    g_tableCount = atoi(argv[1]);
    g_distinctValue = atoi(argv[2]);
    g_repeatCount = atoi(argv[3]);

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    //std::this_thread::sleep_for(10000ms);

    g_inputTables = (Table **)malloc(g_tableCount * sizeof(Table *));
    buildSortTestData(g_tableCount, g_distinctValue, g_repeatCount, g_inputTables);

    int sourceTypes[] = {2, 2};
    int outputCols[] = {0, 1};
    int sortCols[] = {0, 1};
    int ascendings[] = {1, 1};
    int nullFirsts[] = {0, 0};
    g_factory = SortOperatorFactory::createSortOperatorFactory(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    int threadNums[] = {1, 8, 16, 32, 64, 100};
    for (int i = 0; i < 6; i++) {
        int threadCount = threadNums[i];
        auto t_ = threadCount < processor_count ? processor_count / threadCount : 1;

        struct timespec cpu_start, wall_start, cpu_end, wall_end;
        clock_gettime(CLOCK_REALTIME, &wall_start);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_start);
        double wall_elapsed = 0;
        double cpu_elapsed = 0;

        std::vector<std::thread> threads;
        for (int i = 0; i < threadCount; i++) {
            std::thread threadObj(sortProcess);
            threads.push_back(std::move(threadObj));
        }

        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }

        clock_gettime(CLOCK_REALTIME, &wall_end);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_end);
        long seconds_wall = wall_end.tv_sec - wall_start.tv_sec;
        long seconds_cpu = cpu_end.tv_sec - cpu_start.tv_sec;
        long ns_wall = wall_end.tv_nsec - wall_start.tv_nsec;
        long ns_cpu = cpu_end.tv_nsec - cpu_start.tv_nsec;
        wall_elapsed = seconds_wall + ns_wall*1e-9;
        cpu_elapsed = seconds_cpu + ns_cpu*1e-9;
        std::cout << threadCount << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
        std::cout << threadCount << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;

        //std::cout << "PROCESS finished, each thread elapsed time: " << (g_time / threadCount) << " ms" << endl;
        //g_time = 0;
    }

    freeDataInColumn(g_inputTables, g_tableCount);
    freeInputTable(g_inputTables, g_tableCount);
    delete g_factory;
    return 0;
}
