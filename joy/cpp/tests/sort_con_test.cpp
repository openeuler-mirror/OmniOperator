#include "../src/jni/sort_api.h"
#include <time.h>
#include <unistd.h>
#include <vector>
#include <iostream>
#include <thread>
#include <atomic>

int g_tableCount;
int g_distinctValue;
int g_repeatCount;
long g_contextAddress;
long *g_datas;
long *g_nulls;
Table *g_expectedTable;

typedef std::chrono::high_resolution_clock Time;
typedef std::chrono::milliseconds ms;
typedef std::chrono::duration<float> fsec;
atomic_long g_time;

void buildSortTestData(int tableCount, int distinctValueCount, int repeatCount, long *datas, long *nulls)
{
    uint32_t positionCount = distinctValueCount * repeatCount;
    long *data1;
    long *data2;
    long *null1;
    long *null2;
    uint32_t size = positionCount * sizeof(long);
    uint32_t idx = 0;

    auto t0 = Time::now();
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
    auto t1 = Time::now();
    fsec fs = t1 - t0;
    ms d = std::chrono::duration_cast<ms>(fs);
    std::cout << "buildSortTestData finished elapsed end time: " << (double)d.count() << " ms" << std::endl;
}

//void sortProcess(long **datas, long **nulls, int tableCount, uint32_t rowNum)
void sortProcess()
{
    //std::cout << "current thread on CPU " << sched_getcpu() << endl;
    long *datas = (long *)malloc(g_tableCount * 2 * sizeof(long));
    long *nulls = (long *)malloc(g_tableCount * 2 * sizeof(long));
    buildSortTestData(g_tableCount, g_distinctValue, g_repeatCount, datas, nulls);

    uint32_t rowNum = g_distinctValue * g_repeatCount;
    long rowCounts[g_tableCount];
    for (int i = 0; i < g_tableCount; i++) {
        rowCounts[i] = rowNum;
    }

    int sourceTypes[] = {2, 2};
    int outputCols[] = {0, 1};
    int sortCols[] = {0, 1};
    int ascendings[] = {1, 1};
    int nullFirsts[] = {0, 0};

    auto t0 = Time::now();
    long sortAddress = sortCreateOperator(g_contextAddress, sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);
    sortAddInput(g_contextAddress, sortAddress, datas, nulls, g_tableCount, rowCounts, g_tableCount * rowNum);
    sortExecute(g_contextAddress, sortAddress);
    Table *output = sortGetOutput(g_contextAddress, sortAddress);
    auto t1 = Time::now();
    fsec fs = t1 - t0;
    ms d = std::chrono::duration_cast<ms>(fs);
    g_time = g_time + (long)d.count();
    std::cout << "THREAD sortProcess finished elapsed end time: " << (double)d.count() << " ms" << std::endl;

    for (int i = 0; i < g_tableCount; i++) {
        delete[] (long *)(datas[i * 2 + 0]);
        delete[] (long *)(datas[i * 2 + 1]);
        delete[] (long *)(nulls[i * 2 + 0]);
        delete[] (long *)(nulls[i * 2 + 1]);
    }
    delete[] datas;
    delete[] nulls;
}

int main(int argc, char **argv) {
    g_tableCount = stoi(argv[1]);
    g_distinctValue = stoi(argv[2]);
    g_repeatCount = stoi(argv[3]);

    const auto processor_count = std::thread::hardware_concurrency();
    std::cout << "core number: " << processor_count << std::endl;
    //std::this_thread::sleep_for(10000ms);

    int sourceTypes[] = {2, 2};
    int outputCols[] = {0, 1};
    int sortCols[] = {0, 1};
    int ascendings[] = {1, 1};
    int nullFirsts[] = {0, 0};
    g_contextAddress = sortPrepare(sourceTypes, 2, outputCols, 2, sortCols, ascendings, nullFirsts, 2);

    int threadNums[] = {1, 8, 16, 32, 64, 100};
    for (int i = 0; i < 6; i++) {
        int threadCount = threadNums[i];
        //auto t_ = threadCount < processor_count ? processor_count / threadCount : 1;

        //struct timespec cpu_start, wall_start, cpu_end, wall_end;
        //clock_gettime(CLOCK_REALTIME, &wall_start);
        //clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_start);
        //double wall_elapsed = 0;
        //double cpu_elapsed = 0;
        vector<std::thread> threads;
        for (int i = 0; i < threadCount; i++) {
            std::thread threadObj(sortProcess);
            threads.push_back(std::move(threadObj));
        }

        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
//        clock_gettime(CLOCK_REALTIME, &wall_end);
//        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_end);
//        long seconds_wall = wall_end.tv_sec - wall_start.tv_sec;
//        long seconds_cpu = cpu_end.tv_sec - cpu_start.tv_sec;
//        long ns_wall = wall_end.tv_nsec - wall_start.tv_nsec;
//        long ns_cpu = cpu_end.tv_nsec - cpu_start.tv_nsec;
//        wall_elapsed = seconds_wall + ns_wall*1e-9;
//        cpu_elapsed = seconds_cpu + ns_cpu*1e-9;
//        std::cout << threadCount << " wall_elapsed time: " << wall_elapsed << "s" << std::endl;
//        std::cout << threadCount << " cpu_elapsed time: " << cpu_elapsed / processor_count * t_ << "s" << std::endl;
        std::cout << "PROCESS finished, each thread elapsed time: " << (g_time / threadCount) << " ms" << endl;
        g_time = 0;
        threads.clear();
        std::this_thread::sleep_for(100ms);
    }
}
