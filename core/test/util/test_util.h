#ifndef __TEST_UTIL_H__
#define __TEST_UTIL_H__

#include "../../src/vector/table.h"
#include "../../src/operator/operator.h"
#include "../../src/operator/operator_factory.h"
#include <time.h>

bool tableMatch(Table *outputTables, Table *expectTable);
omniruntime::op::Operator *createTestOperator(OperatorFactory *operatorFactory);

class Timer
{
public:
    Timer() : wall_elapsed(0), cpu_elapsed(0) {}
    ~Timer() {}
    void setStart()
    {
        clock_gettime(CLOCK_REALTIME, &wall_start);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_start);
    }
    void calculateElapse()
    {
        clock_gettime(CLOCK_REALTIME, &wall_end);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_end);
        long seconds_wall = wall_end.tv_sec - wall_start.tv_sec;
        long seconds_cpu = cpu_end.tv_sec - cpu_start.tv_sec;
        long ns_wall = wall_end.tv_nsec - wall_start.tv_nsec;
        long ns_cpu = cpu_end.tv_nsec - cpu_start.tv_nsec;
        wall_elapsed = seconds_wall + ns_wall*1e-9;
        cpu_elapsed = seconds_cpu + ns_cpu*1e-9;
    }
double getWallElapse()
    {
        return wall_elapsed;
    }
    double getCpuElapse()
    {
        return cpu_elapsed;
    }
    void reset()
    {
        wall_elapsed = 0;
        cpu_elapsed = 0;
        clock_gettime(CLOCK_REALTIME, &wall_start);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_start);
    }
private:
    double wall_elapsed;
    double cpu_elapsed;
    struct timespec cpu_start;
    struct timespec wall_start;
    struct timespec cpu_end;
    struct timespec wall_end;
};
#endif
