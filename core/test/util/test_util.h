/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Type Util Class
 */
#ifndef __TEST_UTIL_H__
#define __TEST_UTIL_H__

#include <time.h>
#include "../../src/vector/vector_common.h"
#include "../../src/operator/operator.h"
#include "../../src/operator/operator_factory.h"
#include "../../src/type/data_types.h"
#include "../../src/type/data_type.h"
#include "../../src/vector/vector_allocator_factory.h"
#include "codegen/func_signature.h"
#include "codegen/func_registry.h"
#include "expression/expressions.h"

using namespace omniruntime::vec;

bool VecBatchMatch(omniruntime::vec::VectorBatch *outputPages, omniruntime::vec::VectorBatch *expectPage);
omniruntime::vec::VectorBatch *CreateVectorBatch(omniruntime::type::DataTypes &types, int32_t rowCount, ...);
omniruntime::vec::VectorBatch *createEmptyVectorBatch(std::vector<DataType> &dataTypes);
omniruntime::vec::VarcharVector *CreateVarcharVector(omniruntime::type::VarcharDataType type, std::string *values,
    int32_t length);
omniruntime::vec::DictionaryVector *CreateDictionaryVector(omniruntime::type::DataType &dataType, int32_t rowCount,
    int32_t *ids, int32_t idsCount, ...);
void AssertVecBatchEquals(omniruntime::vec::VectorBatch *vectorBatch, int32_t expectedVecCount,
    int32_t expectedRowCount, ...);
void AssertDoubleVectorEquals(omniruntime::vec::DoubleVector *vector, double *expectedValues);
void AssertVarcharVectorEquals(omniruntime::vec::VarcharVector *vector, std::string *expectedValues);


omniruntime::op::Operator *CreateTestOperator(OperatorFactory *operatorFactory);
void DeleteOperatorFactory(OperatorFactory *operatorFactory);
omniruntime::vec::VectorBatch *DuplicateVectorBatch(omniruntime::vec::VectorBatch *input);

class Timer {
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
        wall_elapsed = seconds_wall + ns_wall * 1e-9;
        cpu_elapsed = seconds_cpu + ns_cpu * 1e-9;
    }

    void start(const char *title)
    {
        wall_elapsed = 0;
        cpu_elapsed = 0;
        clock_gettime(CLOCK_REALTIME, &wall_start);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_start);
        this->title = title;
    }

    void end()
    {
        clock_gettime(CLOCK_REALTIME, &wall_end);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpu_end);
        long seconds_wall = wall_end.tv_sec - wall_start.tv_sec;
        long seconds_cpu = cpu_end.tv_sec - cpu_start.tv_sec;
        long ns_wall = wall_end.tv_nsec - wall_start.tv_nsec;
        long ns_cpu = cpu_end.tv_nsec - cpu_start.tv_nsec;
        wall_elapsed = seconds_wall + ns_wall * 1e-9;
        cpu_elapsed = seconds_cpu + ns_cpu * 1e-9;
        std::cout << title << " \t: wall " << wall_elapsed << " \tcpu " << cpu_elapsed << std::endl;
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
    const char *title;
};

template <typename T, typename V> T *CreateVector(V *values, int32_t length)
{
    VectorAllocator *vecAllocator = VectorAllocatorFactory::GetGlobalAllocator();
    auto vector = new T(vecAllocator, length);
    vector->SetValues(0, values, length);
    return vector;
}

template <typename T, typename E> void AssertVectorEquals(T *vector, E *expectedValues)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        // TODO: need to find a better way to compare NULLs
        if (vector->IsValueNull(i)) {
            continue;
        }
        EXPECT_EQ(vector->GetValue(i), expectedValues[i]);
    }
}

void ToVectorTypes(const int32_t *dataTypeIds, int32_t dataTypeCount, std::vector<DataType> &dataTypes);

void GetTestTypeIds(DataTypes &inputTypes, std::string *projectKeys, int32_t projectKeysCount,
    std::vector<int32_t> &typeIds, int32_t *projectCols);

omniruntime::expressions::FuncExpr *GetFuncExpr(const std::string &funcName,
    std::vector<omniruntime::expressions::Expr *> args, omniruntime::expressions::DataTypePtr returnType);

#endif
