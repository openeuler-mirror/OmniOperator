/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2023. All rights reserved.
 * Description: Type Util Class
 */
#ifndef __TEST_UTIL_H__
#define __TEST_UTIL_H__

#include <ctime>
#include <cstdint>
#include <string>
#include <gtest/gtest.h>
#include "vector/vector_common.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "type/data_types.h"
#include "util/type_util.h"
#include "codegen/func_signature.h"
#include "codegen/func_registry.h"
#include "expression/expressions.h"

namespace TestUtil {
bool VecBatchMatch(omniruntime::vec::VectorBatch *outputPages, omniruntime::vec::VectorBatch *expectPage);
bool VecBatchMatchIgnoreOrder(omniruntime::vec::VectorBatch *resultBatch, omniruntime::vec::VectorBatch *expectedBatch);
bool VecBatchMatches(std::vector<omniruntime::vec::VectorBatch *> &outputPages,
    std::vector<omniruntime::vec::VectorBatch *> &expectPage);
bool VecBatchesIgnoreOrderMatch(std::vector<omniruntime::vec::VectorBatch *> &resultBatches,
    std::vector<omniruntime::vec::VectorBatch *> &expectedBatches);
omniruntime::vec::VectorBatch *CreateVectorBatch(const omniruntime::type::DataTypes &types, int32_t rowCount, ...);
omniruntime::vec::VectorBatch *CreateEmptyVectorBatch(const std::vector<omniruntime::type::DataTypePtr> &dataTypes);
omniruntime::vec::VarcharVector *CreateVarcharVector(omniruntime::type::DataType &type, std::string *values,
    int32_t length);
omniruntime::vec::DictionaryVector *CreateDictionaryVector(omniruntime::type::DataType &dataType, int32_t rowCount,
    int32_t *ids, int32_t idsCount, ...);
omniruntime::vec::ContainerVector *CreateContainerVector(std::vector<omniruntime::vec::DataTypePtr> &fieldTypes,
    int32_t rowCount, va_list &args);
omniruntime::vec::Vector *CreateVector(omniruntime::vec::DataType &dataType, int32_t rowCount, va_list &args);

void AssertVecBatchEquals(omniruntime::vec::VectorBatch *vectorBatch, int32_t expectedVecCount,
    int32_t expectedRowCount, ...);
void AssertDoubleVectorEquals(omniruntime::vec::DoubleVector *vector, double *expectedValues);
void AssertVarcharVectorEquals(omniruntime::vec::VarcharVector *vector, std::string *expectedValues);

omniruntime::op::Operator *CreateTestOperator(omniruntime::op::OperatorFactory *operatorFactory);
void DeleteOperatorFactory(omniruntime::op::OperatorFactory *operatorFactory);
omniruntime::vec::VectorBatch *DuplicateVectorBatch(omniruntime::vec::VectorBatch *input);

void SetNulls(omniruntime::vec::Vector *vector, std::vector<bool> &nulls);

omniruntime::vec::VarcharVector *CreateVarcharVector(std::vector<std::string> &values, std::vector<bool> &nulls);

omniruntime::vec::VectorBatch *CreateVectorBatch(int32_t rowCount, std::vector<omniruntime::vec::Vector *> &vectors);

bool ColumnMatch(omniruntime::vec::Vector *actualColumn, omniruntime::vec::Vector *expectColumn);
bool ColumnMatchIgnoreOrder(omniruntime::vec::Vector *actualColumn, omniruntime::vec::Vector *expectColumn);

bool VecBatchMatchIgnoreOrder(omniruntime::vec::VectorBatch *resultBatch, omniruntime::vec::VectorBatch *expectedBatch,
    const double error);

class Timer {
public:
    Timer() : wallElapsed(0), cpuElapsed(0) {}

    ~Timer() {}

    void SetStart()
    {
        clock_gettime(CLOCK_REALTIME, &wallStart);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpuStart);
    }

    void CalculateElapse()
    {
        clock_gettime(CLOCK_REALTIME, &wallEnd);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpuEnd);
        long secondsWall = wallEnd.tv_sec - wallStart.tv_sec;
        long secondsCpu = cpuEnd.tv_sec - cpuStart.tv_sec;
        long nsWall = wallEnd.tv_nsec - wallStart.tv_nsec;
        long nsCpu = cpuEnd.tv_nsec - cpuStart.tv_nsec;
        wallElapsed = secondsWall + nsWall * 1e-9;
        cpuElapsed = secondsCpu + nsCpu * 1e-9;
    }

    void Start(const char *TestTitle)
    {
        wallElapsed = 0;
        cpuElapsed = 0;
        clock_gettime(CLOCK_REALTIME, &wallStart);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpuStart);
        this->title = TestTitle;
    }

    void End()
    {
        clock_gettime(CLOCK_REALTIME, &wallEnd);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpuEnd);
        long secondsWall = wallEnd.tv_sec - wallStart.tv_sec;
        long secondsCpu = cpuEnd.tv_sec - cpuStart.tv_sec;
        long nsWall = wallEnd.tv_nsec - wallStart.tv_nsec;
        long nsCpu = cpuEnd.tv_nsec - cpuStart.tv_nsec;
        wallElapsed = secondsWall + nsWall * 1e-9;
        cpuElapsed = secondsCpu + nsCpu * 1e-9;
        std::cout << title << " \t: wall " << wallElapsed << " \tcpu " << cpuElapsed << std::endl;
    }

    double GetWallElapse()
    {
        return wallElapsed;
    }

    double GetCpuElapse()
    {
        return cpuElapsed;
    }

    void Reset()
    {
        wallElapsed = 0;
        cpuElapsed = 0;
        clock_gettime(CLOCK_REALTIME, &wallStart);
        clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &cpuStart);
    }

private:
    double wallElapsed;
    double cpuElapsed;
    struct timespec cpuStart;
    struct timespec wallStart;
    struct timespec cpuEnd;
    struct timespec wallEnd;
    const char *title;
};

template <typename T, typename V> T *CreateVector(V *values, int32_t length)
{
    omniruntime::vec::VectorAllocator *vecAllocator = omniruntime::vec::VectorAllocator::GetGlobalAllocator();
    auto vector = new T(vecAllocator, length);
    vector->SetValues(0, values, length);
    return vector;
}

template <typename T, typename E> void AssertVectorEquals(T *vector, E *expectedValues)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsValueNull(i)) {
            continue;
        }
        EXPECT_EQ(vector->GetValue(i), expectedValues[i]);
    }
}

void ToVectorTypes(const int32_t *dataTypeIds, int32_t dataTypeCount,
    std::vector<omniruntime::vec::DataTypePtr> &dataTypes);

void GetTestTypeIds(omniruntime::type::DataTypes &inputTypes, std::string *projectKeys, int32_t projectKeysCount,
    std::vector<int32_t> &typeIds, int32_t *projectCols);

omniruntime::expressions::FuncExpr *GetFuncExpr(const std::string &funcName,
    std::vector<omniruntime::expressions::Expr *> args, omniruntime::expressions::DataTypePtr returnType);

void AssertStringEquals(std::vector<std::string> &expected, std::vector<uint8_t *> &result,
    std::vector<int32_t> &outLen);

void AssertStringEquals(std::vector<std::string> &expected, int32_t offset, int32_t rowCnt,
    std::vector<uint8_t *> &result, std::vector<int32_t> &outLen);

template <typename T> void AssertEquals(const std::vector<T> &expected, const std::vector<T> &result)
{
    for (size_t i = 0; i < expected.size(); i++) {
        EXPECT_EQ(result[i], expected[i]);
    }
}

std::string GenerateSpillPath();

int32_t *MakeInts(const int32_t size, const int32_t start = 0);

int64_t *MakeDecimals(const int32_t size, const int32_t start = 0);

int64_t *MakeLongs(const int32_t size, const int64_t start = 0);

double *MakeDoubles(const int32_t size, const double start = 0);

int16_t *MakeShorts(const int32_t size, const int16_t start = 0);

int32_t DecodeAddFlag(int32_t resultCode);

int32_t DecodeFetchFlag(int32_t resultCode);
}

#endif