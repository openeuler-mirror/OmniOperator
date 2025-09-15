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
#include "operator/operator.h"
#include "operator/operator_factory.h"
#include "type/data_types.h"
#include "util/type_util.h"
#include "codegen/func_signature.h"
#include "codegen/func_registry.h"
#include "expression/expressions.h"
#include "type/data_type.h"
#include "vector/vector_helper.h"
#include "vector/large_string_container.h"

namespace omniruntime::TestUtil {
using namespace omniruntime::expressions;

bool VecBatchMatch(vec::VectorBatch *outputPages, vec::VectorBatch *expectPage);

bool VecBatchesIgnoreOrderMatch(std::vector<omniruntime::vec::VectorBatch *> &resultBatches,
    std::vector<omniruntime::vec::VectorBatch *> &expectedBatches);

bool ColumnMatch(vec::BaseVector *actualColumn, vec::BaseVector *expectColumn);

vec::VectorBatch *CreateVectorBatch(const type::DataTypes &types, int32_t rowCount, ...);

omniruntime::vec::BaseVector *CreateVector(type::DataType &dataType, int32_t rowCount, va_list &args);

template <typename T> vec::BaseVector *CreateVector(int32_t length, T *values)
{
    vec::Vector<T> *vector = new vec::Vector<T>(length);
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, values[i]);
    }
    return vector;
}

template <omniruntime::type::DataTypeId typeId> vec::BaseVector *CreateFlatVector(int32_t length, va_list &args)
{
    using namespace omniruntime::type;
    using T = typename NativeType<typeId>::type;
    using VarcharVector = vec::Vector<vec::LargeStringContainer<std::string_view>>;
    if constexpr (std::is_same_v<T, std::string_view>) {
        VarcharVector *vector = new VarcharVector(length);
        std::string *str = va_arg(args, std::string *);
        for (int32_t i = 0; i < length; i++) {
            std::string_view value(str[i].data(), str[i].length());
            vector->SetValue(i, value);
        }
        return vector;
    } else {
        vec::Vector<T> *vector = new vec::Vector<T>(length);
        T *value = va_arg(args, T *);
        for (int32_t i = 0; i < length; i++) {
            vector->SetValue(i, value[i]);
        }
        return vector;
    }
}

void SetValue(vec::BaseVector *vector, int32_t index, void *value);

template <type::DataTypeId typeId>
vec::BaseVector *DictionaryVectorSlice(vec::BaseVector *vector, int32_t offset, int32_t length)
{
    using T = typename type::NativeType<typeId>::type;
    if constexpr (std::is_same_v<T, std::string_view>) {
        return reinterpret_cast<vec::Vector<vec::DictionaryContainer<std::string_view>> *>(vector)->Slice(offset,
            length);
    } else {
        return reinterpret_cast<vec::Vector<vec::DictionaryContainer<T>> *>(vector)->Slice(offset, length);
    }
}

template <type::DataTypeId typeId>
vec::BaseVector *FlatVectorSlice(vec::BaseVector *vector, int32_t offset, int32_t length)
{
    using T = typename type::NativeType<typeId>::type;
    if constexpr (std::is_same_v<T, std::string_view>) {
        return reinterpret_cast<vec::Vector<vec::LargeStringContainer<std::string_view>> *>(vector)->Slice(offset,
            length);
    } else {
        return reinterpret_cast<vec::Vector<T> *>(vector)->Slice(offset, length);
    }
}

vec::BaseVector *SliceVector(vec::BaseVector *vector, int32_t offset, int32_t length);

omniruntime::op::Operator *CreateTestOperator(omniruntime::op::OperatorFactory *operatorFactory);

omniruntime::vec::VectorBatch *DuplicateVectorBatch(omniruntime::vec::VectorBatch *input);

void FreeVecBatches(vec::VectorBatch **vecBatches, int32_t vecBatchCount);

void AssertVecBatchEquals(omniruntime::vec::VectorBatch *vectorBatch, int32_t expectedVecCount,
    int32_t expectedRowCount, ...);
void AssertDoubleVectorEquals(omniruntime::vec::BaseVector *vector, double *expectedValues);
void AssertVarcharVectorEquals(omniruntime::vec::BaseVector *vector, std::string *expectedValues);

vec::BaseVector *CreateDictionaryVector(omniruntime::type::DataType &dataType, int32_t rowCount, int32_t *ids,
    int32_t idsCount, ...);

template <type::DataTypeId typeId>
vec::BaseVector *CreateDictionary(vec::BaseVector *vector, int32_t *ids, int32_t size)
{
    using T = typename type::NativeType<typeId>::type;
    if constexpr (std::is_same_v<T, std::string_view>) {
        return vec::VectorHelper::CreateStringDictionary(ids, size,
            reinterpret_cast<vec::Vector<vec::LargeStringContainer<std::string_view>> *>(vector));
    } else {
        return vec::VectorHelper::CreateDictionary(ids, size, reinterpret_cast<vec::Vector<T> *>(vector));
    }
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

omniruntime::op::Operator *CreateTestOperator(omniruntime::op::OperatorFactory *operatorFactory);


template <typename T> void AssertVectorEquals(vec::BaseVector *vector, T *expectedValues)
{
    for (int32_t i = 0; i < vector->GetSize(); i++) {
        if (vector->IsNull(i)) {
            continue;
        }
        EXPECT_EQ(static_cast<vec::Vector<T> *>(vector)->GetValue(i), expectedValues[i]);
    }
}

vec::BaseVector *CreateVarcharVector(std::string *values, int32_t length);

omniruntime::expressions::FuncExpr *GetFuncExpr(const std::string &funcName,
    std::vector<omniruntime::expressions::Expr *> args, omniruntime::expressions::DataTypePtr returnType);

void AssertStringEquals(std::vector<std::string> &expected, std::vector<uint8_t *> &result,
    std::vector<int32_t> &outLen);

void AssertStringEquals(std::vector<std::string> &expected, int32_t offset, int32_t rowCnt,
    std::vector<uint8_t *> &result, std::vector<int32_t> &outLen);

void AssertIntEquals(std::vector<int32_t> &expected, std::vector<int32_t> &result);

void AssertLongEquals(std::vector<int64_t> &expected, std::vector<int64_t> &result);

void AssertDoubleEquals(std::vector<double> &expected, std::vector<double> &result);

void AssertBoolEquals(std::vector<bool> &expected, bool *result);

std::string GenerateSpillPath();

int32_t *MakeInts(int32_t size, const int32_t start = 0);

int64_t *MakeDecimals(int32_t size, const int32_t start = 0);

int64_t *MakeLongs(int32_t size, const int64_t start = 0);

double *MakeDoubles(int32_t size, const double start = 0);

vec::VectorBatch *CreateEmptyVectorBatch(const DataTypes &dataTypes);

int32_t DecodeAddFlag(int32_t resultCode);

int32_t DecodeFetchFlag(int32_t resultCode);

template <typename D, typename V>
bool CompareUnorderedRows(vec::BaseVector *resultVector, vec::BaseVector *expectedVector,
    const double error = DBL_EPSILON);

bool CompareUnorderedRowsContainer(vec::ContainerVector *resultContainerVector,
    vec::ContainerVector *expectedContainerVector, const double error = DBL_EPSILON);

bool ColumnMatchIgnoreOrder(vec::BaseVector *resultVector, vec::BaseVector *expectedVector,
    const double error = DBL_EPSILON);

bool VecBatchMatchIgnoreOrder(vec::VectorBatch *resultBatch, vec::VectorBatch *expectedBatch,
    const double error = DBL_EPSILON);

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
}
#endif // __TEST_UTIL_H__