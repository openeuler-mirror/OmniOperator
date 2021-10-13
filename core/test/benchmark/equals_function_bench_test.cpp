/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "../util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;

const int32_t ROW_SIZE = 1000000;
const int32_t VAR_LEN = 10;
const int32_t ROUNDS = 10;

const std::string STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

static string getString(int32_t index, int32_t offset, int32_t width)
{
    std::string str;
    for (int j = 0; j < width; j++) {
        char c = STRING.at((index + offset + j) % STRING.length());
        str.push_back(c);
    }
    return str;
}

static bool hashagg_equal(Vector* vector1, uint32_t offset1, Vector* vector2, uint32_t offset2)
{
    bool isInputNull1 = vector1->IsValueNull(offset1);
    bool isInputNull2 = vector2->IsValueNull(offset2);
    if (!isInputNull1 && !isInputNull2) {
        uint8_t *data1 = nullptr;
        int32_t valLen1 = static_cast<VarcharVector *>(vector1)->GetValue(offset1, &data1);
        uint8_t *data2 = nullptr;
        int32_t valLen2 = static_cast<VarcharVector *>(vector2)->GetValue(offset2, &data2);
        bool isSame = valLen1 == valLen2 && memcmp(data1, data2, std::min(valLen1, valLen2)) == 0;
        return isSame;
    }
    if (isInputNull1 != isInputNull2) {
        return false;
    }
    return true;
}

static bool join_equal(VarcharVector *leftVector, int32_t leftIndex, VarcharVector *rightVector, int32_t rightIndex)
{
    uint8_t *leftValue = nullptr;
    uint8_t *rightValue = nullptr;
    int32_t leftLength = 0;
    int32_t rightLength = 0;

    leftLength = leftVector->GetValue(leftIndex, &leftValue);
    rightLength = rightVector->GetValue(rightIndex, &rightValue);
    if (leftLength != rightLength) {
        return false;
    }
    if (memcmp(leftValue, rightValue, leftLength) == 0) {
        return true;
    } else {
        return false;
    }
}

TEST(varcharType, VarcharValueEqualsValueIgnoreNullsPerf){
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("varchar");
    VarcharVector *vector1 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);
    VarcharVector *vector2 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);

    for (int i = 0; i < ROW_SIZE; i++) {
        std::string str = getString(i, 10, VAR_LEN);
        vector1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        vector2->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    std::cout << "Test times: " << ROW_SIZE << std::endl;
    std::cout << "varchar length: " << VAR_LEN << std::endl;

    Timer timer;
    timer.setStart();

    std::cout << "Compare same varchar: " << std::endl;
    double sum = 0;
    bool isEqual;
    for (int j = 0; j < ROUNDS; j++){
        timer.reset();

        for (int i = 0; i < ROW_SIZE; i++) {
            isEqual = join_equal(vector1, i, vector2, i);
        }

        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse() * 1000;
        double cpu_elapsed = timer.getCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
        sum += wall_elapsed;
    }
    std::cout << "isEqual: " << isEqual << std::endl;
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;

    VarcharVector *vector3 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);
    for (int i = 0; i < ROW_SIZE; i++) {
        std::string  str = getString(i, 20, VAR_LEN);
        vector3->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    std::cout << "Compare different varchar: " << std::endl;
    sum = 0;
    for (int j = 0; j < ROUNDS; j++){
        timer.reset();

        for (int i = 0; i < ROW_SIZE; i++) {
            isEqual = join_equal(vector1, i, vector3, i);
        }

        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse() * 1000;
        double cpu_elapsed = timer.getCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
        sum += wall_elapsed;
    }
    std::cout << "isEqual: " << isEqual << std::endl;
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;

    delete vector1;
    delete vector2;
    delete vector3;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

TEST(varcharType, IsSameNodeFuncVarcharImplPerf){
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("varchar");
    VarcharVector *vector1 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);
    VarcharVector *vector2 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);

    for (int i = 0; i < ROW_SIZE; i++) {
        std::string  str = getString(i, 10, VAR_LEN);
        vector1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        vector2->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        vector1->SetValueNotNull(i);
        vector2->SetValueNotNull(i);
    }

    //Test perf
    std::cout << "Test times: " << ROW_SIZE << std::endl;
    std::cout << "varchar length: " << VAR_LEN << std::endl;

    Timer timer;
    timer.setStart();

    std::cout << "Compare same varchar: " << std::endl;
    double sum = 0;
    bool isEqual;
    for (int j = 0; j < ROUNDS; j++){
        timer.reset();

        for (int i = 0; i < ROW_SIZE; i++) {
            isEqual = hashagg_equal(vector1, i, vector2, i);
        }

        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse() * 1000;
        double cpu_elapsed = timer.getCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
        sum += wall_elapsed;
    }
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;
    std::cout << "isEqual: " << isEqual << std::endl;

    VarcharVector *vector3 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);
    for (int i = 0; i < ROW_SIZE; i++) {
        std::string  str = getString(i, 20, VAR_LEN);
        vector3->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        vector3->SetValueNotNull(i);
    }

    std::cout << "Compare different varchar: " << std::endl;
    sum = 0;
    for (int j = 0; j < ROUNDS; j++){
        timer.reset();

        for (int i = 0; i < ROW_SIZE; i++) {
            isEqual = hashagg_equal(vector1, i, vector3, i);
        }

        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse() * 1000;
        double cpu_elapsed = timer.getCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
        sum += wall_elapsed;
    }
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;
    std::cout << "isEqual: " << isEqual << std::endl;

    delete vector1;
    delete vector2;
    delete vector3;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}


