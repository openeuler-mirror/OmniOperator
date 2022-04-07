/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "../util/test_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;
using namespace TestUtil;

const int32_t ROW_SIZE = 1000000;
const int32_t VAR_LEN = 10;
const int32_t ROUNDS = 10;

const std::string STRING = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
static const int32_t SIZE_OF_LONG = 8;
static string GetString(int32_t index, int32_t offset, int32_t width)
{
    std::string str;
    for (int j = 0; j < width; j++) {
        char c = STRING.at((index + offset + j) % STRING.length());
        str.push_back(c);
    }
    return str;
}

static int32_t CompareVarchar(Vector *leftColumn, int32_t leftColumnPosition, Vector *rightColumn,
    int32_t rightColumnPosition)
{
    auto leftVarCharColumn = static_cast<VarcharVector *>(leftColumn);
    auto rightVarCharColumn = static_cast<VarcharVector *>(rightColumn);
    uint8_t *leftValue = nullptr;
    int32_t leftLength = leftVarCharColumn->GetValue(leftColumnPosition, &leftValue);
    uint8_t *rightValue = nullptr;
    int32_t rightLength = rightVarCharColumn->GetValue(rightColumnPosition, &rightValue);
    int32_t result = memcmp(leftValue, rightValue, std::min(leftLength, rightLength));
    if (result != 0) {
        return (result > 0) ? 1 : -1;
    } else if (leftLength == rightLength) {
        return 0;
    } else {
        return (leftLength > rightLength) ? 1 : -1;
    }
}

int64_t ReverseBytes(int64_t var0)
{
    auto result = static_cast<uint64_t>(var0);
    result = (result & 71777214294589695L) << 8 | (result >> 8 & 71777214294589695L);
    return result << 48 | (result & 4294901760L) << 16 | (result >> 16 & 4294901760L) | result >> 48;
}

int64_t LongBytesToLong(int64_t bytes)
{
    return ReverseBytes(bytes) ^ LONG_MIN;
}

int32_t CmpByLong(uint8_t *var1, uint8_t *var2, int32_t compareLength)
{
    auto *left = reinterpret_cast<int64_t *>(var1);
    auto *right = reinterpret_cast<int64_t *>(var2);
    while (compareLength >= 8) {
        int64_t leftVal = *left;
        int64_t rightVal = *right;
        if (leftVal != rightVal) {
            return (LongBytesToLong(leftVal) < LongBytesToLong(rightVal)) ? -1 : 1;
        }
        left++;
        right++;
        compareLength -= 8;
        var1 += 8;
        var2 += SIZE_OF_LONG;
    }
    while (compareLength > 0) {
        int8_t value1 = *(var1);
        int8_t value2 = *(var2);
        if (value1 != value2) {
            return value1 - value2;
        }
        var1++;
        var2++;
        compareLength--;
    }
    return 0;
}

int32_t CompareVarcharByLong(Vector *leftColumn, int32_t leftColumnPosition, Vector *rightColumn,
    int32_t rightColumnPosition)
{
    auto leftVarCharColumn = static_cast<VarcharVector *>(leftColumn);
    auto rightVarCharColumn = static_cast<VarcharVector *>(rightColumn);
    uint8_t *leftValue = nullptr;
    int32_t leftLength = leftVarCharColumn->GetValue(leftColumnPosition, &leftValue);
    uint8_t *rightValue = nullptr;
    int32_t rightLength = rightVarCharColumn->GetValue(rightColumnPosition, &rightValue);
    int32_t result = CmpByLong(leftValue, rightValue, std::min(leftLength, rightLength));
    if (result != 0) {
        return (result > 0) ? 1 : -1;
    } else if (leftLength == rightLength) {
        return 0;
    } else {
        return (leftLength > rightLength) ? 1 : -1;
    }
}

TEST(varcharType, CompareVarcharPerf)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("varchar");
    VarcharVector *vector1 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);
    VarcharVector *vector2 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);

    for (int i = 0; i < ROW_SIZE; i++) {
        std::string str = GetString(i, 10, VAR_LEN);
        vector1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        vector2->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    std::cout << "Test times: " << ROW_SIZE << std::endl;
    std::cout << "varchar length: " << VAR_LEN << std::endl;

    Timer timer;
    timer.SetStart();

    std::cout << "Compare same varchar: " << std::endl;
    double sum = 0;
    int comp;
    for (int j = 0; j < ROUNDS; j++) {
        timer.Reset();

        for (int i = 0; i < ROW_SIZE; i++) {
            comp = CompareVarchar(vector1, i, vector2, i);
        }

        timer.CalculateElapse();
        double wallElapsed = timer.GetWallElapse() * 1000;
        double cpuElapsed = timer.GetCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;
        sum += wallElapsed;
    }
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;
    std::cout << "comp: " << comp << std::endl;

    VarcharVector *vector3 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);
    for (int i = 0; i < ROW_SIZE; i++) {
        std::string str = GetString(i, 20, VAR_LEN);
        vector3->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    std::cout << "Compare different varchar:" << std::endl;
    sum = 0;
    for (int j = 0; j < ROUNDS; j++) {
        timer.Reset();

        for (int i = 0; i < ROW_SIZE; i++) {
            comp = CompareVarchar(vector1, i, vector3, i);
        }

        timer.CalculateElapse();
        double wallElapsed = timer.GetWallElapse() * 1000;
        double cpuElapsed = timer.GetCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;
        sum += wallElapsed;
    }
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;
    std::cout << "comp: " << comp << std::endl;

    delete vector1;
    delete vector2;
    delete vector3;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}

TEST(varcharType, CompareVarcharByLongPerf)
{
    VectorAllocator *allocator = VectorAllocatorFactory::GetOrCreateAllocator("varchar");
    VarcharVector *vector1 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);
    VarcharVector *vector2 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);

    for (int i = 0; i < ROW_SIZE; i++) {
        std::string str = GetString(i, 10, VAR_LEN);
        vector1->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
        vector2->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    // Test perf
    std::cout << "Test times: " << ROW_SIZE << std::endl;
    std::cout << "varchar length: " << VAR_LEN << std::endl;

    Timer timer;
    timer.SetStart();

    std::cout << "Compare equal varchar" << std::endl;
    double sum = 0;
    int comp;
    for (int j = 0; j < ROUNDS; j++) {
        timer.Reset();

        for (int i = 0; i < ROW_SIZE; i++) {
            comp = CompareVarcharByLong(vector1, i, vector2, i);
        }

        timer.CalculateElapse();
        double wallElapsed = timer.GetWallElapse() * 1000;
        double cpuElapsed = timer.GetCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;
        sum += wallElapsed;
    }
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;
    std::cout << "comp: " << comp << std::endl;

    VarcharVector *vector3 = new VarcharVector(allocator, ROW_SIZE * VAR_LEN, ROW_SIZE);
    for (int i = 0; i < ROW_SIZE; i++) {
        std::string str = GetString(i, 20, VAR_LEN);
        vector3->SetValue(i, reinterpret_cast<const uint8_t *>(str.c_str()), str.length());
    }

    std::cout << "Compare not equal varchar" << std::endl;
    sum = 0;
    for (int j = 0; j < ROUNDS; j++) {
        timer.Reset();

        for (int i = 0; i < ROW_SIZE; i++) {
            comp = CompareVarcharByLong(vector1, i, vector3, i);
        }

        timer.CalculateElapse();
        double wallElapsed = timer.GetWallElapse() * 1000;
        double cpuElapsed = timer.GetCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;
        sum += wallElapsed;
    }
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;
    std::cout << "comp: " << comp << std::endl;

    delete vector1;
    delete vector2;
    delete vector3;
    VectorAllocatorFactory::DeleteAllocator(&allocator);
}
