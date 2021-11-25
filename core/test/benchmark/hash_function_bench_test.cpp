/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

#include "gtest/gtest.h"
#include "../util/test_util.h"
#include "../../src/operator/hash_util.h"

using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;

const int32_t ROW_SIZE = 1000000;
const int32_t VAR_LEN = 10;
const int32_t ROUNDS = 10;
const int64_t START = 0;
const int64_t END = 1000000;

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

TEST(varcharType, VarcharHashPerf){
    std::vector<string> vec;
    for (int i = 0; i < ROW_SIZE; i++) {
        string str = getString(i, 10, VAR_LEN);
        vec.emplace_back(str);
    }

    //Test perf
    std::cout << "Test times: " << ROW_SIZE << std::endl;
    std::cout << "varchar length: " << VAR_LEN << std::endl;

    Timer timer;
    timer.setStart();

    std::hash<std::string> hashVarChar;
    std::cout << "std::hash()" << std::endl;

    double sum = 0;
    int hashVal;
    for (int j = 0; j < ROUNDS; j++){
        timer.reset();

        for (int i = 0; i < ROW_SIZE; i++) {
            hashVal = hashVarChar(vec[i]);
        }

        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse() * 1000;
        double cpu_elapsed = timer.getCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
        sum += wall_elapsed;
    }
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;
    std::cout << "hashVal: " << hashVal << std::endl;

    std::cout << "HashUtil::HashValue()" << std::endl;
    sum = 0;
    for (int j = 0; j < ROUNDS; j++ ){
        timer.reset();

        for (int i = 0; i < ROW_SIZE; i++) {
            hashVal = HashUtil::HashValue((int8_t *) vec[i].c_str(), VAR_LEN);
        }

        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse() * 1000;
        double cpu_elapsed = timer.getCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
        sum += wall_elapsed;
    }
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;
    std::cout << "hashVal: " << hashVal << std::endl;
}

TEST(LongType, LongHashPerf) {
    std::cout << "Test times: " << ROW_SIZE << std::endl;
    std::cout << "long scope: [" << START << ", " << START + ROW_SIZE << "]" << std::endl;

    Timer timer;
    timer.setStart();

    std::hash<long> hashLong;
    std::cout << "std::hash()" << std::endl;

    double sum = 0;
    long hashVal;
    for (int j = 0; j < ROUNDS; j++) {
        timer.reset();

        for (long i = START; i < END; i++) {
            hashVal = hashLong(i);
        }

        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse() * 1000;
        double cpu_elapsed = timer.getCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
        sum += wall_elapsed;
    }
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;
    std::cout << "hashVal: " << hashVal << std::endl;

    std::cout << "HashUtil::HashValue()" << std::endl;
    sum = 0;
    for (int j = 0; j < ROUNDS; j++) {
        timer.reset();

        for (long i = START; i < END; i++) {
            hashVal = HashUtil::HashValue(i);
        }

        timer.calculateElapse();
        double wall_elapsed = timer.getWallElapse() * 1000;
        double cpu_elapsed = timer.getCpuElapse() * 1000;
        std::cout << "round: " << j + 1 << " wall " << wall_elapsed << " cpu " << cpu_elapsed << std::endl;
        sum += wall_elapsed;
    }
    std::cout << "avg: " << sum / ROUNDS << " ms" << std::endl;
    std::cout << "hashVal: " << hashVal << std::endl;
}