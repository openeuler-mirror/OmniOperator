#include "gtest/gtest.h"
#include "../src/jni/filter_api.h"
#include <iostream>
#include <cstring>

TEST (FilterTest, TestFilterCompile) {
    // simple unit test
    string filterExpression = "AND(AND($operator$GT(#3, 8766), $operator$LT(#3, 9131)), AND(BETWEEN(#2, 0.05, 0.07), $operator$LT(#0, 24.0)))";
    
    int32_t vecCount = 3;
    int64_t inputType = (int64_t)malloc(sizeof(int32_t)*vecCount);
    *((int32_t*)inputType) = 1;
    *(int32_t*)(inputType+sizeof(int32_t)) = 2;
    *(int32_t*)(inputType+sizeof(int32_t)*2) = 3;
    filterCompile(filterExpression, inputType, vecCount);

}