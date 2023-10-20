#include <arm_neon.h>
#include <iostream>
#include <functional>
#include "vector"
#include "chrono"
#include "src/operator/aggregation/neon_aggregation/neon_aggregation_external.h"
#include <gtest/gtest.h>
#include "time.h"
using namespace omniruntime::simd;

int32_t simdNoNull(int32_t *values, int32_t size){
    int64_t result = 0;
    int64_t flag = 0;
    SIMDAdd<int32_t,int64_t,BasicOp::Sum>(&result, flag, values, size);
    return result;
}
template<typename T>
T simdWithNull(T *values, uint8_t *nulls, int32_t size){
    int64_t result = 0;
    int64_t flag = 0;
    SIMDAddConditional<T,int64_t ,BasicOp::Sum>(&result, flag, values, size, nulls);
    return result;
}
template<typename T>
T gcc_sum_with_nulls(T *values, uint8_t *nulls, int32_t size) {
    int i = 0;
    int64_t result = 0;
    for (; i < size; i ++) {
        if (not nulls[i]){
            result += values[i];
        }
    }
    return result;
}
template<typename T>
T simdWithDict(int32_t size,  T* values, int32_t *indexs){

    int64_t result = 0;
    int64_t flag = 0;
    SIMDAddDict<T,int64_t,BasicOp::Sum>(&result, flag, values, size, indexs);
    return result;
}
template<typename T>
T simdWithDictWithNull(const T* values, const uint8_t * nulls, int32_t *indexs,int32_t size){

    double result = 0;
    int64_t flag = 0;
    SIMDAddDictConditional<T,double,BasicOp::Sum>(&result, flag, values, size, nulls,indexs);
    return result;
}
template<typename T>
T gccWithDictWithNull(const T* values, const uint8_t * nulls, int32_t *indexs,int32_t size){

    double result = 0;
    int64_t flag = 0;
    for(int i=0;i<size;++i){
        if(not nulls[i]){
            result+= values[indexs[i]];
            ++flag;
        }
    }
    return result;
}
template<typename T>
T gccSumDict(int n, T* data, int* idx)
{
    double total_sum = 0;
    for (int i = 0; i < n; i ++) {
        total_sum += data[idx[i]];
    }
    return total_sum;
}


int32_t gccTestNoNull(int32_t *values, int32_t size){
    int64_t result = 0;

    for(int i=0;i<size;++i){
        result+=values[i];
    }
    return result;
}

template<typename Ret, typename... Args>
int32_t calc(const std::string &name,
             Ret(*func)(Args...),
             Args... args){
    auto start = std::chrono::high_resolution_clock::now();
    float val = 0;
    for(int i=0;i<10000;i++){
        Ret ret = func(args...);
        val += ret;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout<<name<<" spend : "<< duration<<"ms,result is "<< val <<std::endl;
    return duration;
}
template<typename T>
void test_simd_with_null(int32_t size){
    T values[size];
    uint8_t nulls[size];
    for(int i=0;i<size;++i){
        values[i] = i * 1000;
        nulls[i] = i%200==0;
    }
    calc<T>("simdWithNull",simdWithNull,values,nulls, (size));
    calc<T>("gcc_sum_with_nulls",gcc_sum_with_nulls,values,nulls, (size));
}

template<typename T>
void test_simd_with_no_null(int32_t size){
    T values[size];
    for(int i=0;i<size;++i){
        values[i] = i * 1000;
    }
    calc<T>("simdNoNull",simdNoNull,values,(size));
    calc<T>("gccTestNoNull",gccTestNoNull,values,(size));
}

template<typename T>
T benchWithDict(int32_t size) {
    T values[size] ;
    int32_t indexs[size];
    for(int i=0;i<size;++i) {
        values[i] = i;
    }
    for(int i=0;i<size;++i) {
        indexs[i] = 4;
    }

    calc<T,int, T*, int32_t*>("simdWithDict",simdWithDict,(size), values,indexs);
    calc<T,int, T*, int32_t*>("gccSumDict", gccSumDict,(size),values,indexs);
}

template<typename T>
void test_simd_with_dict(int32_t size){
    int32_t indexs[size];
    T values[size];
    uint8_t nulls[size];
    for(int i=0;i<size;++i) {
        values[i] = i;
    }
    for(int i=0;i<size;++i) {
        indexs[i] = 4;
        nulls[i] = i %size == 0;
    }

    calc<T,const T* , const uint8_t *, int32_t *,int32_t>("simdWithDictWithNull",simdWithDictWithNull,values,nulls,indexs, (size));
    calc<T,const T*, const uint8_t *, int32_t *,int32_t>("gcc_sum_with_nulls",gccWithDictWithNull,values,nulls,indexs, (size));
}

void verify() {
    int32_t expect = 0;
    int32_t actual = 0;
    srand(time(nullptr));
    int labCount = 1000;
    while(labCount--) {
        int value = random() % 59999;
        int nullNum = random();
        if (nullNum < 0) {
            nullNum = -1 * nullNum;
        }
        nullNum %= value;
        std::vector<int32_t> data;
        data.resize(value);
        uint8_t nulls[value];
        int countNull = nullNum;
        for (int i = 0; i < value; i++) {
            if (countNull != 0 && rand() & 0x1) {
                nulls[i] = true;
                --countNull;
            } else {
                nulls[i] = false;
                data[i] = random() % 100;
            }
        }
        expect = 0;
        for (int i = 0; i < value; i++) {
            if (not nulls[i]) {
                expect += data[i];
            }
        }
        int64_t flag = 0;
        int32_t actual =0 ;
        SIMDAddConditional<int32_t, int32_t, BasicOp::Sum>(&actual, flag, data.data(), value, nulls);
        if (expect != actual) {
            for (int i = 0; i < value; ++i) {
                std::cout << data[i] << std::endl;
            }
            for (int i = 0; i < value; ++i) {
                std::cout << (nulls[i] == true) << std::endl;
            }
            EXPECT_EQ(expect,actual);
            break;
        }
    }

}

TEST(AggregatorNeonTest, int64_simd_neon)
{
    verify();
    test_simd_with_null<int64_t>(9999);
    test_simd_with_no_null<int32_t>(9999);
    test_simd_with_dict<int32_t>(9999);
}
