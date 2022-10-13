/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: HMPP_hash_TEST operator test implementations
 */
#include <HMPP/hmpp.h>
#include <random>
#include "gtest/gtest.h"
#include "operator/hash_util.h"
#include "../util/test_util.h"

namespace HmppHashTest {
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;
using namespace TestUtil;

bool MatchHash(int len, int64_t *combinedHashOmni, int64_t *combinedHash)
{
    for (int i = 0; i < len; i++) {
        if (combinedHashOmni[i] != combinedHash[i]) {
            std::cout << "CombineHash: " << combinedHash[i] << std::endl;
            std::cout << "combinedHashOmni: " << combinedHashOmni[i] << std::endl;
            return false;
        }
    }
    return true;
}

std::string GetRandomString(int len)
{
    std::string str = "";
    for (int i = 1; i <= len; i++) {
        int flag = rand() % 2;
        if (flag == 1) {
            str += rand() % ('Z' - 'A' + 1) + 'A';
        } else {
            str += rand() % ('z' - 'a' + 1) + 'a';
        }
    }
    return str;
}

TEST(HmppHash, VarcharPerf)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_hmpp");
    int vectorSize = 10000000;
    VarcharVector *varcharVector = new VarcharVector(allocator, 1024, vectorSize);
    // Generate random varchar.
    std::string s;
    srand((unsigned)time(nullptr));
    for (int i = 0; i < vectorSize; i++) {
        s = GetRandomString(15);
        varcharVector->SetValue(i, reinterpret_cast<const uint8_t *>(s.c_str()), s.length());
    }

    int64_t hashVal;
    uint8_t *varcharValue = nullptr;
    int32_t valueLength;
    int64_t *combinedHashOmni = new int64_t[varcharVector->GetSize()];
    for (int i = 0; i < varcharVector->GetSize(); i++) {
        combinedHashOmni[i] = 0;
    }

    Timer timer;
    timer.SetStart();
    timer.Reset();

    for (int i = 0; i < varcharVector->GetSize(); i++) {
        varcharValue = nullptr;
        valueLength = static_cast<VarcharVector *>(varcharVector)->GetValue(i, &varcharValue);
        hashVal = HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
        combinedHashOmni[i] = HashUtil::CombineHash(combinedHashOmni[i], hashVal);
    }

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse() * 1000;
    double cpuElapsed = timer.GetCpuElapse() * 1000;
    std::cout << " wall-varchar:" << wallElapsed << " cpu-varchar:" << cpuElapsed << std::endl;

    int8_t *nullAddr = nullptr;
    int32_t positionOffset = varcharVector->GetPositionOffset();
    int64_t *resultVarchar = new int64_t[varcharVector->GetSize()];
    uint8_t *varcharVectorAddr = static_cast<uint8_t *>((varcharVector)->GetValues());
    int32_t *offest = static_cast<int32_t *>((varcharVector)->GetValueOffsets()) + positionOffset;
    int64_t *combinedHashVarchar = new int64_t[varcharVector->GetSize()];
    for (int i = 0; i < varcharVector->GetSize(); i++) {
        combinedHashVarchar[i] = 0;
    }

    timer.Reset();
    HMPPS_Hash_varchar(varcharVectorAddr, offest, varcharVector->GetSize(), nullAddr, resultVarchar);
    HMPPS_CombineHash(combinedHashVarchar, resultVarchar, varcharVector->GetSize(), combinedHashVarchar);

    timer.CalculateElapse();
    wallElapsed = timer.GetWallElapse() * 1000;
    cpuElapsed = timer.GetCpuElapse() * 1000;
    std::cout << " wall-varchar-hmmpp:" << wallElapsed << " cpu-varchar-hmmpp:" << cpuElapsed << std::endl;
    EXPECT_TRUE(MatchHash(varcharVector->GetSize(), combinedHashOmni, combinedHashVarchar));

    delete[] combinedHashVarchar;
    delete[] combinedHashOmni;
    delete[] resultVarchar;
    delete varcharVector;
    delete allocator;
}

TEST(HmppHash, LongPerf)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("LongVector_hmpp");
    int vectorSize = 10000000;
    // long combiehash
    int64_t hashVal;
    int8_t *nullAddr = nullptr;
    LongVector *longVector = new LongVector(allocator, vectorSize);
    int64_t *combinedHashLong = new int64_t[longVector->GetSize()]();
    int64_t *combinedHashOmni = new int64_t[longVector->GetSize()]();
    // Generate long random number.
    std::uniform_int_distribution<long long> dist(INT32_MIN, INT32_MAX);
    std::default_random_engine rd;
    for (int i = 0; i < longVector->GetSize(); i++) {
        longVector->SetValue(i, dist(rd));
    }
    for (int i = 0; i < longVector->GetSize(); i++) {
        combinedHashOmni[i] = 2;
    }
    for (int i = 0; i < longVector->GetSize(); i++) {
        combinedHashLong[i] = 2;
    }

    Timer timer;
    timer.SetStart();
    timer.Reset();

    for (long i = 0; i < longVector->GetSize(); i++) {
        hashVal = HashUtil::HashValue(longVector->GetValue(i));
        combinedHashOmni[i] = HashUtil::CombineHash(combinedHashOmni[i], hashVal);
    }

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse() * 1000;
    double cpuElapsed = timer.GetCpuElapse() * 1000;
    std::cout << " wall-long:" << wallElapsed << " cpu-long:" << cpuElapsed << std::endl;

    nullAddr = nullptr;
    int64_t *resultLong = new int64_t[longVector->GetSize()];
    int32_t positionOffset = longVector->GetPositionOffset();

    timer.Reset();

    HMPPS_Hash_64s(reinterpret_cast<const int64_t *>(longVector->GetValues()) + positionOffset, longVector->GetSize(),
        nullAddr, resultLong);
    HMPPS_CombineHash(combinedHashLong, resultLong, longVector->GetSize(), combinedHashLong);

    timer.CalculateElapse();
    wallElapsed = timer.GetWallElapse() * 1000;
    cpuElapsed = timer.GetCpuElapse() * 1000;
    std::cout << " wall-long-hmmpp:" << wallElapsed << " cpu-long-hmmpp:" << cpuElapsed << std::endl;

    std::cout << "compare: ***********" << std::endl;
    EXPECT_TRUE(MatchHash(longVector->GetSize(), combinedHashOmni, combinedHashLong));

    delete[] combinedHashLong;
    delete[] combinedHashOmni;
    delete[] resultLong;
    delete longVector;
    delete allocator;
}

TEST(HmppHash, DoublePerf)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("DoubleVector_hmpp");
    int vectorSize = 10000000;
    DoubleVector *doubleVector = new DoubleVector(allocator, vectorSize);
    // Generate double random number.
    uniform_real_distribution<double> dist(DBL_MIN, DBL_MAX);
    default_random_engine rd;
    for (int i = 0; i < doubleVector->GetSize(); i++) {
        doubleVector->SetValue(i, dist(rd));
    }
    int64_t hashVal;
    int8_t *nullAddr = nullptr;

    int64_t *combinedHashDouble = new int64_t[doubleVector->GetSize()];
    int64_t *combinedHashOmni = new int64_t[doubleVector->GetSize()];
    for (int i = 0; i < doubleVector->GetSize(); i++) {
        combinedHashOmni[i] = 0;
    }
    for (int i = 0; i < doubleVector->GetSize(); i++) {
        combinedHashDouble[i] = 0;
    }

    Timer timer;
    timer.SetStart();
    timer.Reset();

    for (long i = 0; i < doubleVector->GetSize(); i++) {
        hashVal = HashUtil::HashValue(doubleVector->GetValue(i));
        combinedHashOmni[i] = HashUtil::CombineHash(combinedHashOmni[i], hashVal);
    }

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse() * 1000;
    double cpuElapsed = timer.GetCpuElapse() * 1000;
    std::cout << " wall-double:" << wallElapsed << " cpu-double:" << cpuElapsed << std::endl;
    nullAddr = nullptr;
    int64_t *resultDouble = new int64_t[doubleVector->GetSize()]();

    int32_t positionOffset = doubleVector->GetPositionOffset();
    nullAddr = static_cast<int8_t *>(doubleVector->GetValueNulls());

    timer.Reset();

    HMPPS_Hash_64f(reinterpret_cast<const double *>(doubleVector->GetValues()) + positionOffset,
        doubleVector->GetSize(), nullAddr, resultDouble);
    HMPPS_CombineHash(combinedHashDouble, resultDouble, doubleVector->GetSize(), combinedHashDouble);

    timer.CalculateElapse();
    wallElapsed = timer.GetWallElapse() * 1000;
    cpuElapsed = timer.GetCpuElapse() * 1000;
    std::cout << " wall-double-hmmpp:" << wallElapsed << " cpu-double-hmmpp:" << cpuElapsed << std::endl;

    std::cout << "compare: ***********" << std::endl;
    EXPECT_TRUE(MatchHash(doubleVector->GetSize(), combinedHashOmni, combinedHashDouble));

    delete[] combinedHashDouble;
    delete[] combinedHashOmni;
    delete[] resultDouble;
    delete doubleVector;
    delete allocator;
}

TEST(HmppHash, Decimal128hashtest)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("Decimal128Vector_hmpp");

    Decimal128Vector *decimal128Vector = new Decimal128Vector(allocator, 4);
    auto *value = new uint64_t[4 * 2];
    for (int i = 0; i < 4; i++) {
        value[i * 2] = 12;
        value[i * 2 + 1] = i * 2;
    }
    decimal128Vector->SetValues(0, value, 4);

    int64_t hashVal;
    int8_t *nullAddr = nullptr;

    int64_t *combinedHashDecimal128 = new int64_t[decimal128Vector->GetSize()];
    int64_t *combinedHashOmni = new int64_t[decimal128Vector->GetSize()];
    for (int i = 0; i < decimal128Vector->GetSize(); i++) {
        combinedHashOmni[i] = 0;
    }
    for (int i = 0; i < decimal128Vector->GetSize(); i++) {
        combinedHashDecimal128[i] = 0;
    }

    for (long i = 0; i < decimal128Vector->GetSize(); i++) {
        if (decimal128Vector->IsValueNull(i)) {
            continue;
        }
        hashVal =
            HashUtil::HashValue(decimal128Vector->GetValue(i).LowBits(), decimal128Vector->GetValue(i).HighBits());
        combinedHashOmni[i] = HashUtil::CombineHash(combinedHashOmni[i], hashVal);
    }


    // hmpp
    nullAddr = nullptr;
    int64_t *resultDecimal128 = new int64_t[decimal128Vector->GetSize()]();
    for (int i = 0; i < decimal128Vector->GetSize(); i++) {
        combinedHashDecimal128[i] = 0;
    }
    nullAddr = static_cast<int8_t *>(decimal128Vector->GetValueNulls());
    HmppDecimal128 *decimalAddr = static_cast<HmppDecimal128 *>((decimal128Vector)->GetValues());
    HMPPS_Hash_decimal128(decimalAddr, decimal128Vector->GetSize(), nullAddr, resultDecimal128);
    HMPPS_CombineHash(combinedHashDecimal128, resultDecimal128, decimal128Vector->GetSize(), combinedHashDecimal128);

    std::cout << "compare: ***********" << std::endl;
    EXPECT_TRUE(MatchHash(decimal128Vector->GetSize(), combinedHashOmni, combinedHashDecimal128));

    delete[] value;
    delete[] combinedHashDecimal128;
    delete[] combinedHashOmni;
    delete[] resultDecimal128;
    delete decimal128Vector;
    delete allocator;
}

TEST(HmppHash, IntPerf)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("IntVector_hmpp");
    int vectorSize = 10000000;
    IntVector *intVector = new IntVector(allocator, vectorSize);
    // Generate int random number.
    std::uniform_int_distribution<int> dist(INT32_MIN, INT32_MAX);
    std::default_random_engine rd;
    for (int i = 0; i < intVector->GetSize(); i++) {
        intVector->SetValue(i, dist(rd));
    }

    int64_t hashVal;
    int8_t *nullAddr = nullptr;

    int64_t *combinedHashInt = new int64_t[intVector->GetSize()];
    int64_t *combinedHashOmni = new int64_t[intVector->GetSize()];
    for (int i = 0; i < intVector->GetSize(); i++) {
        combinedHashOmni[i] = 0;
    }
    for (int i = 0; i < intVector->GetSize(); i++) {
        combinedHashInt[i] = 0;
    }
    Timer timer;
    timer.SetStart();
    timer.Reset();
    for (long i = 0; i < intVector->GetSize(); i++) {
        hashVal = HashUtil::HashValue(intVector->GetValue(i));
        combinedHashOmni[i] = HashUtil::CombineHash(combinedHashOmni[i], hashVal);
    }
    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse() * 1000;
    double cpuElapsed = timer.GetCpuElapse() * 1000;
    std::cout << " wall-int:" << wallElapsed << " cpu-int:" << cpuElapsed << std::endl;

    // hmpp
    nullAddr = nullptr;
    int64_t *resultInt = new int64_t[intVector->GetSize()];

    int32_t positionOffset = intVector->GetPositionOffset();
    timer.Reset();
    HMPPS_Hash_32s(reinterpret_cast<const int32_t *>(intVector->GetValues()) + positionOffset, intVector->GetSize(),
        nullAddr, resultInt);
    HMPPS_CombineHash(combinedHashInt, resultInt, intVector->GetSize(), combinedHashInt);
    timer.CalculateElapse();
    wallElapsed = timer.GetWallElapse() * 1000;
    cpuElapsed = timer.GetCpuElapse() * 1000;
    std::cout << " wall-int-hmpp:" << wallElapsed << " cpu-int-hmpp:" << cpuElapsed << std::endl;

    std::cout << "compare: ***********" << std::endl;
    EXPECT_TRUE(MatchHash(intVector->GetSize(), combinedHashOmni, combinedHashInt));

    delete[] combinedHashInt;
    delete[] combinedHashOmni;
    delete[] resultInt;
    delete intVector;
    delete allocator;
}

TEST(HmppHash, Boolhashtest)
{
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("BoolVector_hmpp");
    int vectorSize = 10000000;
    BooleanVector *booleanVector = new BooleanVector(allocator, vectorSize);
    for (int i = 0; i < booleanVector->GetSize(); i++) {
        booleanVector->SetValue(i, i % 2 == 0);
    }

    int64_t hashVal;
    int8_t *nullAddr = nullptr;

    int64_t *combinedHashBool = new int64_t[booleanVector->GetSize()];
    int64_t *combinedHashOmni = new int64_t[booleanVector->GetSize()];
    for (int i = 0; i < booleanVector->GetSize(); i++) {
        combinedHashOmni[i] = 0;
    }
    for (int i = 0; i < booleanVector->GetSize(); i++) {
        combinedHashBool[i] = 0;
    }

    for (long i = 0; i < booleanVector->GetSize(); i++) {
        if (booleanVector->IsValueNull(i)) {
            continue;
        }
        hashVal = HashUtil::HashValue(booleanVector->GetValue(i));
        combinedHashOmni[i] = HashUtil::CombineHash(combinedHashOmni[i], hashVal);
    }

    // hmpp
    nullAddr = nullptr;
    int64_t *resultBool = new int64_t[booleanVector->GetSize()]();

    nullAddr = static_cast<int8_t *>(booleanVector->GetValueNulls());
    bool *boolAddr = static_cast<bool *>((booleanVector)->GetValues());
    int32_t positionOffset = booleanVector->GetPositionOffset();
    HMPPS_Hash_bool(boolAddr + positionOffset, booleanVector->GetSize(), nullAddr, resultBool);
    HMPPS_CombineHash(combinedHashBool, resultBool, booleanVector->GetSize(), combinedHashBool);

    std::cout << "compare: ***********" << std::endl;
    EXPECT_TRUE(MatchHash(booleanVector->GetSize(), combinedHashOmni, combinedHashBool));

    delete[] combinedHashBool;
    delete[] combinedHashOmni;
    delete[] resultBool;
    delete booleanVector;
    delete allocator;
}

TEST(HmppHash, VarcharSlicetest)
{
    int vectorSize = 1000;
    VectorAllocator *allocator = VectorAllocator::GetGlobalAllocator()->NewChildAllocator("VarcharVector_hmpp");
    VarcharVector *varcharVector = new VarcharVector(allocator, 1024, vectorSize);
    // hash vachar test
    std::string s;
    srand((unsigned)time(nullptr));
    for (int i = 0; i < vectorSize; i++) {
        s = GetRandomString(15);
        varcharVector->SetValue(i, reinterpret_cast<const uint8_t *>(s.c_str()), s.length());
    }
    VarcharVector *varcharSliceVector = varcharVector->Slice(10, 100);
    int64_t hashVal;
    uint8_t *varcharValue = nullptr;
    int32_t valueLength;
    int64_t *combinedHashOmni = new int64_t[varcharSliceVector->GetSize()];
    for (int i = 0; i < varcharSliceVector->GetSize(); i++) {
        combinedHashOmni[i] = 0;
    }

    for (int i = 0; i < varcharSliceVector->GetSize(); i++) {
        varcharValue = nullptr;
        valueLength = static_cast<VarcharVector *>(varcharSliceVector)->GetValue(i, &varcharValue);
        hashVal = HashUtil::HashValue(reinterpret_cast<int8_t *>(varcharValue), valueLength);
        combinedHashOmni[i] = HashUtil::CombineHash(combinedHashOmni[i], hashVal);
    }

    int8_t *nullAddr = nullptr;
    int32_t positionOffset = varcharSliceVector->GetPositionOffset();
    int64_t *resultVarchar = new int64_t[varcharSliceVector->GetSize()];
    uint8_t *varcharSliceVectorAddr = static_cast<uint8_t *>((varcharSliceVector)->GetValues());
    int32_t *offest = static_cast<int32_t *>((varcharSliceVector)->GetValueOffsets()) + positionOffset;
    int64_t *combinedHashVarchar = new int64_t[varcharSliceVector->GetSize()];
    for (int i = 0; i < varcharSliceVector->GetSize(); i++) {
        combinedHashVarchar[i] = 0;
    }

    HMPPS_Hash_varchar(varcharSliceVectorAddr, offest, varcharSliceVector->GetSize(), nullAddr, resultVarchar);
    HMPPS_CombineHash(combinedHashVarchar, resultVarchar, varcharSliceVector->GetSize(), combinedHashVarchar);

    EXPECT_TRUE(MatchHash(varcharSliceVector->GetSize(), combinedHashOmni, combinedHashVarchar));

    delete[] combinedHashVarchar;
    delete[] combinedHashOmni;
    delete[] resultVarchar;
    delete varcharVector;
    delete varcharSliceVector;
    delete allocator;
}
}