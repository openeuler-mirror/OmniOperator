/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: HMPP_hash_TEST operator test implementations
 */
#include <HMPP/hmpp.h>
#include <random>
#include "gtest/gtest.h"
#include "operator/hash_util.h"
#include "util/test_util.h"
#include "operator/hmpp_hash_util.h"

namespace HmppHashTest {
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace std;
using namespace omniruntime::TestUtil;

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
    int vectorSize = 10000000;
    auto *varcharVector = new Vector<LargeStringContainer<std::string_view>>(vectorSize, 1024);
    // Generate random varchar.
    srand((unsigned)time(nullptr));
    for (int i = 0; i < vectorSize; i++) {
        std::string str = GetRandomString(15);
        std::string_view stringView(str.data(), str.length());
        varcharVector->SetValue(i, stringView);
    }

    int64_t hashVal;
    auto *combinedHashOmni = new int64_t[varcharVector->GetSize()];
    for (int i = 0; i < varcharVector->GetSize(); i++) {
        combinedHashOmni[i] = 0;
    }

    Timer timer;
    timer.SetStart();
    timer.Reset();

    for (int i = 0; i < varcharVector->GetSize(); i++) {
        auto varcharValue = varcharVector->GetValue(i);
        hashVal = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(varcharValue.data())),
            varcharValue.length());
        combinedHashOmni[i] = HashUtil::CombineHash(combinedHashOmni[i], hashVal);
    }

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse() * 1000;
    double cpuElapsed = timer.GetCpuElapse() * 1000;
    std::cout << " wall-varchar:" << wallElapsed << " cpu-varchar:" << cpuElapsed << std::endl;

    int8_t *nullAddr = nullptr;
    auto *resultVarchar = new int64_t[varcharVector->GetSize()];
    auto *varcharVectorAddr = reinterpret_cast<const varchar *>(VectorHelper::GetValues(varcharVector, OMNI_VARCHAR));
    auto *offset = static_cast<int32_t *>(VectorHelper::GetOffsetsAddr(varcharVector, OMNI_VARCHAR));
    auto *combinedHashVarchar = new int64_t[varcharVector->GetSize()];
    for (int i = 0; i < varcharVector->GetSize(); i++) {
        combinedHashVarchar[i] = 0;
    }

    timer.Reset();
    HMPPS_Hash_varchar(varcharVectorAddr, offset, varcharVector->GetSize(), nullAddr, resultVarchar);
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
}

TEST(HmppHash, LongPerf)
{
    int vectorSize = 10000000;
    // long combiehash
    int64_t hashVal;
    auto *longVector = new Vector<int64_t>(vectorSize);
    auto *combinedHashLong = new int64_t[longVector->GetSize()]();
    auto *combinedHashOmni = new int64_t[longVector->GetSize()]();
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

    int8_t *nullAddr = nullptr;
    auto *resultLong = new int64_t[longVector->GetSize()];

    timer.Reset();

    HMPPS_Hash_64s(reinterpret_cast<const int64_t *>(VectorHelper::GetValues(longVector, OMNI_LONG)),
        longVector->GetSize(), nullAddr, resultLong);
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
}

TEST(HmppHash, DoublePerf)
{
    int vectorSize = 10000000;
    auto *doubleVector = new Vector<double>(vectorSize);
    // Generate double random number.
    uniform_real_distribution<double> dist(DBL_MIN, DBL_MAX);
    default_random_engine rd;
    for (int i = 0; i < doubleVector->GetSize(); i++) {
        doubleVector->SetValue(i, dist(rd));
    }
    int64_t hashVal;

    auto *combinedHashDouble = new int64_t[doubleVector->GetSize()];
    auto *combinedHashOmni = new int64_t[doubleVector->GetSize()];
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
    auto *resultDouble = new int64_t[doubleVector->GetSize()]();

    auto *nullAddr = reinterpret_cast<int8_t *>(unsafe::UnsafeBaseVector::GetNulls(doubleVector));

    timer.Reset();

    HMPPS_Hash_64f(reinterpret_cast<const double *>(VectorHelper::GetValues(doubleVector, OMNI_DOUBLE)),
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
}

TEST(HmppHash, Decimal128hashtest)
{
    auto *decimal128Vector = new Vector<Decimal128>(4);
    auto *value = new Decimal128[4 * 2];
    for (int i = 0; i < 4; i++) {
        value[i * 2] = 12;
        value[i * 2 + 1] = i * 2;
    }
    for (int32_t j = 0; j < 4; ++j) {
        decimal128Vector->SetValue(j, value[j]);
    }

    int64_t hashVal;
    auto *combinedHashDecimal128 = new int64_t[decimal128Vector->GetSize()];
    auto *combinedHashOmni = new int64_t[decimal128Vector->GetSize()];
    for (int i = 0; i < decimal128Vector->GetSize(); i++) {
        combinedHashOmni[i] = 0;
    }
    for (int i = 0; i < decimal128Vector->GetSize(); i++) {
        combinedHashDecimal128[i] = 0;
    }

    for (long i = 0; i < decimal128Vector->GetSize(); i++) {
        if (decimal128Vector->IsNull(i)) {
            continue;
        }
        hashVal =
            HashUtil::HashValue(decimal128Vector->GetValue(i).LowBits(), decimal128Vector->GetValue(i).HighBits());
        combinedHashOmni[i] = HashUtil::CombineHash(combinedHashOmni[i], hashVal);
    }


    // hmpp
    auto *resultDecimal128 = new int64_t[decimal128Vector->GetSize()]();
    for (int i = 0; i < decimal128Vector->GetSize(); i++) {
        combinedHashDecimal128[i] = 0;
    }
    auto *nullAddr = reinterpret_cast<int8_t *>(unsafe::UnsafeBaseVector::GetNulls(decimal128Vector));
    auto *decimalAddr =
        reinterpret_cast<const HmppDecimal128 *>(VectorHelper::GetValues(decimal128Vector, OMNI_DECIMAL128));
    HMPPS_Hash_decimal128(decimalAddr, decimal128Vector->GetSize(), nullAddr, resultDecimal128);
    HMPPS_CombineHash(combinedHashDecimal128, resultDecimal128, decimal128Vector->GetSize(), combinedHashDecimal128);

    std::cout << "compare: ***********" << std::endl;
    EXPECT_TRUE(MatchHash(decimal128Vector->GetSize(), combinedHashOmni, combinedHashDecimal128));

    delete[] value;
    delete[] combinedHashDecimal128;
    delete[] combinedHashOmni;
    delete[] resultDecimal128;
    delete decimal128Vector;
}

TEST(HmppHash, IntPerf)
{
    int vectorSize = 10000000;
    auto *intVector = new Vector<int32_t>(vectorSize);
    // Generate int random number.
    std::uniform_int_distribution<int> dist(INT32_MIN, INT32_MAX);
    std::default_random_engine rd;
    for (int i = 0; i < intVector->GetSize(); i++) {
        intVector->SetValue(i, dist(rd));
    }

    int64_t hashVal;

    auto *combinedHashInt = new int64_t[intVector->GetSize()];
    auto *combinedHashOmni = new int64_t[intVector->GetSize()];
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
    int8_t *nullAddr = nullptr;
    auto *resultInt = new int64_t[intVector->GetSize()];

    timer.Reset();
    HMPPS_Hash_32s(reinterpret_cast<const int32_t *>(VectorHelper::GetValues(intVector, OMNI_INT)),
        intVector->GetSize(), nullAddr, resultInt);
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
}

TEST(HmppHash, Boolhashtest)
{
    int vectorSize = 10000000;
    auto *booleanVector = new Vector<bool>(vectorSize);
    for (int i = 0; i < booleanVector->GetSize(); i++) {
        booleanVector->SetValue(i, i % 2 == 0);
    }

    int64_t hashVal;
    auto *combinedHashBool = new int64_t[booleanVector->GetSize()];
    auto *combinedHashOmni = new int64_t[booleanVector->GetSize()];
    for (int i = 0; i < booleanVector->GetSize(); i++) {
        combinedHashOmni[i] = 0;
    }
    for (int i = 0; i < booleanVector->GetSize(); i++) {
        combinedHashBool[i] = 0;
    }

    for (long i = 0; i < booleanVector->GetSize(); i++) {
        if (booleanVector->IsNull(i)) {
            continue;
        }
        hashVal = HashUtil::HashValue(booleanVector->GetValue(i));
        combinedHashOmni[i] = HashUtil::CombineHash(combinedHashOmni[i], hashVal);
    }

    // hmpp
    auto *resultBool = new int64_t[booleanVector->GetSize()]();

    auto *nullAddr = reinterpret_cast<int8_t *>(unsafe::UnsafeBaseVector::GetNulls(booleanVector));
    bool *boolAddr = static_cast<bool *>(VectorHelper::GetValues(booleanVector, OMNI_BOOLEAN));
    HMPPS_Hash_bool(boolAddr, booleanVector->GetSize(), nullAddr, resultBool);
    HMPPS_CombineHash(combinedHashBool, resultBool, booleanVector->GetSize(), combinedHashBool);

    std::cout << "compare: ***********" << std::endl;
    EXPECT_TRUE(MatchHash(booleanVector->GetSize(), combinedHashOmni, combinedHashBool));

    delete[] combinedHashBool;
    delete[] combinedHashOmni;
    delete[] resultBool;
    delete booleanVector;
}

TEST(HmppHash, VarcharSlicetest)
{
    int vectorSize = 1000;
    auto *varcharVector = new Vector<LargeStringContainer<std::string_view>>(vectorSize, 1024);
    // hash vachar test
    std::string str;
    srand((unsigned)time(nullptr));
    for (int i = 0; i < vectorSize; i++) {
        str = GetRandomString(15);
        std::string_view stringView(str.data(), str.length());
        varcharVector->SetValue(i, stringView);
    }
    auto *varcharSliceVector = varcharVector->Slice(10, 100).release();
    int64_t hashVal;
    auto *combinedHashOmni = new int64_t[varcharSliceVector->GetSize()];
    for (int i = 0; i < varcharSliceVector->GetSize(); i++) {
        combinedHashOmni[i] = 0;
    }

    for (int i = 0; i < varcharSliceVector->GetSize(); i++) {
        auto varcharValue = varcharSliceVector->GetValue(i);
        hashVal = HashUtil::HashValue(reinterpret_cast<int8_t *>(const_cast<char *>(varcharValue.data())),
            varcharValue.length());
        combinedHashOmni[i] = HashUtil::CombineHash(combinedHashOmni[i], hashVal);
    }

    int8_t *nullAddr = nullptr;
    auto *resultVarchar = new int64_t[varcharSliceVector->GetSize()];
    auto *varcharSliceVectorAddr =
        reinterpret_cast<const varchar *>(VectorHelper::GetValues(varcharSliceVector, OMNI_VARCHAR));
    auto *offset = static_cast<int32_t *>(VectorHelper::GetOffsetsAddr(varcharSliceVector, OMNI_VARCHAR));
    auto *combinedHashVarchar = new int64_t[varcharSliceVector->GetSize()];
    for (int i = 0; i < varcharSliceVector->GetSize(); i++) {
        combinedHashVarchar[i] = 0;
    }

    HMPPS_Hash_varchar(varcharSliceVectorAddr, offset, varcharSliceVector->GetSize(), nullAddr, resultVarchar);
    HMPPS_CombineHash(combinedHashVarchar, resultVarchar, varcharSliceVector->GetSize(), combinedHashVarchar);

    EXPECT_TRUE(MatchHash(varcharSliceVector->GetSize(), combinedHashOmni, combinedHashVarchar));

    delete[] combinedHashVarchar;
    delete[] combinedHashOmni;
    delete[] resultVarchar;
    delete varcharVector;
    delete varcharSliceVector;
}
}