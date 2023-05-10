/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "gtest/gtest.h"
#include "memory_manager.h"
#include "memory/thread_memory_manager.h"
#include "vector/vector.h"
#include "jemalloc/jemalloc.h"
#include "boost/multiprecision/cpp_dec_float.hpp"
#include "boost/multiprecision/number.hpp"
#include "vector/vector_helper.h"

namespace omniruntime::vec {
using boost_dec64 = boost::multiprecision::number<boost::multiprecision::cpp_dec_float<64>>;


void TestUnlimitedAccountMultipleThreads(int count, int64_t size)
{
    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto currentMemoryManager = std::make_unique<mem::MemoryManager>(globalMemoryManager);
    EXPECT_TRUE(currentMemoryManager != nullptr);

    for (int i = 0; i < count; ++i) {
        currentMemoryManager->Account(size);
    }
    int64_t currentMemoryAmount = currentMemoryManager->GetMemoryAmount();
    EXPECT_EQ(currentMemoryAmount, count * size);
}

// test: unlimited account under multiple threads
TEST(MemoryManager, testUnlimitedAccountMultipleThreads)
{
    int processorCount = static_cast<int>(std::thread::hardware_concurrency());
    std::cout << "core number: " << processorCount << std::endl;
    int threadNums[] = {2, 4, 8, 16};
    for (uint32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
        globalMemoryManager->Clear();

        int threadNum = threadNums[i] <= processorCount ? threadNums[i] : processorCount;

        std::vector<std::thread> memoryMangerOfThreads;
        int count = 10;
        int64_t positiveSize = 1 * 1024 * 1024;
        for (int j = 0; j < threadNum; ++j) {
            std::thread t(TestUnlimitedAccountMultipleThreads, count, positiveSize);
            memoryMangerOfThreads.push_back(std::move(t));
        }
        for (auto &th : memoryMangerOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        int64_t globalMemoryAmount = globalMemoryManager->GetMemoryAmount();

        EXPECT_EQ(globalMemoryAmount, threadNum * count * positiveSize);

        int64_t negativeSize = -1 * 1024 * 1024;
        for (int j = 0; j < threadNum; ++j) {
            std::thread t(TestUnlimitedAccountMultipleThreads, count, negativeSize);
            memoryMangerOfThreads.push_back(std::move(t));
        }
        for (auto &th : memoryMangerOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        globalMemoryAmount = globalMemoryManager->GetMemoryAmount();

        EXPECT_EQ(globalMemoryAmount, 0);
    }
}

void testLimitedAccountMultipleThreads(int count, int64_t size)
{
    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto currentMemoryManager = std::make_unique<mem::MemoryManager>(globalMemoryManager);
    EXPECT_TRUE(currentMemoryManager != nullptr);

    for (int i = 0; i < count; ++i) {
        try {
            currentMemoryManager->Account(size);
        } catch (exception::OmniException &e) {
            break;
        }
    }
}

// test: limited account under multiple threads
TEST(MemoryManager, testLimitedAccountMultipleThreads)
{
    int64_t globalThreshold = 10 * 1024 * 1024;
    int processorCount = static_cast<int>(std::thread::hardware_concurrency());
    std::cout << "core number: " << processorCount << std::endl;
    int threadNums[] = {2, 4, 8, 16};
    for (uint32_t i = 0; i < sizeof(threadNums) / sizeof(int); ++i) {
        auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
        globalMemoryManager->Clear();

        mem::MemoryManager::SetGlobalMemoryLimit(globalThreshold);
        int threadNum = threadNums[i] <= processorCount ? threadNums[i] : processorCount;

        std::vector<std::thread> memoryMangerOfThreads;
        int count = 20;
        int64_t positiveSize = 1 * 1024 * 1024;
        for (int j = 0; j < threadNum; ++j) {
            std::thread t(testLimitedAccountMultipleThreads, count, positiveSize);
            memoryMangerOfThreads.push_back(std::move(t));
        }
        for (auto &th : memoryMangerOfThreads) {
            if (th.joinable()) {
                th.join();
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        int64_t globalMemoryAmount = globalMemoryManager->GetMemoryAmount();

        EXPECT_GE(globalMemoryAmount, globalThreshold);
    }
}

template <class T> auto CreateVector(int vecSize)
{
    auto vector = std::make_unique<Vector<T>>(vecSize);
    return vector;
}

// In coverage mode, the memory allocation is taken over by the Asan, and the memory size is not aligned.
// Conversely, The memory allocation is taken over by the jemalloc, and the memory size is aligned.
#ifdef COVERAGE
// test: accuracy of memory statistics
TEST(MemoryManager, testAsanStatisticsFunction)
{
    // Avoid interference between UTs.
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();


    // actual mem 564 = vector size(64) + null size(100) + value size(400). Take null as an example,
    // 100 indicates the overhead of new bool[100].
    auto int32Vector = CreateVector<int32_t>(100);
    int64_t threadUntracked = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(threadUntracked, 564);

    // actual mem  964 = vector size(64) + null size(100) + value size(800)
    auto int64Vector = CreateVector<int64_t>(100);
    threadUntracked = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(threadUntracked, 1528); // int64Vector + int32Vector

    // actual mem 964 = vector size(64) + null size(100) + value size(800)
    auto doubleVector = CreateVector<double>(100);
    threadUntracked = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(threadUntracked, 2492); // doubleVector + int64Vector + int32Vector

    // actual mem 6164 = vector size(64) + null size(100) + value size(6000)
    auto dec64Vector = CreateVector<boost_dec64>(100);
    threadUntracked = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(threadUntracked, 8656); // total size include vector above

    int32Vector.reset();
    int64Vector.reset();
    doubleVector.reset();
    dec64Vector.reset();

    int64_t finalSize = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(finalSize, 0);

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    globalMemoryManager->Clear();
}

// test: statistics function of Limit
TEST(MemoryManager, testAsanStatisticsFunctionMemoryLimit)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    int size = 1024 * 1024;
    int limit = 10 * 1024 * 1024;
    globalMemoryManager->SetMemoryLimit(limit);
    auto vector1 = std::make_unique<Vector<int32_t>>(size);
    int64_t globalMemoryAmount = globalMemoryManager->GetMemoryAmount();
    // 5242944 = vector size(64) + null size(1048576) + value size(4194304),
    EXPECT_EQ(globalMemoryAmount, 5242944);
    // it is equivalent to "auto vector = std::make_unique<Vector<int32_t>>(size)"
    auto currentMemoryManager = std::make_unique<mem::MemoryManager>(globalMemoryManager);
    EXPECT_ANY_THROW(currentMemoryManager->Account(globalMemoryAmount));

    globalMemoryAmount = globalMemoryManager->GetMemoryAmount();
    EXPECT_EQ(globalMemoryAmount, 10485888);
}
#else
TEST(MemoryManager, testJemallocStatisticsFunction)
{
    // Avoid interference between UTs.
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    // alignment mem 688 = vector size(64) + null size(112) + value size(448). Take null as an example,
    // 112 indicates the overhead of new bool[100].
    auto int32Vector = CreateVector<int32_t>(100);
    int64_t threadUntracked = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(threadUntracked, 624);

    // alignment mem  1072 = vector size(64) + null size(112) + value size(896)
    auto int64Vector = CreateVector<int64_t>(100);
    threadUntracked = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(threadUntracked, 1696); // int64Vector + int32Vector

    // alignment mem  1072 = vector size(64) + null size(112) + value size(896)
    auto doubleVector = CreateVector<double>(100);
    threadUntracked = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(threadUntracked, 2768); // doubleVector + int64Vector + int32Vector

    // alignment mem 6320 = vector size(64) + null size(112) + value size(6144)
    auto dec64Vector = CreateVector<boost_dec64>(100);
    threadUntracked = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(threadUntracked, 9088); // total size include vector above

    int32Vector.reset();
    int64Vector.reset();
    doubleVector.reset();
    dec64Vector.reset();

    int64_t finalSize = threadMemoryManager->GetUntrackedMemory();
    EXPECT_EQ(finalSize, 0);

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    globalMemoryManager->Clear();
}

// test: statistics function of Limit
TEST(MemoryManager, testjemallocStatisticsFunctionMemoryLimit)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    int size = 1024 * 1024;
    int limit = 10 * 1024 * 1024;
    globalMemoryManager->SetMemoryLimit(limit);
    auto vector1 = std::make_unique<Vector<int32_t>>(size);
    int64_t globalMemoryAmount = globalMemoryManager->GetMemoryAmount();
    // 5242944 = vector size(64) + null size(1048576) + value size(4194304),
    EXPECT_EQ(globalMemoryAmount, 5242944);
    // it is equivalent to "auto vector = std::make_unique<Vector<int32_t>>(size)"
    auto currentMemoryManager = std::make_unique<mem::MemoryManager>(globalMemoryManager);
    EXPECT_ANY_THROW(currentMemoryManager->Account(globalMemoryAmount));

    globalMemoryAmount = globalMemoryManager->GetMemoryAmount();
    EXPECT_EQ(globalMemoryAmount, 10485888);
}
#endif

TEST(MemoryManager, testFixedVectorStatisticsFunction)
{
    // Avoid interference between UTs.
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto int32Vector = std::make_unique<Vector<int32_t>>(1000);
    auto slicedIntVector = int32Vector->Slice(0, 100);
    int32Vector.reset();
    slicedIntVector.reset();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto globalMemoryAmount = globalMemoryManager->GetMemoryAmount();
    EXPECT_EQ(globalMemoryAmount, 0);
}

TEST(MemoryManager, testVarcharVectorStatisticsFunction)
{
    // Avoid interference between UTs.
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto varcharVector = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(1000);
    auto slicedVarcharVector = varcharVector->Slice(0, 100);
    varcharVector.reset();
    slicedVarcharVector.reset();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto globalMemoryAmount = globalMemoryManager->GetMemoryAmount();
    EXPECT_EQ(globalMemoryAmount, 0);
}

TEST(MemoryManager, testDictionaryVarcharVectorStatisticsFunction)
{
    // Avoid interference between UTs.
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    int32_t dicSize = 1000;
    int32_t valueSize = 7;
    auto originVec = std::make_unique<Vector<LargeStringContainer<std::string_view>>>(dicSize);
    for (int32_t i = 0; i < dicSize; ++i) {
        if (i % 2 == 0) {
            originVec->SetNull(i);
            continue;
        }
        auto str = "string " + std::to_string(i);
        std::string_view value(str.data(), str.length());
        originVec->SetValue(i, value);
    }

    int32_t values[] = {2, 3, 4, 5, 6, 8, 9};
    std::shared_ptr<bool[]> nulls = std::shared_ptr<bool[]>(new bool[valueSize]);
    for (int i = 0; i < valueSize; i++) {
        nulls[i] = originVec->IsNull(values[i]);
    }

    auto dictionary = std::make_shared<DictionaryContainer<std::string_view>>(values, valueSize,
            unsafe::UnsafeStringVector::GetContainer(originVec.get()), originVec->GetSize(), originVec->GetOffset());
    auto dictionaryVarcharVector = std::make_unique<Vector<DictionaryContainer<std::string_view>>>(valueSize,
            dictionary, nulls);
    auto slicedDictionaryVarcharVector = dictionaryVarcharVector->Slice(0, 6);

    nulls.reset();
    originVec.reset();
    dictionary.reset();
    dictionaryVarcharVector.reset();
    slicedDictionaryVarcharVector.reset();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto globalMemoryAmount = globalMemoryManager->GetMemoryAmount();
    EXPECT_EQ(globalMemoryAmount, 0);
}

TEST(MemoryManager, testDictionaryFixedVectorStatisticsFunction)
{
    // Avoid interference between UTs.
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    int32_t dicSize = 1000;
    int32_t valueSize = 7;
    auto originVec = std::make_unique<Vector<long>>(dicSize);

    int32_t values[] = {2, 3, 4, 5, 6, 8, 9};
    std::shared_ptr<bool[]> nulls = std::shared_ptr<bool[]>(new bool[valueSize]);
    auto dictionary = std::make_shared<DictionaryContainer<long>>(values, valueSize,
               unsafe::UnsafeVector::GetValues<long>(originVec.get()), originVec->GetSize(), originVec->GetOffset());
    auto dictionaryFixedVector = std::make_unique<Vector<DictionaryContainer<long>>>(valueSize, dictionary, nulls);
    auto slicedDictionaryFixedVector = dictionaryFixedVector->Slice(0, 6);

    nulls.reset();
    originVec.reset();
    dictionary.reset();
    dictionaryFixedVector.reset();
    slicedDictionaryFixedVector.reset();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto globalMemoryAmount = globalMemoryManager->GetMemoryAmount();
    EXPECT_EQ(globalMemoryAmount, 0);
}

// test: set global memory limit
TEST(MemoryManager, testSetGlobalMemoryLimit)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    int64_t limit = 10 * 1024 * 1024;
    mem::MemoryManager::SetGlobalMemoryLimit(limit);
    int64_t actualLimit = globalMemoryManager->GetMemoryLimit();
    EXPECT_EQ(actualLimit, limit);
}

// test: get global accounted memory
TEST(ThreadMemoryManager, testGetGlobalAccountedMemory)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    int64_t size = 1024 * 1024;
    globalMemoryManager->SetMemoryAmount(size);

    int64_t globalAccountedMemory = mem::MemoryManager::GetGlobalAccountedMemory();
    EXPECT_EQ(globalAccountedMemory, size);
}

// test: set and get memory limit in thread_level memory manager
TEST(MemoryManager, testSetAndGetMemoryLimit)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto currentMemoryManager = std::make_unique<mem::MemoryManager>(globalMemoryManager);
    int64_t limit = 10 * 1024 * 1024;
    currentMemoryManager->SetMemoryLimit(limit);
    int64_t actualLimit = currentMemoryManager->GetMemoryLimit();
    EXPECT_EQ(actualLimit, limit);
}

// test: set and get parent
TEST(MemoryManager, testSetAndGetParent)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    mem::MemoryManager *parent = globalMemoryManager->GetParent();
    EXPECT_TRUE(parent == nullptr);
    auto currentMemoryManager = std::make_unique<mem::MemoryManager>();
    currentMemoryManager->SetParent(globalMemoryManager);
    EXPECT_EQ(currentMemoryManager->GetParent(), globalMemoryManager);
}

// test: set and get memoryAmount
TEST(MemoryManager, testSetAndGetMemoryAmount)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto currentMemoryManager = std::make_unique<mem::MemoryManager>(globalMemoryManager);
    EXPECT_TRUE(currentMemoryManager != nullptr);
    int64_t size = 10 * 1024 * 1024;
    currentMemoryManager->SetMemoryAmount(size);
    EXPECT_EQ(currentMemoryManager->GetMemoryAmount(), size);
}

// test: set and get memoryPeak
TEST(MemoryManager, testSetAndGetMemoryPeak)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto currentMemoryManager = std::make_unique<mem::MemoryManager>(globalMemoryManager);
    EXPECT_TRUE(currentMemoryManager != nullptr);
    int64_t size = 10 * 1024 * 1024;
    currentMemoryManager->SetMemoryPeak(size);
    EXPECT_EQ(currentMemoryManager->GetMemoryPeak(), size);
}

// test: update memoryPeak
TEST(MemoryManager, testUpdateMemoryPeak)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto currentMemoryManager = std::make_unique<mem::MemoryManager>(globalMemoryManager);
    EXPECT_TRUE(currentMemoryManager != nullptr);

    int64_t size = 10 * 1024 * 1024;
    currentMemoryManager->SetMemoryPeak(size);
    int64_t smallSize = 1 * 1024 * 1024;
    currentMemoryManager->UpdatePeak(smallSize);
    EXPECT_EQ(currentMemoryManager->GetMemoryPeak(), size);
    int64_t largeSize = 20 * 1024 * 1024;
    currentMemoryManager->UpdatePeak(largeSize);
    EXPECT_EQ(currentMemoryManager->GetMemoryPeak(), largeSize);
}

// test: unlimited account
TEST(MemoryManager, testUnlimitedAccount)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto currentMemoryManager = std::make_unique<mem::MemoryManager>(globalMemoryManager);
    EXPECT_TRUE(currentMemoryManager != nullptr);

    int count = 10;
    int64_t positiveSize = 1 * 1024 * 1024;
    int64_t negativeSize = -1 * 1024 * 1024;
    for (int i = 0; i < count; ++i) {
        currentMemoryManager->Account(positiveSize);
    }
    int64_t currentMemoryAmount = currentMemoryManager->GetMemoryAmount();
    EXPECT_EQ(currentMemoryAmount, count * positiveSize);

    auto *parentMemoryManager = currentMemoryManager->GetParent();
    EXPECT_TRUE(parentMemoryManager != nullptr);
    int64_t parentMemoryAmount = parentMemoryManager->GetMemoryAmount();
    EXPECT_EQ(parentMemoryAmount, count * positiveSize);

    for (int i = 0; i < count; ++i) {
        currentMemoryManager->Account(negativeSize);
    }
    currentMemoryAmount = currentMemoryManager->GetMemoryAmount();
    EXPECT_EQ(currentMemoryAmount, 0);

    parentMemoryAmount = parentMemoryManager->GetMemoryAmount();
    EXPECT_EQ(parentMemoryAmount, 0);
}

// test: limited account
TEST(MemoryManager, testLimitedAccount)
{
    auto threadMemoryManager = mem::ThreadMemoryManager::GetThreadMemoryManager();
    threadMemoryManager->Clear();

    int64_t globalThreshold = 10 * 1024 * 1024;
    mem::MemoryManager::SetGlobalMemoryLimit(globalThreshold);
    auto globalMemoryManager = mem::MemoryManager::GetGlobalMemoryManager();
    auto currentMemoryManager = std::make_unique<mem::MemoryManager>(globalMemoryManager);
    EXPECT_TRUE(currentMemoryManager != nullptr);

    int count = 20;
    int64_t positiveSize = 1 * 1024 * 1024;
    for (int i = 0; i < count; ++i) {
        try {
            currentMemoryManager->Account(positiveSize);
        } catch (exception::OmniException &e) {
            break;
        }
    }

    int64_t currentMemoryAmount = currentMemoryManager->GetMemoryAmount();
    EXPECT_EQ(currentMemoryAmount, globalThreshold);

    auto *parentMemoryManager = currentMemoryManager->GetParent();
    EXPECT_TRUE(globalMemoryManager != nullptr);
    int64_t parentMemoryAmount = parentMemoryManager->GetMemoryAmount();
    EXPECT_EQ(parentMemoryAmount, globalThreshold - positiveSize);
}
}