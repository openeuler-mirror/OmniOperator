/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#include <vector>
#include "gtest/gtest.h"
#include "operator/union/union.h"
#include "operator/aggregation/group_hash_map/group_hash_map.h"

namespace op {
namespace vec {
namespace HashMapTest {
    class InfoCollection {
    public:
        static InfoCollection &GetInstance()
        {
            static InfoCollection infoCollection;
            return infoCollection;
        }
        int AddState(int s)
        {
            size += s;
            return size;
        }
        int MinusState(int s)
        {
            size -= s;
            return size;
        }
        int GetState()
        {
            return size;
        }
    private:
        InfoCollection() = default;
        uint32_t size = 0;
    };
    class ValueWithResource {
    public:
        ValueWithResource() : curSize(0)
        {
            resPtr = nullptr;
        }
        ValueWithResource(const ValueWithResource &o) = delete;
        ValueWithResource& operator=(const ValueWithResource &o) = delete;
        ValueWithResource(ValueWithResource && o) noexcept
        {
            this->curSize = o.curSize;
            o.curSize = 0;
            this->resPtr = o.resPtr;
            o.resPtr = nullptr;
        }

        explicit ValueWithResource(uint32_t size) : curSize(size)
        {
            resPtr = static_cast<char *>(malloc(size));
            InfoCollection::GetInstance().AddState(size);
            ASSERT(resPtr != nullptr);
        }
        ~ValueWithResource()
        {
            if (resPtr != nullptr) {
                free(resPtr);
                InfoCollection::GetInstance().MinusState(curSize);
            }
        }
    private:
        using ResourcePtr = char*;
        ResourcePtr resPtr = nullptr;
        int curSize = 0;
    };
    TEST(GroupByHashMapTest, TestBasicEmplace) {
        omniruntime::op::DefaultHashMap<int, int> hashMap;

        constexpr uint32_t total = 1000;

        for(uint32_t i = 0; i < total ; ++i) {
            auto ret = hashMap.Emplace(i);
            if (ret.IsInsert()) {
                ret.SetValue(i * i);
            }
        }
        EXPECT_EQ(hashMap.GetElementsSize(), total);
    }
    TEST(GroupByHashMapTest, TestRelease) {
        omniruntime::op::DefaultHashMap<int, ValueWithResource*> hashMap;
        constexpr uint32_t total = 1000;
        constexpr uint32_t totalBytes = 1000 * 100;
        std::vector<ValueWithResource *> p(total, nullptr);
        for (uint32_t i = 0; i < total ; ++i) {
            auto ret = hashMap.Emplace(i);
            EXPECT_TRUE(ret.IsInsert());
            auto *ptr = new ValueWithResource(100);
            ret.SetValue(ptr);
            p[i] = ptr;
        }
        EXPECT_EQ(InfoCollection::GetInstance().GetState(), totalBytes);
        //resource must release by caller
        for (auto iter:p) {
            delete iter;
        }

        EXPECT_EQ(InfoCollection::GetInstance().GetState(), 0);
    }
}
} // end vec
} // end op
