/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
#ifndef OMNI_RUNTIME_GROUP_HASH_MAP_H
#define OMNI_RUNTIME_GROUP_HASH_MAP_H

#include <iostream>
#include <vector>
#include <cmath>
#include <cstring>
#include <sstream>
#include <unordered_map>
#include <functional>
#include <jemalloc/jemalloc.h>
#include "group_hasher.h"
#include "memory/memory_pool.h"
#include "null_key_traits.h"
#include "type/string_ref.h"

namespace omniruntime {
namespace op {
template <typename KeyType, typename ValueType> class GroupByHashSlot {
public:
    GroupByHashSlot() = default;

    ~GroupByHashSlot() = default;

    GroupByHashSlot(const KeyType &key_) : isAssign(true)
    {
        kv.first = key_;
    }

    GroupByHashSlot(const KeyType &key, const ValueType &value) : kv(key, value), isAssign(true) {}

    bool IsSameKey(const size_t &otherHashVal, const KeyType &key1)
    {
        if (hashVal != otherHashVal) {
            return false;
        }
        return kv.first == key1;
    }

    size_t GetHashVal() const
    {
        return hashVal;
    }

    void SetValue(const ValueType &value)
    {
        kv.second = value;
    }

    KeyType GetKey() const
    {
        return kv.first;
    }

    ValueType &GetValue()
    {
        return kv.second;
    }
    const ValueType &GetValue() const
    {
        return kv.second;
    }
    void SetHashVal(const size_t hashValueIn)
    {
        hashVal = hashValueIn;
    }
    GroupByHashSlot &operator = (const GroupByHashSlot &o)
    {
        kv = o.kv;
        SetHashVal(o.GetHashVal());
        isAssign = true;
        return *this;
    }
    GroupByHashSlot(GroupByHashSlot &&o) noexcept
    {
        isAssign = true;
        kv = o.kv;
        hashVal = o.hashVal;
        o.kv = {};
        o.hashVal = 0;
    }

    bool IsAssign() const noexcept
    {
        return isAssign;
    }

private:
    std::pair<KeyType, ValueType> kv;
    size_t hashVal;
    bool isAssign = false;
};

class OmniHashmapAllocator {
public:
    OmniHashmapAllocator()
    {
        pool = mem::GetMemoryPool();
    }

    int Allocate(uint64_t size, uint8_t **buffer)
    {
        auto ret = pool->Allocate(static_cast<int64_t>(size), buffer);
        auto err = memset_s(*buffer, size, 0, size);
        if (err != EOK) {
            return 0;
        }
        return ret;
    }

    int Release(uint8_t *buffer)
    {
        return pool->Release(buffer);
    }

    ~OmniHashmapAllocator() = default;

private:
    mem::MemoryPool *pool = nullptr;
};

template <typename ValueType> class InsertResult {
public:
    InsertResult(ValueType &emplaceValue, bool ins = false) : emplaceValue(emplaceValue), inserted(ins) {}

    ~InsertResult() = default;

    bool IsInsert() const
    {
        return inserted;
    }
    void SetValue(const ValueType &value)
    {
        if (not inserted) {
            throw std::runtime_error("can not Set a value to Cell which is not insert");
        }
        emplaceValue = value;
    }

    const ValueType &GetValue() const
    {
        return emplaceValue;
    }

private:
    ValueType &emplaceValue;
    bool inserted = false;
};

class Grower {
public:
    static constexpr short enlargeThreshold = 23;
    Grower(uint8_t degree) : degree(degree) {}

    uint64_t GrowSize()
    {
        degree += (degree >= enlargeThreshold ? 1 : 2);
        return 1 << degree;
    }
    uint64_t GetCurrentSize()
    {
        return 1 << degree;
    }
    uint64_t GetThreshHold()
    {
        return 1 << (degree - 1);
    }

    ~Grower() = default;

private:
    uint8_t degree;
};


/**
 * design for group by
 * @tparam KeyType
 * @tparam ValueType
 * @tparam HashType Hash Algorithm
 * @tparam GrowStrategy Rehash when size exceed totalSize / 2
 * @tparam Allocator memory pool
 */

template <typename KeyType, typename ValueType, typename HashType, typename GrowStrategy, typename Allocator,
    std::enable_if_t<std::is_copy_assignable<ValueType>::value> * = nullptr>
class GroupByHashMap {
public:
    using Slot = GroupByHashSlot<KeyType, ValueType>;
    using HashKey = KeyType;
    using ResultType = InsertResult<ValueType>;

public:
    GroupByHashMap(uint8_t initDegree = defaultDegreeSize) : grower(initDegree)
    {
        auto defaultSize = grower.GetCurrentSize();
        allocator.Allocate(defaultSize * sizeof(Slot), &address);
        totalSize = defaultSize;
        capacity = totalSize * sizeof(ValueType);
        cells = reinterpret_cast<Slot *>(address);
        elementsSize = 0;
    }

    GroupByHashMap(const GroupByHashMap &) = delete;
    GroupByHashMap &operator = (const GroupByHashMap &) = delete;
    GroupByHashMap(GroupByHashMap &&o) noexcept
    {
        address = o.address;
        totalSize = o.totalSize;
        capacity = o.capacity;
        cells = o.cells;
        elementsSize = o.elementsSize;
        o.address = nullptr;
        o.cells = nullptr;
        o.totalSize = 0;
        o.capacity = 0;
        o.elementsSize = 0;
    }

    size_t GetCapacity() const
    {
        return capacity;
    }

    void Reinsert(Slot &cell)
    {
        auto pos = FindPosition(cell.GetKey(), cell.GetHashVal());
        cells[pos] = cell;
    }

    void Rehash()
    {
        Slot *oldCells = cells;
        uint64_t reHashSize = grower.GrowSize();
        allocator.Allocate(reHashSize * sizeof(Slot), &address);
        cells = reinterpret_cast<Slot *>(address);
        EnlargeSizeRecord(reHashSize);
        int remainNum = elementsSize;
        int index = 0;
        while (remainNum != 0) {
            while (not oldCells[index].IsAssign()) {
                ++index;
            }
            Reinsert(oldCells[index]);
            --remainNum;
            ++index;
        }
        allocator.Release((uint8_t *)oldCells);
    }

    size_t CalculateHash(const KeyType &key)
    {
        return hasher(key);
    }

    bool NeedRehash()
    {
        return elementsSize > grower.GetThreshHold();
    };

    size_t GetElementsSize() const
    {
        return elementsSize;
    }

    size_t FindPosition(const KeyType &key, size_t hashValue)
    {
        auto nextPos = (hashValue & (totalSize - 1));
        while (cells[nextPos].IsAssign() && not cells[nextPos].IsSameKey(hashValue, key)) {
            ++nextPos;
            nextPos &= (totalSize - 1);
        }
        return nextPos;
    }
    bool IsNullValue(const KeyType &key)
    {
        return NullValueTypeTraits<KeyType>::IsNullValue(key);
    }
    InsertResult<ValueType> EmplaceNullValue(const KeyType &key)
    {
        if (nullCell == nullptr) {
            allocator.Allocate(sizeof(Slot), reinterpret_cast<uint8_t **>(&nullCell));
            return InsertResult<ValueType> { nullCell->GetValue(), true };
        } else {
            return InsertResult<ValueType> { nullCell->GetValue(), false };
        }
    }

    InsertResult<ValueType> Emplace(const KeyType &key)
    {
        if (NeedRehash()) {
            Rehash();
        }
        if (IsNullValue(key)) {
            return EmplaceNullValue(key);
        }
        auto hashValue = CalculateHash(key);
        auto pos = FindPosition(key, hashValue);
        bool inserted = false;

        if (not cells[pos].IsAssign()) {
            inserted = true;
            new (&cells[pos]) Slot(key);
            ++elementsSize;
        }
        auto &curCell = cells[pos];
        curCell.SetHashVal(hashValue);
        return InsertResult<ValueType>(curCell.GetValue(), inserted);
    }

    template <class Func> void ForEachKV(Func &&func)
    {
        int remainNum = elementsSize;
        int index = 0;
        while (remainNum) {
            while (not cells[index].IsAssign()) {
                ++index;
            }
            func(cells[index].GetKey(), cells[index].GetValue());
            ++index;
            --remainNum;
        }
        return;
    }

    template <class WriteStream> void Spill(WriteStream &writeStream) {}

    template <class ReadStream> void Unspill(ReadStream &readStream) {}

    ~GroupByHashMap()
    {
        if ((not std::is_pointer<ValueType>::value) && (not std::is_trivially_destructible<ValueType>::value)) {
            ForEachKV([](const auto &key, auto &mapped) { mapped.~ValueType(); });
        }
        allocator.Release(address);
    }

private:
    uint8_t *address;
    Allocator allocator;
    Slot *cells;
    Slot *nullCell = nullptr;
    size_t elementsSize;
    HashType hasher;
    uint64_t totalSize;
    uint64_t capacity;
    static constexpr uint8_t defaultDegreeSize = 8;
    GrowStrategy grower;

private:
    void EnlargeSizeRecord(uint64_t newElementsSize)
    {
        totalSize = newElementsSize;
        capacity = newElementsSize * sizeof(Slot);
    }
};


template <typename KeyType, typename ValueType>
using DefaultHashMap = GroupByHashMap<KeyType, ValueType, GroupbyHashCalculator<KeyType>, Grower, OmniHashmapAllocator>;

template <typename ValueType>
using DefaultInt64HashMap =
    GroupByHashMap<uint64_t, ValueType, GroupbyHashCalculator<uint64_t>, Grower, OmniHashmapAllocator>;
}
}
#endif // OMNI_RUNTIME_GROUP_HASH_MAP_H
