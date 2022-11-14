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
#include "memory/base_allocator.h"
#include "null_key_traits.h"
#include "type/string_ref.h"

/**
 * group_hashmap contains 8 requirements to notice:
 * 1 hashmap use slot as basic unit, every slot contains (key, value ,hash value of key)
 * 2 the key and value of hashmap must support movable ,
 *   the value must have default constructor and must support copy assign function or move assign function
 * 3 the key must overload operator== function or it is a POD structure
 * 4 the hash function will use std::hash by default if user didn't set hash function in group_hasher.h
 * 5 the user-defined NullValueTypeTraits function in null_key_traits.h is used to
 * determine whether the key of the hashmap is null. hashmap think all key is not null by default ,
 * but stringref is null when (stringref.size == 0) and pointer is null when (pointer == nullptr)
 * 6 the hashmap will resize when exceed thresh hold in GrowStrategy
 * 7 the Allocator is used to allocate memory for cells, user can define custom allocator as OmniHashmapAllocator did
 * 8 hashmap will call key and value 's deconstruct function in deconstruct function if necessary
 *
 * Basic Usage:
 * DefaultHashMap<uint32,uint32> hashmap;
 * auto ret = hash.emplace(key); //or auto ret = hash.emplace(std::move(key));  key is std::unique_ptr<int>
 * if(not ret.IsInserted()){
 * ret.SetValue(value);
 * };
 * hashmap.GetElementsSize();
 * hashmap.ForEachKv(CustomFunction);
 */

namespace omniruntime {
namespace op {
/**
 * Basic Unit of Hashmap , Hashmap contains a continuous memory of GroupByHashSlots
 * Slot will cache hash value by default to compare value easily
 * and isAssigned to
 * @tparam KeyType the key type of hashmap
 * @tparam ValueType the value type of hashmap
 */
template <typename KeyType, typename ValueType> class GroupByHashSlot {
public:
    GroupByHashSlot() = default;

    ~GroupByHashSlot() = default;

    GroupByHashSlot(const KeyType &key_) : isAssigned(true)
    {
        kv.first = key_;
    }

    GroupByHashSlot(KeyType &&key_) : isAssigned(true), kv(std::move(key_),ValueType{})
    {
    }

    GroupByHashSlot(GroupByHashSlot &&o) noexcept : kv(std::move(o.kv)),  hashVal(o.hashVal)
    {
        isAssigned = true;
        o.hashVal = 0;
        o.isAssigned = false;
    }

    GroupByHashSlot(const KeyType &key, const ValueType &value) : kv(key, value), isAssigned(true) {}

    GroupByHashSlot &operator = (GroupByHashSlot &&o) noexcept
    {
        kv = std::move(o.kv);
        SetHashVal(o.GetHashVal());
        isAssigned = true;
        return *this;
    }

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

    const KeyType &GetKey() const
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
        isAssigned = true;
        return *this;
    }

    bool IsAssigned() const noexcept
    {
        return isAssigned;
    }

private:
    std::pair<KeyType, ValueType> kv;
    size_t hashVal;
    /**
     * isAssigned indicates whether the class is constructed.
     * the usage of this class in Hashmap is divided into two phases:
     * memory initialization and object construction.
     * when the slot memory is initialized, memset sets all memory fields to zero
     * and the obtained value is false if caller attempts to get value of isAssigned.
     * when the constructor function is invoked, the value of isAssigned is set to true.
     */
    bool isAssigned = false;
};

/**
 *
 * a user-defined Allocator must implement 2 functions
 * 1 Allocate(uint64_t size, uint8_t **buffer)
 * allocate @size bytes , and set ret pointer to *buffer ,ret allocated size,
 * *buffer should be set zero
 * 2 Release(uint8_t *buffer)
 * release the memory which allocate by Allocator
 *
 */
class OmniHashmapAllocator {
public:
    OmniHashmapAllocator()
    {
        pool = mem::BaseAllocator::GetRootAllocator();
    }

    int Allocate(uint64_t size, uint8_t **buffer)
    {
        auto ret = pool->alloc(static_cast<int64_t>(size));
        *buffer = static_cast<uint8_t*>(ret);
        if(ret == nullptr){
            throw exception::OmniException("allocate in OmniHashmapAllocator","allocate memory fail");
        }
        auto err = memset_s(*buffer, size, 0, size);
        if (err != EOK) {
            throw exception::OmniException("allocate in OmniHashmapAllocator","memset_sp 0 fail");
        }
        return size;
    }

    void Release(uint8_t *buffer, uint64_t size)
    {
        pool->free((void*)buffer,size);
    }

    ~OmniHashmapAllocator() = default;

private:
    mem::BaseAllocator *pool = nullptr;
};

template <typename ValueType> class InsertResult {
public:
    InsertResult(ValueType &emplaceValue, bool ins = false) : emplaceValue(emplaceValue), inserted(ins) {}

    ~InsertResult() = default;

    bool IsInsert() const
    {
        return inserted;
    }

    // for copyable object use value it self, for only movable object use std::move(value)
    void SetValue(const ValueType &value)
    {
        if (not inserted) {
            throw std::runtime_error("can not Set a value to Cell which is not insert");
        }
        emplaceValue = value;
    }

    // this interface will be used for only movable object
    void SetValue(ValueType &&value)
    {
        if (not inserted) {
            throw std::runtime_error("can not Set a value to Cell which is not insert");
        }
        emplaceValue = std::move(value);
    }

    const ValueType &GetValue() const
    {
        return emplaceValue;
    }

    ValueType &GetValue()
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
    /* *
     * the way to grow the size of hashmap
     * @return
     */
    uint64_t GrowSize()
    {
        degree += (degree >= enlargeThreshold ? 1 : 2);
        return 1 << degree;
    }
    /* *
     * @return allocate byte of hashmap
     */
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
 * @tparam KeyType must have default constructor , must support move-assign function or copy-assign function
 * @tparam ValueType must support movable
 * @tparam HashType Hash Algorithm of KeyType
 * @tparam GrowStrategy Rehash when size exceed totalSize / 2
 * @tparam Allocator memory pool
 */

template <typename KeyType, typename ValueType, typename HashType, typename GrowStrategy, typename Allocator,
    std::enable_if_t<std::is_move_constructible_v<KeyType> && std::is_move_constructible_v<ValueType>
                    &&(std::is_move_assignable_v<ValueType> || std::is_copy_assignable_v<ValueType>)>* = nullptr>
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
        capacity = totalSize * sizeof(Slot);
        slots = reinterpret_cast<Slot *>(address);
        elementsSize = 0;
    }

    // Should hashmap be copyable?
    GroupByHashMap(const GroupByHashMap &) = delete;

    GroupByHashMap &operator = (const GroupByHashMap &) = delete;

    GroupByHashMap &operator = (GroupByHashMap &&) = delete;

    GroupByHashMap(GroupByHashMap &&o) noexcept
    {
        address = o.address;
        totalSize = o.totalSize;
        capacity = o.capacity;
        slots = o.slots;
        elementsSize = o.elementsSize;
        o.address = nullptr;
        o.slots = nullptr;
        o.totalSize = 0;
        o.capacity = 0;
        o.elementsSize = 0;
    }

    size_t GetCapacity() const
    {
        return capacity;
    }

    bool HasNullCell() const
    {
        return nullSlot != nullptr;
    }

    size_t GetElementsSize() const
    {
        return elementsSize;
    }

    /* *
     * only allowed insert key , if a new key inserted, the key will be copied or moved to create Slot,
     * at the same time, empty value will be created in Slot,
     * and caller can check InsertResult to determine whether need to set value
     * after caller call SetValue of InsertResult<ValueType> , both the key and value are set finish.
     *
     * caller can use this Emplace to Update data too. just call get value by call GetValue function of InsertResult
     * InsertResult's GetValue function will return value reference, and caller can update the value
     */
    template<typename T,std::enable_if_t<
            std::is_same_v<std::remove_reference_t<std::remove_cv_t<T>>,KeyType>>* = nullptr>
    InsertResult<ValueType> Emplace(T &&key)
    {
        if (NeedRehash()) {
            Rehash();
        }
        if (IsNullValue(key)) {
            return EmplaceNullValue(std::forward<T>(key));
        }
        auto hashValue = CalculateHash(key);
        auto pos = FindPosition(key, hashValue);
        bool inserted = false;

        if (not slots[pos].IsAssigned()) {
            inserted = true;
            new (&slots[pos]) Slot(std::forward<T>(key));
            ++elementsSize;
        }
        auto &curSlot = slots[pos];
        curSlot.SetHashVal(hashValue);
        return InsertResult<ValueType>(curSlot.GetValue(), inserted);
    }

    template <class Func> void ForEachKV(Func &&func)
    {
        int remainNum = elementsSize;
        if (HasNullCell()) {
            --remainNum;
            func(nullSlot->GetKey(), nullSlot->GetValue());
        }

        int index = 0;
        while (remainNum) {
            while (not slots[index].IsAssigned()) {
                ++index;
            }
            func(slots[index].GetKey(), slots[index].GetValue());
            ++index;
            --remainNum;
        }
        return;
    }

    template <class WriteStream> void Spill(WriteStream &writeStream) {}

    template <class ReadStream> void Unspill(ReadStream &readStream) {}

    ~GroupByHashMap()
    {
        //add constexpr
        if constexpr (not std::is_trivially_destructible<Slot>::value) {
            DeconstructAllSlot();
        }
        allocator.Release(address, GetCapacity());
        if(nullSlot != nullptr){
            allocator.Release(reinterpret_cast<uint8_t*>(nullSlot), sizeof(Slot));
        }
    }

private:
    void Reinsert(Slot &cell)
    {
        auto pos = FindPosition(cell.GetKey(), cell.GetHashVal());
        new (&slots[pos]) Slot(cell);
    }

    void Reinsert(Slot &&cell)
    {
        auto pos = FindPosition(cell.GetKey(), cell.GetHashVal());
        new (&slots[pos]) Slot(std::move(cell));
    }

    void Rehash()
    {
        auto oldSize = GetCapacity();
        Slot *oldCells = slots;
        uint64_t reHashSize = grower.GrowSize();
        allocator.Allocate(reHashSize * sizeof(Slot), &address);
        slots = reinterpret_cast<Slot *>(address);
        EnlargeSizeRecord(reHashSize);
        int remainNum = elementsSize - (HasNullCell() ? 1 : 0);
        int index = 0;
        while (remainNum != 0) {
            while (not oldCells[index].IsAssigned()) {
                ++index;
            }
            Reinsert(std::move(oldCells[index]));
            --remainNum;
            ++index;
        }
        allocator.Release((uint8_t *)oldCells, oldSize);
    }

    size_t CalculateHash(const KeyType &key)
    {
        return hasher(key);
    }

    bool NeedRehash()
    {
        return elementsSize > grower.GetThreshHold();
    };

    size_t FindPosition(const KeyType &key, size_t hashValue)
    {
        auto nextPos = (hashValue & (totalSize - 1));
        while (slots[nextPos].IsAssigned() && not slots[nextPos].IsSameKey(hashValue, key)) {
            ++nextPos;
            nextPos &= (totalSize - 1);
        }
        return nextPos;
    }

    bool IsNullValue(const KeyType &key)
    {
        return NullValueTypeTraits<KeyType>::IsNullValue(key);
    }

    //no need to check T
    template<typename T>
    InsertResult<ValueType> EmplaceNullValue(T&& key)
    {
        if (nullSlot == nullptr) {
            ++elementsSize;
            allocator.Allocate(sizeof(Slot), reinterpret_cast<uint8_t **>(&nullSlot));
            new(nullSlot) Slot(std::forward<T>(key));
            return InsertResult<ValueType> {nullSlot->GetValue(), true };
        } else {
            return InsertResult<ValueType> {nullSlot->GetValue(), false };
        }
    }

    // call deconstruct function of every cell
    void DeconstructAllSlot()
    {
        int remainNum = elementsSize;
        if (HasNullCell()) {
            --remainNum;
            nullSlot->~Slot();
        }
        int index = 0;
        while (remainNum) {
            while (not slots[index].IsAssigned()) {
                ++index;
            }
            slots[index].~Slot();
            ++index;
            --remainNum;
        }
        return;
    }

    void EnlargeSizeRecord(uint64_t newElementsSize)
    {
        totalSize = newElementsSize;
        capacity = newElementsSize * sizeof(Slot);
    }

    uint8_t *address;
    Allocator allocator;
    Slot *slots;
    Slot *nullSlot = nullptr;
    size_t elementsSize;
    HashType hasher;
    uint64_t totalSize;
    uint64_t capacity;
    static constexpr uint8_t defaultDegreeSize = 8;
    GrowStrategy grower;
};


template <typename KeyType, typename ValueType>
using DefaultHashMap = GroupByHashMap<KeyType, ValueType, GroupbyHashCalculator<KeyType>, Grower, OmniHashmapAllocator>;

template <typename ValueType>
using DefaultInt64HashMap =
    GroupByHashMap<uint64_t, ValueType, GroupbyHashCalculator<uint64_t>, Grower, OmniHashmapAllocator>;
}
}
#endif // OMNI_RUNTIME_GROUP_HASH_MAP_H
