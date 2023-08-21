/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */
#ifndef OMNI_RUNTIME_BASE_HASH_MAP_H
#define OMNI_RUNTIME_BASE_HASH_MAP_H

#include <iostream>
#include <vector>
#include <cmath>
#include <cstring>
#include <sstream>
#include <unordered_map>
#include <functional>
#include <jemalloc/jemalloc.h>
#include <arm_neon.h>

#include "operator/hash_util.h"
#include "group_hasher.h"
#include "memory/memory_pool.h"
#include "memory/allocator.h"
#include "null_key_traits.h"
#include "type/string_ref.h"

/**
 * base_hashmap contains 8 requirements to notice:
 * 1 hashmap use slot as basic unit, every slot contains (key, value ,hash value of key)
 * 2 the key and value of hashmap must support movable ,
 * the value must have default constructor and must support copy assign function or move assign function
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
static const int32_t EIGHT = 8;
static const double PROPORTION = 0.9;

/**
 * Basic Unit of Hashmap , Hashmap contains a continuous memory of HashSlots
 * Slot will cache hash value by default to compare value easily
 * and isAssigned to
 * @tparam KeyType the key type of hashmap
 * @tparam ValueType the value type of hashmap
 */
template <typename KeyType, typename ValueType> class HashSlot {
public:
    HashSlot() = default;

    ~HashSlot() = default;

    explicit HashSlot(const KeyType &keyParam)
    {
        kv.first = keyParam;
    }

    explicit HashSlot(KeyType &&keyParam) : kv(std::move(keyParam), ValueType {}) {}

    HashSlot(HashSlot &&o) noexcept : kv(std::move(o.kv)), hashVal(o.hashVal)
    {
        o.hashVal = 0;
    }

    HashSlot(const KeyType &key, const ValueType &value) : kv(key, value) {}

    HashSlot &operator = (HashSlot &&o) noexcept
    {
        kv = std::move(o.kv);
        SetHashVal(o.GetHashVal());
        return *this;
    }

    bool IsSameKey(const size_t &otherHashVal, const KeyType &key1)
    {
        if (otherHashVal != hashVal) {
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

    HashSlot &operator = (const HashSlot &o)
    {
        kv = o.kv;
        SetHashVal(o.GetHashVal());
        return *this;
    }

    HashSlot(const HashSlot &o)
    {
        kv = o.kv;
        SetHashVal(o.GetHashVal());
    }

private:
    std::pair<KeyType, ValueType> kv;
    size_t hashVal;
};

/**
 * A user-defined Allocator must implement 2 functions
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
        pool = mem::Allocator::GetAllocator();
    }

    int Allocate(uint64_t size, uint8_t **buffer)
    {
        auto ret = pool->Alloc(static_cast<int64_t>(size), false);
        if (ret == nullptr) {
            throw exception::OmniException("allocate in OmniHashmapAllocator", "allocate memory fail");
        }
        *buffer = static_cast<uint8_t *>(ret);
        return size;
    }

    void Release(uint8_t *buffer, uint64_t size)
    {
        pool->Free((void *)buffer, size);
    }

    ~OmniHashmapAllocator() = default;

private:
    mem::Allocator *pool = nullptr;
};

template <typename ValueType> class InsertResult {
public:
    explicit InsertResult(ValueType &emplaceValue, bool ins = false) : emplaceValue(emplaceValue), inserted(ins) {}

    ~InsertResult() = default;

    bool IsInsert() const
    {
        return inserted;
    }

    // for copyable object use value itself, for only movable object use std::move(value)
    void SetValue(const ValueType &value)
    {
        emplaceValue = value;
    }

    // this interface will be used for only movable object
    void SetValue(ValueType &&value)
    {
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
    static constexpr short expandFactoryOne = 1;
    static constexpr short expandFactoryTwo = 2;
    explicit Grower(uint8_t degree) : degree(degree)
    {
        CalculateThreshold();
    }

    /**
     * the way to grow the size of hashmap
     * @return
     */
    uint64_t GrowSize()
    {
        degree += (degree >= enlargeThreshold ? expandFactoryOne : expandFactoryTwo);
        CalculateThreshold();
        return 1ULL << degree;
    }

    /**
     * @return allocate byte of hashmap
     */
    uint64_t GetCurrentSize()
    {
        return 1ULL << degree;
    }

    uint64_t GetThreshHold()
    {
        return threshold;
    }

    ~Grower() = default;

private:
    void CalculateThreshold()
    {
        threshold = static_cast<uint64_t>(std::ceil(static_cast<double>(1ULL << degree) * PROPORTION));
    }

    uint64_t threshold = 0;
    uint8_t degree;
};

struct OutputState {
    uint32_t outputHashmapPos = 0;
    uint32_t hasBeenOutputNum = 0;

    void UpdateState(OutputState &o)
    {
        outputHashmapPos = o.outputHashmapPos;
        hasBeenOutputNum += o.hasBeenOutputNum;
    }

    explicit OutputState(uint32_t outputHashmapPos = 0, uint32_t hasBeenOutputNum = 0)
        : outputHashmapPos(outputHashmapPos), hasBeenOutputNum(hasBeenOutputNum)
    {}
};

ALWAYS_INLINE static uint32_t CountTrailingZerosNonZero64(uint64_t n)
{
    return __builtin_ctzll(n);
}

ALWAYS_INLINE static uint32_t CountTrailingZerosNonZero32(uint32_t n)
{
    return __builtin_ctz(n);
}

template <typename T> ALWAYS_INLINE uint32_t TrailingZeros(T x)
{
    if constexpr (sizeof(T) == 8) {
        return CountTrailingZerosNonZero64(static_cast<uint64_t>(x));
    } else {
        return CountTrailingZerosNonZero32(static_cast<uint32_t>(x));
    }
}

/**
 * An abstraction over a bitmask. It provides an easy way to iterate through the
 * indexes of the set bits of a bitmask.  When Shift=0 (platforms with SSE),
 * this is a true bitmask.  On non-SSE, platforms the arithematic used to
 * emulate the SSE behavior works in bytes (Shift=3) and leaves each bytes as
 * either 0x00 or 0x80.
 * For example:
 * for (i : BitMask<uint32_t, 16>(0x5)) -> yields 0, 2
 * for (i : BitMask<uint64_t, 8, 3>(0x0000000080800000)) -> yields 2, 3
 */
template <class T, int SignificantBits, int Shift = 0> class BitMask {
public:
    explicit BitMask(T mask) : mask_(mask) {}

    BitMask &operator ++ ()
    {
        mask_ &= (mask_ - 1);
        return *this;
    }

    explicit operator bool() const
    {
        return mask_ != 0;
    }

    uint32_t operator*() const
    {
        return LowestBitSet();
    }

    uint32_t LowestBitSet() const
    {
        return TrailingZeros(mask_) >> Shift;
    }

    BitMask begin() const
    {
        return *this;
    }

    BitMask end() const
    {
        return BitMask(0);
    }

private:
    friend bool operator == (const BitMask &left, const BitMask &right)
    {
        return left.mask_ == right.mask_;
    }
    friend bool operator != (const BitMask &left, const BitMask &right)
    {
        return left.mask_ != right.mask_;
    }

    T mask_;
};

static const uint32_t SHIFT_DISTANCE_7 = 7;
using ctrl_t = signed char;
using h2_t = uint8_t;

ALWAYS_INLINE static size_t H1(size_t hashval)
{
    return (hashval >> SHIFT_DISTANCE_7);
}

ALWAYS_INLINE static h2_t H2(size_t hashval)
{
    return (h2_t)(ctrl_t)(hashval & 0x7F);
}

enum Ctrl : ctrl_t {
    kEmpty = -128, // 0b10000000 or 0x80
    kSentinel = -1
};

static uint32_t mm_movemask_epi8(uint8x16_t input)
{
    // Shift out everything but the sign bits with an unsigned shift right.
    //
    // Bytes of the vector::
    // 89 ff 1d c0 00 10 99 33
    // \  \  \  \  \  \  \  \    high_bits = (uint16x4_t)(input >> 7)
    //  |  |  |  |  |  |  |  |
    // 01 01 00 01 00 00 01 00
    //
    // Bits of first important lane(s):
    // 10001001 (89)
    // \______
    //        |
    // 00000001 (01)
    uint16x8_t high_bits = vreinterpretq_u16_u8(vshrq_n_u8(input, 7));

    // Merge the even lanes together with a 16-bit unsigned shift right + add.
    // 'xx' represents garbage data which will be ignored in the final result.
    // In the important bytes, the add functions like a binary OR.
    //
    // 01 01 00 01 00 00 01 00
    //  \_ |  \_ |  \_ |  \_ |   paired16 = (uint32x4_t)(input + (input >> 7))
    //    \|    \|    \|    \|
    // xx 03 xx 01 xx 00 xx 02
    //
    // 00000001 00000001 (01 01)
    //        \_______ |
    //                \|
    // xxxxxxxx xxxxxx11 (xx 03)
    uint32x4_t paired16 = vreinterpretq_u32_u16(vsraq_n_u16(high_bits, high_bits, 7));

    // Repeat with a wider 32-bit shift + add.
    // xx 03 xx 01 xx 00 xx 02
    //     \____ |     \____ |  paired32 = (uint64x1_t)(paired16 + (paired16 >>
    //     14))
    //          \|          \|
    // xx xx xx 0d xx xx xx 02
    //
    // 00000011 00000001 (03 01)
    //        \\_____ ||
    //         '----.\||
    // xxxxxxxx xxxx1101 (xx 0d)
    uint64x2_t paired32 = vreinterpretq_u64_u32(vsraq_n_u32(paired16, paired16, 14));

    // Last, an even wider 64-bit shift + add to get our result in the low 8 bit
    // lanes. xx xx xx 0d xx xx xx 02
    //            \_________ |   paired64 = (uint8x8_t)(paired32 + (paired32 >>
    //            28))
    //                      \|
    // xx xx xx xx xx xx xx d2
    //
    // 00001101 00000010 (0d 02)
    //     \   \___ |  |
    //      '---.  \|  |
    // xxxxxxxx 11010010 (xx d2)
    uint8x16_t paired64 = vreinterpretq_u8_u64(vsraq_n_u64(paired32, paired32, 28));

    // Extract the low 8 bits from each 64-bit lane with 2 8-bit extracts.
    // xx xx xx xx xx xx xx d2
    //                      ||  return paired64[0]
    //                      d2
    // Note: Little endian would return the correct value 4b (01001011) instead.
    return vgetq_lane_u8(paired64, 0) | ((int)vgetq_lane_u8(paired64, EIGHT) << EIGHT);
}

struct GroupNeonImpl {
    enum {
        kWidth = 16
    }; // the number of slots per group

    explicit GroupNeonImpl(const ctrl_t *pos)
    {
        ctrl = vld1q_s8(reinterpret_cast<const int8_t *>(pos));
    }

    // Returns a bitmask representing the positions of slots that match hash.
    BitMask<uint32_t, kWidth> Match(h2_t hash) const
    {
        int8x16_t match = vdupq_n_s8((uint8_t)hash);
        uint8x16_t cmp_result = vceqq_s8(match, ctrl);

        // Convert NEON register to bitmask
        auto bitmask = mm_movemask_epi8(cmp_result);
        return BitMask<uint32_t, kWidth>(bitmask);
    }

    // Returns a bitmask representing the positions of empty slots.
    BitMask<uint32_t, kWidth> MatchEmpty()
    {
        return Match(static_cast<h2_t>(kEmpty));
    }

    // Returns the number of trailing empty elements in the group.
    uint32_t CountLeadingEmpty() const
    {
        auto special = vdupq_n_s8(static_cast<uint8_t>(kSentinel));
        return TrailingZeros(static_cast<uint32_t>(mm_movemask_epi8(vcgtq_s8(special, ctrl)) + 1));
    }

    int8x16_t ctrl;
};
using Group = GroupNeonImpl;

template <size_t Width> class ProbeSeq {
public:
    ProbeSeq(size_t hashval, size_t mask)
    {
        assert(((mask + 1) & mask) == 0 && "not a mask");
        mask_ = mask;
        offset_ = hashval & mask_;
    }

    ALWAYS_INLINE size_t GetOffset() const
    {
        return offset_;
    }

    ALWAYS_INLINE size_t GetOffset(size_t i) const
    {
        return (offset_ + i) & mask_;
    }

    ALWAYS_INLINE void GetNext()
    {
        index_ += Width;
        offset_ += index_;
        offset_ &= mask_;
    }

private:
    size_t mask_;
    size_t offset_;
    size_t index_ = 0;
};

/**
 * design for group by
 * @tparam KeyType must have default constructor , must support move-assign function or copy-assign function
 * @tparam ValueType must support movable
 * @tparam HashType Hash Algorithm of KeyType
 * @tparam GrowStrategy Rehash when size exceed (1ULL << degree) * 0.75
 * @tparam Allocator memory pool
 */
template <typename KeyType, typename ValueType, typename HashType, typename GrowStrategy, typename Allocator,
    std::enable_if_t<std::is_move_constructible_v<KeyType> && std::is_move_constructible_v<ValueType> &&
    (std::is_move_assignable_v<ValueType> || std::is_copy_assignable_v<ValueType>)>* = nullptr>
class BaseHashMap {
public:
    using Slot = HashSlot<KeyType, ValueType>;
    using HashKey = KeyType;
    using ResultType = InsertResult<ValueType>;
    using Keys = KeyType;

public:
    BaseHashMap(uint8_t initDegree = defaultDegreeSize) : grower(initDegree)
    {
        InitSlots(grower.GetCurrentSize());
    }

    ALWAYS_INLINE void InitSlots(uint64_t newCapacity)
    {
        capacity = newCapacity;

        allocator.Allocate((newCapacity + Group::kWidth) * sizeof(ctrl_t), &ctrlAddress);
        identifiers = reinterpret_cast<ctrl_t *>(ctrlAddress);
        memset_sp(identifiers, sizeof(ctrl_t) * newCapacity, kEmpty, sizeof(ctrl_t) * newCapacity);
        memset_sp(identifiers + newCapacity, sizeof(ctrl_t) * Group::kWidth, kSentinel, sizeof(ctrl_t) * Group::kWidth);

        allocator.Allocate(newCapacity * sizeof(Slot), &slotsAddress);
        slots = reinterpret_cast<Slot *>(slotsAddress);
    }

    BaseHashMap(const BaseHashMap &) = delete;

    BaseHashMap &operator = (const BaseHashMap &) = delete;

    BaseHashMap &operator = (BaseHashMap &&) = delete;

    BaseHashMap(BaseHashMap &&o) = delete;

    bool HasNullCell() const
    {
        return nullSlot != nullptr;
    }

    size_t GetElementsSize() const
    {
        return elementsSize;
    }

    /**
     * Find Matched Join Position
     * Note: We use the second element of InsertResult to indicate whether to find the matched position or not.
     */
    template <typename T,
        std::enable_if_t<std::is_same_v<std::remove_reference_t<std::remove_cv_t<T>>, KeyType>>* = nullptr>
    ALWAYS_INLINE InsertResult<ValueType> FindMatchPosition(T &&key)
    {
        auto hashValue = CalculateHash(key);
        bool notHasKey = false;
        auto pos = FindPosition(key, hashValue, notHasKey);
        if (LIKELY(notHasKey)) {
            return InsertResult<ValueType>(nullSlot->GetValue(), false);
        }
        auto &curSlot = slots[pos];
        return InsertResult<ValueType>(curSlot.GetValue(), true);
    }

    /**
     * only allowed insert key , if a new key inserted, the key will be copied or moved to create Slot,
     * at the same time, empty value will be created in Slot,
     * and caller can check InsertResult to determine whether need to set value
     * after caller call SetValue of InsertResult<ValueType> , both the key and value are set finish.
     *
     * caller can use this Emplace to Update data too. just call get value by call GetValue function of InsertResult
     * InsertResult's GetValue function will return value reference, and caller can update the value
     */
    template <typename T,
        std::enable_if_t<std::is_same_v<std::remove_reference_t<std::remove_cv_t<T>>, KeyType>>* = nullptr>
    InsertResult<ValueType> Emplace(T &&key)
    {
        if (NeedRehash()) {
            Rehash();
        }
        if (IsNullValue(key)) {
            return EmplaceNullValue(std::forward<T>(key));
        }
        auto hashValue = CalculateHash(key);
        bool inserted = false;
        auto pos = FindPosition(key, hashValue, inserted);
        if (LIKELY(inserted)) {
            new (&slots[pos]) Slot(std::forward<T>(key));
            identifiers[pos] = (h2_t)H2(hashValue);
            ++elementsSize;
        }
        auto &slot = slots[pos];
        slot.SetHashVal(hashValue);
        return InsertResult<ValueType>(slot.GetValue(), inserted);
    }

    template <typename T> ALWAYS_INLINE InsertResult<ValueType> EmplaceNotNullKey(T &&key)
    {
        bool inserted = false;
        auto hashValue = hasher(key);
        auto pos = FindPosition(key, hashValue, inserted);

        if (LIKELY(inserted)) {
            new (&slots[pos]) Slot(std::forward<T>(key));
            identifiers[pos] = (h2_t)H2(hashValue);
            ++elementsSize;
        }
        slots[pos].SetHashVal(hashValue);
        return InsertResult<ValueType>(slots[pos].GetValue(), inserted);
    }

    template <typename T> ALWAYS_INLINE InsertResult<ValueType> EmplaceNotNullKey(T &&key, size_t &hashValue)
    {
        bool inserted = false;
        auto pos = FindPosition(key, hashValue, inserted);

        if (LIKELY(inserted)) {
            new (&slots[pos]) Slot(std::forward<T>(key));
            identifiers[pos] = (h2_t)H2(hashValue);
            ++elementsSize;
        }
        slots[pos].SetHashVal(hashValue);
        return InsertResult<ValueType>(slots[pos].GetValue(), inserted);
    }

    // no need to check T
    template <typename T> InsertResult<ValueType> EmplaceNullValue(T &&key)
    {
        if (nullSlot == nullptr) {
            ++elementsSize;
            allocator.Allocate(sizeof(Slot), reinterpret_cast<uint8_t **>(&nullSlot));
            new (nullSlot)Slot(std::forward<T>(key));
            return InsertResult<ValueType> { nullSlot->GetValue(), true };
        } else {
            return InsertResult<ValueType> { nullSlot->GetValue(), false };
        }
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
            while (IsEmptyOrDeleted(*(identifiers + index))) {
                // ctrl is not necessarily aligned to Group::kWidth. It is also likely
                // to read past the space for ctrl bytes and into slots. This is ok
                // because ctrl has sizeof() == 1 and slot has sizeof() >= 1 so there
                // is no way to read outside the combined slot array.
                uint32_t shift = Group { identifiers + index }.CountLeadingEmpty();
                index += shift;
            }
            auto &slot = slots[index];
            func(slot.GetKey(), slot.GetValue());
            ++index;
            --remainNum;
        }
        return;
    }

    template <class Func> void ForEachValue(Func &&func)
    {
        int remainNum = elementsSize;
        if (HasNullCell()) {
            --remainNum;
            func(nullSlot->GetValue());
        }
        int index = 0;
        while (remainNum && index < capacity) {
            while (IsEmptyOrDeleted(*(identifiers + index))) {
                // ctrl is not necessarily aligned to Group::kWidth. It is also likely
                // to read past the space for ctrl bytes and into slots. This is ok
                // because ctrl has sizeof() == 1 and slot has sizeof() >= 1 so there
                // is no way to read outside the combined slot array.
                uint32_t shift = Group { identifiers + index }.CountLeadingEmpty();
                index += shift;
            }
            auto &slot = slots[index];
            func(slot.GetValue());
            ++index;
            --remainNum;
        }
    }

    size_t CalculateHash(const KeyType &key)
    {
        return hasher(key);
    }

    ~BaseHashMap()
    {
        if constexpr (not std::is_trivially_destructible<Slot>::value) {
            DeconstructAllSlot();
        }
        ReleaseSlots(capacity);
        if (nullSlot != nullptr) {
            nullSlot->~Slot();
            allocator.Release(reinterpret_cast<uint8_t *>(nullSlot), sizeof(Slot));
        }
    }

    void ReleaseSlots(uint64_t newCapacity) noexcept
    {
        allocator.Release(slotsAddress, sizeof(Slot) * newCapacity);
        allocator.Release(ctrlAddress, sizeof(ctrl_t) * (newCapacity + Group::kWidth));
    }

    class HashmapIteratorOutput {
    public:
        HashmapIteratorOutput(BaseHashMap<KeyType, ValueType, HashType, GrowStrategy, Allocator> *mapPtr, uint32_t pos,
            uint32_t hasBeenOutputCount, ctrl_t *identifiers)
            : hashMapPtr(mapPtr)
        {
            remainSlot = hashMapPtr->GetElementsSize() - (hashMapPtr->HasNullCell() ? 1 : 0) - hasBeenOutputCount;

            identifiers_ = std::move(identifiers);
            this->pos = pos;
        }

        template <class Func> OutputState HandleElements(uint32_t expectSize, Func func)
        {
            uint32_t remainHandleSize = expectSize;
            while (remainSlot && remainHandleSize) {
                FindNext();
                --remainSlot;
                --remainHandleSize;
                func((hashMapPtr->slots + pos)->GetKey(), (hashMapPtr->slots + pos)->GetValue());
                // value in cur pos has been assigned , so we need to plus one
                MoveToNext();
            }
            if (remainHandleSize == 0) {
                return OutputState(pos, expectSize);
            }
            if (hashMapPtr->HasNullCell()) {
                --remainHandleSize;
                func(hashMapPtr->nullSlot->GetKey(), hashMapPtr->nullSlot->GetValue());
            }
            return OutputState(pos, expectSize - remainHandleSize);
        }

    private:
        BaseHashMap<KeyType, ValueType, HashType, GrowStrategy, Allocator> *hashMapPtr;
        ctrl_t *identifiers_ = EmptyGroup();
        uint32_t pos = 0;
        uint32_t remainSlot = 0;

        void FindNext()
        {
            while (IsEmptyOrDeleted(*(identifiers_ + pos))) {
                // ctrl is not necessarily aligned to Group::kWidth. It is also likely
                // to read past the space for ctrl bytes and into slots. This is ok
                // because ctrl has sizeof() == 1 and slot has sizeof() >= 1 so there
                // is no way to read outside the combined slot array.
                uint32_t shift = Group { identifiers_ + pos }.CountLeadingEmpty();
                pos += shift;
            }
        }

        void MoveToNext()
        {
            ++pos;
        }
    };

    HashmapIteratorOutput GetOutputMachine(uint32_t pos = 0, uint32_t hasBeenOutputCount = 0)
    {
        return HashmapIteratorOutput(this, pos, hasBeenOutputCount, identifiers);
    }

private:
    void Reinsert(Slot &&cell)
    {
        auto pos = FindPosition(cell.GetKey(), cell.GetHashVal());
        identifiers[pos] = (h2_t)H2(cell.GetHashVal());
        new (&slots[pos]) Slot(std::move(cell));
    }

    void Rehash()
    {
        auto oldIdentifiersAddress = ctrlAddress;
        auto oldElements = elementsSize;
        Slot *oldSlots = slots;
        auto oldSlotsAddress = slotsAddress;
        auto oldIdentifiers = identifiers;
        auto oldCapacity = capacity;

        uint64_t reHashSize = grower.GrowSize();
        InitSlots(reHashSize);
        int remainNum = oldElements - (HasNullCell() ? 1 : 0);
        int index = 0;
        while (remainNum != 0) {
            while (IsEmptyOrDeleted(*(oldIdentifiers + index))) {
                // ctrl is not necessarily aligned to Group::kWidth. It is also likely
                // to read past the space for ctrl bytes and into slots. This is ok
                // because ctrl has sizeof() == 1 and slot has sizeof() >= 1 so there
                // is no way to read outside the combined slot array.
                uint32_t shift = Group { oldIdentifiers + index }.CountLeadingEmpty();
                index += shift;
            }
            Reinsert(std::move(oldSlots[index]));
            --remainNum;
            ++index;
        }

        allocator.Release(oldSlotsAddress, sizeof(Slot) * oldCapacity);
        allocator.Release(oldIdentifiersAddress, sizeof(ctrl_t) * (oldCapacity + Group::kWidth));
    }

    bool NeedRehash()
    {
        return elementsSize > grower.GetThreshHold();
    }

    static bool IsEmptyOrDeleted(ctrl_t c)
    {
        return c < kSentinel;
    }

    ProbeSeq<Group::kWidth> Probe(size_t hashval) const
    {
        return ProbeSeq<Group::kWidth>(H1(hashval), capacity - 1);
    }

    size_t FindPosition(const KeyType &key, size_t hashValue, bool &inserted)
    {
        auto seq = Probe(hashValue);
        while (identifiers[seq.GetOffset()] != kEmpty) {
            Group g { identifiers + seq.GetOffset() };
            // Traverse all the keys which match the low 7 bit hash
            for (uint32_t i : g.Match((h2_t)H2(hashValue))) {
                // Compare the values of key. KeyType must be primitive or overload operator==()
                if (slots[seq.GetOffset((size_t)i)].IsSameKey(hashValue, key)) {
                    return seq.GetOffset((size_t)i);
                }
            }
            auto mask = g.MatchEmpty();
            if (mask) {
                inserted = true;
                return seq.GetOffset((size_t)mask.LowestBitSet());
            }
            seq.GetNext();
        }
        inserted = true;
        return seq.GetOffset();
    }

    // update newIsAssigned when rehash
    size_t FindPosition(const KeyType &key, size_t hashValue)
    {
        auto noFlag = false;
        return FindPosition(key, hashValue, noFlag);
    }

    bool IsNullValue(const KeyType &key)
    {
        return NullValueTypeTraits<KeyType>::IsNullValue(key);
    }

    // call deconstruct function of every cell
    void DeconstructAllSlot()
    {
        int remainNum = HasNullCell() ? elementsSize - 1 : elementsSize;
        int index = 0;
        while (remainNum) {
            while (IsEmptyOrDeleted(*(identifiers + index))) {
                // ctrl is not necessarily aligned to Group::kWidth. It is also likely
                // to read past the space for ctrl bytes and into slots. This is ok
                // because ctrl has sizeof() == 1 and slot has sizeof() >= 1 so there
                // is no way to read outside the combined slot array.
                uint32_t shift = Group { identifiers + index }.CountLeadingEmpty();
                index += shift;
            }
            slots[index].~Slot();
            ++index;
            --remainNum;
        }
        return;
    }

    static ctrl_t *EmptyGroup()
    {
        alignas(16) static constexpr ctrl_t empty_group[] = {
            kSentinel, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty,
            kEmpty,    kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty, kEmpty
        };
        return const_cast<ctrl_t *>(empty_group);
    }

    Allocator allocator;
    uint8_t *slotsAddress;
    Slot *slots;
    ctrl_t *identifiers = EmptyGroup();
    uint8_t *ctrlAddress;
    Slot *nullSlot = nullptr;
    size_t elementsSize = 0;
    HashType hasher;
    uint64_t capacity;
    static constexpr uint8_t defaultDegreeSize = 15;
    GrowStrategy grower;
};

template <typename KeyType, typename ValueType>
using DefaultHashMap = BaseHashMap<KeyType, ValueType, GroupbyHashCalculator<KeyType>, Grower, OmniHashmapAllocator>;
}
}
#endif // OMNI_RUNTIME_BASE_HASH_MAP_H
