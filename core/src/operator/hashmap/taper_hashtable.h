/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2028. All rights reserved.
*/

#pragma once

#include <array>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <type_traits>

#include "util/compiler_util.h"
#include "util/bit_util.h"
#include "memory/simple_arena_allocator.h"


// #define TAPER_HASH_STAT
// #define HWP_PREFETCH
// #define DISABLE_PREFETCH
#define DUMMY_CMP [](auto, auto, auto) { return false; }

namespace omniruntime::op {

static constexpr size_t kHashMapPrefetchDist = 16;

namespace taper {

struct FixedBuf {
  char buf[0];
};

constexpr size_t broadcastByte(uint8_t val, size_t num) {
  size_t ret = 0;
  size_t typedVal = val;
  for (size_t i = 0; i < num; i++) {
    ret |= (typedVal << (i * 8));
  }
  return ret;
}

// refer to:
// https://github.com/greg7mdp/parallel-hashmap/blob/f5e1638a912cdb83fde49ed4611acf9832b54586/parallel_hashmap/phmap.h#L230
class PHBitMask {
  static constexpr int kShift = 3;

 public:
  explicit PHBitMask(uint64_t mask) : mask_(mask) {}

  PHBitMask& operator++() { // ++iterator
    mask_ &= (mask_ - 1); // clear the least significant bit set
    return *this;
  }

  explicit operator bool() const {
    return mask_ != 0;
  }
  uint32_t operator*() const {
    return lowestBitSet();
  }

  uint32_t lowestBitSet() const {
    return __builtin_ctzll(mask_) >> kShift;
  }

  PHBitMask begin() const {
    return *this;
  }
  PHBitMask end() const {
    return PHBitMask(0);
  }

  static inline PHBitMask matchTag(uint64_t tagVal, uint8_t tagHash) {
    constexpr uint64_t kMsbs = 0x8080808080808080ULL;
    constexpr uint64_t kLsbs = 0x0101010101010101ULL;
    auto x = tagVal ^ (kLsbs * tagHash);
    return PHBitMask((x - kLsbs) & ~x & kMsbs);
  }

  static inline PHBitMask matchEmpty(
      uint64_t tagVal,
      uint64_t msbs = 0x8080808080808080ULL) {
    return PHBitMask((tagVal & (~tagVal << 7)) & msbs);
  }

 private:
  friend bool operator==(const PHBitMask& a, const PHBitMask& b) {
    return a.mask_ == b.mask_;
  }
  friend bool operator!=(const PHBitMask& a, const PHBitMask& b) {
    return a.mask_ != b.mask_;
  }

  uint64_t mask_;
};

template <uint8_t Count, uint8_t Step>
static constexpr std::array<uint8_t, Count> genByteSeq() {
  std::array<uint8_t, Count> result{};
  for (uint8_t i = 0; i < Count; ++i) {
    result[i] = i * Step;
  }
  return result;
}

} // namespace taper

class TaperHashTableChunk {
 public:
  uint8_t* tagsBuf() {
    return buf();
  }

  uint8_t* buf() {
    return u_.buf_;
  }
  const uint8_t* buf() const {
    return u_.buf_;
  }

  uint64_t getU64Tags() {
    // TODO 对于小端服务器这里需要提供一个加载小端的版本
    return *reinterpret_cast<uint64_t*>(u_.buf_);
  }

 public:
  static constexpr uint8_t kPointerSignificantBits = 48;
  static constexpr uint64_t kPointerMask =
      BitUtil::LowMask(kPointerSignificantBits);
  static constexpr uint8_t kChunkSize = 128;

 private:

  union {
    uint8_t buf_[kChunkSize];
  } u_;
};

class TaperContainer {
 public:
  virtual size_t allocatedBytes() const = 0;
  virtual size_t size() const = 0;
  virtual void printStats() = 0;
  virtual void clear() = 0;
};

template <typename Key, bool KeyScattered = false>
class TaperHashTableBase : public TaperContainer {
 public:
  using Value = taper::FixedBuf;
  using Chunk = TaperHashTableChunk;
  using ChunkPtr = TaperHashTableChunk*;
  using ChunkPos = uint32_t;

  // from raw_hash_set
  // using difference_type = ptrdiff_t;

  TaperHashTableBase(
      mem::SimpleArenaAllocator& memPool,
      uint32_t keySize,
      uint32_t valueSize)
      : pool(memPool),
        keySize_(keySize),
        valueSize_(valueSize),
        rowSize_(keySize + valueSize) {}

  TaperHashTableBase(const TaperHashTableBase& other) = delete;
  TaperHashTableBase(TaperHashTableBase&&) noexcept = delete;
  TaperHashTableBase& operator=(const TaperHashTableBase&) = delete;
  TaperHashTableBase& operator=(TaperHashTableBase&&) noexcept = delete;
#ifdef DEBUG_STAT_ENABLED
  virtual ~TaperHashTableBase() {
    debugStat_.print();
  };
#endif

  size_t size() const override {
    return size_;
  }

  uint32_t rowSize() const {
    return rowSize_;
  }

  size_t capacity() {
    return getChunksCapacity() * elemNumInChunk_;
  }

  void clear() override {
    init(0);
  }

  ALWAYS_INLINE
  size_t hash(const Key& x) const {
    if constexpr (KeyScattered) {
      return x;
    } else {
      return hashCalculator(x);
    }
  }

  size_t allocatedBytes() const override {
    return getChunksCapacity() * sizeof(TaperHashTableChunk);
  }

  void printStats() override {
#ifdef TAPER_HASH_STAT
    LOG(WARNING) << "key size: " << keySize_;
    LOG(WARNING) << "taper hash p1 statistics: ";
    LOG(WARNING) << "emplace count: " << p1EmplaceCount_
                 << ", collision count: " << p1CollisionCount_
                 << ", first collision count: " << p1FirstCollisionCount_
                 << ", max collision step: " << p1MaxCollisionStep_;
    LOG(WARNING) << "rehash collision count: " << p1RehashCollisionCount_
                 << ", rehash first collision count: "
                 << p1RehashFirstCollisionCount_
                 << ", rehash max collision step: "
                 << p1RehashMaxCollisionStep_;
    LOG(WARNING) << "taper hash p2 statistics: ";
    LOG(WARNING) << "emplace count: " << p2EmplaceCount_
                 << ", collision count: " << p2CollisionCount_
                 << ", first collision count: " << p2FirstCollisionCount_
                 << ", max collision step: " << p2MaxCollisionStep_;
    LOG(WARNING) << "rehash collision count: " << p2RehashCollisionCount_
                 << ", rehash first collision count: "
                 << p2RehashFirstCollisionCount_
                 << ", rehash max collision step: " << p2RehashMaxCollisiontep_;
#endif
#if (defined(HWP_PREFETCH) && !defined(DISABLE_PREFETCH))
    LOG(WARNING) << "hwp prefetch count: " << hwpPrefetchCount_
                 << ", hwp prefetch items num: " << hwpPrefetchItemsNum_
                 << ", hwp terminate num: " << hwpPrefetchTerminateCount_;
#endif
  }

  mem::SimpleArenaAllocator &Pool()
  {
      return pool;
  }

  virtual std::string dbgDump(const std::string& fnPart) = 0;

 private:
  ChunkPos getChunkPos(size_t hashVal) {
    return hashVal & lastChunkIdx_;
  }

  ChunkPos getRehashPos(size_t collisionBatch, ChunkPos curPos) {
    return (curPos + collisionBatch) & lastChunkIdx_;
  }

  bool shouldExpand() {
    return size_ >= expandThreshold_;
  }

  void ALWAYS_INLINE prefetch(ChunkPos pos) {
    auto* chunk = chunks_ + pos;
    __builtin_prefetch(chunk);
    __builtin_prefetch(reinterpret_cast<char*>(chunk) + 64);
    static_assert(sizeof(TaperHashTableChunk) == 128);
  }

  void ALWAYS_INLINE
  prefetchIdx(const std::vector<ChunkPos>& indices, size_t idx, size_t idxEnd) {
#if (!defined(DISABLE_PREFETCH))
    auto pi = idx + kHashMapPrefetchDist;
    if (LIKELY(pi < idxEnd)) {
      prefetch(indices[pi]);
    }
#endif
  }

  void ALWAYS_INLINE
  prefetchIdx(const std::vector<ChunkPos>& indices, size_t idx) {
    prefetchIdx(indices, idx, indices.size());
  }

  void hwpPrefetch(const std::vector<ChunkPos>& indices) {
    hwpPrefetch(indices, indices.size());
  }

  void hwpPrefetch(const std::vector<ChunkPos>& indices, size_t size) {
#if (defined(HWP_PREFETCH) && !defined(DISABLE_PREFETCH))
    ++hwpPrefetchCount_;
    hwpPrefetchItemsNum_ += size;
    setupLinkHWP(chunks_, sizeof(TaperHashTableChunk), indices.data(), size);
#endif
  }

  void
  hwpPrefetch(const std::vector<ChunkPos>& indices, size_t begin, size_t end) {
#if (defined(HWP_PREFETCH) && !defined(DISABLE_PREFETCH))
    hwpPrefetchItemsNum_ += (end - begin);
    ++hwpPrefetchCount_;
    setupLinkHWP(
        chunks_,
        sizeof(TaperHashTableChunk),
        indices.data() + begin,
        end - begin);
#endif
  }

  void hwpPrefetchTerminate() {
#if (defined(HWP_PREFETCH) && !defined(DISABLE_PREFETCH))
    ++hwpPrefetchTerminateCount_;
    terminateLinkRprfm();
#endif
  }

  static inline bool isValidCapacity(size_t n) {
    return n > 0 && ((n - 1) & n) == 0;
  }

 protected:
  constexpr static uint8_t kEmptyTag = 0x80;

  union VisitorPos {
    using ChunkIdx = uint16_t;

    struct {
      ChunkIdx chunk;
      uint16_t tag;
    } chunkPos;
    uint32_t pos;
    VisitorPos() : pos(0) {}
    VisitorPos(uint16_t chunk, uint16_t tag) : chunkPos{chunk, tag} {}
    explicit VisitorPos(uint32_t val) : pos{val} {}
  };

  struct BatchContext {
    std::vector<uint64_t> hashVals;
    std::vector<ChunkPos> chunkPositions;
    std::vector<VisitorPos> collisionIndices{};

    void resize(uint32_t size) {
      hashVals.resize(size);
      chunkPositions.resize(size);
      collisionIndices.resize(size);
    }
  };

#ifdef TAPER_HASH_STAT
  size_t p1EmplaceCount_ = 0;
  size_t p1CollisionCount_ = 0;
  size_t p1FirstCollisionCount_ = 0;
  size_t p1MaxCollisionStep_ = 0;
  size_t p1RehashCollisionCount_ = 0;
  size_t p1RehashFirstCollisionCount_ = 0;
  size_t p1RehashMaxCollisionStep_ = 0;

  size_t p2EmplaceCount_ = 0;
  size_t p2CollisionCount_ = 0;
  size_t p2FirstCollisionCount_ = 0;
  size_t p2MaxCollisionStep_ = 0;
  size_t p2RehashFirstCollisionCount_ = 0;
  size_t p2RehashMaxCollisiontep_ = 0;
  size_t p2RehashCollisionCount_ = 0;
#endif

#if (defined(HWP_PREFETCH) && !defined(DISABLE_PREFETCH))
  size_t hwpPrefetchCount_ = 0;
  size_t hwpPrefetchItemsNum_ = 0;
  size_t hwpPrefetchTerminateCount_ = 0;
#endif

#ifdef DEBUG_STAT_ENABLED
  debug::DebugStat* debugStat() {
    return &debugStat_;
  }
#endif

 private:
  mem::SimpleArenaAllocator &pool;
  GroupbyHashCalculator<Key> hashCalculator {};

  ChunkPtr chunks_ = nullptr;

  BatchContext emplaceContext_;
  BatchContext rehashContext_;
  static constexpr int32_t kStringPrefetchDist = 16;

  uint32_t size_ = 0;
  uint32_t lastChunkIdx_ = 0;
  uint32_t expandThreshold_ = 0;

  uint32_t keySize_ = 0;
  uint32_t valueSize_ = 0;
  uint32_t rowSize_ = 0;
  uint8_t elemNumInChunk_ = 0;

  bool rehashing_ = false;

 protected:
  ChunkPtr chunks() const {
    return chunks_;
  }
  uint32_t keySize() const {
    return keySize_;
  }
  uint32_t valueSize() const {
    return valueSize_;
  }
  uint8_t elemNumInChunk() const {
    return elemNumInChunk_;
  }
  void setElemNumInChunk(uint8_t val) {
    elemNumInChunk_ = val;
  }
  void incSize(uint32_t delta = 1) {
    size_ += delta;
  }

  void init(uint32_t lastChunkIdx) {
    auto chunkCapacity = lastChunkIdx + 1;
    OMNI_CHECK_D(isValidCapacity(chunkCapacity));

    auto bytesNum = chunkCapacity * sizeof(TaperHashTableChunk);
    chunks_ = reinterpret_cast<ChunkPtr>(pool.Allocate(bytesNum));
    memset(chunks_, kEmptyTag, bytesNum);
    lastChunkIdx_ = lastChunkIdx;
    size_ = 0;

    // 7*capacity/8
    auto capacity = chunkCapacity * elemNumInChunk_;
    expandThreshold_ = capacity - (capacity >> 3);
    LogInfo("lastChunkIdx %d, bytesNum: %d", lastChunkIdx, bytesNum);
  }

  template <
      bool InsertOnly,
      typename FKCmp,
      typename Derived,
      typename FInit,
      typename FUpdate>
  void emplaceImpl(
      Derived& derived,
      const Key& key,
      FKCmp&& fKeyCmp,
      FInit&& fInit,
      FUpdate&& fUpdate) {
    auto hashVal = hash(key);
    auto chunkPos = getChunkPos(hashVal);
    size_t collisionBatch = 1;
    while (!derived.template tryEmplaceAtPos<InsertOnly>(
        key,
        hashVal,
        chunkPos,
        std::forward<FKCmp>(fKeyCmp),
        std::forward<FInit>(fInit),
        std::forward<FUpdate>(fUpdate))) {
#ifdef TAPER_HASH_STAT
      if (collisionBatch == 1) {
        p1FirstCollisionCount_++;
        if (rehashing_) {
          p1RehashFirstCollisionCount_++;
        }
      }
      p1CollisionCount_++;
      if (rehashing_) {
        p1RehashCollisionCount_++;
      }
#endif
      OMNI_CHECK_D(collisionBatch <= getChunksCapacity());
      if (shouldExpand()) {
        expandCapacityDirectly(derived);
        chunkPos = getChunkPos(hashVal);
        collisionBatch = 1;
      } else {
        chunkPos = getRehashPos(collisionBatch, chunkPos);
        ++collisionBatch;
      }
    }
#ifdef TAPER_HASH_STAT
    auto step = collisionBatch - 1;
    if (step > p1MaxCollisionStep_) {
      p1MaxCollisionStep_ = step;
    }
    if (rehashing_ && step > p1RehashMaxCollisionStep_) {
      p1RehashMaxCollisionStep_ = step;
    }
#endif
  }

  template <typename Derived>
  void expandCapacityDirectly(Derived& derived) {
    auto vOld = derived.getResultVisitor();
    init(expandLastChunkIdx());
    OMNI_CHECK_D(!rehashing_);
    rehashing_ = true;
    while (!vOld.finished()) {
      derived.rehashEmplace(vOld);
      vOld.next();
    }
    rehashing_ = false;
  }

  template <
      typename Derived,
      typename Filter,
      typename FKCmp,
      typename FInit,
      typename FUpdate>
  void emplaceBatchImpl(
      Derived& derived,
      const Key* keys,
      uint32_t numRows,
      Filter&& filter,
      FKCmp&& fKeyCmp,
      FInit&& fInit,
      FUpdate&& fUpdate) {
    if (capacity() < numRows) {
      // 容量小于插入行数时，有可能在扩容过程中再次触发扩容，目前还没有处理该逻辑
      emplaceBatchDirectly(
          derived,
          keys,
          numRows,
          std::forward<Filter>(filter),
          std::forward<FKCmp>(fKeyCmp),
          std::forward<FInit>(fInit),
          std::forward<FUpdate>(fUpdate));
      return;
    }

    resetEmplaceContext(keys, numRows);
    uint32_t collisionBatch = 1;
    int32_t collisionCount = 0;
    bool resized = false;
    auto resetPositions = [&](size_t begin, size_t end) {
      for (auto i = begin; i != end; i++) {
        emplaceContext_.chunkPositions[i] =
            getChunkPos(emplaceContext_.hashVals[i]);
      }
    };

    auto tryEmplaceIdx = [&]<typename ResizeProc>(
                             uint32_t rowIdx,
                             int32_t hashIdx,
                             ResizeProc&& resizeProc) {
      auto succeed = derived.template tryEmplaceAtPos<false>(
          keyAt(keys, rowIdx),
          emplaceContext_.hashVals[hashIdx],
          emplaceContext_.chunkPositions[hashIdx],
          [&](const Key& key, Chunk& chunk, uint8_t slot) {
            return fKeyCmp(rowIdx, key, chunk, slot);
          },
          [&](auto data) { fInit(rowIdx, data); },
          [&](auto data, bool initFlag) { fUpdate(rowIdx, data, initFlag); });
      if (!succeed) {
#ifdef TAPER_HASH_STAT
        p2CollisionCount_++;
        if (collisionBatch == 1) {
          p2FirstCollisionCount_++;
        }
#endif
        emplaceContext_.collisionIndices[collisionCount].pos = rowIdx;
        emplaceContext_.hashVals[collisionCount] =
            emplaceContext_.hashVals[hashIdx];
        emplaceContext_.chunkPositions[collisionCount] = getRehashPos(
            collisionBatch, emplaceContext_.chunkPositions[hashIdx]);
        collisionCount++;
        if (shouldExpand()) {
          expandCapacityIteratively(derived);
          resized = true;
          resizeProc();
        }
      }
#ifdef TAPER_HASH_STAT
      if (collisionBatch - 1 > p2MaxCollisionStep_) {
        p2MaxCollisionStep_ = collisionBatch - 1;
      }
#endif
    };

    auto tryEmplaceRehashedCollisions = [&] {
      resetPositions(0, collisionCount);
      auto curCount = collisionCount;
      collisionCount = 0;
      hwpPrefetch(emplaceContext_.chunkPositions, curCount);
      for (int32_t idx = 0; idx < curCount; idx++) {
        prefetchIdx(emplaceContext_.chunkPositions, idx, curCount);
        // 这里需要保证只rehash一次（hash表容量超过num_rows才使用该模式），不然这里再次处理rehash的话，逻辑就复杂了
        tryEmplaceIdx(emplaceContext_.collisionIndices[idx].pos, idx, [&] {
          OMNI_CHECK_D(false);
        });
      }
    };

    auto resizeProc = [&](size_t remainIdxFrom, size_t remainIdxTo) {
      // 插入过程中遇到扩容，对于剩余没插入的两批元素处理方法为：
      // 1.
      // 对于由于冲突搁置的元素，统一重新插入一次，以确保当前循环结束后由于冲突未插入元素的冲突批次都是1
      // 2. 对于还未循环到的元素，要重新计算目标chunk位置
      collisionBatch = 1;
      tryEmplaceRehashedCollisions();
      resetPositions(remainIdxFrom, remainIdxTo);
      hwpPrefetch(emplaceContext_.chunkPositions, remainIdxFrom, remainIdxTo);
    };

    hwpPrefetch(emplaceContext_.chunkPositions);
    // 第一遍尝试插入所有元素，并且将冲突的元素记下来等后续处理
    for (uint32_t i = 0; i < numRows; ++i) {
      if (filter(i)) {
        continue;
      }
#ifdef TAPER_HASH_STAT
      p2EmplaceCount_++;
#endif
      prefetchIdx(emplaceContext_.chunkPositions, i, numRows);
      tryEmplaceIdx(i, i, [&] { resizeProc(i + 1, numRows); });
    }

    // 迭代式处理插入冲突，随着迭代次数的增加，冲突元素逐步减少
    while (collisionCount > 0) {
      auto curCount = collisionCount;
      collisionCount = 0;
      collisionBatch++;
      hwpPrefetch(emplaceContext_.chunkPositions, curCount);
      for (int32_t idx = 0; idx < curCount; idx++) {
        prefetchIdx(emplaceContext_.chunkPositions, idx, curCount);
        tryEmplaceIdx(emplaceContext_.collisionIndices[idx].pos, idx, [&] {
          resizeProc(idx + 1, curCount);
        });
      }
    }
  }

  template <
      typename Derived,
      typename Filter,
      typename FKCmp,
      typename FInit,
      typename FUpdate>
  void emplaceBatchDirectly(
      Derived& derived,
      const Key* keys,
      uint32_t numRows,
      Filter&& filter,
      FKCmp&& fKeyCmp,
      FInit&& fInit,
      FUpdate&& fUpdate) {
    for (uint32_t i = 0; i < numRows; ++i) {
      if (filter(i)) {
        continue;
      }
#ifdef TAPER_HASH_STAT
      p1EmplaceCount_++;
#endif
      emplaceImpl<false>(
          derived,
          keyAt(keys, i),
          [&](const Key& key, Chunk& chunk, uint8_t slot) {
            return fKeyCmp(i, key, chunk, slot);
          },
          [&](auto data) { fInit(i, data); },
          [&](auto data, bool initFlag) { fUpdate(i, data, initFlag); });
    }
  }

  template <typename Derived>
  void expandCapacityIteratively(Derived& derived) {
    auto oldChunksNum = getChunksCapacity();
    auto* oldChunks = chunks_;
    hwpPrefetchTerminate();
    init(expandLastChunkIdx());

    derived.rehashChunksIteratively(oldChunks, oldChunksNum);
  }

  template <typename Derived, typename Visitor>
  void rehashBatch(Derived& derived, Visitor visitor) {
    OMNI_CHECK_D(!rehashing_);
    rehashing_ = true;
    // 第一遍处理的时候，顺序访问所有旧的元素，同时生成hash值以及目标chunk位置
    // 第二遍处理的时候，src_collision_positions里记录所有冲突元素的位置（chunk位置以及tag位置），并更新hash值以及目标chunk位置
    // 所以src_hash_values和dst_chunk_positions数组里的值始终和当前要访问的src元素顺序是match的
    resetRehashContext(visitor);
    uint32_t collisionBatch = 1;
    uint32_t collisionCount = 0;
    // 只在指定位置（dst_chunk_positions[srcItemIdx]）尝试插入一个元素，插入失败就记下来等下一轮处理
    auto tryEmplace =
        [&]<typename VisitorPosRecorder>(
            VisitorPos vPos, size_t srcItemIdx, VisitorPosRecorder recorder) {
          auto succeed = derived.tryRehashAtPos(
              visitor,
              vPos,
              rehashContext_.hashVals[srcItemIdx],
              rehashContext_.chunkPositions[srcItemIdx]);
          if (!succeed) {
#ifdef TAPER_HASH_STAT
            p2RehashCollisionCount_++;
            if (collisionBatch == 1) {
              p2RehashFirstCollisionCount_++;
            }
#endif
            recorder();
            rehashContext_.hashVals[collisionCount] =
                rehashContext_.hashVals[srcItemIdx];
            rehashContext_.chunkPositions[collisionCount] = getRehashPos(
                collisionBatch, rehashContext_.chunkPositions[srcItemIdx]);
            collisionCount++;
          }
        };

    hwpPrefetch(rehashContext_.chunkPositions);
    // 第一遍尝试插入所有元素，并且将冲突的元素记下来等后续处理
    for (size_t srcItemIdx = 0; !visitor.finished(); srcItemIdx++) {
      prefetchIdx(rehashContext_.chunkPositions, srcItemIdx);
      auto visitorPos = visitor.curPos();
      tryEmplace(visitorPos, srcItemIdx, [&] {
        rehashContext_.collisionIndices[collisionCount] = visitorPos;
      });
      visitor.next();
    }

    // 迭代式处理插入冲突，随着迭代次数的增加，冲突元素逐步减少
    while (collisionCount > 0) {
      auto curCount = collisionCount;
      collisionCount = 0;
      OMNI_CHECK_D(collisionBatch < getChunksCapacity());
      collisionBatch++;
      hwpPrefetch(rehashContext_.chunkPositions, curCount);
      for (size_t srcItemIdx = 0; srcItemIdx < curCount; srcItemIdx++) {
        prefetchIdx(rehashContext_.chunkPositions, srcItemIdx, curCount);
        tryEmplace(
            rehashContext_.collisionIndices[srcItemIdx], srcItemIdx, [&] {
              rehashContext_.collisionIndices[collisionCount] =
                  rehashContext_.collisionIndices[srcItemIdx];
            });
      }
    }
    rehashing_ = false;
#ifdef TAPER_HASH_STAT
    if (collisionBatch - 1 > p2RehashMaxCollisiontep_) {
      p2RehashMaxCollisiontep_ = collisionBatch - 1;
    }
#endif
  }

  size_t getChunksCapacity() const {
    return lastChunkIdx_ + 1;
  }

  uint32_t expandLastChunkIdx() {
    return 2 * lastChunkIdx_ + 1;
  }

  ALWAYS_INLINE
  bool keyEquals(const Key& l, const Key& r) {
    return l == r;
  }

  const Key& keyAt(const Key* keys, size_t idx) {
    if constexpr (
        std::is_same_v<Key, taper::FixedBuf>) {
      return *reinterpret_cast<const Key*>(
          reinterpret_cast<const uint8_t*>(keys) + idx * keySize_);
    } else {
      return keys[idx];
    }
  }

  const Key& keyAt(char** rows, size_t idx) {
    return *reinterpret_cast<const Key*>(rows[idx]);
  }

  void resetEmplaceContext(const Key* keys, uint32_t numRows) {
    emplaceContext_.resize(numRows);
    for (uint32_t i = 0; i < numRows; i++) {
      auto val = hash(keyAt(keys, i));
      emplaceContext_.hashVals[i] = val;
      emplaceContext_.chunkPositions[i] = getChunkPos(val);
    }
  }

  template <typename Visitor>
  void resetRehashContext(Visitor v) {
    rehashContext_.resize(v.maxElems());

    for (uint32_t i = 0; !v.finished(); i++) {
      auto val = hash(v.curKey());
      rehashContext_.hashVals[i] = val;
      rehashContext_.chunkPositions[i] = getChunkPos(val);
      v.next();
    }
  }

  static void setupLinkHWP(
      const void* elementBaseAddr,
      uint8_t elemSize,
      const uint32_t* indexBaseAddr,
      uint32_t idxSize) {
    constexpr uint32_t LAZY_STEP = 64;
    if (LIKELY(idxSize > LAZY_STEP)) {
      indexBaseAddr += LAZY_STEP;
      idxSize -= LAZY_STEP;
    }

    uint64_t elementSize = elemSize;
    uint64_t indexSize = (idxSize << 2);
    // build data for HWP_LINK_RPRFM_EL0
    uint64_t msrParam = 0;
    msrParam |= reinterpret_cast<uint64_t>(elementBaseAddr) & 0x1ffffffffffff;
    msrParam |= (elementSize & 0xff) << 49;
    msrParam |= (1ULL) << 63;
    asm volatile("msr S3_3_c15_c2_0, %0" : : "r"(msrParam));
    asm volatile("isb");

    register uint64_t x3 asm("x3") = indexSize;
    register uint64_t x6 asm("x6") = (uint64_t)indexBaseAddr;
    __asm__ __volatile__(".inst 0xF8A348D8" : : "r"(x3), "r"(x6) : "memory");
  }

  static void terminateLinkRprfm() {
    // build data for HWP_LINK_RPRFM_EL0
    uint64_t msrParam = 0;
    asm volatile("msr S3_3_c15_c2_0, %0" : : "r"(msrParam));
  }

  template <typename ChunkDumper>
  std::string dbgDumpImpl(
      const std::string& fileNamePart,
      ChunkDumper cDumper) {
    std::ostringstream oss;
    oss << "/tmp/hash-result-" << fileNamePart << "-"
        << reinterpret_cast<void*>(this);
    std::ofstream outfile(oss.str());
    auto endChunk = chunks() + getChunksCapacity();
    for (auto chunk = chunks(); chunk != endChunk; chunk++) {
      int cnt = 0;
      for (int tagPos = 0; tagPos < elemNumInChunk(); tagPos++) {
        if (chunk->tagsBuf()[tagPos] != kEmptyTag) {
          cDumper(chunk, tagPos, outfile);
          cnt++;
        }
      }
      outfile << "------- " << cnt << "\n";
    }
    outfile.close();
    return oss.str();
  }
};

template <typename Key, bool KeyScattered = false>
class TaperFlatHashTable : public TaperHashTableBase<Key, KeyScattered> {
  using Base = TaperHashTableBase<Key, KeyScattered>;
  using Chunk = TaperHashTableChunk;
  using ChunkPtr = TaperHashTableChunk*;
  using ChunkPos = typename Base::ChunkPos;
  using VisitorPos = typename Base::VisitorPos;
  using Value = typename Base::Value;
  using PHBitMask = typename taper::PHBitMask;

  friend class TaperHashTableBase<Key, KeyScattered>;

  // TODO: read config from system
  constexpr static size_t kL1DataSize = 64 * 1024;

 public:
  class Visitor {
    friend class TaperFlatHashTable<Key, KeyScattered>;

   public:
    void next() {
      do {
        tagPos_++;
        if (tagPos_ >= table_.elemNumInChunk()) {
          tagPos_ = 0;
          ++curChunk_;
        }
      } while (curChunk_ != endChunk_ &&
               curChunk_->tagsBuf()[tagPos_] == Base::kEmptyTag);
    }

    bool finished() const {
      return curChunk_ >= endChunk_;
    }

    uint32_t maxElems() const {
      return (endChunk_ - fromChunk_) * table_.elemNumInChunk();
    }

    const Key& curKey() const {
      return table_.getChunkKey(*curChunk_, tagPos_);
    }

    const Value& curVal() const {
      return table_.getChunkValue(*curChunk_, tagPos_);
    }

    template <typename Fn>
    void savePos(Fn&& fn) {
      fn(curChunk_, tagPos_);
    }

    uint8_t rowSize() {
      return table_.rowSize();
    }

   protected:
    TaperFlatHashTable& table_;
    ChunkPtr curChunk_;
    ChunkPtr fromChunk_;
    ChunkPtr endChunk_;
    uint16_t tagPos_;

    // friend class TaperHashTable;

    explicit Visitor(
        TaperFlatHashTable& table,
        ChunkPtr fromChunk = nullptr,
        ChunkPtr endChunk = nullptr,
        uint16_t tagPos = 0)
        : table_(table),
          curChunk_{fromChunk},
          fromChunk_{fromChunk},
          endChunk_{endChunk},
          tagPos_{tagPos} {
      if (curChunk_ < endChunk_ &&
          curChunk_->tagsBuf()[tagPos_] == Base::kEmptyTag) {
        next();
      }
    }

    ChunkPtr getChunk(uint32_t offset) const {
      return fromChunk_ + offset;
    }
  };

  class ShortVisitor : public Visitor {
    using ChunkIdx = typename VisitorPos::ChunkIdx;

   public:
    explicit ShortVisitor(
        TaperFlatHashTable& table,
        ChunkPtr fromChunk = nullptr,
        ChunkPtr endChunk = nullptr,
        uint16_t tagPos = 0)
        : Visitor{table, fromChunk, endChunk, tagPos} {
      OMNI_CHECK_D(Visitor::maxElems() < std::numeric_limits<ChunkIdx>::max());
    }

    VisitorPos curPos() const {
      auto c = static_cast<ChunkIdx>(Visitor::curChunk_ - Visitor::fromChunk_);
      return VisitorPos{c, Visitor::tagPos_};
    }
  };

  TaperFlatHashTable(
      mem::SimpleArenaAllocator &memPool,
      uint8_t keySize,
      uint8_t valueSize)
      : Base(memPool, getKeySize(keySize), valueSize) {
    constexpr size_t kTagByte = 1;
    auto estimateElemSize = kTagByte + Base::keySize() + Base::valueSize();
    OMNI_CHECK_D(estimateElemSize < sizeof(TaperHashTableChunk));
    uint8_t elemNum = sizeof(TaperHashTableChunk) / estimateElemSize;
    while (!initChunkOffsets(elemNum)) {
      elemNum--;
    }
    Base::setElemNumInChunk(elemNum);
    emptyTags_ = taper::broadcastByte(Base::kEmptyTag, elemNum);
    Base::init(0);
  }

  template <typename Filter, typename FInit, typename FUpdate>
  void emplaceBatch(
      const Key* keys,
      uint32_t numRows,
      Filter&& filter,
      FInit&& fInit,
      FUpdate&& fUpdate) {
    Base::emplaceBatchImpl(
        *this,
        keys,
        numRows,
        std::forward<Filter>(filter),
        [&](uint32_t, const Key& key, Chunk& chunk, uint8_t slot) {
          return Base::keyEquals(key, getChunkKey(chunk, slot));
        },
        std::forward<FInit>(fInit),
        std::forward<FUpdate>(fUpdate));
  }
  template <typename FKCmp, typename FInit, typename FUpdate>
  void
  emplace(const Key& key, FKCmp&& fKeyCmp, FInit&& fInit, FUpdate&& fUpdate) {
    Base::template emplaceImpl<false>(*this, key, fKeyCmp, fInit, fUpdate);
  }

  template <typename FInit, typename FUpdate>
 void
  emplace(const Key& key, FInit&& fInit, FUpdate&& fUpdate) {
    Base::template emplaceImpl<false>(*this, key, DUMMY_CMP, fInit, fUpdate);
  }

  Visitor getResultVisitor() {
    auto end = Base::chunks() + Base::getChunksCapacity();
    return Visitor(*this, Base::chunks(), end);
  }

  Visitor getResultVisitor(char* from, uint16_t tagPos) {
    return Visitor(
        *this,
        reinterpret_cast<ChunkPtr>(from),
        Base::chunks() + Base::getChunksCapacity(),
        tagPos);
  }

  Value& getChunkValue(TaperHashTableChunk& chunk, uint32_t idx) {
    return *reinterpret_cast<Value*>(
        chunk.buf() + valOffsetInChunk_ + idx * Base::valueSize());
  }

  std::string dbgDump(const std::string& fnPart) override {
    return Base::dbgDumpImpl(fnPart, [&](auto chunk, auto tagPos, auto& os) {
      auto key = *reinterpret_cast<const int*>(&getChunkKey(*chunk, tagPos));
      // auto val = *reinterpret_cast<double*>(&getChunkValue(*chunk, tagPos));
      os << key << "\n";
    });
  }

 private:
  uint64_t emptyTags_ = 0;
  uint8_t keyOffsetInChunk_ = 0;
  uint8_t valOffsetInChunk_ = 0;

  bool initChunkOffsets(uint8_t elemNum) {
    keyOffsetInChunk_ = (elemNum + 7) & 0xF8;
    valOffsetInChunk_ =
        (keyOffsetInChunk_ + elemNum * Base::keySize() + 15) & 0xF0;
    return valOffsetInChunk_ + elemNum * Base::valueSize() <=
        sizeof(TaperHashTableChunk);
  }

  const Key& getChunkKey(const TaperHashTableChunk& chunk, uint32_t idx) {
    if constexpr (std::is_arithmetic_v<Key>) {
      return (
          reinterpret_cast<const Key*>(chunk.buf() + keyOffsetInChunk_))[idx];
    } else {
      return *reinterpret_cast<const Key*>(
          chunk.buf() + keyOffsetInChunk_ + idx * Base::keySize());
    }
  }

  void setChunkKey(TaperHashTableChunk& chunk, uint32_t idx, const Key& key) {
    if constexpr (
        std::is_same_v<Key, taper::FixedBuf>) {
      auto dst = chunk.buf() + keyOffsetInChunk_ + idx * Base::keySize();
      memcpy(dst, &key, Base::keySize());
    } else {
      (reinterpret_cast<Key*>(chunk.buf() + keyOffsetInChunk_))[idx] = key;
    }
  }

  void copyValue(char* dst, const Value& src) {
    memcpy(dst, &src, Base::valueSize());
  }

  template <bool IsExpansion, typename FKCmp, typename FInit, typename FUpdate>
  bool tryEmplaceAtPos(
      const Key& key,
      size_t hashVal,
      ChunkPos chunkPos,
      FKCmp&& fKeyCmp,
      FInit&& fInit,
      FUpdate&& fUpdate) {
    auto curChunk = Base::chunks() + chunkPos;
    uint8_t tagHash = (hashVal >> 16) & 0x7F;

    auto tags = curChunk->getU64Tags();
    if constexpr (!IsExpansion) {
      for (auto i : PHBitMask::matchTag(tags, tagHash)) {
        if (fKeyCmp(key, *curChunk, i)) {
          fUpdate(getChunkValue(*curChunk, i).buf, false);
          return true;
        }
      }
    }
    for (auto i : PHBitMask::matchEmpty(tags, emptyTags_)) {
      Base::incSize();
      curChunk->tagsBuf()[i] = tagHash;
      setChunkKey(*curChunk, i, key);
      auto& val = getChunkValue(*curChunk, i);
      fInit(val.buf);
      fUpdate(val.buf, true);
      return true;
    }
    return false;
  }

  bool tryRehashAtPos(
      const Visitor& visitor,
      VisitorPos visitorPos,
      size_t hashVal,
      ChunkPos chunkPos) {
    auto chunk = visitor.getChunk(visitorPos.chunkPos.chunk);
    return tryEmplaceAtPos<true>(
        getChunkKey(*chunk, visitorPos.chunkPos.tag),
        hashVal,
        chunkPos,
        DUMMY_CMP,
        [&](char* dstVal) {
          copyValue(dstVal, getChunkValue(*chunk, visitorPos.chunkPos.tag));
        },
        [](auto, bool) {});
  }

  void rehashEmplace(const Visitor& visitor) {
    Base::template emplaceImpl<true>(
        *this,
        visitor.curKey(),
        DUMMY_CMP,
        [&](char* dstVal) { copyValue(dstVal, visitor.curVal()); },
        [](auto, bool) {});
  }

  void rehashChunksIteratively(ChunkPtr chunks, size_t chunksNum) {
    constexpr size_t kStep = kL1DataSize / sizeof(TaperHashTableChunk) * 3 / 4;
    for (size_t chunkEndIdx = 0; chunkEndIdx < chunksNum;) {
      auto* fromChunk = chunks + chunkEndIdx;
      chunkEndIdx = std::min(chunkEndIdx + kStep, chunksNum);
      auto* endChunk = chunks + chunkEndIdx;
      Base::rehashBatch(*this, ShortVisitor{*this, fromChunk, endChunk});
    }
  }

  static uint8_t getKeySize(uint8_t keySizeHint) {
    if constexpr (std::is_arithmetic_v<Key>) {
      return sizeof(Key);
    } else {
      return keySizeHint;
    }
  }
};
} // namespace bytedance::bolt::exec

#undef DUMMY_CMP
