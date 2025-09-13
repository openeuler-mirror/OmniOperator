/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 * @Description: RowRef implementation, borrowed from Doris.
 */

#ifndef __ROWREF_H__
#define __ROWREF_H__

#include "memory/simple_arena_allocator.h"

struct RowRef {
    uint32_t rowIdx = 0;
    uint32_t vecBatchIdx = 0;

    RowRef() = default;
    RowRef(uint32_t rowIdx, uint32_t vecBatchIdx) : rowIdx(rowIdx), vecBatchIdx(vecBatchIdx) {}
};

template <typename RowRefType> struct Batch {
    explicit Batch(Batch<RowRefType> *parent) : next(parent), size(0) {}

    bool IsFull() const
    {
        return size == MAX_SIZE;
    }

    Batch<RowRefType> *Insert(RowRefType &&rowRef, omniruntime::mem::SimpleArenaAllocator &pool)
    {
        if (IsFull()) {
            auto rowBatch = reinterpret_cast<Batch<RowRefType> *>(pool.Allocate(sizeof(Batch<RowRefType>)));
            *rowBatch = Batch<RowRefType>(this);
            rowBatch->Insert(std::move(rowRef), pool);
            return rowBatch;
        }

        rowRefs[size++] = std::move(rowRef);
        return this;
    }

    static constexpr size_t MAX_SIZE = 7;

    Batch<RowRefType> *next = nullptr;
    uint32_t size;
    RowRefType rowRefs[MAX_SIZE];
};

template <typename RowRefListType> class ForwardIterator {
public:
    using RowRefType = typename RowRefListType::RowRefType;
    ForwardIterator() : root(nullptr), isHead(false), rowBatch(nullptr), posInBatch(0) {}

    explicit ForwardIterator(RowRefListType *begin) : root(begin), isHead(true), rowBatch(root->next), posInBatch(0) {}

    RowRefType &operator*()
    {
        if (isHead) {
            return *root;
        }
        return rowBatch->rowRefs[posInBatch];
    }

    RowRefType *operator->()
    {
        return &(**this);
    }

    bool operator == (const ForwardIterator<RowRefListType> &rhs) const
    {
        if (IsOk() != rhs.IsOk()) {
            return false;
        }
        if (isHead && rhs.isHead) {
            return true;
        }
        return rowBatch == rhs.rowBatch && posInBatch == rhs.posInBatch;
    }

    bool operator != (const ForwardIterator<RowRefListType> &rhs) const
    {
        return !(*this == rhs);
    }

    void operator ++ ()
    {
        if (isHead) {
            isHead = false;
            return;
        }

        if (rowBatch) {
            ++posInBatch;
            if (posInBatch >= rowBatch->size) {
                rowBatch = rowBatch->next;
                posInBatch = 0;
            }
        }
    }

    bool IsOk() const
    {
        return isHead || rowBatch;
    }

    static ForwardIterator<RowRefListType> End()
    {
        return ForwardIterator();
    }

private:
    RowRefListType *root;
    bool isHead;
    Batch<RowRefType> *rowBatch;
    size_t posInBatch;
};

struct RowRefList : RowRef {
    using RowRefType = RowRef;

    RowRefList() = default;
    RowRefList(uint32_t rowIdx, uint32_t vecBatchIdx) : RowRef(rowIdx, vecBatchIdx) {}

    ForwardIterator<RowRefList> Begin()
    {
        return ForwardIterator<RowRefList>(this);
    }

    static ForwardIterator<RowRefList> End()
    {
        return ForwardIterator<RowRefList>::End();
    }

    void Insert(RowRef &&rowRef, omniruntime::mem::SimpleArenaAllocator &pool)
    {
        rowCnt++;

        if (!next) {
            next = reinterpret_cast<Batch<RowRefType> *>(pool.Allocate(sizeof(Batch<RowRefType>)));
            *next = Batch<RowRefType>(nullptr);
        }
        next = next->Insert(std::move(rowRef), pool);
    }

    uint32_t GetRowCount()
    {
        return rowCnt;
    }

private:
    friend class ForwardIterator<RowRefList>;

    Batch<RowRefType> *next = nullptr;
    uint32_t rowCnt = 1;
};

struct RowRefWithFlag : public RowRef {
    bool visited;

    RowRefWithFlag() = default;
    RowRefWithFlag(uint32_t rowIdx, uint32_t vecBatchIdx, bool isVisited = false)
        : RowRef(rowIdx, vecBatchIdx), visited(isVisited)
    {}
};

struct RowRefListWithFlags : RowRefWithFlag {
    using RowRefType = RowRefWithFlag;

    RowRefListWithFlags() = default;
    RowRefListWithFlags(uint32_t rowIdx, uint32_t vecBatchIdx) : RowRefWithFlag(rowIdx, vecBatchIdx) {}

    ForwardIterator<RowRefListWithFlags> Begin()
    {
        return ForwardIterator<RowRefListWithFlags>(this);
    }

    static ForwardIterator<RowRefListWithFlags> End()
    {
        return ForwardIterator<RowRefListWithFlags>::End();
    }

    void Insert(RowRefWithFlag &&rowRef, omniruntime::mem::SimpleArenaAllocator &pool)
    {
        rowCnt++;

        if (!next) {
            next = reinterpret_cast<Batch<RowRefType> *>(pool.Allocate(sizeof(Batch<RowRefType>)));
            *next = Batch<RowRefType>(nullptr);
        }
        next = next->Insert(std::move(rowRef), pool);
    }

    uint32_t GetRowCount()
    {
        return rowCnt;
    }

private:
    friend class ForwardIterator<RowRefListWithFlags>;

    Batch<RowRefType> *next = nullptr;
    uint32_t rowCnt = 1;
};

#endif