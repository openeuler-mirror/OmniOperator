/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: vector batch merger
 */

#ifndef OMNI_RUNTIME_VECTOR_BATCH_MERGER_H
#define OMNI_RUNTIME_VECTOR_BATCH_MERGER_H

#include <queue>
#include "spill_iterator.h"
#include "vector/vector_batch.h"
#include "operator/util/operator_util.h"

namespace omniruntime {
namespace op {
class VecBatchWithPosition {
public:
    VecBatchWithPosition(omniruntime::vec::VectorBatch *vecBatch, int32_t position)
        : vecBatch(vecBatch), position(position)
    {}

    ~VecBatchWithPosition() = default;

    omniruntime::vec::VectorBatch *GetVectorBatch()
    {
        return vecBatch;
    }

    int32_t GetPosition() const
    {
        return position;
    }

private:
    omniruntime::vec::VectorBatch *vecBatch;
    int32_t position;
};

class VecBatchWithPositionIterator {
public:
    VecBatchWithPositionIterator() = default;

    virtual ~VecBatchWithPositionIterator() = default;

    virtual bool HasNext()
    {
        return false;
    }

    virtual VecBatchWithPosition *Next()
    {
        return nullptr;
    }
};

class SpillIterator : public VecBatchWithPositionIterator {
public:
    explicit SpillIterator(VectorBatchUnitIter *iterator) : vecBatchUnitIter(iterator) {}

    ~SpillIterator() override
    {
        delete vecBatchUnitIter;
    }

    bool HasNext() override;

    VecBatchWithPosition *Next() override;

private:
    VectorBatchUnitIter *vecBatchUnitIter; // the iterator get VecBatch from disk or memory
    omniruntime::vec::VectorBatch *vecBatch = nullptr;
    int32_t position = 0;
    int32_t positionCount = 0;
};

class ElementAndIterator {
public:
    explicit ElementAndIterator(VecBatchWithPositionIterator *iterator) : element(nullptr), iterator(iterator) {}

    ~ElementAndIterator() = default;

    void SetElement(VecBatchWithPosition *vecBatchWithPosition)
    {
        this->element = vecBatchWithPosition;
    }

    VecBatchWithPosition *GetElement()
    {
        return element;
    }

    VecBatchWithPositionIterator *GetIterator()
    {
        return iterator;
    }

private:
    VecBatchWithPosition *element;
    VecBatchWithPositionIterator *iterator;
};

class VecBatchWithPositionComparator {
public:
    VecBatchWithPositionComparator(omniruntime::type::ContainerDataTypePtr sourceTypes, std::vector<int32_t> &sortCols,
        std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts);

    ~VecBatchWithPositionComparator() = default;

    bool Less(ElementAndIterator *lhs, ElementAndIterator *rhs)
    {
        VecBatchWithPosition *left = lhs->GetElement();
        VecBatchWithPosition *right = rhs->GetElement();
        int32_t result =
            CompareTo(left->GetVectorBatch(), left->GetPosition(), right->GetVectorBatch(), right->GetPosition());
        return (result > 0);
    }

private:
    int32_t CompareTo(VectorBatch *leftVectorBatch, int32_t leftPosition, VectorBatch *rightVectorBatch,
        int32_t rightPosition);

    std::vector<int32_t> sortCols;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    std::vector<OperatorUtil::CompareFunc> sortCompareFuncs;
};

template <typename Comparator> class ComparatorWrapper {
    Comparator *comp;

public:
    explicit ComparatorWrapper(Comparator &comp) : comp(&comp) {}

    ~ComparatorWrapper() = default;

    template <typename T> bool operator () (const T &lhs, const T &rhs) const
    {
        return comp->Less(lhs, rhs);
    }
};

class VectorBatchMerger : public VecBatchWithPositionIterator {
public:
    explicit VectorBatchMerger(VecBatchWithPositionComparator *comparator)
    {
        this->comparator = new ComparatorWrapper<VecBatchWithPositionComparator>(*comparator);
    }

    ~VectorBatchMerger() override
    {
        delete comparator;
    }

    void MergeFromDiskAndMemory(std::vector<VecBatchWithPositionIterator *> disks,
        VecBatchWithPositionIterator *memory);

    bool HasNext() override;

    VecBatchWithPosition *Next() override;

    void PushHeap(ElementAndIterator *elementAndIterator)
    {
        elementAndIterators.push_back(elementAndIterator);
        std::push_heap(elementAndIterators.begin(), elementAndIterators.end(), *comparator);
    }

    ElementAndIterator *PopHeap()
    {
        auto result = elementAndIterators.front();
        std::pop_heap(elementAndIterators.begin(), elementAndIterators.end(), *comparator);
        elementAndIterators.pop_back();
        return result;
    }

private:
    std::vector<ElementAndIterator *> elementAndIterators;
    ElementAndIterator *curElementAndIterator = nullptr;
    ComparatorWrapper<VecBatchWithPositionComparator> *comparator = nullptr;
};
}
}
#endif // OMNI_RUNTIME_VECTOR_BATCH_MERGER_H
