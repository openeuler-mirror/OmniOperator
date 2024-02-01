/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: vector batch merger implements
 */
#include "vector_batch_merger.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::type;

bool SpillIterator::HasNext()
{
    if (position < positionCount) {
        return true;
    }

    // position >= positionCount, we will get next VectorBatch here
    if (vecBatch) {
        VectorHelper::FreeVecBatch(vecBatch);
        vecBatch = nullptr;
    }

    if (vecBatchUnitIter->HasNext()) {
        vecBatch = vecBatchUnitIter->Next()->GetVectorBatch();
        positionCount = vecBatch->GetRowCount();
        position = 0;
        return true;
    } else {
        return false;
    }
}

VecBatchWithPosition *SpillIterator::Next()
{
    return new VecBatchWithPosition(vecBatch, position++);
}

VecBatchWithPositionComparator::VecBatchWithPositionComparator(omniruntime::type::DataTypes &sourceTypes,
    std::vector<int32_t> &sortCols, std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts)
    : sortCols(sortCols), sortAscendings(sortAscendings), sortNullFirsts(sortNullFirsts)
{
    auto sortColsCount = sortCols.size();
    for (size_t i = 0; i < sortColsCount; i++) {
        int32_t sortColTypeId = sourceTypes.GetType(sortCols[i])->GetId();
        switch (sortColTypeId) {
            case OMNI_INT:
            case OMNI_DATE32:
                sortCompareFuncs.push_back(OperatorUtil::CompareTemplate<int32_t>);
                break;
            case OMNI_SHORT:
                sortCompareFuncs.push_back(OperatorUtil::CompareTemplate<int16_t>);
                break;
            case OMNI_DECIMAL64:
            case OMNI_LONG:
                sortCompareFuncs.push_back(OperatorUtil::CompareTemplate<int64_t>);
                break;
            case OMNI_BOOLEAN:
                sortCompareFuncs.push_back(OperatorUtil::CompareTemplate<bool>);
                break;
            case OMNI_DOUBLE:
                sortCompareFuncs.push_back(OperatorUtil::CompareDouble);
                break;
            case OMNI_CHAR:
            case OMNI_VARCHAR:
                sortCompareFuncs.push_back(OperatorUtil::CompareVarchar);
                break;
            case OMNI_DECIMAL128:
                sortCompareFuncs.push_back(OperatorUtil::CompareDecimal128);
                break;
            default:
                break;
        }
    }
}

int32_t VecBatchWithPositionComparator::CompareTo(VectorBatch *leftVectorBatch, int32_t leftPosition,
    VectorBatch *rightVectorBatch, int32_t rightPosition)
{
    auto sortColsCount = sortCols.size();
    for (size_t i = 0; i < sortColsCount; i++) {
        int32_t sortCol = sortCols[i];
        BaseVector *leftVector = leftVectorBatch->Get(sortCol);
        BaseVector *rightVector = rightVectorBatch->Get(sortCol);

        int32_t compare =
            OperatorUtil::CompareNull(leftVector, leftPosition, rightVector, rightPosition, sortNullFirsts[i]);
        if (compare == OperatorUtil::COMPARE_STATUS_OTHER) {
            // neither the left nor the right is NULL
            compare = sortCompareFuncs[i](leftVector, leftPosition, rightVector, rightPosition);
            if (sortAscendings[i] == 0) {
                compare = -compare;
            }
        }

        if (compare == OperatorUtil::COMPARE_STATUS_EQUAL) {
            continue;
        } else {
            return compare;
        }
    }
    return 0;
}

void VectorBatchMerger::MergeFromDiskAndMemory(std::vector<VecBatchWithPositionIterator *> disks,
    VecBatchWithPositionIterator *memory)
{
    auto spillSize = disks.size();
    for (size_t i = 0; i < spillSize; i++) {
        auto iter = disks[i];
        if (iter->HasNext()) {
            VecBatchWithPosition *vecBatchWithPosition = iter->Next();
            auto elementAndIterator = new ElementAndIterator(iter);
            elementAndIterator->SetElement(vecBatchWithPosition);
            PushHeap(elementAndIterator);
        }
    }

    if (memory != nullptr && memory->HasNext()) {
        VecBatchWithPosition *vecBatchWithPosition = memory->Next();
        auto elementAndIterator = new ElementAndIterator(memory);
        elementAndIterator->SetElement(vecBatchWithPosition);
        PushHeap(elementAndIterator);
    }
}

bool VectorBatchMerger::HasNext()
{
    if (curElementAndIterator) {
        auto iterator = curElementAndIterator->GetIterator();
        if (iterator->HasNext()) {
            curElementAndIterator->SetElement(iterator->Next());
            PushHeap(curElementAndIterator);
        } else {
            delete iterator;
            delete curElementAndIterator;
            curElementAndIterator = nullptr;
        }
    }
    if (elementAndIterators.empty()) {
        return false;
    }

    curElementAndIterator = PopHeap();
    return true;
}

VecBatchWithPosition *VectorBatchMerger::Next()
{
    return curElementAndIterator->GetElement();
}
}
}
