/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: sort implementations
 */
#include "sort.h"
#include "util/type_util.h"
#include "util/debug.h"
#include "vector/vector_helper.h"
#include "operator/util/operator_util.h"
#include "operator/spill/vector_batch_spiller.h"
#include "operator/spill/spill_iterator.h"
#include "util/omni_exception.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

SortOperatorFactory::SortOperatorFactory(const DataTypes &dataTypes, int32_t *outputCols, int32_t outputColCount,
    int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount,
    const OperatorConfig &operatorConfig)
    : sourceTypes(dataTypes), operatorConfig(operatorConfig)
{
    this->outputCols.insert(this->outputCols.end(), outputCols, outputCols + outputColCount);
    this->sortCols.insert(this->sortCols.end(), sortCols, sortCols + sortColCount);
    this->sortAscendings.insert(this->sortAscendings.end(), sortAscendings, sortAscendings + sortColCount);
    this->sortNullFirsts.insert(this->sortNullFirsts.end(), sortNullFirsts, sortNullFirsts + sortColCount);
}

SortOperatorFactory::~SortOperatorFactory() = default;

SortOperatorFactory *SortOperatorFactory::CreateSortOperatorFactory(const DataTypes &dataTypes, int32_t *outputCols,
    int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount)
{
    OperatorConfig defaultConfig;
    return CreateSortOperatorFactory(dataTypes, outputCols, outputColCount, sortCols, sortAscendings, sortNullFirsts,
        sortColCount, defaultConfig);
}

SortOperatorFactory *SortOperatorFactory::CreateSortOperatorFactory(const DataTypes &dataTypes, int32_t *outputCols,
    int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount,
    const OperatorConfig &operatorConfig)
{
    OperatorConfig::CheckOperatorConfig(operatorConfig);
    auto pOperatorFactory = new SortOperatorFactory(dataTypes, outputCols, outputColCount, sortCols, sortAscendings,
        sortNullFirsts, sortColCount, operatorConfig);
    return pOperatorFactory;
}

Operator *SortOperatorFactory::CreateOperator()
{
    auto pSortOperator =
        new SortOperator(sourceTypes, outputCols, sortCols, sortAscendings, sortNullFirsts, operatorConfig);
    return pSortOperator;
}

// function implements for class Sort
SortOperator::SortOperator(const DataTypes &dataTypes, std::vector<int32_t> &outputCols, std::vector<int32_t> &sortCols,
    std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts, const OperatorConfig &operatorConfig)
    : sourceTypes(dataTypes), operatorConfig(operatorConfig)
{
    this->outputCols = outputCols;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->pagesIndex = std::make_unique<PagesIndex>(sourceTypes);
    maxRowCountPerBatch = OperatorUtil::GetMaxRowCount(dataTypes.Get(), outputCols.data(), outputCols.size());
    maxRowCountPerBatch = maxRowCountPerBatch == 0 ? 1 : maxRowCountPerBatch;
    if (sourceTypes.GetSize() == 1) {
        auto typeId = sourceTypes.GetType(0)->GetId();
        if (typeId != OMNI_VARCHAR && typeId != OMNI_CHAR && typeId != OMNI_BOOLEAN) {
            canInplaceSort = true;
        }
    }

    if (not canInplaceSort) {
        this->radixSortSizeThreshold = operatorConfig.GetAdaptivityThreshold();
        canRadixSort = this->CanUseRadixSort();
    }
}

SortOperator::~SortOperator() = default;

int32_t SortOperator::AddInput(VectorBatch *vecBatch)
{
    totalRowCount += vecBatch->GetRowCount();
    pagesIndex->AddVecBatch(vecBatch);
    if (operatorConfig.GetSpillConfig()->NeedSpill(pagesIndex.get())) {
        auto result = SpillToDisk();
        pagesIndex->Clear();
        if (UNLIKELY(result != ErrorCode::SUCCESS)) {
            throw omniruntime::exception::OmniException(GetErrorCode(result), GetErrorMessage(result));
        }
    }
    return 0;
}

// return error code
int32_t SortOperator::GetOutput(VectorBatch **outputVecBatch)
{
    // input data is empty, or all data has been returned.
    if (totalRowCount == 0 || totalRowCount == rowCountOutputted) {
        pagesIndex->Clear();
        SetStatus(OMNI_STATUS_FINISHED);
        return 0;
    }
    if (spiller == nullptr) {
        GetOutputFromMemory(outputVecBatch);
    } else {
        MergeFromDiskAndMemory(outputVecBatch);
    }

    // through the reference counting mechanism, can vecBatch be released early?
    if (totalRowCount == rowCountOutputted) { // all result have been generated
        pagesIndex->Clear();
        SetStatus(OMNI_STATUS_FINISHED);
        return 0;
    }
    return 0;
}

OmniStatus SortOperator::Close()
{
    if (comparator) {
        delete comparator;
    }
    if (spiller) {
        delete spiller;
    }
    // ensure free pagesIndex if exception occurs
    pagesIndex->Clear();
    return OMNI_STATUS_NORMAL;
}

uint64_t SortOperator::GetSpilledBytes()
{
    if (spiller != nullptr) {
        return spiller->GetSpilledBytes();
    } else {
        return 0;
    }
}

ErrorCode SortOperator::SpillToDisk()
{
    if (!canInplaceSort) {
        pagesIndex->Prepare();
    } else {
        DYNAMIC_TYPE_DISPATCH(pagesIndex->PrepareInplaceSort, sourceTypes.GetType(0)->GetId(), sortNullFirsts[0]);
    }

    Sort();

    if (spiller == nullptr) {
        comparator = new VecBatchWithPositionComparator(sourceTypes, sortCols, sortAscendings, sortNullFirsts);
        spiller = new VectorBatchSpiller(operatorConfig.GetSpillConfig()->GetSpillPath(), sourceTypes, outputCols,
            comparator);
        spiller->SetSpillTracker(GetRootSpillTracker().CreateSpillTracker());
    }

    // spill data from memory to disk
    std::vector<VectorBatch *> vecBatchesForSpill;
    GetVecBatchesForSpill(vecBatchesForSpill);

    VectorBatchUnitIter iter(vecBatchesForSpill);
    auto result = spiller->Spill(iter);
    VectorHelper::FreeVecBatches(vecBatchesForSpill);
    return result;
}

void SortOperator::Sort()
{
    int32_t positionCount = pagesIndex->GetRowCount();
    int32_t sortColCount = sortCols.size();
    if (canInplaceSort) {
        pagesIndex->SortInplace(sortCols.data(), sortAscendings.data(), sortNullFirsts.data(), sortColCount, 0,
            positionCount);
    } else if (canRadixSort) {
        pagesIndex->SortWithRadixSort(sortCols.data(), sortAscendings.data(), sortNullFirsts.data(), sortColCount, 0,
            positionCount);
    } else {
        pagesIndex->Sort(sortCols.data(), sortAscendings.data(), sortNullFirsts.data(), sortColCount, 0, positionCount);
    }
}

void SortOperator::GetVecBatchesForSpill(std::vector<VectorBatch *> &vecBatchesForSpill)
{
    int32_t typesCount = sourceTypes.GetSize();
    std::vector<int32_t> outputCols(typesCount);
    for (int32_t i = 0; i < typesCount; i++) {
        outputCols[i] = i;
    }
    // This call GetOutput, which will decide whether to use radix sort
    pagesIndex->GetSortedVecBatches(outputCols, vecBatchesForSpill, canInplaceSort);
}

bool SortOperator::CanUseRadixSort()
{
    bool canUseRadixSort = (sortCols.size() == 1 && radixSortSizeThreshold != -1);
    if (not canUseRadixSort) {
        return false;
    }
    auto sortDataType = sourceTypes.GetType(sortCols[0])->GetId();
    bool isAllowedDataType = (sortDataType == OMNI_LONG || sortDataType == OMNI_DATE32 || sortDataType == OMNI_INT ||
        sortDataType == OMNI_SHORT || sortDataType == OMNI_BOOLEAN || sortDataType == OMNI_DECIMAL64);
    return isAllowedDataType;
}

bool SortOperator::CanUseRadixSortByRuntimeInfo()
{
    return pagesIndex->GetVectorBatchSize() <= UINT16_MAX && pagesIndex->GetRowCount() > radixSortSizeThreshold &&
        !pagesIndex->HasDictionary(sortCols[0]);
}

void SortOperator::PrepareOutput()
{
    if (pagesIndex->GetRowCount() <= 0 || hasSorted) {
        return;
    }
    // update radix sort state
    if (canRadixSort) {
        canRadixSort = CanUseRadixSortByRuntimeInfo();
    }

    if (canInplaceSort) {
        DYNAMIC_TYPE_DISPATCH(pagesIndex->PrepareInplaceSort, sourceTypes.GetType(0)->GetId(), sortNullFirsts[0]);
    } else if (canRadixSort) {
        LogDebug("radix Sort size threshold is %d\n", radixSortSizeThreshold);
        auto typeId = sourceTypes.GetType(sortCols[0])->GetId();
        switch (typeId) {
            case OMNI_LONG:
                pagesIndex->PrepareRadixSort<OMNI_LONG>(sortAscendings[0], sortNullFirsts[0], sortCols[0]);
                break;
            case OMNI_DATE32:
            case OMNI_INT:
                pagesIndex->PrepareRadixSort<OMNI_INT>(sortAscendings[0], sortNullFirsts[0], sortCols[0]);
                break;
            case OMNI_SHORT:
                pagesIndex->PrepareRadixSort<OMNI_SHORT>(sortAscendings[0], sortNullFirsts[0], sortCols[0]);
                break;
            case OMNI_BOOLEAN:
                pagesIndex->PrepareRadixSort<OMNI_BOOLEAN>(sortAscendings[0], sortNullFirsts[0], sortCols[0]);
                break;
            case OMNI_DECIMAL64:
                pagesIndex->PrepareRadixSort<OMNI_DECIMAL64>(sortAscendings[0], sortNullFirsts[0], sortCols[0]);
                break;
            default:
                std::string errStr = "Do not support the data type" + std::to_string(typeId) + " in radix sort.";
                throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", errStr);
        }
    } else {
        pagesIndex->Prepare();
    }
    // first step, sort
    Sort();
    hasSorted = true;
}

void SortOperator::GetOutputFromMemory(VectorBatch **outputVecBatch)
{
    // first if has not sorted,need to sort first.
    PrepareOutput();
    // second step, get sorted vector batches
    int32_t rowCountToOutput =
        static_cast<int32_t>(std::min(static_cast<size_t>(maxRowCountPerBatch), (totalRowCount - rowCountOutputted)));

    auto *result = new VectorBatch(rowCountToOutput);
    if (canInplaceSort) {
        pagesIndex->GetOutputInplaceSort(outputCols.data(), outputCols.size(), result, sourceTypes.GetIds(),
            rowCountOutputted, rowCountToOutput);
    } else if (canRadixSort) {
        pagesIndex->GetOutputRadixSort(outputCols.data(), outputCols.size(), result, sourceTypes.GetIds(),
            rowCountOutputted, rowCountToOutput);
    } else {
        pagesIndex->GetOutput(outputCols.data(), outputCols.size(), result, sourceTypes.GetIds(), rowCountOutputted,
            rowCountToOutput);
    }
    rowCountOutputted += rowCountToOutput;
    *outputVecBatch = result;
}

void SortOperator::MergeFromDiskAndMemory(VectorBatch **outputVecBatch)
{
    if (!hasSorted) {
        std::vector<VectorBatch *> vecBatchesForSpill;
        if (pagesIndex->GetRowCount() > 0) {
            if (!canInplaceSort) {
                pagesIndex->Prepare();
            } else {
                DYNAMIC_TYPE_DISPATCH(pagesIndex->PrepareInplaceSort, sourceTypes.GetType(0)->GetId(),
                    sortNullFirsts[0]);
            }
            // first step, sort
            Sort();
            // second step, get sorted vector batches
            GetVecBatchesForSpill(vecBatchesForSpill);
        }

        // third step, merge data from disk and memory
        VectorBatchUnitIter memoryIter(vecBatchesForSpill);
        spiller->MergeFromDiskAndMemory(memoryIter);
        hasSorted = true;
        hasNext = spiller->HasNext();
    }
    if (hasNext) {
        auto *vectorBatchUnit = static_cast<VectorBatchUnit *>(spiller->Next());
        auto *result = vectorBatchUnit->GetVectorBatch();
        *outputVecBatch = result;
        rowCountOutputted += result->GetRowCount();
        delete vectorBatchUnit;
    }
    hasNext = spiller->HasNext();
}
} // end of namespace op
} // end of namespace omniruntime