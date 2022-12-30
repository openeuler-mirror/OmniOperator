/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * @Description: sort implementations
 */
#include "sort.h"
#include "util/type_util.h"
#include "util/debug.h"
#include "vector/vector_helper.h"
#include "operator/util/operator_util.h"
#include "operator/spill/vector_batch_spiller.h"
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
}

SortOperator::~SortOperator() = default;

int32_t SortOperator::AddInput(VectorBatch *vecBatch)
{
    totalRowCount += vecBatch->GetRowCount();
    if (operatorConfig.GetSpillConfig()->NeedSpill(pagesIndex.get())) {
        auto result = SpillToDisk();
        pagesIndex->Clear();
        if (result != ErrorCode::SUCCESS) {
            throw omniruntime::exception::OmniException(GetErrorCode(result), GetErrorMessage(result));
        }
    }

    pagesIndex->AddVecBatch(vecBatch);
    return 0;
}

// return error code
int32_t SortOperator::GetOutput(vector<VectorBatch *> &outputPages)
{
    // input data is empty, or all data has been returned.
    if (totalRowCount == 0 || totalRowCount == rowCountOutputted) {
        pagesIndex->Clear();
        SetStatus(OMNI_STATUS_FINISHED);
        return 0;
    }
    if (spiller == nullptr) {
        GetOutputFromMemory(outputPages);
    } else {
        MergeFromDiskAndMemory(outputPages);
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

ErrorCode SortOperator::SpillToDisk()
{
    pagesIndex->Prepare();

    Sort();

    if (spiller == nullptr) {
        comparator = new VecBatchWithPositionComparator(sourceTypes, sortCols, sortAscendings, sortNullFirsts);
        spiller = new VectorBatchSpiller(operatorConfig.GetSpillConfig()->GetSpillPath(), sourceTypes, outputCols,
            comparator, vecAllocator);
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
    int32_t to = positionCount;
    int32_t from = 0;
    int32_t sortColTypes[sortColCount];
    for (int32_t i = 0; i < sortColCount; i++) {
        sortColTypes[i] = sourceTypes.GetType(sortCols[i])->GetId();
    }
    pagesIndex->Sort(sortCols.data(), sortColTypes, sortAscendings.data(), sortNullFirsts.data(), sortColCount, from,
        to);
}

void SortOperator::GetVecBatchesForSpill(std::vector<VectorBatch *> &vecBatchesForSpill)
{
    int32_t typesCount = sourceTypes.GetSize();
    std::vector<int32_t> outputCols(typesCount);
    for (int32_t i = 0; i < typesCount; i++) {
        outputCols[i] = i;
    }

    pagesIndex->GetSortedVecBatches(vecAllocator, outputCols, vecBatchesForSpill);
}

void SortOperator::PrepareOutput()
{
    if (pagesIndex->GetRowCount() <= 0 || hasSorted) {
        return;
    }
    pagesIndex->Prepare();
    // first step, sort
    Sort();
    hasSorted = true;
}

void SortOperator::GetOutputFromMemory(vector<VectorBatch *> &outputPages)
{
    // first if has not sorted,need to sort first.
    PrepareOutput();
    // second step, get sorted vector batches
    int32_t rowCountToOutput =
        static_cast<int32_t>(std::min(static_cast<size_t>(maxRowCountPerBatch), (totalRowCount - rowCountOutputted)));

    auto *result = new VectorBatch(outputCols.size(), rowCountToOutput);
    pagesIndex->GetOutput(outputCols.data(), outputCols.size(), result, sourceTypes.GetIds(), rowCountOutputted,
        rowCountToOutput, vecAllocator);
    rowCountOutputted += rowCountToOutput;
    outputPages.emplace_back(result);
}

void SortOperator::MergeFromDiskAndMemory(vector<VectorBatch *> &outputPages)
{
    if (!hasSorted) {
        std::vector<VectorBatch *> vecBatchesForSpill;
        if (pagesIndex->GetRowCount() > 0) {
            pagesIndex->Prepare();
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
        outputPages.push_back(result);
        rowCountOutputted += result->GetRowCount();
        delete vectorBatchUnit;
    }
    hasNext = spiller->HasNext();
}
} // end of namespace op
} // end of namespace omniruntime