/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * @Description: sort implementations
 */
#include "sort.h"

#include <utility>
#include "util/type_util.h"
#include "util/debug.h"
#include "vector/vector_common.h"
#include "vector/vector_helper.h"
#include "operator/util/operator_util.h"
#include "operator/spill/vector_batch_spiller.h"
#include "util/omni_exception.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

SortOperatorFactory::SortOperatorFactory(ContainerDataTypePtr dataTypes, int32_t *outputCols, int32_t outputColCount,
    int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount,
    const OperatorConfig &operatorConfig)
    : operatorConfig(operatorConfig)
{
    this->sourceTypes = std::move(dataTypes);
    this->outputCols.insert(this->outputCols.end(), outputCols, outputCols + outputColCount);
    this->sortCols.insert(this->sortCols.end(), sortCols, sortCols + sortColCount);
    this->sortAscendings.insert(this->sortAscendings.end(), sortAscendings, sortAscendings + sortColCount);
    this->sortNullFirsts.insert(this->sortNullFirsts.end(), sortNullFirsts, sortNullFirsts + sortColCount);
}

SortOperatorFactory::~SortOperatorFactory() = default;

SortOperatorFactory *SortOperatorFactory::CreateSortOperatorFactory(ContainerDataTypePtr dataTypes, int32_t *outputCols,
    int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount)
{
    OperatorConfig defaultConfig;
    return CreateSortOperatorFactory(std::move(dataTypes), outputCols, outputColCount, sortCols, sortAscendings, sortNullFirsts,
        sortColCount, defaultConfig);
}

SortOperatorFactory *SortOperatorFactory::CreateSortOperatorFactory(ContainerDataTypePtr dataTypes, int32_t *outputCols,
    int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount,
    const OperatorConfig &operatorConfig)
{
    OperatorConfig::CheckOperatorConfig(operatorConfig);
    auto pOperatorFactory = new SortOperatorFactory(std::move(dataTypes), outputCols, outputColCount, sortCols, sortAscendings,
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
SortOperator::SortOperator(ContainerDataTypePtr dataTypes, std::vector<int32_t> &outputCols, std::vector<int32_t> &sortCols,
    std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts, const OperatorConfig &operatorConfig)
    : sourceTypes(std::move(dataTypes)), operatorConfig(operatorConfig)
{
    this->outputCols = outputCols;
    this->sortCols = sortCols;
    this->sortAscendings = sortAscendings;
    this->sortNullFirsts = sortNullFirsts;
    this->pagesIndex = std::make_unique<PagesIndex>(this->sourceTypes);
}

SortOperator::~SortOperator() = default;

int32_t SortOperator::AddInput(VectorBatch *vecBatch)
{
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
    if (spiller == nullptr) {
        GetOutputFromMemory(outputPages);
    } else {
        MergeFromDiskAndMemory(outputPages);
    }

    pagesIndex->Clear();
    SetStatus(OMNI_STATUS_FINISHED);
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
        sortColTypes[i] = sourceTypes->GetFieldType(sortCols[i])->GetId();
    }
    pagesIndex->Sort(sortCols.data(), sortColTypes, sortAscendings.data(), sortNullFirsts.data(), sortColCount, from,
        to);
}

void SortOperator::GetVecBatchesForSpill(std::vector<VectorBatch *> &vecBatchesForSpill)
{
    int32_t typesCount = sourceTypes->GetSize();
    std::vector<int32_t> outputCols(typesCount);
    for (int32_t i = 0; i < typesCount; i++) {
        outputCols[i] = i;
    }

    pagesIndex->GetSortedVecBatches(vecAllocator, outputCols, vecBatchesForSpill);
}

void SortOperator::GetOutputFromMemory(vector<VectorBatch *> &outputPages)
{
    if (pagesIndex->GetRowCount() <= 0) {
        return;
    }

    pagesIndex->Prepare();
    // first step, sort
    Sort();
    // second step, get sorted vector batches
    pagesIndex->GetSortedVecBatches(vecAllocator, outputCols, outputPages);
}

void SortOperator::MergeFromDiskAndMemory(vector<VectorBatch *> &outputPages)
{
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
    while (spiller->HasNext()) {
        VectorBatchUnit *vectorBatchUnit = static_cast<VectorBatchUnit *>(spiller->Next());
        outputPages.push_back(vectorBatchUnit->GetVectorBatch());
        delete vectorBatchUnit;
    }
}
} // end of namespace op
} // end of namespace omniruntime