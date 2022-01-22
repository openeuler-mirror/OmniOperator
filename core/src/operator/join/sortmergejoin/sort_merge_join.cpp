/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort merge join core implementations
 */

#include "sort_merge_join.h"
#include <memory>
#include "../../../vector/vector_helper.h"
#include "../../util/operator_util.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

SortMergeJoinOperator::SortMergeJoinOperator(JoinType joinType, std::string &filter)
{
    this->joinType = joinType;
    this->filter = filter;
}

SortMergeJoinOperator::~SortMergeJoinOperator()
{
    delete streamedTypes;
    delete bufferedTypes;
    streamedTblPagesIndex->FreeAllRemainingVecBatch();
    bufferedTblPagesIndex->FreeAllRemainingVecBatch();
    delete streamedTblPagesIndex;
    delete bufferedTblPagesIndex;
    delete smjScanner;
    delete joinResultBuilder;
}

void SortMergeJoinOperator::ConfigStreamedTblInfo(const vec::VecTypes &streamedTypes,
    const std::vector<int32_t> &streamedKeysCols, const std::vector<int32_t> &streamedOutputCols)
{
    this->streamedTypes = std::make_unique<vec::VecTypes>(streamedTypes).release();
    this->streamedKeysCols = streamedKeysCols;
    this->streamedOutputCols = streamedOutputCols;
}

void SortMergeJoinOperator::ConfigBufferedTblInfo(const vec::VecTypes &bufferedTypes,
    std::vector<int32_t> &bufferedKeysCols, std::vector<int32_t> &bufferedOutputCols)
{
    this->bufferedTypes = std::make_unique<vec::VecTypes>(bufferedTypes).release();
    this->bufferedKeysCols = bufferedKeysCols;
    this->bufferedOutputCols = bufferedOutputCols;
}

void SortMergeJoinOperator::InitScannerAndResultBuilder()
{
    this->streamedTblPagesIndex = std::make_unique<DynamicPagesIndex>(*this->streamedTypes).release();
    this->bufferedTblPagesIndex = std::make_unique<DynamicPagesIndex>(*this->bufferedTypes).release();

    this->smjScanner = std::make_unique<SortMergeJoinScanner>(*this->streamedTypes, this->streamedKeysCols.data(),
        this->streamedKeysCols.size(), this->streamedTblPagesIndex, *this->bufferedTypes, this->bufferedKeysCols.data(),
        this->bufferedTblPagesIndex, this->joinType, false).release();

    this->joinResultBuilder = std::make_unique<JoinResultBuilder>(*this->streamedTypes, this->streamedOutputCols.data(),
        this->streamedOutputCols.size(), this->streamedTblPagesIndex, *this->bufferedTypes,
        this->bufferedOutputCols.data(), this->bufferedOutputCols.size(), this->bufferedTblPagesIndex, this->filter,
        this->vecAllocator).release();
}

int32_t SortMergeJoinOperator::GetJoinResult()
{
    if (streamedTypes == nullptr) {
        return SMJ_NEED_STREAM_TBL_INFO;
    }

    if (bufferedTypes == nullptr) {
        return SMJ_NEED_BUFFER_TBL_INFO;
    }

    // check streamed table have input, if not return data need to add input
    if (streamedTblPagesIndex->GetPositionCount() == 0 && !streamedTblPagesIndex->IsDataFinish()) {
        return SMJ_NEED_ADD_STREAM_TBL_DATA;
    }

    // check buffered table have input, if not return data need to add input
    if (bufferedTblPagesIndex->GetPositionCount() == 0 && !bufferedTblPagesIndex->IsDataFinish()) {
        return SMJ_NEED_ADD_BUFFER_TBL_DATA;
    }

    // one of join side no data
    if ((streamedTblPagesIndex->GetPositionCount() == 0 && streamedTblPagesIndex->IsDataFinish()) ||
        (bufferedTblPagesIndex->GetPositionCount() == 0 && bufferedTblPagesIndex->IsDataFinish())) {
        return SMJ_NO_RESULT;
    }

    auto joinScannerRet = smjScanner->FindNextJoinRows();

    // 1)put matched rows to result builder, and cache the result
    auto matchResultRet = DecodeJoinResult(joinScannerRet);
    if (matchResultRet == HAS_RESULT) {
        std::vector<bool> isPreKeyMatched;
        std::vector<int64_t> streamedTblMatchedValueAddresses;
        std::vector<int64_t> bufferedTblMatchedValueAddresses;
        smjScanner->GetMatchedValueAddresses(isPreKeyMatched, streamedTblMatchedValueAddresses,
            bufferedTblMatchedValueAddresses);
        auto joinResultBuilderRet = joinResultBuilder->AddJoinValueAddresses(isPreKeyMatched,
            streamedTblMatchedValueAddresses, bufferedTblMatchedValueAddresses);
        if (joinResultBuilderRet == 1) {
            joinResultBuilder->GetOutput(returnVectorBatchs);
        }
    }

    // 2)check need to add data for streamed table
    auto streamedRet = DecodeStreamedTblResult(joinScannerRet);
    if (streamedRet == NEED_DATA) {
        return SMJ_NEED_ADD_STREAM_TBL_DATA;
    }

    // 3)check need to add data for streamed table
    auto bufferedRet = DecodeBufferedTblResult(joinScannerRet);
    if (bufferedRet == NEED_DATA) {
        return SMJ_NEED_ADD_BUFFER_TBL_DATA;
    }

    // 4) scan finished, need to get last builder result
    if (streamedRet == SCAN_FINISHED && bufferedRet == SCAN_FINISHED) {
        joinResultBuilder->GetOutput(returnVectorBatchs);
    }

    // 5)finish the join scan
    SortMergeJoinAddInputCode returnCode = returnVectorBatchs.empty() ? SMJ_NO_RESULT : SMJ_FETCH_JOIN_DATA;
    if (returnCode == SMJ_NO_RESULT) {
        joinResultBuilder->Finish();
    }
    return returnCode;
}

int32_t SortMergeJoinOperator::AddStreamedTableInput(omniruntime::vec::VectorBatch *vecBatch)
{
    std::vector<VectorBatch *> vecBatchVector;
    vecBatchVector.push_back(vecBatch);
    streamedTblPagesIndex->AddVecBatches(vecBatchVector);
    return GetJoinResult();
}

int32_t SortMergeJoinOperator::AddBufferedTableInput(omniruntime::vec::VectorBatch *vecBatch)
{
    std::vector<VectorBatch *> vecBatchVector;
    vecBatchVector.push_back(vecBatch);
    bufferedTblPagesIndex->AddVecBatches(vecBatchVector);
    return GetJoinResult();
}

int32_t SortMergeJoinOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    return -1;
}

int32_t SortMergeJoinOperator::GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages)
{
    outputPages.assign(returnVectorBatchs.begin(), returnVectorBatchs.end());
    returnVectorBatchs.clear();
    return 0;
}

OmniStatus SortMergeJoinOperator::Close()
{
    return OMNI_STATUS_NORMAL;
}
} // end of namespace op
} // end of namespace omniruntime