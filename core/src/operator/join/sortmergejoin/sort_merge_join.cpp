/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort merge join core implementations
 */

#include "sort_merge_join.h"
#include <memory>
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

SortMergeJoinOperator::SortMergeJoinOperator(JoinType joinType, std::string &filter)
    : joinType(joinType), filter(filter)
{}

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

void SortMergeJoinOperator::ConfigStreamedTblInfo(const type::DataTypes &streamedDataTypes,
    const std::vector<int32_t> &streamedKeysCols, const std::vector<int32_t> &streamedOutputCols)
{
    this->streamedTypes = new DataTypes(streamedDataTypes);
    this->streamedKeysCols = streamedKeysCols;
    this->streamedOutputCols = streamedOutputCols;
}

void SortMergeJoinOperator::ConfigBufferedTblInfo(const type::DataTypes &bufferedDataTypes,
    std::vector<int32_t> &bufferedKeysCols, std::vector<int32_t> &bufferedOutputCols)
{
    this->bufferedTypes = new DataTypes(bufferedDataTypes);
    this->bufferedKeysCols = bufferedKeysCols;
    this->bufferedOutputCols = bufferedOutputCols;
}

void SortMergeJoinOperator::InitScannerAndResultBuilder(OverflowConfig *overflowConfig)
{
    streamedTblPagesIndex = new DynamicPagesIndex(*streamedTypes);
    bufferedTblPagesIndex = new DynamicPagesIndex(*bufferedTypes);

    smjScanner = new SortMergeJoinScanner(*streamedTypes, streamedKeysCols.data(), streamedKeysCols.size(),
        streamedTblPagesIndex, *bufferedTypes, bufferedKeysCols.data(), bufferedTblPagesIndex, joinType, false);

    joinResultBuilder = new JoinResultBuilder(*streamedTypes, streamedOutputCols.data(), streamedOutputCols.size(),
        streamedTblPagesIndex, *bufferedTypes, bufferedOutputCols.data(), bufferedOutputCols.size(),
        bufferedTblPagesIndex, filter, vecAllocator, overflowConfig, joinType);
}

int32_t HandleSortMergeJoinNoResultSituation(DynamicPagesIndex *streamedTblPagesIndex,
    DynamicPagesIndex *bufferedTblPagesIndex, JoinType joinType)
{
    switch (joinType) {
        case omniruntime::op::JoinType::OMNI_JOIN_TYPE_INNER:
        case JoinType::OMNI_JOIN_TYPE_LEFT_SEMI: {
            if (streamedTblPagesIndex->IsEmptyBatch() || bufferedTblPagesIndex->IsEmptyBatch()) {
                return static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NO_RESULT);
            }
            break;
        }
        case omniruntime::op::JoinType::OMNI_JOIN_TYPE_LEFT: {
            if (streamedTblPagesIndex->IsEmptyBatch()) {
                return static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NO_RESULT);
            }
            break;
        }
        case omniruntime::op::JoinType::OMNI_JOIN_TYPE_FULL: {
            if (streamedTblPagesIndex->IsEmptyBatch() && bufferedTblPagesIndex->IsEmptyBatch()) {
                return static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NO_RESULT);
            }
            break;
        }
        default: {
            LogError("Unsupported join type: %u.", joinType);
        }
    }
    return -1;
}

int32_t SortMergeJoinOperator::GetJoinResult()
{
    if (streamedTypes == nullptr) {
        return static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_STREAM_TBL_INFO);
    }

    if (bufferedTypes == nullptr) {
        return static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_BUFFER_TBL_INFO);
    }

    // check streamed table have input, if not return data need to add input
    if (streamedTblPagesIndex->GetPositionCount() == 0 && !streamedTblPagesIndex->IsDataFinish()) {
        return static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA);
    }

    // check buffered table have input, if not return data need to add input
    if (bufferedTblPagesIndex->GetPositionCount() == 0 && !bufferedTblPagesIndex->IsDataFinish()) {
        return static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA);
    }

    // check  SMJ_NO_RESULT situation
    int32_t handResult = HandleSortMergeJoinNoResultSituation(streamedTblPagesIndex, bufferedTblPagesIndex, joinType);
    if (handResult != -1) {
        return handResult;
    }

    auto joinScannerRet = smjScanner->FindNextJoinRows();

    // 1)put matched rows to result builder, and cache the result
    auto matchResultRet = DecodeJoinResult(joinScannerRet);
    if (static_cast<JoinResultCode>(matchResultRet) == JoinResultCode::HAS_RESULT) {
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
    if (static_cast<JoinTableCode>(streamedRet) == JoinTableCode::NEED_DATA) {
        return static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA);
    }

    // 3)check need to add data for streamed table
    auto bufferedRet = DecodeBufferedTblResult(joinScannerRet);
    if (static_cast<JoinTableCode>(bufferedRet) == JoinTableCode::NEED_DATA) {
        return static_cast<int32_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA);
    }

    // 4) scan finished, need to get last builder result
    if (static_cast<JoinTableCode>(streamedRet) == JoinTableCode::SCAN_FINISHED &&
        static_cast<JoinTableCode>(bufferedRet) == JoinTableCode::SCAN_FINISHED) {
        joinResultBuilder->GetOutput(returnVectorBatchs);
    }

    // 5)finish the join scan
    SortMergeJoinAddInputCode returnCode = returnVectorBatchs.empty() ? SortMergeJoinAddInputCode::SMJ_NO_RESULT :
                                                                        SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA;
    if (returnCode == SortMergeJoinAddInputCode::SMJ_NO_RESULT) {
        joinResultBuilder->Finish();
    }
    return static_cast<int32_t>(returnCode);
}

int32_t SortMergeJoinOperator::AddStreamedTableInput(omniruntime::vec::VectorBatch *vecBatch)
{
    if (streamedTblPagesIndex->IsDataFinish()) {
        VectorHelper::FreeVecBatch(vecBatch);
    } else {
        std::vector<VectorBatch *> vecBatchVector;
        vecBatchVector.push_back(vecBatch);
        streamedTblPagesIndex->AddVecBatches(vecBatchVector);
    }
    return GetJoinResult();
}

int32_t SortMergeJoinOperator::AddBufferedTableInput(omniruntime::vec::VectorBatch *vecBatch)
{
    if (bufferedTblPagesIndex->IsDataFinish()) {
        VectorHelper::FreeVecBatch(vecBatch);
    } else {
        std::vector<VectorBatch *> vecBatchVector;
        vecBatchVector.push_back(vecBatch);
        bufferedTblPagesIndex->AddVecBatches(vecBatchVector);
    }
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