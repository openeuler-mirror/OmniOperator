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
    bool onlyBufferedFirstMatch =
        (joinType == OMNI_JOIN_TYPE_LEFT_SEMI || joinType == OMNI_JOIN_TYPE_LEFT_ANTI) && filter.empty();

    smjScanner = new SortMergeJoinScanner(*streamedTypes, streamedKeysCols.data(), streamedKeysCols.size(),
        streamedTblPagesIndex, *bufferedTypes, bufferedKeysCols.data(), bufferedTblPagesIndex, joinType,
        onlyBufferedFirstMatch);

    joinResultBuilder = new JoinResultBuilder(*streamedTypes, streamedOutputCols.data(), streamedOutputCols.size(),
        streamedTblPagesIndex, *bufferedTypes, bufferedOutputCols.data(), bufferedOutputCols.size(),
        bufferedTblPagesIndex, filter, vecAllocator, overflowConfig, joinType);
}

int32_t HandleSortMergeJoinNoResultSituation(DynamicPagesIndex *streamedTblPagesIndex,
    DynamicPagesIndex *bufferedTblPagesIndex, JoinType joinType, int32_t resultCode)
{
    switch (joinType) {
        case JoinType::OMNI_JOIN_TYPE_INNER:
        case JoinType::OMNI_JOIN_TYPE_LEFT_SEMI: {
            if (streamedTblPagesIndex->IsEmptyBatch() || bufferedTblPagesIndex->IsEmptyBatch()) {
                return SetAddFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH), resultCode);
            }
            break;
        }
        case JoinType::OMNI_JOIN_TYPE_LEFT:
        case JoinType::OMNI_JOIN_TYPE_LEFT_ANTI: {
            if (streamedTblPagesIndex->IsEmptyBatch()) {
                return SetAddFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH), resultCode);
            }
            break;
        }
        case JoinType::OMNI_JOIN_TYPE_FULL: {
            if (streamedTblPagesIndex->IsEmptyBatch() && bufferedTblPagesIndex->IsEmptyBatch()) {
                return SetAddFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH), resultCode);
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
    // the resultCode consists of two parts, the first 16bits indicate whether to add data,
    // and the last 16 bits indicate whether to fetch data.
    // NeedDataFlag has 3 values: 2, 3, 4
    // 2 -> add streamTable data
    // 3 -> add buffedTable data
    // 4 -> streamTable and buffedTable scan is finished
    // FetchDataFlag has 2 values: 0, 5
    // 0 -> init status code, it means no result to fetch
    // 5 -> operator produced the result data, we should fetch data
    int32_t resultCode = 0;
    if (streamedTypes == nullptr) {
        resultCode =
            SetAddFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_NEED_STREAM_TBL_INFO), resultCode);
        return resultCode;
    }

    if (bufferedTypes == nullptr) {
        resultCode =
            SetAddFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_NEED_BUFFER_TBL_INFO), resultCode);
        return resultCode;
    }

    // check streamed table have input, if not return data need to add input
    if (streamedTblPagesIndex->GetPositionCount() == 0 && !streamedTblPagesIndex->IsDataFinish()) {
        resultCode =
            SetAddFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA), resultCode);
        return resultCode;
    }

    // check buffered table have input, if not return data need to add input
    if (bufferedTblPagesIndex->GetPositionCount() == 0 && !bufferedTblPagesIndex->IsDataFinish()) {
        resultCode =
            SetAddFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA), resultCode);
        return resultCode;
    }

    // check  SMJ_NO_RESULT situation
    int32_t handResult =
        HandleSortMergeJoinNoResultSituation(streamedTblPagesIndex, bufferedTblPagesIndex, joinType, resultCode);
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
        std::vector<bool> &isSameBufferedKeyMatched = smjScanner->GetMatchedBufferedKeyFlag();
        smjScanner->GetMatchedValueAddresses(isPreKeyMatched, streamedTblMatchedValueAddresses,
            bufferedTblMatchedValueAddresses);
        auto joinResultBuilderRet = joinResultBuilder->AddJoinValueAddresses(isPreKeyMatched,
            streamedTblMatchedValueAddresses, bufferedTblMatchedValueAddresses, isSameBufferedKeyMatched);
        // clear smjScanner current matched result for next match
        smjScanner->Clear();
        if (joinResultBuilderRet == 1) {
            joinResultBuilder->GetOutput(returnVectorBatchs);
            resultCode =
                SetFetchFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA), resultCode);
        }
    }

    // 2)check need to add data for streamed table
    auto streamedRet = DecodeStreamedTblResult(joinScannerRet);
    if (static_cast<JoinTableCode>(streamedRet) == JoinTableCode::NEED_DATA) {
        resultCode =
            SetAddFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_STREAM_TBL_DATA), resultCode);
        return resultCode;
    }

    // 3)check need to add data for buffered table
    auto bufferedRet = DecodeBufferedTblResult(joinScannerRet);
    if (static_cast<JoinTableCode>(bufferedRet) == JoinTableCode::NEED_DATA) {
        resultCode =
            SetAddFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_NEED_ADD_BUFFER_TBL_DATA), resultCode);
        return resultCode;
    }

    // 4) scan finished, need to get last builder result
    if (static_cast<JoinTableCode>(streamedRet) == JoinTableCode::SCAN_FINISHED &&
        static_cast<JoinTableCode>(bufferedRet) == JoinTableCode::SCAN_FINISHED) {
        joinResultBuilder->GetOutput(returnVectorBatchs);
    }

    // 5)finish the join scan
    resultCode = SetAddFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_SCAN_FINISH), resultCode);
    if (returnVectorBatchs.empty()) {
        joinResultBuilder->Finish();
    } else {
        resultCode = SetFetchFlag(static_cast<int16_t>(SortMergeJoinAddInputCode::SMJ_FETCH_JOIN_DATA), resultCode);
    }
    return resultCode;
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