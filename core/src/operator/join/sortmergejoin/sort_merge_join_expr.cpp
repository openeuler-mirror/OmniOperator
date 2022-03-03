/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Sort merge join interface layer implementations
 */
#include "sort_merge_join_expr.h"
#include "../../util/operator_util.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

StreamedTableWithExprOperatorFactory *StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(
    const vec::VecTypes &streamedTypes, const std::vector<omniruntime::expressions::Expr *> &streamedKeyExprCols,
    int32_t streamedKeyExprColsCnt, int32_t *streamedOutputCols, int32_t streamedOutputColsCnt, JoinType joinType,
    std::string &filter)
{
    auto pOperatorFactory = std::make_unique<StreamedTableWithExprOperatorFactory>(streamedTypes, streamedKeyExprCols,
        streamedKeyExprColsCnt, streamedOutputCols, streamedOutputColsCnt, joinType, filter);
    return pOperatorFactory.release();
}

StreamedTableWithExprOperatorFactory::StreamedTableWithExprOperatorFactory(const vec::VecTypes &streamedTypes,
    const std::vector<omniruntime::expressions::Expr *> &streamedKeyExprCols, int32_t streamedKeyExprColsCnt,
    int32_t *streamedOutputCols, int32_t streamedOutputColsCnt, JoinType joinType, std::string &filter)
{
    std::vector<VecType> newBuildTypes;
    OperatorUtil::CreateProjectFuncs(streamedTypes, streamedKeyExprCols, streamedKeyExprColsCnt, newBuildTypes,
        this->rowProjections, this->streamedKeyCols, this->projectFuncs);
    this->streamedTypes = std::make_unique<vec::VecTypes>(newBuildTypes);
    this->streamedOutputCols.insert(this->streamedOutputCols.end(), streamedOutputCols,
        streamedOutputCols + streamedOutputColsCnt);
    this->joinType = joinType;
    this->filter = filter;
    this->smjOperator = std::make_unique<SortMergeJoinOperator>(joinType, filter).release();
    this->smjOperator->ConfigStreamedTblInfo(*(this->streamedTypes.get()), this->streamedKeyCols,
        this->streamedOutputCols);
}

StreamedTableWithExprOperatorFactory::~StreamedTableWithExprOperatorFactory()
{
    delete this->smjOperator;
}

Operator *StreamedTableWithExprOperatorFactory::CreateOperator()
{
    auto pOperatorItf = std::make_unique<StreamedTableWithExprOperator>(*(streamedTypes.get()), streamedKeyCols,
        projectFuncs, smjOperator);
    return pOperatorItf.release();
}

SortMergeJoinOperator *StreamedTableWithExprOperatorFactory::GetSmjOperator()
{
    return this->smjOperator;
}

StreamedTableWithExprOperator::StreamedTableWithExprOperator(const vec::VecTypes &streamedTypes,
    const std::vector<int32_t> &streamedKeyCols, const std::vector<RowProjFunc> &projectFuncs,
    SortMergeJoinOperator *smjOperator)
    : streamedTypes(streamedTypes),
      streamedKeyCols(streamedKeyCols),
      projectFuncs(projectFuncs),
      smjOperator(smjOperator)
{}

StreamedTableWithExprOperator::~StreamedTableWithExprOperator() {}

int32_t StreamedTableWithExprOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    int32_t retCode = -1;
    if (vecBatch == nullptr) {
        return retCode;
    }

    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(vecBatch, this->streamedTypes, this->projectFuncs, this->streamedKeyCols);
    if (newInputVecBatch != nullptr) {
        retCode = smjOperator->AddStreamedTableInput(newInputVecBatch);
        VectorHelper::FreeVecBatch(vecBatch);
    } else {
        retCode = smjOperator->AddStreamedTableInput(vecBatch);
    }
    return retCode;
}

int32_t StreamedTableWithExprOperator::GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages)
{
    return smjOperator->GetOutput(outputPages);
}

OmniStatus StreamedTableWithExprOperator::Close()
{
    return smjOperator->Close();
}

BufferedTableWithExprOperatorFactory *BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(
    const vec::VecTypes &bufferedTypes, const std::vector<omniruntime::expressions::Expr *> &bufferedKeyExprCols,
    int32_t bufferedKeyExprCnt, int32_t *bufferedOutputCols, int32_t bufferedOutputColsCnt,
    int64_t streamedTableFactoryAddr)
{
    auto pOperatorFactory = std::make_unique<BufferedTableWithExprOperatorFactory>(bufferedTypes, bufferedKeyExprCols,
        bufferedKeyExprCnt, bufferedOutputCols, bufferedOutputColsCnt, streamedTableFactoryAddr);
    return pOperatorFactory.release();
}

BufferedTableWithExprOperatorFactory::BufferedTableWithExprOperatorFactory(const vec::VecTypes &bufferedTypes,
    const std::vector<omniruntime::expressions::Expr *> &bufferedKeyExprCols, int32_t bufferedKeyExprCnt,
    int32_t *bufferedOutputCols, int32_t bufferedOutputColsCnt, int64_t streamedTableFactoryAddr)
{
    std::vector<VecType> newBuildTypes;
    OperatorUtil::CreateProjectFuncs(bufferedTypes, bufferedKeyExprCols, bufferedKeyExprCnt, newBuildTypes,
        this->rowProjections, this->bufferedKeyCols, this->projectFuncs);
    this->bufferedTypes = std::make_unique<vec::VecTypes>(newBuildTypes);
    this->bufferedOutputCols.insert(this->bufferedOutputCols.end(), bufferedOutputCols,
        bufferedOutputCols + bufferedOutputColsCnt);

    this->streamTblWithExprOperatorFactory =
        reinterpret_cast<StreamedTableWithExprOperatorFactory *>(streamedTableFactoryAddr);

    this->streamTblWithExprOperatorFactory->GetSmjOperator()->ConfigBufferedTblInfo(*(this->bufferedTypes.get()),
        this->bufferedKeyCols, this->bufferedOutputCols);
    this->streamTblWithExprOperatorFactory->GetSmjOperator()->InitScannerAndResultBuilder();
}

BufferedTableWithExprOperatorFactory::~BufferedTableWithExprOperatorFactory() {}

Operator *BufferedTableWithExprOperatorFactory::CreateOperator()
{
    auto smjOperator = this->streamTblWithExprOperatorFactory->GetSmjOperator();

    auto pOperatorItf = std::make_unique<BufferedTableWithExprOperator>(*(bufferedTypes.get()), bufferedKeyCols,
        projectFuncs, smjOperator);
    return pOperatorItf.release();
}

BufferedTableWithExprOperator::BufferedTableWithExprOperator(const vec::VecTypes &bufferedTypes,
    const std::vector<int32_t> &bufferedKeyCols, const std::vector<RowProjFunc> &projectFuncs,
    SortMergeJoinOperator *smjOperator)
    : bufferedTypes(bufferedTypes),
      bufferedKeyCols(bufferedKeyCols),
      projectFuncs(projectFuncs),
      smjOperator(smjOperator)
{}

BufferedTableWithExprOperator::~BufferedTableWithExprOperator() {}


int32_t BufferedTableWithExprOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    int32_t retCode = -1;
    if (vecBatch == nullptr) {
        return retCode;
    }
    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(vecBatch, this->bufferedTypes, this->projectFuncs, this->bufferedKeyCols);
    if (newInputVecBatch != nullptr) {
        retCode = smjOperator->AddBufferedTableInput(newInputVecBatch);
        VectorHelper::FreeVecBatch(vecBatch);
    } else {
        retCode = smjOperator->AddBufferedTableInput(vecBatch);
    }
    return retCode;
}

int32_t BufferedTableWithExprOperator::GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages)
{
    return smjOperator->GetOutput(outputPages);
}

OmniStatus BufferedTableWithExprOperator::Close()
{
    return smjOperator->Close();
}
}
}