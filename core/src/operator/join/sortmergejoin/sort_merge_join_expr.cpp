/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Sort merge join interface layer implementations
 */
#include "sort_merge_join_expr.h"
#include "operator/util/operator_util.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

StreamedTableWithExprOperatorFactory *StreamedTableWithExprOperatorFactory::CreateStreamedTableWithExprOperatorFactory(
    type::ContainerDataTypePtr streamedTypes, const std::vector<omniruntime::expressions::Expr *> &streamedKeyExprCols,
    int32_t streamedKeyExprColsCnt, int32_t *streamedOutputCols, int32_t streamedOutputColsCnt, JoinType inputJoinType,
    std::string &filterExpression)
{
    return new StreamedTableWithExprOperatorFactory(streamedTypes, streamedKeyExprCols, streamedKeyExprColsCnt,
        streamedOutputCols, streamedOutputColsCnt, inputJoinType, filterExpression);
}

StreamedTableWithExprOperatorFactory::StreamedTableWithExprOperatorFactory(type::ContainerDataTypePtr streamedTypes,
    const std::vector<omniruntime::expressions::Expr *> &streamedKeyExprCols, int32_t streamedKeyExprColsCnt,
    int32_t *streamedOutputCols, int32_t streamedOutputColsCnt, JoinType joinType, std::string &filter)
    : joinType(joinType), filter(filter), smjOperator(new SortMergeJoinOperator(joinType, filter))
{
    std::vector<DataTypePtr> newBuildTypes;
    OperatorUtil::CreateProjectFuncs(*streamedTypes, streamedKeyExprCols, streamedKeyExprColsCnt, newBuildTypes,
        rowProjections, streamedKeyCols, projectFuncs);
    this->streamedTypes = std::make_shared<ContainerDataType>(newBuildTypes);
    this->streamedOutputCols.insert(this->streamedOutputCols.end(), streamedOutputCols,
        streamedOutputCols + streamedOutputColsCnt);
    smjOperator->ConfigStreamedTblInfo(streamedTypes, streamedKeyCols, this->streamedOutputCols);
}

StreamedTableWithExprOperatorFactory::~StreamedTableWithExprOperatorFactory()
{
    delete smjOperator;
}

Operator *StreamedTableWithExprOperatorFactory::CreateOperator()
{
    return new StreamedTableWithExprOperator(streamedTypes, streamedKeyCols, projectFuncs, smjOperator);
}

SortMergeJoinOperator *StreamedTableWithExprOperatorFactory::GetSmjOperator()
{
    return smjOperator;
}

StreamedTableWithExprOperator::StreamedTableWithExprOperator(type::ContainerDataTypePtr streamedTypes,
    const std::vector<int32_t> &streamedKeyCols, const std::vector<RowProjFunc> &projectFuncs,
    SortMergeJoinOperator *smjOperator)
    : smjOperator(smjOperator),
      streamedTypes(std::move(streamedTypes)),
      streamedKeyCols(streamedKeyCols),
      projectFuncs(projectFuncs)
{}

StreamedTableWithExprOperator::~StreamedTableWithExprOperator() {}

int32_t StreamedTableWithExprOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    int32_t retCode = -1;
    if (vecBatch == nullptr) {
        return retCode;
    }

    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(vecBatch, *streamedTypes, projectFuncs, streamedKeyCols, vecAllocator);
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
    type::ContainerDataTypePtr &bufferedTypes, const std::vector<omniruntime::expressions::Expr *> &bufferedKeyExprCols,
    int32_t bufferedKeyExprCnt, int32_t *bufferedOutputCols, int32_t bufferedOutputColsCnt,
    int64_t streamedTableFactoryAddr)
{
    return new BufferedTableWithExprOperatorFactory(bufferedTypes, bufferedKeyExprCols, bufferedKeyExprCnt,
        bufferedOutputCols, bufferedOutputColsCnt, streamedTableFactoryAddr);
}

BufferedTableWithExprOperatorFactory::BufferedTableWithExprOperatorFactory(type::ContainerDataTypePtr &bufferedTypes,
    const std::vector<omniruntime::expressions::Expr *> &bufferedKeyExprCols, int32_t bufferedKeyExprCnt,
    int32_t *bufferedOutputCols, int32_t bufferedOutputColsCnt, int64_t streamedTableFactoryAddr)
    : streamTblWithExprOperatorFactory(
    reinterpret_cast<StreamedTableWithExprOperatorFactory *>(streamedTableFactoryAddr))
{
    std::vector<DataTypePtr> newBuildTypes;
    OperatorUtil::CreateProjectFuncs(*bufferedTypes, bufferedKeyExprCols, bufferedKeyExprCnt, newBuildTypes,
        rowProjections, bufferedKeyCols, projectFuncs);
    this->bufferedTypes = std::make_shared<ContainerDataType>(newBuildTypes);
    this->bufferedOutputCols.insert(this->bufferedOutputCols.end(), bufferedOutputCols,
        bufferedOutputCols + bufferedOutputColsCnt);

    streamTblWithExprOperatorFactory->GetSmjOperator()->ConfigBufferedTblInfo(this->bufferedTypes,
        bufferedKeyCols, this->bufferedOutputCols);
    streamTblWithExprOperatorFactory->GetSmjOperator()->InitScannerAndResultBuilder();
}

BufferedTableWithExprOperatorFactory::~BufferedTableWithExprOperatorFactory() {}

Operator *BufferedTableWithExprOperatorFactory::CreateOperator()
{
    auto smjOperator = streamTblWithExprOperatorFactory->GetSmjOperator();
    return new BufferedTableWithExprOperator(bufferedTypes, bufferedKeyCols, projectFuncs, smjOperator);
}

BufferedTableWithExprOperator::BufferedTableWithExprOperator(type::ContainerDataTypePtr bufferedTypes,
    const std::vector<int32_t> &bufferedKeyCols, const std::vector<RowProjFunc> &projectFuncs,
    SortMergeJoinOperator *smjOperator)
    : smjOperator(smjOperator),
      bufferedTypes(bufferedTypes),
      bufferedKeyCols(bufferedKeyCols),
      projectFuncs(projectFuncs)
{}

BufferedTableWithExprOperator::~BufferedTableWithExprOperator() {}


int32_t BufferedTableWithExprOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    int32_t retCode = -1;
    if (vecBatch == nullptr) {
        return retCode;
    }
    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(vecBatch, *bufferedTypes, projectFuncs, bufferedKeyCols, vecAllocator);
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