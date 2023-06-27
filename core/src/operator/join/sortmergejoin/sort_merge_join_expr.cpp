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
    const type::DataTypes &streamedTypes, const std::vector<omniruntime::expressions::Expr *> &streamedKeyExprCols,
    int32_t streamedKeyExprColsCnt, int32_t *streamedOutputCols, int32_t streamedOutputColsCnt, JoinType inputJoinType,
    std::string &filterExpression, OverflowConfig *overflowConfig)
{
    return new StreamedTableWithExprOperatorFactory(streamedTypes, streamedKeyExprCols, streamedKeyExprColsCnt,
        streamedOutputCols, streamedOutputColsCnt, inputJoinType, filterExpression, overflowConfig);
}

StreamedTableWithExprOperatorFactory::StreamedTableWithExprOperatorFactory(const type::DataTypes &streamedTypes,
    const std::vector<omniruntime::expressions::Expr *> &streamedKeyExprCols, int32_t streamedKeyExprColsCnt,
    int32_t *streamedOutputCols, int32_t streamedOutputColsCnt, JoinType joinType, std::string &filter,
    OverflowConfig *overflowConfig)
    : joinType(joinType), filter(filter), smjOperator(new SortMergeJoinOperator(joinType, filter))
{
    std::vector<DataTypePtr> newBuildTypes;
    OperatorUtil::CreateProjectFuncs(streamedTypes, streamedKeyExprCols, streamedKeyExprColsCnt, newBuildTypes,
        Projections, streamedKeyCols, projectFuncs, overflowConfig);
    this->streamedTypes = std::make_unique<DataTypes>(newBuildTypes);
    this->streamedOutputCols.insert(this->streamedOutputCols.end(), streamedOutputCols,
        streamedOutputCols + streamedOutputColsCnt);
    smjOperator->ConfigStreamedTblInfo(*(this->streamedTypes), streamedKeyCols, this->streamedOutputCols,
        streamedTypes.GetSize());
}

StreamedTableWithExprOperatorFactory::~StreamedTableWithExprOperatorFactory()
{
    delete smjOperator;
}

Operator *StreamedTableWithExprOperatorFactory::CreateOperator()
{
    return new StreamedTableWithExprOperator(*streamedTypes, streamedKeyCols, projectFuncs, smjOperator);
}

SortMergeJoinOperator *StreamedTableWithExprOperatorFactory::GetSmjOperator()
{
    return smjOperator;
}

StreamedTableWithExprOperator::StreamedTableWithExprOperator(const type::DataTypes &streamedTypes,
    const std::vector<int32_t> &streamedKeyCols, const std::vector<ProjFunc> &projectFuncs,
    SortMergeJoinOperator *smjOperator)
    : smjOperator(smjOperator),
      streamedTypes(streamedTypes),
      streamedKeyCols(streamedKeyCols),
      projectFuncs(projectFuncs)
{}

StreamedTableWithExprOperator::~StreamedTableWithExprOperator() = default;

int32_t StreamedTableWithExprOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    int32_t retCode = -1;
    if (vecBatch == nullptr) {
        return retCode;
    }

    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(vecBatch, streamedTypes, projectFuncs, streamedKeyCols);
    if (newInputVecBatch != nullptr) {
        retCode = smjOperator->AddStreamedTableInput(newInputVecBatch);
        VectorHelper::FreeVecBatch(vecBatch);
    } else {
        retCode = smjOperator->AddStreamedTableInput(vecBatch);
    }
    SetStatus(smjOperator->GetStatus());
    return retCode;
}

int32_t StreamedTableWithExprOperator::GetOutput(omniruntime::vec::VectorBatch **outputVecBatch)
{
    int32_t retCode = smjOperator->GetOutput(outputVecBatch);
    SetStatus(smjOperator->GetStatus());
    return retCode;
}

OmniStatus StreamedTableWithExprOperator::Close()
{
    return smjOperator->Close();
}

BufferedTableWithExprOperatorFactory *BufferedTableWithExprOperatorFactory::CreateBufferedTableWithExprOperatorFactory(
    const DataTypes &bufferedTypes, const std::vector<omniruntime::expressions::Expr *> &bufferedKeyExprCols,
    int32_t bufferedKeyExprCnt, int32_t *bufferedOutputCols, int32_t bufferedOutputColsCnt,
    int64_t streamedTableFactoryAddr, OverflowConfig *overflowConfig)
{
    return new BufferedTableWithExprOperatorFactory(bufferedTypes, bufferedKeyExprCols, bufferedKeyExprCnt,
        bufferedOutputCols, bufferedOutputColsCnt, streamedTableFactoryAddr, overflowConfig);
}

BufferedTableWithExprOperatorFactory::BufferedTableWithExprOperatorFactory(const type::DataTypes &bufferedTypes,
    const std::vector<omniruntime::expressions::Expr *> &bufferedKeyExprCols, int32_t bufferedKeyExprCnt,
    int32_t *bufferedOutputCols, int32_t bufferedOutputColsCnt, int64_t streamedTableFactoryAddr,
    OverflowConfig *overflowConfig)
    : streamTblWithExprOperatorFactory(
    reinterpret_cast<StreamedTableWithExprOperatorFactory *>(streamedTableFactoryAddr))
{
    std::vector<DataTypePtr> newBuildTypes;
    OperatorUtil::CreateProjectFuncs(bufferedTypes, bufferedKeyExprCols, bufferedKeyExprCnt, newBuildTypes, Projections,
        bufferedKeyCols, projectFuncs, overflowConfig);
    this->bufferedTypes = std::make_unique<DataTypes>(newBuildTypes);
    this->bufferedOutputCols.insert(this->bufferedOutputCols.end(), bufferedOutputCols,
        bufferedOutputCols + bufferedOutputColsCnt);

    streamTblWithExprOperatorFactory->GetSmjOperator()->ConfigBufferedTblInfo(*(this->bufferedTypes), bufferedKeyCols,
        this->bufferedOutputCols, bufferedTypes.GetSize());
    streamTblWithExprOperatorFactory->GetSmjOperator()->InitScannerAndResultBuilder(overflowConfig);
}

BufferedTableWithExprOperatorFactory::~BufferedTableWithExprOperatorFactory() = default;

Operator *BufferedTableWithExprOperatorFactory::CreateOperator()
{
    auto *smjOperator = streamTblWithExprOperatorFactory->GetSmjOperator();
    return new BufferedTableWithExprOperator(*bufferedTypes, bufferedKeyCols, projectFuncs, smjOperator);
}

BufferedTableWithExprOperator::BufferedTableWithExprOperator(const type::DataTypes &bufferedTypes,
    const std::vector<int32_t> &bufferedKeyCols, const std::vector<ProjFunc> &projectFuncs,
    SortMergeJoinOperator *smjOperator)
    : smjOperator(smjOperator),
      bufferedTypes(bufferedTypes),
      bufferedKeyCols(bufferedKeyCols),
      projectFuncs(projectFuncs)
{}

BufferedTableWithExprOperator::~BufferedTableWithExprOperator() = default;

int32_t BufferedTableWithExprOperator::AddInput(omniruntime::vec::VectorBatch *vecBatch)
{
    int32_t retCode = -1;
    if (vecBatch == nullptr) {
        return retCode;
    }
    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(vecBatch, bufferedTypes, projectFuncs, bufferedKeyCols);
    if (newInputVecBatch != nullptr) {
        retCode = smjOperator->AddBufferedTableInput(newInputVecBatch);
        VectorHelper::FreeVecBatch(vecBatch);
    } else {
        retCode = smjOperator->AddBufferedTableInput(vecBatch);
    }
    SetStatus(smjOperator->GetStatus());
    return retCode;
}

int32_t BufferedTableWithExprOperator::GetOutput(omniruntime::vec::VectorBatch **outputVecBatch)
{
    int32_t retCode = smjOperator->GetOutput(outputVecBatch);
    SetStatus(smjOperator->GetStatus());
    return retCode;
}

OmniStatus BufferedTableWithExprOperator::Close()
{
    return smjOperator->Close();
}
}
}