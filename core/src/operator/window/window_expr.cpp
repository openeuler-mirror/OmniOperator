/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2024. All rights reserved.
 * @Description: window implementations
 */
#include "window_expr.h"
#include "operator/util/function_type.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

static bool HasArgument(int32_t functionType)
{
    switch (functionType) {
        case OMNI_AGGREGATION_TYPE_SUM:
        case OMNI_AGGREGATION_TYPE_COUNT_COLUMN:
        case OMNI_AGGREGATION_TYPE_AVG:
        case OMNI_AGGREGATION_TYPE_MAX:
        case OMNI_AGGREGATION_TYPE_MIN:
        case OMNI_AGGREGATION_TYPE_FIRST_INCLUDENULL:
        case OMNI_AGGREGATION_TYPE_FIRST_IGNORENULL:
            return true;
        default:
            return false;
    }
}

WindowWithExprOperatorFactory::WindowWithExprOperatorFactory(const type::DataTypes &sourceTypes, int32_t *outputCols,
    int32_t outputColsCount, int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols,
    int32_t partitionCount, int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix,
    int32_t expectedPositions, const type::DataTypes &outputDataTypes,
    const std::vector<omniruntime::expressions::Expr *> &argumentKeys, int32_t argumentChannelsCount,
    int32_t *windowFrameTypesField, int32_t *windowFrameStartTypesField, int32_t *windowFrameStartChannelsField,
    int32_t *windowFrameEndTypesField, int32_t *windowFrameEndChannelsField, const OperatorConfig &operatorConfig)
{
    std::vector<DataTypePtr> newTypes;
    std::vector<int32_t> fullArgumentChannels;
    int32_t nonFieldProjectCount = OperatorUtil::CreateProjections(sourceTypes, argumentKeys, newTypes,
        this->projections, this->argumentChannels, operatorConfig.GetOverflowConfig());
    this->sourceTypes = std::make_unique<DataTypes>(newTypes);

    int position = 0;
    for (int i = 0; i < windowFunctionCount; ++i) {
        if (HasArgument(windowFunctionTypes[i])) {
            fullArgumentChannels.push_back(this->argumentChannels[position++]);
        } else {
            fullArgumentChannels.push_back(-1);
        }
    }

    // refact alltypes since sourcetypes changed
    std::vector<DataTypePtr> allTypesVec;
    allTypesVec.insert(allTypesVec.end(), sourceTypes.Get().begin(), sourceTypes.Get().end());
    allTypesVec.insert(allTypesVec.end(), std::begin(this->sourceTypes->Get()) + sourceTypes.GetSize(),
        std::end(this->sourceTypes->Get()));
    allTypesVec.insert(allTypesVec.end(), outputDataTypes.Get().begin(), outputDataTypes.Get().end());
    DataTypes allTypes(allTypesVec);
    auto newOutputColsCount = outputColsCount + nonFieldProjectCount;
    int newOutputCols[newOutputColsCount];
    for (int32_t i = 0; i < outputColsCount; i++) {
        newOutputCols[i] = outputCols[i];
    }
    for (int32_t i = 0; i < nonFieldProjectCount; i++) {
        auto index = outputColsCount + i;
        newOutputCols[index] = sourceTypes.GetSize() + static_cast<int32_t>(i);
    }
    this->operatorFactory =
        WindowOperatorFactory::CreateWindowOperatorFactory(*(this->sourceTypes), newOutputCols, newOutputColsCount,
        windowFunctionTypes, windowFunctionCount, partitionCols, partitionCount, preGroupedCols, preGroupedCount,
        sortCols, sortAscendings, sortNullFirsts, sortColCount, preSortedChannelPrefix, expectedPositions, allTypes,
        fullArgumentChannels.data(), fullArgumentChannels.size(), windowFrameTypesField, windowFrameStartTypesField,
        windowFrameStartChannelsField, windowFrameEndTypesField, windowFrameEndChannelsField, operatorConfig);
}

WindowWithExprOperatorFactory::~WindowWithExprOperatorFactory()
{
    delete this->operatorFactory;
}

WindowWithExprOperatorFactory *WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
    const type::DataTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount, int32_t *windowFunctionTypes,
    int32_t windowFunctionCount, int32_t *partitionCols, int32_t partitionCount, int32_t *preGroupedCols,
    int32_t preGroupedCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount,
    int32_t preSortedChannelPrefix, int32_t expectedPositions, const type::DataTypes &outputDataTypes,
    const std::vector<omniruntime::expressions::Expr *> &argumentKeys, int32_t argumentChannelsCount,
    int32_t *windowFrameTypesField, int32_t *windowFrameStartTypesField, int32_t *windowFrameStartChannelsField,
    int32_t *windowFrameEndTypesField, int32_t *windowFrameEndChannelsField)
{
    auto factory = new WindowWithExprOperatorFactory(sourceTypes, outputCols, outputColsCount, windowFunctionTypes,
        windowFunctionCount, partitionCols, partitionCount, preGroupedCols, preGroupedCount, sortCols, sortAscendings,
        sortNullFirsts, sortColCount, preSortedChannelPrefix, expectedPositions, outputDataTypes, argumentKeys,
        argumentChannelsCount, windowFrameTypesField, windowFrameStartTypesField, windowFrameStartChannelsField,
        windowFrameEndTypesField, windowFrameEndChannelsField, OperatorConfig());
    return factory;
}

WindowWithExprOperatorFactory *WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
    const type::DataTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount, int32_t *windowFunctionTypes,
    int32_t windowFunctionCount, int32_t *partitionCols, int32_t partitionCount, int32_t *preGroupedCols,
    int32_t preGroupedCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount,
    int32_t preSortedChannelPrefix, int32_t expectedPositions, const type::DataTypes &outputDataTypes,
    const std::vector<omniruntime::expressions::Expr *> &argumentKeys, int32_t argumentChannelsCount,
    int32_t *windowFrameTypesField, int32_t *windowFrameStartTypesField, int32_t *windowFrameStartChannelsField,
    int32_t *windowFrameEndTypesField, int32_t *windowFrameEndChannelsField, const OperatorConfig &operatorConfig)
{
    auto factory = new WindowWithExprOperatorFactory(sourceTypes, outputCols, outputColsCount, windowFunctionTypes,
        windowFunctionCount, partitionCols, partitionCount, preGroupedCols, preGroupedCount, sortCols, sortAscendings,
        sortNullFirsts, sortColCount, preSortedChannelPrefix, expectedPositions, outputDataTypes, argumentKeys,
        argumentChannelsCount, windowFrameTypesField, windowFrameStartTypesField, windowFrameStartChannelsField,
        windowFrameEndTypesField, windowFrameEndChannelsField, operatorConfig);
    return factory;
}

Operator *WindowWithExprOperatorFactory::CreateOperator()
{
    auto windowOperator = static_cast<WindowOperator *>(operatorFactory->CreateOperator());
    auto windowWithExprOperator = new WindowWithExprOperator(*sourceTypes, projections, windowOperator);
    return windowWithExprOperator;
}

WindowWithExprOperator::WindowWithExprOperator(const type::DataTypes &sourceTypes,
    std::vector<std::unique_ptr<Projection>> &projections, WindowOperator *windowOperator)
    : sourceTypes(std::move(sourceTypes)),
      projections(projections),
      windowOperator(windowOperator),
      executionContext(new ExecutionContext())
{}

WindowWithExprOperator::~WindowWithExprOperator()
{
    delete windowOperator;
    delete executionContext;
}

int32_t WindowWithExprOperator::AddInput(VectorBatch *vecBatch)
{
    VectorBatch *newInputVecBatch = OperatorUtil::ProjectVectors(vecBatch, sourceTypes, projections, executionContext);
    VectorHelper::FreeVecBatch(vecBatch);
    ResetInputVecBatch();
    windowOperator->AddInput(newInputVecBatch);
    return 0;
}

int32_t WindowWithExprOperator::GetOutput(VectorBatch **outputVecBatch)
{
    int32_t status = windowOperator->GetOutput(outputVecBatch);
    SetStatus(windowOperator->GetStatus());
    return status;
}

OmniStatus WindowWithExprOperator::Close()
{
    windowOperator->Close();
    return OMNI_STATUS_NORMAL;
}

uint64_t WindowWithExprOperator::GetSpilledBytes()
{
    return windowOperator->GetSpilledBytes();
}
}
}