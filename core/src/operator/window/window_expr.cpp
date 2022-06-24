/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window implementations
 */
#include "window_expr.h"

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
            return true;
        default:
            return false;
    }
}

WindowWithExprOperatorFactory::WindowWithExprOperatorFactory(const type::ContainerDataTypePtr &sourceTypes, int32_t *outputCols,
    int32_t outputColsCount, int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols,
    int32_t partitionCount, int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix,
    int32_t expectedPositions, const type::ContainerDataTypePtr &outputDataTypes,
    const std::vector<omniruntime::expressions::Expr *> &argumentKeys, int32_t argumentChannelsCount,
    int32_t *windowFrameTypesField, int32_t *windowFrameStartTypesField, int32_t *windowFrameStartChannelsField,
    int32_t *windowFrameEndTypesField, int32_t *windowFrameEndChannelsField)
{
    std::vector<DataTypePtr> newTypes;
    std::vector<int32_t> fullArgumentChannels;
    OperatorUtil::CreateProjectFuncs(*sourceTypes, argumentKeys, argumentChannelsCount, newTypes, this->rowProjections,
        this->argumentChannels, this->projectFuncs);
    this->sourceTypes = std::make_shared<ContainerDataType>(newTypes);

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
    allTypesVec.insert(allTypesVec.end(), sourceTypes->GetFieldTypes().begin(), sourceTypes->GetFieldTypes().end());
    allTypesVec.insert(allTypesVec.end(), std::begin(this->sourceTypes->GetFieldTypes()) + sourceTypes->GetSize(),
        std::end(this->sourceTypes->GetFieldTypes()));
    allTypesVec.insert(allTypesVec.end(), outputDataTypes->GetFieldTypes().begin(), outputDataTypes->GetFieldTypes().end());
    ContainerDataTypePtr allTypes = std::make_shared<ContainerDataType>(allTypesVec);
    auto newOutputColsCount = outputColsCount + this->projectFuncs.size();
    int newOutputCols[newOutputColsCount];
    for (int32_t i = 0; i < outputColsCount; i++) {
        newOutputCols[i] = outputCols[i];
    }
    for (uint32_t i = 0; i < this->projectFuncs.size(); i++) {
        auto index = outputColsCount + i;
        newOutputCols[index] = sourceTypes->GetSize() + static_cast<int32_t>(i);
    }
    this->operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(this->sourceTypes,
        newOutputCols, newOutputColsCount, windowFunctionTypes, windowFunctionCount, partitionCols, partitionCount,
        preGroupedCols, preGroupedCount, sortCols, sortAscendings, sortNullFirsts, sortColCount, preSortedChannelPrefix,
        expectedPositions, std::move(allTypes), fullArgumentChannels.data(), fullArgumentChannels.size(), windowFrameTypesField,
        windowFrameStartTypesField, windowFrameStartChannelsField, windowFrameEndTypesField,
        windowFrameEndChannelsField);
}

WindowWithExprOperatorFactory::~WindowWithExprOperatorFactory()
{
    delete this->operatorFactory;
}

WindowWithExprOperatorFactory *WindowWithExprOperatorFactory::CreateWindowWithExprOperatorFactory(
    const type::ContainerDataTypePtr &sourceTypes, int32_t *outputCols, int32_t outputColsCount, int32_t *windowFunctionTypes,
    int32_t windowFunctionCount, int32_t *partitionCols, int32_t partitionCount, int32_t *preGroupedCols,
    int32_t preGroupedCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount,
    int32_t preSortedChannelPrefix, int32_t expectedPositions, const type::ContainerDataTypePtr &outputDataTypes,
    const std::vector<omniruntime::expressions::Expr *> &argumentKeys, int32_t argumentChannelsCount,
    int32_t *windowFrameTypesField, int32_t *windowFrameStartTypesField, int32_t *windowFrameStartChannelsField,
    int32_t *windowFrameEndTypesField, int32_t *windowFrameEndChannelsField)
{
    auto factory = new WindowWithExprOperatorFactory(sourceTypes, outputCols, outputColsCount, windowFunctionTypes,
        windowFunctionCount, partitionCols, partitionCount, preGroupedCols, preGroupedCount, sortCols, sortAscendings,
        sortNullFirsts, sortColCount, preSortedChannelPrefix, expectedPositions, outputDataTypes, argumentKeys,
        argumentChannelsCount, windowFrameTypesField, windowFrameStartTypesField, windowFrameStartChannelsField,
        windowFrameEndTypesField, windowFrameEndChannelsField);
    return factory;
}

Operator *WindowWithExprOperatorFactory::CreateOperator()
{
    auto windowOperator = static_cast<WindowOperator *>(operatorFactory->CreateOperator());
    auto windowWithExprOperator =
        new WindowWithExprOperator(sourceTypes, argumentChannels, projectFuncs, windowOperator);
    return windowWithExprOperator;
}

WindowWithExprOperator::WindowWithExprOperator(type::ContainerDataTypePtr sourceTypes,
    std::vector<int32_t> &argumentChannels, std::vector<RowProjFunc> &projectFuncs, WindowOperator *windowOperator)
    : sourceTypes(std::move(sourceTypes)),
      argumentChannels(argumentChannels),
      projectFuncs(projectFuncs),
      windowOperator(windowOperator)
{}

WindowWithExprOperator::~WindowWithExprOperator()
{
    delete windowOperator;
}

int32_t WindowWithExprOperator::AddInput(VectorBatch *vecBatch)
{
    VectorBatch *newInputVecBatch =
        OperatorUtil::ProjectVectors(vecBatch, *sourceTypes, projectFuncs, argumentChannels, vecAllocator);
    if (newInputVecBatch != nullptr) {
        windowOperator->AddInput(newInputVecBatch);
        VectorHelper::FreeVecBatch(vecBatch);
    } else {
        windowOperator->AddInput(vecBatch);
    }
    return 0;
}

int32_t WindowWithExprOperator::GetOutput(vector<VectorBatch *> &outputPages)
{
    windowOperator->GetOutput(outputPages);
    SetStatus(OMNI_STATUS_FINISHED);
    return 0;
}

OmniStatus WindowWithExprOperator::Close()
{
    return OMNI_STATUS_NORMAL;
}
}
}