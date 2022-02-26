/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window implementations
 */
#include "../window/window_expr.h"
#include "../sort/sort.h"
#include "../status.h"
#include "../util/operator_util.h"

using namespace std;
namespace omniruntime {
namespace op {
using namespace omniruntime::vec;

WindowWithExprOperatorFactory::WindowWithExprOperatorFactory(const type::DataTypes &sourceTypes, int32_t *outputCols,
    int32_t outputColsCount, int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols,
    int32_t partitionCount, int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols,
    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix,
    int32_t expectedPositions, const type::DataTypes &outputDataTypes,
    const std::vector<omniruntime::expressions::Expr *> &argumentKeys, int32_t argumentChannelsCount)
{
    std::vector<DataType> newTypes;
    OperatorUtil::CreateProjectFuncs(sourceTypes, argumentKeys, argumentChannelsCount, newTypes, this->rowProjections,
        this->argumentChannels, this->projectFuncs);
    this->sourceTypes = std::make_unique<DataTypes>(newTypes);

    // refact alltypes since sourcetypes changed
    std::vector<DataType> allTypesVec;
    allTypesVec.insert(allTypesVec.end(), sourceTypes.Get().begin(), sourceTypes.Get().end());
    allTypesVec.insert(allTypesVec.end(), std::begin((*(this->sourceTypes.get())).Get()) + sourceTypes.GetSize(),
        std::end((*(this->sourceTypes.get())).Get()));
    allTypesVec.insert(allTypesVec.end(), outputDataTypes.Get().begin(), outputDataTypes.Get().end());
    DataTypes allTypes(allTypesVec);
    auto newOutputColsCount = outputColsCount + this->projectFuncs.size();
    int newOutputCols[newOutputColsCount];
    for (int32_t i = 0; i < outputColsCount; i++) {
        newOutputCols[i] = outputCols[i];
    }
    for (int32_t i = 0; i < this->projectFuncs.size(); i++) {
        auto index = outputColsCount + i;
        newOutputCols[index] = sourceTypes.GetSize() + i;
    }
    this->operatorFactory = WindowOperatorFactory::CreateWindowOperatorFactory(*(this->sourceTypes.get()),
        newOutputCols, newOutputColsCount, windowFunctionTypes, windowFunctionCount, partitionCols, partitionCount,
        preGroupedCols, preGroupedCount, sortCols, sortAscendings, sortNullFirsts, sortColCount, preSortedChannelPrefix,
        expectedPositions, allTypes, this->argumentChannels.data(), argumentChannelsCount);
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
    const std::vector<omniruntime::expressions::Expr *> &argumentKeys, int32_t argumentChannelsCount)
{
    auto factory = std::make_unique<WindowWithExprOperatorFactory>(sourceTypes, outputCols, outputColsCount,
        windowFunctionTypes, windowFunctionCount, partitionCols, partitionCount, preGroupedCols, preGroupedCount,
        sortCols, sortAscendings, sortNullFirsts, sortColCount, preSortedChannelPrefix, expectedPositions,
        outputDataTypes, argumentKeys, argumentChannelsCount);
    return factory.release();
}

Operator *WindowWithExprOperatorFactory::CreateOperator()
{
    auto windowOperator = static_cast<WindowOperator *>(operatorFactory->CreateOperator());
    auto windowWithExprOperator =
        std::make_unique<WindowWithExprOperator>(*(sourceTypes.get()), argumentChannels, projectFuncs, windowOperator);
    return windowWithExprOperator.release();
}

WindowWithExprOperator::WindowWithExprOperator(const type::DataTypes &sourceTypes,
    std::vector<int32_t> &argumentChannels, std::vector<RowProjFunc> &projectFuncs, WindowOperator *windowOperator)
    : sourceTypes(sourceTypes),
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
    VectorBatch *newInputVecBatch = OperatorUtil::ProjectVectors(vecBatch, sourceTypes, projectFuncs, argumentChannels);
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