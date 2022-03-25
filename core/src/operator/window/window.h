/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: window implementations
 */
#ifndef __WINDOW_H__
#define __WINDOW_H__

#include <vector>
#include "../operator.h"
#include "../operator_factory.h"
#include "../pages_index.h"
#include "../../type/data_types.h"
#include "window_partition.h"

namespace omniruntime {
namespace op {
class WindowOperatorFactory : public OperatorFactory {
public:
    WindowOperatorFactory(const type::DataTypes &sourceTypes, int32_t *outputCols, int32_t outputColsCount,
        int32_t *windowFunctionTypes, int32_t windowFunctionCount, int32_t *partitionCols, int32_t partitionCount,
        int32_t *preGroupedCols, int32_t preGroupedCount, int32_t *sortCols, int32_t *sortAscendings,
        int32_t *sortNullFirsts, int32_t sortColCount, int32_t preSortedChannelPrefix, int32_t expectedPositions,
        const type::DataTypes &allTypes, int32_t *argumentChannels, int32_t argumentChannelsCount);

    ~WindowOperatorFactory() override;

    static WindowOperatorFactory *CreateWindowOperatorFactory(const type::DataTypes &sourceTypesField, int32_t *outputColsField,
        int32_t outputColsCountField, int32_t *windowFunctionTypesField, int32_t windowFunctionCountField, int32_t *partitionColsField,
        int32_t partitionCountField, int32_t *preGroupedColsField, int32_t preGroupedCountField, int32_t *sortColsField,
        int32_t *sortAscendingsField, int32_t *sortNullFirstsField, int32_t sortColCountField, int32_t preSortedChannelPrefixField,
        int32_t expectedPositionsField, const type::DataTypes &allTypesField, int32_t *argumentChannelsField,
        int32_t argumentChannelsCountField);

    Operator *CreateOperator() override;

    DataTypes *GetSourceTypes()
    {
        return sourceTypes.get();
    }

    int32_t *GetSourceTypeIds() const
    {
        return const_cast<int32_t *>(sourceTypes->GetIds());
    }

    int32_t GetTypesCount() const
    {
        return sourceTypes->GetSize();
    }

    int32_t *GetOutputCols() const
    {
        return const_cast<int32_t *>(outputCols.data());
    }

    int32_t GetOutputColsCount() const
    {
        return outputColsCount;
    }

    int32_t *GetWindowFunctionTypes() const
    {
        return const_cast<int32_t *>(windowFunctionTypes.data());
    }

    int32_t GetWindowFunctionCount() const
    {
        return windowFunctionCount;
    }

    int32_t *GetPartitionCols() const
    {
        return const_cast<int32_t *>(partitionCols.data());
    }

    int32_t GetPartitionCount() const
    {
        return partitionCount;
    }

    int32_t *GetPreGroupedCols() const
    {
        return const_cast<int32_t *>(preGroupedCols.data());
    }

    int32_t GetPreGroupedCount() const
    {
        return preGroupedCount;
    }

    int32_t *GetSortCols() const
    {
        return const_cast<int32_t *>(sortCols.data());
    }

    int32_t *GetSortAscendings() const
    {
        return const_cast<int32_t *>(sortAscendings.data());
    }

    int32_t *GetSortNullFirsts() const
    {
        return const_cast<int32_t *>(sortNullFirsts.data());
    }

    int32_t GetSortColCount() const
    {
        return sortColCount;
    }

    int32_t GetPreSortedChannelPrefix() const
    {
        return preSortedChannelPrefix;
    }

    int32_t GetExpectedPositions() const
    {
        return expectedPositions;
    }

    int32_t *GetAllTypes() const
    {
        return const_cast<int32_t *>(allTypes->GetIds());
    }

    int32_t GetAllCount() const
    {
        return allTypes->GetSize();
    }

    int32_t *GetArgumentChannels() const
    {
        return const_cast<int32_t *>(argumentChannels.data());
    }

    int32_t GetArgumentChannelsCount() const
    {
        return argumentChannelsCount;
    }

    OmniStatus Init();

private:
    std::unique_ptr<type::DataTypes> sourceTypes;
    std::vector<int32_t> outputCols;
    int32_t outputColsCount;
    std::vector<int32_t> windowFunctionTypes;
    int32_t windowFunctionCount;
    std::vector<int32_t> partitionCols;
    int32_t partitionCount;
    std::vector<int32_t> preGroupedCols;
    int32_t preGroupedCount;
    std::vector<int32_t> sortCols;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    int32_t sortColCount;
    int32_t preSortedChannelPrefix;
    int32_t expectedPositions;
    std::unique_ptr<type::DataTypes> allTypes;
    std::vector<int32_t> argumentChannels;
    int32_t argumentChannelsCount;
};

class WindowOperator : public Operator {
public:
    WindowOperator(const type::DataTypes &sourceTypes, std::vector<int32_t> &outputCols, int32_t outputColsCount,
        std::vector<int32_t> &windowFunctionTypes, int32_t windowFunctionCount, std::vector<int32_t> &partitionCols,
        int32_t partitionCount, std::vector<int32_t> &preGroupedCols, int32_t preGroupedCount,
        std::vector<int32_t> &sortCols, std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts,
        int32_t sortColCount, int32_t preSortedChannelPrefix, int32_t expectedPositions,
        const type::DataTypes &allTypes, std::vector<int32_t> &argumentChannels, int32_t argumentChannelsCount);

    ~WindowOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;
    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

    void SortPagesIndexIfNecessary();
    void FinishPagesIndex();

    OmniStatus Init() override;

private:
    const type::DataTypes &sourceTypes;
    int32_t typesCount;
    std::vector<int32_t> outputCols;
    int32_t outputColsCount;
    std::vector<int32_t> windowFunctionTypes;
    int32_t windowFunctionCount;
    std::vector<int32_t> partitionCols;
    int32_t partitionCount;
    std::vector<int32_t> preGroupedCols;
    int32_t preGroupedCount;
    std::vector<int32_t> originSortCols;
    int32_t originSortColCount;
    std::vector<int32_t> sortCols;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    int32_t sortColCount;
    int32_t preSortedChannelPrefix;
    int32_t expectedPositions;
    const type::DataTypes &allTypes;
    std::unique_ptr<PagesIndex> pagesIndex;
    omniruntime::vec::VectorBatch *pendingInput;
    std::unique_ptr<PagesHashStrategy> preGroupedPartitionHashStrategy = nullptr;
    std::unique_ptr<PagesHashStrategy> unGroupedPartitionHashStrategy = nullptr;
    std::unique_ptr<PagesHashStrategy> preSortedPartitionHashStrategy = nullptr;
    std::unique_ptr<PagesHashStrategy> peerGroupHashStrategy = nullptr;
    std::unique_ptr<WindowPartition> partition;
    std::vector<std::unique_ptr<WindowFunction>> windowFunctions;
    std::vector<omniruntime::vec::VectorBatch *> inputVecBatches;
    std::vector<int32_t> argumentChannels;
    int32_t argumentChannelsCount;

    void Initialization();

    void ProcessData(int32_t positionCount, int finalOutputColsCount, int32_t maxRowCount,
        std::vector<type::DataType> &outputTypes, int32_t position, omniruntime::vec::VectorBatch *&vecBatch,
        int32_t &rowCount);

    void InitResultVectors(const std::vector<DataType> &outputTypesField, VectorBatch *&vecBatchField, const int32_t &rowCountField,
        const int32_t outputColsCountField, const int finalOutputColsCountField) const;
};

int32_t FindGroupEnd(PagesIndex *pagesIndex, PagesHashStrategy *pagesHashStrategy, int32_t startPosition);
}
}

const int MID_SEARCH_FACTOR = 2;
#endif