/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * @Description: sort implementations
 */
#ifndef __SORT_H__
#define __SORT_H__

#include <vector>
#include <memory>
#include "util/error_code.h"
#include "operator/operator_factory.h"
#include "operator/config/operator_config.h"
#include "operator/pages_index.h"
#include "operator/spill/spiller.h"
#include "operator/spill/vector_batch_merger.h"
#include "type/data_types.h"
#include "type/data_type.h"

namespace omniruntime {
namespace op {
class SortOperatorFactory : public OperatorFactory {
public:
    SortOperatorFactory(const type::DataTypes &dataTypes, int32_t *outputCols, int32_t outputColCount,
        int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount,
        const OperatorConfig &operatorConfig);

    ~SortOperatorFactory() override;

    static SortOperatorFactory *CreateSortOperatorFactory(const type::DataTypes &dataTypes, int32_t *outputCols,
        int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts,
        int32_t sortColCount);

    static SortOperatorFactory *CreateSortOperatorFactory(const type::DataTypes &dataTypes, int32_t *outputCols,
        int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts,
        int32_t sortColCount, const OperatorConfig &operatorConfig);

    Operator *CreateOperator() override;

    int32_t GetSourceTypeCount()
    {
        return sourceTypes.GetSize();
    }

    int32_t *GetOutputCols()
    {
        return outputCols.data();
    }

    int32_t GetOutputColCount()
    {
        return outputCols.size();
    }

    int32_t *GetSortCols()
    {
        return sortCols.data();
    }

    int32_t *GetSortAscendings()
    {
        return sortAscendings.data();
    }

    int32_t *GetSortNullFirsts()
    {
        return sortNullFirsts.data();
    }

    int32_t GetSortColCount()
    {
        return sortCols.size();
    }

private:
    DataTypes sourceTypes;
    std::vector<int32_t> outputCols;
    std::vector<int32_t> sortCols;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    OperatorConfig operatorConfig;
};

class SortOperator : public Operator {
public:
    SortOperator(const type::DataTypes &dataTypes, std::vector<int32_t> &outputCols, std::vector<int32_t> &sortCols,
        std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts,
        const OperatorConfig &operatorConfig);

    ~SortOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(omniruntime::vec::VectorBatch **outputVecBatch) override;

    OmniStatus Close() override;

    int32_t GetTypescount()
    {
        return sourceTypes.GetSize();
    }

    int32_t *GetOutputCols()
    {
        return outputCols.data();
    }

    int32_t GetOutputColsCount()
    {
        return outputCols.size();
    }

    int32_t *GetSortCols()
    {
        return sortCols.data();
    }

    int32_t *GetSortAscendings()
    {
        return sortAscendings.data();
    }

    int32_t *GetSortNullFirsts()
    {
        return sortNullFirsts.data();
    }

    int32_t GetSortColCount()
    {
        return sortCols.size();
    }

    PagesIndex *GetPagesIndex() const
    {
        return pagesIndex.get();
    }

private:
    void PrepareOutput();
    ErrorCode SpillToDisk();
    void Sort();
    void GetVecBatchesForSpill(std::vector<omniruntime::vec::VectorBatch *> &vecBatchesForSpill);
    void GetOutputFromMemory(VectorBatch **outputVecBatch);
    void MergeFromDiskAndMemory(VectorBatch **outputVecBatch);

    type::DataTypes sourceTypes;
    std::vector<int32_t> outputCols;
    std::vector<int32_t> sortCols;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    std::unique_ptr<PagesIndex> pagesIndex;
    int32_t maxRowCountPerBatch = 0;
    size_t totalRowCount = 0;
    size_t rowCountOutputted = 0;
    bool hasSorted = false;

    // for spill
    OperatorConfig operatorConfig;
    VecBatchWithPositionComparator *comparator = nullptr;
    Spiller *spiller = nullptr;
    bool hasNext = true;
    bool canInplaceSort = false;
    bool useRadixSort = false;
    int32_t radixSortSizeThreshold = -1;
};
} // end of namespace op
} // end of namespace omniruntime
#endif