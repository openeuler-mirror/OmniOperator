/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */
#ifndef __SORT_H__
#define __SORT_H__

#include "../operator_factory.h"
#include "../pages_index.h"

#include <vector>
#include <memory>

namespace omniruntime {
namespace op {
class SortOperatorFactory : public OperatorFactory {
public:
    SortOperatorFactory(int32_t *sourceTypes, int32_t sourceTypeCount, int32_t *outputCols, int32_t outputColCount,
        int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount);

    ~SortOperatorFactory() override;

    static SortOperatorFactory *CreateSortOperatorFactory(int32_t *sourceTypes, int32_t sourceTypeCount,
        int32_t *outputCols, int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings,
        int32_t *sortNullFirsts, int32_t sortColCount);

    Operator *CreateOperator() override;

    int32_t *GetSourceTypes()
    {
        return sourceTypes.data();
    }

    int32_t GetSourceTypeCount()
    {
        return sourceTypes.size();
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
    std::vector<int32_t> sourceTypes;
    std::vector<int32_t> outputCols;
    std::vector<int32_t> sortCols;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
};

class SortOperator : public Operator {
public:
    SortOperator(std::vector<int32_t> &sourceTypes, std::vector<int32_t> &outputCols, std::vector<int32_t> &sortCols,
        std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts);

    ~SortOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

    int32_t GetTypescount()
    {
        return sourceTypes.size();
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
    std::vector<int32_t> sourceTypes;
    std::vector<int32_t> outputCols;
    std::vector<int32_t> sortCols;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    std::unique_ptr<PagesIndex> pagesIndex;
    std::vector<omniruntime::vec::VectorBatch *> inputVecBatches;
};

int32_t GetMaxRowCount(const int32_t *sourceTypes, const int32_t *outputCols, int32_t outputColsCount);
int32_t GetPageCount(int32_t positionCount, int32_t maxRowCount);
} // end of namespace op
} // end of namespace omniruntime
#endif