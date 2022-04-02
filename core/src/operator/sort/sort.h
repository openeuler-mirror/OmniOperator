/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * @Description: sort implementations
 */
#ifndef __SORT_H__
#define __SORT_H__

#include <vector>
#include <memory>
#include "operator/operator_factory.h"
#include "operator/pages_index.h"
#include "type/data_types.h"
#include "type/data_type.h"

namespace omniruntime {
namespace op {
class SortOperatorFactory : public OperatorFactory {
public:
    SortOperatorFactory(const type::DataTypes &dataTypes, int32_t *outputCols, int32_t outputColCount,
        int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount);

    ~SortOperatorFactory() override;

    static SortOperatorFactory *CreateSortOperatorFactory(const type::DataTypes &dataTypes, int32_t *outputCols,
        int32_t outputColCount, int32_t *sortCols, int32_t *sortAscendings, int32_t *sortNullFirsts,
        int32_t sortColCount);

    Operator *CreateOperator() override;

    int32_t GetSourceTypeCount()
    {
        return sourceTypes->GetSize();
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
    std::unique_ptr<type::DataTypes> sourceTypes;
    std::vector<int32_t> outputCols;
    std::vector<int32_t> sortCols;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
};

class SortOperator : public Operator {
public:
    SortOperator(const type::DataTypes &dataTypes, std::vector<int32_t> &outputCols, std::vector<int32_t> &sortCols,
        std::vector<int32_t> &sortAscendings, std::vector<int32_t> &sortNullFirsts);

    ~SortOperator() override;

    int32_t AddInput(omniruntime::vec::VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<omniruntime::vec::VectorBatch *> &outputPages) override;

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
    const type::DataTypes &sourceTypes;
    std::vector<int32_t> outputCols;
    std::vector<int32_t> sortCols;
    std::vector<int32_t> sortAscendings;
    std::vector<int32_t> sortNullFirsts;
    std::unique_ptr<PagesIndex> pagesIndex;
    std::vector<omniruntime::vec::VectorBatch *> inputVecBatches;
};
} // end of namespace op
} // end of namespace omniruntime
#endif