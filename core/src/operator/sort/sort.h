#ifndef __SORT_H__
#define __SORT_H__

#include "../operator_factory.h"
#include "../pages_index.h"

#include <vector>

namespace omniruntime {
namespace op {

class SortOperatorFactory : public OperatorFactory {
public:
    SortOperatorFactory(
            int32_t *sourceTypes,
            int32_t sourceTypeCount,
            int32_t *outputCols,
            int32_t outputColCount,
            int32_t *sortCols,
            int32_t *sortAscendings,
            int32_t *sortNullFirsts,
            int32_t sortColCount);

    ~SortOperatorFactory();

    static SortOperatorFactory *createSortOperatorFactory(
            int32_t *sourceTypes,
            int32_t sourceTypeCount,
            int32_t *outputCols,
            int32_t outputColCount,
            int32_t *sortCols,
            int32_t *sortAscendings,
            int32_t *sortNullFirsts,
            int32_t sortColCount);

    Operator *createOperator();

    int32_t *getSourceTypes() { return sourceTypes; }

    int32_t getSourceTypeCount() { return sourceTypeCount; }

    int32_t *getOutputCols() { return outputCols; }

    int32_t getOutputColCount() { return outputColCount; }

    int32_t *getSortCols() { return sortCols; }

    int32_t *getSortAscendings() { return sortAscendings; }

    int32_t *getSortNullFirsts() { return sortNullFirsts; }

    int32_t getSortColCount() { return sortColCount; }

private:
    int32_t *sourceTypes;
    int32_t sourceTypeCount;
    int32_t *outputCols;
    int32_t outputColCount;
    int32_t *sortCols;
    int32_t *sortAscendings;
    int32_t *sortNullFirsts;
    int32_t sortColCount;
};

class SortOperator : public Operator {
public:
    SortOperator(
            int32_t *sourceTypes,
            int32_t typesCount,
            int32_t *outputCols,
            int32_t outputColsCount,
            int32_t *sortCols,
            int32_t *sortAscendings,
            int32_t *sortNullFirsts,
            int32_t sortColCount);

    ~SortOperator();

    int32_t AddInput(VectorBatch *vecBatch) override;

    int32_t GetOutput(std::vector<VectorBatch *> &outputPages) override;

    int32_t getTypescount() { return typesCount; }

    int32_t *getOutputCols() { return outputCols; }

    int32_t getOutputColsCount() { return outputColsCount; }

    int32_t *getSortCols() { return sortCols; }

    int32_t *getSortAscendings() { return sortAscendings; }

    int32_t *getSortNullFirsts() { return sortNullFirsts; }

    int32_t getSortColCount() { return sortColCount; }

    PagesIndex *getPagesIndex() { return pagesIndex; }

private:
    int32_t typesCount;
    int32_t *outputCols;
    int32_t outputColsCount;
    int32_t *sortCols;
    int32_t *sortAscendings;
    int32_t *sortNullFirsts;
    int32_t sortColCount;
    PagesIndex *pagesIndex;
    vector<VectorBatch *> inputVecBatches;
};

int32_t getMaxRowCount(int32_t *sourceTypes, int32_t *outputCols, int32_t outputColsCount);
int32_t getPageCount(int32_t positionCount, int32_t maxRowCount);

} // end of namespace op
} // end of namespace omniruntime
#endif