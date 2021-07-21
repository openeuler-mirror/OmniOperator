#ifndef __PAGES_INDEX_H__
#define __PAGES_INDEX_H__

#include "../vector/vector_batch.h"

#include <stdint.h>
#include <vector>

class PagesIndex
{
public:
    PagesIndex(int32_t *types, int32_t typesCount);
    ~PagesIndex();
    int32_t addVecBatches(std::vector<VectorBatch *> &vecBatches);
    void sort(
        int32_t *sortCols,
        int32_t *sortColTypes,
        int32_t *sortAscendings,
        int32_t *sortNullFirsts,
        int32_t sortColCount,
        int32_t from,
        int32_t to);
    void getOutput(int32_t *outputCols, int32_t outputColsCount, VectorBatch *outputVecBatch, int32_t *sourceTypes, int32_t offset, int32_t length);

    int32_t *getTypes()
    {
        return types;
    }
    int32_t getTypesCount() {
        return typesCount;
    }
    int64_t *getValueAddresses()
    {
        return this->valueAddresses;
    }
    int32_t getPositionCount()
    {
        return this->positionCount;
    }

    Vector ***getColumns()
    {
        return this->columns;
    }

private:
    int32_t *types;
    int32_t typesCount;
    Vector ***columns; // Vector* [columnIndex][tableIndex]
    int64_t *valueAddresses;
    int32_t positionCount;
};

int64_t encodeSyntheticAddress(int32_t sliceIndex, int32_t sliceOffset);
int32_t decodeSliceIndex(int64_t sliceAddress);
int32_t decodePosition(int64_t sliceAddress);

#endif
