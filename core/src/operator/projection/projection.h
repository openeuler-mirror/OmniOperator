#ifndef __PROJECTION_H__
#define __PROJECTION_H__

#include "../../vector/table.h"
#include "../../util/debug.h"

#include <stdint.h>

class Projection
{
public:
    Projection(int32_t *inputTypes,
               int32_t inputVecCount,
               int32_t inputVecLength,
               int32_t *projectVecs,
               int32_t projectVecCount);
    ~Projection();

    Table *project(int32_t *selectedPosition, int selectedPositionCount, Table *table);

private:
    int32_t *inputTypes;
    int32_t inputVecCount;
    int32_t inputVecLength;
    int32_t *projectVecs;
    int32_t projectVecCount;

    void allocColumns(int64_t outputTableAddr, int32_t *sourceTypes, int32_t *outputCols, int32_t outputColCount, int32_t positionCount);
};

#endif