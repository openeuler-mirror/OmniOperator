#ifndef __PROJECTION_H__
#define __PROJECTION_H__

#include "../../vector/vector_batch.h"
#include "../../util/debug.h"
#include <stdint.h>

class Projection {
public:
    Projection(int32_t *inputTypes,
               int32_t inputVecCount,
               int32_t inputVecLength,
               int32_t *projectVecs,
               int32_t projectVecCount);

    ~Projection();

    VectorBatch *project(int32_t *selectedPosition, int selectedPositionCount, VectorBatch *vecBatch);

private:
    int32_t *inputTypes;
    int32_t inputVecCount;
    int32_t inputVecLength;
    int32_t *projectVecs;
    int32_t projectVecCount;
};

#endif