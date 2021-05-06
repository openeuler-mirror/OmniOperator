#include "projection.h"

Projection::Projection(int32_t *inputTypes,
                       int32_t inputVecCount,
                       int32_t inputVecLength,
                       int32_t *projectVecs,
                       int32_t projectVecCount,
                       int64_t *projectedVecAddrs)
{
    this->inputTypes = inputTypes;
    this->inputVecCount = inputVecCount;
    this->inputVecLength = inputVecLength;
    this->projectVecs = projectVecs;
    this->projectVecCount = projectVecCount;
    this->projectedVecAddrs = projectedVecAddrs;
}

Projection::~Projection() {}

void Projection::project(int32_t *selectedPosition, int selectedPositionCount, Table *table)
{
    if (selectedPositionCount == table->getPositionCount())
    {
        // no need to copy the values
        return;
    }

    for (int vecIndex = 0; vecIndex < this->projectVecCount; vecIndex++)
    {
        int32_t projectVecIndex = this->projectVecs[vecIndex];
        auto column = table->getColumn(projectVecIndex);
        switch (column->getType())
        {
            case INT32:
            {
                int32_t *originalVec = (int32_t *)column->getData();
                int32_t *projectedVec = (int32_t *)projectedVecAddrs[vecIndex];
                for (int rowIndex = 0; rowIndex < selectedPositionCount; rowIndex++)
                {
                    projectedVec[rowIndex] = originalVec[rowIndex];
                }
                break;
            }
            case INT64:
            {
                int64_t *originalVec = (int64_t *)column->getData();
                int64_t *projectedVec = (int64_t *)projectedVecAddrs[vecIndex];
                for (int rowIndex = 0; rowIndex < selectedPositionCount; rowIndex++)
                {
                    projectedVec[rowIndex] = originalVec[rowIndex];
                }
                break;
            }
            case DOUBLE:
            {
                double *originalVec = (double *)column->getData();
                double *projectedVec = (double *)projectedVecAddrs[vecIndex];
                for (int rowIndex = 0; rowIndex < selectedPositionCount; rowIndex++)
                {
                    projectedVec[rowIndex] = originalVec[rowIndex];
                }
                break;
            }
            default:
                DebugError("Unsupported column type %d", column->getType());
                break;
        }
    }
}