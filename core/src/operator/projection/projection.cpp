#include "projection.h"
#include "../../memory/memory_pool.h"

Projection::Projection(int32_t *inputTypes,
                       int32_t inputVecCount,
                       int32_t inputVecLength,
                       int32_t *projectVecs,
                       int32_t projectVecCount)
{
    this->inputTypes = inputTypes;
    this->inputVecCount = inputVecCount;
    this->inputVecLength = inputVecLength;
    this->projectVecs = projectVecs;
    this->projectVecCount = projectVecCount;
}

Projection::~Projection() {}

Table *Projection::project(int32_t *selectedPosition, int selectedPositionCount, Table *table)
{
    if (selectedPositionCount == table->getPositionCount())
    {
        // no need to copy the values
        return table;
    }

    Table *projectedData = new Table(selectedPositionCount, this->projectVecCount);

    for (int vecIndex = 0; vecIndex < this->projectVecCount; vecIndex++)
    {
        int32_t projectVecIndex = this->projectVecs[vecIndex];
        auto column = table->getColumn(projectVecIndex);
        switch (column->getType())
        {
            case INT32:
            {
                int32_t* projectedVec = (int32_t*) omni_allocate(selectedPositionCount * sizeof(int32_t));
                int32_t *originalVec = (int32_t *)column->getData();
                for (int rowIndex = 0; rowIndex < selectedPositionCount; rowIndex++)
                {
                    projectedVec[rowIndex] = originalVec[selectedPosition[rowIndex]];
                }
                Column *projectedColumn = new Column(projectedVec, column->getType(), selectedPositionCount);
                projectedData->setColumn(projectedColumn, column->getType());
                break;
            }
            case INT64:
            {
                int64_t* projectedVec = (int64_t*) omni_allocate(selectedPositionCount * sizeof(int64_t));
                int64_t *originalVec = (int64_t *)column->getData();
                for (int rowIndex = 0; rowIndex < selectedPositionCount; rowIndex++)
                {
                    projectedVec[rowIndex] = originalVec[selectedPosition[rowIndex]];
                }
                Column *projectedColumn = new Column(projectedVec, column->getType(), selectedPositionCount);
                projectedData->setColumn(projectedColumn, column->getType());
                break;
            }
            case DOUBLE:
            {
                double* projectedVec = (double*) omni_allocate(selectedPositionCount * sizeof(double));
                double *originalVec = (double *)column->getData();
                for (int rowIndex = 0; rowIndex < selectedPositionCount; rowIndex++)
                {
                    projectedVec[rowIndex] = originalVec[selectedPosition[rowIndex]];
                }
                Column *projectedColumn = new Column(projectedVec, column->getType(), selectedPositionCount);
                projectedData->setColumn(projectedColumn, column->getType());
                break;
            }
            default:
                DebugError("Unsupported column type %d", column->getType());
                break;
        }
    }

    return projectedData;
}