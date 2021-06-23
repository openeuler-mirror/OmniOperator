#include "projection.h"
#include "../../memory/memory_pool.h"
#include "../../vector/vector_common.h"

Projection::Projection(int32_t *inputTypes,
                       int32_t inputVecCount,
                       int32_t inputVecLength,
                       int32_t *projectVecs,
                       int32_t projectVecCount) {
    this->inputTypes = inputTypes;
    this->inputVecCount = inputVecCount;
    this->inputVecLength = inputVecLength;
    this->projectVecs = projectVecs;
    this->projectVecCount = projectVecCount;
}

Projection::~Projection() {}

VectorBatch *Projection::project(int32_t *selectedPosition, int selectedPositionCount, VectorBatch *vecBatch) {
    if (selectedPositionCount == vecBatch->getRowCount()) {
        // no need to copy the values
        return vecBatch;
    }

    VectorBatch *projectedData = new VectorBatch(this->projectVecCount);

    for (int vecIndex = 0; vecIndex < this->projectVecCount; vecIndex++) {
        int32_t projectVecIndex = this->projectVecs[vecIndex];
        auto vector = vecBatch->getVector(projectVecIndex);
        switch (vector->getType()) {
            case OMNI_VEC_TYPE_INT: {
                IntVector *projectedVector = new IntVector(vector->getAllocator(), selectedPositionCount);
                IntVector *originalVector = (IntVector *) vector;
                for (int rowIndex = 0; rowIndex < selectedPositionCount; rowIndex++) {
                    projectedVector->setValue(rowIndex, originalVector->getValue(selectedPosition[rowIndex]));
                }
                projectedData->setVector(vecIndex, projectedVector);
                break;
            }
            case OMNI_VEC_TYPE_LONG: {
                LongVector *projectedVector = new LongVector(vector->getAllocator(), selectedPositionCount);
                LongVector *originalVector = (LongVector *) vector;
                for (int rowIndex = 0; rowIndex < selectedPositionCount; rowIndex++) {
                    projectedVector->setValue(rowIndex, originalVector->getValue(selectedPosition[rowIndex]));
                }
                projectedData->setVector(vecIndex, projectedVector);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                DoubleVector *projectedVector = new DoubleVector(vector->getAllocator(), selectedPositionCount);
                DoubleVector *originalVector = (DoubleVector *) vector;
                for (int rowIndex = 0; rowIndex < selectedPositionCount; rowIndex++) {
                    projectedVector->setValue(rowIndex, originalVector->getValue(selectedPosition[rowIndex]));
                }
                projectedData->setVector(vecIndex, projectedVector);
                break;
            }
            default:
                DebugError("Unsupported vector type %d", vector->getType());
                break;
        }
    }

    return projectedData;
}