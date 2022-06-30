/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: vector batch spiller
 */

#ifndef OMNI_RUNTIME_VECTOR_BATCH_SPILLER_H
#define OMNI_RUNTIME_VECTOR_BATCH_SPILLER_H

#include "spiller.h"
#include "vector_batch_writer.h"
#include "vector_batch_reader.h"
#include "vector_batch_merger.h"

namespace omniruntime {
namespace op {
class VectorBatchSpiller : public Spiller {
public:
    VectorBatchSpiller(const std::string &path, const omniruntime::type::DataTypes &sourceTypes,
        VecBatchWithPositionComparator *comparator)
        : path(path),
          sourceTypes(sourceTypes),
          comparator(comparator),
          merger(nullptr),
          tracker(nullptr),
          vectorAllocator(VectorAllocator::GetGlobalAllocator()),
          totalRowCount(0),
          remainingRowCount(0)
    {
        for (int32_t i = 0; i < this->sourceTypes.GetSize(); i++) {
            outputCols.push_back(i);
            outputTypes.push_back(this->sourceTypes.GetType(i));
        }
        int32_t rowSize = OperatorUtil::GetRowSize(outputTypes);
        maxRowCountPerVecBatch = OperatorUtil::GetMaxRowCount(rowSize);
    }

    VectorBatchSpiller(std::string &path, omniruntime::type::DataTypes &sourceTypes, std::vector<int32_t> &outputCols,
        VecBatchWithPositionComparator *comparator, VectorAllocator *vectorAllocator)
        : path(path),
          sourceTypes(sourceTypes),
          comparator(comparator),
          merger(nullptr),
          tracker(nullptr),
          vectorAllocator(vectorAllocator),
          totalRowCount(0),
          remainingRowCount(0),
          outputCols(outputCols)
    {
        for (auto &outputCol : outputCols) {
            outputTypes.push_back(this->sourceTypes.GetType(outputCol));
        }
        int32_t rowSize = OperatorUtil::GetRowSize(outputTypes);
        maxRowCountPerVecBatch = OperatorUtil::GetMaxRowCount(rowSize);
    }

    ~VectorBatchSpiller() override
    {
        for (auto writer : writers) {
            delete writer;
        }
        for (auto reader : readers) {
            delete reader;
        }
        if (merger != nullptr) {
            delete merger;
        }

        if (tracker != nullptr) {
            delete tracker;
        }
    }

    void SetSpillTracker(SpillTracker *spillTracker) override
    {
        this->tracker = spillTracker;
    }

    ErrorCode Spill(SpillUnitIter &vecBatches) override;

    void MergeFromDiskAndMemory(SpillUnitIter &memoryIter) override;

    bool HasNext() override;

    VectorBatchUnit *Next() override;

private:
    std::string path;
    omniruntime::type::DataTypes sourceTypes;
    VecBatchWithPositionComparator *comparator;
    VectorBatchMerger *merger;
    SpillTracker *tracker;
    VectorAllocator *vectorAllocator;
    int64_t totalRowCount;
    int64_t remainingRowCount;
    std::vector<int32_t> outputCols;
    std::vector<omniruntime::type::DataTypePtr> outputTypes;
    int32_t maxRowCountPerVecBatch;
    std::vector<VectorBatchWriter *> writers;
    std::vector<VectorBatchReader *> readers;
};
}
}
#endif // OMNI_RUNTIME_VECTOR_BATCH_SPILLER_H
