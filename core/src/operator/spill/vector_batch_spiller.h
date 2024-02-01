/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
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
          comparator(comparator)
    {
        const auto sourceTypesSize = this->sourceTypes.GetSize();
        outputCols.reserve(sourceTypesSize);
        outputTypes.reserve(sourceTypesSize);
        for (int32_t i = 0; i < this->sourceTypes.GetSize(); ++i) {
            outputCols.push_back(i);
            outputTypes.push_back(this->sourceTypes.GetType(i));
        }
        const int32_t rowSize = OperatorUtil::GetRowSize(outputTypes);
        maxRowCountPerVecBatch = OperatorUtil::GetMaxRowCount(rowSize);
    }

    VectorBatchSpiller(std::string &path, omniruntime::type::DataTypes &sourceTypes, std::vector<int32_t> &outputCols,
        VecBatchWithPositionComparator *comparator)
        : path(path),
          outputCols(outputCols),
          sourceTypes(sourceTypes),
          comparator(comparator)
    {
        const auto outputColsSize = outputCols.size();
        outputTypes.reserve(outputColsSize);
        for (auto &outputCol : outputCols) {
            outputTypes.push_back(this->sourceTypes.GetType(outputCol));
        }
        const int32_t rowSize = OperatorUtil::GetRowSize(outputTypes);
        maxRowCountPerVecBatch = OperatorUtil::GetMaxRowCount(rowSize);
    }

    ~VectorBatchSpiller() override
    {
        for (auto writer : writers) {
            delete writer;
        }
        writers.clear();

        for (auto reader : readers) {
            delete reader;
        }
        readers.clear();

        delete merger;
        merger = nullptr;

        delete tracker;
        tracker = nullptr;
    }

    void SetSpillTracker(SpillTracker *spillTracker) override
    {
        this->tracker = spillTracker;
    }

    ErrorCode Spill(SpillUnitIter &vecBatches) override;

    void MergeFromDiskAndMemory(SpillUnitIter &memoryIter) override;

    bool HasNext() override;

    VectorBatchUnit *Next() override;

    uint64_t GetSpilledBytes() override
    {
        return tracker->GetSpilledBytes();
    }

private:
    int32_t maxRowCountPerVecBatch;
    int64_t totalRowCount = 0;
    int64_t remainingRowCount = 0;
    std::string path;

    std::vector<int32_t> outputCols;
    std::vector<omniruntime::type::DataTypePtr> outputTypes;
    std::vector<VectorBatchWriter *> writers;
    std::vector<VectorBatchReader *> readers;

    omniruntime::type::DataTypes sourceTypes;
    VecBatchWithPositionComparator *comparator;
    VectorBatchMerger *merger = nullptr;
    SpillTracker *tracker = nullptr;
};
}
}
#endif // OMNI_RUNTIME_VECTOR_BATCH_SPILLER_H
