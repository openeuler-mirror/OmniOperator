/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
 * @Description: spiller implementation
 */

#ifndef OMNI_RUNTIME_SPILLER_H
#define OMNI_RUNTIME_SPILLER_H

#include "type/data_types.h"
#include "spill_merger.h"
#include "operator/pages_index.h"
#include "operator/aggregation/group_aggregation_sort.h"

namespace omniruntime {
namespace op {
class SpillWriter {
public:
    SpillWriter(const type::DataTypes &dataTypes, const std::string &dirPath, uint64_t writeBufferSize = 0)
        : dataTypes(dataTypes), dirPath(dirPath), writeBufferSize(writeBufferSize), writeBufferOffset(0)
    {
        if (writeBufferSize != 0) {
            writeBuffer = new char[writeBufferSize];
        }
    }

    ~SpillWriter()
    {
        delete[] writeBuffer;
    }

    ErrorCode WriteVecBatch(vec::VectorBatch *vectorBatch, uint64_t vectorBatchSize);

    SpillFileInfo GetSpillFileInfo()
    {
        SpillFileInfo file{ filePath, fileLength, totalRowCount };
        return file;
    }

    ErrorCode Close();

private:
    ErrorCode CreateTempFile();

    ErrorCode WriteVecBatchToBuffer(vec::VectorBatch *vectorBatch);

    template <typename T>
    ErrorCode WriteVectorToBuffer(vec::BaseVector *vector, int32_t rowCount, int32_t &writeOffset);

    ErrorCode WriteVecBatchToFile(vec::VectorBatch *vectorBatch);

    template <typename T> ErrorCode WriteVector(omniruntime::vec::BaseVector *vector, int32_t rowCount);

    ErrorCode Write(void *buf, size_t length);

    type::DataTypes dataTypes;
    std::string dirPath;
    int32_t fd = -1;
    std::string filePath;
    uint64_t writeBufferSize = 0;
    uint64_t writeBufferOffset = 0;
    char *writeBuffer = nullptr;
    uint64_t fileLength = 0;
    uint64_t totalRowCount = 0;
};

class Spiller {
public:
    Spiller(const type::DataTypes &dataTypes, const std::vector<int32_t> &sortCols,
        const std::vector<SortOrder> &sortOrders, const std::string &spillPath, uint64_t maxSpillBytes,
        uint64_t writeBufferSize = 0)
        : dataTypes(dataTypes), sortCols(sortCols), sortOrders(sortOrders), writeBufferSize(writeBufferSize)
    {
        dirPaths.emplace_back(spillPath);
        int32_t dataTypeCount = dataTypes.GetSize();
        for (int32_t i = 0; i < dataTypeCount; i++) {
            outputCols.emplace_back(i);
        }

        maxRowCountPerBatch = OperatorUtil::GetMaxRowCount(dataTypes.Get(), outputCols.data(), dataTypeCount);
        spillTracker = GetRootSpillTracker().CreateSpillTracker(maxSpillBytes);
    }

    ~Spiller()
    {
        for (auto writer : writers) {
            delete writer;
        }
        writers.clear();
    }

    ErrorCode Spill(PagesIndex *pagesIndex, bool canInplaceSort, bool canRadixSort);

    ErrorCode Spill(AggregationSort *aggregationSort);

    std::vector<SpillFileInfo> FinishSpill()
    {
        std::vector<SpillFileInfo> spillFiles;
        for (auto writer : writers) {
            spillFiles.emplace_back(writer->GetSpillFileInfo());
        }
        return spillFiles;
    }

    SpillMerger *CreateSpillMerger(const std::vector<SpillFileInfo> &spillFiles)
    {
        auto merger = SpillMerger::Create(dataTypes, sortCols, sortOrders, spillTracker, spillFiles);
        return merger;
    }

    uint64_t GetSpilledBytes()
    {
        return spillTracker->GetSpilledBytes();
    }

    SpillTracker *GetSpillTracker() const
    {
        return spillTracker;
    }

    // the spill files should be deleted if there is exception
    void RemoveSpillFiles()
    {
        for (auto writer : writers) {
            auto spillFile = writer->GetSpillFileInfo();
            remove(spillFile.filePath.c_str());
        }
    }

private:
    uint64_t CollectVecBatchSize(vec::VectorBatch *vectorBatch);

    template <typename T> uint64_t CollectVectorSize(vec::BaseVector *vector);

    DataTypes dataTypes;
    std::vector<int32_t> sortCols;
    std::vector<SortOrder> sortOrders;
    std::vector<std::string> dirPaths;
    std::vector<int32_t> outputCols;
    int32_t maxRowCountPerBatch;
    std::unique_ptr<vec::VectorBatch> spillVecBatch = nullptr;
    std::vector<SpillWriter *> writers;
    SpillTracker *spillTracker = nullptr;
    uint64_t writeBufferSize = 0;
};
}
}
#endif // OMNI_RUNTIME_SPILLER_H
