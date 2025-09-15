/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 * Description: spill merger implementation
 */

#ifndef OMNI_RUNTIME_SPILL_MERGER_H
#define OMNI_RUNTIME_SPILL_MERGER_H

#include "type/data_types.h"
#include "vector/vector_batch.h"
#include "loser_tree.h"
#include "operator/util/operator_util.h"
#include "spill_tracker.h"

namespace omniruntime {
namespace op {
static constexpr int32_t ERROR_BUFFER_SIZE = 128;

static void GetErrorMsg(int32_t errorCode, char *buffer, int32_t bufferLen)
{
    if (strerror_r(errorCode, buffer, bufferLen) == nullptr) {
        throw exception::OmniException("SPILL FAILED", "strerror failed.");
    }
}

class SortOrder {
public:
    SortOrder() = default;

    SortOrder(bool sortAscending, bool sortNullsFirst) : sortAscending(sortAscending), sortNullsFirst(sortNullsFirst) {}

    ~SortOrder() = default;

    bool IsAscending() const
    {
        return sortAscending;
    }

    bool IsNullsFirst() const
    {
        return sortNullsFirst;
    }

private:
    bool sortAscending = true;
    bool sortNullsFirst = true;
};

struct SpillFileInfo {
    std::string filePath;
    uint64_t fileLength;
    uint64_t totalRowCount;
};

class SpillReader {
public:
    SpillReader(const type::DataTypes &dataTypes, const std::string &filePath, uint64_t fileLength,
        uint64_t totalRowCount)
        : dataTypes(dataTypes), filePath(filePath), fileLength(fileLength), totalRowCount(totalRowCount)
    {}

    ~SpillReader();

    ErrorCode ReadVecBatch(std::unique_ptr<vec::VectorBatch> &vectorBatch, bool &isEnd);

    uint64_t GetFileLength() const
    {
        return fileLength;
    }

    std::string GetFilePath() const
    {
        return filePath;
    }

private:
    template <typename T> ErrorCode ReadVector(vec::BaseVector *vector, int32_t rowCount);

    ErrorCode Read(void *buf, size_t bufSize);

    DataTypes dataTypes;
    std::string filePath;
    uint64_t fileLength = 0;
    FILE *file = nullptr;
    uint64_t totalRowCount = 0;
    uint64_t rowOffset = 0;
    int32_t maxRowCount = 0; // for reuse vector batch memory
};

class SpillMergeStream {
public:
    static SpillMergeStream *Create(const type::DataTypes &dataTypes, const SpillFileInfo &fileInfo,
        const std::vector<int32_t> &sortCols, std::vector<OperatorUtil::CompareFunc> &sortCompareFuncs,
        SpillTracker *spillTracker)
    {
        auto *stream = new SpillMergeStream(dataTypes, fileInfo, sortCols, sortCompareFuncs, spillTracker);
        auto result = stream->GetNextBatch();
        if (result != ErrorCode::SUCCESS) {
            delete stream;
            return nullptr;
        }
        return stream;
    }

    ~SpillMergeStream()
    {
        delete reader;
    }

    int32_t CompareTo(const SpillMergeStream &other);

    bool operator < (const SpillMergeStream &other)
    {
        return CompareTo(other) < 0;
    }

    bool HasData()
    {
        return currentRowIdx < currentRowCount;
    }

    ErrorCode GetNextBatch()
    {
        bool isEnd = false;
        auto result = reader->ReadVecBatch(currentBatch, isEnd);
        currentBatchPtr = currentBatch.get();
        if (isEnd) {
            currentRowIdx = 0;
            currentRowCount = 0;
            return result;
        }

        currentRowIdx = 0;
        currentRowCount = currentBatchPtr->GetRowCount();
        return result;
    }

    VectorBatch *GetCurrentBatch()
    {
        return currentBatchPtr;
    }

    int32_t GetCurrentRowIdx()
    {
        return currentRowIdx;
    }

    int32_t GetCurrentRowIdx(bool &isLastRow)
    {
        isLastRow = (currentRowIdx == currentRowCount - 1);
        return currentRowIdx;
    }

    void Pop()
    {
        if (++currentRowIdx >= currentRowCount) {
            auto result = GetNextBatch();
            if (result != ErrorCode::SUCCESS) {
                std::string errMsg = "Read from spill file " + reader->GetFilePath() + " failed.";
                throw omniruntime::exception::OmniException("SPILL_READ_FAILED", errMsg);
            }
        }
    }

private:
    SpillMergeStream(const type::DataTypes &dataTypes, const SpillFileInfo &fileInfo,
        const std::vector<int32_t> &sortCols, std::vector<OperatorUtil::CompareFunc> &sortCompareFuncs,
        SpillTracker *spillTracker)
        : sortCols(sortCols), sortCompareFuncs(sortCompareFuncs), spillTracker(spillTracker)
    {
        reader = new SpillReader(dataTypes, fileInfo.filePath, fileInfo.fileLength, fileInfo.totalRowCount);
    }

    std::vector<int32_t> sortCols; // which columns in currentBatch will be used to sort ?
    std::vector<OperatorUtil::CompareFunc> sortCompareFuncs;
    SpillTracker *spillTracker = nullptr;
    SpillReader *reader = nullptr;
    std::unique_ptr<vec::VectorBatch> currentBatch = nullptr;
    vec::VectorBatch *currentBatchPtr = nullptr;
    int32_t currentRowIdx = 0;
    int32_t currentRowCount = 0;
};

class SpillMerger {
public:
    static SpillMerger *Create(const type::DataTypes &dataTypes, const std::vector<int32_t> &sortCols,
        const std::vector<SortOrder> &sortOrders, SpillTracker *spillTracker,
        const std::vector<SpillFileInfo> &spillFiles)
    {
        std::vector<OperatorUtil::CompareFunc> sortCompareFuncs;
        SetCompareFunctions(dataTypes, sortCols, sortOrders, sortCompareFuncs);

        std::vector<SpillMergeStream *> streams;
        uint64_t totalRowCount = 0;
        for (auto &fileInfo : spillFiles) {
            auto stream = SpillMergeStream::Create(dataTypes, fileInfo, sortCols, sortCompareFuncs, spillTracker);
            if (stream == nullptr) {
                for (auto createdStream : streams) {
                    delete createdStream;
                }
                return nullptr;
            }
            streams.emplace_back(stream);
            totalRowCount += fileInfo.totalRowCount;
        }
        return new SpillMerger(streams, totalRowCount, spillTracker);
    }

    ~SpillMerger()
    {
        delete mergeStreams;
        delete spillTracker;
    }

    vec::VectorBatch *CurrentBatch()
    {
        currentStream = mergeStreams->Next();
        return currentStream->GetCurrentBatch();
    }

    vec::VectorBatch *CurrentBatchWithEqual(bool &isEqual)
    {
        auto stream = mergeStreams->NextWithEqual(isEqual);
        if (stream == nullptr) {
            return nullptr;
        }
        currentStream = stream;
        return currentStream->GetCurrentBatch();
    }

    int32_t CurrentRowIndex()
    {
        return currentStream->GetCurrentRowIdx();
    }

    int32_t CurrentRowIndex(bool &isLastRow)
    {
        return currentStream->GetCurrentRowIdx(isLastRow);
    }

    void Pop()
    {
        currentStream->Pop();
    }

    uint64_t GetTotalRowCount()
    {
        return totalRowCount;
    }

private:
    static void SetCompareFunctions(const type::DataTypes &dataTypes, const std::vector<int32_t> &sortCols,
        const std::vector<SortOrder> &sortOrders, std::vector<OperatorUtil::CompareFunc> &sortCompareFuncs);

    template <typename T>
    static void SetCompareFunction(bool isAscending, bool isNullsFirst,
        std::vector<OperatorUtil::CompareFunc> &sortCompareFuncs);

    SpillMerger(const std::vector<SpillMergeStream *> &streams, uint64_t totalRowCount, SpillTracker *spillTracker)
        : mergeStreams(new LoserTree<SpillMergeStream>(streams)),
          totalRowCount(totalRowCount),
          spillTracker(spillTracker)
    {}

    LoserTree<SpillMergeStream> *mergeStreams;
    uint64_t totalRowCount = 0;
    SpillTracker *spillTracker = nullptr;
    SpillMergeStream *currentStream = nullptr;
};
}
}

#endif // OMNI_RUNTIME_SPILL_MERGER_H
