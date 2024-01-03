/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2024. All rights reserved.
 * Description: spiller implementation
 */

#include <cstdlib>
#include <unistd.h>
#include <cstring>
#include <sys/stat.h>
#include "vector/unsafe_vector.h"
#include "spiller.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using VarcharVector = Vector<LargeStringContainer<std::string_view>>;

ErrorCode Spiller::Spill(PagesIndex *pagesIndex, bool canInplaceSort, bool canRadixSort)
{
    int64_t totalRowCount = pagesIndex->GetRowCount();
    if (totalRowCount <= 0) {
        return ErrorCode::SUCCESS;
    }

    // create spill writer object
    auto writer = new SpillWriter(dataTypes, dirPaths[0]);
    writers.emplace_back(writer);

    int64_t totalRowOffset = 0;
    int32_t vecBatchCount = OperatorUtil::GetVecBatchCount(totalRowCount, maxRowCountPerBatch);
    int32_t maxRowCount = 0; // for reuse vector batch memory
    for (int32_t vecBatchIdx = 0; vecBatchIdx < vecBatchCount; vecBatchIdx++) {
        auto rowCount = std::min(maxRowCountPerBatch, static_cast<int32_t>(totalRowCount - totalRowOffset));
        if (spillVecBatch == nullptr || rowCount > maxRowCount) {
            VectorHelper::FreeVecBatch(spillVecBatch);
            spillVecBatch = new VectorBatch(rowCount);
            VectorHelper::AppendVectors(spillVecBatch, dataTypes, rowCount);
            maxRowCount = rowCount;
        } else {
            spillVecBatch->Resize(rowCount);
        }
        pagesIndex->SetSpillVecBatch(spillVecBatch, outputCols, totalRowOffset, canInplaceSort, canRadixSort);
        auto vecBatchSize = CollectVecBatchSize(spillVecBatch);
        if (spillTracker->CheckIfExceedAndReserve(vecBatchSize)) {
            return ErrorCode::EXCEED_SPILL_THRESHOLD;
        }

        auto result = writer->WriteVecBatch(spillVecBatch, vecBatchSize);
        if (result != ErrorCode::SUCCESS) {
            spillTracker->Free(vecBatchSize);
            return result;
        }
        totalRowOffset += rowCount;
    }

    writer->Close();
    return ErrorCode::SUCCESS;
}

uint64_t Spiller::CollectVecBatchSize(vec::VectorBatch *vectorBatch)
{
    uint64_t result = sizeof(int32_t); // for row count size
    int32_t vecCount = vectorBatch->GetVectorCount();
    for (int32_t i = 0; i < vecCount; i++) {
        auto vector = vectorBatch->Get(i);
        switch (dataTypes.GetType(i)->GetId()) {
            case OMNI_BOOLEAN:
                result += CollectVectorSize<bool>(vector);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                result += CollectVectorSize<int32_t>(vector);
                break;
            case OMNI_SHORT:
                result += CollectVectorSize<int16_t>(vector);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
            case OMNI_TIMESTAMP:
                result += CollectVectorSize<int64_t>(vector);
                break;
            case OMNI_DOUBLE:
                result += CollectVectorSize<double>(vector);
                break;
            case OMNI_DECIMAL128:
                result += CollectVectorSize<Decimal128>(vector);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                result += CollectVectorSize<std::string_view>(vector);
                break;
            default: {
                break;
            }
        }
    }
    return result;
}

template <typename T> uint64_t Spiller::CollectVectorSize(vec::BaseVector *vector)
{
    int32_t rowCount = vector->GetSize();
    uint64_t result = rowCount; // nulls byte size
    if constexpr (std::is_same_v<T, std::string_view>) {
        // offsets
        result += (rowCount + 1) * sizeof(int32_t);
        // value length
        auto varcharVector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
        auto offsets = unsafe::UnsafeStringVector::GetOffsets(varcharVector);
        result += offsets[rowCount] - offsets[0];
    } else {
        // value length
        result += rowCount * sizeof(T);
    }
    return result;
}

ErrorCode SpillWriter::CreateTempFile()
{
    auto dirPathChars = dirPath.c_str();
    struct stat st = { 0 };
    if (stat(dirPathChars, &st) == -1) {
        // create the directory when the dir path not exist
        if (mkdir(dirPathChars, 0750) == -1) {
            LogError("Mkdir %s failed.", dirPathChars);
            return ErrorCode::MKDIR_FAILED;
        }
    }

    std::string spillFilePath(dirPath);
    const char templateName[] = "spill-XXXXXX";
    if (dirPathChars[spillFilePath.size() - 1] == '/') {
        spillFilePath.append(templateName);
    } else {
        spillFilePath.append("/");
        spillFilePath.append(templateName);
    }

    auto filePathChars = spillFilePath.c_str();
    // it will open the file and the file permission is 600
    int32_t tempFd = mkstemp(const_cast<char *>(filePathChars));
    if (tempFd == -1) {
        LogError("Mkstemp failed since %s.", strerror(errno));
        return ErrorCode::MKSTEMP_FAILED;
    }
    // set the file permission to 600
    if (fchmod(tempFd, S_IRUSR | S_IWUSR) == -1) {
        LogError("Fchmod failed since %s.", strerror(errno));
        return ErrorCode::WRITE_FAILED;
    }
    fd = tempFd;
    filePath = spillFilePath;
    return ErrorCode::SUCCESS;
}

ErrorCode SpillWriter::WriteVecBatch(vec::VectorBatch *vectorBatch, uint64_t vectorBatchSize)
{
    ErrorCode result = ErrorCode::SUCCESS;
    if (fd == -1) {
        result = CreateTempFile();
        if (result != ErrorCode::SUCCESS) {
            return result;
        }
    }

    if (writeBufferSize != 0) {
        if (writeBufferOffset + vectorBatchSize > writeBufferSize) {
            if (write(fd, writeBuffer, writeBufferOffset) < static_cast<ssize_t>(writeBufferOffset)) {
                LogError("Write buffer to file failed.");
                return ErrorCode::WRITE_FAILED;
            }
            fileLength += writeBufferOffset;
            writeBufferOffset = 0;
        }
        if (vectorBatchSize > writeBufferSize) {
            result = WriteVecBatchToFile(vectorBatch);
            if (result == ErrorCode::SUCCESS) {
                writeBufferOffset += vectorBatchSize;
            }
        } else {
            // write vector batch to writer buffer
            result = WriteVecBatchToBuffer(vectorBatch);
            if (result == ErrorCode::SUCCESS) {
                fileLength += vectorBatchSize;
            }
        }
        return result;
    }

    result = WriteVecBatchToFile(vectorBatch);
    if (result == ErrorCode::SUCCESS) {
        fileLength += vectorBatchSize;
    }
    return result;
}

ErrorCode SpillWriter::WriteVecBatchToBuffer(vec::VectorBatch *vectorBatch)
{
    int32_t rowCount = vectorBatch->GetRowCount();
    int32_t writeOffset = writeBufferOffset;
    auto writeBufferStart = writeBuffer + writeOffset;
    errno_t ret = memcpy_s(writeBufferStart, sizeof(rowCount), &rowCount, sizeof(rowCount));
    if (ret != EOK) {
        LogError("Write row count to buffer failed..");
        return ErrorCode::WRITE_FAILED;
    }
    writeOffset += sizeof(rowCount);

    int32_t vecCount = vectorBatch->GetVectorCount();
    for (int32_t i = 0; i < vecCount; i++) {
        auto vector = vectorBatch->Get(i);
        auto result = ErrorCode::SUCCESS;
        switch (dataTypes.GetType(i)->GetId()) {
            case OMNI_BOOLEAN:
                result = WriteVectorToBuffer<bool>(vector, rowCount, writeOffset);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                result = WriteVectorToBuffer<int32_t>(vector, rowCount, writeOffset);
                break;
            case OMNI_SHORT:
                result = WriteVectorToBuffer<int16_t>(vector, rowCount, writeOffset);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
            case OMNI_TIMESTAMP:
                result = WriteVectorToBuffer<int64_t>(vector, rowCount, writeOffset);
                break;
            case OMNI_DOUBLE:
                result = WriteVectorToBuffer<double>(vector, rowCount, writeOffset);
                break;
            case OMNI_DECIMAL128:
                result = WriteVectorToBuffer<Decimal128>(vector, rowCount, writeOffset);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                result = WriteVectorToBuffer<std::string_view>(vector, rowCount, writeOffset);
                break;
            default: {
                result = ErrorCode::WRITE_FAILED;
                break;
            }
        }
        if (result != ErrorCode::SUCCESS) {
            return result;
        }
    }
    totalRowCount += rowCount;
    return ErrorCode::SUCCESS;
}

template <typename T>
ErrorCode SpillWriter::WriteVectorToBuffer(vec::BaseVector *vector, int32_t rowCount, int32_t &writeOffset)
{
    bool *nulls = unsafe::UnsafeBaseVector::GetNulls(vector);
    errno_t ret = memcpy_s(writeBuffer + writeOffset, rowCount, nulls, rowCount);
    if (ret != EOK) {
        LogError("Write value nulls to buffer failed.");
        return ErrorCode::WRITE_FAILED;
    }
    writeOffset += rowCount;

    if constexpr (std::is_same_v<T, std::string_view>) {
        // write offsets
        auto valueOffsets = reinterpret_cast<int32_t *>(VectorHelper::UnsafeGetOffsetsAddr(vector));
        auto offsetLength = static_cast<ssize_t>((rowCount + 1) * sizeof(int32_t));
        ret = memcpy_s(writeBuffer + writeOffset, offsetLength, valueOffsets, offsetLength);
        if (ret != EOK) {
            LogError("Write value offsets to buffer failed.");
            return op::ErrorCode::WRITE_FAILED;
        }
        writeOffset += offsetLength;

        // write values
        char *values = unsafe::UnsafeStringVector::GetValues(reinterpret_cast<VarcharVector *>(vector));
        auto valueLength = static_cast<ssize_t>(valueOffsets[rowCount] - valueOffsets[0]);
        ret = memcpy_s(writeBuffer + writeOffset, valueLength, values, valueLength);
        if (ret != EOK) {
            LogError("Write values to buffer failed.");
            return op::ErrorCode::WRITE_FAILED;
        }
        writeOffset += valueLength;
        return ErrorCode::SUCCESS;
    } else {
        auto length = static_cast<ssize_t>(rowCount * sizeof(T));
        T *values = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<T> *>(vector));
        ret = memcpy_s(writeBuffer + writeOffset, length, values, length);
        if (ret != EOK) {
            LogError("Write values to buffer failed.");
            return ErrorCode::WRITE_FAILED;
        }
        writeOffset += length;
        return ErrorCode::SUCCESS;
    }
}

ErrorCode SpillWriter::WriteVecBatchToFile(vec::VectorBatch *vectorBatch)
{
    int32_t rowCount = vectorBatch->GetRowCount();
    if (write(fd, &rowCount, sizeof(rowCount)) < static_cast<ssize_t>(sizeof(rowCount))) {
        LogError("Write row count failed.");
        return ErrorCode::WRITE_FAILED;
    }

    int32_t vecCount = vectorBatch->GetVectorCount();
    for (int32_t i = 0; i < vecCount; i++) {
        auto vector = vectorBatch->Get(i);
        auto result = ErrorCode::SUCCESS;
        switch (dataTypes.GetType(i)->GetId()) {
            case OMNI_BOOLEAN:
                result = WriteVector<bool>(vector, rowCount);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                result = WriteVector<int32_t>(vector, rowCount);
                break;
            case OMNI_SHORT:
                result = WriteVector<int16_t>(vector, rowCount);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
            case OMNI_TIMESTAMP:
                result = WriteVector<int64_t>(vector, rowCount);
                break;
            case OMNI_DOUBLE:
                result = WriteVector<double>(vector, rowCount);
                break;
            case OMNI_DECIMAL128:
                result = WriteVector<Decimal128>(vector, rowCount);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                result = WriteVector<std::string_view>(vector, rowCount);
                break;
            default: {
                result = ErrorCode::WRITE_FAILED;
                break;
            }
        }

        if (result != ErrorCode::SUCCESS) {
            return result;
        }
    }

    totalRowCount += rowCount;
    return ErrorCode::SUCCESS;
}

/**
 * vector format stored in file column by column, {nulls meta column, offsets meta column, values}
 * -nulls--offsets--values
 * 0        0      "aab"
 * 1        3        -
 * 0        3      "bbcd"
 * 7
 */
template <typename T> ErrorCode SpillWriter::WriteVector(omniruntime::vec::BaseVector *vector, int32_t rowCount)
{
    bool *nulls = unsafe::UnsafeBaseVector::GetNulls(vector);
    if (write(fd, nulls, rowCount) < rowCount) {
        LogError("Write value nulls failed.");
        return ErrorCode::WRITE_FAILED;
    }

    if constexpr (std::is_same_v<T, std::string_view>) {
        // write offsets
        auto offsets = reinterpret_cast<int32_t *>(VectorHelper::UnsafeGetOffsetsAddr(vector));
        auto offsetSize = static_cast<ssize_t>((rowCount + 1) * sizeof(int32_t));
        if (write(fd, offsets, offsetSize) < offsetSize) {
            LogError("Write value offsets failed.");
            return op::ErrorCode::WRITE_FAILED;
        }

        auto valueLength = static_cast<ssize_t>(offsets[rowCount] - offsets[0]);
        if (valueLength > 0) {
            // write values
            char *values = unsafe::UnsafeStringVector::GetValues(reinterpret_cast<VarcharVector *>(vector));
            if (write(fd, values, valueLength) < valueLength) {
                LogError("Write values failed.");
                return op::ErrorCode::WRITE_FAILED;
            }
        }
        return ErrorCode::SUCCESS;
    } else {
        auto length = static_cast<ssize_t>(rowCount * sizeof(T));
        T *values = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<T> *>(vector));
        if (write(fd, values, length) < length) {
            LogError("Write values failed.");
            return ErrorCode::WRITE_FAILED;
        }
        return ErrorCode::SUCCESS;
    }
}

void SpillWriter::Close()
{
    if (writeBufferOffset != 0) {
        if (write(fd, writeBuffer, writeBufferOffset) < static_cast<ssize_t>(writeBufferOffset)) {
            LogError("Write buffer to file failed.");
        }
        fileLength += writeBufferOffset;
        writeBufferOffset = 0;
    }
    close(fd);
}
}
}