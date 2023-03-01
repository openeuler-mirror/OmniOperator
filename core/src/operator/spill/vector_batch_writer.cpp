/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: vector batch writer implements
 */

#include <cstdlib>
#include <unistd.h>
#include <cstring>
#include "vector/unsafe_vector.h"
#include "vector/vector_helper.h"
#include "operator/util/operator_util.h"
#include "vector_batch_writer.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using VarcharVector = vec::Vector<vec::LargeStringContainer<std::string_view>>;
VectorBatchWriter::VectorBatchWriter(SpillTracker *tracker, const omniruntime::type::DataTypes &sourceTypes)
    : fd(-1), fileLength(0), tracker(tracker), sourceTypes(sourceTypes)
{}

ErrorCode VectorBatchWriter::CreateTempFile(const std::string &path)
{
    const char templateName[] = "tempfile-XXXXXX";
    std::string filePath(path);

    if (filePath.c_str()[filePath.size() - 1] == '/') {
        filePath.append(templateName);
    } else {
        filePath.append("/");
        filePath.append(templateName);
    }

    auto filePathChars = filePath.c_str();
    auto resultFd = mkstemp(const_cast<char *>(filePathChars));
    if (resultFd == -1) {
        LogError("Mkstemp failed since %s.", strerror(errno));
        return ErrorCode::MKSTEMP_FAILED;
    }

    auto result = unlink(filePathChars);
    if (result == -1) {
        LogError("Unlink failed since %s.", strerror(errno));
        return ErrorCode::UNLINK_FAILED;
    }

    fd = resultFd;
    return ErrorCode::SUCCESS;
}

ErrorCode VectorBatchWriter::WriteVecBatches(VectorBatchUnitIter &vecBatches)
{
    LogInfo("Spill data to disk starting.");
    int64_t totalRowCount = 0;
    bool writtenHeader = false;
    while (vecBatches.HasNext()) {
        auto vecBatch = vecBatches.Next()->GetVectorBatch();
        auto vecBatchSize = GetVecBatchSize(vecBatch) + sizeof(totalRowCount);
        vecBatchSize += writtenHeader ? 0 : (1 + vecBatch->GetVectorCount()) * sizeof(int32_t);
        if (tracker->CheckIfExceedAndReserve(vecBatchSize)) {
            close(fd);
            return ErrorCode::EXCEED_SPILL_THRESHOLD;
        }

        totalRowCount += vecBatch->GetRowCount();

        // write file header
        if (!writtenHeader) {
            auto result = WriteFileHeader(vecBatch);
            if (result != ErrorCode::SUCCESS) {
                close(fd);
                return result;
            }
            writtenHeader = true;
        }

        // write data
        if (WriteVecBatch(vecBatch) != ErrorCode::SUCCESS) {
            close(fd);
            return ErrorCode::WRITE_FAILED;
        }
        fileLength += vecBatchSize;
    }

    // write file tail
    if (write(fd, &totalRowCount, sizeof(totalRowCount)) < static_cast<ssize_t>(sizeof(totalRowCount))) {
        close(fd);
        return ErrorCode::WRITE_FAILED;
    }
    LogInfo("Spill data to disk finished.");
    return ErrorCode::SUCCESS;
}

ErrorCode VectorBatchWriter::WriteFileHeader(omniruntime::vec::VectorBatch *vectorBatch)
{
    int32_t vecCount = vectorBatch->GetVectorCount();
    auto typeIds = sourceTypes.GetIds();
    auto vecCountSize = static_cast<ssize_t>(sizeof(vecCount));
    if (write(fd, &vecCount, vecCountSize) < vecCountSize) {
        LogError("Write vec count failed.");
        return ErrorCode::WRITE_FAILED;
    }
    auto typeIdsSize = static_cast<ssize_t>(vecCount * sizeof(int32_t));
    if (write(fd, typeIds, typeIdsSize) < typeIdsSize) {
        LogError("Write type ids failed.");
        return ErrorCode::WRITE_FAILED;
    }
    return ErrorCode::SUCCESS;
}

template <typename T> ErrorCode VectorBatchWriter::WriteVector(omniruntime::vec::BaseVector *vector, int32_t rowCount)
{
    bool *nulls = unsafe::UnsafeBaseVector::GetNulls(vector);
    if (write(fd, nulls, rowCount) < rowCount) {
        LogError("Write value nulls failed.");
        return ErrorCode::WRITE_FAILED;
    }

    auto length = static_cast<ssize_t>(rowCount * sizeof(T));

    T *values = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<T> *>(vector));
    if (write(fd, values, length) < length) {
        LogError("Write values failed.");
        return ErrorCode::WRITE_FAILED;
    }
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
ErrorCode VectorBatchWriter::WriteVarcharVector(omniruntime::vec::BaseVector *vector, int32_t rowCount)
{
    // write nulls
    bool *nulls = unsafe::UnsafeBaseVector::GetNulls(vector);
    if (write(fd, nulls, rowCount) < rowCount) {
        LogError("Write value nulls failed.");
        return op::ErrorCode::WRITE_FAILED;
    }

    // write offsets
    auto valueOffsets = reinterpret_cast<int32_t *>(VectorHelper::GetOffsetsAddr(vector, OMNI_VARCHAR));
    auto offsetSize = static_cast<ssize_t>((rowCount + 1) * sizeof(int32_t));
    if (write(fd, valueOffsets, offsetSize) < offsetSize) {
        LogError("Write value offsets failed.");
        return op::ErrorCode::WRITE_FAILED;
    }

    // write values row by row, values buffer may not continuous memory
    char *values = unsafe::UnsafeStringVector::GetValues(reinterpret_cast<VarcharVector *>(vector));
    auto length = static_cast<ssize_t>(valueOffsets[rowCount] - valueOffsets[0]);
    if (write(fd, values, length) < length) {
        LogError("Write values failed.");
        return op::ErrorCode::WRITE_FAILED;
    }

    return ErrorCode::SUCCESS;
}

ErrorCode VectorBatchWriter::WriteVecBatch(VectorBatch *vectorBatch)
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
        switch (sourceTypes.GetType(i)->GetId()) {
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
                result = WriteVector<int64_t>(vector, rowCount);
                break;
            case OMNI_DOUBLE:
                result = WriteVector<double>(vector, rowCount);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                result = WriteVarcharVector(vector, rowCount);
                break;
            case OMNI_DECIMAL128:
                result = WriteVector<Decimal128>(vector, rowCount);
                break;
            default: {
                break;
            }
        }

        if (result != ErrorCode::SUCCESS) {
            return result;
        }
    }
    return ErrorCode::SUCCESS;
}

uint64_t VectorBatchWriter::GetVecBatchSize(VectorBatch *vectorBatch)
{
    uint64_t size = sizeof(int32_t); // for row count
    int32_t vecCount = vectorBatch->GetVectorCount();
    int32_t rowCount = vectorBatch->GetRowCount();
    for (int32_t i = 0; i < vecCount; i++) {

        size += rowCount * sizeof(bool); // for nulls

        switch (sourceTypes.GetType(i)->GetId()) {
            case type::OMNI_INT:
            case type::OMNI_DATE32:
                size += rowCount * OperatorUtil::SIZE_OF_INT;
                break;
            case type::OMNI_SHORT:
                size += rowCount * OperatorUtil::SIZE_OF_SHORT;
                break;
            case type::OMNI_LONG:
            case type::OMNI_DECIMAL64:
                size += rowCount * OperatorUtil::SIZE_OF_LONG;
                break;
            case type::OMNI_DOUBLE:
                size += rowCount * OperatorUtil::SIZE_OF_DOUBLE;
                break;
            case type::OMNI_BOOLEAN:
                size += rowCount * OperatorUtil::SIZE_OF_BOOL;
                break;
            case type::OMNI_DECIMAL128:
                size += rowCount * OperatorUtil::SIZE_OF_DECIMAL128;
                break;
            case type::OMNI_CHAR:
            case type::OMNI_VARCHAR: {
                auto vector = vectorBatch->Get(i);
                auto valueOffsets = reinterpret_cast<int32_t *>(VectorHelper::GetOffsetsAddr(vector, OMNI_VARCHAR));
                size += (rowCount + 1) * sizeof(int32_t); // for offsets
                size += (valueOffsets[rowCount] - valueOffsets[0]);
                break;
            }
            default:
                break;
        }
    }
    return size;
}
}
}
