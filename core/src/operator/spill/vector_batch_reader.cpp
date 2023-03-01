/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: vector batch reader implements
 */

#include <unistd.h>
#include "util/error_code.h"
#include "vector_batch_reader.h"
#include "vector/unsafe_vector.h"
#include "vector/vector_helper.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::type;
using VarcharVector = vec::Vector<vec::LargeStringContainer<std::string_view>>;
ErrorCode VectorBatchReader::ReadFileTailAndHead()
{
    // read file tail
    lseek(fd, static_cast<int64_t>(-sizeof(int64_t)), SEEK_END);
    if (read(fd, &totalRowCount, sizeof(totalRowCount)) < static_cast<ssize_t>(sizeof(totalRowCount))) {
        LogError("Read total row count failed.");
        return ErrorCode::READ_FAILED;
    }
    lseek(fd, 0, SEEK_SET);

    // read file header
    if (read(fd, &vecCount, sizeof(vecCount)) < static_cast<ssize_t>(sizeof(vecCount))) {
        LogError("Read vec count failed.");
        return ErrorCode::READ_FAILED;
    }
    vecTypeIds = new int32_t[vecCount];
    auto typeIdsSize = static_cast<ssize_t>(vecCount * sizeof(int32_t));
    if (read(fd, vecTypeIds, typeIdsSize) < typeIdsSize) {
        LogError("Read type ids failed.");
        return ErrorCode::READ_FAILED;
    }
    return ErrorCode::SUCCESS;
}

bool VectorBatchReader::HasNext()
{
    if (rowOffset >= totalRowCount) {
        close(fd);
        return false;
    }

    auto result = ReadVecBatch();
    if (result == nullptr) {
        close(fd);
        return false;
    }
    vectorBatchUnit->SetVectorBatch(result);
    return true;
}

VectorBatchUnit *VectorBatchReader::Next()
{
    return vectorBatchUnit;
}

/**
 * vector format stored in file column by column, {nulls meta column, offsets meta column, values}
 * -nulls--offsets--values
 * 0        0      "aab"
 * 1        3        -
 * 0        3      "bbcd"
 *          7
 */
std::unique_ptr<BaseVector> VectorBatchReader::ReadVarcharVector(int32_t rowCount)
{
    auto vector = VectorHelper::CreateStringVector(rowCount);

    // read nulls
    bool *nulls = unsafe::UnsafeBaseVector::GetNulls(vector.get());
    if (read(fd, nulls, rowCount) < rowCount) {
        LogError("Read value nulls failed.");
        return nullptr;
    }

    // read offsets
    auto offsetSize = static_cast<ssize_t>((rowCount + 1) * sizeof(int32_t));
    auto offsets = reinterpret_cast<int32_t *>(VectorHelper::GetOffsetsAddr(vector.get(), OMNI_VARCHAR));
    if (read(fd, offsets, offsetSize) < offsetSize) {
        LogError("Read value offsets failed.");
        return nullptr;
    }

    // read values
    auto length = offsets[rowCount] - offsets[0];
    char *valuesBuffer;
    if  (length <= INITIAL_STRING_SIZE) {
        valuesBuffer = unsafe::UnsafeStringVector::GetValues(reinterpret_cast<VarcharVector *>(vector.get()));
    } else {
        valuesBuffer =
                unsafe::UnsafeStringVector::ExpandStringBuffer(reinterpret_cast<VarcharVector *>(vector.get()), length);
    }
    if (read(fd, valuesBuffer, length) < static_cast<ssize_t>(length)) {
        LogError("Read values failed.");
        return nullptr;
    }

    return vector;
}

template <typename T> std::unique_ptr<BaseVector> VectorBatchReader::ReadVector(int32_t rowCount)
{
    auto vector = std::make_unique<vec::Vector<T>>(rowCount);

    // read valueNulls
    bool *nulls = unsafe::UnsafeBaseVector::GetNulls(vector.get());
    if (read(fd, nulls, rowCount) < static_cast<ssize_t>(rowCount)) {
        LogError("Read value nulls failed.");
        return nullptr;
    }

    // read values
    T *values = unsafe::UnsafeVector::GetRawValues(vector.get());
    auto length = static_cast<ssize_t>(rowCount * sizeof(T));
    if (read(fd, values, length) < length) {
        LogError("Read values failed.");
        return nullptr;
    }

    return vector;
}

VectorBatch *VectorBatchReader::ReadVecBatch()
{
    int32_t rowCount;
    if (read(fd, &rowCount, sizeof(int32_t)) < static_cast<ssize_t>(sizeof(int32_t))) {
        LogError("Read row count failed.");
        return nullptr;
    }

    auto vecBatch = new VectorBatch(rowCount);
    for (int32_t vecIndex = 0; vecIndex < vecCount; vecIndex++) {
        std::unique_ptr<BaseVector> vector = nullptr;
        int32_t typeId = vecTypeIds[vecIndex];
        switch (typeId) {
            case OMNI_BOOLEAN:
                vector = ReadVector<bool>(rowCount);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                vector = ReadVector<int32_t>(rowCount);
                break;
            case OMNI_SHORT:
                vector = ReadVector<int16_t>(rowCount);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                vector = ReadVector<int64_t>(rowCount);
                break;
            case OMNI_DOUBLE:
                vector = ReadVector<double>(rowCount);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                vector = ReadVarcharVector(rowCount);
                break;
            case OMNI_DECIMAL128:
                vector = ReadVector<Decimal128>(rowCount);
                break;
            default:
                break;
        }
        if (vector == nullptr) {
            VectorHelper::FreeVecBatch(vecBatch);
            return nullptr;
        }
        vecBatch->Append(std::move(vector));
    }

    rowOffset += rowCount;
    return vecBatch;
}
}
}
