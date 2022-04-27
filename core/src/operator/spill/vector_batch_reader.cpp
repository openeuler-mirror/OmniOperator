/*
 * @Copyright: Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * @Description: vector batch reader implements
 */

#include <unistd.h>
#include "util/error_code.h"
#include "vector/fixed_width_vector.h"
#include "vector/variable_width_vector.h"
#include "operator/util/operator_util.h"
#include "vector_batch_reader.h"

namespace omniruntime {
namespace op {
using namespace omniruntime::vec;
using namespace omniruntime::type;

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

Vector *VectorBatchReader::ReadVarcharVector(int32_t rowCount)
{
    // read valueNulls and valueOffsets
    auto nullAndOffsetSize = static_cast<ssize_t>(rowCount + (rowCount + 1) * sizeof(int32_t));
    auto nullAndOffsets = new int8_t[nullAndOffsetSize];
    if (read(fd, nullAndOffsets, nullAndOffsetSize) < nullAndOffsetSize) {
        LogError("Read value nulls and offsets failed.");
        delete[] nullAndOffsets;
        return nullptr;
    }

    auto valueOffsets = reinterpret_cast<int32_t *>(nullAndOffsets + rowCount);
    auto length = valueOffsets[rowCount] - valueOffsets[0];
    auto vector = new VarcharVector(vectorAllocator, length, rowCount);
    if (memcpy_s(vector->GetValueNulls(), nullAndOffsetSize, nullAndOffsets, nullAndOffsetSize) != EOK) {
        LogError("Memory copy failed.");
        delete[] nullAndOffsets;
        return nullptr;
    }

    delete[] nullAndOffsets;
    // read values
    if (read(fd, vector->GetValues(), length) < static_cast<ssize_t>(length)) {
        LogError("Read values failed.");
        delete vector;
        return nullptr;
    }
    return vector;
}

template <typename V, typename T> Vector *VectorBatchReader::ReadVector(int32_t rowCount)
{
    auto vector = new V(vectorAllocator, rowCount);

    // read valueNulls
    if (read(fd, vector->GetValueNulls(), rowCount) < static_cast<ssize_t>(rowCount)) {
        LogError("Read value nulls failed.");
        delete vector;
        return nullptr;
    }

    // read values
    auto length = static_cast<ssize_t>(rowCount * sizeof(T));
    if (read(fd, vector->GetValues(), length) < length) {
        LogError("Read values failed.");
        delete vector;
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

    auto vecBatch = new VectorBatch(vecCount, rowCount);
    for (int32_t vecIndex = 0; vecIndex < vecCount; vecIndex++) {
        Vector *vector = nullptr;
        int32_t typeId = vecTypeIds[vecIndex];
        switch (typeId) {
            case OMNI_BOOLEAN:
                vector = ReadVector<BooleanVector, bool>(rowCount);
                break;
            case OMNI_INT:
            case OMNI_DATE32:
                vector = ReadVector<IntVector, int32_t>(rowCount);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                vector = ReadVector<LongVector, int64_t>(rowCount);
                break;
            case OMNI_DOUBLE:
                vector = ReadVector<DoubleVector, double>(rowCount);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                vector = ReadVarcharVector(rowCount);
                break;
            case OMNI_DECIMAL128:
                vector = ReadVector<Decimal128Vector, Decimal128>(rowCount);
                break;
            default:
                break;
        }
        if (vector == nullptr) {
            VectorHelper::FreeVecBatch(vecBatch);
            return nullptr;
        }
        vecBatch->SetVector(vecIndex, vector);
    }

    rowOffset += rowCount;
    return vecBatch;
}
}
}
