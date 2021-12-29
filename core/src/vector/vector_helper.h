/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

#ifndef OMNI_RUNTIME_VECTOR_HELPER_H
#define OMNI_RUNTIME_VECTOR_HELPER_H

#include <memory>
#include <vector>
#include "vector.h"
#include "vector_batch.h"
#include "variable_width_vector.h"
#include "fixed_width_vector.h"
#include "container_vector.h"
#include "dictionary_vector.h"
#include "lazy_vector.h"

namespace omniruntime {
namespace vec {
class VectorHelper {
public:
    static void SetValue(Vector *vector, int32_t index, void *value)
    {
        if (value == nullptr) {
            vector->SetValueNull(index);
            return;
        }
        switch (vector->GetTypeId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
                static_cast<IntVector *>(vector)->SetValue(index, *static_cast<int32_t *>(value));
                break;
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                static_cast<LongVector *>(vector)->SetValue(index, *static_cast<int64_t *>(value));
                break;
            case OMNI_VEC_TYPE_DOUBLE:
                static_cast<DoubleVector *>(vector)->SetValue(index, *static_cast<double *>(value));
                break;
            case OMNI_VEC_TYPE_BOOLEAN:
                static_cast<BooleanVector *>(vector)->SetValue(index, *static_cast<bool *>(value));
                break;
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR:
                static_cast<VarcharVector *>(vector)->SetValue(index,
                    reinterpret_cast<const uint8_t *>(static_cast<std::string *>(value)->c_str()),
                    static_cast<std::string *>(value)->length());
                break;
            case OMNI_VEC_TYPE_DECIMAL128:
                static_cast<Decimal128Vector *>(vector)->SetValue(index, *static_cast<Decimal128 *>(value));
                break;
            default:
                LogError("No such data type %d", vector->GetTypeId());
                break;
        }
    }

    static void SetValue(Vector *vector, int32_t index, void *value, int32_t length)
    {
        switch (vector->GetTypeId()) {
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR:
                static_cast<VarcharVector *>(vector)->SetValue(index, static_cast<uint8_t *>(value), length);
                break;
            default:
                LogError("No such data type %d", vector->GetTypeId());
                break;
        }
    }

    static int32_t GetValue(Vector *vector, int32_t index, void *value)
    {
        int32_t length = 0;
        switch (vector->GetTypeId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32: {
                *static_cast<int32_t *>(value) = static_cast<IntVector *>(vector)->GetValue(index);
                break;
            }
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64: {
                *static_cast<int64_t *>(value) = static_cast<LongVector *>(vector)->GetValue(index);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                *static_cast<double *>(value) = static_cast<DoubleVector *>(vector)->GetValue(index);
                break;
            }
            case OMNI_VEC_TYPE_BOOLEAN: {
                *static_cast<bool *>(value) = static_cast<BooleanVector *>(vector)->GetValue(index);
                break;
            }
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR: {
                length = static_cast<VarcharVector *>(vector)->GetValue(index, static_cast<uint8_t **>(value));
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                *static_cast<Decimal128 *>(value) = static_cast<Decimal128Vector *>(vector)->GetValue(index);
                break;
            }
            case OMNI_VEC_TYPE_DICTIONARY: {
                auto *dictionaryVector = static_cast<DictionaryVector *>(vector);
                Vector *dictionary = dictionaryVector->GetDictionary();
                int32_t id = dictionaryVector->GetId(index);
                length = VectorHelper::GetValue(dictionary, id, value);
                break;
            }
            default:
                LogError("No such data type %d", vector->GetTypeId());
                break;
        }
        return length;
    }

    static Vector *CreateVector(VectorAllocator *allocator, int32_t typeId, int32_t capacityInBytes, int32_t size)
    {
        Vector *vector = nullptr;
        switch (typeId) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32: {
                vector = new IntVector(allocator, size);
                break;
            }
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64: {
                vector = new LongVector(allocator, size);
                break;
            }
            case OMNI_VEC_TYPE_DOUBLE: {
                vector = new DoubleVector(allocator, size);
                break;
            }
            case OMNI_VEC_TYPE_BOOLEAN: {
                vector = new BooleanVector(allocator, size);
                break;
            }
            case OMNI_VEC_TYPE_CONTAINER: {
                vector = new ContainerVector(allocator, capacityInBytes, size);
                break;
            }
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR: {
                vector = new VarcharVector(allocator, capacityInBytes, size);
                break;
            }
            case OMNI_VEC_TYPE_DECIMAL128: {
                vector = new Decimal128Vector(allocator, size);
                break;
            }
            case OMNI_VEC_TYPE_DICTIONARY: {
                vector = new DictionaryVector(allocator, size);
                break;
            }
            case OMNI_VEC_TYPE_LAZY: {
                vector = new LazyVector(allocator, size);
                break;
            }
            default: {
                LogError("No such data type %d", typeId);
                break;
            }
        }
        return vector;
    }

    static Vector *ExpandVectorAndIndex(Vector *vector, int32_t index, int32_t &originalIndex)
    {
        if (vector->GetTypeId() != OMNI_VEC_TYPE_DICTIONARY) {
            originalIndex = index;
            return vector;
        }
        // TODO: add RLE type.

        return static_cast<DictionaryVector *>(vector)->ExtractDictionaryAndId(index, originalIndex);
    }

    static Vector *ExpandVectorAndIndexes(Vector *vector, int32_t offset, int32_t length, int32_t *originalIndexes)
    {
        if (vector->GetTypeId() != OMNI_VEC_TYPE_DICTIONARY) {
            for (int i = 0; i < length; ++i) {
                originalIndexes[i] = offset + i;
            }
            return vector;
        }
        // TODO: add RLE type.

        return static_cast<DictionaryVector *>(vector)->ExtractDictionaryAndIds(offset, length, originalIndexes);
    }

    static int64_t GetValuesAddr(Vector *vector)
    {
        int32_t positionOffset = vector->GetPositionOffset();
        void *values = vector->GetValues();
        switch (vector->GetTypeId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
                return reinterpret_cast<int64_t>(reinterpret_cast<int32_t *>(values) + positionOffset);
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                return reinterpret_cast<int64_t>(reinterpret_cast<int64_t *>(values) + positionOffset);
            case OMNI_VEC_TYPE_DOUBLE:
                return reinterpret_cast<int64_t>(reinterpret_cast<double *>(values) + positionOffset);
            case OMNI_VEC_TYPE_BOOLEAN:
                return reinterpret_cast<int64_t>(reinterpret_cast<bool *>(values) + positionOffset);
            case OMNI_VEC_TYPE_DECIMAL128:
                return reinterpret_cast<int64_t>(reinterpret_cast<int64_t *>(values) + 2 * positionOffset);
            case OMNI_VEC_TYPE_VARCHAR:
            case OMNI_VEC_TYPE_CHAR:
                return reinterpret_cast<int64_t>(values);
            case OMNI_VEC_TYPE_LAZY:
                return reinterpret_cast<int64_t>(values);
            default:
                LogError("Do not support such vector type %d", vector->GetTypeId());
                return 0;
        }
    }

    static int64_t GetValuePtrAndLength(Vector *vector, int32_t rowIndex, int32_t *length)
    {
        int32_t positionOffset = vector->GetPositionOffset();
        int32_t finalIndex = positionOffset + rowIndex;
        void *values = vector->GetValues();
        switch (vector->GetTypeId()) {
            case OMNI_VEC_TYPE_INT:
            case OMNI_VEC_TYPE_DATE32:
                return reinterpret_cast<int64_t>(reinterpret_cast<int32_t *>(values) + finalIndex);
            case OMNI_VEC_TYPE_LONG:
            case OMNI_VEC_TYPE_DECIMAL64:
                return reinterpret_cast<int64_t>(reinterpret_cast<int64_t *>(values) + finalIndex);
            case OMNI_VEC_TYPE_DOUBLE:
                return reinterpret_cast<int64_t>(reinterpret_cast<double *>(values) + finalIndex);
            case OMNI_VEC_TYPE_BOOLEAN:
                return reinterpret_cast<int64_t>(reinterpret_cast<bool *>(values) + finalIndex);
            case OMNI_VEC_TYPE_DECIMAL128:
                return reinterpret_cast<int64_t>(reinterpret_cast<int64_t *>(values) + 2 * finalIndex);
            case OMNI_VEC_TYPE_VARCHAR: {
                uint8_t *value = nullptr;
                *length = static_cast<VarcharVector *>(vector)->GetValue(rowIndex, &value);
                return reinterpret_cast<int64_t>(value);
            }
            case OMNI_VEC_TYPE_DICTIONARY: {
                auto dictionaryVector = static_cast<DictionaryVector *>(vector);
                int32_t originalRowIndex;
                auto dictionary = dictionaryVector->ExtractDictionaryAndId(rowIndex, originalRowIndex);
                return GetValuePtrAndLength(dictionary, originalRowIndex, length);
            }
            default:
                LogError("Do not support such vector type %d", vector->GetTypeId());
                return 0;
        }
    }

    static int64_t GetNullsAddr(Vector *vector)
    {
        return reinterpret_cast<int64_t>(reinterpret_cast<bool *>(vector->GetValueNulls()) +
            vector->GetPositionOffset());
    }

    static int64_t GetOffsetsAddr(Vector *vector)
    {
        return reinterpret_cast<int64_t>(reinterpret_cast<int32_t *>(vector->GetValueOffsets()) +
            vector->GetPositionOffset());
    }

    static void FreeVecBatch(VectorBatch *vecBatch);

    static void FreeVecBatches(VectorBatch **vecBatches, int32_t vecBatchCount);

    static void FreeVecBatches(std::vector<VectorBatch *> &vecBatches);

    static void PrintVecBatch(VectorBatch *vecBatch);

    static void PrintVectorValue(Vector *vector, int32_t rowIndex);

    static VectorBatch *ConcatVectorBatches(std::vector<VectorBatch *> &vecBatches);

private:
};
} // namespace vec
} // namespace omniruntime
#endif // OMNI_RUNTIME_VECTOR_HELPER_H
