/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */
#ifndef OMNI_RUNTIME_VECTOR_HELPER_H
#define OMNI_RUNTIME_VECTOR_HELPER_H

#include "vector.h"
#include "large_string_container.h"
#include "unsafe_vector.h"
#include "type/data_types.h"
#include "vector_batch.h"
#include "operator/aggregation/container_vector.h"

namespace omniruntime::vec {
class VectorHelper {
public:
    static BaseVector *CreateStringDictionary(int32_t *values, int32_t valueSize,
        Vector<LargeStringContainer<std::string_view>> *vector)
    {
        std::shared_ptr<bool[]> nulls = std::shared_ptr<bool[]>(new bool[valueSize]);
        for (int i = 0; i < valueSize; i++) {
            nulls[i] = vector->IsNull(values[i]);
        }

        // todo:: handing other types of container
        auto dictionary = std::make_shared<DictionaryContainer<std::string_view>>(values, valueSize,
            unsafe::UnsafeStringVector::GetContainer(
            reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)),
            vector->GetSize(), vector->GetOffset());
        return new Vector<DictionaryContainer<std::string_view>>(valueSize, dictionary, nulls, OMNI_CHAR);
    }

    template <typename T>
    static BaseVector *CreateDictionary(int32_t *values, int32_t valueSize, Vector<T> *vector)
    {
        std::shared_ptr<bool[]> nulls = std::shared_ptr<bool[]>(new bool[valueSize]);
        for (int i = 0; i < valueSize; i++) {
            nulls[i] = vector->IsNull(values[i]);
        }

        auto dictionary = std::make_shared<DictionaryContainer<T>>(values, valueSize,
            unsafe::UnsafeVector::GetValues<T>(vector), vector->GetSize(), vector->GetOffset());
        return new Vector<DictionaryContainer<T>>(valueSize, dictionary, nulls, TYPE_ID<T>);
    }

    /* *
     * string vector creation helper
     * @param vectorSize
     */
    static ALWAYS_INLINE BaseVector *CreateStringVector(uint32_t vectorSize)
    {
        return new Vector<LargeStringContainer<std::string_view>>(vectorSize);
    }

    static BaseVector *CreateVector(int32_t vectorEncodingId, int32_t dataTypeId, int32_t size,
        int32_t capacityInBytes = INITIAL_STRING_SIZE)
    {
        using namespace omniruntime::type;
        if (vectorEncodingId == OMNI_FLAT) {
            return DYNAMIC_TYPE_DISPATCH(CreateFlatVector, dataTypeId, size, capacityInBytes);
        } else if (vectorEncodingId == OMNI_ENCODING_CONTAINER) {
            return new ContainerVector(capacityInBytes, size);
        } else {
            LogError("No such encoding type %d", vectorEncodingId);
            return nullptr;
        }
    }

    static BaseVector *CreateFlatVector(int32_t dataTypeId, int32_t size,
                                                    int32_t capacityInBytes = INITIAL_STRING_SIZE)
    {
        using namespace omniruntime::type;
        return DYNAMIC_TYPE_DISPATCH(CreateFlatVector, dataTypeId, size, capacityInBytes);
    }

    template <type::DataTypeId typeId>
    static BaseVector *CreateFlatVector(int32_t size, int32_t capacityInBytes = INITIAL_STRING_SIZE)
    {
        using T = typename type::NativeType<typeId>::type;
        if constexpr (std::is_same_v<T, std::string_view>) {
            return new Vector<LargeStringContainer<std::string_view>>(size, capacityInBytes);
        }
        return new Vector<T>(size, typeId);
    }

    template <type::DataTypeId typeId> static void VectorSetValue(vec::BaseVector *vector, int32_t index, void *value)
    {
        using T = typename type::NativeType<typeId>::type;
        if (value == nullptr) {
            if constexpr (std::is_same_v<T, std::string_view>) {
                static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->SetNull(index);
            } else {
                vector->SetNull(index);
            }
            return;
        }

        if constexpr (std::is_same_v<T, std::string_view>) {
            std::string_view data = std::string_view(static_cast<std::string *>(value)->data(),
                static_cast<std::string *>(value)->length());
            static_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->SetValue(index, data);
        } else {
            static_cast<Vector<T> *>(vector)->SetValue(index, *static_cast<T *>(value));
        }
        return;
    }

    static void SetValue(vec::BaseVector *vector, int32_t index, void *value)
    {
        using namespace omniruntime::type;
        return DYNAMIC_TYPE_DISPATCH(VectorSetValue, vector->GetTypeId(), vector, index, value);
    }

    static void PrintVecBatch(VectorBatch *vecBatch)
    {
        int32_t vectorCount = vecBatch->GetVectorCount();
        int32_t rowCount = vecBatch->GetRowCount();
        for (int32_t rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
            for (int32_t colIdx = 0; colIdx < vectorCount; ++colIdx) {
                auto vector = vecBatch->Get(colIdx);
                PrintVectorValue(vector, rowIdx);
            }
            std::cout << std::endl;
        }
    }

    template <type::DataTypeId typeId> static void PrintDictionaryVectorValue(BaseVector *vector, int32_t rowIndex)
    {
        using namespace omniruntime::type;
        using T = typename NativeType<typeId>::type;
        using DictionaryVarchar = Vector<DictionaryContainer<std::string_view, LargeStringContainer>>;
        if constexpr (std::is_same_v<T, std::string_view>) {
            std::cout << std::dec << static_cast<DictionaryVarchar *>(vector)->GetValue(rowIndex) << "\t";
        } else {
            std::cout << std::dec << static_cast<Vector<DictionaryContainer<T>> *>(vector)->GetValue(rowIndex) << "\t";
        }
    }

    template <type::DataTypeId typeId> static void PrintFlatVectorValue(BaseVector *vector, int32_t rowIndex)
    {
        using namespace omniruntime::type;
        using T = typename NativeType<typeId>::type;
        if constexpr (std::is_same_v<T, std::string_view>) {
            std::cout << std::dec << static_cast<Vector<LargeStringContainer<T>> *>(vector)->GetValue(rowIndex) << "\t";
        } else {
            std::cout << std::dec << static_cast<Vector<T> *>(vector)->GetValue(rowIndex) << "\t";
        }
    }

    static void PrintVectorValue(BaseVector *vector, int32_t rowIndex)
    {
        using namespace omniruntime::type;
        if (vector->IsNull(rowIndex)) {
            std::cout << "NULL"
                      << "\t";
            return;
        }

        if (vector->GetEncoding() != vec::OMNI_DICTIONARY) {
            DYNAMIC_TYPE_DISPATCH(PrintFlatVectorValue, vector->GetTypeId(), vector, rowIndex);
        } else {
            DYNAMIC_TYPE_DISPATCH(PrintDictionaryVectorValue, vector->GetTypeId(), vector, rowIndex);
        }
    }

    static BaseVector *CreateDictionaryVector(int *values, int valueSize, BaseVector *vector,
        int dataTypeId)
    {
        switch (dataTypeId) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return CreateDictionary(values, valueSize, reinterpret_cast<Vector<int32_t> *>(vector));
            }
            case type::OMNI_SHORT:
                return CreateDictionary(values, valueSize, reinterpret_cast<Vector<int16_t> *>(vector));
            case type::OMNI_LONG:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                return CreateDictionary(values, valueSize, reinterpret_cast<Vector<int64_t> *>(vector));
            }
            case type::OMNI_DECIMAL128: {
                return CreateDictionary(values, valueSize, reinterpret_cast<Vector<type::Decimal128> *>(vector));
            }
            case type::OMNI_DOUBLE: {
                return CreateDictionary(values, valueSize, reinterpret_cast<Vector<double> *>(vector));
            }
            case type::OMNI_BOOLEAN: {
                return CreateDictionary(values, valueSize, reinterpret_cast<Vector<bool> *>(vector));
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return CreateStringDictionary(values, valueSize,
                    reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector));
            }
            default: {
                LogError("No such data type %d", dataTypeId);
                break;
            }
        }
    }

    static void *UnsafeGetValuesDictionary(BaseVector *vector)
    {
        switch (vector->GetTypeId()) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetIds(
                    reinterpret_cast<Vector<DictionaryContainer<int32_t>> *>(vector)));
            }
            case type::OMNI_SHORT: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetIds(
                    reinterpret_cast<Vector<DictionaryContainer<int16_t>> *>(vector)));
            }
            case type::OMNI_LONG:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetIds(
                    reinterpret_cast<Vector<DictionaryContainer<int64_t>> *>(vector)));
            }
            case type::OMNI_DECIMAL128: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetIds(
                    reinterpret_cast<Vector<DictionaryContainer<type::Decimal128>> *>(vector)));
            }
            case type::OMNI_DOUBLE: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetIds(
                    reinterpret_cast<Vector<DictionaryContainer<double>> *>(vector)));
            }
            case type::OMNI_BOOLEAN: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetIds(
                    reinterpret_cast<Vector<DictionaryContainer<bool>> *>(vector)));
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetIds(
                    reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vector)));
            }
            default: {
                LogError("No such data type %d", vector->GetTypeId());
                break;
            }
        }
    }

    static void *UnsafeGetValues(BaseVector *vector)
    {
        if (vector->GetEncoding() == OMNI_DICTIONARY) {
            return UnsafeGetValuesDictionary(vector);
        }
        DataTypeId dataTypeId = vector->GetTypeId();
        switch (dataTypeId) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return reinterpret_cast<void *>(
                    unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int32_t> *>(vector)));
            }
            case type::OMNI_SHORT: {
                return reinterpret_cast<void *>(
                    unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int16_t> *>(vector)));
            }
            case type::OMNI_LONG:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                return reinterpret_cast<void *>(
                    unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int64_t> *>(vector)));
            }
            case type::OMNI_DECIMAL128: {
                return reinterpret_cast<void *>(
                    unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<type::Decimal128> *>(vector)));
            }
            case type::OMNI_DOUBLE: {
                return reinterpret_cast<void *>(
                    unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<double> *>(vector)));
            }
            case type::OMNI_BOOLEAN: {
                return reinterpret_cast<void *>(
                    unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<bool> *>(vector)));
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return reinterpret_cast<void *>(unsafe::UnsafeStringVector::GetValues(
                    reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)));
            }
            case type::OMNI_CONTAINER:
                return reinterpret_cast<void *>(
                    unsafe::UnsafeVector::GetRawValues(reinterpret_cast<ContainerVector *>(vector)));
            default: {
                LogError("No such data type %d", dataTypeId);
                return nullptr;
            }
        }
    }

    static void *UnsafeGetOffsetsAddr(BaseVector *vector)
    {
        DataTypeId dataTypeId = vector->GetTypeId();
        if (dataTypeId == type::OMNI_VARCHAR || dataTypeId == type::OMNI_CHAR) {
            if (vector->GetEncoding() == OMNI_DICTIONARY) {
                auto dictVarCharVec = reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector);
                return unsafe::UnsafeDictionaryVector::GetDictionaryOffsets(dictVarCharVec);
            } else {
                auto varCharVec = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
                return reinterpret_cast<void *>(unsafe::UnsafeStringVector::GetOffsets(varCharVec));
            }
        }
        return nullptr;
    }

    static BaseVector *SliceDictionaryVector(BaseVector *vector, int positionOffset,
        int length)
    {
        switch (vector->GetTypeId()) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return reinterpret_cast<Vector<DictionaryContainer<int32_t>> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_SHORT: {
                return reinterpret_cast<Vector<DictionaryContainer<int16_t>> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_LONG:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                return reinterpret_cast<Vector<DictionaryContainer<int64_t>> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_DECIMAL128: {
                return reinterpret_cast<Vector<DictionaryContainer<int32_t>> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_DOUBLE: {
                return reinterpret_cast<Vector<DictionaryContainer<double>> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_BOOLEAN: {
                return reinterpret_cast<Vector<DictionaryContainer<bool>> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return reinterpret_cast<Vector<DictionaryContainer<std::string_view>> *>(vector)->Slice(positionOffset,
                    length);
            }
            default: {
                LogError("No such data type %d", vector->GetTypeId());
                return nullptr;
            }
        }
    }
    static BaseVector *SliceVector(BaseVector *vector, int positionOffset, int length)
    {
        if (vector->GetEncoding() == OMNI_DICTIONARY) {
            return SliceDictionaryVector(vector, positionOffset, length);
        }
        DataTypeId dataTypeId = vector->GetTypeId();
        switch (dataTypeId) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return reinterpret_cast<Vector<int32_t> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_SHORT: {
                return reinterpret_cast<Vector<int16_t> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_LONG:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                return reinterpret_cast<Vector<int64_t> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_DECIMAL128: {
                return reinterpret_cast<Vector<type::Decimal128> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_DOUBLE: {
                return reinterpret_cast<Vector<double> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_BOOLEAN: {
                return reinterpret_cast<Vector<bool> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->Slice(positionOffset,
                    length);
            }
            case type::OMNI_CONTAINER:
                return reinterpret_cast<ContainerVector *>(vector)->Slice(positionOffset, length);
            default: {
                LogError("No such data type %d", dataTypeId);
                return nullptr;
            }
        }
    }

    static BaseVector *CopyPositionsDictionaryVector(BaseVector *vector, int *positions, int offset,
        int length)
    {
        DataTypeId dataTypeId = vector->GetTypeId();
        switch (dataTypeId) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return reinterpret_cast<Vector<DictionaryContainer<int32_t>> *>(vector)->CopyPositions(positions,
                    offset, length);
            }
            case type::OMNI_SHORT: {
                return reinterpret_cast<Vector<DictionaryContainer<int16_t>> *>(vector)->CopyPositions(positions,
                    offset, length);
            }
            case type::OMNI_LONG:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                return reinterpret_cast<Vector<DictionaryContainer<int64_t>> *>(vector)->CopyPositions(positions,
                    offset, length);
            }
            case type::OMNI_DECIMAL128: {
                return reinterpret_cast<Vector<DictionaryContainer<type::Decimal128>> *>(vector)->CopyPositions(
                    positions, offset, length);
            }
            case type::OMNI_DOUBLE: {
                return reinterpret_cast<Vector<DictionaryContainer<double>> *>(vector)->CopyPositions(positions, offset,
                    length);
            }
            case type::OMNI_BOOLEAN: {
                return reinterpret_cast<Vector<DictionaryContainer<bool>> *>(vector)->CopyPositions(positions, offset,
                    length);
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vector)
                    ->CopyPositions(positions, offset, length);
            }
            default: {
                LogError("No such data type %d", dataTypeId);
                break;
            }
        }
    }

    static BaseVector *CopyPositionsVector(BaseVector *vector, int *positions, int offset, int length)
    {
        if (vector->GetEncoding() == OMNI_DICTIONARY) {
            return CopyPositionsDictionaryVector(vector, positions, offset, length);
        }
        DataTypeId dataTypeId = vector->GetTypeId();
        switch (dataTypeId) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return reinterpret_cast<Vector<int32_t> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_SHORT: {
                return reinterpret_cast<Vector<int16_t> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_LONG:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                return reinterpret_cast<Vector<int64_t> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_DOUBLE: {
                return reinterpret_cast<Vector<double> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_BOOLEAN: {
                return reinterpret_cast<Vector<bool> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->CopyPositions(
                    positions, offset, length);
            }
            case type::OMNI_DECIMAL128: {
                return reinterpret_cast<Vector<type::Decimal128> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_CONTAINER:
                return reinterpret_cast<ContainerVector *>(vector)->CopyPositions(positions, offset, length);
            default: {
                LogError("No such data type %d", dataTypeId);
                return nullptr;
            }
        }
    }

    static void *UnsafeGetDictionary(BaseVector *vector)
    {
        DataTypeId dataTypeId = vector->GetTypeId();
        switch (dataTypeId) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetDictionary(
                    static_cast<Vector<DictionaryContainer<int32_t>> *>(vector)));
            }
            case type::OMNI_SHORT: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetDictionary(
                    static_cast<Vector<DictionaryContainer<int16_t>> *>(vector)));
            }
            case type::OMNI_LONG:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetDictionary(
                    static_cast<Vector<DictionaryContainer<int64_t>> *>(vector)));
            }
            case type::OMNI_DECIMAL128: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetDictionary(
                    static_cast<Vector<DictionaryContainer<type::Decimal128>> *>(vector)));
            }
            case type::OMNI_DOUBLE: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetDictionary(
                    static_cast<Vector<DictionaryContainer<double>> *>(vector)));
            }
            case type::OMNI_BOOLEAN: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetDictionary(
                    static_cast<Vector<DictionaryContainer<bool>> *>(vector)));
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetVarCharDictionary(
                    static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vector)));
            }

            default: {
                LogError("No such data type %d", dataTypeId);
                return nullptr;
            }
        }
    }

    static void AppendVector(BaseVector *destVector, int32_t offset, BaseVector *srcVector, int32_t length)
    {
        DataTypeId dataTypeId = destVector->GetTypeId();
        switch (dataTypeId) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                reinterpret_cast<Vector<int32_t> *>(destVector)->Append(srcVector, offset, length);
                break;
            }
            case type::OMNI_SHORT: {
                reinterpret_cast<Vector<int16_t> *>(destVector)->Append(srcVector, offset, length);
                break;
            }
            case type::OMNI_LONG:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                reinterpret_cast<Vector<int64_t> *>(destVector)->Append(srcVector, offset, length);
                break;
            }
            case type::OMNI_DECIMAL128: {
                reinterpret_cast<Vector<type::Decimal128> *>(destVector)->Append(srcVector, offset, length);
                break;
            }
            case type::OMNI_DOUBLE: {
                reinterpret_cast<Vector<double> *>(destVector)->Append(srcVector, offset, length);
                break;
            }
            case type::OMNI_BOOLEAN: {
                reinterpret_cast<Vector<bool> *>(destVector)->Append(srcVector, offset, length);
                break;
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(destVector)
                    ->Append(srcVector, offset, length);
                break;
            }
            case type::OMNI_CONTAINER:
                reinterpret_cast<ContainerVector *>(destVector)->Append(srcVector, offset, length);
                break;
            default: {
                LogError("No such data type %d", dataTypeId);
                break;
            }
        }
    }

    static ALWAYS_INLINE void AppendVectors(VectorBatch *vectorBatch, const type::DataTypes &sourceTypes,
        int64_t positionCount)
    {
        const std::vector<omniruntime::type::DataTypePtr> &types = sourceTypes.Get();
        for (int i = 0; i < sourceTypes.GetSize(); i++) {
            vectorBatch->Append(CreateVector(OMNI_FLAT, types[i]->GetId(), positionCount));
        }
    }

    static ALWAYS_INLINE void FreeVecBatches(std::vector<VectorBatch *> &vecBatches)
    {
        // free vectorBatch and vectors inside it.
        for (auto &vecBatch : vecBatches) {
            FreeVecBatch(vecBatch);
        }
    }

    static ALWAYS_INLINE void FreeVecBatch(VectorBatch *vecBatch)
    {
        vecBatch->FreeAllVectors();
        delete vecBatch;
    }
};
}

#endif // OMNI_RUNTIME_VECTOR_HELPER_H
