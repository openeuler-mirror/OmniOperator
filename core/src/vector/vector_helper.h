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
#include "omni_row.h"
#include "row_vector.h"
#include "map_vector.h"
#include "array_vector.h"
#include "util/type_util.h"

namespace omniruntime::vec {
class VectorHelper {
public:
    static RowBatch *TransRowBatchFromVectorBatch(VectorBatch *vecBatch)
    {
        int32_t vecCount = vecBatch->GetVectorCount();
        std::vector<type::DataTypeId> typeIds;
        std::vector<Encoding> encodings;
        for (int i = 0; i < vecCount; i++) {
            typeIds.push_back(vecBatch->Get(i)->GetTypeId());
            encodings.push_back(vecBatch->Get(i)->GetEncoding());
        }

        auto rowBuffer = std::make_unique<RowBuffer>(typeIds, encodings, typeIds.size() - 1);

        auto rowBatch = new RowBatch(vecBatch->GetRowCount());
        for (int32_t i = 0; i < vecBatch->GetRowCount(); ++i) {
            // 1.get value from vector batch
            rowBuffer->TransValueFromVectorBatch(vecBatch, i);

            // 2.generate one buffer of one row
            auto oneRowLen = rowBuffer->FillBuffer();

            // 3.set one row
            rowBatch->SetRow(i, new RowInfo(rowBuffer->TakeRowBuffer(), oneRowLen));
        }
        return rowBatch;
    }

    static BaseVector *CreateStringDictionary(const int32_t *values, int32_t valueSize,
        Vector<LargeStringContainer<std::string_view>> *vector)
    {
        auto nullsBuffer = std::make_unique<NullsBuffer>(valueSize);
        for (int i = 0; i < valueSize; i++) {
            if (UNLIKELY(vector->IsNull(values[i]))) {
                nullsBuffer->SetNull(i);
            }
        }

        // todo:: handing other types of container
        auto dictionary = std::make_shared<DictionaryContainer<std::string_view>>(values, valueSize,
            unsafe::UnsafeStringVector::GetContainer(
            reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)),
            vector->GetSize(), vector->GetOffset());
        return new Vector<DictionaryContainer<std::string_view>>(valueSize, dictionary, nullsBuffer.get(), OMNI_CHAR);
    }

    template <typename T> static BaseVector *CreateDictionary(int32_t *values, int32_t valueSize, Vector<T> *vector)
    {
        auto nullsBuffer = std::make_unique<NullsBuffer>(valueSize);
        for (int i = 0; i < valueSize; i++) {
            if (UNLIKELY(vector->IsNull(values[i]))) {
                nullsBuffer->SetNull(i);
            }
        }

        auto dictionary = std::make_shared<DictionaryContainer<T>>(values, valueSize,
            unsafe::UnsafeVector::GetValues<T>(vector), vector->GetSize(), vector->GetOffset());
        return new Vector<DictionaryContainer<T>>(valueSize, dictionary, nullsBuffer.get(), TYPE_ID<T>);
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
            std::string omniExceptionInfo =
                "In function CreateVector, no such encoding type " + std::to_string(vectorEncodingId);
            throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
        }
    }

    static BaseVector *CreateComplexVector(DataType* dataType, int32_t size)
    {
        using namespace omniruntime::type;
        auto fieldType = dataType->GetId();
        if (fieldType == OMNI_ROW) {
            std::vector<std::shared_ptr<BaseVector>> children;
            auto rowType = static_cast<RowType*>(dataType);
            for (int i = 0; i < rowType->Size(); i++) {
                children.push_back(std::shared_ptr<BaseVector>(CreateComplexVector(rowType->Type(i).get(), size)));
            }
            return new RowVector(size, children);
        } else if (fieldType == OMNI_MAP) {
            std::vector<std::shared_ptr<BaseVector>> children;
            auto mapType = static_cast<MapType*>(dataType);
            for (int i = 0; i < mapType->Size(); i++) {
                children.push_back(std::shared_ptr<BaseVector>(CreateComplexVector(mapType->Children()[i].get(), 0)));
            }
            return new MapVector(size, children[0], children[1]);
        } else if (fieldType == OMNI_ARRAY) {
            std::vector<std::shared_ptr<BaseVector>> children;
            auto arrayType = static_cast<ArrayType*>(dataType);
            for (int i = 0; i < arrayType->Size(); i++) {
                children.push_back(std::shared_ptr<BaseVector>(CreateComplexVector(arrayType->ElementType().get(), 0)));
            }
            return new ArrayVector(size, children[0]);
        } else {
            return CreateFlatVector(fieldType, size);
        }
    }

    static BaseVector *CreateEmptyComplexVector(DataType* dataType, int32_t size)
    {
        using namespace omniruntime::type;
        auto fieldType = dataType->GetId();
        if (fieldType == OMNI_ROW) {
            return new RowVector(size);
        } else if (fieldType == OMNI_MAP) {
            return new MapVector(size);
        } else if (fieldType == OMNI_ARRAY) {
            auto arrayType = static_cast<ArrayType*>(dataType);
            return new ArrayVector(size);
        } else {
            return CreateFlatVector(fieldType, size);
        }
    }

    static BaseVector *CreateFlatVector(int32_t dataTypeId, int32_t size, int32_t capacityInBytes = INITIAL_STRING_SIZE)
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

    static std::shared_ptr<BaseVector> CreateFlatVectorShared(int32_t dataTypeId, int32_t size,
        int32_t capacityInBytes = INITIAL_STRING_SIZE)
    {
        using namespace omniruntime::type;
        return DYNAMIC_TYPE_DISPATCH(CreateFlatVectorShared, dataTypeId, size, capacityInBytes);
    }

    template <type::DataTypeId typeId>
    static std::shared_ptr<BaseVector> CreateFlatVectorShared(int32_t size,
        int32_t capacityInBytes = INITIAL_STRING_SIZE)
    {
        using T = typename type::NativeType<typeId>::type;
        if constexpr (std::is_same_v<T, std::string_view>) {
            return std::make_shared<Vector<LargeStringContainer<std::string_view>>>(size, capacityInBytes);
        }
        return std::make_shared<Vector<T>>(size, typeId);
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

    static void PrintVec(BaseVector *vector, int32_t rowCount)
    {
        for (int32_t rowIdx = 0; rowIdx < rowCount; ++rowIdx) {
            PrintVectorValue(vector, rowIdx);
            std::cout << std::endl;
        }
    }

    template <type::DataTypeId typeId> static void PrintArrayElement(BaseVector* elementVec, int32_t index) {
        using namespace omniruntime::type;
        using T = typename NativeType<typeId>::type;
        if constexpr (std::is_same_v<T, std::string_view>) {
            std::cout << std::dec << static_cast<Vector<LargeStringContainer<T>> *>(elementVec)->GetValue(index) << "\t";
        } else {
            std::cout << std::dec << static_cast<Vector<T> *>(elementVec)->GetValue(index) << "\t";
        }
    }

    static void PrintStructVectorValue(BaseVector* vector, int32_t rowIndex) {
        auto* structVec = dynamic_cast<RowVector*>(vector);
        if (!structVec) {
            std::cout << "[Invalid StructVector]";
            return;
        }
        if (structVec->IsNull(rowIndex)) {
            std::cout << "NULL\t";
            return;
        }

        std::cout << "{";
        int32_t childSize = structVec->ChildSize();
        for (int32_t i = 0; i < childSize; i++) {
            if (i > 0) {
                std::cout << ", ";
            }
            auto childVec = structVec->ChildAt(i);
            PrintVectorValue(childVec.get(), rowIndex);
        }
        std::cout << "}";
    }

    static void PrintArrayVectorValue(BaseVector* vector, int32_t rowIndex) {
        auto* arrayVec = dynamic_cast<ArrayVector*>(vector);
        if (!arrayVec) {
            std::cout << "[Invalid ArrayVector]";
            return;
        }
        int32_t offset = arrayVec->GetOffset(rowIndex);
        int32_t size = arrayVec->GetSize(rowIndex);
        auto elementVec = arrayVec->GetElementVector();
        if (!elementVec) {
            std::cout << "[No Element Vector]";
            return;
        }
        const auto elementType = elementVec->GetTypeId();
        std::cout << "[";
        for (int i = 0; i < size; i++) {
            if (i > 0)
                std::cout << ", ";
            switch (elementType) {
                case OMNI_INT:
                    PrintArrayElement<OMNI_INT>(elementVec.get(), offset + i);
                    break;
                case OMNI_LONG:
                    PrintArrayElement<OMNI_LONG>(elementVec.get(), offset + i);
                    break;
                case OMNI_VARCHAR:
                    PrintArrayElement<OMNI_VARCHAR>(elementVec.get(), offset + i);
                    break;
                case OMNI_CHAR:
                    PrintArrayElement<OMNI_CHAR>(elementVec.get(), offset + i);
                    break;
                case OMNI_BOOLEAN:
                    PrintArrayElement<OMNI_BOOLEAN>(elementVec.get(), offset + i);
                    break;
                case OMNI_DOUBLE:
                    PrintArrayElement<OMNI_DOUBLE>(elementVec.get(), offset + i);
                    break;
                default:
                    std::cout << "?";
                    break;
            }
        }
        std::cout << "]";
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

    static void PrintArrayVectorOffsetsAndNulls(BaseVector* vector, int32_t rowIndex)
    {
        auto* arrayVec = dynamic_cast<ArrayVector*>(vector);
        if (!arrayVec) {
            throw omniruntime::exception::OmniException("RUNTIME_ERROR", "ArrayVector is null!");
        }
        int32_t offset = arrayVec->GetOffset(rowIndex);
        int32_t size = arrayVec->GetSize(rowIndex);
        std::cout << rowIndex << "--- offset: " << offset << "; size: " << size << "; isNull: " << arrayVec->IsNull(rowIndex) << std::endl;
    }

    template <type::DataTypeId typeId> static void PrintFlatVectorValue(BaseVector *vector, int32_t rowIndex)
    {
        if constexpr (typeId == OMNI_CONTAINER) {
            auto vec0 = reinterpret_cast<Vector<double> *>(static_cast<ContainerVector *>(vector)->GetValue(0));
            auto vec1 = reinterpret_cast<Vector<int64_t> *>(static_cast<ContainerVector *>(vector)->GetValue(1));
            std::cout << std::dec << vec0->GetValue(rowIndex) << "/" << vec1->GetValue(rowIndex) << "\t";
            return;
        }
        using namespace omniruntime::type;
        using T = typename NativeType<typeId>::type;
        if constexpr (std::is_same_v<T, std::string_view>) {
            std::cout << std::dec << static_cast<Vector<LargeStringContainer<T>> *>(vector)->GetValue(rowIndex) << "\t";
        } else {
            std::cout << std::dec << static_cast<Vector<T> *>(vector)->GetValue(rowIndex) << "\t";
        }
    }

    static void PrintMapVectorValue(MapVector *mapVec, int32_t rowIndex)
    {
        if (mapVec->IsNull(rowIndex)) {
            std::cout << "NULL\t";
            return;
        }

        auto offsets = mapVec->GetOffsets();
        uint32_t start = offsets[rowIndex];
        uint32_t end = offsets[rowIndex + 1];

        if (start == end) {
            std::cout << "{}\t";
            return;
        }
        std::cout << "{";
        auto keysVec = mapVec->GetKeyVector();
        auto valuesVec = mapVec->GetValueVector();

        for (uint32_t i = start; i < end; ++i) {
            if (i > start) {
                std::cout << ",";
            }
            PrintVectorValue(keysVec.get(), static_cast<int32_t>(i));
            std::cout << ":";
            PrintVectorValue(valuesVec.get(), static_cast<int32_t>(i));
        }
        std::cout << "}\t";
    }

    static void PrintVectorValue(BaseVector *vector, int32_t rowIndex)
    {
        using namespace omniruntime::type;
        if (vector->IsNull(rowIndex)) {
            std::cout << "NULL"
                      << "\t";
            return;
        }

        auto encoding = vector->GetEncoding();
        if (encoding == vec::OMNI_DICTIONARY) {
            DYNAMIC_TYPE_DISPATCH(PrintDictionaryVectorValue, vector->GetTypeId(), vector, rowIndex);
        } else if (encoding == vec::OMNI_ENCODING_CONTAINER) {
            auto vecCount = static_cast<ContainerVector *>(vector)->GetVectorCount();
            for (int32_t vecIdx = 0; vecIdx < vecCount; vecIdx++) {
                auto value = static_cast<ContainerVector *>(vector)->GetValue(vecIdx);
                auto valueVec = reinterpret_cast<BaseVector *>(value);
                DYNAMIC_TYPE_DISPATCH(PrintFlatVectorValue, valueVec->GetTypeId(), valueVec, rowIndex);
            }
        } else if (encoding == vec::OMNI_ENCODING_ARRAY) {
            PrintArrayVectorValue(vector, rowIndex);
        } else if (encoding == vec::OMNI_ENCODING_STRUCT) {
            PrintStructVectorValue(vector, rowIndex);
        } else if (encoding == vec::OMNI_ENCODING_MAP) {
            PrintMapVectorValue(static_cast<MapVector *>(vector), rowIndex);
        } else {
            DYNAMIC_TYPE_DISPATCH(PrintFlatVectorValue, vector->GetTypeId(), vector, rowIndex);
        }
    }

    static BaseVector *CreateDictionaryVector(int *values, int valueSize, BaseVector *vector, int dataTypeId)
    {
        switch (dataTypeId) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return CreateDictionary(values, valueSize, reinterpret_cast<Vector<int32_t> *>(vector));
            }
            case type::OMNI_SHORT:
                return CreateDictionary(values, valueSize, reinterpret_cast<Vector<int16_t> *>(vector));
            case type::OMNI_BYTE:
                return CreateDictionary(values, valueSize, reinterpret_cast<Vector<int8_t> *>(vector));
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
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
            case type::OMNI_FLOAT: {
                return CreateDictionary(values, valueSize, reinterpret_cast<Vector<float> *>(vector));
            }
            case type::OMNI_BOOLEAN: {
                return CreateDictionary(values, valueSize, reinterpret_cast<Vector<bool> *>(vector));
            }
            case type::OMNI_VARBINARY:
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return CreateStringDictionary(values, valueSize,
                    reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector));
            }
            default: {
                std::string omniExceptionInfo =
                    "In function CreateDictionaryVector, no such data type " + std::to_string(dataTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
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
            case type::OMNI_BYTE: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetIds(
                    reinterpret_cast<Vector<DictionaryContainer<int8_t>> *>(vector)));
            }
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
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
            case type::OMNI_FLOAT: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetIds(
                    reinterpret_cast<Vector<DictionaryContainer<float>> *>(vector)));
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
                std::string omniExceptionInfo =
                    "In function UnsafeGetValuesDictionary, no such data type " + std::to_string(vector->GetTypeId());
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
            }
        }
    }

    static BaseVector *CastConstVectorToVector(BaseVector *vector)
    {
        switch (vector->GetTypeId()) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                auto *col = new Vector<int32_t>(1);
                col->SetValue(0, reinterpret_cast<ConstVector<int32_t> *>(vector)->GetConstValue());
                delete vector;
                return col;
            }
            case type::OMNI_SHORT: {
                auto *col = new Vector<int16_t>(1);
                col->SetValue(0, reinterpret_cast<ConstVector<int16_t> *>(vector)->GetConstValue());
                delete vector;
                return col;
            }
            case type::OMNI_BYTE: {
                auto *col = new Vector<int8_t>(1);
                col->SetValue(0, reinterpret_cast<ConstVector<int8_t> *>(vector)->GetConstValue());
                delete vector;
                return col;
            }
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                auto *col = new Vector<int64_t>(1);
                col->SetValue(0, reinterpret_cast<ConstVector<int64_t> *>(vector)->GetConstValue());
                delete vector;
                return col;
            }
            case type::OMNI_DECIMAL128: {
                auto *col = new Vector<Decimal128>(1);
                col->SetValue(0, reinterpret_cast<ConstVector<Decimal128> *>(vector)->GetConstValue());
                delete vector;
                return col;
            }
            case type::OMNI_DOUBLE: {
                auto *col = new Vector<double>(1);
                col->SetValue(0, reinterpret_cast<ConstVector<double> *>(vector)->GetConstValue());
                delete vector;
                return col;
            }
            case type::OMNI_FLOAT: {
                auto *col = new Vector<float>(1);
                col->SetValue(0, reinterpret_cast<ConstVector<float> *>(vector)->GetConstValue());
                delete vector;
                return col;
            }
            case type::OMNI_BOOLEAN: {
                auto *col = new Vector<bool>(1);
                col->SetValue(0, reinterpret_cast<ConstVector<bool> *>(vector)->GetConstValue());
                delete vector;
                return col;
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                auto *col = new Vector<LargeStringContainer<std::string_view>>(1);
                auto tmp = std::string_view(reinterpret_cast<ConstVector<std::string> *>(vector)->GetConstValue());
                col->SetValue(0, tmp);
                return col;
            }
            default: {
                std::string omniExceptionInfo = "In function UnsafeGetValuesDictionary, no such data type " +
                    std::to_string(vector->GetTypeId());
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
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
            case type::OMNI_BYTE: {
                return reinterpret_cast<void *>(
                    unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<int8_t> *>(vector)));
            }
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
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
            case type::OMNI_FLOAT: {
                return reinterpret_cast<void *>(
                        unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<float> *>(vector)));
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
            case type::OMNI_ARRAY:
            case type::OMNI_MAP:
            case type::OMNI_ROW:
                return 0;
            default: {
                std::string omniExceptionInfo =
                    "In function UnsafeGetValues, no such data type " + std::to_string(dataTypeId);
                throw omniruntime::exception::OmniException("VECTOR_HELPER_ERROR", omniExceptionInfo);
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

    static BaseVector *SliceDictionaryVector(BaseVector *vector, int positionOffset, int length)
    {
        switch (vector->GetTypeId()) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return reinterpret_cast<Vector<DictionaryContainer<int32_t>> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_SHORT: {
                return reinterpret_cast<Vector<DictionaryContainer<int16_t>> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_BYTE: {
                return reinterpret_cast<Vector<DictionaryContainer<int8_t>> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
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
            case type::OMNI_FLOAT: {
                return reinterpret_cast<Vector<DictionaryContainer<float>> *>(vector)->Slice(positionOffset, length);
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
                std::string omniExceptionInfo =
                    "In function SliceDictionaryVector, no such data type " + std::to_string(vector->GetTypeId());
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
            }
        }
    }
    static BaseVector *SliceVector(BaseVector *vector, int positionOffset, int length)
    {
        auto encoding = vector->GetEncoding();
        if (encoding == OMNI_DICTIONARY) {
            return SliceDictionaryVector(vector, positionOffset, length);
        }
        if (encoding == OMNI_ENCODING_CONTAINER) {
            auto containerVector = reinterpret_cast<ContainerVector *>(vector);
            std::vector<int64_t> newFieldVecs;
            auto fieldCount = containerVector->GetVectorCount();
            for (int32_t i = 0; i < fieldCount; i++) {
                auto field = reinterpret_cast<BaseVector *>(containerVector->GetValue(i));
                auto newField = SliceVector(field, positionOffset, length);
                newFieldVecs.emplace_back(reinterpret_cast<int64_t>(newField));
            }
            return new ContainerVector(length, newFieldVecs, containerVector->GetDataTypes());
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
            case type::OMNI_BYTE: {
                return reinterpret_cast<Vector<int8_t> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
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
            case type::OMNI_FLOAT: {
                return reinterpret_cast<Vector<float> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_BOOLEAN: {
                return reinterpret_cast<Vector<bool> *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector)->Slice(positionOffset,
                    length);
            }
            case type::OMNI_ARRAY: {
                return reinterpret_cast<ArrayVector *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_MAP: {
                return reinterpret_cast<MapVector *>(vector)->Slice(positionOffset, length);
            }
            case type::OMNI_ROW: {
                return reinterpret_cast<RowVector *>(vector)->Slice(positionOffset, length);
            }
            default: {
                std::string omniExceptionInfo =
                    "In function SliceVector, no such data type " + std::to_string(dataTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
            }
        }
    }

    static BaseVector *CopyPositionsDictionaryVector(BaseVector *vector, int *positions, int offset, int length)
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
            case type::OMNI_BYTE: {
                return reinterpret_cast<Vector<DictionaryContainer<int8_t>> *>(vector)->CopyPositions(positions,
                    offset, length);
            }
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
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
            case type::OMNI_FLOAT: {
                return reinterpret_cast<Vector<DictionaryContainer<float>> *>(vector)->CopyPositions(positions, offset,
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
                std::string omniExceptionInfo =
                    "In fuction CopyPositionsDictionaryVector, no such data type " + std::to_string(dataTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
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
            case type::OMNI_BYTE: {
                return reinterpret_cast<Vector<int8_t> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                return reinterpret_cast<Vector<int64_t> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_DOUBLE: {
                return reinterpret_cast<Vector<double> *>(vector)->CopyPositions(positions, offset, length);
            }
            case type::OMNI_FLOAT: {
                return reinterpret_cast<Vector<float> *>(vector)->CopyPositions(positions, offset, length);
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
                std::string omniExceptionInfo =
                    "In function CopyPositionsVector, no such data type " + std::to_string(dataTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
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
            case type::OMNI_BYTE: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetDictionary(
                    static_cast<Vector<DictionaryContainer<int8_t>> *>(vector)));
            }
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
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
            case type::OMNI_FLOAT: {
                return reinterpret_cast<void *>(unsafe::UnsafeDictionaryVector::GetDictionary(
                    static_cast<Vector<DictionaryContainer<float>> *>(vector)));
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
                std::string omniExceptionInfo =
                    "In function UnsafeGetDictionary, no such data type " + std::to_string(dataTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
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
            case type::OMNI_BYTE: {
                reinterpret_cast<Vector<int8_t> *>(destVector)->Append(srcVector, offset, length);
                break;
            }
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
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
            case type::OMNI_FLOAT: {
                reinterpret_cast<Vector<float> *>(destVector)->Append(srcVector, offset, length);
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
            case type::OMNI_ARRAY:
                reinterpret_cast<ArrayVector *>(destVector)->Append(srcVector, offset, length);
                break;
            case type::OMNI_ROW:
                reinterpret_cast<RowVector *>(destVector)->Append(srcVector, offset, length);
                break;
            default: {
                std::string omniExceptionInfo =
                    "In function AppendVector, no such data type " + std::to_string(dataTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
            }
        }
    }

    static void ExpandElementVector(BaseVector *elementVec, type::DataTypeId typeId, int32_t newSize) {
        switch (typeId) {
            case type::OMNI_INT: {
                auto *vec = dynamic_cast<Vector<int32_t> *>(elementVec);
                vec->Expand(newSize);
                break;
            }
            case type::OMNI_LONG: {
                auto *vec = dynamic_cast<Vector<int64_t> *>(elementVec);
                vec->Expand(newSize);
                break;
            }
            case type::OMNI_DOUBLE: {
                auto *vec = dynamic_cast<Vector<double> *>(elementVec);
                vec->Expand(newSize);
                break;
            }
            case type::OMNI_BOOLEAN: {
                auto *vec = dynamic_cast<Vector<bool> *>(elementVec);
                vec->Expand(newSize);
                break;
            }
            case type::OMNI_CHAR:
            case type::OMNI_VARCHAR: {
                auto *vec = dynamic_cast<Vector<LargeStringContainer<std::string_view>> *>(elementVec);
                vec->Expand(newSize);
                break;
            }
            default:
                throw OmniException("ExpandElementVector", "Unsupported element type: " + std::to_string(typeId));
        }
    }

    static ALWAYS_INLINE void AppendVectors(VectorBatch *vectorBatch, const type::DataTypes &sourceTypes,
        int64_t positionCount)
    {
        const std::vector<omniruntime::type::DataTypePtr> &types = sourceTypes.Get();
        for (int i = 0; i < sourceTypes.GetSize(); i++) {
            vectorBatch->Append(CreateComplexVector(types[i].get(), positionCount));
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
        if (vecBatch == nullptr) {
            return;
        }
        vecBatch->FreeAllVectors();
        delete vecBatch;
    }

    template <typename T> static BaseVector *DecodeFlatDictionaryVector(BaseVector *vector)
    {
        int size = vector->GetSize();
        auto *flatVector = new Vector<T>(size);
        auto dicVector = reinterpret_cast<Vector<DictionaryContainer<T>> *>(vector);
        for (int i = 0; i < size; i++) {
            if (dicVector->IsNull(i)) {
                flatVector->SetNull(i);
            } else {
                flatVector->SetValue(i, dicVector->GetValue(i));
            }
        }
        return flatVector;
    }

    static BaseVector *DecodeVarcharDictionaryVector(BaseVector *vector)
    {
        using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
        int size = vector->GetSize();
        VarcharVector *flatVector = new VarcharVector(size);
        auto dicVector =
            reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vector);
        for (int i = 0; i < size; i++) {
            if (dicVector->IsNull(i)) {
                flatVector->SetNull(i);
            } else {
                std::string_view value = dicVector->GetValue(i);
                flatVector->SetValue(i, value);
            }
        }
        return flatVector;
    }

    static BaseVector *DecodeDictionaryVector(BaseVector *vector)
    {
        int32_t dataTypeId = vector->GetTypeId();
        switch (dataTypeId) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                return DecodeFlatDictionaryVector<int32_t>(vector);
            }
            case type::OMNI_SHORT: {
                return DecodeFlatDictionaryVector<int16_t>(vector);
            }
            case type::OMNI_BYTE: {
                return DecodeFlatDictionaryVector<int8_t>(vector);
            }
            case type::OMNI_LONG:
            case type::OMNI_TIMESTAMP:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64: {
                return DecodeFlatDictionaryVector<int64_t>(vector);
            }
            case type::OMNI_DECIMAL128: {
                return DecodeFlatDictionaryVector<type::Decimal128>(vector);
            }
            case type::OMNI_DOUBLE: {
                return DecodeFlatDictionaryVector<double>(vector);
            }
            case type::OMNI_FLOAT: {
                return DecodeFlatDictionaryVector<float>(vector);
            }
            case type::OMNI_BOOLEAN: {
                return DecodeFlatDictionaryVector<bool>(vector);
            }
            case type::OMNI_VARCHAR:
            case type::OMNI_CHAR: {
                return DecodeVarcharDictionaryVector(vector);
            }
            default: {
                std::string message("No such data type " + std::to_string(dataTypeId));
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", message);
            }
        }
    }

    template <typename T>
    static void CopyFlatVector(BaseVector *destVector, BaseVector *srcVector, int32_t offset, int32_t length)
    {
        // copy values
        auto destValues = unsafe::UnsafeVector::GetRawValues(static_cast<Vector<T> *>(destVector));
        auto srcValues = unsafe::UnsafeVector::GetRawValues(static_cast<Vector<T> *>(srcVector)) + offset;
        auto ret = memcpy_s(destValues, length * sizeof(T), srcValues, length * sizeof(T));
        if (ret != EOK) {
            std::string message("Values memory copy failed " + std::to_string(ret));
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", message);
        }
    }

    // used for copy from flat vector to another flat vector, and it does not support string types
    static void CopyFlatVector(BaseVector *destVector, BaseVector *srcVector, int32_t offset, int32_t length)
    {
        if (srcVector->GetEncoding() != OMNI_FLAT || destVector->GetEncoding() != OMNI_FLAT) {
            std::string message("Unsupported copy vector from or to a non flat vector.");
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", message);
        }

        auto destDataTypeId = destVector->GetTypeId();
        auto srcDataTypeId = srcVector->GetTypeId();
        if (destDataTypeId != srcDataTypeId) {
            std::string message("Can't copy from data type " + std::to_string(srcDataTypeId) + " to data type " +
                std::to_string(destDataTypeId));
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", message);
        }

        auto srcSize = srcVector->GetSize();
        if (UNLIKELY(offset + length > srcSize)) {
            std::string message("Copy vector out of src range(needed size:%d, real size:%d).", offset + length,
                srcSize);
            throw OmniException("OPERATOR_RUNTIME_ERROR", message);
        }

        auto destSize = destVector->GetSize();
        if (length > destSize) {
            std::string message("Can't copy since dest vector size " + std::to_string(destSize) +
                " is smaller than src length " + std::to_string(length));
            throw omniruntime::exception::OmniException("OPERATOR_RUNTIME_ERROR", message);
        }

        auto destNulls = unsafe::UnsafeBaseVector::GetNulls(destVector);
        auto srcNulls = unsafe::UnsafeBaseVector::GetNulls(srcVector);
        BitUtil::CopyBits(reinterpret_cast<uint64_t *>(srcNulls), offset, reinterpret_cast<uint64_t *>(destNulls), 0,
            length);
        switch (destDataTypeId) {
            case type::OMNI_INT:
            case type::OMNI_DATE32: {
                CopyFlatVector<int32_t>(destVector, srcVector, offset, length);
                break;
            }
            case type::OMNI_SHORT: {
                CopyFlatVector<int16_t>(destVector, srcVector, offset, length);
                break;
            }
            case type::OMNI_BYTE: {
                CopyFlatVector<int8_t>(destVector, srcVector, offset, length);
                break;
            }
            case type::OMNI_LONG:
            case type::OMNI_DATE64:
            case type::OMNI_DECIMAL64:
            case type::OMNI_TIMESTAMP: {
                CopyFlatVector<int64_t>(destVector, srcVector, offset, length);
                break;
            }
            case type::OMNI_DECIMAL128: {
                CopyFlatVector<Decimal128>(destVector, srcVector, offset, length);
                break;
            }
            case type::OMNI_DOUBLE: {
                CopyFlatVector<double>(destVector, srcVector, offset, length);
                break;
            }
            case type::OMNI_FLOAT: {
                CopyFlatVector<float>(destVector, srcVector, offset, length);
                break;
            }
            case type::OMNI_BOOLEAN: {
                CopyFlatVector<bool>(destVector, srcVector, offset, length);
                break;
            }
            default: {
                std::string message("Unsupported data type " + std::to_string(destDataTypeId));
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", message);
            }
        }
    }

    static DataTypePtr GetDataType(BaseVector *srcVector)
    {
        DataTypeId dataTypeId = srcVector->GetTypeId();
        switch (dataTypeId) {
            case type::OMNI_INT: {
                return IntType();
            }
            case type::OMNI_DATE32: {
                return Date32Type();
            }
            case type::OMNI_SHORT: {
                return ShortType();
            }
            case type::OMNI_BYTE: {
                return ByteType();
            }
            case type::OMNI_LONG: {
                return LongType();
            }
            case type::OMNI_TIMESTAMP: {
                return TimestampType();
            }
            case type::OMNI_DATE64: {
                return Date64Type();
            }
            case type::OMNI_DECIMAL64: {
                return Decimal64Type();
            }
            case type::OMNI_DECIMAL128: {
                return Decimal128Type();
            }
            case type::OMNI_DOUBLE: {
                return DoubleType();
            }
            case type::OMNI_FLOAT: {
                return FloatType();
            }
            case type::OMNI_BOOLEAN: {
                return BooleanType();
            }
            case type::OMNI_VARCHAR: {
                return VarcharType();
            }
            case type::OMNI_CHAR: {
                return CharType();
            }
            case type::OMNI_CONTAINER: {
                return ContainerType();
            }
            case type::OMNI_ROW: {
                auto rowVector = static_cast<RowVector *>(srcVector);
                std::vector<DataTypePtr> fieldTypes;
                for (int i = 0; i < rowVector->ChildSize(); i++) {
                    fieldTypes.push_back(GetDataType(rowVector->ChildAt(i).get()));
                }
                return RowDataType(fieldTypes);
            }
            case type::OMNI_ARRAY: {
                auto arrayVector = static_cast<ArrayVector *>(srcVector);
                return ArrayDataType(GetDataType(arrayVector->GetElementVector().get()));
            }
            case type::OMNI_MAP: {
                auto mapVector = static_cast<MapVector *>(srcVector);
                return MapDataType(GetDataType(mapVector->GetKeyVector().get()),
                    GetDataType(mapVector->GetValueVector().get()));
            }
            default: {
                std::string omniExceptionInfo =
                    "In function GetDataType, no such data type " + std::to_string(dataTypeId);
                throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
            }
        }
    }

    static void EmptyArrayProjection(ArrayVector *dstArrVec, DataTypeId typeId)
    {
        if (dstArrVec == nullptr) {
            return;
        }
        auto emptyElemVec = VectorHelper::CreateFlatVector(static_cast<int32_t>(typeId), 0);
        dstArrVec->SetElementVector(std::shared_ptr<BaseVector>(emptyElemVec));
    }
};
}

#endif // OMNI_RUNTIME_VECTOR_HELPER_H
