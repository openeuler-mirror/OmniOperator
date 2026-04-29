/**
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ParquetWriter.h"
#include "ParquetReader.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/data.h"
#include <arrow/type.h>
#include <arrow/array.h>
#include <arrow/api.h>
#include "arrow/util/bitmap.h"
#include "arrow/chunked_array.h"
#include "arrow/buffer_builder.h"
#include "arrow/table.h"
#include "reader/arrowadapter/FileSystemAdapter.h"
#include "reader/common/UriInfo.h"
#include "reader/common/Directories.h"
#include "parquet/arrow/reader.h"
#include "parquet/exception.h"
#include "parquet/properties.h"
#include <sys/stat.h>
#include <mutex>
#include <unordered_map>
#include <iostream>
#include <codecvt>
#include <locale>
#include <limits>
#include <exception>
#include <stdexcept>
#include <stdio.h>
#include <cerrno>
#include <cstring>

using namespace arrow;
using namespace arrow::internal;
using namespace parquet::arrow;
using namespace omniruntime::writer;
using namespace omniruntime::reader;

static std::mutex mutex_;

namespace omniruntime::writer
{

    arrow::Status ParquetWriter::InitRecordWriter(UriInfo &uri, std::string &ugi)
    {
        parquet::WriterProperties::Builder writer_properties;
        parquet::ArrowWriterProperties::Builder arrow_writer_properties;

        arrow::Status result;
        Filesystem *fs = GetFileSystemPtr(uri, ugi, result);
        if (fs == nullptr || fs->filesys_ptr == nullptr) {
            return arrow::Status::IOError(result);
        }

        std::string uriPath = uri.ToString();
        std::string path = uri.Path();
        if (uri.Scheme() == UriInfo::LOCAL_FILE) {
            std::string dirPath = common::getParentPath(path);
            if (common::createDirectories(dirPath) != 0) {
                OMNI_FAIL("Create local directories fail, path: {}, err msg: {}", dirPath, strerror(errno));
            }
        }
        std::shared_ptr<io::OutputStream> outputStream;
        ARROW_ASSIGN_OR_RAISE(outputStream, fs->filesys_ptr->OpenOutputStream(path));

        writer_properties.disable_dictionary();
        auto fileWriterResult = FileWriter::Open(
            *schema_, arrow::default_memory_pool(), outputStream,
            writer_properties.build(), parquet::default_arrow_writer_properties());
        if (!fileWriterResult.ok()) {
            throw std::runtime_error("Error opening file writer" + fileWriterResult.status().ToString());
            return fileWriterResult.status();
        }
        ARROW_ASSIGN_OR_RAISE(arrow_writer, fileWriterResult);
        return arrow::Status::OK();
    }

    std::shared_ptr<::arrow::ChunkedArray> buildBooleanChunk(DataTypeId typeId, BaseVector *baseVector,
                                                             bool isSplitWrite = false, long startPos  = 0,
                                                             long endPos = 0)
    {
        using T = typename NativeType<OMNI_BOOLEAN>::type;
        auto vector = (Vector<T> *)baseVector;

        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }

        int64_t vectorSize = endPos - startPos;
        bool values[vectorSize];
        int64_t index = 0;
        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);

        if (vector->HasNull()) {
            for (long j = startPos; j < endPos; j++) {
                if (vector->IsNull(j)) {
                    bitmap.SetBitTo(index, false);
                } else if(isSplitWrite) {
                    values[index] = vector->GetValue(j);
                }
                index++;
            }
        } else if (isSplitWrite) {
              for (long j = startPos; j < endPos; j++) {
                   values[index] = vector->GetValue(j);
                   index++;
              }
        }

        TypedBufferBuilder<bool> builder;
        builder.Resize(vectorSize);

        builder.Append(reinterpret_cast<uint8_t *>(isSplitWrite?values:VectorHelper::UnsafeGetValues(vector)), vectorSize);
        std::shared_ptr<arrow::Buffer> databuffer = *(builder.Finish());

        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(databuffer);

        auto booleanType = std::make_shared<arrow::BooleanType>();
        auto arrayData = arrow::ArrayData::Make(booleanType, vectorSize, buffers);

        std::vector<std::shared_ptr<arrow::Array>> arrayVector;
        auto booleanArray = std::make_shared<arrow::BooleanArray>(arrayData);
        arrayVector.emplace_back(booleanArray);

        return arrow::ChunkedArray::Make(arrayVector, booleanType).ValueOrDie();
    }

    template<DataTypeId Type_ID, typename ArrowType, typename ChunkType>
    std::shared_ptr<::arrow::ChunkedArray> buildChunk(BaseVector *baseVector,
                                                           bool isSplitWrite = false, long startPos  = 0,
                                                           long endPos = 0)
    {
        using T=typename NativeType<Type_ID>::type;
        auto vector =static_cast<Vector<T> *>(baseVector);
        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }
        int64_t vectorSize = endPos - startPos;
        ChunkType values[vectorSize];
        int64_t index = 0;

        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);
        if (vector->HasNull()) {
            for (long j = startPos; j < endPos; j++) {
                if (vector->IsNull(j)) {
                    bitmap.SetBitTo(index, false);
                } else if (isSplitWrite) {
                    values[index] = vector->GetValue(j);
                }
                index++;
            }
        } else if (isSplitWrite) {
               for (long j = startPos; j < endPos; j++) {
                    values[index] = vector->GetValue(j);
                    index++;
               }
        }

        TypedBufferBuilder<ChunkType> builder;
        builder.Resize(vectorSize);
        builder.Append(reinterpret_cast<T *>(isSplitWrite?values:VectorHelper::UnsafeGetValues(vector)), vectorSize);
        auto dataBuffer = *builder.Finish();
        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(dataBuffer);

        auto arrowType = std::make_shared<ArrowType>();
        auto arrayData = arrow::ArrayData::Make(arrowType, vectorSize, buffers);
        std::vector<std::shared_ptr<arrow::Array>> arrayVector;
        auto arrowArray = std::make_shared<NumericArray<ArrowType>>(arrayData);
        arrayVector.emplace_back(arrowArray);
        return ChunkedArray::Make(arrayVector, arrowType).ValueOrDie();
    }

    std::shared_ptr<ChunkedArray> buildVarcharChunk(DataTypeId typeId, BaseVector *baseVector,
                                                    bool isSplitWrite = false, long startPos  = 0,
                                                    long endPos = 0)
    {
        auto vector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(baseVector);

        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }

        int64_t vectorSize = endPos - startPos;
        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);

        TypedBufferBuilder<int32_t> offsetsBuilder;
        TypedBufferBuilder<char> valuesBuilder;
        int32_t currentOffset = 0;
        offsetsBuilder.Append(0);
        valuesBuilder.Resize(vectorSize);

        int64_t index = 0;
        for (long j = startPos; j < endPos; j++) {
            if (vector->IsNull(j)) {
                bitmap.SetBitTo(index, false);
            }
            index++;
            std::string strValue = std::string(vector->GetValue(j));
            size_t length = strValue.length();
            currentOffset += length;
            offsetsBuilder.Append(currentOffset);
            valuesBuilder.Append(strValue.data(), length);
        }

        auto offsetsBuffer = offsetsBuilder.Finish().ValueOrDie();
        auto valuesBuffer = valuesBuilder.Finish().ValueOrDie();

        std::vector<std::shared_ptr<Buffer>> buffers;

        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(offsetsBuffer);
        buffers.emplace_back(valuesBuffer);

        auto utf8Type = std::make_shared<arrow::StringType>();
        auto arrayData = arrow::ArrayData::Make(utf8Type, vectorSize, buffers);

        std::vector<std::shared_ptr<Array>> arrayVector;
        auto stringArray = std::make_shared<StringArray>(arrayData);
        arrayVector.emplace_back(stringArray);

        return ChunkedArray::Make(arrayVector, utf8Type).ValueOrDie();
    }

    std::shared_ptr<ChunkedArray> buildStringChunk(BaseVector *baseVector,
                                                   const std::shared_ptr<arrow::DataType> &arrowType,
                                                   bool isSplitWrite = false, long startPos  = 0,
                                                   long endPos = 0)
    {
        auto vector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(baseVector);
        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }

        int64_t vectorSize = endPos - startPos;
        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);

        const auto typeId = arrowType ? arrowType->id() : arrow::Type::STRING;
        const bool useLargeOffsets = (typeId == arrow::Type::LARGE_STRING);
        std::vector<std::shared_ptr<Buffer>> buffers;
        std::vector<std::shared_ptr<Array>> arrayVector;

        if (useLargeOffsets) {
            TypedBufferBuilder<int64_t> offsetsBuilder;
            TypedBufferBuilder<uint8_t> valuesBuilder;
            int64_t currentOffset = 0;
            offsetsBuilder.Append(0);

            int64_t index = 0;
            for (long j = startPos; j < endPos; j++) {
                if (vector->IsNull(j)) {
                    bitmap.SetBitTo(index, false);
                } else {
                    auto value = vector->GetValue(j);
                    auto length = static_cast<int64_t>(value.size());
                    currentOffset += length;
                    valuesBuilder.Append(reinterpret_cast<const uint8_t *>(value.data()), length);
                }
                offsetsBuilder.Append(currentOffset);
                index++;
            }

            auto offsetsBuffer = offsetsBuilder.Finish().ValueOrDie();
            auto valuesBuffer = valuesBuilder.Finish().ValueOrDie();
            buffers.emplace_back(bitmapBuffer);
            buffers.emplace_back(offsetsBuffer);
            buffers.emplace_back(valuesBuffer);

            auto utf8Type = std::make_shared<arrow::LargeStringType>();
            auto arrayData = arrow::ArrayData::Make(utf8Type, vectorSize, buffers);
            auto stringArray = std::make_shared<arrow::LargeStringArray>(arrayData);
            arrayVector.emplace_back(stringArray);
            return ChunkedArray::Make(arrayVector, utf8Type).ValueOrDie();
        }

        TypedBufferBuilder<int32_t> offsetsBuilder;
        TypedBufferBuilder<uint8_t> valuesBuilder;
        int32_t currentOffset = 0;
        offsetsBuilder.Append(0);

        int64_t index = 0;
        for (long j = startPos; j < endPos; j++) {
            if (vector->IsNull(j)) {
                bitmap.SetBitTo(index, false);
            } else {
                auto value = vector->GetValue(j);
                auto length = static_cast<int32_t>(value.size());
                currentOffset += length;
                valuesBuilder.Append(reinterpret_cast<const uint8_t *>(value.data()), length);
            }
            offsetsBuilder.Append(currentOffset);
            index++;
        }

        auto offsetsBuffer = offsetsBuilder.Finish().ValueOrDie();
        auto valuesBuffer = valuesBuilder.Finish().ValueOrDie();
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(offsetsBuffer);
        buffers.emplace_back(valuesBuffer);

        auto utf8Type = std::make_shared<arrow::StringType>();
        auto arrayData = arrow::ArrayData::Make(utf8Type, vectorSize, buffers);
        auto stringArray = std::make_shared<arrow::StringArray>(arrayData);
        arrayVector.emplace_back(stringArray);
        return ChunkedArray::Make(arrayVector, utf8Type).ValueOrDie();
    }

    std::shared_ptr<ChunkedArray> buildBinaryChunk(BaseVector *baseVector,
                                                   const std::shared_ptr<arrow::DataType> &arrowType,
                                                   bool isSplitWrite = false, long startPos  = 0,
                                                   long endPos = 0)
    {
        auto vector = static_cast<Vector<LargeStringContainer<std::string_view>> *>(baseVector);
        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }

        int64_t vectorSize = endPos - startPos;
        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);

        const auto typeId = arrowType ? arrowType->id() : arrow::Type::BINARY;
        const bool useLargeOffsets = (typeId == arrow::Type::LARGE_BINARY);
        std::vector<std::shared_ptr<Buffer>> buffers;
        std::vector<std::shared_ptr<Array>> arrayVector;

        if (useLargeOffsets) {
            TypedBufferBuilder<int64_t> offsetsBuilder;
            TypedBufferBuilder<uint8_t> valuesBuilder;
            int64_t currentOffset = 0;
            offsetsBuilder.Append(0);

            int64_t index = 0;
            for (long j = startPos; j < endPos; j++) {
                if (vector->IsNull(j)) {
                    bitmap.SetBitTo(index, false);
                } else {
                    auto value = vector->GetValue(j);
                    auto length = static_cast<int64_t>(value.size());
                    currentOffset += length;
                    valuesBuilder.Append(reinterpret_cast<const uint8_t *>(value.data()), length);
                }
                offsetsBuilder.Append(currentOffset);
                index++;
            }

            auto offsetsBuffer = offsetsBuilder.Finish().ValueOrDie();
            auto valuesBuffer = valuesBuilder.Finish().ValueOrDie();
            buffers.emplace_back(bitmapBuffer);
            buffers.emplace_back(offsetsBuffer);
            buffers.emplace_back(valuesBuffer);

            auto binaryType = std::make_shared<arrow::LargeBinaryType>();
            auto arrayData = arrow::ArrayData::Make(binaryType, vectorSize, buffers);
            auto binaryArray = std::make_shared<arrow::LargeBinaryArray>(arrayData);
            arrayVector.emplace_back(binaryArray);
            return ChunkedArray::Make(arrayVector, binaryType).ValueOrDie();
        }

        TypedBufferBuilder<int32_t> offsetsBuilder;
        TypedBufferBuilder<uint8_t> valuesBuilder;
        int32_t currentOffset = 0;
        offsetsBuilder.Append(0);

        int64_t index = 0;
        for (long j = startPos; j < endPos; j++) {
            if (vector->IsNull(j)) {
                bitmap.SetBitTo(index, false);
            } else {
                auto value = vector->GetValue(j);
                auto length = static_cast<int32_t>(value.size());
                currentOffset += length;
                valuesBuilder.Append(reinterpret_cast<const uint8_t *>(value.data()), length);
            }
            offsetsBuilder.Append(currentOffset);
            index++;
        }

        auto offsetsBuffer = offsetsBuilder.Finish().ValueOrDie();
        auto valuesBuffer = valuesBuilder.Finish().ValueOrDie();
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(offsetsBuffer);
        buffers.emplace_back(valuesBuffer);

        auto binaryType = std::make_shared<arrow::BinaryType>();
        auto arrayData = arrow::ArrayData::Make(binaryType, vectorSize, buffers);
        auto binaryArray = std::make_shared<arrow::BinaryArray>(arrayData);
        arrayVector.emplace_back(binaryArray);
        return ChunkedArray::Make(arrayVector, binaryType).ValueOrDie();
    }

    std::shared_ptr<arrow::ChunkedArray> buildDecimal64Chunk(DataTypeId typeId, BaseVector *baseVector, 
                                                            int precision, int scale, bool isSplitWrite = false, 
                                                            long startPos  = 0, long endPos = 0)
    {
        using T = typename NativeType<OMNI_DECIMAL64>::type;
        auto vector = (Vector<T> *)baseVector;

        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }
        int64_t vectorSize = endPos - startPos;
        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);
        BufferBuilder builder;
        builder.Resize(vectorSize);
        std::vector<arrow::Decimal128> decimalArray;

        int64_t index = 0;
        for (long j = startPos; j < endPos; j++) {
             BasicDecimal128 basicDecimal128(0, vector->GetValue(j));
             decimalArray.emplace_back(BasicDecimal128(basicDecimal128));
            if (vector->IsNull(j)) {
                bitmap.SetBitTo(index, false);
            }
            index++;
        }

        builder.Append(decimalArray.data(), decimalArray.size() * sizeof(arrow::Decimal128));
         auto dataBuffer = *builder.Finish();
        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(dataBuffer);

        auto decimal128Type = std::make_shared<arrow::Decimal128Type>(precision, scale);
        auto arrayData = arrow::ArrayData::Make(decimal128Type, vectorSize, buffers);
        std::vector<std::shared_ptr<Array>> arrayVector;
        auto decimal128Array = std::make_shared<Decimal128Array>(arrayData);
        arrayVector.emplace_back(decimal128Array);
        return ChunkedArray::Make(arrayVector, decimal128Type).ValueOrDie();
    }

    std::shared_ptr<ChunkedArray> buildDecimal128Chunk(DataTypeId typeId, BaseVector *baseVector,
                                                      int precision, int scale, bool isSplitWrite = false,
                                                      long startPos  = 0, long endPos = 0)
    {
        using T = typename NativeType<OMNI_DECIMAL128>::type;
        auto vector = (Vector<T> *)baseVector;

        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }
        int64_t vectorSize = endPos - startPos;
        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);
        BufferBuilder builder;
        builder.Resize(vectorSize);
        std::vector<arrow::Decimal128> decimalArray;

        int64_t index = 0;
        for (long j = startPos; j < endPos; j++) {
            auto decimalValue = vector->GetValue(j);
            BasicDecimal128 basicDecimal128(vector->GetValue(j).HighBits(), vector->GetValue(j).LowBits());
            decimalArray.emplace_back(BasicDecimal128(basicDecimal128));
            if (vector->IsNull(j)) {
                bitmap.SetBitTo(index, false);
            }
            index++;
        }

        builder.Append(decimalArray.data(), decimalArray.size() * sizeof(arrow::Decimal128));
        auto dataBuffer = *builder.Finish();
        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(dataBuffer);

        auto decimal128Type = std::make_shared<arrow::Decimal128Type>(precision, scale);
        auto arrayData = arrow::ArrayData::Make(decimal128Type, vectorSize, buffers);
        std::vector<std::shared_ptr<Array>> arrayVector;
        auto decimal128Array = std::make_shared<Decimal128Array>(arrayData);
        arrayVector.emplace_back(decimal128Array);
        return ChunkedArray::Make(arrayVector, decimal128Type).ValueOrDie();
    }

    std::shared_ptr<ChunkedArray> buildTimestampChunk(BaseVector *baseVector,
                                                      const std::shared_ptr<arrow::DataType> &arrowType,
                                                      bool isSplitWrite = false, long startPos = 0,
                                                      long endPos = 0)
    {
        using T = typename NativeType<OMNI_TIMESTAMP>::type;
        auto vector = static_cast<Vector<T> *>(baseVector);

        if (!isSplitWrite) {
            startPos = 0;
            endPos = vector->GetSize();
        }
        int64_t vectorSize = endPos - startPos;
        int64_t values[vectorSize];
        int64_t index = 0;

        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);
        if (vector->HasNull()) {
            for (long j = startPos; j < endPos; j++) {
                if (vector->IsNull(j)) {
                    bitmap.SetBitTo(index, false);
                } else if (isSplitWrite) {
                    values[index] = vector->GetValue(j);
                }
                index++;
            }
        } else if (isSplitWrite) {
            for (long j = startPos; j < endPos; j++) {
                values[index] = vector->GetValue(j);
                index++;
            }
        }

        TypedBufferBuilder<int64_t> builder;
        builder.Resize(vectorSize);
        builder.Append(reinterpret_cast<int64_t *>(isSplitWrite ? values : VectorHelper::UnsafeGetValues(vector)), vectorSize);
        auto dataBuffer = *builder.Finish();
        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(dataBuffer);

        std::shared_ptr<arrow::DataType> resolvedType = arrowType;
        if (!resolvedType || resolvedType->id() != arrow::Type::TIMESTAMP) {
            resolvedType = arrow::timestamp(arrow::TimeUnit::MICRO);
        }
        auto arrayData = arrow::ArrayData::Make(resolvedType, vectorSize, buffers);
        std::vector<std::shared_ptr<Array>> arrayVector;
        auto timestampArray = std::make_shared<arrow::TimestampArray>(arrayData);
        arrayVector.emplace_back(timestampArray);
        return ChunkedArray::Make(arrayVector, resolvedType).ValueOrDie();
    }

    // construct arrow::Array recursively for nest type
    std::shared_ptr<arrow::Array> buildArrayForType(DataTypeId typeId, BaseVector *baseVector,
                                                    const std::shared_ptr<arrow::DataType> &arrowType,
                                                    bool isSplitWrite, long startPos, long endPos);

    // convert omni type to arrow type recursively for nest type
    std::shared_ptr<arrow::DataType> inferArrowType(DataTypeId typeId, BaseVector *baseVector)
    {
        switch (typeId) {
            case OMNI_BOOLEAN:
                return arrow::boolean();
            case OMNI_SHORT:
                return arrow::int16();
            case OMNI_INT:
                return arrow::int32();
            case OMNI_LONG:
                return arrow::int64();
            case OMNI_BYTE:
                return arrow::int8();
            case OMNI_DATE32:
                return arrow::date32();
            case OMNI_DATE64:
                return arrow::date64();
            case OMNI_DOUBLE:
                return arrow::float64();
            case OMNI_FLOAT:
                return arrow::float32();
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                return arrow::utf8();
            case OMNI_VARBINARY:
                return arrow::binary();
            case OMNI_TIMESTAMP:
                return arrow::timestamp(arrow::TimeUnit::MICRO);
            case OMNI_DECIMAL64:
                return arrow::decimal128(omniruntime::type::DECIMAL64_DEFAULT_PRECISION,
                                         omniruntime::type::DECIMAL64_DEFAULT_SCALE);
            case OMNI_DECIMAL128:
                return arrow::decimal128(omniruntime::type::DECIMAL128_DEFAULT_PRECISION,
                                         omniruntime::type::DECIMAL128_DEFAULT_SCALE);
            case OMNI_ARRAY: {
                auto arrayVector = static_cast<omniruntime::vec::ArrayVector *>(baseVector);
                auto elementType = inferArrowType(arrayVector->GetElementVector()->GetTypeId(),
                                                  arrayVector->GetElementVector().get());
                return arrow::list(arrow::field("element", elementType, true));
            }
            case OMNI_MAP: {
                auto mapVector = static_cast<MapVector *>(baseVector);
                auto keyType = inferArrowType(mapVector->GetKeyVector()->GetTypeId(), mapVector->GetKeyVector().get());
                auto valueType = inferArrowType(mapVector->GetValueVector()->GetTypeId(), mapVector->GetValueVector().get());
                return arrow::map(keyType, valueType, true);
            }
            case OMNI_ROW: {
                auto rowVector = static_cast<RowVector *>(baseVector);
                std::vector<std::shared_ptr<arrow::Field>> fields;
                auto childCount = rowVector->ChildSize();
                fields.reserve(childCount);
                for (int i = 0; i < childCount; i++) {
                    auto childVector = rowVector->ChildAt(i);
                    auto childType = inferArrowType(childVector->GetTypeId(), childVector.get());
                    fields.emplace_back(arrow::field("field_" + std::to_string(i), childType, true));
                }
                return arrow::struct_(fields);
            }
            default:
                throw std::runtime_error("Parquet write error : Cannot infer arrow type for omni type: " + std::to_string(typeId));
        }
    }

    std::shared_ptr<arrow::Array> buildListArray(BaseVector *baseVector,
                                                 const std::shared_ptr<arrow::DataType> &arrowType,
                                                 bool isSplitWrite, long startPos, long endPos)
    {
        auto arrayVector = static_cast<omniruntime::vec::ArrayVector *>(baseVector);
        if (!isSplitWrite) {
            startPos = 0;
            endPos = arrayVector->GetSize();
        }
        int64_t vectorSize = endPos - startPos;
        auto offsets = arrayVector->GetOffsets();
        int64_t startOffset = offsets[startPos];
        int64_t endOffset = offsets[endPos];

        std::shared_ptr<arrow::DataType> resolvedType = arrowType;
        if (!resolvedType || (resolvedType->id() != arrow::Type::LIST && resolvedType->id() != arrow::Type::LARGE_LIST)) {
            resolvedType = inferArrowType(OMNI_ARRAY, baseVector);
        }

        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);
        if (arrayVector->HasNull()) {
            int64_t index = 0;
            for (long j = startPos; j < endPos; j++) {
                if (arrayVector->IsNull(j)) {
                    bitmap.SetBitTo(index, false);
                }
                index++;
            }
        }

        const bool useLargeOffsets = (resolvedType->id() == arrow::Type::LARGE_LIST);
        std::shared_ptr<arrow::Buffer> offsetsBuffer;
        if (useLargeOffsets) {
            TypedBufferBuilder<int64_t> offsetsBuilder;
            for (int64_t i = 0; i <= vectorSize; i++) {
                offsetsBuilder.Append(offsets[startPos + i] - startOffset);
            }
            offsetsBuffer = offsetsBuilder.Finish().ValueOrDie();
        } else {
            TypedBufferBuilder<int32_t> offsetsBuilder;
            for (int64_t i = 0; i <= vectorSize; i++) {
                int64_t value = offsets[startPos + i] - startOffset;
                if (value > std::numeric_limits<int32_t>::max()) {
                    throw std::runtime_error("Array offset exceeds int32 range");
                }
                offsetsBuilder.Append(static_cast<int32_t>(value));
            }
            offsetsBuffer = offsetsBuilder.Finish().ValueOrDie();
        }

        auto elementVector = arrayVector->GetElementVector();
        auto elementType = resolvedType->id() == arrow::Type::LARGE_LIST
            ? std::static_pointer_cast<arrow::LargeListType>(resolvedType)->value_type()
            : std::static_pointer_cast<arrow::ListType>(resolvedType)->value_type();
        auto elementArray = buildArrayForType(elementVector->GetTypeId(), elementVector.get(), elementType, true,
                                              startOffset, endOffset);

        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(offsetsBuffer);
        auto arrayData = arrow::ArrayData::Make(resolvedType, vectorSize, buffers, {elementArray->data()});
        return arrow::MakeArray(arrayData);
    }

    std::shared_ptr<arrow::Array> buildMapArray(BaseVector *baseVector,
                                                const std::shared_ptr<arrow::DataType> &arrowType,
                                                bool isSplitWrite, long startPos, long endPos)
    {
        if (!isSplitWrite) {
            startPos = 0;
            endPos = baseVector->GetSize();
        }
        auto mapVector = static_cast<MapVector *>(baseVector);
        int64_t vectorSize = endPos - startPos;
        auto offsets = mapVector->GetOffsets();
        int64_t startOffset = offsets[startPos];
        int64_t endOffset = offsets[endPos];

        std::shared_ptr<arrow::DataType> resolvedType = arrowType;
        if (!resolvedType || resolvedType->id() != arrow::Type::MAP) {
            resolvedType = inferArrowType(OMNI_MAP, baseVector);
        }
        auto mapType = std::static_pointer_cast<arrow::MapType>(resolvedType);

        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);
        if (mapVector->HasNull()) {
            int64_t index = 0;
            for (long j = startPos; j < endPos; j++) {
                if (mapVector->IsNull(j)) {
                    bitmap.SetBitTo(index, false);
                }
                index++;
            }
        }

        TypedBufferBuilder<int32_t> offsetsBuilder;
        for (int64_t i = 0; i <= vectorSize; i++) {
            int64_t value = offsets[startPos + i] - startOffset;
            if (value > std::numeric_limits<int32_t>::max()) {
                throw std::runtime_error("Map offset exceeds int32 range");
            }
            offsetsBuilder.Append(static_cast<int32_t>(value));
        }
        auto offsetsBuffer = offsetsBuilder.Finish().ValueOrDie();

        auto keyVector = mapVector->GetKeyVector();
        auto valueVector = mapVector->GetValueVector();
        auto keyArray = buildArrayForType(keyVector->GetTypeId(), keyVector.get(), mapType->key_type(), true,
                                          startOffset, endOffset);
        auto valueArray = buildArrayForType(valueVector->GetTypeId(), valueVector.get(), mapType->item_type(), true,
                                            startOffset, endOffset);

        auto entriesType = arrow::struct_({mapType->key_field(), mapType->item_field()});
        auto entriesData = arrow::ArrayData::Make(entriesType, endOffset - startOffset, {nullptr},
                                                  {keyArray->data(), valueArray->data()});

        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        buffers.emplace_back(offsetsBuffer);
        auto mapData = arrow::ArrayData::Make(mapType, vectorSize, buffers, {entriesData});
        return arrow::MakeArray(mapData);
    }

    std::shared_ptr<arrow::Array> buildStructArray(BaseVector *baseVector,
                                                   const std::shared_ptr<arrow::DataType> &arrowType,
                                                   bool isSplitWrite, long startPos, long endPos)
    {
        auto rowVector = static_cast<RowVector *>(baseVector);
        if (!isSplitWrite) {
            startPos = 0;
            endPos = rowVector->GetSize();
        }
        int64_t vectorSize = endPos - startPos;

        std::shared_ptr<arrow::DataType> resolvedType = arrowType;
        if (!resolvedType || resolvedType->id() != arrow::Type::STRUCT) {
            resolvedType = inferArrowType(OMNI_ROW, baseVector);
        }
        auto structType = std::static_pointer_cast<arrow::StructType>(resolvedType);

        auto bitmapBuffer = AllocateBitmap(vectorSize).ValueOrDie();
        arrow::internal::Bitmap bitmap(bitmapBuffer, 0, vectorSize);
        bitmap.SetBitsTo(true);
        if (rowVector->HasNull()) {
            int64_t index = 0;
            for (long j = startPos; j < endPos; j++) {
                if (rowVector->IsNull(j)) {
                    bitmap.SetBitTo(index, false);
                }
                index++;
            }
        }

        std::vector<std::shared_ptr<arrow::ArrayData>> childData;
        auto childCount = rowVector->ChildSize();
        for (int i = 0; i < childCount; i++) {
            auto childVector = rowVector->ChildAt(i);
            std::shared_ptr<arrow::DataType> childType;
            if (i < structType->num_fields()) {
                childType = structType->field(i)->type();
            }
            auto childArray = buildArrayForType(childVector->GetTypeId(), childVector.get(), childType,
                                                true, startPos, endPos);
            childData.emplace_back(childArray->data());
        }

        std::vector<std::shared_ptr<Buffer>> buffers;
        buffers.emplace_back(bitmapBuffer);
        auto structData = arrow::ArrayData::Make(structType, vectorSize, buffers, childData);
        return arrow::MakeArray(structData);
    }

    std::shared_ptr<arrow::Array> buildArrayForType(DataTypeId typeId, BaseVector *baseVector,
                                                    const std::shared_ptr<arrow::DataType> &arrowType,
                                                    bool isSplitWrite, long startPos, long endPos)
    {
        switch (typeId) {
            case OMNI_BOOLEAN:
                return buildBooleanChunk(typeId, baseVector, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_SHORT:
                return buildChunk<OMNI_SHORT, arrow::Int16Type, int16_t>(baseVector, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_INT:
                return buildChunk<OMNI_INT, arrow::Int32Type, int32_t>(baseVector, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_LONG:
                return buildChunk<OMNI_LONG, arrow::Int64Type, int64_t>(baseVector, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_BYTE:
                return buildChunk<OMNI_BYTE, arrow::Int8Type, int8_t>(baseVector, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_DATE32:
                return buildChunk<OMNI_DATE32, arrow::Date32Type, int32_t>(baseVector, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_DATE64:
                return buildChunk<OMNI_DATE64, arrow::Date64Type, int64_t>(baseVector, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_DOUBLE:
                return buildChunk<OMNI_DOUBLE, arrow::DoubleType, double>(baseVector, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_FLOAT:
                return buildChunk<OMNI_FLOAT, arrow::FloatType, float>(baseVector, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                return buildStringChunk(baseVector, arrowType, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_VARBINARY:
                return buildBinaryChunk(baseVector, arrowType, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_TIMESTAMP:
                return buildTimestampChunk(baseVector, arrowType, isSplitWrite, startPos, endPos)->chunk(0);
            case OMNI_DECIMAL64: {
                if (!arrowType || arrowType->id() != arrow::Type::DECIMAL) {
                    throw std::runtime_error("Decimal64 type requires arrow schema info");
                }
                auto decimalType = std::static_pointer_cast<arrow::Decimal128Type>(arrowType);
                int precision = decimalType->precision();
                int scale = decimalType->scale();
                return buildDecimal64Chunk(typeId, baseVector, precision, scale, isSplitWrite, startPos, endPos)->chunk(0);
            }
            case OMNI_DECIMAL128: {
                if (!arrowType || arrowType->id() != arrow::Type::DECIMAL) {
                    throw std::runtime_error("Decimal128 type requires arrow schema info");
                }
                auto decimalType = std::static_pointer_cast<arrow::Decimal128Type>(arrowType);
                int precision = decimalType->precision();
                int scale = decimalType->scale();
                return buildDecimal128Chunk(typeId, baseVector, precision, scale, isSplitWrite, startPos, endPos)->chunk(0);
            }
            case OMNI_ARRAY:
                return buildListArray(baseVector, arrowType, isSplitWrite, startPos, endPos);
            case OMNI_MAP:
                return buildMapArray(baseVector, arrowType, isSplitWrite, startPos, endPos);
            case OMNI_ROW:
                return buildStructArray(baseVector, arrowType, isSplitWrite, startPos, endPos);
            default:
                throw std::runtime_error("Native columnar write not support for this type: " + std::to_string(typeId));
        }
    }

    void ParquetWriter::write(long *vecNativeId, int colNums,
                              const int *omniTypes,
                              const unsigned char *dataColumnsIds,
                              bool isSplitWrite, long startPos , long endPos)
    {
        std::vector<std::shared_ptr<::arrow::ChunkedArray>> chunks;
        int decimalIndex = 0;
        int precision = 0;
        int scale = 0;
        for (int i = 0; i < colNums; ++i) {
            if (!dataColumnsIds[i]) {
                continue;
            }

            auto vec = (BaseVector *)vecNativeId[i];
            auto typeId = static_cast<DataTypeId>(omniTypes[i]);
            auto fieldType = schema_ ? schema_->field(i)->type() : nullptr;
            switch (typeId) {
            case OMNI_BOOLEAN:
                chunks.emplace_back(buildBooleanChunk(typeId, vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_BYTE:
                chunks.emplace_back(buildChunk<OMNI_BYTE, arrow::Int8Type, int8_t>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_SHORT:
                chunks.emplace_back(buildChunk<OMNI_SHORT, arrow::Int16Type, int16_t>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_INT:
                chunks.emplace_back(buildChunk<OMNI_INT, arrow::Int32Type, int32_t>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_LONG:
                chunks.emplace_back(buildChunk<OMNI_LONG, arrow::Int64Type, int64_t>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_DATE32:
                chunks.emplace_back(buildChunk<OMNI_DATE32, arrow::Date32Type, int32_t>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_DATE64:
                chunks.emplace_back(buildChunk<OMNI_DATE64, arrow::Date64Type, int64_t>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_DOUBLE:
                chunks.emplace_back(buildChunk<OMNI_DOUBLE, arrow::DoubleType, double>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_FLOAT:
                chunks.emplace_back(buildChunk<OMNI_FLOAT, arrow::FloatType, float>(vec, isSplitWrite, startPos, endPos));
                break;
            case OMNI_TIMESTAMP:
                chunks.emplace_back(buildTimestampChunk(vec, fieldType, isSplitWrite, startPos, endPos));
                break;
            case OMNI_VARCHAR:
                chunks.emplace_back(buildStringChunk(vec, fieldType, isSplitWrite, startPos, endPos));
                break;
            case OMNI_CHAR:
                chunks.emplace_back(buildStringChunk(vec, fieldType, isSplitWrite, startPos, endPos));
                break;
            case OMNI_VARBINARY:
                chunks.emplace_back(buildBinaryChunk(vec, fieldType, isSplitWrite, startPos, endPos));
                break;
            case OMNI_DECIMAL64:
                precision = precisions[decimalIndex];
                scale = scales[decimalIndex];
                chunks.emplace_back(buildDecimal64Chunk(typeId, vec, precision, scale, isSplitWrite, startPos, endPos));
                decimalIndex++;
                break;
            case OMNI_DECIMAL128:
                precision = precisions[decimalIndex];
                scale = scales[decimalIndex];
                chunks.emplace_back(buildDecimal128Chunk(typeId, vec, precision, scale, isSplitWrite, startPos, endPos));
                decimalIndex++;
                break;
            case OMNI_ARRAY:
            case OMNI_MAP:
            case OMNI_ROW: {
                auto array = buildArrayForType(typeId, vec, fieldType, isSplitWrite, startPos, endPos);
                chunks.emplace_back(ChunkedArray::Make({array}, array->type()).ValueOrDie());
                break;
            }
            default:
                throw std::runtime_error(
                    "Native columnar write not support for this type: " + std::to_string(typeId));
            }
        }
        auto numRows = chunks.empty() ? 0 : chunks[0]->length();

        auto table = arrow::Table::Make(schema_, std::move(chunks), numRows);
        if (!arrow_writer) {
            throw std::runtime_error("Arrow writer is not initialized");
        }
        PARQUET_THROW_NOT_OK(arrow_writer->WriteTable(*table));
    }

} // namespace omniruntime::writer
