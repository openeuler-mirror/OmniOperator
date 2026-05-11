/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: NamedStruct function implementation
 */

#include "NamedStruct.h"
#include "vector/vector.h"
#include "vector/array_vector.h"
#include "vector/map_vector.h"
#include "type/data_operations.h"
#include "util/type_util.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;

void NamedStructFunction::Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType,
                               BaseVector *&result, ExecutionContext *context) const {
    size_t argCount = inputDataTypes_.size();
    std::vector<BaseVector *> allArgs;
    for (int i = 0; i < argCount; ++i) {
        allArgs.push_back(args.top());
        args.pop();
    }
    std::reverse(allArgs.begin(), allArgs.end());

    auto *rowType = dynamic_cast<const RowType *>(outputType.get());
    if (rowType == nullptr) {
        OMNI_THROW("NamedStruct Error", "Output type must be RowType");
    }
    size_t fieldCount = static_cast<size_t>(rowType->Size());
    size_t consumedCount = 0;
    std::vector<BaseVector *> argVectors;
    std::vector<BaseVector *> nameVectors;

    auto isNameValueLayout = [&](const std::vector<BaseVector *> &selected) {
        if (selected.size() != 2 * fieldCount) {
            return false;
        }
        for (size_t i = 0; i < fieldCount; ++i) {
            if (!TypeUtil::IsStringType(selected[2 * i]->GetTypeId())) {
                return false;
            }
        }
        return true;
    };

    auto takeFromSuffix = [&](size_t suffixCount) {
        size_t startIdx = allArgs.size() - suffixCount;
        std::vector<BaseVector *> selected(allArgs.begin() + static_cast<int64_t>(startIdx), allArgs.end());
        if (suffixCount == fieldCount) {
            argVectors = std::move(selected);
        } else {
            argVectors.reserve(fieldCount);
            nameVectors.reserve(fieldCount);
            for (size_t i = 0; i < fieldCount; ++i) {
                nameVectors.push_back(selected[2 * i]);
                argVectors.push_back(selected[2 * i + 1]);
            }
        }
        consumedCount = suffixCount;
    };

    if (allArgs.size() == fieldCount) {
        takeFromSuffix(fieldCount);
    } else if (allArgs.size() == 2 * fieldCount) {
        takeFromSuffix(2 * fieldCount);
    } else if (allArgs.size() > 2 * fieldCount) {
        // Some callers leave unrelated vectors on the stack before invoking a nested named_struct.
        // Consume only the tail that belongs to this call and restore the prefix afterwards.
        std::vector<BaseVector *> candidate(allArgs.end() - static_cast<int64_t>(2 * fieldCount), allArgs.end());
        if (isNameValueLayout(candidate)) {
            takeFromSuffix(2 * fieldCount);
        } else {
            takeFromSuffix(fieldCount);
        }
    } else if (allArgs.size() > fieldCount) {
        takeFromSuffix(fieldCount);
    } else {
        OMNI_THROW("NamedStruct Error", "Argument count mismatch: expected " +
                   std::to_string(fieldCount) + " or " + std::to_string(2 * fieldCount) +
                   " but got " + std::to_string(allArgs.size()));
    }
    int32_t size = argVectors[0]->GetSize();
    for (size_t i = 1; i < argVectors.size(); ++i) {
        if (argVectors[i]->GetSize() != size) {
            OMNI_THROW("NamedStruct Error", "All arguments must have the same size");
        }
    }
    result = VectorHelper::CreateComplexVector(const_cast<DataType *>(outputType.get()), size);
    auto *resultRow = static_cast<RowVector *>(result);
    std::vector<int64_t> mapRunningOffsets(argVectors.size(), 0);
    for (int32_t row = 0; row < size; ++row) {
        for (size_t childIdx = 0; childIdx < argVectors.size(); ++childIdx) {
            BaseVector *srcVec = argVectors[childIdx];
            BaseVector *dstVec = resultRow->ChildAt(static_cast<int32_t>(childIdx)).get();
            int32_t readRow = (srcVec->GetEncoding() == OMNI_ENCODING_CONST) ? 0 : row;
            if (srcVec->IsNull(readRow)) {
                dstVec->SetNull(row);
            } else {
                DataTypeId fieldTypeId = rowType->Type(static_cast<int>(childIdx))->GetId();
                int64_t *pOffset = (fieldTypeId == OMNI_MAP) ? &mapRunningOffsets[childIdx] : nullptr;
                CopyFieldAtRow(srcVec, dstVec, row, fieldTypeId, readRow, pOffset);
            }
        }
    }
    for (size_t i = 0; i + consumedCount < allArgs.size(); ++i) {
        args.push(allArgs[i]);
    }
    for (auto *vec : argVectors) {
        if (vec != nullptr && !vec->GetIsField()) {
            delete vec;
        }
    }
    for (auto *vec : nameVectors) {
        if (vec != nullptr && !vec->GetIsField()) {
            delete vec;
        }
    }
}

void NamedStructFunction::CopyFieldAtRow(BaseVector *srcVec, BaseVector *dstVec, int32_t dstRow,
                                        DataTypeId typeId, int32_t srcRow,
                                        int64_t *mapRunningOffset) const {
    switch (typeId) {
        case OMNI_BOOLEAN: {
            bool v = GetValueFromVector<bool>(srcVec, srcRow);
            SetValueToVector<bool>(dstVec, dstRow, v);
            break;
        }
        case OMNI_BYTE: {
            int8_t v = GetValueFromVector<int8_t>(srcVec, srcRow);
            SetValueToVector<int8_t>(dstVec, dstRow, v);
            break;
        }
        case OMNI_SHORT: {
            int16_t v = GetValueFromVector<int16_t>(srcVec, srcRow);
            SetValueToVector<int16_t>(dstVec, dstRow, v);
            break;
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            int32_t v = GetValueFromVector<int32_t>(srcVec, srcRow);
            SetValueToVector<int32_t>(dstVec, dstRow, v);
            break;
        }
        case OMNI_LONG:
        case OMNI_TIMESTAMP:
        case OMNI_DECIMAL64: {
            int64_t v = GetValueFromVector<int64_t>(srcVec, srcRow);
            SetValueToVector<int64_t>(dstVec, dstRow, v);
            break;
        }
        case OMNI_FLOAT: {
            float v = GetValueFromVector<float>(srcVec, srcRow);
            SetValueToVector<float>(dstVec, dstRow, v);
            break;
        }
        case OMNI_DOUBLE: {
            double v = GetValueFromVector<double>(srcVec, srcRow);
            SetValueToVector<double>(dstVec, dstRow, v);
            break;
        }
        case OMNI_DECIMAL128: {
            Decimal128 v = GetValueFromVector<Decimal128>(srcVec, srcRow);
            SetValueToVector<Decimal128>(dstVec, dstRow, v);
            break;
        }
        case OMNI_VARCHAR:
        case OMNI_CHAR:
        case OMNI_VARBINARY: {
            std::string_view v = GetStringValueFromVector(srcVec, srcRow);
            SetStringValueToVector(dstVec, dstRow, const_cast<std::string_view &>(v));
            break;
        }
        case OMNI_ARRAY: {
            auto *srcArray = static_cast<ArrayVector *>(srcVec);
            auto *dstArray = static_cast<ArrayVector *>(dstVec);
            BaseVector *elemSlice = srcArray->GetValue(srcRow);
            dstArray->SetValue(dstRow, elemSlice);
            delete elemSlice;
            break;
        }
        case OMNI_MAP: {
            auto *srcMap = static_cast<MapVector *>(srcVec);
            auto *dstMap = static_cast<MapVector *>(dstVec);
            int64_t mapEntryCount = srcMap->GetSize(srcRow);
            int64_t srcStartOffset = srcMap->GetOffset(srcRow);
            int64_t totalBefore = mapRunningOffset ? *mapRunningOffset : 0;
            if (mapEntryCount > 0) {
                BaseVector *keySlice = srcMap->GetKeyVector()->Slice(static_cast<int32_t>(srcStartOffset),
                                                                     static_cast<int32_t>(mapEntryCount));
                BaseVector *valSlice = srcMap->GetValueVector()->Slice(static_cast<int32_t>(srcStartOffset),
                                                                       static_cast<int32_t>(mapEntryCount));
                VectorHelper::ExpandElementVector(dstMap->GetKeyVector().get(),
                                                  dstMap->GetKeyVector()->GetTypeId(),
                                                  static_cast<int32_t>(totalBefore + mapEntryCount));
                VectorHelper::ExpandElementVector(dstMap->GetValueVector().get(),
                                                  dstMap->GetValueVector()->GetTypeId(),
                                                  static_cast<int32_t>(totalBefore + mapEntryCount));
                VectorHelper::AppendVector(dstMap->GetKeyVector().get(), static_cast<int32_t>(totalBefore),
                                           keySlice, static_cast<int32_t>(mapEntryCount));
                VectorHelper::AppendVector(dstMap->GetValueVector().get(), static_cast<int32_t>(totalBefore),
                                           valSlice, static_cast<int32_t>(mapEntryCount));
                delete keySlice;
                delete valSlice;
            }
            dstMap->SetOffset(dstRow, static_cast<int32_t>(totalBefore));
            dstMap->SetSize(dstRow, static_cast<int32_t>(mapEntryCount));
            if (mapRunningOffset) {
                *mapRunningOffset += mapEntryCount;
            }
            break;
        }
        case OMNI_ROW: {
            auto *srcRowVec = static_cast<RowVector *>(srcVec);
            auto *dstRowVec = static_cast<RowVector *>(dstVec);
            for (int32_t i = 0; i < srcRowVec->ChildSize(); ++i) {
                CopyFieldAtRow(srcRowVec->ChildAt(i).get(), dstRowVec->ChildAt(i).get(), dstRow,
                               dstRowVec->ChildAt(i)->GetTypeId(), srcRow, nullptr);
            }
            break;
        }
        default:
            OMNI_THROW("NamedStruct Error", "Unsupported field type: " + TypeUtil::TypeToString(typeId));
    }
}

template <typename T>
T NamedStructFunction::GetValueFromVector(BaseVector *vec, int32_t row) const {
    Encoding encoding = vec->GetEncoding();
    if (encoding == OMNI_ENCODING_CONST) {
        auto *constVec = static_cast<ConstVector<T> *>(vec);
        return constVec->GetConstValue();
    }
    if (encoding == OMNI_FLAT) {
        auto *flatVec = static_cast<Vector<T> *>(vec);
        return flatVec->GetValue(row);
    }
    if (encoding == OMNI_DICTIONARY) {
        auto *dictVec = static_cast<Vector<DictionaryContainer<T>> *>(vec);
        return dictVec->GetValue(row);
    }
    OMNI_THROW("NamedStruct Error", "Unsupported encoding type");
}

std::string_view NamedStructFunction::GetStringValueFromVector(BaseVector *vec, int32_t row) const {
    Encoding encoding = vec->GetEncoding();
    if (encoding == OMNI_ENCODING_CONST) {
        auto *constVec = static_cast<ConstVector<std::string_view> *>(vec);
        return constVec->GetConstValue();
    }
    if (encoding == OMNI_FLAT) {
        auto *flatVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
        return flatVec->GetValue(row);
    }
    if (encoding == OMNI_DICTIONARY) {
        auto *dictVec = static_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vec);
        return dictVec->GetValue(row);
    }
    OMNI_THROW("NamedStruct Error", "Unsupported encoding type for string");
}

template <typename T>
void NamedStructFunction::SetValueToVector(BaseVector *vec, int32_t row, const T &value) const {
    auto *resultVec = static_cast<Vector<T> *>(vec);
    resultVec->SetValue(row, value);
}

void NamedStructFunction::SetStringValueToVector(BaseVector *vec, int32_t row, std::string_view &value) const {
    auto *resultVec = static_cast<Vector<LargeStringContainer<std::string_view>> *>(vec);
    resultVec->SetValue(row, value);
}

template int8_t NamedStructFunction::GetValueFromVector<int8_t>(BaseVector *, int32_t) const;
template int16_t NamedStructFunction::GetValueFromVector<int16_t>(BaseVector *, int32_t) const;
template int32_t NamedStructFunction::GetValueFromVector<int32_t>(BaseVector *, int32_t) const;
template int64_t NamedStructFunction::GetValueFromVector<int64_t>(BaseVector *, int32_t) const;
template float NamedStructFunction::GetValueFromVector<float>(BaseVector *, int32_t) const;
template double NamedStructFunction::GetValueFromVector<double>(BaseVector *, int32_t) const;
template bool NamedStructFunction::GetValueFromVector<bool>(BaseVector *, int32_t) const;
template omniruntime::type::Decimal128 NamedStructFunction::GetValueFromVector<omniruntime::type::Decimal128>(BaseVector *, int32_t) const;

template void NamedStructFunction::SetValueToVector<int8_t>(BaseVector *, int32_t, const int8_t &) const;
template void NamedStructFunction::SetValueToVector<int16_t>(BaseVector *, int32_t, const int16_t &) const;
template void NamedStructFunction::SetValueToVector<int32_t>(BaseVector *, int32_t, const int32_t &) const;
template void NamedStructFunction::SetValueToVector<int64_t>(BaseVector *, int32_t, const int64_t &) const;
template void NamedStructFunction::SetValueToVector<float>(BaseVector *, int32_t, const float &) const;
template void NamedStructFunction::SetValueToVector<double>(BaseVector *, int32_t, const double &) const;
template void NamedStructFunction::SetValueToVector<bool>(BaseVector *, int32_t, const bool &) const;
template void NamedStructFunction::SetValueToVector<omniruntime::type::Decimal128>(BaseVector *, int32_t, const omniruntime::type::Decimal128 &) const;
}
