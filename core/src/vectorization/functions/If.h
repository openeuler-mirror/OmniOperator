/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: If conditional function for vectorized expressions
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vector/array_vector.h"
#include "vector/map_vector.h"
#include "vector/row_vector.h"
#include "vector/vector_helper.h"
#include "type/data_operations.h"
#include "util/debug.h"
#include <string_view>

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace omniruntime::op;

class IfFunction : public VectorFunction {
public:
    explicit IfFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        ExecutionContext *context) const override
    {
        auto falseVec = args.top();
        args.pop();
        auto trueVec = args.top();
        args.pop();
        auto condVec = args.top();
        args.pop();
        auto *boolVec = static_cast<Vector<bool> *>(condVec);
        auto size = boolVec->GetSize();
        DataTypeId typeId = outputType->GetId();

        CreateResultVector(typeId, size, trueVec, result);

        for (int32_t row = 0; row < size; ++row) {
            BaseVector* temp = nullptr;
            if (boolVec->IsNull(row)) {
                temp = falseVec;
            } else {
                temp = boolVec->GetValue(row) ? trueVec : falseVec;
            }
            if (temp->IsNull(row)) {
                if (typeId == OMNI_ARRAY) {
                    static_cast<ArrayVector *>(result)->SetNull(row);
                } else if (typeId == OMNI_MAP) {
                    static_cast<MapVector *>(result)->SetNull(row);
                } else {
                    result->SetNull(row);
                }
                continue;
            }
            switch (typeId) {
                case OMNI_BOOLEAN :
                {
                    auto res = static_cast<Vector<bool> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_BYTE :
                {
                    auto res = static_cast<Vector<int8_t> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_SHORT :
                {
                    auto res = static_cast<Vector<int16_t> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_INT :
                case OMNI_DATE32 :
                {
                    auto res = static_cast<Vector<int32_t> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_LONG :
                case OMNI_DATE64 :
                case OMNI_TIMESTAMP :
                case OMNI_DECIMAL64 :
                {
                    auto res = static_cast<Vector<int64_t> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_FLOAT :
                {
                    auto res = static_cast<Vector<float> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_DOUBLE :
                {
                    auto res = static_cast<Vector<double> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_VARCHAR :
                case OMNI_VARBINARY :
                {
                    auto res = static_cast<Vector<LargeStringContainer<std::string_view>> *>(temp)->GetValue(row);
                    static_cast<Vector<LargeStringContainer<std::string_view>> *>(result)->SetValue(row, res);
                    break;
                }
                case OMNI_DECIMAL128 :
                {
                    auto res = static_cast<Vector<Decimal128> *>(temp)->GetValue(row);
                    VectorHelper::SetValue(result, row, &res);
                    break;
                }
                case OMNI_ARRAY :
                {
                    CopyArrayValue(temp, result, row);
                    break;
                }
                case OMNI_MAP :
                {
                    CopyMapValue(temp, result, row);
                    break;
                }
                case OMNI_ROW :
                {
                    CopyRowValue(temp, result, row);
                    break;
                }
                default :
                    OMNI_THROW("If expr Error", "Unsupported output type Id:" + std::to_string(typeId));
            }
        }
    }

private:
    void CreateResultVector(DataTypeId typeId, int32_t size, BaseVector *sourceVec,
        BaseVector *&result) const
    {
        switch (typeId) {
            case OMNI_ARRAY :
            {
                auto *srcArray = static_cast<ArrayVector *>(sourceVec);
                auto elemVec = std::shared_ptr<BaseVector>(
                    VectorHelper::CreateFlatVector(srcArray->GetElementVector()->GetTypeId(), 0));
                result = new ArrayVector(size, elemVec);
                break;
            }
            case OMNI_MAP :
            {
                auto *srcMap = static_cast<MapVector *>(sourceVec);
                auto keyVec = std::shared_ptr<BaseVector>(
                    VectorHelper::CreateFlatVector(srcMap->GetKeyVector()->GetTypeId(), 0));
                auto valVec = std::shared_ptr<BaseVector>(
                    VectorHelper::CreateFlatVector(srcMap->GetValueVector()->GetTypeId(), 0));
                result = new MapVector(size, keyVec, valVec);
                break;
            }
            case OMNI_ROW :
            {
                auto *srcRow = static_cast<RowVector *>(sourceVec);
                auto *resRow = new RowVector(size);
                for (int32_t c = 0; c < srcRow->ChildSize(); ++c) {
                    auto childTypeId = srcRow->ChildAt(c)->GetTypeId();
                    resRow->AddChild(VectorHelper::CreateFlatVector(childTypeId, size));
                }
                result = resRow;
                break;
            }
            default :
                result = VectorHelper::CreateFlatVector(typeId, size);
                break;
        }
    }

    void CopyArrayValue(BaseVector *source, BaseVector *result, int32_t row) const
    {
        auto *srcArray = static_cast<ArrayVector *>(source);
        auto *resArray = static_cast<ArrayVector *>(result);
        BaseVector *elemSlice = srcArray->GetValue(row);
        resArray->SetValue(row, elemSlice);
        delete elemSlice;
    }

    void CopyMapValue(BaseVector *source, BaseVector *result, int32_t row) const
    {
        auto *srcMap = static_cast<MapVector *>(source);
        auto *resMap = static_cast<MapVector *>(result);
        int64_t mapEntryCount = srcMap->GetSize(row);
        int64_t srcStartOffset = srcMap->GetOffset(row);
        int64_t resStartOffset = resMap->GetOffset(row);

        if (mapEntryCount > 0) {
            BaseVector *resKeys = const_cast<BaseVector *>(resMap->GetKeyVector().get());
            VectorHelper::ExpandElementVector(resKeys, resKeys->GetTypeId(),
                static_cast<int32_t>(resStartOffset + mapEntryCount));
            BaseVector *srcKeySlice = srcMap->GetKeyVector()->Slice(
                static_cast<int>(srcStartOffset), static_cast<int>(mapEntryCount));
            VectorHelper::AppendVector(resKeys, static_cast<int32_t>(resStartOffset),
                srcKeySlice, static_cast<int32_t>(mapEntryCount));
            delete srcKeySlice;

            BaseVector *resValues = const_cast<BaseVector *>(resMap->GetValueVector().get());
            VectorHelper::ExpandElementVector(resValues, resValues->GetTypeId(),
                static_cast<int32_t>(resStartOffset + mapEntryCount));
            BaseVector *srcValSlice = srcMap->GetValueVector()->Slice(
                static_cast<int>(srcStartOffset), static_cast<int>(mapEntryCount));
            VectorHelper::AppendVector(resValues, static_cast<int32_t>(resStartOffset),
                srcValSlice, static_cast<int32_t>(mapEntryCount));
            delete srcValSlice;
        }
        resMap->SetSize(row, static_cast<int32_t>(mapEntryCount));
    }

    void CopyRowValue(BaseVector *source, BaseVector *result, int32_t row) const
    {
        auto *srcRow = static_cast<RowVector *>(source);
        auto *resRow = static_cast<RowVector *>(result);

        for (int32_t c = 0; c < srcRow->ChildSize(); ++c) {
            BaseVector *srcChild = srcRow->ChildAt(c).get();
            BaseVector *resChild = resRow->ChildAt(c).get();
            if (srcChild->IsNull(row)) {
                resChild->SetNull(row);
            } else {
                VectorHelper::CopyValue(srcChild, row, resChild, row);
            }
        }
    }
};
}
