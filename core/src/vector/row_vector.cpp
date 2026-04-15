/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: MapVector  implementation
 */

#include "row_vector.h"
#include "vector_helper.h"

namespace omniruntime::vec {
    RowVector *RowVector::CopyPositions(const int *positions, int positionOffset, int length)
    {
        if (UNLIKELY((positions == nullptr) || (length < 0))) {
            std::string message = "StructVector positions is null or the input length is incorrect: " + std::to_string(length) + ".";
            throw OmniException("OPERATOR_RUNTIME_ERROR", message);
        }

        RowVector* newRowVector = new RowVector(static_cast<int32_t>(length));
        const int* startPositions = positions + positionOffset;
        for (int32_t i = 0; i < length; i++) {
            int position = startPositions[i];
            if (UNLIKELY(IsNull(position))) {
                newRowVector->SetNull(i);
            }
        }

        for (int i = 0; i < children_.size(); i++) {
            newRowVector->AddChild(children_[i]->CopyPositions(positions, positionOffset, length));
        }
        return newRowVector;
    }

    void RowVector::Append(BaseVector *other, int positionOffset, int length) {
        if (UNLIKELY(other == nullptr || positionOffset < 0 || length <= 0)) {
            std::string message = "Invalid input for RowVector::Append";
            throw OmniException("OPERATOR_RUNTIME_ERROR", message);
        }

        auto *otherRowVector = static_cast<RowVector *>(other);
        if (UNLIKELY(otherRowVector == nullptr)) {
            std::string message = "RowVector::Append expects another RowVector";
            throw OmniException("TYPE_MISMATCH_ERROR", message);
        }
        if (UNLIKELY(children_.size() != otherRowVector->ChildSize())) {
            std::string message = "RowVector child count mismatch: " +
                                  std::to_string(children_.size()) + " vs " +
                                  std::to_string(otherRowVector->ChildSize());
            throw OmniException("TYPE_MISMATCH_ERROR", message);
        }

        int32_t newSize = positionOffset + length;
        Expand(newSize);

        for (int i = 0; i < length; i++) {
            int destIndex = positionOffset + i;
            if (otherRowVector->IsNull(i)) {
                SetNull(destIndex);
            }
        }

        for (int i = 0 ; i < children_.size(); i++) {
            BaseVector* child = children_[i].get();
            BaseVector* otherChild = otherRowVector->ChildAt(i).get();
            if (UNLIKELY(child->GetTypeId() != otherChild->GetTypeId())) {
                std::string message = "RowVector child type mismatch at index " + std::to_string(i);
                throw OmniException("TYPE_MISMATCH_ERROR", message);
            }

            child->Expand(newSize);
            VectorHelper::AppendVector(child, positionOffset, otherChild, length);
        }
        // When source row is null, parent was SetNull but children got copied; clear children at null rows to avoid residual data.
        for (int i = 0; i < length; i++) {
            if (otherRowVector->IsNull(i)) {
                int destIndex = positionOffset + i;
                for (int c = 0; c < static_cast<int>(children_.size()); c++) {
                    children_[c]->SetNull(destIndex);
                }
            }
        }
    }

    std::vector<BaseVector*> RowVector::GetValue(int index)
    {
        if (UNLIKELY(index < 0 || index >= size)) {
            std::string message("slice vector out of range(needed size:%d, real size:%d).", index,
                size);
            throw OmniException("OPERATOR_RUNTIME_ERROR", message);
        }
        auto childSize = ChildSize();
        std::vector<BaseVector*> childs(childSize);
        for (auto i = 0; i < childSize; i++) {
            auto child = ChildAt(i);
            BaseVector* baseVector = child.get()->Slice(index, 1, false);
            childs[i] = baseVector;
        }
        return childs;
    }

    void RowVector::SetValue(int index, RowVector* value)
    {
        auto childSize = ChildSize();
        if (childSize != value->ChildSize()) {
            std::string message("RowVector size not match.");
            throw OmniException("OPERATOR_RUNTIME_ERROR", message);
        }
        for (auto i = 0; i < childSize; i++) {
            auto child = ChildAt(i).get();
            auto valueChild = value->ChildAt(i).get();
            auto childDataTypeId = child->GetTypeId();
            switch (childDataTypeId) {
                case OMNI_BOOLEAN:
                    static_cast<Vector<bool> *>(child)->SetValue(index, static_cast<Vector<bool> *>(valueChild)->GetValue(0));
                    break;
                case OMNI_INT:
                case OMNI_DATE32:
                    static_cast<Vector<int32_t> *>(child)->SetValue(index, static_cast<Vector<int32_t> *>(valueChild)->GetValue(0));
                    break;
                case OMNI_SHORT:
                    static_cast<Vector<int16_t> *>(child)->SetValue(index, static_cast<Vector<int16_t> *>(valueChild)->GetValue(0));
                    break;
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64:
                    static_cast<Vector<int64_t> *>(child)->SetValue(index, static_cast<Vector<int64_t> *>(valueChild)->GetValue(0));
                    break;
                case OMNI_DOUBLE:
                    static_cast<Vector<double> *>(child)->SetValue(index, static_cast<Vector<double> *>(valueChild)->GetValue(0));
                    break;
                case OMNI_FLOAT:
                    static_cast<Vector<float> *>(child)->SetValue(index, static_cast<Vector<float> *>(valueChild)->GetValue(0));
                    break;
                case OMNI_VARCHAR:
                case OMNI_CHAR: {
                    static_cast<Vector<LargeStringContainer<std::string_view>> *>(child)->SetValue(index,
                        static_cast<Vector<LargeStringContainer<std::string_view>> *>(valueChild)->GetValue(0));
                    break;
                }
                case OMNI_DECIMAL128:
                    static_cast<Vector<Decimal128> *>(child)->SetValue(index, static_cast<Vector<Decimal128> *>(valueChild)->GetValue(0));
                    break;
                case OMNI_BYTE:
                    static_cast<Vector<int8_t> *>(child)->SetValue(index, static_cast<Vector<int8_t> *>(valueChild)->GetValue(0));
                    break;
                case OMNI_ARRAY:
                    static_cast<ArrayVector *>(child)->SetValue(index, static_cast<ArrayVector *>(valueChild)->GetValue(0));
                    break;
                case OMNI_MAP:
                    static_cast<MapVector *>(child)->SetValue(index, static_cast<MapVector *>(valueChild));
                    break;
                case OMNI_ROW:
                    static_cast<RowVector *>(child)->SetValue(index, static_cast<RowVector *>(valueChild));
                    break;
                default:
                    break;
            }
        }
    }
}