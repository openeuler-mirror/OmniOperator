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
    }
}