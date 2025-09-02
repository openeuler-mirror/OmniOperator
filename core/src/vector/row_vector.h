/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: RowVector  implementation
 */

#ifndef OMNI_RUNTIME_ROW_VECTOR_H
#define OMNI_RUNTIME_ROW_VECTOR_H

#include <vector>
#include <memory>

namespace omniruntime::vec {
    class RowVector : public BaseVector {
    public:
        RowVector(const RowVector&) = delete;
        RowVector& operator=(const RowVector&) = delete;

        RowVector(int32_t size)
            : BaseVector(size, Encoding::OMNI_ENCODING_STRUCT, DataTypeId::OMNI_ROW) {}

        RowVector(int32_t size, std::vector<std::shared_ptr<BaseVector>> children)
            : BaseVector(size, Encoding::OMNI_ENCODING_STRUCT, DataTypeId::OMNI_ROW),
              children_(std::move(children)) {}

        ~RowVector() override = default;


        std::shared_ptr<BaseVector>& ChildAt(int32_t index) {
            return children_[index];
        }

        std::vector<std::shared_ptr<BaseVector>>& Children() {
            return children_;
        }

        void AddChild(std::shared_ptr<BaseVector> child) {
            children_.push_back(std::move(child));
        }

        int32_t ChildSize() const {
            return children_.size();
        }

        void Add(int32_t index, BaseVector* addedVec) {
            children_[index] = std::shared_ptr<BaseVector>(addedVec);
        }

        void Append(BaseVector* appendedVec) {
            children_.emplace_back(std::shared_ptr<BaseVector>(appendedVec));
        }


        RowVector *Slice(int positionOffset, int length, bool isCopy = false) override
        {
            if (UNLIKELY(positionOffset + length > size)) {
                std::string message("slice vector out of range(needed size:%d, real size:%d).", positionOffset + length,
                                    size);
                throw OmniException("OPERATOR_RUNTIME_ERROR", message);
            }
            auto sliced = new RowVector(length);
            sliced->offset = offset + positionOffset; // update offset
            sliced->isSliced = true;
            for (int i = 0; i < length; ++i) {
                if (IsNull(positionOffset + i)) {
                    sliced->SetNull(i);
                }
            }
            for (int i = 0; i < children_.size(); ++i) {
                sliced->AddChild(std::shared_ptr<BaseVector>(ChildAt(i)->Slice(positionOffset, length, isCopy)));
            }
            return sliced;
        }

        BaseVector* CopyPositions(const int *positions, int positionOffset, int length)  {
            if (UNLIKELY((positions == nullptr) || (length < 0))) {
                std::string message("StructVector positions is null or the input length is incorrect: %d.", length);
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
                newRowVector->Append(children_[i]->CopyPositions(positions, positionOffset, length));
            }
            return newRowVector;
        }

    private:
        std::vector<std::shared_ptr<BaseVector>> children_;
    };
}

#endif // OMNI_RUNTIME_ROW_VECTOR_H