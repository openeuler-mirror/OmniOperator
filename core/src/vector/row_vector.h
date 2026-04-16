/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: RowVector  implementation
 */

#ifndef OMNI_RUNTIME_ROW_VECTOR_H
#define OMNI_RUNTIME_ROW_VECTOR_H

#include <vector>
#include <memory>
#include "vector.h"

namespace omniruntime::vec {
    class RowVector : public BaseVector {
    public:
        RowVector(const RowVector&) = delete;
        RowVector& operator=(const RowVector&) = delete;

        RowVector(int32_t size)
            : BaseVector(size, Encoding::OMNI_ENCODING_STRUCT, DataTypeId::OMNI_ROW), capacity(size) {}

        RowVector(int32_t size, std::vector<std::shared_ptr<BaseVector>> children)
            : BaseVector(size, Encoding::OMNI_ENCODING_STRUCT, DataTypeId::OMNI_ROW),
              children_(std::move(children)), capacity(size) {}

        RowVector(int32_t size, std::vector<BaseVector*> children)
            : BaseVector(size, Encoding::OMNI_ENCODING_STRUCT, DataTypeId::OMNI_ROW),
            rawChildren_(std::move(children)), capacity(size) {}

        ~RowVector() override = default;

        std::shared_ptr<BaseVector>& ChildAt(int32_t index)
        {
            return children_[index];
        }

        std::vector<std::shared_ptr<BaseVector>>& Children()
        {
            return children_;
        }

        std::vector<BaseVector *> GetRawChildren()
        {
            return rawChildren_;
        }

        void AddChild(std::shared_ptr<BaseVector> child)
        {
            children_.push_back(std::move(child));
        }

        int32_t ChildSize() const
        {
            return children_.size();
        }

        void Set(int32_t index, BaseVector* setVec)
        {
            // if the index exceeds the current size, expand children_
            if (index >= children_.size()) {
                children_.resize(index + 1);
            }
            children_[index] = std::shared_ptr<BaseVector>(setVec);
        }

        void AddChild(BaseVector* addedVec)
        {
            children_.emplace_back(std::shared_ptr<BaseVector>(addedVec));
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

        void Expand(int32_t needCapacity) override
        {
            if (needCapacity <= size) {
                return;
            }

            if (needCapacity <= capacity) {
                size = needCapacity;
                for (auto &child : children_) {
                    if (child) {
                        child->Expand(needCapacity);
                    }
                }
                return;
            }

            int32_t newCapacity = std::max(capacity * 2, needCapacity);
            int32_t oldSize = size;

            auto oldNullsBuffer = nullsBuffer;
            nullsBuffer = std::make_shared<NullsBuffer>(newCapacity);
            if (oldNullsBuffer != nullptr) {
                nullsBuffer->SetNulls(0, oldNullsBuffer.get(), oldSize);
            } else {
                nullsBuffer->SetNulls(0, false, newCapacity);
            }

            capacity = newCapacity;
            size = needCapacity;

            for (auto &child : children_) {
                if (child) {
                    child->Expand(needCapacity);
                }
            }
        }

        /* *
         * Copies the values of the vector at the indicated positions
         * @param positions
         * @param offset
         * @param length
         */
        RowVector* CopyPositions(const int *positions, int positionOffset, int length);

        void Append(BaseVector *other, int positionOffset, int length);

        std::vector<BaseVector*> GetValue(int index);

        void SetValue(int index, RowVector* value);

    private:
        std::vector<std::shared_ptr<BaseVector>> children_;
        // rawChildren_ elements no needs to release, this only use for immutable view, like string and string_view
        std::vector<BaseVector *> rawChildren_;
        int32_t capacity;
    };
}

#endif // OMNI_RUNTIME_ROW_VECTOR_H