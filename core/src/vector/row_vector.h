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

        private:
        std::vector<std::shared_ptr<BaseVector>> children_;
    };
}

#endif // OMNI_RUNTIME_ROW_VECTOR_H