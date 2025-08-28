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

        RowVector(int32_t size, std::vector<std::shared_ptr<BaseVector>> children)
                : BaseVector(size, Encoding::OMNI_ENCODING_STRUCT, DataTypeId::OMNI_ROW),childrenSize_(children.size()),
                  children_(std::move(children)) {}

        ~RowVector() override = default;


        std::shared_ptr<BaseVector>& ChildAt(int32_t index) {
            if(index < 0 || index >= childrenSize_) {
                throw exception::OmniException("Trying to access non-existing child in RowVector");
            }
            return children_[index];
        }


        std::vector<std::shared_ptr<BaseVector>& Children() {
            return children_;
        }

        int32_t ChildSize() const {
            return childrenSize_;
        }

        private:
        const std::vector<std::shared_ptr<BaseVector>> children_;
        const int32_t childrenSize_;
    };
}

#endif // OMNI_RUNTIME_ROW_VECTOR_H