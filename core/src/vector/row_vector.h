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
                newRowVector->Append(CopyChildPositionsVector(children_[i].get(), positions, positionOffset, length));
            }
        }

    private:
        std::vector<std::shared_ptr<BaseVector>> children_;

        BaseVector *CopyChildPositionsVector(BaseVector *vector, const int *positions, int offset, int length)
        {
            DataTypeId dataTypeId = vector->GetTypeId();
            switch (dataTypeId) {
                case type::OMNI_INT:
                case type::OMNI_DATE32: {
                    return reinterpret_cast<Vector<int32_t> *>(vector)->CopyPositions(positions, offset, length);
                }
                case type::OMNI_SHORT: {
                    return reinterpret_cast<Vector<int16_t> *>(vector)->CopyPositions(positions, offset, length);
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
                default: {
                    std::string omniExceptionInfo =
                        "In function CopyChildPositionsVector, no such data type " + std::to_string(dataTypeId);
                    throw omniruntime::exception::OmniException("UNSUPPORTED_ERROR", omniExceptionInfo);
                }
            }
        }
    };
}

#endif // OMNI_RUNTIME_ROW_VECTOR_H