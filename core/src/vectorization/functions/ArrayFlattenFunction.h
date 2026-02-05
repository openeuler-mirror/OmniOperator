/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: visitor class for expressions
 */
#pragma once
#include "vectorization/VectorFunction.h"
#include "vectorization/ExprEval.h"
#include "vector/array_vector.h"
#include "type/data_operations.h"

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;
    using namespace omniruntime::op;

    class ArrayFlattenFunction : public VectorFunction {
    public:
        explicit ArrayFlattenFunction () {}

        void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
            ExecutionContext *context) const override
        {
            BaseVector* inputVec = args.top();
            args.pop();
            const int32_t rowSize = inputVec->GetSize();
            result = VectorHelper::CreateComplexVector(outputType.get(), rowSize);
            for (int32_t row = 0; row < rowSize; ++row)
            {
                auto innerArrayVector = dynamic_cast<ArrayVector *>(dynamic_cast<ArrayVector *>(inputVec)->GetValue(row));// Array<Array<T>>
                for (int32_t col = 0; col < innerArrayVector->GetSize(); ++col)
                {
                    ArrayVector* arrayVector1 = dynamic_cast<ArrayVector *>(innerArrayVector->GetValue(col)); //Array<T>
                    for (int i = 0; i < arrayVector1->GetSize(); ++i)
                    {
                        dynamic_cast<ArrayVector *>(result)->SetValue(i, arrayVector1->GetValue(i));
                    }
                }
            }
        }
    };
}