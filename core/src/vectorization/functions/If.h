/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: If function for vectorized conditional expressions
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vector/array_vector.h"
#include "type/data_operations.h"
#include "util/debug.h"
#include "util/type_util.h"
#include "vector/vector_helper.h"
#include <vector>
#include <string_view>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;
    using namespace omniruntime::op;

class IfFunction : public VectorFunction {
public:
    explicit IfFunction() {}

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
               ExecutionContext *context) const override;

private:
    // Helper: Get value from vector with different encodings
    template<typename T>
    T GetValueFromVector(BaseVector *vec, int32_t row) const;
    
    // Helper: Get string value from vector
    std::string_view GetStringValueFromVector(BaseVector *vec, int32_t row) const;
    
    // Helper: Set value to vector
    template<typename T>
    void SetValueToVector(BaseVector *vec, int32_t row, const T &value) const;
    
    // Helper: Set string value to vector
    void SetStringValueToVector(BaseVector *vec, int32_t row, std::string_view &value) const;
    
    // Dispatch if based on output type
    void DispatchIf(BaseVector *condVec, BaseVector *trueVec, BaseVector *falseVec, 
                    const DataTypePtr &outputType, BaseVector *&result) const;
    
    // Template implementations for different types
    template<typename T>
    void IfNumeric(BaseVector *condVec, BaseVector *trueVec, BaseVector *falseVec, 
                   BaseVector *&result, const DataTypePtr &outputType) const;
    
    void IfString(BaseVector *condVec, BaseVector *trueVec, BaseVector *falseVec, 
                  BaseVector *&result, const DataTypePtr &outputType) const;
    
    void IfBoolean(BaseVector *condVec, BaseVector *trueVec, BaseVector *falseVec, 
                   BaseVector *&result, const DataTypePtr &outputType) const;
};
}
