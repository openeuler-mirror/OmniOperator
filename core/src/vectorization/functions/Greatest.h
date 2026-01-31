/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Greatest function for vectorized comparison expressions
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vectorization/functions/Comparisons.h"
#include "vector/array_vector.h"
#include "type/data_operations.h"
#include "util/debug.h"
#include "util/type_util.h"
#include "vector/vector_helper.h"
#include <vector>
#include <string_view>
#include <iostream>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;
    using namespace omniruntime::op;

/// Greatest function
/// greatest(arg1, arg2, ..., argN) -> T
/// Returns the greatest value among all input arguments.
/// NULL values are ignored - if any non-NULL value exists, that value is used.
/// If all values are NULL, the result is NULL.
/// For floating point types, NaN is treated as greater than any other value (Spark SQL semantics).
class GreatestFunction : public VectorFunction {
public:
    explicit GreatestFunction() {}

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
    
    // Dispatch greatest based on output type
    void DispatchGreatest(const std::vector<BaseVector *> &argVectors, const DataTypePtr &outputType,
                          BaseVector *&result) const;
    
    // Template implementations for different types
    template<typename T>
    void GreatestNumeric(const std::vector<BaseVector *> &argVectors, BaseVector *&result,
                        const DataTypePtr &outputType) const;
    
    void GreatestString(const std::vector<BaseVector *> &argVectors, BaseVector *&result,
                        const DataTypePtr &outputType) const;
    
    void GreatestBoolean(const std::vector<BaseVector *> &argVectors, BaseVector *&result,
                         const DataTypePtr &outputType) const;
};

// Factory function declaration
std::shared_ptr<VectorFunction> makeGreatest(const std::string &name, 
    const std::vector<DataTypeId> &inputArgs, const config::QueryConfig &);

// Signatures for greatest function
std::vector<std::shared_ptr<codegen::FunctionSignature>> GreatestSignatures();

}
