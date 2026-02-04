/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Cast function for vectorized type conversion
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
 #include <sstream>
 #include <iomanip>
 #include <cmath>
 
 namespace omniruntime::vectorization {
     using namespace omniruntime::type;
     using namespace omniruntime::vec;
     using namespace omniruntime::op;
 
 class CastFunction : public VectorFunction {
 public:
     explicit CastFunction() {}
 
     void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
                ExecutionContext *context) const override;
 
 private:
     // Get input type from vector
     DataTypeId GetInputTypeId(BaseVector *input) const;
     
     // Main dispatch function
     void DispatchCast(BaseVector *input, DataTypeId inputTypeId, const DataTypePtr &outputType, 
                       BaseVector *&result, ExecutionContext *context) const;

     // String to numeric conversions
     template<typename T>
     void CastStringToNumeric(BaseVector *input, BaseVector *&result, const DataTypePtr &outputType,
                              ExecutionContext *context) const;

     // Numeric to string conversions
     template<typename T>
     void CastNumericToString(BaseVector *input, BaseVector *&result, const DataTypePtr &outputType,
                              ExecutionContext *context) const;

     // Numeric to numeric conversions
     template<typename TInput, typename TOutput>
     void CastNumericToNumeric(BaseVector *input, BaseVector *&result, const DataTypePtr &outputType,
                               ExecutionContext *context) const;

     // Boolean conversions
     void CastToBoolean(BaseVector *input, DataTypeId inputTypeId, BaseVector *&result,
                        const DataTypePtr &outputType, ExecutionContext *context) const;
     void CastFromBoolean(BaseVector *input, BaseVector *&result, const DataTypePtr &outputType,
                          ExecutionContext *context) const;

     // String conversions
     void CastToString(BaseVector *input, DataTypeId inputTypeId, BaseVector *&result,
                       const DataTypePtr &outputType, ExecutionContext *context) const;

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

     // Convert numeric to string
     template<typename T>
     std::string NumericToString(T value) const;

     // Helper: Check if vector element is null (handles ConstVector correctly)
     bool IsNullAt(BaseVector *vec, int32_t row) const;
 };
 }
