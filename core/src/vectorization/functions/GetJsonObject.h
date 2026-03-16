/*
* Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: get_json_object function for JSON operations
 */

 #pragma once
 #include "vectorization/VectorFunction.h"
 #include "vector/vector.h"
 #include "vector/vector_helper.h"
 #include "rapidjson/document.h"
 #include <string>
 #include <string_view>
 
 namespace omniruntime::vectorization {
     using namespace omniruntime::type;
     using namespace omniruntime::vec;
     using namespace omniruntime::op;
 
     class GetJsonObjectFunction final : public VectorFunction {
     public:
         explicit GetJsonObjectFunction() {}
 
         void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
             ExecutionContext *context) const override;
 
     private:
         void ProcessJsonPath(const std::string_view &jsonStr, const std::string_view &pathStr,
             Vector<LargeStringContainer<std::string_view>> *resultVec, int32_t row) const;
        void ProcessJsonPathWithNormalizedPath(const std::string_view &jsonStr, const std::string_view &normalizedPath,
            Vector<LargeStringContainer<std::string_view>> *resultVec, int32_t row) const;
 
         std::string NormalizeJsonPath(std::string_view pathStr) const;
 
         std::string RemoveSingleQuotes(const std::string &path) const;
 
         void ExtractValueAsString(const rapidjson::Value &value,
             Vector<LargeStringContainer<std::string_view>> *resultVec, int32_t row) const;
 
         std::string FormatDouble(double value) const;
 
         std::string_view GetStringValue(BaseVector *vector, int32_t row) const;
     };
 }
 