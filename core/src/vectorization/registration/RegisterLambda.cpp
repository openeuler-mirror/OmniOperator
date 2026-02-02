/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "RegistrationHelpers.h"
#include "vectorization/functions/TransForm.h"
#include "vectorization/functions/TransformKeysValues.h"

namespace omniruntime::vectorization {

    static constexpr std::array<DataTypeId, 9> BASE_DATA_TYPES = {
            OMNI_INT,
            OMNI_LONG,
            OMNI_DOUBLE,
            OMNI_BOOLEAN,
            OMNI_SHORT,
            OMNI_VARCHAR,
            OMNI_CHAR,
            OMNI_BYTE,
            OMNI_FLOAT
    };

    template <typename FuncT>
    void RegisterTransformSingleType(const std::string& funcName, DataTypeId elemType, const std::shared_ptr<FuncT>& func) {
        VectorFunction::RegisterVectorFunction(
                funcName,
                {OMNI_ARRAY, elemType},
                OMNI_ARRAY,
                func
        );
    }

    template <typename FuncT>
    void RegisterMapTransformSingleType(const std::string& funcName, DataTypeId elemType, const std::shared_ptr<FuncT>& func) {
        VectorFunction::RegisterVectorFunction(
                funcName,
                {OMNI_MAP, elemType},
                OMNI_MAP,
                func
        );
    }

    template <typename F, size_t N>
    void BatchRegister(const std::array<DataTypeId, N>& types, F&& f) {
        (f(types[0]), f(types[1]), f(types[2]), f(types[3]), f(types[4]),
                f(types[5]), f(types[6]), f(types[7]), f(types[8]));
    }

    void RegisterLambdaFunctions(const std::string &prefix)
    {
        auto transformFunc = std::make_shared<TransformVectorFunction>();
        BatchRegister(BASE_DATA_TYPES, [&](DataTypeId elemType) {
            RegisterTransformSingleType("transform", elemType, transformFunc);
        });

        auto transformKeysFunc = std::make_shared<TransformKeysValuesVectorFunction<true>>();
        BatchRegister(BASE_DATA_TYPES, [&](DataTypeId elemType) {
            RegisterMapTransformSingleType("transform_keys", elemType, transformKeysFunc);
        });

        auto transformValuesFunc = std::make_shared<TransformKeysValuesVectorFunction<false>>();
        BatchRegister(BASE_DATA_TYPES, [&](DataTypeId elemType) {
            RegisterMapTransformSingleType("transform_values", elemType, transformValuesFunc);
        });
    }
}