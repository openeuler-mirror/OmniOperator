/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: visitor class for expressions
 */

#include "RegistrationHelpers.h"
#include "vectorization/functions/TransForm.h"
#include "vectorization/functions/TransformKeysValues.h"
#include "vectorization/functions/MapFilter.h"
#include "vectorization/functions/ZipWith.h"
#include "vectorization/functions/ForAll.h"
#include "vectorization/functions/Exists.h"
#include "vectorization/functions/ArrayFilter.h"
#include "vectorization/functions/MapZipWith.h"

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

    template <typename FuncT>
    void RegisterZipWithSingleType(const std::string& funcName, DataTypeId elemType, const std::shared_ptr<FuncT>& func) {
        VectorFunction::RegisterVectorFunction(
                funcName,
                {OMNI_ARRAY, OMNI_ARRAY, elemType},
                OMNI_ARRAY,
                func
        );
    }

    template <typename FuncT>
    void RegisterMapZipWithSingleType(const std::string& funcName, DataTypeId elemType, const std::shared_ptr<FuncT>& func) {
        VectorFunction::RegisterVectorFunction(
                funcName,
                {OMNI_MAP, OMNI_MAP, elemType},
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
        RegisterTransformSingleType("transform", OMNI_ARRAY, transformFunc);

        auto transformKeysFunc = std::make_shared<TransformKeysValuesVectorFunction<true>>();
        BatchRegister(BASE_DATA_TYPES, [&](DataTypeId elemType) {
            RegisterMapTransformSingleType("transform_keys", elemType, transformKeysFunc);
        });

        auto transformValuesFunc = std::make_shared<TransformKeysValuesVectorFunction<false>>();
        BatchRegister(BASE_DATA_TYPES, [&](DataTypeId elemType) {
            RegisterMapTransformSingleType("transform_values", elemType, transformValuesFunc);
        });

        auto mapFilterFunc = std::make_shared<MapFilterVectorFunction>();
        BatchRegister(BASE_DATA_TYPES, [&](DataTypeId elemType) {
            RegisterMapTransformSingleType("map_filter", elemType, mapFilterFunc);
        });

        auto zipWithFunc = std::make_shared<ZipWithVectorFunction>();
        BatchRegister(BASE_DATA_TYPES, [&](DataTypeId elemType) {
            RegisterZipWithSingleType("zip_with", elemType, zipWithFunc);
        });

        auto forallFunc = std::make_shared<ForAllVectorFunction>();
        VectorFunction::RegisterVectorFunction(
                "forall",
                {OMNI_ARRAY, OMNI_BOOLEAN},
                OMNI_BOOLEAN,
                forallFunc
        );

        auto existsFunc = std::make_shared<ExistsVectorFunction>();
        VectorFunction::RegisterVectorFunction(
                "exists",
                {OMNI_ARRAY, OMNI_BOOLEAN},
                OMNI_BOOLEAN,
                existsFunc
        );

        auto arrayFilterFunc = std::make_shared<ArrayFilterVectorFunction>();
        BatchRegister(BASE_DATA_TYPES, [&](DataTypeId elemType) {
            RegisterTransformSingleType("filter", elemType, arrayFilterFunc);
        });

        auto mapZipWithFunc = std::make_shared<MapZipWithVectorFunction>();
        BatchRegister(BASE_DATA_TYPES, [&](DataTypeId elemType) {
            RegisterMapZipWithSingleType("map_zip_with", elemType, mapZipWithFunc);
        });
    }
}