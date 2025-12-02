/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: In expression implementation for SimpleFunction framework (无空值版)
 */

#pragma once

#include <unordered_set>
#include "SimpleFunction.h"
#include "./vectorization/VectorReaders.h"
#include "type/data_type.h"
#include "../SimpleFunctionMetadata.h"
#include <iostream>

namespace omniruntime::vectorization {

    template <typename TInput>
    struct InCoreLogic {
        std::unordered_set<TInput> candidates_;

        void initialize(const std::vector<type::DataTypePtr>& inputTypes, const config::QueryConfig& config,
            const std::vector<VectorPtr>& constantInputs)
        {
            if (constantInputs.size() != 1) {
                OMNI_THROW("InCoreLogic Error", "Constant inputs must be a single RowVector");
            }
            auto rowVec = std::dynamic_pointer_cast<vec::RowVector>(constantInputs[0]);
            if (!rowVec) {
                OMNI_THROW("InCoreLogic Error", "Constant input is not a RowVector");
            }

            for (int32_t i = 0; i < rowVec->ChildSize(); ++i) {
                auto childVec = rowVec->ChildAt(i);
                if (!childVec || childVec->GetEncoding() != OMNI_ENCODING_CONST) {
                    OMNI_THROW("InCoreLogic Error", "RowVector child is not a ConstVector");
                }
                if (childVec->IsNull(0)) {
                    continue;
                }
                ConstVectorReader<TInput> reader(childVec.get());
                TInput value = reader[0];
                candidates_.emplace(value);
            }
        }

        ALWAYS_INLINE void call(bool& result, const TInput& fieldValue)
        {
            result = (candidates_.find(fieldValue) != candidates_.end());
        }

        using exec_return_type = bool;
        using return_type = bool;
        using exec_arg_types = std::tuple<TInput>;
        using arg_types = std::tuple<TInput>;
        static constexpr int num_args = 1;
    };

    template <>
    struct InCoreLogic<std::string> {
        std::unordered_set<std::string> candidates_;

        void initialize(const std::vector<type::DataTypePtr>& inputTypes, const config::QueryConfig& config,
                        const std::vector<VectorPtr>& constantInputs)
        {
            if (constantInputs.size() != 1) {
                OMNI_THROW("InCoreLogic Error", "Constant inputs must be a single RowVector");
            }
            auto rowVec = std::dynamic_pointer_cast<vec::RowVector>(constantInputs[0]);
            if (!rowVec) {
                OMNI_THROW("InCoreLogic Error", "Constant input is not a RowVector");
            }

            for (int32_t i = 0; i < rowVec->ChildSize(); ++i) {
                auto childVec = rowVec->ChildAt(i);
                if (!childVec || childVec->GetEncoding() != OMNI_ENCODING_CONST) {
                    OMNI_THROW("InCoreLogic Error", "RowVector child is not a ConstVector");
                }
                if (childVec->IsNull(0)) {
                    continue;
                }
                ConstVectorReader<std::string> reader(childVec.get());
                std::string value = reader[0];
                candidates_.emplace(value);
            }
        }

        ALWAYS_INLINE void call(bool& result, const std::string_view& fieldValue)
        {
            result = (candidates_.find(std::string(fieldValue)) != candidates_.end());
        }

        using exec_return_type = bool;
        using return_type = bool;
        using exec_arg_types = std::tuple<std::string_view>;
        using arg_types = std::tuple<std::string_view>;
        static constexpr int num_args = 1;
    };

    std::shared_ptr<VectorFunction> makeInSimpleFunction(const std::string& name, const std::vector<type::DataTypeId>& inputArgs,
        const config::QueryConfig& config, const std::vector<VectorPtr>& constantInputs)
    {
        if (inputArgs.size() != 2 || inputArgs[1] != OMNI_ROW) {
            OMNI_THROW("InFunction Error:", "Input args must be (fieldType, row), but got " + std::to_string(inputArgs.size()) + " args");
        }

        std::vector<type::DataTypePtr> inputTypes;
        inputTypes.reserve(inputArgs.size());
        for (auto typeId : inputArgs) {
            inputTypes.push_back(std::make_shared<type::DataType>(typeId));
        }

        switch (inputArgs[0]) {
            case type::OMNI_INT: {
                using InIntLogic = InCoreLogic<int32_t>;
                using InIntHolder = FunctionHolder<InIntLogic, bool, int32_t>;
                return std::make_shared<SimpleFunction<InIntHolder>>(inputTypes, config, constantInputs);
            }
            case type::OMNI_LONG: {
                using InLongLogic = InCoreLogic<int64_t>;
                using InLongHolder = FunctionHolder<InLongLogic, bool, int64_t>;
                return std::make_shared<SimpleFunction<InLongHolder>>(inputTypes, config, constantInputs);
            }
            case type::OMNI_VARCHAR: {
                using InStringLogic = InCoreLogic<std::string>;
                using InStringHolder = FunctionHolder<InStringLogic, bool, std::string_view>;
                return std::make_shared<SimpleFunction<InStringHolder>>(inputTypes, config, constantInputs);
            }
            case type::OMNI_FLOAT: {
                using InFloatLogic = InCoreLogic<float>;
                using InFloatHolder = FunctionHolder<InFloatLogic, bool, float>;
                return std::make_shared<SimpleFunction<InFloatHolder>>(inputTypes, config, constantInputs);
            }
            case type::OMNI_DOUBLE: {
                using InDoubleLogic = InCoreLogic<double>;
                using InDoubleHolder = FunctionHolder<InDoubleLogic, bool, double>;
                return std::make_shared<SimpleFunction<InDoubleHolder>>(inputTypes, config, constantInputs);
            }
            default:
                OMNI_THROW("InFunction Error:", "Unsupported Field Type: " + std::to_string(inputArgs[0]));
        }
    }

    std::shared_ptr<codegen::FunctionSignature> makeInSignature(const std::string& name, type::DataTypeId fieldType) {
        return codegen::FunctionSignatureBuilder()
                .FuncName(name)
                .ReturnType(OMNI_BOOLEAN)
                .ArgumentType(fieldType)
                .ArgumentType(OMNI_ROW)
                .Build();
    }

    std::vector<std::shared_ptr<codegen::FunctionSignature>> getInSignatures(const std::string& name) {
        std::vector<std::shared_ptr<codegen::FunctionSignature>> signatures;
        signatures.emplace_back(makeInSignature(name, OMNI_INT));
        signatures.emplace_back(makeInSignature(name, OMNI_LONG));
        signatures.emplace_back(makeInSignature(name, OMNI_VARCHAR));
        signatures.emplace_back(makeInSignature(name, OMNI_FLOAT));
        signatures.emplace_back(makeInSignature(name, OMNI_DOUBLE));
        return signatures;
    }
}