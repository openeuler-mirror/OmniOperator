/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: MapConcat function implementation
 */

#include "MapConcatFunction.h"
#include "codegen/func_signature.h"

namespace omniruntime::vectorization {
using namespace omniruntime::codegen;

std::shared_ptr<VectorFunction> makeMapConcat(const std::string &name,
    const std::vector<DataTypeId> &inputArgs, const config::QueryConfig &)
{
    int32_t numArgs = static_cast<int32_t>(inputArgs.size());
    return std::make_shared<MapConcatFunction>(numArgs);
}

std::vector<std::shared_ptr<FunctionSignature>> MapConcatSignatures()
{
    std::vector<std::shared_ptr<FunctionSignature>> signatures;
    signatures.emplace_back(FunctionSignature::Variadic("map_concat", OMNI_MAP, OMNI_MAP, 1));
    return signatures;
}

}
