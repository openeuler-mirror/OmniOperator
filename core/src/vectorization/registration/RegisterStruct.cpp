/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Registration of struct functions
 */

#include <string>
#include <memory>
#include "RegistrationHelpers.h"
#include "vectorization/VectorFunction.h"
#include "vectorization/functions/NameStruct.h"
#include "codegen/func_signature.h"
#include "type/data_type.h"

namespace omniruntime::vectorization {
using namespace omniruntime::type;
using namespace omniruntime::codegen;

void RegisterStructFunctions(const std::string& prefix)
{
    auto factory = [](const std::string&, const std::vector<DataTypeId>&,
                      const config::QueryConfig&) -> std::shared_ptr<VectorFunction> {
        return std::make_shared<NameStructFunction>();
    };
    std::vector<std::shared_ptr<FunctionSignature>> sigs;
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_BOOLEAN}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_BYTE}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_SHORT}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_INT}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_LONG}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_FLOAT}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_DOUBLE}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_VARCHAR}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_DATE32}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_TIMESTAMP}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_DECIMAL64}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_DECIMAL128}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_VARBINARY}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_ARRAY}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_MAP}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_ROW}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_INT, OMNI_DOUBLE}, OMNI_ROW));
    sigs.push_back(std::make_shared<FunctionSignature>(
        prefix + "name_struct", std::vector<DataTypeId>{OMNI_BOOLEAN, OMNI_INT, OMNI_VARCHAR}, OMNI_ROW));
    VectorFunction::RegisterVectorFunctionFactory(sigs, factory);
}

}
