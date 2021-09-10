/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry external function
 */
#ifndef __EXTERNAL_FUNC_REGISTRY_H__
#define __EXTERNAL_FUNC_REGISTRY_H__

#include <map>
#include <set>
#include <string>
#include <fstream>


#include "../../common/expressions.h"
#include "../func_signature.h"
#include "../../../libconfig.h"

#include <memory>

namespace {
    // Only updated once
    // Initialized in external_func_registry.cpp
    static std::set<std::string> g_allExtFnNames;
    static std::map<std::string, omniruntime::expressions::DataType> g_nameToRetType;
    static std::map<std::string, FunctionSignature> g_funcSignatureMap;

    // Tells whether UpdateFuncSigMap has been called so that it only needs to be called once
    // Initialized in external_func_registry.cpp
    static bool g_hasInitialized;

    const std::string EXTERNAL_FUNCTIONS_FILE_PATH = "/etc/externalfunctions/externalregistration.conf";
    const std::string EXTERNAL_FUNCTIONS_LIB_PATH = GetLibPath() + "externalfunctions.so";
    const int32_t PAREN_LENGTH = 1;
}

class ExternalFuncRegistry {
public:
    ExternalFuncRegistry();
    ~ExternalFuncRegistry();
    
    // Returns a set containing strings of all the external function names
    std::set<std::string> GetAllExternalFunctionNames() const;

    // Returns a map from function name to return type
    std::map<std::string, omniruntime::expressions::DataType> GetFuncReturnTypeMap() const;

    // Add the signatures for your own functions here
    FunctionSignature GetExternalSignature(std::string funcName) const;

    // Helper functions for UpdateFuncSigMap
    int64_t FetchHandle() const;
    std::ifstream FetchExternalFunctionInfo(int64_t handle) const;

    // Uses externalregistration.txt to update funcSignatureMap
    // Also updates allExtFnNames and FuncRetTypeMap
    void UpdateFuncSigMap() const;
};


#endif