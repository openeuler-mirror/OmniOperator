/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:registry external function
 */
#include <vector>
#include <map>
#include <set>
#include <string>
#include <iostream>
#include <algorithm>
#include <dlfcn.h>

#include "./external_func_registry.h"
#include "../../util/debug.h"

using namespace std;
using namespace omniruntime::expressions;


ExternalFuncRegistry::ExternalFuncRegistry()
{
    if (!g_hasInitialized) {
        UpdateFuncSigMap();
        g_hasInitialized = true;
    }
}

ExternalFuncRegistry::~ExternalFuncRegistry()
{
}


// Returns a set containing strings of all the external function names
set<string> ExternalFuncRegistry::GetAllExternalFunctionNames() const
{
    return g_allExtFnNames;
}


// Returns a map from function name to return type
map<string, DataType> ExternalFuncRegistry::GetFuncReturnTypeMap() const
{
    return g_nameToRetType;
}


// Add the signatures for your own functions here
// Create a new conditional branch for it
// Possible DataTypes are: BOOLD, INT32D, INT64D, DOUBLED, STRINGD
FunctionSignature ExternalFuncRegistry::GetExternalSignature(string funcName) const
{
    return g_funcSignatureMap[funcName];
}

int64_t ExternalFuncRegistry::FetchHandle() const
{
    // Get symbols from .so file
    auto handle = dlopen(EXTERNAL_FUNCTIONS_LIB_PATH.c_str(), RTLD_LAZY);
    if (!handle) {
        LLVM_DEBUG_LOG("Could not open externalfunctions library file; %s\n", dlerror());
        LLVM_DEBUG_LOG("Error occurred with external functions. No external functions will be registered\n");
        return 0;
    }

    void *h = &handle;
    auto ch = static_cast<int64_t *>(h);
    return *ch;
}

ifstream ExternalFuncRegistry::FetchExternalFunctionInfo(int64_t handlePtr) const
{
    void *h = &handlePtr;
    auto ch = static_cast<void **>(h);
    void *handle = *ch;

    ifstream readRegistration;
    readRegistration.open(EXTERNAL_FUNCTIONS_FILE_PATH);
    // Check if the file was found
    if (!readRegistration.is_open()) {
        LLVM_DEBUG_LOG("Could not find externalregistration.txt file\n");
        LLVM_DEBUG_LOG("Error occurred with external functions. No external functions will be registered\n");
    }

    return readRegistration;
}

// Goes through externalregistration.txt file and adds function signatures to funcSignatureMap
// Updates allExtNames to contain names of all external functions
// Updates nameToRetType with names and return types of all external functions
void ExternalFuncRegistry::UpdateFuncSigMap() const
{
    auto handlePtr = this->FetchHandle();
    void *h = &handlePtr;
    auto ch = static_cast<void **>(h);
    void *handle = *ch;

    if (handle == nullptr) {
        return;
    }
    ifstream readRegistration = this->FetchExternalFunctionInfo(handlePtr);
    // Check if the file was found
    if (!readRegistration.is_open()) {
        return;
    }
    string currLine;
    for (string line; getline(readRegistration, currLine);) {
        currLine.erase(remove(currLine.begin(), currLine.end(), ' '), currLine.end()); // strip spaces

        // Parse the line
        int commentIdx = currLine.find("/");
        // Ignore empty line or lines that are only comments
        if (currLine.size() <= 1 || commentIdx == 0) {
            continue;
        }

        // First remove the comments
        currLine = currLine.substr(0, commentIdx);

        // Get the name
        int colonIdx = currLine.find(":");
        if (colonIdx == string::npos) {
            continue;
        }
        string fnName = currLine.substr(0, colonIdx);
        currLine = currLine.substr(colonIdx + 1);

        // Get the return type
        int arrowIdx = currLine.find("->");
        if (arrowIdx == string::npos) {
            continue;
        }
        DataType retType = StringToDataType(currLine.substr(arrowIdx + PAREN_LENGTH + 1));
        currLine = currLine.substr(0, arrowIdx).substr(1, arrowIdx - PAREN_LENGTH - 1); // remove parentheses

        // Get the argument types
        vector<DataType> argTypes;
        int leftIdx = 0;

        for (int i = 0; i < currLine.size(); i++) {
            if (currLine[i] == ',') {
                string typeStr = currLine.substr(leftIdx, i - leftIdx);
                leftIdx = i + 1;
                argTypes.push_back(StringToDataType(typeStr));
            }
        }
        // last argument
        argTypes.push_back(StringToDataType(currLine.substr(leftIdx, currLine.size() - leftIdx)));

        // Add to allExtFnNames
        g_allExtFnNames.insert(fnName);
        // Add mapping to nameToRetType map
        g_nameToRetType[fnName] = retType;

        // Create FunctionSignature and add to funcSignatureMap with function address retrieved via dlsym
        FunctionSignature funcSig (fnName, argTypes, retType, dlsym(handle, fnName.c_str()));
        g_funcSignatureMap[fnName] = funcSig;

    }

    readRegistration.close();
}