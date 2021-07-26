/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2027. All rights reserved.
 * Description:registry external function
 */
#include <vector>
#include <map>
#include <set>
#include <string>
#include <cstring>

#include "./external_func_registry.h"

using namespace std;
using namespace omniruntime::expressions;


// Returns a set containing strings of all the external function names
// Add the names of your own functions here
set<string> GetAllExternalFunctionNames()
{
    set<string> allExtFnNames;
    // Insert all the names into the set here
    allExtFnNames.insert(id_int32_str);
    allExtFnNames.insert(add1_int32_str);



    return allExtFnNames;
}


// Returns a map from function name to return type
// Add the return types of your own functions here
map<string, DataType> GetFuncReturnTypeMap()
{
    map<string, DataType> nameToRetType;
    // Insert all the name and return type pairs here
    nameToRetType[id_int32_str] = DataType::INT32D;
    nameToRetType[add1_int32_str] = DataType::INT32D;


    return nameToRetType;
}


// Add the signatures for your own functions here
// Create a new conditional branch for it
// Possible DataTypes are: BOOLD, INT32D, INT64D, DOUBLED, STRINGD
FunctionSignature* GetExternalSignature(string funcName)
{
    if (funcName == add1_int32_str) {
        // Vector containing parameter types
        vector <DataType> add1_int32_types{DataType::INT32D};
        // Return type
        DataType retType = DataType::INT32D;
        // Void pointer to function address
        void* fnAddr = reinterpret_cast<void *>(Add1Int32);
        auto add1Int32Sig = std::make_unique<FunctionSignature>(add1_int32_str, add1_int32_types, retType, fnAddr)
                .release();
        return add1Int32Sig;
    } else {
        // Vector containing parameter types
        vector <DataType> id_int32_types{DataType::INT32D};
        // Return type
        DataType retType = DataType::INT32D;
        // Void pointer to function address
        void* fnAddr = reinterpret_cast<void *>(IdInt32);
        auto idInt32Sig = std::make_unique<FunctionSignature>(id_int32_str, id_int32_types, retType, fnAddr).release();
        return idInt32Sig;
    }
}