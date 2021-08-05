/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry external function
 */
#ifndef __EXTERNAL_FUNC_REGISTERY_H__
#define __EXTERNAL_FUNC_REGISTERY_H__
#include <vector>
#include <map>
#include <set>
#include <string>
#include <cstring>


#include "../../common/expressions.h"
#include "../func_signature.h"
#include "./mathfunctions.h"
#include "./stringfunctions.h"
#include "./externalfunctions.h"


// Returns a set containing strings of all the external function names
// Modify in external_func_registry.cpp
std::set<std::string> GetAllExternalFunctionNames();


// Returns a map from function name to return type
// Modify in external_func_registry.cpp
std::map<std::string, DataType> GetFuncReturnTypeMap();


// Add the signatures for your own functions here
// Modify in external_func_registry.cpp
FunctionSignature* GetExternalSignature(std::string funcName);



#endif