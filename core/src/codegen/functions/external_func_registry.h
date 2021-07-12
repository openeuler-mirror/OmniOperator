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
set<string> getAllExternalFunctionNames();


// Returns a map from function name to return type
// Modify in external_func_registry.cpp
map<string, DataType> getFuncReturnTypeMap();


// Add the signatures for your own functions here
// Modify in external_func_registry.cpp
FunctionSignature* getExternalSignature(std::string funcName);



#endif