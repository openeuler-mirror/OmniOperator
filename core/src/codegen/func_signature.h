#ifndef __FUNC_SIGNATURE_H__
#define __FUNC_SIGNATURE_H__
#include <vector>
#include <map>
#include <set>
#include <string>
#include <cstring>

#include "../common/expressions.h"

using namespace std;
using namespace omniruntime::expressions;


class FunctionSignature {
public:
    FunctionSignature(std::string name, vector<DataType> params, DataType returnType, void* address);
    ~FunctionSignature();
    std::string getId();
    std::string getName();
    std::vector<DataType> getParams();
    DataType getReturnType();
    void* getFunctionAddress();
private:
    std::string func_name;
    std::vector<DataType> param_types;
    DataType ret_type;
    void* func_address;
};

vector<FunctionSignature> getFunctionSignatures();

#endif