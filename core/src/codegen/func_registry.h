#ifndef __FUNC_REGISTERY_H__
#define __FUNC_REGISTERY_H__
#include <vector>
#include <map>
#include "../common/expressions.h"

using namespace std;

class FunctionSignature {


    public:
        std::string getId();
        std::string getName();
        std::vector<DataType> getParams();
        DataType getReturnType(); 
        uint64_t getFunctionAddress();
    private:
        std::string func_name;
        std::vector<DataType> param_types_;
        DataType ret_type_;
        uint64_t func_address;
};

class FunctionRegistry {

    public:
        bool containsFunc(FunctionSignature signature);
        void getFunction(FunctionSignature signature);
    private:
        map<string,FunctionSignature*> registery;
};

#endif