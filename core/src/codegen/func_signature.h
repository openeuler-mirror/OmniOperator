/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:
 */
#ifndef __FUNC_SIGNATURE_H__
#define __FUNC_SIGNATURE_H__
#include <vector>
#include <map>
#include <set>
#include <string>
#include <vector/vector_type.h>


class FunctionSignature {
public:
    FunctionSignature();
    FunctionSignature(const std::string name, std::vector<omniruntime::vec::VecTypeId> params,
                      const omniruntime::vec::VecTypeId &returnType, void* address);
    FunctionSignature(const FunctionSignature &fs);
    FunctionSignature &operator=(FunctionSignature other)
    {
        std::swap(funcName, other.funcName);
        std::swap(paramTypes, other.paramTypes);
        std::swap(retType, other.retType);
        std::swap(funcAddress, other.funcAddress);
        return *this;
    }
    ~FunctionSignature();
    std::string GetId() const;
    std::string GetName() const;
    std::vector<omniruntime::vec::VecTypeId> GetParams() const;
    omniruntime::vec::VecTypeId GetReturnType() const;
    void* GetFunctionAddress() const;
private:
    std::string funcName = "";
    std::vector<omniruntime::vec::VecTypeId> paramTypes {};
    omniruntime::vec::VecTypeId retType;
    void* funcAddress = nullptr;
};

#endif