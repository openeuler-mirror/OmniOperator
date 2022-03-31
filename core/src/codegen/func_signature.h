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
#include <type/data_type.h>

class FunctionSignature {
public:
    FunctionSignature(const std::string name, std::vector<omniruntime::type::DataTypeId> params,
        const omniruntime::type::DataTypeId &returnType);
    FunctionSignature(const FunctionSignature &fs);
    FunctionSignature &operator = (FunctionSignature other);
    bool operator == (const FunctionSignature &other) const;
    ~FunctionSignature();
    std::string GetName() const;
    const std::vector<omniruntime::type::DataTypeId> &GetParams() const;
    omniruntime::type::DataTypeId GetReturnType() const;
    size_t HashCode() const;
    std::string ToString() const;
private:
    std::string funcName;
    std::vector<omniruntime::type::DataTypeId> paramTypes {};
    omniruntime::type::DataTypeId retType;
};

#endif