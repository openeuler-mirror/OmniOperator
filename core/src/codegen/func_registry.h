/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_H
#define OMNI_RUNTIME_FUNC_REGISTRY_H

#include <unordered_map>
#include <memory>

#include "function.h"
#include "external_func_registry.h"
#include "func_registry_context.h"
#include "func_registry_decimal.h"
#include "func_registry_dictionary.h"
#include "func_registry_math.h"
#include "func_registry_hash.h"
#include "func_registry_string.h"
#include "func_registry_varchar_vector.h"

namespace omniruntime {
struct Hash {
    std::size_t operator () (const FunctionSignature *signature) const
    {
        return signature->HashCode();
    }
};
struct Equals {
    bool operator () (const FunctionSignature *s1, const FunctionSignature *s2) const
    {
        return *s1 == *s2;
    }
};

using FunctionMapPtr = std::unique_ptr<std::unordered_map<const FunctionSignature *, const Function *, Hash, Equals>>;

class FunctionRegistry {
public:
    ~FunctionRegistry();

    static const Function *LookupFunction(FunctionSignature *signature);

    static bool LookupNullFunction(FunctionSignature *signature);

    static bool IsNullExecutionContextSet(FunctionSignature *signature);

    static std::vector<std::unique_ptr<BaseFunctionRegistry>> GetFunctionRegistries();

    static std::vector<Function> &GetFunctions();

private:
    static std::vector<Function> registeredFunctions;
    static FunctionMapPtr functionRegistry;
    static FunctionMapPtr functionNullRegistry;

    static std::vector<Function> Initialize();
};
}
#endif // OMNI_RUNTIME_FUNC_REGISTRY_H
