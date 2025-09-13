/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: registry function
 */
#ifndef OMNI_RUNTIME_FUNC_REGISTRY_H
#define OMNI_RUNTIME_FUNC_REGISTRY_H

#include <unordered_map>
#include <memory>
#include <mutex>

#include "function.h"
#include "func_registry_context.h"
#include "func_registry_decimal.h"
#include "func_registry_dictionary.h"
#include "func_registry_math.h"
#include "func_registry_hash.h"
#include "func_registry_might_contain.h"
#include "func_registry_string.h"
#include "func_registry_varchar_vector.h"
#include "func_registry_hive_udf.h"
#include "func_registry_datetime.h"

#include "batch_func_registry_decimal.h"
#include "batch_func_registry_dictionary.h"
#include "batch_func_registry_math.h"
#include "batch_func_registry_hash.h"
#include "batch_func_registry_string.h"
#include "batch_func_registry_varchar_vector.h"
#include "batch_func_registry_util.h"
#include "batch_func_registry_datetime.h"

namespace omniruntime::codegen {
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
using HiveUdfMapPtr = std::unique_ptr<std::unordered_map<std::string, std::string>>;

class FunctionRegistry {
public:
    ~FunctionRegistry();

    static const Function *LookupFunction(FunctionSignature *signature);

    static bool LookupNullFunction(FunctionSignature *signature);

    static bool IsNullExecutionContextSet(FunctionSignature *signature);

    static const std::string &LookupHiveUdf(const std::string &udfName);

    static std::vector<std::unique_ptr<BaseFunctionRegistry>> GetRowFunctionRegistries();

    static std::vector<std::unique_ptr<BaseFunctionRegistry>> GetBatchFunctionRegistries();

    static std::vector<Function> &GetRowFunctions();

    static std::vector<Function> &GetBatchFunctions();

    static void InitHiveUdfMap();

private:
    static std::vector<Function> registeredRowFunctions;
    static std::vector<Function> registeredBatchFunctions;
    static FunctionMapPtr functionRegistry;
    static FunctionMapPtr functionNullRegistry;
    static HiveUdfMapPtr hiveUdfMap;
    static std::once_flag initHiveUdfMap;

    static std::vector<Function> InitializeRowFunc();
    static std::vector<Function> InitializeBatchFunc();
};
}
#endif // OMNI_RUNTIME_FUNC_REGISTRY_H
