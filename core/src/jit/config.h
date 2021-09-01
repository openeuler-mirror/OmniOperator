/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_CONFIG_H__
#define __OMNI_JIT_CONFIG_H__

#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Pass.h>

namespace omniruntime {
namespace jit {
enum class Optimization {
    EARLY_CSE,
    SROA,
    REASSOCIATE,
    AGGRESIVE_DCE,
    CFG_SIMPLIFICATION,
    MERGED_LOAD_STORE_MOTION,
    IPSCCP,
    SCCP,
    LOWER_ATOMIC,
    CONSTANT_HOISTING,
    NEW_GVN,
    CALLED_VALUE_PROPAGATION,
    LOOP_LOAD_ELIMINATION,
    LOOP_UNROLL,
    PARTIALLY_INLINE_LIB_CALLS,
    SEPARATE_CONST_OFFSET_FROM_GEP,
    LOOP_STRENGTH_REDUCE,
    INDUCTIVE_RANGE_CHECK_ELIMINATION,
    LOOP_DISTRIBUTE,
    LOOP_SIMPLIFY_CFG,
    LOOP_INST_SIMPLIFY,
    LOOP_EXTRACTOR,
    LOOP_VERSIONING_LICM,
    LOOP_DELETION,
    LOOP_DATA_PREFETCH,
    LICM,
    FUNCTION_INLINING,
    LOWER_CONSTANT_INTRINSICS,
    IND_VAR_SIMPLIFY,
    LOOP_UNSWITCH,
    MERGE_ICMPS_LEGACY,
    DEAD_STORE_ELIMINATION,
    STRUCTURIZE_CFG,
    DEAD_CODE_ELIMINATION
};

enum class ModuleOptimization {
    CALLED_VALUE_PROPAGATION,
    CONSTANT_MERGE,
    NEW_GVN,
    TAIL_CALL_ELIMINATION,
    SEPARATE_CONST_OFFSET_FROM_GEP,
    SIMPLE_LOOP_UNROLL,
    FUNCTION_INLINING,
    IND_VAR_SIMPLIFY,
    DEAD_ARG_ELIMINATION,
    GLOBAL_OPTIMIZER,
    IPSCCP,
    PARTIALLY_INLINE_LIB_CALLS,
    MERGE_ICMPS_LEGACY,
    PARTIAL_INLINING,
    CFG_SIMPLIFICATION,
    LOWER_CONSTANT_INTRINSICS,
    PRUNE_EH,
    STRUCTURIZE_CFG,
    MEM_CPY_OPT,
    AGGRESIVE_DCE
};

class Config {
public:
    Config()
    {
        InitFuncPass();
        InitModulePass();
    }

    ~Config() {}

    void populate(llvm::legacy::FunctionPassManager &FPM, llvm::legacy::PassManager &MPM,
        const std::vector<Optimization> &optimizations, const std::vector<ModuleOptimization> &moduleOptimizations);

    static Config *GetConf()
    {
        return new Config();
    };

private:
    static const int NUM_FUNC_OPTIMIZATIONS = 34;
    static const int NUM_MODULE_OPTIMIZATIONS = 20;
    static const int DEFAULT_OPT_LEVEL = 2;
    static const int DEFAULT_LOOP_UNROLL_COUNT = 10;
    static const int DEFAULT_LOOP_UNROLL_THRESHOLD = -1;

    llvm::Pass *(*func_pass[NUM_FUNC_OPTIMIZATIONS])();

    llvm::Pass *(*module_pass[NUM_MODULE_OPTIMIZATIONS])();

    void InitFuncPass();

    void InitModulePass();
};
}
}

#endif