/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OPTIMIZER_H__
#define __OPTIMIZER_H__

#include <llvm/ExecutionEngine/Orc/Core.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/Support/Error.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include <llvm/IR/LegacyPassManager.h>
#include "../config.h"

#include <set>

namespace omniruntime {
namespace jit {
class HardenOptimizer {
public:
    HardenOptimizer(unsigned optLevel, const std::vector<Optimization> &optimizations,
        const std::vector<ModuleOptimization> &moduleOptimizations,
        std::map<std::string, std::set<std::string>> &specializedModules)
    {
        pmb.OptLevel = optLevel;
        conf = *Config::GetConf();
        this->specializedModules = specializedModules;

        this->optimizations = optimizations;
        if (optimizations.empty()) {
            this->optimizations = defaultOptimizations;
        }
        this->moduleOptimizations = moduleOptimizations;
        if (moduleOptimizations.empty()) {
            this->moduleOptimizations = defaultModuleOptimizations;
        }
    }

    HardenOptimizer(unsigned optLevel, const std::vector<Optimization> &optimizations,
        const std::vector<ModuleOptimization> &moduleOptimizations, const Config &optConfig,
        std::map<std::string, std::set<std::string>> &specializedModules)
    {
        pmb.OptLevel = optLevel;
        conf = optConfig;
        this->specializedModules = specializedModules;

        this->optimizations = optimizations;
        if (optimizations.empty()) {
            this->optimizations = defaultOptimizations;
        }
        this->moduleOptimizations = moduleOptimizations;
        if (moduleOptimizations.empty()) {
            this->moduleOptimizations = defaultModuleOptimizations;
        }
    }

    ~HardenOptimizer() {}

    llvm::Expected<llvm::orc::ThreadSafeModule> operator () (llvm::orc::ThreadSafeModule TSM,
        const llvm::orc::MaterializationResponsibility &);

private:
    llvm::PassManagerBuilder pmb;
    Config conf;
    std::map<std::string, std::set<std::string>> specializedModules;
    std::vector<Optimization> optimizations;
    std::vector<ModuleOptimization> moduleOptimizations;

    std::vector<Optimization> defaultOptimizations = {
        Optimization::SCCP,
        Optimization::SROA,
        Optimization::NEW_GVN,
        Optimization::INDUCTIVE_RANGE_CHECK_ELIMINATION,
        Optimization::IND_VAR_SIMPLIFY,
        Optimization::LICM,
        Optimization::LOOP_UNROLL,
        Optimization::LOOP_UNSWITCH,
        Optimization::LOOP_LOAD_ELIMINATION,
        Optimization::INDUCTIVE_RANGE_CHECK_ELIMINATION,
        Optimization::IND_VAR_SIMPLIFY,
        Optimization::LOOP_INST_SIMPLIFY,
        Optimization::LOOP_SIMPLIFY_CFG,
        Optimization::MERGED_LOAD_STORE_MOTION,
        Optimization::MERGE_ICMPS_LEGACY,
        Optimization::AGGRESIVE_DCE,
        Optimization::DEAD_STORE_ELIMINATION,
    };

    std::vector<ModuleOptimization> defaultModuleOptimizations = {
        ModuleOptimization::PRUNE_EH,
        ModuleOptimization::FUNCTION_INLINING
    };

    void populatePass(llvm::legacy::FunctionPassManager &FPM, llvm::legacy::PassManager &MPM);
};
}
}
#endif
