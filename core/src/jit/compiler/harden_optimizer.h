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
            HardenOptimizer(unsigned optLevel, const std::set<std::string> &specializedModules)
            {
                pmb.OptLevel = optLevel;
                conf = *Config::GetConf();
                this->specializedModules = specializedModules;
            }

            HardenOptimizer(unsigned optLevel, const Config &optConfig, const std::set<std::string> &specializedModules)
            {
                pmb.OptLevel = optLevel;
                conf = optConfig;
                this->specializedModules = specializedModules;
            }

            ~HardenOptimizer(){}

            llvm::Expected<llvm::orc::ThreadSafeModule>
            operator()(llvm::orc::ThreadSafeModule TSM,
                       const llvm::orc::MaterializationResponsibility &);

        private:
            llvm::PassManagerBuilder pmb;
            Config conf;
            std::set<std::string> specializedModules;

            void populatePass(llvm::legacy::FunctionPassManager &FPM, llvm::legacy::PassManager &MPM);
        };
    }
}
#endif
