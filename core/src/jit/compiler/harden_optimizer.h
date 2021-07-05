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
            HardenOptimizer(unsigned OptLevel, std::set<std::string> &specializedModules) {
                pmb.OptLevel = OptLevel;
                conf = *Config::getConf();
                this->specializedModules = specializedModules;
            }

            HardenOptimizer(unsigned OptLevel, Config &opt_config, std::set<std::string> &specializedModules) {
                pmb.OptLevel = OptLevel;
                conf = opt_config;
                this->specializedModules = specializedModules;
            }

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
