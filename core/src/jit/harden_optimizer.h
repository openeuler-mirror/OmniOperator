#ifndef __SIMPLEOPTIMIZER_H__
#define __SIMPLEOPTIMIZER_H__

#include <llvm/ExecutionEngine/Orc/Core.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/Support/Error.h>
#include <llvm/Transforms/IPO/PassManagerBuilder.h>
#include "hammer_config.h"
#include <llvm/IR/LegacyPassManager.h>
namespace omniruntime {
namespace codegen {

class HardenOptimizer {
public:
    HardenOptimizer(unsigned OptLevel) {
        pmb.OptLevel = OptLevel;
        conf = *HammerConfig::getConf();
    }

    HardenOptimizer(unsigned OptLevel, HammerConfig &opt_config) {
        pmb.OptLevel = OptLevel;
        conf = opt_config;
    }

    llvm::Expected<llvm::orc::ThreadSafeModule>
    operator()(llvm::orc::ThreadSafeModule TSM,
                const llvm::orc::MaterializationResponsibility &);

private:
    llvm::PassManagerBuilder pmb;
    HammerConfig conf;

    void populatePass(legacy::FunctionPassManager &FPM, legacy::PassManager &MPM);
};
} // end of namespace codegen
} // end of namespace omniruntime
#endif
