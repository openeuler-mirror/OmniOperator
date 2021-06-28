#include "harden_optimizer.h"

#include "llvm/Transforms/Scalar.h"
#include <llvm/Analysis/SparsePropagation.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include "llvm/Analysis/TargetTransformInfo.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/ExecutionEngine/Orc/ObjectLinkingLayer.h"
#include "llvm/ExecutionEngine/Orc/ThreadSafeModule.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Transforms/Utils/Cloning.h"

namespace omniruntime {
    namespace jit {
        using namespace llvm;
        using namespace llvm::orc;

        void HardenOptimizer::populatePass(legacy::FunctionPassManager &FPM, legacy::PassManager &MPM) {
            conf.populate(FPM, MPM);
        }

        Expected<ThreadSafeModule>
        HardenOptimizer::operator()(ThreadSafeModule TSM,
                                    const MaterializationResponsibility &) {

            Module &M = *TSM.getModuleUnlocked();
            if (this->specializedModules.count(M.getName().str()) == 0) {
                return std::move(TSM);
            }
            int original_count = M.getInstructionCount();

            legacy::FunctionPassManager FPM(&M);
            legacy::PassManager MPM;

            populatePass(FPM, MPM);

            pmb.populateFunctionPassManager(FPM);
            pmb.populateModulePassManager(MPM);

            FPM.doInitialization();
            for (Function &F : M) {
                FPM.run(F);
            }
            FPM.doFinalization();
            MPM.run(M);

            // dbgs() << "--- AFTER OPTIMIZATION ---\n" << M << "\n";

            int new_count = M.getInstructionCount();
            // outs() << "\n " << M.getName() << " instruct count: original: " << original_count << " new:" << new_count;

            return std::move(TSM);
        }
    }
}
