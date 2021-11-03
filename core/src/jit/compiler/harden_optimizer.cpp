/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
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

        void HardenOptimizer::populatePass(legacy::FunctionPassManager &FPM, legacy::PassManager &MPM)
        {
            conf->populate(FPM, MPM, this->optimizations, this->moduleOptimizations);
        }

        Expected<ThreadSafeModule> HardenOptimizer::operator()(ThreadSafeModule TSM,
            const MaterializationResponsibility &)
        {

            Module &M = *TSM.getModuleUnlocked();
            if (this->specializedModules.count(M.getName().str()) == 0) {
                return std::move(TSM);
            }

            using llvm::Function;
            std::set<std::string> specializedFuncNames = this->specializedModules.at(M.getName().str());

            legacy::FunctionPassManager FPM(&M);
            legacy::PassManager MPM;

            populatePass(FPM, MPM);

            pmb.populateFunctionPassManager(FPM);
            pmb.populateModulePassManager(MPM);

            FPM.doInitialization();
            for (Function &F : M) {
                if (specializedFuncNames.count(F.getName().str()) > 0) {
                    bool optimized = FPM.run(F);
                    if (optimized) {
#ifdef DEBUG_LLVM
                        outs() << "Specialized function optimized: " + F.getName().str() << "\n";
#endif
                    }
                }
            }
            FPM.doFinalization();

            bool optimized = MPM.run(M);
            if (optimized) {
#ifdef DEBUG_LLVM
                outs() << "Module optimized: " + M.getName().str() << "\n";
                M.print(outs(), nullptr);
#endif
            }

            return std::move(TSM);
        }
    }
}
