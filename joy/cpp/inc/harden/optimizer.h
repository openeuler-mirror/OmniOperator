#ifndef __OPTIMIZER_H__
#define __OPTIMIZER_H__

#include "llvm/ExecutionEngine/Orc/Core.h"
#include "llvm/ExecutionEngine/Orc/ThreadSafeModule.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Transforms/IPO.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Instrumentation/AddressSanitizer.h"
#include "llvm/Transforms/Scalar.h"

using namespace llvm;
using namespace llvm::orc;

namespace codegen
{
    // A function object that creates a simple pass pipeline to apply to each
    // module as it passes through the IRTransformLayer.
    class OptimizationTransform
    {
    public:
        OptimizationTransform() : PM(std::make_unique<legacy::PassManager>())
        {
            PM->add(createCalledValuePropagationPass());
            PM->add(createConstantMergePass());
            PM->add(createCalledValuePropagationPass());
            PM->add(createNewGVNPass());
            PM->add(createTailCallEliminationPass());
            PM->add(createSeparateConstOffsetFromGEPPass());
            PM->add(createSimpleLoopUnrollPass());
            PM->add(createFunctionInliningPass());
            PM->add(createIndVarSimplifyPass());
            PM->add(createSeparateConstOffsetFromGEPPass());
            PM->add(createCorrelatedValuePropagationPass());
            PM->add(createDeadArgEliminationPass());
            PM->add(createGlobalOptimizerPass());
            PM->add(createIPSCCPPass());
            PM->add(createPartiallyInlineLibCallsPass());
            PM->add(createMergeICmpsLegacyPass());
            PM->add(createPartialInliningPass());
            PM->add(createCFGSimplificationPass());
            PM->add(createSCCPPass());
            PM->add(createFunctionInliningPass());
            PM->add(createLoopUnrollPass());
            PM->add(createCalledValuePropagationPass());
            PM->add(createConstantHoistingPass());
            PM->add(createLoopUnswitchPass());
            PM->add(createInstSimplifyLegacyPass());
            PM->add(createLICMPass());
            PM->add(createFunctionInliningPass());
            PM->add(createMergeICmpsLegacyPass());
            PM->add(createAggressiveDCEPass());
            PM->add(createGlobalDCEPass());
            PM->add(createGlobalOptimizerPass());
            PM->add(createAggressiveDCEPass());
            PM->add(createDeadCodeEliminationPass());
            PM->add(createCalledValuePropagationPass());
            PM->add(createNewGVNPass());
        }

        std::unique_ptr<legacy::FunctionPassManager> init_func_pass(Module &module)
        {
            //TODO: simplify the required optimizer to improve hardening performance
            auto fpm = std::make_unique<legacy::FunctionPassManager>(&module);

            //            fpm->add(createEarlyCSEPass());

            fpm->add(createSCCPPass());
            fpm->add(createCorrelatedValuePropagationPass());
            fpm->add(createSROAPass());
            fpm->add(createInductiveRangeCheckEliminationPass());
            //fpm->add(createConstantMergePass()); //==> core dump
            //fpm->add(createIPSCCPPass()); //==> core dump
            fpm->add(createDeadCodeEliminationPass());
            fpm->add(createReassociatePass());
            fpm->add(createIndVarSimplifyPass());
            fpm->add(createLoopUnrollPass());
            //fpm->add(createLoopUnrollAndJamPass(2));
            fpm->add(createCFGSimplificationPass());
            //            fpm->add(createFlattenCFGPass());
            fpm->add(createLoopPredicationPass());
            fpm->add(createConstantHoistingPass());
            //fpm->add(createAddressSanitizerFunctionPass()); //==> core dump
            fpm->add(createLoopLoadEliminationPass());
            fpm->add(createInstructionCombiningPass());

            fpm->add(createCFGSimplificationPass());

            //            fpm->add(createLoopInstSimplifyPass());
            //            fpm->add(createLoopDeletionPass());
            //            fpm->add(createLoopVersioningLICMPass());
            //            fpm->add(createLoopExtractorPass());

            //fpm->add(createLoopUnrollAndJamPass(2));
            fpm->add(createLoopUnrollPass(2));
            fpm->add(createDeadCodeEliminationPass());

            //            fpm->add(createMergeICmpsLegacyPass());
            //            fpm->add(createFunctionInliningPass());
            //            fpm->add(createConstantMergePass());
            fpm->add(createAggressiveDCEPass());

            //            fpm->add(createCrossDSOCFIPass());
            //            fpm->add(createConstraintEliminationPass());
            //            fpm->add(createDeadArgEliminationPass());

            //            fpm->add(createEarlyCSEPass());
            //            fpm->add(createLowerExpectIntrinsicPass());

            //            fpm->add(llvm::createFunctionInliningPass());
            fpm->add(llvm::createLoopRotatePass());
            fpm->add(llvm::createLICMPass());
            //            fpm->add(llvm::createInstructionCombiningPass());
            fpm->add(llvm::createMemCpyOptPass());

            fpm->add(createAggressiveDCEPass());

            //fpm->add(llvm::createNewGVNPass());
            //            fpm->add(llvm::createCFGSimplificationPass());
            //fpm->add(llvm::createGlobalOptimizerPass());

            //            fpm->add(createConstantMergePass()); //==> core dump

            fpm->doInitialization();
            return fpm;
        }

        Expected<ThreadSafeModule> operator()(ThreadSafeModule TSM,
                                              MaterializationResponsibility &R)
        {
            TSM.withModuleDo([this](Module &M) {
                int original_count = M.getInstructionCount();
                // dbgs() << "--- BEFORE OPTIMIZATION ---\n"
                //        << M << "\n";

                auto fpm = init_func_pass(M);
                for (auto &func : M.getFunctionList())
                {
                    fpm->run(func);
                }
                PM->run(M);
                //                dbgs() << "--- AFTER OPTIMIZATION ---\n"
                //                       << M << "\n";
                int new_count = M.getInstructionCount();
                outs() << "\ninstruct count: original: " << original_count << " new:" << new_count;
            });
            return std::move(TSM);
        }

    private:
        std::unique_ptr<legacy::PassManager> PM;
    };
}
#endif