/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "config.h"
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/Scalar.h>

namespace omniruntime {
namespace jit {

void Config::InitFuncPass()
{
    using namespace llvm;
    func_pass[static_cast<int>(Optimization::EARLY_CSE)] = (Pass* (*)()) &llvm::createEarlyCSEPass;
    func_pass[static_cast<int>(Optimization::SROA)] = (Pass* (*)()) &llvm::createSROAPass;
    func_pass[static_cast<int>(Optimization::REASSOCIATE)] = (Pass* (*)()) &llvm::createReassociatePass;
    func_pass[static_cast<int>(Optimization::AGGRESIVE_DCE)] = (Pass* (*)()) &llvm::createAggressiveDCEPass;
    func_pass[static_cast<int>(Optimization::CFG_SIMPLIFICATION)] = (Pass* (*)()) &llvm::createCFGSimplificationPass;
    func_pass[static_cast<int>(Optimization::MERGED_LOAD_STORE_MOTION)] =
        (Pass* (*)()) &llvm::createMergedLoadStoreMotionPass;
    func_pass[static_cast<int>(Optimization::IPSCCP)] = (Pass* (*)()) &llvm::createIPSCCPPass;
    func_pass[static_cast<int>(Optimization::SCCP)] = (Pass* (*)()) &llvm::createSCCPPass;
    func_pass[static_cast<int>(Optimization::LOWER_ATOMIC)] = (Pass* (*)()) &llvm::createLowerAtomicPass;
    func_pass[static_cast<int>(Optimization::CONSTANT_HOISTING)] = (Pass* (*)()) &llvm::createConstantHoistingPass;
    func_pass[static_cast<int>(Optimization::NEW_GVN)] = (Pass* (*)()) &llvm::createNewGVNPass;
    func_pass[static_cast<int>(Optimization::CALLED_VALUE_PROPAGATION)] =
        (Pass* (*)()) &llvm::createCalledValuePropagationPass;
    func_pass[static_cast<int>(Optimization::LOOP_LOAD_ELIMINATION)] =
        (Pass* (*)()) &llvm::createLoopLoadEliminationPass;
    func_pass[static_cast<int>(Optimization::LOOP_UNROLL)] = (Pass* (*)()) &llvm::createLoopUnrollPass;
    func_pass[static_cast<int>(Optimization::PARTIALLY_INLINE_LIB_CALLS)] =
        (Pass* (*)()) &llvm::createPartiallyInlineLibCallsPass;
    func_pass[static_cast<int>(Optimization::SEPARATE_CONST_OFFSET_FROM_GEP)] =
        (Pass* (*)()) &llvm::createSeparateConstOffsetFromGEPPass;
    func_pass[static_cast<int>(Optimization::LOOP_STRENGTH_REDUCE)] =
        (Pass* (*)()) &llvm::createLoopStrengthReducePass;
    func_pass[static_cast<int>(Optimization::INDUCTIVE_RANGE_CHECK_ELIMINATION)] =
        (Pass* (*)()) &llvm::createInductiveRangeCheckEliminationPass;
    func_pass[static_cast<int>(Optimization::LOOP_DISTRIBUTE)] = (Pass* (*)()) &llvm::createLoopDistributePass;
    func_pass[static_cast<int>(Optimization::LOOP_SIMPLIFY_CFG)] = (Pass* (*)()) &llvm::createLoopSimplifyCFGPass;
    func_pass[static_cast<int>(Optimization::LOOP_INST_SIMPLIFY)] = (Pass* (*)()) &llvm::createLoopInstSimplifyPass;
    func_pass[static_cast<int>(Optimization::LOOP_EXTRACTOR)] = (Pass* (*)()) &llvm::createLoopExtractorPass;
    func_pass[static_cast<int>(Optimization::LOOP_VERSIONING_LICM)] =
        (Pass* (*)()) &llvm::createLoopVersioningLICMPass;
    func_pass[static_cast<int>(Optimization::LOOP_DELETION)] = (Pass* (*)()) &llvm::createLoopDeletionPass;
    func_pass[static_cast<int>(Optimization::LOOP_DATA_PREFETCH)] = (Pass* (*)()) &llvm::createLoopDataPrefetchPass;
    func_pass[static_cast<int>(Optimization::LICM)] = (Pass* (*)()) &llvm::createLICMPass;
    func_pass[static_cast<int>(Optimization::FUNCTION_INLINING)] = (Pass* (*)()) &llvm::createFunctionInliningPass;
    func_pass[static_cast<int>(Optimization::LOWER_CONSTANT_INTRINSICS)] =
        (Pass* (*)()) &llvm::createLowerConstantIntrinsicsPass;
    func_pass[static_cast<int>(Optimization::IND_VAR_SIMPLIFY)] = (Pass* (*)()) &llvm::createIndVarSimplifyPass;
    func_pass[static_cast<int>(Optimization::LOOP_UNSWITCH)] = (Pass* (*)()) &llvm::createLoopUnswitchPass;
    func_pass[static_cast<int>(Optimization::MERGE_ICMPS_LEGACY)] = (Pass* (*)()) &llvm::createMergeICmpsLegacyPass;
    func_pass[static_cast<int>(Optimization::DEAD_STORE_ELIMINATION)] =
        (Pass* (*)()) &llvm::createDeadStoreEliminationPass;
    func_pass[static_cast<int>(Optimization::STRUCTURIZE_CFG)] = (Pass* (*)()) &llvm::createStructurizeCFGPass;
    // find a way to add createInstructionCombiningPass
}

void Config::InitModulePass()
{
    using namespace llvm;
    module_pass[static_cast<int>(ModuleOptimization::CALLED_VALUE_PROPAGATION)] =
        (Pass* (*)()) &llvm::createCalledValuePropagationPass;
    module_pass[static_cast<int>(ModuleOptimization::CONSTANT_MERGE)] = (Pass* (*)()) &llvm::createConstantMergePass;
    module_pass[static_cast<int>(ModuleOptimization::NEW_GVN)] = (Pass* (*)()) &llvm::createNewGVNPass;
    module_pass[static_cast<int>(ModuleOptimization::TAIL_CALL_ELIMINATION)] =
        (Pass* (*)()) &llvm::createTailCallEliminationPass;
    module_pass[static_cast<int>(ModuleOptimization::SEPARATE_CONST_OFFSET_FROM_GEP)] =
        (Pass* (*)()) &llvm::createSeparateConstOffsetFromGEPPass;
    module_pass[static_cast<int>(ModuleOptimization::SIMPLE_LOOP_UNROLL)] =
        (Pass* (*)()) &llvm::createSimpleLoopUnrollPass;
    module_pass[static_cast<int>(ModuleOptimization::FUNCTION_INLINING)] =
        (Pass* (*)()) &llvm::createFunctionInliningPass;
    module_pass[static_cast<int>(ModuleOptimization::IND_VAR_SIMPLIFY)] =
        (Pass* (*)()) &llvm::createIndVarSimplifyPass;
    module_pass[static_cast<int>(ModuleOptimization::DEAD_ARG_ELIMINATION)] =
        (Pass* (*)()) &llvm::createDeadArgEliminationPass;
    module_pass[static_cast<int>(ModuleOptimization::GLOBAL_OPTIMIZER)] =
        (Pass* (*)()) &llvm::createGlobalOptimizerPass;
    module_pass[static_cast<int>(ModuleOptimization::IPSCCP)] = (Pass* (*)()) &llvm::createIPSCCPPass;
    module_pass[static_cast<int>(ModuleOptimization::PARTIALLY_INLINE_LIB_CALLS)] =
        (Pass* (*)()) &llvm::createPartiallyInlineLibCallsPass;
    module_pass[static_cast<int>(ModuleOptimization::MERGE_ICMPS_LEGACY)] =
        (Pass* (*)()) &llvm::createMergeICmpsLegacyPass;
    module_pass[static_cast<int>(ModuleOptimization::PARTIAL_INLINING)] =
        (Pass* (*)()) &llvm::createPartialInliningPass;
    module_pass[static_cast<int>(ModuleOptimization::CFG_SIMPLIFICATION)] =
        (Pass* (*)()) &llvm::createCFGSimplificationPass;
    module_pass[static_cast<int>(ModuleOptimization::LOWER_CONSTANT_INTRINSICS)] =
        (Pass* (*)()) &llvm::createLowerConstantIntrinsicsPass;
    module_pass[static_cast<int>(ModuleOptimization::PRUNE_EH)] = (Pass* (*)()) &llvm::createPruneEHPass;
    module_pass[static_cast<int>(ModuleOptimization::STRUCTURIZE_CFG)] =
        (Pass* (*)()) &llvm::createStructurizeCFGPass;
    module_pass[static_cast<int>(ModuleOptimization::MEM_CPY_OPT)] = (Pass* (*)()) &llvm::createMemCpyOptPass;
    module_pass[static_cast<int>(ModuleOptimization::AGGRESIVE_DCE)] = (Pass* (*)()) &llvm::createAggressiveDCEPass;
}

void Config::populate(llvm::legacy::FunctionPassManager &FPM, llvm::legacy::PassManager &MPM,
    const std::vector<Optimization> &optimizations, const std::vector<ModuleOptimization> &moduleOptimizations)
{
    using namespace llvm;

    for (auto &optimization : optimizations) {
        int optimizationIndex = static_cast<int>(optimization);
        if (optimization == Optimization::LOOP_UNROLL) {
            // find a better way to handle params
            FPM.add(createLoopUnrollPass(DEFAULT_OPT_LEVEL, false, false, DEFAULT_LOOP_UNROLL_THRESHOLD,
                DEFAULT_LOOP_UNROLL_COUNT, true));
        } else {
            FPM.add(func_pass[optimizationIndex]());
        }
        outs() << "Function pass added: " << optimizationIndex << "\n";
    }

    for (auto &optimization : moduleOptimizations) {
        int optimizationIndex = static_cast<int>(optimization);
        MPM.add(module_pass[optimizationIndex]());
        outs() << "Module pass added: " << optimizationIndex << "\n";
    }
}
}
}