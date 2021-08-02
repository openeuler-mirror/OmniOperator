/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "config.h"
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/Scalar.h>

namespace omniruntime {
    namespace jit {
        void Config::ToConf(int n, bool *conf)
        {
            int k;
            for (k = 0; k < sizeof(int) * 8; k++) { // 8
                int mask = 1 << k;
                int maskedN = n & mask;
                int thebit = maskedN >> k;
                conf[k] = thebit;
            }
        }

        void Config::InitFuncPass()
        {
            using namespace llvm;
            func_pass[0] = (Pass *(*)()) &llvm::createEarlyCSEPass; // 0
            func_pass[1] = (Pass *(*)()) &llvm::createSROAPass; // 1
            func_pass[2] = (Pass *(*)()) &llvm::createReassociatePass; // 2
            func_pass[3] = (Pass *(*)()) &llvm::createAggressiveDCEPass; // 3
            func_pass[4] = (Pass *(*)()) &llvm::createCFGSimplificationPass; // 4
            func_pass[5] = (Pass *(*)()) &llvm::createMergedLoadStoreMotionPass; // 5
            func_pass[6] = (Pass *(*)()) &llvm::createIPSCCPPass; // 6
            func_pass[7] = (Pass *(*)()) &llvm::createSCCPPass; // 7
            func_pass[8] = (Pass *(*)()) &llvm::createLowerAtomicPass; // 8
            func_pass[9] = (Pass *(*)()) &llvm::createConstantHoistingPass; // 9
            func_pass[10] = (Pass *(*)()) &llvm::createNewGVNPass; // 10
            func_pass[11] = (Pass *(*)()) &llvm::createCalledValuePropagationPass; // 11
            func_pass[12] = (Pass *(*)()) &llvm::createLoopLoadEliminationPass; // 12
            func_pass[13] = (Pass *(*)()) &llvm::createLoopUnrollPass; // 13
            func_pass[14] = (Pass *(*)()) &llvm::createPartiallyInlineLibCallsPass; // 14
            func_pass[15] = (Pass *(*)()) &llvm::createSeparateConstOffsetFromGEPPass; // 15
            func_pass[16] = (Pass *(*)()) &llvm::createLoopStrengthReducePass; // 16
            func_pass[17] = (Pass *(*)()) &llvm::createInductiveRangeCheckEliminationPass; // 17
            func_pass[18] = (Pass *(*)()) &llvm::createInductiveRangeCheckEliminationPass; // 18
            func_pass[19] = (Pass *(*)()) &llvm::createLoopDistributePass; // 19
            func_pass[20] = (Pass *(*)()) &llvm::createLoopSimplifyCFGPass; // 20
            func_pass[21] = (Pass *(*)()) &llvm::createLoopInstSimplifyPass; // 21
            func_pass[22] = (Pass *(*)()) &llvm::createLoopExtractorPass; // 22
            func_pass[23] = (Pass *(*)()) &llvm::createLoopVersioningLICMPass; // 23
            func_pass[24] = (Pass *(*)()) &llvm::createLoopDeletionPass; // 24
            func_pass[25] = (Pass *(*)()) &llvm::createLoopDataPrefetchPass; // 25
            func_pass[26] = (Pass *(*)()) &llvm::createLICMPass; // 26
            func_pass[27] = (Pass *(*)()) &llvm::createFunctionInliningPass; // 27
            func_pass[28] = (Pass *(*)()) &llvm::createLowerConstantIntrinsicsPass; // 28
        }

        void Config::InitModulePass()
        {
            using namespace llvm;
            module_pass[0] = (Pass *(*)()) &llvm::createCalledValuePropagationPass; // 0
            module_pass[1] = (Pass *(*)()) &llvm::createConstantMergePass; // 1
            module_pass[2] = (Pass *(*)()) &llvm::createNewGVNPass; // 2
            module_pass[3] = (Pass *(*)()) &llvm::createTailCallEliminationPass; // 3
            module_pass[4] = (Pass *(*)()) &llvm::createSeparateConstOffsetFromGEPPass; // 4
            module_pass[5] = (Pass *(*)()) &llvm::createSimpleLoopUnrollPass; // 5
            module_pass[6] = (Pass *(*)()) &llvm::createFunctionInliningPass; // 6
            module_pass[7] = (Pass *(*)()) &llvm::createIndVarSimplifyPass; // 7
            module_pass[8] = (Pass *(*)()) &llvm::createDeadArgEliminationPass; // 8
            module_pass[9] = (Pass *(*)()) &llvm::createGlobalOptimizerPass; // 9
            module_pass[10] = (Pass *(*)()) &llvm::createIPSCCPPass; // 10
            module_pass[11] = (Pass *(*)()) &llvm::createPartiallyInlineLibCallsPass; // 11
            module_pass[12] = (Pass *(*)()) &llvm::createMergeICmpsLegacyPass; // 12
            module_pass[13] = (Pass *(*)()) &llvm::createPartialInliningPass; // 13
            module_pass[14] = (Pass *(*)()) &llvm::createCFGSimplificationPass; // 14
            module_pass[15] = (Pass *(*)()) &llvm::createCalledValuePropagationPass; // 15
            module_pass[16] = (Pass *(*)()) &llvm::createLowerConstantIntrinsicsPass; // 16
            module_pass[17] = (Pass *(*)()) &llvm::createPruneEHPass; // 17
            module_pass[18] = (Pass *(*)()) &llvm::createStructurizeCFGPass; // 18
            module_pass[19] = (Pass *(*)()) &llvm::createMemCpyOptPass; // 19
            module_pass[20] = (Pass *(*)()) &llvm::createAggressiveDCEPass; // 20
        }

        void Config::populate(llvm::legacy::FunctionPassManager &FPM, llvm::legacy::PassManager &MPM)
        {
            using namespace llvm;

            // propage constants
            FPM.add(createSCCPPass());
            FPM.add(createNewGVNPass());
            FPM.add(createInductiveRangeCheckEliminationPass());
            FPM.add(createIndVarSimplifyPass());

            FPM.add(createLICMPass());
            FPM.add(createLoopUnrollPass());
            FPM.add(createLoopUnswitchPass());

            FPM.add(createLoopLoadEliminationPass());
            FPM.add(createInductiveRangeCheckEliminationPass());
            FPM.add(createIndVarSimplifyPass());
            FPM.add(createLoopInstSimplifyPass());
            FPM.add(createLoopSimplifyCFGPass());
            FPM.add(createMergedLoadStoreMotionPass());
            FPM.add(createMergeICmpsLegacyPass());
            FPM.add(createAggressiveDCEPass());
            FPM.add(createDeadStoreEliminationPass());

            MPM.add(createFunctionInliningPass());
            MPM.add(createPruneEHPass());
        }
    }
}