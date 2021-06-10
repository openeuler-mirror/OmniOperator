#include "config.h"
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/Scalar.h>

namespace omniruntime {
    namespace jit {
        void Config::to_conf(int n, bool *conf) {
            int k;
            for (k = 0; k < sizeof(int) * 8; k++) {
                int mask = 1 << k;
                int masked_n = n & mask;
                int thebit = masked_n >> k;
                conf[k] = thebit;
            }
        }

        void Config::init_func_pass() {
            using namespace llvm;
            func_pass[0] = (Pass *(*)()) &llvm::createEarlyCSEPass;
            func_pass[1] = (Pass *(*)()) &llvm::createSROAPass;
            func_pass[2] = (Pass *(*)()) &llvm::createReassociatePass;
            func_pass[3] = (Pass *(*)()) &llvm::createAggressiveDCEPass;
            func_pass[4] = (Pass *(*)()) &llvm::createCFGSimplificationPass;
            func_pass[5] = (Pass *(*)()) &llvm::createMergedLoadStoreMotionPass;
            func_pass[6] = (Pass *(*)()) &llvm::createIPSCCPPass;
            func_pass[7] = (Pass *(*)()) &llvm::createSCCPPass;
            func_pass[8] = (Pass *(*)()) &llvm::createLowerAtomicPass;
            func_pass[9] = (Pass *(*)()) &llvm::createConstantHoistingPass;
            func_pass[10] = (Pass *(*)()) &llvm::createNewGVNPass;
            func_pass[11] = (Pass *(*)()) &llvm::createCalledValuePropagationPass;
            func_pass[12] = (Pass *(*)()) &llvm::createLoopLoadEliminationPass;
            func_pass[13] = (Pass *(*)()) &llvm::createLoopUnrollPass;
            func_pass[14] = (Pass *(*)()) &llvm::createPartiallyInlineLibCallsPass;
            func_pass[15] = (Pass *(*)()) &llvm::createSeparateConstOffsetFromGEPPass;
            func_pass[16] = (Pass *(*)()) &llvm::createLoopStrengthReducePass;
            func_pass[17] = (Pass *(*)()) &llvm::createInductiveRangeCheckEliminationPass;
            func_pass[18] = (Pass *(*)()) &llvm::createInductiveRangeCheckEliminationPass;
            func_pass[19] = (Pass *(*)()) &llvm::createLoopDistributePass;
            func_pass[20] = (Pass *(*)()) &llvm::createLoopSimplifyCFGPass;
            func_pass[21] = (Pass *(*)()) &llvm::createLoopInstSimplifyPass;
            func_pass[22] = (Pass *(*)()) &llvm::createLoopExtractorPass;
            func_pass[23] = (Pass *(*)()) &llvm::createLoopVersioningLICMPass;
            func_pass[24] = (Pass *(*)()) &llvm::createLoopDeletionPass;
            func_pass[25] = (Pass *(*)()) &llvm::createLoopDataPrefetchPass;
            func_pass[26] = (Pass *(*)()) &llvm::createLICMPass;
            func_pass[27] = (Pass *(*)()) &llvm::createFunctionInliningPass;
            func_pass[28] = (Pass *(*)()) &llvm::createLowerConstantIntrinsicsPass;
        }

        void Config::init_module_pass() {
            using namespace llvm;
            module_pass[0] = (Pass *(*)()) &llvm::createCalledValuePropagationPass;
            module_pass[1] = (Pass *(*)()) &llvm::createConstantMergePass;
            module_pass[2] = (Pass *(*)()) &llvm::createNewGVNPass;
            module_pass[3] = (Pass *(*)()) &llvm::createTailCallEliminationPass;
            module_pass[4] = (Pass *(*)()) &llvm::createSeparateConstOffsetFromGEPPass;
            module_pass[5] = (Pass *(*)()) &llvm::createSimpleLoopUnrollPass;
            module_pass[6] = (Pass *(*)()) &llvm::createFunctionInliningPass;
            module_pass[7] = (Pass *(*)()) &llvm::createIndVarSimplifyPass;
            module_pass[8] = (Pass *(*)()) &llvm::createDeadArgEliminationPass;
            module_pass[9] = (Pass *(*)()) &llvm::createGlobalOptimizerPass;
            module_pass[10] = (Pass *(*)()) &llvm::createIPSCCPPass;
            module_pass[11] = (Pass *(*)()) &llvm::createPartiallyInlineLibCallsPass;
            module_pass[12] = (Pass *(*)()) &llvm::createMergeICmpsLegacyPass;
            module_pass[13] = (Pass *(*)()) &llvm::createPartialInliningPass;
            module_pass[14] = (Pass *(*)()) &llvm::createCFGSimplificationPass;
            module_pass[15] = (Pass *(*)()) &llvm::createCalledValuePropagationPass;
            module_pass[16] = (Pass *(*)()) &llvm::createLowerConstantIntrinsicsPass;
            module_pass[17] = (Pass *(*)()) &llvm::createPruneEHPass;
            module_pass[18] = (Pass *(*)()) &llvm::createStructurizeCFGPass;
            module_pass[19] = (Pass *(*)()) &llvm::createMemCpyOptPass;
            module_pass[20] = (Pass *(*)()) &llvm::createAggressiveDCEPass;
        }

        void Config::populate(llvm::legacy::FunctionPassManager &FPM, llvm::legacy::PassManager &MPM) {
            using namespace llvm;

            //propage constants
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

            //    printf("=============== populating passes ================\n");
            //    for (int i = 0; i < 29; i++) {
            //        if (func_conf[i] && (long) func_pass[i] != -1) {
            //            //printf("adding func %d", i);
            //            FPM.add(func_pass[i]());
            //            break;
            //        }
            //    }
            //    for (int i = 0; i < 22; i++) {
            //        if (module_conf[i] && (long) module_pass[i] != -1) {
            //            //printf("adding mod %d", i);
            //            MPM.add(module_pass[i]());
            //            break;
            //        }
            //    }
        }
    }
}