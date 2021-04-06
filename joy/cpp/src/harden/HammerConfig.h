#include "llvm/Transforms/Scalar.h"
#include <llvm/Analysis/SparsePropagation.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Transforms/IPO.h>
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
#include "./Hammer.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/SourceMgr.h"

#ifndef JOY_HAMMERCONFIG_H
#define JOY_HAMMERCONFIG_H

using namespace llvm;

namespace codegen {
    class HammerConfig {
    public:
        /**
         *
         * @param func_conf boolean array indicating function pass is enabled or disabled
         * @param module_conf boolean array indicating module pass is enabled or disabled
         */
        HammerConfig(bool *func_conf, bool *module_conf) {
            this->func_conf = func_conf;
            this->module_conf = module_conf;
            init_func_pass();
            init_module_pass();
        }

        HammerConfig() {
            bool ALL[32] = {true};
            this->func_conf = ALL;
            this->module_conf = ALL;
            init_func_pass();
            init_module_pass();
        }

        void populate(legacy::FunctionPassManager &FPM, legacy::PassManager &MPM);

        static HammerConfig * getConf() {
            return getConf(2047, 2047);
        };

        static HammerConfig * getConf(int f, int m) {
            bool f_conf[32], m_conf[32];
            to_conf(f, f_conf);
            to_conf(m, m_conf);
            return new HammerConfig(f_conf, m_conf);
        };
    private:
        bool *func_conf;
        bool *module_conf;

        Pass *(*func_pass[100])();
        Pass *(*module_pass[100])();

        static void to_conf(int n, bool *conf);
        void init_func_pass();
        void init_module_pass();
    };

}

#endif //JOY_HAMMERCONFIG_H
