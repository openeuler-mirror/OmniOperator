/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#ifndef __OMNI_JIT_CONFIG_H__
#define __OMNI_JIT_CONFIG_H__

#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Pass.h>

namespace omniruntime {
    namespace jit {
        class Config {
        public:
            /**
         *
         * @param func_conf boolean array indicating function pass is enabled or disabled
         * @param module_conf boolean array indicating module pass is enabled or disabled
         */
            Config(bool &funcConf, bool &moduleConf)
            {
                this->funcConf = &funcConf;
                this->moduleConf = &moduleConf;
                InitFuncPass();
                InitModulePass();
            }

            Config(bool *funcConf, int funcConfLength, bool *moduleConf, int moduleConfLength)
                : funcConf(funcConf), moduleConf(moduleConf), func_pass(), module_pass()
            {
                InitFuncPass();
                InitModulePass();
            }

            Config()
            {
                bool all[32] = {true};
                this->funcConf = all;
                this->moduleConf = all;
                InitFuncPass();
                InitModulePass();
            }

            ~Config(){}

            void populate(llvm::legacy::FunctionPassManager &FPM, llvm::legacy::PassManager &MPM);

            static Config *GetConf()
            {
                return GetConf(2047, 2047); // 2047 2047
            };

            static Config *GetConf(int f, int m)
            {
                bool fConf[32], mConf[32]; // 32 32
                ToConf(f, fConf);
                ToConf(m, mConf);
                return new Config(fConf, 32, mConf, 32); // 32 32
            };

        private:
            bool *funcConf;
            bool *moduleConf;

            llvm::Pass *(*func_pass[100])(); // 100

            llvm::Pass *(*module_pass[100])(); // 100

            static void ToConf(int n, bool *conf);

            void InitFuncPass();

            void InitModulePass();
        };
    }
}

#endif