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
            Config(bool *func_conf, bool *module_conf) {
                this->func_conf = func_conf;
                this->module_conf = module_conf;
                init_func_pass();
                init_module_pass();
            }

            Config() {
                bool ALL[32] = {true};
                this->func_conf = ALL;
                this->module_conf = ALL;
                init_func_pass();
                init_module_pass();
            }

            void populate(llvm::legacy::FunctionPassManager &FPM, llvm::legacy::PassManager &MPM);

            static Config *getConf() {
                return getConf(2047, 2047);
            };

            static Config *getConf(int f, int m) {
                bool f_conf[32], m_conf[32];
                to_conf(f, f_conf);
                to_conf(m, m_conf);
                return new Config(f_conf, m_conf);
            };

        private:
            bool *func_conf;
            bool *module_conf;

            llvm::Pass *(*func_pass[100])();

            llvm::Pass *(*module_pass[100])();

            static void to_conf(int n, bool *conf);

            void init_func_pass();

            void init_module_pass();
        };
    }
}

#endif