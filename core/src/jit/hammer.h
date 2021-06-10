#ifndef __HAMMER_H__
#define __HAMMER_H__
#include "harden_optimizer.h"
#include "hammer_config.h"
#include "iosfwd"
#include "map"
#include "param_value.h"
#include "llvm/ExecutionEngine/Orc/Core.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Transforms/IPO.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Instrumentation/AddressSanitizer.h"
#include "llvm/Transforms/Scalar.h"


namespace omniruntime {
namespace codegen {
    using namespace llvm;
    using namespace llvm::orc;
    
    class Hammer
    {
    public:
        bool harden();

        std::unique_ptr<LLJIT> create_jitter(std::list<Hammer *> deps, HammerConfig &config);

        std::unique_ptr<Module> load(StringRef file);
        std::unique_ptr<Module> get_module()
        {
            return std::move(module);
        }

        std::unique_ptr<LLVMContext> get_context()
        {
            return std::move(context);
        }

        Hammer(StringRef module, std::map<std::string, ParamValue *> _params);
    protected:
        ExitOnError ExitOnErr;

    private:
        std::unique_ptr<LLVMContext> context;
        std::unique_ptr<StringRef> layout;
        std::unique_ptr<IRBuilder<>> builder;
        std::unique_ptr<legacy::FunctionPassManager> fpm;
        std::unique_ptr<Module> module;
        std::map<std::string, ParamValue *> params;

        bool eligible(Function &function);

        bool eligible(CallInst &callInst);

        bool removeFnAttribute(Function &function);

        bool specialize_callsite(CallInst call, std::map<std::string, std::vector<ParamValue>>);

        bool harden_function(Function &function);

        bool harden_param(Function &function);

        bool harden_call(Function &function);

        bool verify_param(std::map<std::string, ParamValue *> params);

        Constant *to_llvm_value(std::string name, ParamValue value);
        Constant *to_scalar_llvm_value(ParamValue value);
        Constant *to_array_llvm_value(std::string name, ParamValue value);
        Constant *to_vector_llvm_value(std::string name, ParamValue value);
        Constant *to_2darray_llvm_value(std::string name, ParamValue value);
    };
} // end of namespace codegen
} // end of namespace omniruntime
#endif