/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generation utilities
 */
#ifndef OMNI_RUNTIME_LLVM_ENGINE_H
#define OMNI_RUNTIME_LLVM_ENGINE_H

#include <memory>
#include <set>
#include <string>
#include <vector>
#include <utility>

#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/IPO.h"
#include "llvm/Transforms/Utils.h"
#include "llvm/Transforms/IPO/PassManagerBuilder.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "llvm/Transforms/Utils/Cloning.h"
#include "llvm/Transforms/Vectorize.h"
#include "llvm/ADT/APInt.h"
#include "llvm/ADT/APFloat.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/Host.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/ExecutionEngine/ExecutionEngine.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/Analysis/TargetTransformInfo.h"

#include "expression/expressions.h"
#include "llvm_types.h"
#include "codegen_value.h"

namespace omniruntime {
namespace codegen {
using namespace llvm;
using namespace orc;
using namespace omniruntime;
using namespace omniruntime::expressions;
using namespace omniruntime::type;

class LLVMEngine {
public:
    LLVMEngine();

    virtual ~LLVMEngine() = default;

    void OptimizeFunctionsAndModule();

    void OptimizeModule();

    llvm::CallInst *CreateCall(llvm::Function *func, const std::vector<llvm::Value *> &argsVals,
        const std::string &name);

    llvm::Value *CallExternFunction(const std::string &fn_name,
        const std::vector<omniruntime::type::DataTypeId> &params, const omniruntime::type::DataTypeId returnType,
        const std::vector<llvm::Value *> &args, llvm::Value *executionContextPtr, const std::string &msg = "",
        omniruntime::op::OverflowConfig *overflowConfig = nullptr, llvm::Value *overflowNull = nullptr);

    static void InitializeCodegenTargets();

    void RegisterFunctions(const std::vector<Function> &func);

    void MakeThreadSafe(llvm::orc::ResourceTrackerSP *res);

    void RecordMainFunction(llvm::Function *func);

    void RemoveUnusedFunctions();

    std::shared_ptr<DecimalValue> BuildDecimalValue(llvm::Value *data, omniruntime::type::DataType &retType,
        llvm::Value *isNull = nullptr);

    // Make from i128 value
    DecimalSplitValue Split(llvm::Value *fullValue);

    // Combine the two parts into an i128
    llvm::Value *ToInt128(llvm::Value *high, llvm::Value *low) const;

    llvm::Value *CallDecimalFunction(const std::string &function_name, llvm::Type *return_type,
        const std::vector<llvm::Value *> &args, llvm::Value *executionContextPtr = nullptr,
        omniruntime::op::OverflowConfig *overflowConfig = nullptr, llvm::Value *overflowNull = nullptr);

    /* *
     * optimize and compiles the generated module contained in this LLVM Engine instance
     * @return function address
     */
    int64_t Compile();

    llvm::IRBuilder<> *GetIRBuilder();

    llvm::Module *GetModule();

    llvm::LLVMContext *GetContext();

    LLVMTypes *GetTypes();

protected:
    std::unique_ptr<llvm::LLVMContext> context;
    std::unique_ptr<llvm::orc::LLJIT> jit;
    std::unique_ptr<llvm::IRBuilder<>> builder;
    llvm::Module *modulePtr;
    std::unique_ptr<LLVMTypes> llvmTypes;
    std::unique_ptr<llvm::legacy::FunctionPassManager> fpm = nullptr;
    std::unique_ptr<llvm::ExecutionEngine> execution_engine;
    llvm::Function *function = nullptr;
    llvm::ExitOnError eoe;
    llvm::orc::ResourceTrackerSP rt;
    llvm::legacy::PassManager mpm;

private:
    std::vector<llvm::Type *> GetFunctionArgTypeVector(std::vector<omniruntime::type::DataTypeId> &params,
        omniruntime::type::DataTypeId &retTypeId, bool needsContext);
};
}
}

#endif // OMNI_RUNTIME_LLVM_ENGINE_H