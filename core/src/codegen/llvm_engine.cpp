/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generation utilities
 */

#include "expr_info_extractor.h"
#include "func_registry.h"
#include "llvm_engine.h"

using namespace llvm;
using namespace std;
using namespace orc;
using namespace omniruntime;
using namespace omniruntime::expressions;
using namespace omniruntime::type;

namespace {
    std::once_flag codegen_target_init_flag;
}

llvm::IRBuilder<> *LLVMEngine::GetIRBuilder()
{
    return builder.get();
}

Module *LLVMEngine::GetModule()
{
    return module.get();
}

LLVMContext *LLVMEngine::GetContext()
{
    return context.get();
}

LLJIT *LLVMEngine::GetJit()
{
    return jit.get();
}

LLVMTypes *LLVMEngine::GetTypes()
{
    return llvmTypes.get();
}

ExitOnError LLVMEngine::GetEoe()
{
    return eoe;
}

void LLVMEngine::MakeThreadSafe(ResourceTrackerSP* resTracker)
{
    auto threadSafeModule = llvm::orc::ThreadSafeModule(move(module), move(context));
    eoe(jit->addIRModule(*resTracker, std::move(threadSafeModule)));
}

void LLVMEngine::Create(std::unique_ptr<LLVMEngine>* out)
{
    std::call_once(codegen_target_init_flag, InitializeCodegenTargets);
    llvm::ExitOnError eoe;
    auto jit = eoe(LLJITBuilder().create());

    auto context = std::make_unique<LLVMContext>();
    auto llvmTypes = std::make_unique<LLVMTypes>(*context);
    // Create module called the_module
    auto module = std::make_unique<Module>("the_module", *context);
    module->setDataLayout(jit->getDataLayout());
    // Create IR builder to create IR instructions
    auto builder = std::make_unique<IRBuilder<>>(*context);
    auto fpm = std::make_unique<legacy::FunctionPassManager>(module.get());
    std::unique_ptr<LLVMEngine> engine {
            new LLVMEngine(std::move(context), std::move(jit), std::move(builder), std::move(module),
                           std::move(llvmTypes), std::move(fpm))};
    engine->RegisterFunctions(FunctionRegistry::GetFunctions());
    *out = std::move(engine);
}

void LLVMEngine::InitializeCodegenTargets()
{
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetDisassembler();
    llvm::sys::DynamicLibrary::LoadLibraryPermanently(nullptr);
}

void LLVMEngine::RegisterFunctions(const std::vector<omniruntime::Function> &functions)
{
    std::set<std::string> jitRegisteredFuncs;
    for (auto &func : functions) {
        auto &jd = jit->getMainJITDylib();
        auto &dl = jit->getDataLayout();
        llvm::orc::MangleAndInterner mangle(jit->getExecutionSession(), dl);
        std::vector<DataTypeId> params = func.GetParamTypes();
        DataTypeId retType = func.GetReturnType();
        std::vector<Type *> args = this->GetFunctionArgTypeVector(params, retType, func.IsExecutionContextSet());
        auto s = llvm::orc::absoluteSymbols({ { mangle(func.GetId()),
                                                      JITEvaluatedSymbol(pointerToJITTargetAddress(func.GetAddress()),
                                                                         JITSymbolFlags::Exported) } });
        auto ign = jd.define(s);
        if (ign) {
            LogError("Error while defining absolute symbol in jd");
        }
        llvm::Type *ret = (retType == OMNI_DECIMAL128) ? llvmTypes->VoidType() : llvmTypes->ToLLVMType(retType);
        llvm::FunctionType *ft = llvm::FunctionType::get(ret, args, false);
        auto linkage = llvm::Function::ExternalLinkage;
        llvm::Function *fn = llvm::Function::Create(ft, linkage, func.GetId(), *module);
        FunctionCallee callee = module->getOrInsertFunction(func.GetId(), ft);
    }
}

std::vector<Type *> LLVMEngine::GetFunctionArgTypeVector(std::vector<DataTypeId> &params, DataTypeId &retTypeId, bool needsContext)
{
    std::vector<Type *> args;
    if (needsContext) {
        args.push_back(llvmTypes->I64Type());
    }
    for (auto type : params) {
        if (type == OMNI_DECIMAL128) {
            // add high and low
            args.push_back(llvmTypes->I64Type());
            args.push_back(llvmTypes->I64Type());
        } else {
            args.push_back(llvmTypes->ToLLVMType(type));
            if (TypeUtil::IsStringType(type)) {
                if (type == OMNI_CHAR) {
                    // Add Type for width support
                    args.push_back(llvmTypes->I32Type());
                }
                // Add Type for Length of the string
                args.push_back(llvmTypes->I32Type());
            }
        }
    }
    // return arguments
    if (TypeUtil::IsStringType(retTypeId)) {
        args.push_back(llvmTypes->I32PtrType());
    } else if (retTypeId == OMNI_DECIMAL128) {
        // Add high and low output pointers
        args.push_back(llvmTypes->I64PtrType());
        args.push_back(llvmTypes->I64PtrType());
    }
    return args;
}

void LLVMEngine::OptimizeFunctionsAndModule()
{
    fpm->add(createSCCPPass());
    fpm->add(createNewGVNPass());
    fpm->add(createInductiveRangeCheckEliminationPass());
    fpm->add(createIndVarSimplifyPass());

    fpm->add(createLICMPass());
    fpm->add(createLoopUnrollPass());
    fpm->add(createLoopUnswitchPass());

    fpm->add(createLoopLoadEliminationPass());
    fpm->add(createInductiveRangeCheckEliminationPass());
    fpm->add(createIndVarSimplifyPass());
    fpm->add(createLoopInstSimplifyPass());
    fpm->add(createLoopSimplifyCFGPass());
    fpm->add(createMergedLoadStoreMotionPass());
    fpm->add(createMergeICmpsLegacyPass());
    fpm->add(createAggressiveDCEPass());
    fpm->add(createDeadStoreEliminationPass());
    fpm->add(createPromoteMemoryToRegisterPass());

    mpm.add(createFunctionInliningPass());
    mpm.add(createPruneEHPass());

    fpm->doInitialization();
    for (auto &F : *module)
        fpm->run(F);
    mpm.run(*module);
}

void LLVMEngine::OptimizeModule()
{
    mpm.add(createFunctionInliningPass());
    mpm.add(createPruneEHPass());

    mpm.run(*module);
}

CallInst *LLVMEngine::CreateCall(llvm::Function *func, std::vector<llvm::Value *> argsVals, string name)
{
    return builder->CreateCall(func, argsVals, name);
}

Value *LLVMEngine::CallExternFunction(const std::string fn_name, std::vector<omniruntime::type::DataTypeId> params,
                                      const omniruntime::type::DataTypeId &returnType,
                                      std::vector<llvm::Value *> args, std::string msg)
{
    std::string funcId = FunctionSignature(fn_name, params, returnType).ToString();
    auto f = module->getFunction(funcId);
    auto ret = CreateCall(f, args, msg);
    return ret;
}

void LLVMEngine::RecordMainFunction(llvm::Function *func)
{
    this->function = func;
}

void LLVMEngine::RemoveUnusedFunctions()
{
    llvm::Function *preserved = function;
    mpm.add(llvm::createInternalizePass([preserved](const llvm::GlobalValue &func) {
        return (func.getName().str() == preserved->getName().str()); }));
    mpm.add(llvm::createGlobalDCEPass());
}