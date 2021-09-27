/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "harden_optimizer.h"
#include "../annotation.h"
#include "../../util/debug.h"
#include "llvm_compiler.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/ExecutionEngine/Orc/Core.h"
#include "llvm/ExecutionEngine/Orc/JITTargetMachineBuilder.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/Orc/ObjectTransformLayer.h"
#include "llvm/ExecutionEngine/Orc/ThreadSafeModule.h"
#include "llvm/IR/Attributes.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/Verifier.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/TargetSelect.h"

#include <set>

using llvm::Module;
using llvm::outs;
using std::map;
using std::set;
using std::string;
using std::to_string;
using std::unique_ptr;

namespace omniruntime {
namespace jit {
LLVMCompiler::LLVMCompiler()
{
    this->config = std::make_unique<Config>();
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetAsmPrinter();
    this->context = std::make_unique<llvm::LLVMContext>();
    this->layout = std::make_unique<llvm::StringRef>();
    this->builder = std::make_unique<llvm::IRBuilder<>>(*context);

    // TODO: load needed libraries for each module
    LoadExtraLibraries();
}

LLVMCompiler::~LLVMCompiler() {}

bool LLVMCompiler::LoadModule(std::string templatePath)
{
    // TODO: have a proper registry for all the operators instead of loading from /opt/lib/ir
    llvm::SMDiagnostic error;
    auto module = llvm::parseIRFile(templatePath, error, *context);

    if (!module) {
        error.print("error loadding module", llvm::errs());
        // FIXME: proper error handling using exceptions?
        return false;
    }

    for (auto const &function : module->getFunctionList()) {
        this->functionSymbols.push_back(function.getName().str());
    }

    this->modules.push_back(std::move(module));
    return true;
}

LibraryLoader LLVMCompiler::ll;

void LLVMCompiler::LoadExtraLibraries()
{
    using namespace llvm::sys;
    StringOrNull ev = std::getenv("LD_LIBRARY_PATH");
    auto vec = ll.LoadLibraries(ev.msg());
    string err;
    for (auto &s : vec) {
        if (DynamicLibrary::LoadLibraryPermanently(s.c_str(), &err)) {
            llvm::errs() << "Failed to load core library at path " << s << "\n";
            llvm::errs() << err << "\n";
        } else {
            LLVM_DEBUG_LOG("Successfully loaded core library at path %s", s.c_str());
        }
    }
}

bool LLVMCompiler::SpecializeAndCompile(const std::vector<Optimization> &optimizations,
    const std::vector<ModuleOptimization> &moduleOptimizations)
{
    map<string, set<string>> specializedModules;
    for (auto const & module : this->modules) {
        auto specializedFuncs = specializeModule(module);
        if (!specializedFuncs.empty()) {
            specializedModules.insert(make_pair(module->getName().str(), specializedFuncs));
        }
    }
    auto jit = compileModules(specializedModules, optimizations, moduleOptimizations);
    specializedModules.clear();

    if (jit) {
        jitter = std::move(jit);
        return true;
    } else {
        llvm::errs() << "Error: Unable to compile the modules\n";
    }

    return false;
}

set<string> LLVMCompiler::specializeModule(const std::unique_ptr<llvm::Module> &module)
{
    using namespace llvm;

    set<string> specializedFuncs;

    map<string, Function *> annotatedFuncs = getAnnotatedFuncs(module);
    if (annotatedFuncs.empty()) {
        return specializedFuncs;
    }

    for (auto &funcPair : annotatedFuncs) {
        string id = funcPair.first;
        Function *func = funcPair.second;
        optimizeAttributes(func);
        if (harden_function(id, func, module)) {
            specializedFuncs.insert(func->getName().str());
        }
    }

    verifyModule(*module);

    return specializedFuncs;
}

void LLVMCompiler::AddSpecialization(std::string id, Specialization specialization)
{
    this->specializations.insert(std::make_pair(id, specialization));
}

uint64_t LLVMCompiler::GetJitedFunction(std::string functionName, bool isNameMangled)
{
    using namespace llvm;

    if (isNameMangled) {
        auto expected = this->jitter->lookup(functionName);
        if (expected) {
            return expected->getAddress();
        } else {
            errs() << "Cannot find mangled function name: " << functionName << "\n";
            return 0;
        }
    } else {
        for (auto const &function : this->functionSymbols) {
            if (function.find(functionName) == string::npos) {
                continue;
            }

            auto expectedFunc = this->jitter->lookup(function);
            if (expectedFunc) {
                return expectedFunc->getAddress();
            }
        }

        llvm::errs() << "Cannot find function name: " << functionName << "\n";
        return 0;
    }
}

// replaces the value of parameters passed to a function
// this is done without modifying the signature
// replacing the value directly inside of the function also make it
// easier for optimizers to perform constant folding and propagation
bool LLVMCompiler::harden_function(const string &specializationId, llvm::Function *function,
    const unique_ptr<Module> &module)
{
    if (this->specializations.count(specializationId) == 0) {
        return false;
    }
#ifdef DEBUG_LLVM
    llvm::outs() << "hardening: " << function->getName().str() << "\n";
#endif
    Specialization specialization = this->specializations.at(specializationId);

    int count = 0;
    for (auto &arg : function->args()) {
        // 1. find the values from the Parameters that can be used for harden
        // use function_name and arg name as the key
        if (specialization.HasSpecializedParam(count)) {
            ParamValue *newValue = specialization.GetSpecializedParam(count);
            auto newArg = this->to_llvm_value(build_param_key(*function, count), *newValue, module);
            arg.replaceAllUsesWith(newArg);
        }
        count++;
    }

    return true;
}

// values for the parameters that is not harden
// conflicting params, e.g. param value provided during hardening cannot be provided here again
// this should be used for testing purpose only, we should expose a new function with new function type
std::unique_ptr<llvm::orc::LLJIT> LLVMCompiler::compileModules(map<string, set<string>> &specializedModules,
    const std::vector<Optimization> &optimizations, const std::vector<ModuleOptimization> &moduleOptimizations)
{
    using namespace llvm;
    using namespace llvm::orc;
    // ELF format on linux to be supported later with llvm-12.0.1 fix
    ExitOnError ExitOnErr;

    auto JTMB = ExitOnErr(JITTargetMachineBuilder::detectHost());
    JTMB.setCodeGenOptLevel(CodeGenOpt::Default);

    auto JITTER = ExitOnErr(LLJITBuilder()
                                .setJITTargetMachineBuilder(std::move(JTMB))
                                //                    .setObjectLinkingLayerCreator(
                                //                            [&](ExecutionSession &ES, const Triple &TT) {
                                //                                return std::make_unique<ObjectLinkingLayer>(
                                //                            })
                                .create());

    JITTER->getIRTransformLayer().setTransform(
        HardenOptimizer(CodeGenOpt::Default, optimizations, moduleOptimizations, specializedModules));

    // enable loading common libraries available in the current process
    JITTER->getMainJITDylib().addGenerator(
        ExitOnErr(DynamicLibrarySearchGenerator::GetForCurrentProcess(JITTER->getDataLayout().getGlobalPrefix())));
    for (auto &module : this->modules) {
        std::string moduleName = module->getName().str();
#ifdef DEBUG_LLVM
        outs() << "addIRModule: " << moduleName << "\n";
#endif
        auto err =
            JITTER->addIRModule(ThreadSafeModule(std::move(module), std::move(std::make_unique<llvm::LLVMContext>())));
        if (err) {
            errs() << "Error: failed adding IR Module " << moduleName << "\n";
            return nullptr;
        }
    }

    return JITTER;
}

llvm::Constant *LLVMCompiler::to_llvm_value(const std::string &name, ParamValue value,
    const std::unique_ptr<Module> &module)
{
    if (value.type == ParamType::ARRAY2D) {
        return to_2darray_llvm_value(name, value, module);
    } else if (value.IsScalar()) {
        return to_scalar_llvm_value(value);
    } else { // array type
        if (value.vector) {
            return to_vector_llvm_value(name, value, module);
        } else {
            return to_array_llvm_value(name, value, module);
        }
    }
}

llvm::Constant *LLVMCompiler::to_scalar_llvm_value(ParamValue value)
{
    using namespace llvm;
    Constant *llvmValue = nullptr;
    switch (value.type) {
        case ParamType::INT32:
            llvmValue = ConstantInt::get(IntegerType::get(*context, 32), value.ToInt32(), true); // 32
            break;
        case ParamType::INT64:
            llvmValue = ConstantInt::get(IntegerType::get(*context, 64), value.ToInt64(), true); // 64
            break;
        case ParamType::FP64:
            llvmValue = ConstantFP::get(*context, APFloat(value.ToFp64()));
            break;
        default:
            break;
    }
    return llvmValue;
}

llvm::Constant *LLVMCompiler::to_2darray_llvm_value(const std::string &name, ParamValue value,
    const std::unique_ptr<Module> &module)
{
    using namespace llvm;
    auto params = value.ToParamList();
    std::vector<Constant *> vec2dValues;
    auto i64 = IntegerType::get(*context, 64); // 64
    auto arrayType = ArrayType::get(i64, params->size());

    int count = 0;
    for (ParamValue param : *params) {
        Constant *element = to_array_llvm_value(name + "_" + to_string(count), param, module);
        element->print(errs());
        vec2dValues.push_back(element);
        count++;
    }

    module->getOrInsertGlobal(name, arrayType);
    auto array = module->getNamedGlobal(name);
    array->setInitializer(ConstantArray::get(arrayType, vec2dValues));
    array->setConstant(true);
    array->setLinkage(GlobalValue::LinkageTypes::PrivateLinkage);

    auto i32 = IntegerType::get(*context, 32); // 32
    auto Zero = ConstantInt::get(i32, 0);
    Constant *GEPIndices[] = {Zero, Zero};
    return ConstantExpr::getGetElementPtr(arrayType, array, GEPIndices);
}

llvm::Constant *LLVMCompiler::to_int32_vector_llvm_value(ParamValue value, std::vector<llvm::Constant *> &vecValues)
{
    using namespace llvm;
    auto values = *value.ToInt32Vec();
    auto i32 = IntegerType::get(*context, 32); // 32
    for (int i = 0; i < value.size; ++i) {
        Constant *c = ConstantInt::get(i32, values[i]);
        vecValues.push_back(c);
    }
    auto vec = ConstantVector::get(vecValues);
    return vec;
}

llvm::Constant *LLVMCompiler::to_vector_llvm_value(const std::string &name, ParamValue value,
    const std::unique_ptr<Module> &module)
{
    using namespace llvm;
    std::vector<Constant *> vecValues;
    switch (value.type) {
        case ParamType::INT32: {
            return to_int32_vector_llvm_value(value, vecValues);
        }
        case ParamType::INT64: {
            auto values = value.ToInt32Array();
            auto i64 = IntegerType::get(*context, 64); // 64
            auto arrayType = ArrayType::get(i64, value.size);
            for (int i = 0; i < value.size; ++i) {
                Constant *c = ConstantInt::get(i64, values[i]);
            }

            auto vector = ConstantVector::get(vecValues);
            module->getOrInsertGlobal(name, vector->getType());
            auto array = module->getNamedGlobal(name);
            array->setConstant(true);
            array->setLinkage(GlobalValue::LinkageTypes::PrivateLinkage);

            auto i32 = IntegerType::get(*context, 32); // 32
            auto Zero = ConstantInt::get(i32, 0);
            Constant *GEPIndices[] = {Zero, Zero};
            return ConstantExpr::getGetElementPtr(arrayType, array, GEPIndices);
        }
        case ParamType::FP64: {
            auto values = value.ToInt32Array();
            auto fp64 = Type::getFloatTy(*context);
            auto arrayType = ArrayType::get(fp64, value.size);
            for (int i = 0; i < value.size; ++i) {
                Constant *c = ConstantFP::get(*context, APFloat(value.ToFp64()));
                vecValues.push_back(c);
            }

            auto vector = ConstantVector::get(vecValues);
            module->getOrInsertGlobal(name, vector->getType());
            auto array = module->getNamedGlobal(name);
            array->setConstant(true);
            array->setLinkage(GlobalValue::LinkageTypes::PrivateLinkage);

            auto i32 = IntegerType::get(*context, 32); // 32
            auto Zero = ConstantInt::get(i32, 0);
            Constant *GEPIndices[] = {Zero, Zero};
            return ConstantExpr::getGetElementPtr(arrayType, array, GEPIndices);
        }
        default:
            return nullptr;
    }
}

llvm::Constant *LLVMCompiler::ToInt32ArrayLlvmValue(const std::string &name, ParamValue value,
    const std::unique_ptr<Module> &module, std::vector<llvm::Constant *> &vecValues)
{
    using namespace llvm;
    auto values = value.ToInt32Array();
    auto i32 = IntegerType::get(*context, 32); // 32
    auto arrayType = ArrayType::get(i32, value.size);
    for (int i = 0; i < value.size; ++i) {
        Constant *c = ConstantInt::get(i32, values[i]);
        vecValues.push_back(c);
    }

    module->getOrInsertGlobal(name, arrayType);
    auto array = module->getNamedGlobal(name);
    array->setInitializer(ConstantArray::get(arrayType, vecValues));
    array->setConstant(true);
    array->setLinkage(GlobalValue::LinkageTypes::PrivateLinkage);

    auto Zero = ConstantInt::get(i32, 0);
    Constant *GEPIndices[] = {Zero, Zero};
    return ConstantExpr::getGetElementPtr(arrayType, array, GEPIndices);
}

llvm::Constant *LLVMCompiler::to_array_llvm_value(const std::string &name, ParamValue value,
    const std::unique_ptr<Module> &module)
{
    using namespace llvm;
    std::vector<Constant *> vecValues;
    switch (value.type) {
        case ParamType::INT32: {
            return ToInt32ArrayLlvmValue(name, value, module, vecValues);
        }
        case ParamType::INT64: {
            auto values = value.ToInt32Array();
            auto i64 = IntegerType::get(*context, 64); // 64
            auto arrayType = ArrayType::get(i64, value.size);
            for (int i = 0; i < value.size; ++i) {
                Constant *c = ConstantInt::get(i64, values[i]);
            }
            module->getOrInsertGlobal(name, arrayType);
            auto array = module->getNamedGlobal(name);
            array->setInitializer(ConstantArray::get(arrayType, vecValues));
            array->setConstant(true);
            array->setLinkage(GlobalValue::LinkageTypes::PrivateLinkage);

            auto i32 = IntegerType::get(*context, 32); // 32
            auto Zero = ConstantInt::get(i32, 0);
            Constant *GEPIndices[] = {Zero, Zero};
            return ConstantExpr::getGetElementPtr(arrayType, array, GEPIndices);
        }
        case ParamType::FP64: {
            auto values = value.ToInt32Array();
            auto fp64 = Type::getFloatTy(*context);
            auto arrayType = ArrayType::get(fp64, value.size);
            for (int i = 0; i < value.size; ++i) {
                Constant *c = ConstantFP::get(*context, APFloat(value.ToFp64()));
            }
            module->getOrInsertGlobal(name, arrayType);
            auto array = module->getNamedGlobal(name);
            array->setInitializer(ConstantArray::get(arrayType, vecValues));
            array->setConstant(true);
            array->setLinkage(GlobalValue::LinkageTypes::PrivateLinkage);

            auto i32 = IntegerType::get(*context, 32); // 32
            auto Zero = ConstantInt::get(i32, 0);
            Constant *GEPIndices[] = {Zero, Zero};
            return ConstantExpr::getGetElementPtr(arrayType, array, GEPIndices);
        }
        default:
            return nullptr;
    }
}

bool optimizeAttributes(llvm::Function *function)
{
    using llvm::Attribute;
    function->removeFnAttr(Attribute::AttrKind::NoInline);
    function->addFnAttr(Attribute::AttrKind::AlwaysInline);
    function->addFnAttr(Attribute::AttrKind::ZExt);
    function->addFnAttr(Attribute::AttrKind::Hot);

    return true;
}

void annotatedFuncs(Module::global_iterator I, map<string, llvm::Function *> &annotFuncs)
{
    using namespace llvm;
    if (I->getName() == "llvm.global.annotations") {
        auto *CA = dyn_cast<ConstantArray>(I->getOperand(0));
        for (auto OI = CA->op_begin(); OI != CA->op_end(); ++OI) {
            auto *CS = dyn_cast<ConstantStruct>(OI->get());
            auto *FUNC = dyn_cast<Function>(CS->getOperand(0)->getOperand(0));
            auto *AnnotationGL = dyn_cast<GlobalVariable>(CS->getOperand(1)->getOperand(0));
            StringRef annotation = dyn_cast<ConstantDataArray>(AnnotationGL->getInitializer())->getAsCString();
            size_t index = annotation.find(SUFFIX);
            if (index != llvm::StringRef::npos) {
                StringRef specializationId = annotation.substr(0, index);
                annotFuncs.insert(std::make_pair(specializationId.str(), FUNC));
#ifdef DEBUG_LLVM
                outs() << "Found annotated function " << specializationId << ", " << FUNC->getName() << "\n";
#endif
            }
        }
    }
}
map<string, llvm::Function *> getAnnotatedFuncs(const std::unique_ptr<Module> &module)
{
    using namespace llvm;
    map<string, Function *> annotFuncs;
    for (Module::global_iterator I = module->global_begin(), E = module->global_end(); I != E; ++I) {
        annotatedFuncs(I, annotFuncs);
    }

    return annotFuncs;
}

string build_param_key(llvm::Function &func, int arg_pos)
{
    return func.getName().str() + "@" + std::to_string(arg_pos);
}
}
}

