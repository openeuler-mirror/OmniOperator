/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "harden_optimizer.h"
#include "../annotation.h"
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

using llvm::Module;
using llvm::outs;
using std::map;
using std::set;
using std::string;
using std::to_string;
using std::unique_ptr;

namespace omniruntime {
    namespace jit {
        LLVMCompiler::LLVMCompiler() : createOperatorSymbol()
        {
            this->jitter = nullptr;
            llvm::InitializeNativeTarget();
            llvm::InitializeNativeTargetAsmPrinter();
            this->config = std::make_unique<Config>();
            this->context = std::make_unique<llvm::LLVMContext>();
            this->layout = std::make_unique<llvm::StringRef>();
            this->builder = std::make_unique<llvm::IRBuilder<>>(*context);

            // TODO: load needed libraries for each module
            LoadExtraLibraries();
        }

        LLVMCompiler::~LLVMCompiler(){}

        bool LLVMCompiler::LoadOperatorTemplate(string operatorName, bool isDependency)
        {
            // TODO: have a proper registry for all the operators instead of loading from /opt/lib/ir
            // TODO: load operator templates by folder
            string templatePath = this->operatorPath + operatorName + LLVMCompiler::templateFileSuffix;

            llvm::SMDiagnostic error;
            auto module = llvm::parseIRFile(templatePath, error, *context);

            if (!module) {
                error.print("error loadding module", llvm::errs());
                // FIXME: proper error handling using exceptions?
                return false;
            }

            if (!isDependency) {
                for (auto &func : module->getFunctionList()) {
                    if (func.getName().contains(Compiler::entryFuncName)) {
                        this->createOperatorSymbol = func.getName().str();
                        break;
                    }
                }

                if (this->createOperatorSymbol.empty()) {
                    outs() << "Error: Couldn't find CreateOperator function\n";
                }
            }

            this->modules.push_back(std::move(module));
            return true;
        }

        LibraryLoader LLVMCompiler::ll;

        void LLVMCompiler::LoadExtraLibraries()
        {
            if (ll.FinishedLoading()) {
                return;
            }
            using namespace llvm::sys;
            StringOrNull ev = std::getenv("LD_LIBRARY_PATH");
            auto vec = ll.LoadLibraries(ev.msg(), true);
            ev = std::getenv("OMNI_LIBRARY_PATH");
            auto vec2 = ll.LoadLibraries(ev.msg(), false);
            string err;
            for (auto& s : vec) {
                if (DynamicLibrary::LoadLibraryPermanently(s.c_str(), &err)) {
                    llvm::errs() << "Failed to load core library at path " << s << "\n";
                    llvm::errs() << err << "\n";
                } else {
                    std::cout << "Successfully loaded core library at path " << s << std::endl;
                }
            }
            for (auto& s : vec2) {
                if (DynamicLibrary::LoadLibraryPermanently(s.c_str(), &err)) {
                    llvm::errs() << "Failed to load extra library at path " << s << "\n";
                    llvm::errs() << err << "\n";
                } else {
                    std::cout << "Successfully loaded extra library at path " << s << std::endl;
                }
            }
        }

        uint64_t LLVMCompiler::SpecializeAndCompile()
        {
            std::set<string> specializedModules;
            for (auto const &module : this->modules) {
                bool specialized = SpecializeModule(module);
                if (specialized) {
                    specializedModules.insert(module->getName().str());
                }
            }
            auto jit = CompileModules(specializedModules);
            specializedModules.clear();

            if (jit) {
                if (this->createOperatorSymbol.empty()) {
                    llvm::errs() << "Error: CreateOperator function not found yet\n";
                    return 0;
                }
                auto func = jit->lookup(this->createOperatorSymbol);
                if (auto e = func.takeError()) {
                    string msg = llvm::toString(std::move(e));
                    std::cerr << "LLVM ERROR: " << msg << std::endl;
                    if (msg.find("Failed to materialize symbols") != string::npos) {
                        std::cerr <<
                            "\nDid you forget to include a dependency in the extra dependencies file?"
                            << std::endl;
                    }
                }
                if (func) {
                    jitter = jit.release();
                    llvm::outs() << "Found CreateOperator symbol: " << this->createOperatorSymbol << "\n";
                    return func->getAddress();
                } else {
                    llvm::errs() << "Error: Cannot lookup the jitted CreateOperator method "
                                 << this->createOperatorSymbol
                                 << ", error: " << toString(func.takeError()) << "\n";
                    return 0;
                }
            }

            return 0;
        }

        bool LLVMCompiler::SpecializeModule(const std::unique_ptr<llvm::Module> &module)
        {
            using namespace llvm;

            map<string, Function *> annotatedFuncs = GetAnnotatedFuncs(module);
            if (annotatedFuncs.empty()) {
                return false;
            }

            for (auto &funcPair : annotatedFuncs) {
                string id = funcPair.first;
                Function *func = funcPair.second;
                OptimizeAttributes(func);
                HardenFunction(id, func, module);
            }

            llvm::verifyModule(*module);

            return true;
        }

        void LLVMCompiler::AddSpecialization(std::string id, Specialization specialization)
        {
            this->specializations.insert(std::make_pair(id, specialization));
        }

        // replaces the value of parameters passed to a function
        // this is done without modifying the signature
        // replacing the value directly inside of the function also make it
        // easier for optimizers to perform constant folding and propagation
        bool LLVMCompiler::HardenFunction(const string &specializationId, llvm::Function *function,
                                          const unique_ptr<Module> &module)
        {
            if (this->specializations.count(specializationId) == 0) {
                return false;
            }
            llvm::outs() << "hardening: " << function->getName().str() << "\n";
            Specialization specialization = this->specializations.at(specializationId);

            int count = 0;
            for (auto &arg : function->args()) {
                // 1. find the values from the Parameters that can be used for harden
                // use function_name and arg name as the key
                if (specialization.HasSpecializedParam(count)) {
                    ParamValue *newValue = specialization.GetSpecializedParam(count);
                    auto newArg = this->ToLlvmValue(BuildParamKey(*function, count), *newValue, module);
                    arg.replaceAllUsesWith(newArg);
                }
                count++;
            }

            return true;
        }

        // values for the parameters that is not harden
        // conflicting params, e.g. param value provided during hardening cannot be provided here again
        // this should be used for testing purpose only, we should expose a new function with new function type
        std::unique_ptr<llvm::orc::LLJIT> LLVMCompiler::CompileModules(const set<string> &specializedModules)
        {
            using namespace llvm;
            using namespace llvm::orc;
            // ELF format on linux to be supported later with llvm-12.0.1 fix
            ExitOnError exitOnErr;

            auto JTMB = exitOnErr(JITTargetMachineBuilder::detectHost());
            JTMB.setCodeGenOptLevel(CodeGenOpt::Default);

            auto JITTER = exitOnErr(
                LLJITBuilder().setJITTargetMachineBuilder(std::move(JTMB))
                //                    .setObjectLinkingLayerCreator(
                //                            [&](ExecutionSession &ES, const Triple &TT) {
                //                                return std::make_unique<ObjectLinkingLayer>(
                //                            })
                .create());

            JITTER->getIRTransformLayer().setTransform(HardenOptimizer(CodeGenOpt::Default, specializedModules));

            // enable loading common libraries available in the current process
            JITTER->getMainJITDylib().addGenerator(
                exitOnErr(DynamicLibrarySearchGenerator::GetForCurrentProcess(
                    JITTER->getDataLayout().getGlobalPrefix())));
            for (auto &module : this->modules) {
                std::string moduleName = module->getName().str();
                outs() << "addIRModule: " << moduleName << "\n";
                auto err = JITTER->addIRModule(
                    ThreadSafeModule(std::move(module), std::move(std::make_unique<llvm::LLVMContext>())));
                if (err) {
                    errs() << "Error: failed adding IR Module " << moduleName << "\n";
                    return nullptr;
                }
            }
            return JITTER;
        }

        llvm::Constant *LLVMCompiler::ToLlvmValue(
            const std::string &name, ParamValue value, const std::unique_ptr<Module> &module)
        {
            if (value.type == ParamType::ARRAY2D) {
                return To2darrayLlvmValue(name, value, module);
            } else if (value.IsScalar()) {
                return ToScalarLlvmValue(value);
            } else { // array type
                if (value.vector) {
                    return ToVectorLlvmValue(name, value, module);
                } else {
                    return ToArrayLlvmValue(name, value, module);
                }
            }
        }

        llvm::Constant *LLVMCompiler::ToScalarLlvmValue(ParamValue value)
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

        llvm::Constant *LLVMCompiler::To2darrayLlvmValue(const std::string &name, ParamValue value,
            const std::unique_ptr<Module> &module)
        {
            using namespace llvm;
            auto params = value.ToParamList();
            std::vector<Constant *> vec2dValues;
            auto i64 = IntegerType::get(*context, 64); // 64
            auto arrayType = ArrayType::get(i64, params->size());

            int count = 0;
            for (ParamValue param : *params) {
                Constant *element = ToArrayLlvmValue(name + "_" + to_string(count), param, module);
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
            auto zero = ConstantInt::get(i32, 0);
            Constant *gepIndices[] = {zero, zero};
            return ConstantExpr::getGetElementPtr(arrayType, array, gepIndices);
        }

        llvm::Constant *LLVMCompiler::ToInt32VectorLlvmValue(
            ParamValue value, std::vector<llvm::Constant *> &vecValues)
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

        llvm::Constant *LLVMCompiler::ToVectorLlvmValue(const std::string &name, ParamValue value,
            const std::unique_ptr<Module> &module)
        {
            using namespace llvm;
            std::vector<Constant *> vecValues;
            switch (value.type) {
                case ParamType::INT32: {
                    return ToInt32VectorLlvmValue(value, vecValues);
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
                    auto zero = ConstantInt::get(i32, 0);
                    Constant *gepIndices[] = {zero, zero};
                    return ConstantExpr::getGetElementPtr(arrayType, array, gepIndices);
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
                    auto zero = ConstantInt::get(i32, 0);
                    Constant *gepIndices[] = {zero, zero};
                    return ConstantExpr::getGetElementPtr(arrayType, array, gepIndices);
                }
                default:
                    return nullptr;
            }
        }

        llvm::Constant *LLVMCompiler::ToInt32ArrayLlvmValue(
            const std::string &name, ParamValue value, const std::unique_ptr<Module> &module,
            std::vector<llvm::Constant *> &vecValues)
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

            auto zero = ConstantInt::get(i32, 0);
            Constant *gepIndices[] = {zero, zero};
            return ConstantExpr::getGetElementPtr(arrayType, array, gepIndices);
        }

        llvm::Constant *LLVMCompiler::ToArrayLlvmValue(const std::string &name, ParamValue value,
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
                    auto zero = ConstantInt::get(i32, 0);
                    Constant *gepIndices[] = {zero, zero};
                    return ConstantExpr::getGetElementPtr(arrayType, array, gepIndices);
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
                    auto zero = ConstantInt::get(i32, 0);
                    Constant *gepIndices[] = {zero, zero};
                    return ConstantExpr::getGetElementPtr(arrayType, array, gepIndices);
                }
                default:
                    return nullptr;
            }
        }

        bool OptimizeAttributes(llvm::Function *function)
        {
            using llvm::Attribute;
            function->removeFnAttr(Attribute::AttrKind::NoInline);
            function->addFnAttr(Attribute::AttrKind::AlwaysInline);
            function->addFnAttr(Attribute::AttrKind::ZExt);
            function->addFnAttr(Attribute::AttrKind::Hot);

            return true;
        }

        void AnnotatedFuncs(Module::global_iterator i, map<string, llvm::Function *> &annotFuncs)
        {
            using namespace llvm;
            if (i->getName() == "llvm.global.annotations") {
                auto *ca = dyn_cast<ConstantArray>(i->getOperand(0));
                for (auto oi = ca->op_begin(); oi != ca->op_end(); ++oi) {
                    auto *cs = dyn_cast<ConstantStruct>(oi->get());
                    auto *func = dyn_cast<Function>(cs->getOperand(0)->getOperand(0));
                    auto *annotationGL = dyn_cast<GlobalVariable>(cs->getOperand(1)->getOperand(0));
                    StringRef annotation = dyn_cast<ConstantDataArray>(
                            annotationGL->getInitializer())->getAsCString();
                    size_t index = annotation.find(SUFFIX);
                    if (index != llvm::StringRef::npos) {
                        StringRef specializationId = annotation.substr(0, index);
                        annotFuncs.insert(std::make_pair(specializationId.str(), func));
                        outs() << "Found annotated function " << specializationId << ", " << func->getName()
                               << "\n";
                    }
                }
            }
        }
        map<string, llvm::Function *> GetAnnotatedFuncs(const std::unique_ptr<Module> &module)
        {
            using namespace llvm;
            map<string, Function *> annotFuncs;
            for (Module::global_iterator i = module->global_begin(),
                         e = module->global_end();
                 i != e;
                 ++i) {
                AnnotatedFuncs(i, annotFuncs);
            }

            return annotFuncs;
        }

        string BuildParamKey(llvm::Function &func, int argPos)
        {
            return func.getName().str() + "@" + std::to_string(argPos);
        }
    }
}