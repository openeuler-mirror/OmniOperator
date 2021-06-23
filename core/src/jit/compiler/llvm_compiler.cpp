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

#include <set>

using llvm::Module;
using llvm::outs;
using std::string;

namespace omniruntime {
    namespace jit {
        LLVMCompiler::LLVMCompiler() {
            this->config = new Config();
            llvm::InitializeNativeTarget();
            llvm::InitializeNativeTargetAsmPrinter();
            this->context = std::make_unique<llvm::LLVMContext>();
            this->layout = std::make_unique<llvm::StringRef>();
            this->builder = std::make_unique<llvm::IRBuilder<>>(*context);

            // TODO: load needed libraries for each module
            loadExtraLibraries();
        }

        bool LLVMCompiler::loadOperatorTemplate(string operatorName, bool isDependency) {
            // TODO: have a proper registry for all the operators instead of loading from /opt/lib/ir
            // TODO: load operator templates by folder
            string templatePath = this->operatorPath + operatorName + LLVMCompiler::templateFileSuffix;

            llvm::SMDiagnostic error;
            auto module = llvm::parseIRFile(templatePath, error, *context);

            if (!module) {
                error.print("error loading module", llvm::errs());
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
                    outs() << "Error: Couldn't find createOperator function\n";
                }
            }

            this->modules.push_back(std::move(module));
            return true;
        }

        void LLVMCompiler::loadExtraLibraries() {
            using namespace llvm::sys;

            bool loaded = false;
            // TODO: find a better way to load this lib, it differs on different platform
            loaded = !DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
//            loaded = !DynamicLibrary::LoadLibraryPermanently("/opt/rh/devtoolset-7/root/usr/lib/gcc/x86_64-redhat-linux/7/libstdc++.so");
//            loaded = !DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-redhat-linux/4.8.5/libstdc++.so");
            if (!loaded) {
                llvm::errs() << "Failed to load c++ lib\n";
            }
            loaded = !DynamicLibrary::LoadLibraryPermanently("/usr/local/lib/libjemalloc.so.2");
            if (!loaded) {
                llvm::errs() << "Failed to load jemalloc lib\n";
            }
        }

        uint64_t LLVMCompiler::specializeAndCompile() {
            for (auto const &module : this->modules) {
                specializeModule(module);
            }
            auto jit = compileModules();
            if (jit) {
                if (this->createOperatorSymbol.empty()) {
                    llvm::errs() << "Error: CreateOperator function not found yet\n";
                    return 0;
                }

                auto func = jit->lookup(this->createOperatorSymbol);
                if (func) {
                    jitter = jit.release();
                    llvm::outs() << "Found createOperator symbol: " << this->createOperatorSymbol << "\n";
                    return func->getAddress();
                } else {
                    llvm::errs() << "Error: Cannot lookup the jitted createOperator method "
                                 << this->createOperatorSymbol
                                 << ", error: " << toString(func.takeError()) << "\n";
                    return 0;
                }
            }

            return 0;
        }

        bool LLVMCompiler::specializeModule(const std::unique_ptr<llvm::Module> &module) {
            using namespace llvm;

            map<string, Function *> annotatedFuncs = getAnnotatedFuncs(module);

            for (auto &funcPair : annotatedFuncs) {
                string id = funcPair.first;
                Function *func = funcPair.second;
                //outs() << func.getName() << "\n";
                optimizeAttributes(func);
                harden_function(id, func, module);
            }

            llvm::verifyModule(*module);
            // outs() << "harden end..." << "\n";

            return true;
        }

        void LLVMCompiler::addSpecialization(std::string id, Specialization specialization) {
            this->specializations.insert(std::make_pair(id, specialization));
        }

        // replaces the value of parameters passed to a function
        // this is done without modifying the signature
        // replacing the value directly inside of the function also make it
        // easier for optimizers to perform constant folding and propagation
        bool LLVMCompiler::harden_function(const string &specializationId, llvm::Function *function,
                                           const unique_ptr<Module> &module) {
            if (this->specializations.count(specializationId) == 0) {
                return false;
            }
            llvm::outs() << "hardening: " << function->getName().str() << "\n";
            Specialization specialization = this->specializations.at(specializationId);

            int count = 0;
            for (auto &arg : function->args()) {
                //outs() << "processing arg: " << function.getName() << "@" << arg.getName() << "\n";
                //1. find the values from the Parameters that can be used for harden
                // use function_name and arg name as the key
                if (specialization.hasSpecializedParam(count)) {
                    ParamValue *newValue = specialization.getSpecializedParam(count);
                    auto newArg = this->to_llvm_value(build_param_key(*function, count), *newValue, module);
                    //            arg.print(errs());
                    //            errs() << "\n";
                    //            newArg->print(errs());
                    //            errs() << "\n";
                    arg.replaceAllUsesWith(newArg);
                }
                count++;
            }

            return true;
        }

        // values for the parameters that is not harden
        // conflicting params, e.g. param value provided during hardening cannot be provided here again
        // this should be used for testing purpose only, we should expose a new function with new function type
        std::unique_ptr<llvm::orc::LLJIT> LLVMCompiler::compileModules() {
            using namespace llvm;
            using namespace llvm::orc;
            //ELF format on linux to be supported later with llvm-12.0.1 fix
            ExitOnError ExitOnErr;

            auto JTMB = ExitOnErr(JITTargetMachineBuilder::detectHost());
            //    JTMB.setCodeModel(CodeModel::Small);
            //    JTMB.setRelocationModel(Reloc::PIC_);
            JTMB.setCodeGenOptLevel(CodeGenOpt::Default);

            auto JITTER = ExitOnErr(
                    LLJITBuilder()
                            .setJITTargetMachineBuilder(std::move(JTMB))
                                    //                    .setObjectLinkingLayerCreator(
                                    //                            [&](ExecutionSession &ES, const Triple &TT) {
                                    //                                return std::make_unique<ObjectLinkingLayer>(
                                    //                                        ES, std::make_unique<jitlink::InProcessMemoryManager>());
                                    //                            })
                            .create());

            JITTER->getIRTransformLayer().setTransform(HardenOptimizer(CodeGenOpt::Default));
            // JITTER->getIRTransformLayer().setTransform(OptimizationTransform());

            // enable loading common libraries available in the current process
            JITTER->getMainJITDylib().addGenerator(
                    ExitOnErr(DynamicLibrarySearchGenerator::GetForCurrentProcess(
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

//            JITTER->getObjTransformLayer().setTransform(DumpObjects("/opt/omnijit/dump", "omnijit"));

            return JITTER;
        }

        llvm::Constant *
        LLVMCompiler::to_llvm_value(const std::string &name, ParamValue value, const std::unique_ptr<Module> &module) {
            if (value.type == ParamType::ARRAY2D) {
                return to_2darray_llvm_value(name, value, module);
            } else if (value.isScalar()) {
                return to_scalar_llvm_value(value);
            } else { //array type
                if (value.vector) {
                    return to_vector_llvm_value(name, value, module);
                } else {
                    return to_array_llvm_value(name, value, module);
                }
            }
        }

        llvm::Constant *LLVMCompiler::to_scalar_llvm_value(ParamValue value) {
            using namespace llvm;

            Constant *llvmValue;
            switch (value.type) {
                case ParamType::INT32:
                    //outs() << "creating int32: " << value.to_int32() << " param value \n";
                    llvmValue = ConstantInt::get(IntegerType::get(*context, 32), value.to_int32(), true);
                    break;
                case ParamType::INT64:
                    //outs() << "creating int64" << value.to_int64() << " param value \n";
                    llvmValue = ConstantInt::get(IntegerType::get(*context, 64), value.to_int64(), true);
                    break;
                case ParamType::FP64:
                    //outs() << "creating fp64" << value.to_fp64() << " param value \n";
                    llvmValue = ConstantFP::get(*context, APFloat(value.to_fp64()));
                    break;
            }
            return llvmValue;
        }

        llvm::Constant *LLVMCompiler::to_2darray_llvm_value(const std::string &name, ParamValue value,
                                                            const std::unique_ptr<Module> &module) {
            using namespace llvm;

            auto params = value.to_param_list();
            std::vector<Constant *> vec2dValues;
            auto i64 = IntegerType::get(*context, 64);
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

            auto i32 = IntegerType::get(*context, 32);
            auto Zero = ConstantInt::get(i32, 0);
            Constant *GEPIndices[] = {Zero, Zero};
            return ConstantExpr::getGetElementPtr(arrayType, array, GEPIndices);
        }

        llvm::Constant *LLVMCompiler::to_vector_llvm_value(const std::string &name, ParamValue value,
                                                           const std::unique_ptr<Module> &module) {
            using namespace llvm;

            std::vector<Constant *> vecValues;
            switch (value.type) {
                case ParamType::INT32: {
                    auto values = *value.to_int32_vec();
                    auto i32 = IntegerType::get(*context, 32);
                    for (int i = 0; i < value.size; ++i) {
                        //outs() << "creating int32[" << i << "] = " << values[i] << "\n";
                        Constant *c = ConstantInt::get(i32, values[i]);
                        vecValues.push_back(c);
                    }
                    auto vec = ConstantVector::get(vecValues);
                    return vec;
                }
                case ParamType::INT64: {
                    auto values = value.to_int32_array();
                    auto i64 = IntegerType::get(*context, 64);
                    auto arrayType = ArrayType::get(i64, value.size);
                    for (int i = 0; i < value.size; ++i) {
                        //outs() << "creating int64[" << i << "] = " << values[i] << "\n";
                        Constant *c = ConstantInt::get(i64, values[i]);
                        vecValues.push_back(c);
                    }

                    auto vector = ConstantVector::get(vecValues);
                    module->getOrInsertGlobal(name, vector->getType());
                    auto array = module->getNamedGlobal(name);
                    array->setConstant(true);
                    array->setLinkage(GlobalValue::LinkageTypes::PrivateLinkage);

                    auto i32 = IntegerType::get(*context, 32);
                    auto Zero = ConstantInt::get(i32, 0);
                    Constant *GEPIndices[] = {Zero, Zero};
                    return ConstantExpr::getGetElementPtr(arrayType, array, GEPIndices);
                }
                case ParamType::FP64: {
                    auto values = value.to_int32_array();
                    auto fp64 = Type::getFloatTy(*context);
                    auto arrayType = ArrayType::get(fp64, value.size);
                    for (int i = 0; i < value.size; ++i) {
                        //outs() << "creating int[" << i << "] = " << values[i] << "\n";
                        Constant *c = ConstantFP::get(*context, APFloat(value.to_fp64()));
                        c->getType();
                        vecValues.push_back(c);
                    }

                    auto vector = ConstantVector::get(vecValues);
                    module->getOrInsertGlobal(name, vector->getType());
                    auto array = module->getNamedGlobal(name);
                    array->setConstant(true);
                    array->setLinkage(GlobalValue::LinkageTypes::PrivateLinkage);

                    auto i32 = IntegerType::get(*context, 32);
                    auto Zero = ConstantInt::get(i32, 0);
                    Constant *GEPIndices[] = {Zero, Zero};
                    return ConstantExpr::getGetElementPtr(arrayType, array, GEPIndices);
                }
            }
        }

        llvm::Constant *LLVMCompiler::to_array_llvm_value(const std::string &name, ParamValue value,
                                                          const std::unique_ptr<Module> &module) {
            using namespace llvm;

            std::vector<Constant *> vecValues;
            switch (value.type) {
                case ParamType::INT32: {
                    auto values = value.to_int32_array();
                    auto i32 = IntegerType::get(*context, 32);
                    auto arrayType = ArrayType::get(i32, value.size);
                    for (int i = 0; i < value.size; ++i) {
                        //outs() << "creating int32[" << i << "] = " << values[i] << "\n";
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
                case ParamType::INT64: {
                    auto values = value.to_int32_array();
                    auto i64 = IntegerType::get(*context, 64);
                    auto arrayType = ArrayType::get(i64, value.size);
                    for (int i = 0; i < value.size; ++i) {
                        //outs() << "creating int64[" << i << "] = " << values[i] << "\n";
                        Constant *c = ConstantInt::get(i64, values[i]);
                        vecValues.push_back(c);
                    }
                    module->getOrInsertGlobal(name, arrayType);
                    auto array = module->getNamedGlobal(name);
                    array->setInitializer(ConstantArray::get(arrayType, vecValues));
                    array->setConstant(true);
                    array->setLinkage(GlobalValue::LinkageTypes::PrivateLinkage);

                    auto i32 = IntegerType::get(*context, 32);
                    auto Zero = ConstantInt::get(i32, 0);
                    Constant *GEPIndices[] = {Zero, Zero};
                    return ConstantExpr::getGetElementPtr(arrayType, array, GEPIndices);
                }
                case ParamType::FP64: {
                    auto values = value.to_int32_array();
                    auto fp64 = Type::getFloatTy(*context);
                    auto arrayType = ArrayType::get(fp64, value.size);
                    for (int i = 0; i < value.size; ++i) {
                        //outs() << "creating int[" << i << "] = " << values[i] << "\n";
                        Constant *c = ConstantFP::get(*context, APFloat(value.to_fp64()));
                        vecValues.push_back(c);
                    }
                    module->getOrInsertGlobal(name, arrayType);
                    auto array = module->getNamedGlobal(name);
                    array->setInitializer(ConstantArray::get(arrayType, vecValues));
                    array->setConstant(true);
                    array->setLinkage(GlobalValue::LinkageTypes::PrivateLinkage);

                    auto i32 = IntegerType::get(*context, 32);
                    auto Zero = ConstantInt::get(i32, 0);
                    Constant *GEPIndices[] = {Zero, Zero};
                    return ConstantExpr::getGetElementPtr(arrayType, array, GEPIndices);
                }
            }
        }

        bool optimizeAttributes(llvm::Function *function) {
            using llvm::Attribute;

            //outs() << "found function: " << func.getName() << "\n";
            function->removeFnAttr(Attribute::AttrKind::NoInline);
            function->removeFnAttr(Attribute::AttrKind::OptimizeNone);
            function->addFnAttr(Attribute::AttrKind::AlwaysInline);
            function->addFnAttr(Attribute::AttrKind::ZExt);
            function->addFnAttr(Attribute::AttrKind::Hot);

            return true;
        }

        map<string, llvm::Function *> getAnnotatedFuncs(const std::unique_ptr<Module> &module) {
            using namespace llvm;

            map<string, Function *> annotFuncs;
            for (Module::global_iterator I = module->global_begin(),
                         E = module->global_end();
                 I != E;
                 ++I) {

                if (I->getName() == "llvm.global.annotations") {
                    auto *CA = dyn_cast<ConstantArray>(I->getOperand(0));
                    for (auto OI = CA->op_begin(); OI != CA->op_end(); ++OI) {
                        auto *CS = dyn_cast<ConstantStruct>(OI->get());
                        auto *FUNC = dyn_cast<Function>(CS->getOperand(0)->getOperand(0));
                        auto *AnnotationGL = dyn_cast<GlobalVariable>(CS->getOperand(1)->getOperand(0));
                        StringRef annotation = dyn_cast<ConstantDataArray>(
                                AnnotationGL->getInitializer())->getAsCString();
                        size_t index = annotation.find(SUFFIX);
                        if (index != llvm::StringRef::npos) {
                            StringRef specializationId = annotation.substr(0, index);
                            annotFuncs.insert(std::make_pair(specializationId.str(), FUNC));
                            outs() << "Found annotated function " << specializationId << ", " << FUNC->getName()
                                   << "\n";
                        }
                    }
                }
            }

            return annotFuncs;
        }

        string build_param_key(llvm::Function &func, int arg_pos) {
            return func.getName().str() + "@" + std::to_string(arg_pos);
        }
    }
}