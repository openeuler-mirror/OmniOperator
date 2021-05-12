#include "hammer.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/ExecutionEngine/Orc/ObjectTransformLayer.h"
#include "llvm/ExecutionEngine/Orc/ThreadSafeModule.h"
#include "llvm/IR/Attributes.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"
#include "llvm/IRReader/IRReader.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/SourceMgr.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/IPO.h"

#include <iosfwd>
#include <list>
#include <string>

using namespace codegen;
using namespace llvm::orc;
using namespace std;

ExitOnError ExitOnErr;

Hammer::Hammer(StringRef file, std::map<std::string, ParamValue *> _params) : params(_params) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    context = std::make_unique<LLVMContext>();
    layout = std::make_unique<StringRef>();
    builder = std::make_unique<IRBuilder<>>(*context);
    module = load(file);
}

// we are replacing the parameter with hardcoded values on the existing function.
// future enhancements to create a new version of the function so that we can
// reuse already hardened function
bool Hammer::harden() {

    for (auto &func : module->getFunctionList()) {
        //outs() << func.getName() << "\n";
        if (eligible(func)) {
            removeFnAttribute(func);
            harden_function(func);
        }
    }

    verifyModule(*module);
    // outs() << "harden end..." << "\n";
    return true;
}

// values for the parameters that is not harden
// conflicting params, e.g. param value provided during hardening cannot be provided here again
// this should be used for testing purpose only, we should expose a new function with new function type
std::unique_ptr<LLJIT> Hammer::create_jitter(std::list<Hammer *> deps, HammerConfig &config) {

    //ELF format on linux to be supported later with llvm-12.0.1 fix

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

    //enable loading commom libraries available in the current process
    JITTER->getMainJITDylib().addGenerator(
            ExitOnErr(DynamicLibrarySearchGenerator::GetForCurrentProcess(
                    JITTER->getDataLayout().getGlobalPrefix())));

    auto err = JITTER->addIRModule(ThreadSafeModule(std::move(module), std::move(context)));
    for (Hammer *hammer : deps) {
        auto err = JITTER->addIRModule(ThreadSafeModule(std::move(hammer->module), std::move(hammer->context)));
    }

    //JITTER->getObjTransformLayer().setTransform(DumpObjects("/opt/kkrazy/joy/dump", "kkrazy"));

    if (!err) {
        return JITTER;
    }
    return nullptr;
}

bool Hammer::verify_param(std::map<std::string, ParamValue *> params) {
    for (auto param : params) {
        auto key = param.first;
        if (this->params.find(key) != this->params.end()) {
            //conflicting parameter found
            return false;
        }
    }
    return true;
}

// Load a module from a .ll file
// We can enhance the performance to use .bc file instead
std::unique_ptr<Module> Hammer::load(StringRef file) {
    using namespace llvm;
    using namespace llvm::orc;

    SMDiagnostic error;
    auto module = parseIRFile(file, error, *context);
    if (!module) {
        error.print("error loadding module", errs());
        // FIXME: proper error handling using exceptions?
        return nullptr;
    }
    return module;
}

std::string build_param_key(Function &func, int arg_pos) {
    return func.getName().str() + "@" + std::to_string(arg_pos);
}

// In order to make it possible for param specialization in a loop
// We need to:
//  1. unroll the loop so that each invocation is spelled out, this would
//     provide the opportunity to specialize each individual
//  2.
//
bool Hammer::specialize_callsite(CallInst call, std::map<std::string, std::vector<ParamValue>>) {
    //    auto function = call.getCalledFunction();
    //    CloneFunctionInto()
    return false;
}

// replaces the value of parameters passed to a function
// this is done without modifying the signature
// replacing the value directly inside of the function also make it
// easier for optimizers to perform constant folding and propagation
bool Hammer::harden_function(Function &function) {
    //outs() << "hardening: " << function.getName().str() << "\n";

    int count = 0;
    for (auto &arg : function.args()) {

        //outs() << "processing arg: " << function.getName() << "@" << arg.getName() << "\n";
        //1. find the values from the Parameters that can be used for harden
        // use function_name and arg name as the key
        auto key = build_param_key(function, count);

        if (params.find(key) != params.end()) {
            auto value = params[key];
            auto newArg = to_llvm_value(key, *value);
//            arg.print(errs());
//            errs() << "\n";
//            newArg->print(errs());
//            errs() << "\n";
            arg.replaceAllUsesWith(newArg);
        } else {
            //outs() << "no new argument value to bind\n";
        }
        count++;
    }

    return false;
}

//replace parameter to function with constant paramter value
//will help perform function inlining
bool Hammer::harden_call(Function &function) {
    for (auto &blocks : function) { //visit all blocks in the function
        for (auto &instruction : blocks) { //visit all instructions in the block
            if (CallInst *callInst = dyn_cast<CallInst>(&instruction)) {
                if (eligible(*callInst)) {
                    harden_call(*callInst->getCalledFunction());
                }
            }
        }
    }
    return false;
}

// returns true if a function call is eligible for hardening
bool Hammer::eligible(CallInst &callInst) {
    return false;
}

//returns true if a function is eligible for hardening
bool Hammer::eligible(Function &function) {
    if (function.getName().find("process") != string::npos
        || function.getName().find("inloop") != string::npos
        || function.getName().find("compareTo") != string::npos) {
        //outs() << "found function: " << func.getName() << "\n";
        function.removeFnAttr(Attribute::AttrKind::NoInline);
        function.removeFnAttr(Attribute::AttrKind::OptimizeNone);
        function.addFnAttr(Attribute::AttrKind::AlwaysInline);
        function.addFnAttr(Attribute::AttrKind::ZExt);
        function.addFnAttr(Attribute::AttrKind::Hot);

        return true;
    }
    return false;
}

//remove all attributes that would potentially stop optimization
bool Hammer::removeFnAttribute(Function &function) {
    function.removeFnAttr(Attribute::AttrKind::NoInline);
    function.removeFnAttr(Attribute::AttrKind::OptimizeNone);
    return true;
}

bool Hammer::harden_param(Function &function) {
    return false;
}

Constant *Hammer::to_llvm_value(std::string name, ParamValue value) {
    if (value.type == ParamType::ARRAY2D) {
        return to_2darray_llvm_value(name, value);
    } else if (value.size == 1) { //scalar type
        return to_scalar_llvm_value(value);
    } else { //array type
        if (value.vector) {
            return to_vector_llvm_value(name, value);
        } else {
            return to_array_llvm_value(name, value);
        }
    }
}

Constant *Hammer::to_scalar_llvm_value(ParamValue value) {
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

Constant *Hammer::to_2darray_llvm_value(std::string name, ParamValue value) {
    auto params = value.to_param_list();
    std::vector<Constant *> vec2dValues;
    auto i64 = IntegerType::get(*context, 64);
    auto arrayType = ArrayType::get(i64, params->size());

    int count = 0;
    for (ParamValue param : *params) {
        Constant *element = to_array_llvm_value(name + "_" + to_string(count), param);
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

Constant *Hammer::to_vector_llvm_value(std::string name, ParamValue value) {
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

Constant *Hammer::to_array_llvm_value(std::string name, ParamValue value) {
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