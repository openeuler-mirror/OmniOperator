//
// Created by kkrazy on 2021-03-23.
//
#include "sort_hammer.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/Analysis/TargetTransformInfo.h"
#include "llvm/ExecutionEngine/Orc/ExecutionUtils.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/ExecutionEngine/Orc/ObjectLinkingLayer.h"
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
#include "llvm/Transforms/Utils/Cloning.h"

using namespace codegen;
using namespace llvm;
using namespace llvm::orc;
using namespace std;

// values for the parameters that is not harden
// conflicting params, e.g. param value provided during hardening cannot be provided here again
// this should be used for testing purpose only, we should expose a new function with new function type
std::unique_ptr<LLJIT> SortHammer::create_jitter(std::list<Hammer *> deps, CodeGenOpt::Level level)
{

    //ELF format on linux to be supported later with llvm-12.0.1 fix

    auto JTMB = ExitOnErr(JITTargetMachineBuilder::detectHost());
    //    JTMB.setCodeModel(CodeModel::Small);
    //    JTMB.setRelocationModel(Reloc::PIC_);
    JTMB.setCodeGenOptLevel(level);

    auto JITTER = ExitOnErr(
        LLJITBuilder()
            .setJITTargetMachineBuilder(std::move(JTMB))
            //                    .setObjectLinkingLayerCreator(
            //                            [&](ExecutionSession &ES, const Triple &TT) {
            //                                return std::make_unique<ObjectLinkingLayer>(
            //                                        ES, std::make_unique<jitlink::InProcessMemoryManager>());
            //                            })
            .create());

    JITTER->getIRTransformLayer().setTransform(HardenOptimizer(CodeGenOpt::Aggressive));
    // JITTER->getIRTransformLayer().setTransform(OptimizationTransform());

    //enable loading commom libraries available in the current process
    JITTER->getMainJITDylib().addGenerator(
        ExitOnErr(DynamicLibrarySearchGenerator::GetForCurrentProcess(
            JITTER->getDataLayout().getGlobalPrefix())));

    auto err = JITTER->addIRModule(ThreadSafeModule(std::move(this->get_module()), std::move(this->get_context())));
    for (Hammer *hammer : deps)
    {
        auto err = JITTER->addIRModule(ThreadSafeModule(hammer->get_module(), hammer->get_context()));
    }

    //JITTER->getObjTransformLayer().setTransform(DumpObjects("/opt/kkrazy/joy/dump", "kkrazy"));

    if (!err)
    {
        return JITTER;
    }
    return nullptr;
};
