/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
#include "util/debug.h"
#include "compiler/llvm_compiler.h"
#include "jit.h"

namespace omniruntime {
namespace jit {
void Jit::InitCompile()
{
    this->compiler = new LLVMCompiler();
}

Jit::~Jit()
{
    delete this->compiler;
}

Jit::Jit(std::vector<Context> contexts, CompilerType compilerType) : compiler(nullptr), contexts(std::move(contexts))
{
    switch (compilerType) {
        case CompilerType::LLVM:
            InitCompile();
            break;
        default:
            LogError("Compiler type not supported: %d", static_cast<int32_t>(compilerType));
            break;
    }
}

bool Jit::Specialize(const std::vector<Optimization> &optimizations,
    const std::vector<ModuleOptimization> &moduleOptimizations)
{
    for (auto &context : this->contexts) {
        auto jitTemplate = context.GetJitTemplate();
        bool loaded = this->compiler->LoadModule(jitTemplate);
        if (!loaded) {
            LogError("Failed to load template: %s", jitTemplate.c_str());
            return false;
        }
        LLVM_DEBUG_LOG("Loaded template: %s", context.GetJitTemplate().c_str());

        for (auto &specializationPair : context.GetSpecializations()) {
            this->compiler->AddSpecialization(specializationPair.first, specializationPair.second);
        }
        LLVM_DEBUG_LOG("Added specializations");
    }
    return this->compiler->SpecializeAndCompile(optimizations, moduleOptimizations);
}

uint64_t Jit::GetJitedFunction(const std::string &functionName, bool isNameMangled)
{
    return this->compiler->GetJitedFunction(functionName, isNameMangled);
}
}
}