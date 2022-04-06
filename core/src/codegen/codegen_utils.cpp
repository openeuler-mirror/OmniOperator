/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generation utilities
 */

#include "codegen_utils.h"
#include "func_registry.h"
#include "llvm/Transforms/IPO.h"

using namespace std;
using namespace llvm;
using omniruntime::type::DataTypeId;

namespace omniruntime {
CallInst *CodeGenUtils::CreateCall(llvm::Function *func, const std::vector<llvm::Value *> &argsVals)
{
    return builder.CreateCall(func, argsVals);
}

CallInst *CodeGenUtils::CreateCall(llvm::Function *func, const std::vector<llvm::Value *> &argsVals, const string &name)
{
    return builder.CreateCall(func, argsVals, name);
}

const omniruntime::Function *CodeGenUtils::GetFunction(const string &functionName,
    const std::vector<DataTypeId> &params, DataTypeId returnType)
{
    auto compareFuncSignature = FunctionSignature(functionName, params, returnType);
    return omniruntime::FunctionRegistry::LookupFunction(&compareFuncSignature);
}

void CodeGenUtils::RecordFunctions(llvm::Function *func)
{
    this->visited.insert(func->getName().str());
}

void CodeGenUtils::RemoveUnusedFunctions()
{
    // use set here because we define two main funcs in one module
    // (one is wrapper and another actually does codegen), they both need to be preserved.
    std::set<string> preserved;
    preserved.insert(visited.begin(), visited.end());
    mpm.add(llvm::createInternalizePass([preserved](const llvm::GlobalValue &func) {
        return (preserved.find(func.getName().str()) != preserved.end());
    }));
    mpm.add(llvm::createGlobalDCEPass());
    mpm.run(module);
}
}