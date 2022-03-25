/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: Expression code generation utilities
 */

#include "codegen_utils.h"
#include "llvm/Transforms/IPO.h"

using namespace std;
using namespace llvm;

CallInst *CodeGenUtils::CreateCall(llvm::Function *func, std::vector<llvm::Value *> argsVals, string name)
{
    return builder.CreateCall(func, argsVals, name);
}

void CodeGenUtils::RecordMainFunction(llvm::Function *func)
{
    this->function = func;
}

void CodeGenUtils::RemoveUnusedFunctions()
{
    llvm::Function *preserved = function;
    mpm.add(llvm::createInternalizePass(
        [preserved](const llvm::GlobalValue &func) { return (func.getName().str() == preserved->getName().str()); }));
    mpm.add(llvm::createGlobalDCEPass());
}