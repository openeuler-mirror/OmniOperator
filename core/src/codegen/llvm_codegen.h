#ifndef __LLVM_CODEGEN_H__
#define __LLVM_CODEGEN_H__

#include "../common/expressions.h"
#include <cstring>
#include "llvm/ADT/APFloat.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constants.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Verifier.h"

// Given an expression generates the function for it.
class LLVMCodeGen
{
public:
    LLVMCodeGen();
    llvm::Function *generateFunc(std::string name, Expr exp);

private:
    std::unique_ptr<llvm::LLVMContext> _context;
    std::unique_ptr<llvm::IRBuilder<>> _builder;
    std::unique_ptr<llvm::Module> _module;
        
    
};

int simpleTest();

#endif