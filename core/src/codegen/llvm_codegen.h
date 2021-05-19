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

    void generateFunc(std::string name, ComparisionExpr exp);
    
    void generateFunc(std::string name, BinaryExpr exp);

    void compile();

    bool execute(int32_t left, int32_t right);

private:
    std::string _func_name;
    llvm::Module* _module;        
    
};

int simpleTest();

#endif