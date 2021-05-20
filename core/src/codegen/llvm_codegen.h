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

using namespace llvm;
using namespace std;

// Given an expression generates the function for it.
class LLVMCodeGen
{
public:
    LLVMCodeGen();

    void generateFunc(std::string name, ComparisionExpr exp);
    
    void generateFunc(std::string name, BinaryExpr exp);

    llvm::Value* generateComparisionBody(ComparisionExpr* expr, Value* left , Value* right);

    void compile();

    bool execute(int32_t left, int32_t right);

    bool execute(int32_t arg0, int32_t arg1, int32_t arg2, int32_t arg3);

private:
    std::string _func_name;
    llvm::Module* _module;        
    
};

int simpleTest();

#endif