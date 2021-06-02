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
#include <llvm/ExecutionEngine/ExecutionEngine.h>


using namespace llvm;
using namespace std;

// Given an expression generates the function for it.
class LLVMCodeGen
{
public:
    LLVMCodeGen();

    // Generate the functions
    void generateFunc(string name, Expr* expr);
    
    void generateComparisionExprFunc(ComparisionExpr* expr);

    void generateBinaryExprFunc(BinaryExpr* expr);

    void generateInExprFunc(InExpr* expr);

    void generateBetweenExprFunc(BetweenExpr* expr);

    // compile the generated code
    void compile();

    // Execute the compiled functions
    bool executeComparisionExprFunc(ComparisionExpr* expr, Data* data);

    bool executeBinaryExprFunc(BinaryExpr* expr, Data** dataArr);

    bool executeInExprFunc(InExpr* expr, Data* data);

    bool executeBetweenExprFunc(BetweenExpr* expr, Data* data);

private:
    std::string _func_name;
    llvm::Module* _module;    
    std::unique_ptr<llvm::ExecutionEngine> _ee;    
    int64_t funcAddr;  
};

int simpleTest();

#endif
