#include "llvm_codegen.h"
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
#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace std;

LLVMCodeGen::LLVMCodeGen() {

}

// Logic to generate the function.
// TODO: Currently only supports comparision operator
llvm::Function* LLVMCodeGen::generateFunc(std::string name, Expr expr) 
{
  if (static_cast<const BinaryExpr*>(&expr) != nullptr)  
  {
     const BinaryExpr* b_expr = static_cast<const BinaryExpr*>(&expr);
  } 
  else if (static_cast<const ComparisionExpr*>(&expr) != nullptr)
  {
      const ComparisionExpr* c_expr = static_cast<const ComparisionExpr*>(&expr);
      std::vector<llvm::Type*> Doubles(2,
                             llvm::Type::getDoubleTy(*_context));
    llvm::FunctionType *FT =
        llvm::FunctionType::get(llvm::Type::getDoubleTy(*_context), Doubles, false);

    llvm::Function *fn =
        llvm::Function::Create(FT, llvm::Function::ExternalLinkage, name, _module.get());
    // Name the arguments
    llvm::Function::arg_iterator args = (*fn).arg_begin();
    llvm::Value *left_var = &*args;
    left_var->setName("left_val");
    ++args;
    llvm::Value *right_var = &*args;
    right_var->setName("right_val");
    
     // Create a new basic block to start insertion into.
     llvm::BasicBlock *fn_entry = llvm::BasicBlock::Create(*_context, "entry", fn);
     llvm::BasicBlock *fn_body = llvm::BasicBlock::Create(*_context, "body", fn);
     llvm::BasicBlock *fn_exit = llvm::BasicBlock::Create(*_context, "exit", fn);
     _builder->SetInsertPoint(fn_entry); 
     std::string less_than ("LT");
     _builder->SetInsertPoint(fn_body); 
     if(c_expr->op == 0) {
         _builder->CreateICmpSLT(left_var, right_var, "cmptmp");
     }
     _builder->SetInsertPoint(fn_exit);  

     verifyFunction(*fn);
     return fn;

  }
  
  return nullptr;
}

int main() {
    LLVMCodeGen codeGenObj;
    Expr expr;
    codeGenObj.generateFunc("dummy", expr);
}