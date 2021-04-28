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

static std::unique_ptr<llvm::LLVMContext> TheContext;
static std::unique_ptr<llvm::IRBuilder<>> Builder;
static std::unique_ptr<llvm::Module> TheModule;

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
                             llvm::Type::getDoubleTy(*TheContext));
    llvm::FunctionType *FT =
        llvm::FunctionType::get(llvm::Type::getDoubleTy(*TheContext), Doubles, false);

    llvm::Function *F =
        llvm::Function::Create(FT, llvm::Function::ExternalLinkage, name, TheModule.get());
      
  }
  
  return nullptr;
}

int main() {
    LLVMCodeGen codeGenObj;
}