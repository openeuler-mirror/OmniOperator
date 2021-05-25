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
#include <llvm/IR/IRBuilder.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/ADT/APFloat.h>
#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <iostream>

using namespace std;
using namespace llvm;

static LLVMContext context;
static IRBuilder<> builder(context);

static StringRef cpu_name;
static SmallVector<std::string, 10> cpu_attrs;

JITSymbol dummy_lookup(const string& name)
{
	return JITSymbol(NULL);
}

LLVMCodeGen::LLVMCodeGen() 
{
    _module = new Module("Omniruntime Module", context);
}


// Logic to generate the function.
// TODO: Currently only supports comparision operator
void LLVMCodeGen::generateFunc(std::string name, ComparisionExpr expr) 
{
    _func_name = name;
	// (double, double, double)
	std::vector<Type*> param_type(2, Type::getDoubleTy(context));
	// double (*)(double, double, double)
	FunctionType* prototype = FunctionType::get(Type::getInt32Ty(context), param_type, false);
    cout <<"MOdule::" << _module <<endl;

	Function *func = Function::Create(prototype, Function::ExternalLinkage, name, _module);
	BasicBlock *body = BasicBlock::Create(context, "body", func);
	builder.SetInsertPoint(body);

	std::vector<Value*> args;
	for(auto& arg : func->args())
    {
		args.push_back(&arg);
    }

    Value* temp = generateComparisionBody(&expr, args[0], args[1]);

	builder.CreateRet(temp);
}

void LLVMCodeGen::generateFunc(std::string name, BinaryExpr b_expr) 
{
    _func_name = name;
	std::vector<Type*> param_type(4, Type::getDoubleTy(context));
	// double (*)(double, double, double)
	FunctionType* prototype = FunctionType::get(Type::getInt32Ty(context), param_type, false);
    cout <<"MOdule::" << _module <<endl;

	Function *func = Function::Create(prototype, Function::ExternalLinkage, name, _module);
	BasicBlock *body = BasicBlock::Create(context, "body", func);
	builder.SetInsertPoint(body);

	std::vector<Value*> args;
	for(auto& arg : func->args())
    {
		args.push_back(&arg);
    }
    ComparisionExpr *left_expr =  (ComparisionExpr *) &b_expr.left;
    ComparisionExpr *right_expr = (ComparisionExpr *) &b_expr.right; 

    Value* left  = generateComparisionBody(left_expr, args[0], args[1]);
    Value* right  = generateComparisionBody(right_expr, args[2], args[3]);
    Value* result;
    switch (b_expr.op)
    {
    case AND:
        result = builder.CreateAnd(left, right, "and");
        break;
    case OR:
        result = builder.CreateOr(left, right, "or");
        break;
    }
	builder.CreateRet(result);
}

Value* LLVMCodeGen::generateComparisionBody(ComparisionExpr* c_expr, Value* left, Value* right)
{
    cout << "Generating comparision::" << left <<":" << right<<endl;
    Value* temp;
    switch(c_expr->op) {
        case LT:
            temp = builder.CreateFCmpULT(left, right, "cmplt");
            break;
        case GT:
            temp = builder.CreateFCmpUGT(left, right, "cmpgt");
            break;
        case LTE:
            temp = builder.CreateFCmpULE(left, right, "cmplte");
            break;
        case GTE:
            temp = builder.CreateFCmpUGE(left, right, "cmpgte");
            break;   
        case EQ:
            temp = builder.CreateFCmpUEQ(left, right, "cmpeq");
            break;         
    }
    cout << "Generated expression::" << temp <<endl;
    return temp;
}

void LLVMCodeGen::compile() {
        cout<<"Executing the code"<<endl;
    // Initialization
	LLVMInitializeNativeTarget();
	InitializeNativeTargetAsmPrinter();
    InitializeNativeTargetAsmParser();
    InitializeNativeTargetDisassembler();
    std::string builder_error;

    

    std::unique_ptr<Module> MODULE;
    MODULE.reset(_module);
    auto opt_level = llvm::CodeGenOpt::None;
    EngineBuilder engine_builder(std::move(MODULE));
    TargetMachine* targetMarchine = engine_builder.selectTarget();
    engine_builder.setEngineKind(EngineKind::JIT)
      .setOptLevel(opt_level)
      .setErrorStr(&builder_error);

      
    _ee.reset(engine_builder.create(targetMarchine));
    cout<<"Build error:::" << builder_error << endl;
    if (_ee == nullptr) {
        cout <<"Execution engine is null" << endl;
    }
    cout<<"Adding module..." << endl;
   // exec_engine->addModule(std::move(MODULE));
    cout<<"Finalize module ..." << endl;
    _ee->finalizeObject();
	
}

bool LLVMCodeGen::execute(int32_t left, int32_t right) {

    cout<<"Get the function ..." << endl;
    int32_t (*native_func)(double, double) = (decltype(native_func)) _ee->getFunctionAddress(_func_name);
    printf("%d\n", native_func(left,right));
 
    return true;
}

bool LLVMCodeGen::execute(int32_t arg0, int32_t arg1, int32_t arg2, int32_t arg3) {
    return true;

}


int main()
{
	LLVMCodeGen codeGenObj;
    ComparisionExpr left_expr;
	left_expr.columnData = 2;
	left_expr.columnIdx = 0;
	left_expr.op = ComparisionOperator::LT;
    codeGenObj.generateFunc("test_func", left_expr);
    codeGenObj.compile();
    for (int i = 0; i < 10; i++) {
         codeGenObj.execute(4,i);
    }
     
	return 0;
}