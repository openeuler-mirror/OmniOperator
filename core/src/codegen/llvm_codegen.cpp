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
#include <llvm/ExecutionEngine/Orc/LambdaResolver.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>
#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <iostream>
#include <typeinfo>

using namespace std;
using namespace llvm;

static LLVMContext context;
static IRBuilder<> builder(context);

using LinkLayer = orc::RTDyldObjectLinkingLayer;
using Compiler = orc::SimpleCompiler;
using CompileLayer = orc::IRCompileLayer<LinkLayer, Compiler>;

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
    cout << "expression typeid::" << typeid(expr).name() << endl;
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

    Value* temp;
    switch(expr.op) {
        case LT:
            temp = builder.CreateICmpSLT(args[0], args[1], "cmplt");
            break;
        case GT:
            temp = builder.CreateICmpSGT(args[0], args[1], "cmpgt");
            break;
        case LTE:
            temp = builder.CreateICmpSLE(args[0], args[1], "cmplte");
            break;
        case GTE:
            temp = builder.CreateICmpSGE(args[0], args[1], "cmpgte");
            break;   
        case EQ:
            temp = builder.CreateICmpEQ(args[0], args[1], "cmpeq");
            break;         
    }

	builder.CreateRet(temp);
}

void LLVMCodeGen::generateFunc(std::string name, BinaryExpr expr) 
{
    cout << "expression typeid::" << typeid(expr).name() << endl;
    _func_name = name;
	// TODO: Handle binary operation
}

void LLVMCodeGen::compile() {
    //TODO: Move the compilation code here
	
}

bool LLVMCodeGen::execute(int32_t left, int32_t right) {
    cout<<"Executing the code"<<endl;
    // Initialization
	LLVMInitializeNativeTarget();
	InitializeNativeTargetAsmPrinter();
	TargetMachine* target = EngineBuilder().selectTarget();

	// Emit the LLVM IR to the Module
	//code_gen();

	// Compile the IR to Machine Code 
	const DataLayout dl = target->createDataLayout();
	LinkLayer link_layer([]() { return std::make_shared<SectionMemoryManager>(); });
	CompileLayer compile_layer(link_layer, Compiler(*target));
    cout <<"MOdule::" << _module <<endl;
	auto jit_module_handle = cantFail(compile_layer.addModule(std::shared_ptr<Module>(_module), 
			                  orc::createLambdaResolver(dummy_lookup, dummy_lookup)));
                              // Run the compiled function !
    JITSymbol symbol = compile_layer.findSymbolIn(jit_module_handle, _func_name, false);
	int32_t (*native_func)(double, double) = (decltype(native_func))cantFail(symbol.getAddress());
	printf("%d\n", native_func(left,right));
    return true;
}


int main()
{
	LLVMCodeGen codeGenObj;
    ComparisionExpr exprObj;
	exprObj.columnData = 2;
	exprObj.columnIdx = 0;
	exprObj.op = ComparisionOperator::EQ;
    codeGenObj.generateFunc("test_func", exprObj);
    codeGenObj.execute(2,3);

   
	return 0;
}