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

    Value* temp = generateComparisionBody(&expr, args[0], args[1]);

	builder.CreateRet(temp);
}

void LLVMCodeGen::generateFunc(std::string name, BinaryExpr b_expr) 
{
    cout << "expression typeid::" << typeid(b_expr).name() << endl;
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
    cout << "Operator::" << typeid(b_expr.op).name() << endl;
    ComparisionExpr *left_expr =  (ComparisionExpr *) &b_expr.left;
    ComparisionExpr *right_expr = (ComparisionExpr *) &b_expr.right; 

    Value* left  = generateComparisionBody(left_expr, args[0], args[1]);
    Value* right  = generateComparisionBody(right_expr, args[2], args[3]);
    Value* result;
    switch (b_expr.op)
    {
    case AND:
        cout<<" Before AND operations"<< "left: "<< left<< " right: " << right<< endl;
        result = builder.CreateAnd(left, right, "and");
        cout<<" After AND operations"<< endl;
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
            temp = builder.CreateICmpSLT(left, right, "cmplt");
            break;
        case GT:
            temp = builder.CreateICmpSGT(left, right, "cmpgt");
            break;
        case LTE:
            temp = builder.CreateICmpSLE(left, right, "cmplte");
            break;
        case GTE:
            temp = builder.CreateICmpSGE(left, right, "cmpgte");
            break;   
        case EQ:
            temp = builder.CreateICmpEQ(left, right, "cmpeq");
            break;         
    }
    cout << "Generated expression::" << temp <<endl;
    return temp;
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

bool LLVMCodeGen::execute(int32_t arg0, int32_t arg1, int32_t arg2, int32_t arg3) {
    cout<<"Executing the code binary"<<endl;
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
	int32_t (*native_func)(double, double, double, double) = (decltype(native_func))cantFail(symbol.getAddress());
	printf("%d\n", native_func(arg0, arg1, arg2, arg3));
    return true;
}


int main()
{
	LLVMCodeGen codeGenObj;
    ComparisionExpr left_expr;
	left_expr.columnData = 2;
	left_expr.columnIdx = 0;
	left_expr.op = ComparisionOperator::GT;
    ComparisionExpr right_expr;
	right_expr.columnData = 2;
	right_expr.columnIdx = 0;
	right_expr.op = ComparisionOperator::LT;
    BinaryExpr b_expr;
    b_expr.left = left_expr;
    b_expr.right = right_expr;
    b_expr.op = LogicalOperator::AND;
    codeGenObj.generateFunc("test_func", b_expr);
    codeGenObj.execute(4,3,4,5);

   
	return 0;
}