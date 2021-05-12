#include "llvm_compiler.h"
#include <llvm/IR/IRBuilder.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/ExecutionEngine/SectionMemoryManager.h>
#include <llvm/ExecutionEngine/Orc/CompileUtils.h>
#include <llvm/ExecutionEngine/Orc/IRCompileLayer.h>
#include <llvm/ExecutionEngine/Orc/LambdaResolver.h>
#include <llvm/ExecutionEngine/Orc/RTDyldObjectLinkingLayer.h>

using namespace llvm;
using namespace std;

using LinkLayer = orc::RTDyldObjectLinkingLayer;
using Compiler = orc::SimpleCompiler;
using CompileLayer = orc::IRCompileLayer<LinkLayer, Compiler>;
JITSymbol dummy_lookup(const std::string& name)
{
	    return JITSymbol(NULL);
}

LLVMCompiler::LLVMCompiler(std::string func_name) 
{
	LLVMInitializeNativeTarget();
	InitializeNativeTargetAsmPrinter();
	_targetMachine = EngineBuilder().selectTarget();
	_func_name = func_name;

}

void LLVMCompiler::compile() 
{
	Module* jit_module = NULL;
	// Compile the IR to Machine Code 
	const DataLayout dl = _targetMachine->createDataLayout();
	LinkLayer link_layer([]() { return std::make_shared<SectionMemoryManager>(); });
	CompileLayer compile_layer(link_layer, Compiler(*_targetMachine));
    JITSymbol emptySymbol = JITSymbol(NULL);
	auto jit_module_handle = cantFail(compile_layer.addModule(std::shared_ptr<Module>(jit_module), 
			                  orc::createLambdaResolver(dummy_lookup, dummy_lookup)));
	JITSymbol symbol = compile_layer.findSymbolIn(jit_module_handle, _func_name, false);
    bool (*native_func) (int, int) = (decltype(native_func))cantFail(symbol.getAddress());
}

void LLVMCompiler::execute() 
{
	//TODO: execute the compiled function
}

int main() {

    
}