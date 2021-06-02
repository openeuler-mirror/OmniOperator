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

JITSymbol dummy_lookup(const string &name)
{
    return JITSymbol(NULL);
}

LLVMCodeGen::LLVMCodeGen()
{
    _module = new Module("Omniruntime Module", context);
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
    TargetMachine *targetMarchine = engine_builder.selectTarget();
    engine_builder.setEngineKind(EngineKind::JIT)
        .setOptLevel(opt_level)
        .setErrorStr(&builder_error);

    _ee.reset(engine_builder.create(targetMarchine));
    cout << "Build error:::" << builder_error << endl;
    if (_ee == nullptr)
    {
        cout << "Execution engine is null" << endl;
    }
}

// Logic to generate the function.
// TODO: Currently only supports comparision operator
void LLVMCodeGen::generateFunc(std::string name, Expr *expr)
{
    _func_name = name;
    switch (expr->getType())
    {
    case COMPARISION_E:
    {
        ComparisionExpr *c_expr = (ComparisionExpr *)expr;
        generateComparisionExprFunc(c_expr);
        break;
    }  
    case BINARY_E:
    {
        BinaryExpr *b_expr = (BinaryExpr *)expr;
        generateBinaryExprFunc(b_expr);
        break;
    }
    case BETWEEN_E:
        //TODO: Handle Between operator
        break;
    case IN_E:
        // TODO: Handle IN operator
        break;
    default:
        break;
    }
}

void LLVMCodeGen::generateComparisionExprFunc(ComparisionExpr *c_expr)
{
    FunctionType *prototype;
    switch (c_expr->columnData.dataType)
    {

    case INT32D:
    {
        // (int, int)
        std::vector<Type *> int32_param_type(2, Type::getInt32Ty(context));
        // int (*)(int, int)
        prototype = FunctionType::get(Type::getInt32Ty(context), int32_param_type, false);
        break;
    }
    case INT64D:
    {
        // (int, int)
        std::vector<Type *> int64_param_type(2, Type::getInt64Ty(context));
        // int (*)(int, int)
        prototype = FunctionType::get(Type::getInt32Ty(context), int64_param_type, false);
        break;
    }     
    case DOUBLED:
    {
        // (double, double)
        std::vector<Type *> double_param_type(2, Type::getDoubleTy(context));
        // int (*)(double, double)
        prototype = FunctionType::get(Type::getInt32Ty(context), double_param_type, false);
        break;
    }
    case STRINGD:
    {
        // TODO:: Handle string type
        break;
    }      
    }

    Function *func = Function::Create(prototype, Function::ExternalLinkage, _func_name, _module);
    BasicBlock *body = BasicBlock::Create(context, "body", func);
    builder.SetInsertPoint(body);

    std::vector<Value *> args;
    for (auto &arg : func->args())
    {
        args.push_back(&arg);
    }

    llvm::Value *result = builder.CreateSub(args[0], args[1], "result");
    builder.CreateRet(result);
}

void LLVMCodeGen::generateBinaryExprFunc(BinaryExpr *b_expr)
{
/*     ComparisionExpr *left_expr = (ComparisionExpr *)b_expr->left;
    ComparisionExpr *right_expr = (ComparisionExpr *)b_expr->left;
    FunctionType *prototype;
    if (left_expr->columnData.dataType == right_expr->columnData.dataType)
    {
        switch (left_expr->columnData.dataType)
        {

        case INT32:
            // (int, int)
            std::vector<Type *> param_type(4, Type::getInt32Ty(context));
            // int (*)(int, int)
            prototype = FunctionType::get(Type::getInt32Ty(context), param_type, false);
            break;
        case INT64:
            // (int, int)
            std::vector<Type *> param_type(4, Type::getInt64Ty(context));
            // int (*)(int, int)
            prototype = FunctionType::get(Type::getInt32Ty(context), param_type, false);
            break;
        case DOUBLE:
            // (double, double)
            std::vector<Type *> param_type(4, Type::getDoubleTy(context));
            // int (*)(double, double)
            prototype = FunctionType::get(Type::getInt32Ty(context), param_type, false);
            break;
        case STRING:
            // TODO:: Handle string type
            break;
        }
    }
    else
    {
        std::vector<Type *> lparam_type;
        std::vector<Type *> rparam_type;
        std::vector<Type *> param_type;
        switch (left_expr->columnData.dataType)
        {

        case INT32:
            lparam_type.push_back(Type::getInt32Ty(context));
            lparam_type.push_back(Type::getInt32Ty(context));
            break;
        case INT64:
            lparam_type.push_back(Type::getInt64Ty(context));
            lparam_type.push_back(Type::getInt64Ty(context));
            break;
        case DOUBLE:
            lparam_type.push_back(Type::getDoubleTy(context));
            lparam_type.push_back(Type::getDoubleTy(context));
            break;
        case STRING:
            // TODO:: Handle string type
            break;
        }

        switch (right_expr->columnData.dataType)
        {

        case INT32:
            rparam_type.push_back(Type::getInt32Ty(context));
            rparam_type.push_back(Type::getInt32Ty(context));
            break;
        case INT64:
            rparam_type.push_back(Type::getInt64Ty(context));
            rparam_type.push_back(Type::getInt64Ty(context));
            break;
        case DOUBLE:
            rparam_type.push_back(Type::getDoubleTy(context));
            rparam_type.push_back(Type::getDoubleTy(context));
            break;
        case STRING:
            // TODO:: Handle string type
            break;
        }
        param_type.push_back(lparam_type.pop_back());
        param_type.push_back(lparam_type.pop_back());
        param_type.push_back(rparam_type.pop_back());
        param_type.push_back(rparam_type.pop_back());
        
        prototype = FunctionType::get(Type::getInt32Ty(context), param_type, false);

    }

    Function *func = Function::Create(prototype, Function::ExternalLinkage, _func_name, _module);
    BasicBlock *body = BasicBlock::Create(context, "body", func);
    builder.SetInsertPoint(body);

    std::vector<Value *> args;
    for (auto &arg : func->args())
    {
        args.push_back(&arg);
    }

    llvm::Value *result = builder.CreateSub(args[0], args[1], "result");
    builder.CreateRet(result); */
}

void LLVMCodeGen::generateInExprFunc(InExpr *in_expr)
{
}

void LLVMCodeGen::generateBetweenExprFunc(BetweenExpr *bt_expr)
{
}


    /* Value* LLVMCodeGen::generateComparisionBody(ComparisionExpr* c_expr, Value* left, Value* right)
{
    cout << "Generating comparision::" << left <<":" << right<<endl;
    Value* temp;
    switch(c_expr->op) {
        case LT:
            temp = builder.CreateICmpULT(left, right, "cmplt");
            break;
        case GT:
            temp = builder.CreateICmpUGT(left, right, "cmpgt");
            break;
        case LTE:
            temp = builder.CreateICmpULE(left, right, "cmplte");
            break;
        case GTE:
            temp = builder.CreateICmpUGE(left, right, "cmpgte");
            break;   
        case EQ:
            temp = builder.CreateICmpEQ(left, right, "cmpeq");
            break;         
    }
    cout << "Generated expression::" << temp <<endl;
    return temp;
} */

    void LLVMCodeGen::compile()
    {
        cout << "Compiling..." << endl;
        // exec_engine->addModule(std::move(MODULE));
        cout << "Finalize module ..." << endl;
        _ee->finalizeObject();
    }

bool LLVMCodeGen::executeComparisionExprFunc(ComparisionExpr* c_expr, Data* data)
{
    int32_t result;
     switch (c_expr->columnData.dataType)
    {

    case INT32D:
    {
        int32_t (*native_func)(int32_t, int32_t) = (int32_t(*)(int32_t, int32_t))_ee->getFunctionAddress(_func_name);
        result = native_func(c_expr->columnData.intVal, data->intVal);
        break;
    }
       
    case INT64D:
    {
        int32_t (*native_func)(int64_t, int64_t) = (int32_t(*)(int64_t, int64_t))_ee->getFunctionAddress(_func_name);
        result = native_func(c_expr->columnData.longVal, data->longVal);
        break;
    }
        
    case DOUBLED:
    {
        int32_t (*native_func)(double, double) = (int32_t(*)(double, double))_ee->getFunctionAddress(_func_name);
        result = native_func(c_expr->columnData.doubleVal, data->doubleVal);
        break;
    }
        
    case STRINGD:
        // TODO:: Handle string type
        break;
    }
    
    switch (c_expr->op)
    {
    case LT:
        if (result < 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    case GT:
        if (result > 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    case LTE:
        if (result <= 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    case GTE:
        if (result >= 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    case EQ:
        if (result == 0)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    return false;
}

bool LLVMCodeGen::executeBinaryExprFunc(BinaryExpr* expr, Data** dataArr)
{
    return true;
}

bool LLVMCodeGen::executeInExprFunc(InExpr* expr, Data* data)
{
    return true;
}

bool LLVMCodeGen::executeBetweenExprFunc(BetweenExpr* expr, Data* data)
{
    return true;
}
/*
int main()
{
    LLVMCodeGen codeGenObj;
    Data data;
    data.dataType = DataType::INT32D;
    data.intVal = 2;
    int32_t colIdx = 0;
    ComparisionExpr left_expr (ComparisionOperator::LT, colIdx, data);
    codeGenObj.generateFunc("test_func", &left_expr);
    codeGenObj.compile();
    Data actual;
    actual.dataType = DataType::INT32D;
    actual.intVal = 4;
    cout << "Result:::" << codeGenObj.executeComparisionExprFunc(&left_expr, &actual);

    return 0;
}
*/