/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch filter expression codegen
 */

#include "batch_filter_codegen.h"

#include <utility>

using namespace llvm;
using namespace orc;
using namespace omniruntime::expressions;
using namespace omniruntime;

namespace {
const int INPUT_INDEX = 0;
const int ARGUMENT_ONE = 1;
const int ARGUMENT_TWO = 2;
const int ARGUMENT_THREE = 3;
const int OFFSETS_INDEX = 4;
const int EXECUTION_CONTEXT_IDX = 5;
const int DICTIONARY_VECTORS_IDX = 6;
}

std::unique_ptr<BatchFilterCodeGen> BatchFilterCodeGen::Create(std::string name,
    const omniruntime::expressions::Expr &expression, omniruntime::op::OverflowConfig *overflowConfig)
{
    std::unique_ptr<BatchFilterCodeGen> codegen { new BatchFilterCodeGen(std::move(name), expression, overflowConfig) };
    LLVMEngine::Create(&(codegen->llvmEngine));
    codegen->context = codegen->GetContext();
    codegen->builder = codegen->GetIRBuilder();
    codegen->module = codegen->GetModule();
    codegen->jit = codegen->GetJit();
    codegen->llvmTypes = codegen->GetTypes();
    codegen->decimalIRBuilder = codegen->GetDecimalIRBuilder();
    codegen->ExtractVectorIndexes();
    return codegen;
}

int64_t BatchFilterCodeGen::GetFunction()
{
    llvm::Function *func = this->CreateBatchFunction();
    if (func == nullptr) {
        return 0;
    }
    return this->CreateWrapper(*func);
}

int64_t BatchFilterCodeGen::CreateWrapper(llvm::Function &filterFn)
{
    llvm::Function *filterFunc = &filterFn;

    std::vector<Type *> args;
    args.push_back(llvmTypes->I64PtrType()); // vecBatch
    args.push_back(llvmTypes->I32Type());    // rowCnt
    args.push_back(llvmTypes->I32PtrType()); // selectedRows
    args.push_back(llvmTypes->I64PtrType()); // inputBitmap
    args.push_back(llvmTypes->I64PtrType()); // inputOffsets
    args.push_back(llvmTypes->I64Type());    // execution_context
    args.push_back(llvmTypes->I64PtrType()); // dictionary vectors

    FunctionType *funcSignature = FunctionType::get(llvmTypes->I32Type(), args, false);
    llvm::Function *funcDecl =
        llvm::Function::Create(funcSignature, llvm::Function::ExternalLinkage, "BATCH_FILTER_WRAPPER", module);
    BasicBlock *filterMain = BasicBlock::Create(*context, "FILTER_MAIN", funcDecl);

    // set arg names
    Argument *data = funcDecl->getArg(INPUT_INDEX);
    data->setName("ARGS_ARRAY");
    Argument *numRows = funcDecl->getArg(ARGUMENT_ONE);
    numRows->setName("NUM_ROWS");
    Argument *selectedRows = funcDecl->getArg(ARGUMENT_TWO);
    selectedRows->setName("RESULTS");
    Argument *bitmap = funcDecl->getArg(ARGUMENT_THREE);
    bitmap->setName("BITMAP");
    Argument *offsets = funcDecl->getArg(OFFSETS_INDEX);
    offsets->setName("OFFSETS");
    Argument *executionContext = funcDecl->getArg(EXECUTION_CONTEXT_IDX);
    executionContext->setName("EXECUTION_CONTEXT_ADDRESS");
    Argument *dictionaryVectors = funcDecl->getArg(DICTIONARY_VECTORS_IDX);
    dictionaryVectors->setName("DICTIONARY_VECTORS");

    builder->SetInsertPoint(filterMain);
    AllocaInst *lengthAllocaInst = builder->CreateAlloca(llvmTypes->I32Type(), numRows, "LENGTH_PTR");
    AllocaInst *isNullPtr = builder->CreateAlloca(llvmTypes->I1Type(), numRows, "IS_NULL_PTR");
    AllocaInst *rowIdxArray = builder->CreateAlloca(llvmTypes->I32Type(), numRows, "ROW_INDEX_ARRAY");
    std::vector<Value *> funcArgs { rowIdxArray, numRows };
    llvmEngine->CallExternFunction("fill_rowIdx", { OMNI_INT, OMNI_INT }, OMNI_INT, funcArgs, nullptr, "fill_rowIdx");
    // in the form of {0, 1, 1, ...}. 1 indicates passing the filter, 0 otherwise.
    auto filterResArray = builder->CreateAlloca(llvmTypes->I1Type(), numRows, "FILTER_RES_PTR");

    std::vector<Value *> filterFuncArgs { data,        bitmap,           offsets,          numRows,
        rowIdxArray, lengthAllocaInst, executionContext, dictionaryVectors,
        isNullPtr,   filterResArray };
    builder->CreateCall(filterFunc, filterFuncArgs, "INNER_FUNC");

    std::vector<DataTypeId> paramTypes = { OMNI_BOOLEAN, OMNI_BOOLEAN, OMNI_INT, OMNI_INT };
    funcArgs = { filterResArray, isNullPtr, selectedRows, numRows };
    auto res =
        llvmEngine->CallExternFunction("batch_and_not", paramTypes, OMNI_INT, funcArgs, nullptr, "fill_filter_result");
    builder->CreateRet(res);

    llvmEngine->OptimizeFunctionsAndModule();
    jit->getMainJITDylib().addGenerator(
        eoe(DynamicLibrarySearchGenerator::GetForCurrentProcess(jit->getDataLayout().getGlobalPrefix())));
    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    llvmEngine->MakeThreadSafe(&resTracker);
    rt = resTracker;

    auto sym = eoe(jit->lookup("BATCH_FILTER_WRAPPER"));
    return sym.getAddress();
}