/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:filter code generation methods
 */
#include "filter_codegen.h"

#include <utility>

using namespace llvm;
using namespace orc;
using namespace omniruntime::expressions;

namespace {
const int INPUT_INDEX = 0;
const int ARGUMENT_ONE = 1;
const int ARGUMENT_TWO = 2;
const int ARGUMENT_THREE = 3;
const int OFFSETS_INDEX = 4;
const int EXECUTION_CONTEXT_IDX = 5;
const int DICTIONARY_VECTORS_IDX = 6;
}

std::unique_ptr<FilterCodeGen> FilterCodeGen::Create(std::string name, const omniruntime::expressions::Expr &expression,
    omniruntime::op::OverflowConfig *overflowConfig)
{
    std::unique_ptr<FilterCodeGen> codegen { new FilterCodeGen(std::move(name), expression, overflowConfig) };
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

int64_t FilterCodeGen::GetFunction()
{
    Function *func = this->CreateFunction();
    if (func == nullptr) {
        return 0;
    }
    return this->CreateWrapper(*func);
}

int64_t FilterCodeGen::CreateWrapper(llvm::Function &filterFn)
{
    llvm::Function *filterFunc = &filterFn;

    std::vector<Type *> args;
    Type *ptrArg = llvmTypes->I64PtrType(); // table
    args.push_back(ptrArg);
    args.push_back(llvmTypes->I32Type());    // no of rows
    args.push_back(llvmTypes->I32PtrType()); // output array
    // bitmap is a 2d array of booleans
    Type *bitmapArg = llvmTypes->I64PtrType(); // record nullk values
    args.push_back(bitmapArg);
    args.push_back(llvmTypes->I64PtrType()); // offsets
    args.push_back(llvmTypes->I64Type());    // execution_context address
    args.push_back(llvmTypes->I64PtrType()); // dictionary vectors

    FunctionType *funcSignature = FunctionType::get(llvmTypes->I32Type(), args, false);
    llvm::Function *funcDecl =
        llvm::Function::Create(funcSignature, llvm::Function::ExternalLinkage, "FILTER_WRAPPER", module);
    BasicBlock *preLoop = BasicBlock::Create(*context, "PRE_LOOP", funcDecl);
    BasicBlock *loopBody = BasicBlock::Create(*context, "LOOP_BODY", funcDecl);
    BasicBlock *filterPassed = BasicBlock::Create(*context, "FILTER_PASSED", funcDecl);
    BasicBlock *incrementCounter = BasicBlock::Create(*context, "INCREMENT_COUNTER", funcDecl);
    BasicBlock *endBlock = BasicBlock::Create(*context, "END_BLOCK", funcDecl);
    // preprocessing
    Argument *data = funcDecl->getArg(INPUT_INDEX);
    data->setName("ARGS_ARRAY");
    Argument *numRows = funcDecl->getArg(ARGUMENT_ONE);
    numRows->setName("NUM_ROWS");
    Argument *resultsArray = funcDecl->getArg(ARGUMENT_TWO);
    resultsArray->setName("RESULTS");
    Argument *bitmap = funcDecl->getArg(ARGUMENT_THREE);
    bitmap->setName("BITMAP");
    Argument *offsets = funcDecl->getArg(OFFSETS_INDEX);
    offsets->setName("OFFSETS");
    Argument *executionContext = funcDecl->getArg(EXECUTION_CONTEXT_IDX);
    offsets->setName("EXECUTION_CONTEXT_ADDRESS");
    Argument *dictionaryVectors = funcDecl->getArg(DICTIONARY_VECTORS_IDX);
    offsets->setName("DICTIONARY_VECTORS");

    llvmEngine->RecordMainFunction(funcDecl);

    Value *zero = llvmTypes->CreateConstantInt(0);
    Value *one = llvmTypes->CreateConstantInt(1);
    std::vector<Value *> filterFuncArgs;
    // filterFuncArgs contains the values of the arguments to the filter function
    // value*, bitmap*, offset*, rowIdx, length*, execution_context_ptr, dictionary_vectors*, isNull
    int32_t argsSize = 8;
    filterFuncArgs.reserve(argsSize);

    CallInst *ret;
    // pre loop body
    builder->SetInsertPoint(preLoop);
    // Pointer to the current row index to be processed.
    AllocaInst *indexStore = builder->CreateAlloca(llvmTypes->I32Type(), nullptr, "INDEX_COUNTER");
    // Initialize row index to 0.
    builder->CreateStore(zero, indexStore);
    // Value of the current row index to be processed.
    Value *curIndexVal;
    // Temp value for next row index.
    Value *nextIndexVal;
    // Pointer to the index of the selected positions array to be filled next.
    AllocaInst *selectedIndexStore = builder->CreateAlloca(llvmTypes->I32Type(), nullptr, "SELECTED_INDEX_PTR");
    // Initialize index to 0.
    builder->CreateStore(zero, selectedIndexStore);
    // Value of the selected positions index.
    Value *selectedIndexVal;
    // Address of the selected index for writing.
    Value *selectedAddress;
    // Temp value for next selected index.
    Value *nextSelectedIndexVal;

    // Create a int pointer to store data length
    AllocaInst *lengthAllocaInst = builder->CreateAlloca(llvmTypes->I32Type(), nullptr, "DATA_LENGTH");
    auto isNullPtr = builder->CreateAlloca(llvmTypes->I1Type(), nullptr, "IS_NULL_PTR");

    builder->CreateBr(loopBody);
    // loop body
    builder->SetInsertPoint(loopBody);
    // Get the value of the current row index to process.
    curIndexVal = builder->CreateLoad(indexStore, "CUR_INDEX");

    builder->CreateStore(llvmTypes->CreateConstantInt(0), lengthAllocaInst);
    builder->CreateStore(llvmTypes->CreateConstantBool(false), isNullPtr);

    filterFuncArgs.push_back(data);
    filterFuncArgs.push_back(bitmap);
    filterFuncArgs.push_back(offsets);
    filterFuncArgs.push_back(curIndexVal);
    filterFuncArgs.push_back(lengthAllocaInst);
    filterFuncArgs.push_back(executionContext);
    filterFuncArgs.push_back(dictionaryVectors);
    filterFuncArgs.push_back(isNullPtr);

    // Get the boolean response for this row from the filter function.
    ret = builder->CreateCall(filterFunc, filterFuncArgs, "ROW_EVAL");
    ret = static_cast<CallInst *>(builder->CreateAnd(builder->CreateNot(builder->CreateLoad(isNullPtr)), ret));
    // If true, add row index to selected array, otherwise, process next row.
    builder->CreateCondBr(ret, filterPassed, incrementCounter);
    // Add row index to results array
    builder->SetInsertPoint(filterPassed);
    // Get value of selected index.
    selectedIndexVal = builder->CreateLoad(selectedIndexStore, "SELECTED_INDEX");
    // Get address of selected index.
    selectedAddress = builder->CreateGEP(resultsArray, selectedIndexVal, "SELECTED_ADDRESS");
    // Set the selected value to the current row index.
    builder->CreateStore(curIndexVal, selectedAddress);
    // Increment the selected index.
    nextSelectedIndexVal = builder->CreateAdd(selectedIndexVal, one, "NEXT_SELECTED_INDEX");
    builder->CreateStore(nextSelectedIndexVal, selectedIndexStore);
    // Increment counter and process next row.
    builder->CreateBr(incrementCounter);
    // Increment loop counter
    builder->SetInsertPoint(incrementCounter);
    // Increment counter.
    nextIndexVal = builder->CreateAdd(curIndexVal, one, "NEXT_INDEX");
    builder->CreateStore(nextIndexVal, indexStore);
    // If there are rows remaining, repeat, otherwise, exit.
    Value *cond = builder->CreateICmpSLT(nextIndexVal, numRows, "END_LOOP_COND");
    builder->CreateCondBr(cond, loopBody, endBlock);

    builder->SetInsertPoint(endBlock);

    nextSelectedIndexVal = builder->CreateLoad(selectedIndexStore);
    builder->CreateRet(nextSelectedIndexVal);
    llvmEngine->OptimizeFunctionsAndModule();

    jit->getMainJITDylib().addGenerator(
        eoe(DynamicLibrarySearchGenerator::GetForCurrentProcess(jit->getDataLayout().getGlobalPrefix())));
    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    llvmEngine->MakeThreadSafe(&resTracker);
    rt = resTracker;

    auto sym = eoe(jit->lookup("FILTER_WRAPPER"));
    return sym.getAddress();
}
