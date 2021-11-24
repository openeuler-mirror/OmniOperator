/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description:filter code generation methods
 */
#include "filter_codegen.h"

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

    const int ROW_FILTER_INPUT_INDEX = 0;
    const int ROW_FILTER_OFFSETS_INDEX = 2;
    const int ROW_FILTER_ROW_IDX_INDEX = 3;
    const int ROW_FILTER_EXECUTION_CONTEXT_INDEX = 4;
    const int ROW_FILTER_DICT_VECTORS_INDEX = 5;
}

int64_t FilterCodeGen::GetFunction()
{
    Function *func = this->CreateFunction();
    return this->CreateWrapper(*func);
}

int64_t FilterCodeGen::CreateWrapper(Function &filterFn)
{
    Function *filterFunc = &filterFn;

    std::vector<Type *> args;
    Type *ptrArg = Type::getInt64PtrTy(*context); // table
    args.push_back(ptrArg);
    args.push_back(Type::getInt32Ty(*context)); // no of rows
    args.push_back(Type::getInt32PtrTy(*context)); // output array
    // bitmap is a 2d array of booleans
    Type *bitmapArg = Type::getInt64PtrTy(*context); // record nullk values
    args.push_back(bitmapArg);
    args.push_back(Type::getInt64PtrTy(*context)); // offsets
    args.push_back(Type::getInt64Ty(*context)); // execution_context address
    args.push_back(Type::getInt64PtrTy(*context)); // dictionary vectors

    FunctionType *funcSignature = FunctionType::get(Type::getInt32Ty(*context), args, false);
    Function *funcDecl = Function::Create(funcSignature, Function::ExternalLinkage, "FILTER_WRAPPER", module.get());
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

    Value *zero = this->CreateConstantInt(0);
    Value *one = this->CreateConstantInt(1);
    std::vector<Value*> filterFuncArgs;
    // filterFuncArgs contains the values of the arguments to the filter function
    // value*, bitmap*, offset*, rowIdx, isResultNull*, length*, execution_context_ptr, dictionary_vectors*
    int32_t argsSize = 8;
    filterFuncArgs.reserve(argsSize);

    CallInst *ret;
    // pre loop body
    builder->SetInsertPoint(preLoop);
    // Pointer to the current row index to be processed.
    AllocaInst *indexStore = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "INDEX_COUNTER");
    // Initialize row index to 0.
    builder->CreateStore(zero, indexStore);
    // Value of the current row index to be processed.
    Value *curIndexVal;
    // Temp value for next row index.
    Value *nextIndexVal;
    // Pointer to the index of the selected positions array to be filled next.
    AllocaInst *selectedIndexStore = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "SELECTED_INDEX_PTR");
    // Initialize index to 0.
    builder->CreateStore(zero, selectedIndexStore);
    // Value of the selected positions index.
    Value *selectedIndexVal;
    // Address of the selected index for writing.
    Value *selectedAddress;
    // Temp value for next selected index.
    Value *nextSelectedIndexVal;

    // Create a boolean pointer to store result null value
    AllocaInst *isResultNullStore = builder->CreateAlloca(Type::getInt1Ty(*context), nullptr, "isResultNull");
    // Create a int pointer to store data length
    AllocaInst *lengthAllocaInst = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "DATA_LENGTH");

    builder->CreateBr(loopBody);
    // loop body
    builder->SetInsertPoint(loopBody);
    // Get the value of the current row index to process.
    curIndexVal = builder->CreateLoad(indexStore, "CUR_INDEX");

    builder->CreateStore(CreateConstantBool(false), isResultNullStore);
    builder->CreateStore(CreateConstantInt(0), lengthAllocaInst);

    filterFuncArgs.push_back(data);
    filterFuncArgs.push_back(bitmap);
    filterFuncArgs.push_back(offsets);
    filterFuncArgs.push_back(curIndexVal);
    filterFuncArgs.push_back(isResultNullStore);
    filterFuncArgs.push_back(lengthAllocaInst);
    filterFuncArgs.push_back(executionContext);
    filterFuncArgs.push_back(dictionaryVectors);

    // Get the boolean response for this row from the filter function.
    ret = builder->CreateCall(filterFunc, filterFuncArgs, "ROW_EVAL");
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

    OptimizeFunctionsAndModule();
    jit->getMainJITDylib().addGenerator(
        eoe(DynamicLibrarySearchGenerator::GetForCurrentProcess(jit->getDataLayout().getGlobalPrefix())));
    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = llvm::orc::ThreadSafeModule(move(module), move(context));
    eoe(jit->addIRModule(resTracker, std::move(threadSafeModule)));
    rt = resTracker;

    auto sym = eoe(jit->lookup("FILTER_WRAPPER"));
    return sym.getAddress();
}

std::vector<Type*> GetSingleFilterArguments(LLVMContext &context)
{
    std::vector<Type*> args = {
        Type::getInt64PtrTy(context),
        Type::getInt64PtrTy(context),
        Type::getInt64PtrTy(context),
        Type::getInt32Ty(context),
        Type::getInt64Ty(context),
        Type::getInt64PtrTy(context)
    };
    return args;
}

int64_t FilterCodeGen::GetExpressionEvaluator()
{
    // Array of addresses, bitmap, row index
    std::vector<Type*> args = GetSingleFilterArguments(*context);
    Function* baseFunc = this->CreateFunction();
    FunctionType* funcSignature = FunctionType::get(Type::getInt1Ty(*context), args, false);
    Function *funcDecl = Function::Create(funcSignature, Function::ExternalLinkage, "FUNC_WRAPPER", module.get());
    BasicBlock *wrapperBody = BasicBlock::Create(*context, "DATA_ACCESS", funcDecl);
    builder->SetInsertPoint(wrapperBody);
    // Name the arguments
    Argument *inputData = funcDecl->getArg(ROW_FILTER_INPUT_INDEX);
    inputData->setName("INPUT_DATA");
    Argument *nulls = funcDecl->getArg(ARGUMENT_ONE);
    nulls->setName("NULLS");
    Argument *offsets = funcDecl->getArg(ROW_FILTER_OFFSETS_INDEX);
    offsets->setName("OFFSETS");
    Argument *rowIndex = funcDecl->getArg(ROW_FILTER_ROW_IDX_INDEX);
    rowIndex->setName("ROW_INDEX");
    Argument *executionContext = funcDecl->getArg(ROW_FILTER_EXECUTION_CONTEXT_INDEX);
    executionContext->setName("EXECUTION_CONTEXT_ADDRESS");
    Argument *dictionaryVectors = funcDecl->getArg(ROW_FILTER_DICT_VECTORS_INDEX);
    dictionaryVectors->setName("DICTIONARY_VECTOR_ADDRESSES");

    std::vector<Value*> funcArgs;
    funcArgs.push_back(inputData);
    funcArgs.push_back(nulls);
    funcArgs.push_back(offsets);
    funcArgs.push_back(rowIndex);

    // Create a boolean pointer to store result null value
    AllocaInst *isResultNullStore = builder->CreateAlloca(Type::getInt1Ty(*context), nullptr, "isResultNull");
    builder->CreateStore(CreateConstantBool(false), isResultNullStore);
    funcArgs.push_back(isResultNullStore);

    // Create a boolean pointer to store result null value
    AllocaInst *lengthAllocaInst = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "LENGTH_PTR");
    builder->CreateStore(CreateConstantInt(0), lengthAllocaInst);
    funcArgs.push_back(lengthAllocaInst);
    funcArgs.push_back(executionContext);
    funcArgs.push_back(dictionaryVectors);

    builder->CreateRet(builder->CreateCall(baseFunc, funcArgs, "ROW_EVAL"));
#ifdef DEBUG
    module->print(errs(), nullptr);
#endif
    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = llvm::orc::ThreadSafeModule(move(module), move(context));
    eoe(jit->addIRModule(resTracker, std::move(threadSafeModule)));
    rt = resTracker;
    return eoe(jit->lookup("FUNC_WRAPPER")).getAddress();
}