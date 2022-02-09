/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 * Description: project  codegen
 */
#include "projection_codegen.h"

using namespace llvm;
using namespace orc;
using namespace omniruntime::expressions;


namespace {
    const int INPUT_TABLE_INDEX = 0;
    const int NUM_ROWS_INDEX = 1;
    const int OUTPUT_ADDRESS_INDEX = 2;
    const int SELECTED = 3;
    const int NUM_SELECTED = 4;
    const int BITMAP = 5;
    const int OFFSETS_INDEX = 6;
    const int NEW_NULL_VALUES_INDEX = 7;
    const int OUTPUT_OFFSETS_INDEX = 8;
    const int EXECUTION_CONTEXT_IDX = 9;
    const int DICTIONARY_VECTORS_IDX = 10;
    const int ROW_PROJ_INPUT_INDEX = 0;
    const int ROW_PROJ_NULL_BITMAP_INDEX = 1;
    const int ROW_PROJ_OFFSETS_INDEX = 2;
    const int ROW_PROJ_ROW_IDX_INDEX = 3;
    const int ROW_PROJ_LENGTH_INDEX = 4;
    const int ROW_PROJ_EXECUTION_CONTEXT_INDEX = 5;
    const int ROW_PROJ_DICT_VECTORS_INDEX = 6;
    const int ROW_PROJ_IS_NULL_INDEX = 7;
}

std::unique_ptr<ProjectionCodeGen> ProjectionCodeGen::Create(
    std::string name, const omniruntime::expressions::Expr &expression, bool filter)
{
    std::unique_ptr<ProjectionCodeGen> codegen {new ProjectionCodeGen(std::move(name), expression, filter)};
    codegen->Initialize();
    return codegen;
}

int64_t ProjectionCodeGen::GetFunction()
{
    Function *func = this->CreateFunction();
    if (func == nullptr) {
        return 0;
    }
    return this->CreateWrapper(*func);
}

int64_t ProjectionCodeGen::CreateWrapper(llvm::Function &projFunc)
{
    llvm::Function *proj = &projFunc;

    std::vector<Type*> args;
    /*
    For filter enabled:
    def wrapper_func(i64* input_array, i32 num_rows, i64 out_addr)
    For filter disabled:
    def wrapper_func(i64* input_array, i32 num_rows, i64 out_addr, i32* selected_array, i32 num_selected)
    */
    // Input table, array of addresses
    args.push_back(llvmTypes->I64PtrType());
    // Number of rows in input
    args.push_back(llvmTypes->I32Type());
    // Results column to write to
    args.push_back(llvmTypes->I64Type());
    // These two arguments will not be used if filter is disabled
    // Array of indices from input to select
    args.push_back(llvmTypes->I32PtrType());
    // Number of selected rows
    args.push_back(llvmTypes->I32Type());
    // bitmap is a 2d array of booleans
    Type *bitmapArg = llvmTypes->I64PtrType();
    args.push_back(bitmapArg);
    // Offsets for columns
    args.push_back(llvmTypes->I64PtrType());
    // bool array to return evaluated null status
    args.push_back(llvmTypes->I1PtrType());
    // int array to hold output values
    args.push_back(llvmTypes->I32PtrType());
    // execution context with allocator to allocate, free and track memory
    args.push_back(llvmTypes->I64Type());
    // dictionary vectors
    args.push_back(llvmTypes->I64PtrType());

    FunctionType *funcSignature = FunctionType::get(llvmTypes->I32Type(), args, false);
    llvm::Function *funcDecl = llvm::Function::Create(funcSignature, llvm::Function::ExternalLinkage,
        "PROJECT_WRAPPER", module.get());
    BasicBlock *preLoop = BasicBlock::Create(*context, "PRE_LOOP", funcDecl);
    BasicBlock *loopBody = BasicBlock::Create(*context, "LOOP_BODY", funcDecl);
    BasicBlock *addToOutput = BasicBlock::Create(*context, "ADD_OUTPUT", funcDecl);
    BasicBlock *incrementCounter = BasicBlock::Create(*context, "INCREMENT_COUNTER", funcDecl);
    BasicBlock *endBlock = BasicBlock::Create(*context, "END_BLOCK", funcDecl);
    // preprocessing
    Argument *input = funcDecl->getArg(INPUT_TABLE_INDEX);
    input->setName("INPUT_TABLE");
    Argument *numRows = funcDecl->getArg(NUM_ROWS_INDEX);
    numRows->setName("NUM_ROWS");
    Argument *outputAddress = funcDecl->getArg(OUTPUT_ADDRESS_INDEX);
    outputAddress->setName("OUTPUT_ADDRESS");

    // Only use these values if filter enabled
    Argument *selected;
    Argument *numSelected;
    if (filter) {
        selected = funcDecl->getArg(SELECTED);
        selected->setName("SELECTED_ARRAY");
        numSelected = funcDecl->getArg(NUM_SELECTED);
        numSelected->setName("NUM_SELECTED");
    }

    Argument *bitmap = funcDecl->getArg(BITMAP);
    bitmap->setName("BITMAP");

    Argument *offsets = funcDecl->getArg(OFFSETS_INDEX);
    offsets->setName("OFFSETS");

    Argument *nullValuesAddress = funcDecl->getArg(NEW_NULL_VALUES_INDEX);
    nullValuesAddress->setName("NULL_VALUES_ADDRESS");

    Argument *outputOffsetsAddress = funcDecl->getArg(OUTPUT_OFFSETS_INDEX);
    outputOffsetsAddress->setName("OUTPUT_OFFSETS_ADDRESS");

    Argument *executionContext = funcDecl->getArg(EXECUTION_CONTEXT_IDX);
    executionContext->setName("EXECUTION_CONTEXT_ADDRESS");

    Argument *dictionaryVectors = funcDecl->getArg(DICTIONARY_VECTORS_IDX);
    dictionaryVectors->setName("DICTIONARY_VECTORS");

    Value *zero = llvmTypes->CreateConstantInt(0);
    Value *one = llvmTypes->CreateConstantInt(1);
    Value *gep;

    CallInst *ret;
    // pre loop body
    builder->SetInsertPoint(preLoop);
    // Pointer to counter
    // i32* ptrToCounter
    AllocaInst *indexStore = builder->CreateAlloca(llvmTypes->I32Type(), nullptr, "INDEX_COUNTER");
    // Initialize row index to 0.
    builder->CreateStore(zero, indexStore);
    AllocaInst *offsetStore = builder->CreateAlloca(llvmTypes->I32Type(), nullptr, "CURRENT_OFFSET");
    // Initialize offset to 0.
    builder->CreateStore(zero, offsetStore);
    // Counter variable value.
    // i32 counter
    Value *curIndexVal;
    // Index of row to be processed.
    // i32 rowIndex
    Value *rowIndexVal;
    // Temp value for next row index.
    // i32 nextCounterValue
    Value *nextIndexVal;

    // Only use if filter enabled
    // i64 selectedAddress
    Value *selectedAddress;

    // Type of output column
    Type *outPtrType = nullptr;
    switch (this->expr->GetReturnTypeId()) {
        case OMNI_VEC_TYPE_INT:
        case OMNI_VEC_TYPE_DATE32:
            outPtrType = llvmTypes->I32PtrType();
            break;
        case OMNI_VEC_TYPE_LONG:
            outPtrType = llvmTypes->I64PtrType();
            break;
        case OMNI_VEC_TYPE_DOUBLE:
            outPtrType = llvmTypes->DoublePtrType();
            break;
        case OMNI_VEC_TYPE_CHAR:
        case OMNI_VEC_TYPE_VARCHAR:
            outPtrType = llvmTypes->I8PtrType();
            break;
        case OMNI_VEC_TYPE_DECIMAL128:
            outPtrType = llvmTypes->I64PtrType();
            break;
        case OMNI_VEC_TYPE_BOOLEAN:
            outPtrType = llvmTypes->I1PtrType();
            break;
        default:
            LLVM_DEBUG_LOG("Error: Invalid column type %d", expr->GetReturnType());
            break;
    }
    Value *outColPtr = builder->CreateIntToPtr(outputAddress, outPtrType);
    // Create a integer pointer to store output length value
    AllocaInst *outputLenPtr = builder->CreateAlloca(llvmTypes->I32Type(), nullptr, "OUTPUT_LENGTH");
    auto isNullPtr = builder->CreateAlloca(llvmTypes->I1Type(), nullptr, "IS_NULL");

    builder->CreateBr(loopBody);
    // loop body
    builder->SetInsertPoint(loopBody);
    // Get the value of the current row index to process.
    // i32 counter = *ptrToCounter
    curIndexVal = builder->CreateLoad(indexStore, "CUR_INDEX");
    if (filter) {
        // Get address of selected index.
        // i32* selectedAddress = gep i32* selected, i32 counter
        selectedAddress = builder->CreateGEP(selected, curIndexVal, "SELECTED_ADDRESS");
        // i32 rowIndexVal = *selectedAddress
        rowIndexVal = builder->CreateLoad(selectedAddress);
    } else {
        // i32 rowIndexVal = counter
        rowIndexVal = curIndexVal;
    }

    builder->CreateStore(llvmTypes->CreateConstantBool(false), isNullPtr);

    std::vector<Value*> projFuncArgs;
    // projFuncArgs contains the values of the arguments to the projection function
    // value*, bitmap*, offset*, rowIdx, outputLength*, executionContext, dictionaryVectors, isNullPtr
    int32_t argsSize = 8;
    projFuncArgs.reserve(argsSize);

    projFuncArgs.push_back(input);
    projFuncArgs.push_back(bitmap);
    projFuncArgs.push_back(offsets);
    projFuncArgs.push_back(rowIndexVal);
    projFuncArgs.push_back(outputLenPtr);
    projFuncArgs.push_back(executionContext);
    projFuncArgs.push_back(dictionaryVectors);
    projFuncArgs.push_back(isNullPtr);

    // Get the boolean response for this row from the filter function.
    // ret = column value after applying projection
    ret = builder->CreateCall(proj, projFuncArgs, "ROW_PROCESS");

    // Add the processed value to output column.
    builder->CreateBr(addToOutput);
    // Add row index to results array
    builder->SetInsertPoint(addToOutput);

    if (TypeUtil::IsStringType(expr->GetReturnTypeId())) {
        auto outputLen = builder->CreateLoad(outputLenPtr, "OUTPUT_LENGTH");
        auto stringPtr = builder->CreateIntToPtr(ret, Type::getInt8PtrTy(*context));
        // call wrap_varchar_vector function
        std::vector<Value *> argVals { outColPtr, curIndexVal, stringPtr, outputLen};
        auto f = module->getFunction(WrapVarcharVectorStr);
        builder->CreateCall(f, argVals);
    } else {
        // x* gep = gep x* outColPtr, i32 counter
        gep = builder->CreateGEP(outColPtr, curIndexVal, "OUTPUT_ADDRESS");
        // *gep = ret
        builder->CreateStore(ret, gep);
    }

    gep = builder->CreateGEP(nullValuesAddress, curIndexVal, "NULL_VALUE_POINTER_ADDRESS");
    builder->CreateStore(builder->CreateLoad(isNullPtr), gep);

    builder->CreateBr(incrementCounter);
    // Increment loop counter
    builder->SetInsertPoint(incrementCounter);
    // Increment counter.
    nextIndexVal = builder->CreateAdd(curIndexVal, one, "NEXT_INDEX");
    builder->CreateStore(nextIndexVal, indexStore);
    // If there are rows remaining, repeat, otherwise, exit.
    Value *sentinel;
    if (filter) {
        sentinel = numSelected;
    } else {
        sentinel = numRows;
    }
    Value *cond = builder->CreateICmpSLT(nextIndexVal, sentinel, "END_LOOP_COND");
    builder->CreateCondBr(cond, loopBody, endBlock);
    // Return results
    builder->SetInsertPoint(endBlock);
    builder->CreateRet(nextIndexVal);

    OptimizeFunctionsAndModule();

    jit->getMainJITDylib().addGenerator(
        eoe(DynamicLibrarySearchGenerator::GetForCurrentProcess(jit->getDataLayout().getGlobalPrefix())));
    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = llvm::orc::ThreadSafeModule(move(module), move(context));
    eoe(jit->addIRModule(resTracker, std::move(threadSafeModule)));
    rt = resTracker;
    auto sym = eoe(jit->lookup("PROJECT_WRAPPER"));
    return sym.getAddress();
}

std::vector<Type*> GetSingleProjectArguments(LLVMContext &context)
{
    std::vector<Type*> args = {
        Type::getInt64PtrTy(context),
        Type::getInt64PtrTy(context),
        Type::getInt64PtrTy(context),
        Type::getInt32Ty(context),
        Type::getInt32PtrTy(context),
        Type::getInt64Ty(context),
        Type::getInt64PtrTy(context),
        Type::getInt1PtrTy(context)
    };
    return args;
}

/*
Apply the row expression on a single row in the table.
Returns the address of a function with the signature void* (*) (int64_t*, bool*, int32_t, bool*, int32_t*)
Takes the following arguments
- An array of addresses representing the input table, where each address points to a vec column.
- A 1D array of bools representing the null values in the table.
- An integer representing the row index to select and perform the row expression for.
- A Boolean pointer to represent the null status of the row expression output
- A integer pointer to represent the length if the row expression output is a string

In reality the function returns a pointer of appropriate type depending on the row expression
and input types-> For example if the expected output is an int32, the function will return int32_t*
but since this type is not known at compile time it can be treated as void* or int64_t or any 8 byte
datatype and casted appropriately.
*/
int64_t ProjectionCodeGen::GetExpressionEvaluator()
{
    // Array of addresses, bitmap, row index
    std::vector<Type*> args = GetSingleProjectArguments(*context);
    llvm::Function* baseFunc = this->CreateFunction();
    FunctionType* funcSignature = FunctionType::get(llvmTypes->ToPointerType(expr->GetReturnTypeId()), args, false);
    llvm::Function *funcDecl = llvm::Function::Create(funcSignature, llvm::Function::ExternalLinkage,
        "FUNC_WRAPPER", module.get());
    builder->SetInsertPoint(BasicBlock::Create(*context, "DATA_ACCESS", funcDecl));
    // Name the arguments
    Argument *inputData = funcDecl->getArg(ROW_PROJ_INPUT_INDEX);
    inputData->setName("INPUT_DATA");
    Argument *nulls = funcDecl->getArg(ROW_PROJ_NULL_BITMAP_INDEX);
    nulls->setName("NULLS");
    Argument *offsets = funcDecl->getArg(ROW_PROJ_OFFSETS_INDEX);
    offsets->setName("OFFSETS");
    Argument *rowIndex = funcDecl->getArg(ROW_PROJ_ROW_IDX_INDEX);
    rowIndex->setName("ROW_INDEX");
    Argument *lengthPtr = funcDecl->getArg(ROW_PROJ_LENGTH_INDEX);
    lengthPtr->setName("LENGTH_PTR");
    Argument *executionContext = funcDecl->getArg(ROW_PROJ_EXECUTION_CONTEXT_INDEX);
    executionContext->setName("EXECUTION_CONTEXT_ADDRESS");
    Argument *dictionaryVectors = funcDecl->getArg(ROW_PROJ_DICT_VECTORS_INDEX);
    dictionaryVectors->setName("DICTIONARY_VECTOR_ADDRESSES");
    Argument *isNullPtr = funcDecl->getArg(ROW_PROJ_IS_NULL_INDEX);
    isNullPtr->setName("IS_NULL_PTR");

    std::vector<Value*> funcArgs;
    funcArgs.push_back(inputData);
    funcArgs.push_back(nulls);
    funcArgs.push_back(offsets);
    funcArgs.push_back(rowIndex);
    funcArgs.push_back(lengthPtr);
    funcArgs.push_back(executionContext);
    funcArgs.push_back(dictionaryVectors);
    funcArgs.push_back(isNullPtr);

    // Store the result
    AllocaInst *retStore = builder->CreateAlloca(baseFunc->getReturnType(), nullptr, "RET_STORE");
    builder->CreateStore(builder->CreateCall(baseFunc, funcArgs, "ROW_EVAL"), retStore);

    builder->CreateRet(retStore);
    llvm::verifyFunction(*func);
#ifdef DEBUG
    module->print(errs(), nullptr);
#endif
    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    auto threadSafeModule = llvm::orc::ThreadSafeModule(move(module), move(context));
    eoe(jit->addIRModule(resTracker, std::move(threadSafeModule)));
    rt = resTracker;
    return eoe(jit->lookup("FUNC_WRAPPER")).getAddress();
}
