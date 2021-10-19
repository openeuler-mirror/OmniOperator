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
    const int NEW_NULL_VALUES_INDEX = 7;
    const int NEW_LENGTHS_VALUES_INDEX = 8;
    const int OFFSETS_INDEX = 6;
    const int ARGUMENT_ZERO = 0;
    const int ARGUMENT_ONE = 1;
    const int ARGUMENT_TWO = 2;
    const int ROW_PROJ_OFFSETS_INDEX = 2;
    const int ROW_PROJ_ROW_IDX_INDEX = 3;
    const int ROW_PROJ_NULL_INDEX = 4;
    const int ROW_PROJ_LENGTH_INDEX = 5;
}
int64_t ProjectionCodeGen::GetFunction()
{
    Function *func = this->CreateFunction();
    return this->CreateWrapper(*func);
}


int64_t ProjectionCodeGen::CreateWrapper(Function &projFunc)
{
    Function *proj = &projFunc;

    int32_t nArgs = this->datatypes.size();
    std::vector<Type*> args;
    /*
    For filter enabled:
    def wrapper_func(i64* input_array, i32 num_rows, i64 out_addr)
    For filter disabled:
    def wrapper_func(i64* input_array, i32 num_rows, i64 out_addr, i32* selected_array, i32 num_selected)
    */
    // Input table, array of addresses
    args.push_back(Type::getInt64PtrTy(*context));
    // Number of rows in input
    args.push_back(Type::getInt32Ty(*context));
    // Results column to write to
    args.push_back(Type::getInt64Ty(*context));
    // These two arguments will not be used if filter is disabled
    // Array of indices from input to select
    args.push_back(Type::getInt32PtrTy(*context));
    // Number of selected rows
    args.push_back(Type::getInt32Ty(*context));
    // bitmap is a 2d array of booleans
    Type *bitmapArg = Type::getInt64PtrTy(*context);
    args.push_back(bitmapArg);
    // Offsets for columns
    args.push_back(Type::getInt64PtrTy(*context));
    // bool array to hold null values
    args.push_back(Type::getInt1PtrTy(*context));
    // int array to hold output values
    args.push_back(Type::getInt32PtrTy(*context));

    FunctionType *funcSignature = FunctionType::get(Type::getInt32Ty(*context), args, false);
    Function *funcDecl = Function::Create(funcSignature, Function::ExternalLinkage, "PROJECT_WRAPPER", module.get());
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

    Argument *outputLengthsAddress = funcDecl->getArg(NEW_LENGTHS_VALUES_INDEX);
    outputLengthsAddress->setName("NEW_LENGTH_VALUES_ADDRESS");

    Value *zero = this->CreateConstantInt(0);
    Value *one = this->CreateConstantInt(1);
    std::vector<Value*> projFuncArgs;
    // projFuncArgs contains the values of the arguments to the projection function
    // value*, bitmap*, offset*, rowIdx, isResultNull*, outputLength*
    int32_t argsSize = 6;
    projFuncArgs.reserve(argsSize);
    Value *gep;

    CallInst *ret;
    // pre loop body
    builder->SetInsertPoint(preLoop);
    // Pointer to counter
    // i32* ptrToCounter
    AllocaInst *indexStore = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "INDEX_COUNTER");
    // Initialize row index to 0.
    builder->CreateStore(zero, indexStore);
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
    Type *outPtrType;

    switch (this->expr->GetExprDataType()) {
        case DataType::INT32D:
            outPtrType = Type::getInt32PtrTy(*context);
            break;
        case DataType::INT64D:
            outPtrType = Type::getInt64PtrTy(*context);
            break;
        case DataType::DOUBLED:
            outPtrType = Type::getDoublePtrTy(*context);
            break;
        case DataType::STRINGD:
            outPtrType = Type::getInt64PtrTy(*context);
            break;
        case DataType::DECIMAL128D:
            outPtrType = Type::getInt64PtrTy(*context);
            break;
        default:
            LLVM_DEBUG_LOG("Error: Invalid column type %d", expr->GetExprDataType());
            break;
    }
    Value *outColPtr = builder->CreateIntToPtr(outputAddress, outPtrType);

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

    projFuncArgs.push_back(input);
    projFuncArgs.push_back(bitmap);
    projFuncArgs.push_back(offsets);
    projFuncArgs.push_back(rowIndexVal);

    // Create a boolean pointer to store result null value
    AllocaInst *isResultNullStore = builder->CreateAlloca(Type::getInt1Ty(*context), nullptr, "isResultNull");
    builder->CreateStore(CreateConstantBool(false), isResultNullStore);
    projFuncArgs.push_back(isResultNullStore);

    // Create a integer pointer to store output length value
    AllocaInst *outputLenPtr = builder->CreateAlloca(Type::getInt32Ty(*context), nullptr, "OUTPUT_LENGTH");
    projFuncArgs.push_back(outputLenPtr);

    // Get the boolean response for this row from the filter function.
    // ret = column value after applying projection
    ret = builder->CreateCall(proj, projFuncArgs, "ROW_PROCESS");
    // Add the processed value to output column.
    builder->CreateBr(addToOutput);
    // Add row index to results array
    builder->SetInsertPoint(addToOutput);
    // x* gep = gep x* outColPtr, i32 counter
    gep = builder->CreateGEP(outColPtr, curIndexVal, "OUTPUT_ADDRESS");
    // *gep = ret
    builder->CreateStore(ret, gep);

    auto isResultNull = builder->CreateLoad(isResultNullStore, "isResultNull");
    // update null values
    gep = builder->CreateGEP(nullValuesAddress, curIndexVal, "NULL_VALUE_POINTER_ADDRESS");
    builder->CreateStore(isResultNull, gep);

    auto outputLen = builder->CreateLoad(outputLenPtr, "OUTPUT_LENGTH");
    // update length values
    gep = builder->CreateGEP(outputLengthsAddress, curIndexVal, "OUTPUT_LENGTHS_POINTER_ADDRESS");
    builder->CreateStore(outputLen, gep);

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

    module->print(errs(), nullptr);

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
        Type::getInt1PtrTy(context),
        Type::getInt32PtrTy(context)
    };
    return args;
}

/*
Apply the row expression on a single row in the table.
Returns the address of a function with the signature void* (*) (int64_t*, bool*, int32_t)
Takes the following arguments
- An array of addresses representing the input table, where each address points to a vec column.
- A 1D array of bools representing the null values in the table.
- An integer representing the row index to select and perform the row expression for.
In reality the function returns a pointer of appropriate type depending on the row expression
and input types. For example if the expected output is an int32, the function will return int32_t*
but since this type is not known at compile time it can be treated as void* or int64_t or any 8 byte
datatype and casted appropriately.
*/
int64_t ProjectionCodeGen::GetExpressionEvaluator()
{
    // Array of addresses, bitmap, row index
    std::vector<Type*> args = GetSingleProjectArguments(*context);
    Function* baseFunc = this->CreateFunction();
    FunctionType* funcSignature = FunctionType::get(ToPointerType(expr->GetExprDataType()), args, false);
    Function *funcDecl = Function::Create(funcSignature, Function::ExternalLinkage, "FUNC_WRAPPER", module.get());
    builder->SetInsertPoint(BasicBlock::Create(*context, "DATA_ACCESS", funcDecl));
    // Name the arguments
    Argument *inputData = funcDecl->getArg(ARGUMENT_ZERO);
    inputData->setName("INPUT_DATA");
    Argument *nulls = funcDecl->getArg(ARGUMENT_ONE);
    nulls->setName("NULLS");
    Argument *offsets = funcDecl->getArg(ROW_PROJ_OFFSETS_INDEX);
    offsets->setName("OFFSETS");
    Argument *rowIndex = funcDecl->getArg(ROW_PROJ_ROW_IDX_INDEX);
    rowIndex->setName("ROW_INDEX");
    Argument *nullIndex = funcDecl->getArg(ROW_PROJ_NULL_INDEX);
    nullIndex->setName("NULL_INDEX");
    Argument *lengthPtr = funcDecl->getArg(ROW_PROJ_LENGTH_INDEX);
    lengthPtr->setName("LENGTH_PTR");

    std::vector<Value*> funcArgs;
    funcArgs.push_back(inputData);
    funcArgs.push_back(nulls);
    funcArgs.push_back(offsets);
    funcArgs.push_back(rowIndex);
    funcArgs.push_back(nullIndex);
    funcArgs.push_back(lengthPtr);

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
