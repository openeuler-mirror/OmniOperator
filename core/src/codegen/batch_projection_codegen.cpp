/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch projection expression codegen
 */

#include "batch_projection_codegen.h"

using namespace llvm;
using namespace orc;
using namespace omniruntime::expressions;
using namespace omniruntime::type;
using namespace omniruntime;

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

std::unique_ptr<BatchProjectionCodeGen> BatchProjectionCodeGen::Create(std::string name,
    const omniruntime::expressions::Expr &expression, bool filter, omniruntime::op::OverflowConfig *overflowConfig)
{
    std::unique_ptr<BatchProjectionCodeGen> codegen { new BatchProjectionCodeGen(std::move(name), expression, filter,
        overflowConfig) };
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

int64_t BatchProjectionCodeGen::GetFunction()
{
    llvm::Function *func = this->CreateBatchFunction();
    if (func == nullptr) {
        return 0;
    }
    return this->CreateBatchWrapper(*func);
}

int64_t BatchProjectionCodeGen::CreateBatchWrapper(llvm::Function &projFunc)
{
    llvm::Function *proj = &projFunc;

    std::vector<Type *> args;
    // data, rowCnt, outputData, (selectedRows, numSelected), bitmap, offsets, outputNull, outputOffsets, execution
    // context, dictionary vectors,
    args.push_back(llvmTypes->I64PtrType());
    args.push_back(llvmTypes->I32Type());
    args.push_back(llvmTypes->I64Type());
    args.push_back(llvmTypes->I32PtrType());
    args.push_back(llvmTypes->I32Type());
    args.push_back(llvmTypes->I64PtrType());
    args.push_back(llvmTypes->I64PtrType());
    args.push_back(llvmTypes->I1PtrType());
    args.push_back(llvmTypes->I32PtrType());
    args.push_back(llvmTypes->I64Type());
    args.push_back(llvmTypes->I64PtrType());

    FunctionType *funcSignature = FunctionType::get(llvmTypes->I32Type(), args, false);
    llvm::Function *funcDecl =
        llvm::Function::Create(funcSignature, llvm::Function::ExternalLinkage, "BATCH_PROJECT_WRAPPER", module);
    BasicBlock *projectionMain = BasicBlock::Create(*context, "PROJECTION_MAIN", funcDecl);

    // set args names
    Argument *input = funcDecl->getArg(INPUT_TABLE_INDEX);
    input->setName("INPUT_TABLE");
    Argument *numRows = funcDecl->getArg(NUM_ROWS_INDEX);
    numRows->setName("NUM_ROWS");
    Argument *outputAddress = funcDecl->getArg(OUTPUT_ADDRESS_INDEX);
    outputAddress->setName("OUTPUT_ADDRESS");
    // Only use these values if filter enabled
    Argument *selected = nullptr;
    Argument *numSelected = nullptr;
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

    builder->SetInsertPoint(projectionMain);
    Type *outPtrType = nullptr;
    switch (this->expr->GetReturnTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32:
            outPtrType = llvmTypes->I32PtrType();
            break;
        case OMNI_LONG:
        case OMNI_DECIMAL64:
            outPtrType = llvmTypes->I64PtrType();
            break;
        case OMNI_DOUBLE:
            outPtrType = llvmTypes->DoublePtrType();
            break;
        case OMNI_CHAR:
        case OMNI_VARCHAR: {
            outPtrType = llvmTypes->I8PtrType();
            break;
        }
        case OMNI_DECIMAL128:
            outPtrType = llvmTypes->I128PtrType();
            break;
        case OMNI_BOOLEAN:
            outPtrType = llvmTypes->I1PtrType();
            break;
        default:
            LLVM_DEBUG_LOG("Error: Invalid column type %d", expr->GetReturnTypeId());
            break;
    }
    Value *outColPtr = builder->CreateIntToPtr(outputAddress, outPtrType);

    AllocaInst *rowIdxArray;
    if (filter) {
        rowIdxArray = reinterpret_cast<AllocaInst *>(selected);
        numRows = numSelected;
    } else {
        rowIdxArray = builder->CreateAlloca(llvmTypes->I32Type(), numRows, "ROW_IDX_ARRAY");
        llvmEngine->CallExternFunction("fill_rowIdx", { OMNI_INT, OMNI_INT }, OMNI_INT, { rowIdxArray, numRows },
            nullptr, "fill_rowIdx");
    }
    // generate output array for inner function
    AllocaInst *outputLenPtr = builder->CreateAlloca(llvmTypes->I32Type(), numRows, "OUTPUT_LENGTH");
    auto isNullPtr = builder->CreateAlloca(llvmTypes->I1Type(), numRows, "IS_NULL");
    auto resArray = this->GetResultArray(this->expr->GetReturnTypeId(), numRows);

    std::vector<Value *> projFuncArgs { input,        bitmap,           offsets,           numRows,   rowIdxArray,
        outputLenPtr, executionContext, dictionaryVectors, isNullPtr, resArray };
    builder->CreateCall(proj, projFuncArgs, "INNER_FUNC");

    std::vector<Value *> funcArgs;
    if (TypeUtil::IsStringType(expr->GetReturnTypeId())) {
        std::vector<DataTypeId> paramTypes = { OMNI_LONG, OMNI_VARCHAR, OMNI_INT, OMNI_INT };
        funcArgs = { outColPtr, resArray, outputLenPtr, numRows };
        llvmEngine->CallExternFunction("batch_WrapVarcharVector", paramTypes, OMNI_INT, funcArgs, nullptr,
            "copy_varchar_result");
    } else {
        funcArgs = { outColPtr, resArray, numRows };
        llvmEngine->CallExternFunction("batch_copy", { this->expr->GetReturnTypeId() }, this->expr->GetReturnTypeId(),
            funcArgs, nullptr, "copy_result");
    }

    auto dstNullPtr = builder->CreateIntToPtr(nullValuesAddress, llvmTypes->I1PtrType());
    funcArgs = { dstNullPtr, isNullPtr, numRows };
    llvmEngine->CallExternFunction("batch_copy", { OMNI_BOOLEAN }, OMNI_BOOLEAN, funcArgs, nullptr, "copy_null");
    builder->CreateRet(numRows);

    llvmEngine->OptimizeFunctionsAndModule();
    jit->getMainJITDylib().addGenerator(
        eoe(DynamicLibrarySearchGenerator::GetForCurrentProcess(jit->getDataLayout().getGlobalPrefix())));
    auto resTracker = jit->getMainJITDylib().createResourceTracker();
    llvmEngine->MakeThreadSafe(&resTracker);
    rt = resTracker;
    auto sym = eoe(jit->lookup("BATCH_PROJECT_WRAPPER"));
    return sym.getAddress();
}
