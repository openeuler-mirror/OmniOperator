/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: row projection code generator
 */

#include "row_projection_codegen.h"

namespace omniruntime {
namespace codegen {
using namespace llvm;
using namespace llvm::orc;
using namespace omniruntime::expressions;

namespace {
const int ROW_PROJ_INPUT_INDEX = 0;
const int ROW_PROJ_NULL_BITMAP_INDEX = 1;
const int ROW_PROJ_OFFSETS_INDEX = 2;
const int ROW_PROJ_ROW_IDX_INDEX = 3;
const int ROW_PROJ_LENGTH_INDEX = 4;
const int ROW_PROJ_EXECUTION_CONTEXT_INDEX = 5;
const int ROW_PROJ_DICT_VECTORS_INDEX = 6;
const int ROW_PROJ_IS_NULL_INDEX = 7;
}

std::vector<Type *> GetSingleProjectArguments(LLVMContext &context)
{
    std::vector<Type *> args = {
        Type::getInt64PtrTy(context), // valueArray*
        Type::getInt64PtrTy(context), // isNullArray*
        Type::getInt64PtrTy(context), // offsetArray*
        Type::getInt32Ty(context),    // rowIndex
        Type::getInt32PtrTy(context), // lengthArray*
        Type::getInt64Ty(context),    // executionContext address
        Type::getInt64PtrTy(context), // dictionaryArray*
        Type::getInt1PtrTy(context)   // isResultNull*
    };
    return args;
}

intptr_t RowProjectionCodeGen::GetExpressionEvaluator()
{
    std::vector<Type *> args = GetSingleProjectArguments(*context);
    llvm::Function *baseFunc = CreateFunction();
    if (baseFunc == nullptr) {
        return 0;
    }

    FunctionType *funcSignature = FunctionType::get(llvmTypes->ToPointerType(expr->GetReturnTypeId()), args, false);
    llvm::Function *funcDecl =
        llvm::Function::Create(funcSignature, llvm::Function::ExternalLinkage, "WRAPPER_FUNC", modulePtr);
    RecordMainFunction(funcDecl);
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

    std::vector<Value *> funcArgs { inputData,         nulls,    offsets, rowIndex, lengthPtr, executionContext,
        dictionaryVectors, isNullPtr };

    // Store the result
    AllocaInst *retStore = builder->CreateAlloca(baseFunc->getReturnType(), nullptr, "RET_STORE");
    builder->CreateStore(builder->CreateCall(baseFunc, funcArgs, "ROW_EVAL"), retStore);

    builder->CreateRet(retStore);
    OptimizeModule();
    llvm::verifyFunction(*func);

    return Compile();
}

llvm::Function *RowProjectionCodeGen::CreateFunction()
{
    std::vector<Type *> args = GetSingleProjectArguments(*context);

    FunctionType *prototype = FunctionType::get(llvmTypes->GetFunctionReturnType(expr->GetReturnTypeId()), args, false);
    func = llvm::Function::Create(prototype, llvm::Function::ExternalLinkage, funcName, modulePtr);

    std::vector<std::string> argNames { "data",       "nullBitmap",       "offsets",           "rowIdx",
        "dataLength", "executionContext", "dictionaryVectors", "isNullPtr" };

    int32_t idx = 0;
    for (auto &arg : func->args()) {
        arg.setName(argNames[idx]);
        idx++;
    }

    BasicBlock *body = BasicBlock::Create(*context, "CREATED_FUNC_BODY", func);
    builder->SetInsertPoint(body);

    if (!InitializeCodegenContext(func->args())) {
        return nullptr;
    }

    // Generate code
    auto result = VisitExpr(*expr);
    if (result->data == nullptr) {
        return nullptr;
    }

    // Update final output Length
    if (result->length != nullptr) {
        Argument *outputLength = func->getArg(ROW_PROJ_LENGTH_INDEX);
        Value *lengthGep = builder->CreateGEP(llvmTypes->I32Type(), outputLength, llvmTypes->CreateConstantInt(0),
            "OUTPUT_LENGTH_ADDRESS");
        builder->CreateStore(result->length, lengthGep);
    }

    builder->CreateStore(result->isNull, func->getArg(ROW_PROJ_IS_NULL_INDEX));

    // cast char* to int64 for output
    if (expr->GetReturnTypeId() == DataTypeId::OMNI_VARCHAR) {
        result->data = builder->CreatePtrToInt(result->data, llvmTypes->I64Type());
    }
    // Return value
    builder->CreateRet(result->data);
    verifyFunction(*func);
    return func;
}

void RowProjectionCodeGen::Visit(const FieldExpr &fExpr)
{
    Value *rowIdx = this->codegenContext->rowIdx;
    Value *vecBatch = this->codegenContext->data;
    Value *bitmap = this->codegenContext->nullBitmap;
    Value *offsets = this->codegenContext->offsets;
    Value *dictionaryVectors = this->codegenContext->dictionaryVectors;
    Type *dataType = llvmTypes->ToLLVMType(fExpr.GetReturnTypeId());

    Value *colIdx = llvmTypes->CreateConstantInt(fExpr.colVal);
    // Find address of this column in the addresses array argument.
    Value *gep = builder->CreateGEP(llvmTypes->I64Type(), vecBatch, colIdx);
    Value *length = nullptr;

    auto dictionaryVectorGEP = builder->CreateGEP(llvmTypes->I64Type(), dictionaryVectors, colIdx);
    Value *dictionaryVectorPtr = builder->CreateLoad(llvmTypes->I64Type(), dictionaryVectorGEP);
    auto condition = builder->CreateIsNotNull(dictionaryVectorPtr);

    BasicBlock *trueBlock = BasicBlock::Create(*context, "DICTIONARY_NOT_NULL", func);
    BasicBlock *falseBlock = BasicBlock::Create(*context, "DICTIONARY_IS_NULL");
    BasicBlock *mergeBlock = BasicBlock::Create(*context, "ifcont");

    builder->CreateCondBr(condition, trueBlock, falseBlock);

    builder->SetInsertPoint(trueBlock);

    AllocaInst *lengthAllocaInst = nullptr;
    Value *dictionaryValue =
        this->GetDictionaryVectorValue(*(fExpr.GetReturnType()), rowIdx, dictionaryVectorPtr, lengthAllocaInst);
    if (dictionaryValue == nullptr) {
        return;
    }

    Value *dictionaryLength = nullptr;
    if (TypeUtil::IsStringType(fExpr.GetReturnTypeId())) {
        dictionaryLength = builder->CreateLoad(llvmTypes->I32Type(), lengthAllocaInst, "varchar_length");
    }

    builder->CreateBr(mergeBlock);
    trueBlock = builder->GetInsertBlock();
    func->getBasicBlockList().push_back(falseBlock);

    // If dictionary vector is not present, get vector values
    // using valuesAddress and length using offsets if varchar type
    builder->SetInsertPoint(falseBlock);
    // Load the address value.
    Value *elementAddr = builder->CreateLoad(llvmTypes->I64Type(), gep);

    Value *elementPtr = GetPtrTypeFromInt(fExpr.GetReturnTypeId(), elementAddr);
    Value *dataValue = nullptr;
    if (TypeUtil::IsStringType(fExpr.GetReturnTypeId())) {
        // Get offset for varchar
        auto offsetsGEP = builder->CreateGEP(llvmTypes->I64Type(), offsets, colIdx);
        Value *offsetPtr = builder->CreateLoad(llvmTypes->I64Type(), offsetsGEP);
        offsetPtr = builder->CreateIntToPtr(offsetPtr, llvmTypes->I32PtrType());
        auto colOffsetGEP = builder->CreateGEP(llvmTypes->I32Type(), offsetPtr, rowIdx);
        Value *startOffset = builder->CreateLoad(llvmTypes->I32Type(), colOffsetGEP);
        colOffsetGEP = builder->CreateGEP(llvmTypes->I32Type(), offsetPtr,
            builder->CreateAdd(rowIdx, llvmTypes->CreateConstantInt(1)));
        Value *endOffset = builder->CreateLoad(llvmTypes->I32Type(), colOffsetGEP);
        // Get length for varchar
        length = builder->CreateSub(endOffset, startOffset);
        // Find the address of the row to be processed.
        dataValue = builder->CreateGEP(llvmTypes->I8Type(), elementPtr, startOffset);
    } else {
        // Find the address of the row to be processed.
        gep = builder->CreateGEP(dataType, elementPtr, rowIdx);
        // Value to be processed.
        dataValue = builder->CreateLoad(dataType, gep);
    }

    builder->CreateBr(mergeBlock);
    falseBlock = builder->GetInsertBlock();

    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);

    // Get merged data value and length
    int32_t numReservedValues = 2;

    PHINode *phiValue = builder->CreatePHI(dataType, numReservedValues, "iftmp");
    phiValue->addIncoming(dictionaryValue, trueBlock);
    phiValue->addIncoming(dataValue, falseBlock);

    // Length is only valid for varchar type
    PHINode *phiLength = nullptr;
    if (TypeUtil::IsStringType(fExpr.GetReturnTypeId())) {
        phiLength = builder->CreatePHI(llvmTypes->I32Type(), numReservedValues, "length");
        phiLength->addIncoming(dictionaryLength, trueBlock);
        phiLength->addIncoming(length, falseBlock);
    }

    // Get isNull value
    auto bitmapGEP = builder->CreateGEP(llvmTypes->I64Type(), bitmap, colIdx);
    Value *bitmapValue = builder->CreateLoad(llvmTypes->I64Type(), bitmapGEP);
    bitmapValue = builder->CreateIntToPtr(bitmapValue, llvmTypes->I1PtrType());
    bitmapGEP = builder->CreateGEP(llvmTypes->I1Type(), bitmapValue, rowIdx);
    bitmapValue = builder->CreateLoad(llvmTypes->I1Type(), bitmapGEP);

    if (TypeUtil::IsDecimalType(fExpr.GetReturnTypeId())) {
        Value *precision =
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetPrecision());
        Value *scale =
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetScale());
        this->value = std::make_shared<DecimalValue>(phiValue, bitmapValue, precision, scale);
    } else {
        this->value = std::make_shared<CodeGenValue>(phiValue, bitmapValue, phiLength);
    }
}
}
}
