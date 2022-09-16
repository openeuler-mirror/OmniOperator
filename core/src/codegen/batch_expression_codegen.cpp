/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 * Description: batch expression codegen
 */
#include "batch_expression_codegen.h"
#include "type/decimal128_utils.h"
#include "expr_info_extractor.h"

using namespace llvm;
using namespace orc;
using namespace omniruntime;
using namespace omniruntime::expressions;
using namespace omniruntime::type;
using namespace std;

namespace {
const int INT32_VALUE = 32;
const int INT64_VALUE = 64;
const int EXPRFUNC_ROWCNT_INDEX = 3;
const int EXPRFUNC_OUT_LENGTH_ARG_INDEX = 5;
const int EXPRFUNC_OUT_NULL_INDEX = 8;
const int EXPRFUNC_OUT_DATA_INDEX = 9;
}

BatchExpressionCodeGen::BatchExpressionCodeGen(std::string name, const omniruntime::expressions::Expr &cpExpr,
    omniruntime::op::OverflowConfig *overflowConfig)
    : expr(&cpExpr), overflowConfig(overflowConfig), funcName(std::move(name))
{}

bool BatchExpressionCodeGen::InitializeCodegenContext(iterator_range<llvm::Function::arg_iterator> args)
{
    this->batchCodegenContext = std::make_unique<BatchCodegenContext>();
    for (auto &arg : args) {
        auto argName = arg.getName().str();
        if (argName == "data") {
            batchCodegenContext->data = &arg;
        } else if (argName == "nullBitmap") {
            batchCodegenContext->nullBitmap = &arg;
        } else if (argName == "offsets") {
            batchCodegenContext->offsets = &arg;
        } else if (argName == "rowCnt") {
            batchCodegenContext->rowCnt = &arg;
        } else if (argName == "rowIdxArray") {
            batchCodegenContext->rowIdxArray = &arg;
        } else if (argName == "outputLength" || argName == "outputNull" || argName == "outputData") {
            continue;
        } else if (argName == "executionContext") {
            batchCodegenContext->executionContext = &arg;
        } else if (argName == "dictionaryVectors") {
            batchCodegenContext->dictionaryVectors = &arg;
        } else {
            LogWarn("Invalid argument %s", argName.c_str());
            return false;
        }
    }

    return true;
}

void BatchExpressionCodeGen::ExtractVectorIndexes()
{
    ExprInfoExtractor exprInfoExtractor;
    this->expr->Accept(exprInfoExtractor);
    this->vectorIndexes = exprInfoExtractor.GetVectorIndexes();
}

llvm::Function *BatchExpressionCodeGen::CreateBatchFunction()
{
    int32_t argsSize = 10;
    std::vector<Type *> args;
    args.reserve(argsSize);
    // Values in args vector follow the format:
    // data, nullBitmap, offsets, rowCnt, rowIdxArray, outputLength, executionContext, dictionaryVectors, isNullPtr,
    // resArray
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt32Ty(*context));
    args.push_back(Type::getInt32PtrTy(*context));
    args.push_back(Type::getInt32PtrTy(*context));
    args.push_back(Type::getInt64Ty(*context));
    args.push_back(Type::getInt64PtrTy(*context));
    args.push_back(Type::getInt1PtrTy(*context));
    args.push_back(llvmTypes->ToDataPointerType(expr->GetReturnTypeId()));

    FunctionType *prototype = FunctionType::get(llvmTypes->I32Type(), args, false);
    func = llvm::Function::Create(prototype, llvm::Function::ExternalLinkage, funcName, module);

    std::string argNames[] = {
        "data", "nullBitmap", "offsets", "rowCnt", "rowIdxArray",
        "outputLength", "executionContext", "dictionaryVectors", "outputNull", "outputData"
    };
    int32_t idx = 0;
    for (auto &arg : func->args()) {
        arg.setName(argNames[idx]);
        idx++;
    }

    BasicBlock *body = BasicBlock::Create(*context, "CREATED_BATCH_FUNC_BODY", func);
    builder->SetInsertPoint(body);

    if (!InitializeCodegenContext(func->args())) {
        return nullptr;
    }

    auto result = VisitExpr(*expr);
    if (result->data == nullptr) {
        return nullptr;
    }

    // copy length
    if (result->length != nullptr) {
        llvmEngine->CallExternFunction("batch_copy", { OMNI_INT }, OMNI_INT,
            { func->getArg(EXPRFUNC_OUT_LENGTH_ARG_INDEX), result->length, func->getArg(EXPRFUNC_ROWCNT_INDEX) },
            nullptr, "copy_length");
    }
    // copy data
    auto resultArray = func->getArg(EXPRFUNC_OUT_DATA_INDEX);
    llvmEngine->CallExternFunction("batch_copy", { expr->GetReturnTypeId() }, expr->GetReturnTypeId(),
        { resultArray, result->data, func->getArg(EXPRFUNC_ROWCNT_INDEX) }, nullptr, "copy_data");

    // copy null
    llvmEngine->CallExternFunction("batch_copy", { OMNI_BOOLEAN }, OMNI_BOOLEAN,
        { func->getArg(EXPRFUNC_OUT_NULL_INDEX), result->isNull, func->getArg(EXPRFUNC_ROWCNT_INDEX) }, nullptr,
        "copy_null");

    // Return rowCnt
    builder->CreateRet(func->getArg(EXPRFUNC_ROWCNT_INDEX));
    verifyFunction(*func);
    return func;
}

CodeGenValuePtr BatchExpressionCodeGen::VisitExpr(const Expr &e)
{
    e.Accept(*this);
    return this->value;
}

void BatchExpressionCodeGen::Visit(const LiteralExpr &lExpr)
{
    this->value.reset(LiteralExprConstantHelper(lExpr));
}

CodeGenValue *BatchExpressionCodeGen::LiteralExprConstantHelper(const LiteralExpr &lExpr)
{
    bool isNullLiteral = lExpr.isNull;
    AllocaInst *nullArrayPtr =
        builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "IS_NULL_PTR");
    AllocaInst *literalArrayPtr = GetResultArray(lExpr.GetReturnTypeId(), this->batchCodegenContext->rowCnt);
    Value *literalValue = nullptr;
    Value *isNull = llvmTypes->CreateConstantBool(isNullLiteral);
    Value *length = nullptr;
    Value *lengthArrayPtr = nullptr;
    Value *precision, *scale;
    switch (lExpr.GetReturnTypeId()) {
        case OMNI_INT:
        case OMNI_DATE32: {
            literalValue = llvmTypes->CreateConstantInt(lExpr.intVal);
            break;
        }
        case OMNI_LONG: {
            literalValue = llvmTypes->CreateConstantLong(lExpr.longVal);
            break;
        }
        case OMNI_DOUBLE: {
            literalValue = llvmTypes->CreateConstantDouble(lExpr.doubleVal);
            break;
        }
        case OMNI_CHAR:
        case OMNI_VARCHAR: {
            literalValue = this->CreateConstantString(*(lExpr.stringVal));
            lengthArrayPtr =
                builder->CreateAlloca(llvmTypes->I32Type(), this->batchCodegenContext->rowCnt, "LENGTH_PTR");
            length = llvmTypes->CreateConstantInt(lExpr.stringVal->length());
            break;
        }
        case OMNI_BOOLEAN: {
            literalValue = llvmTypes->CreateConstantBool(lExpr.boolVal);
            break;
        }
        case OMNI_DECIMAL64: {
            precision = llvmTypes->CreateConstantInt(
                static_cast<Decimal64DataType *>(lExpr.GetReturnType().get())->GetPrecision());
            scale =
                llvmTypes->CreateConstantInt(static_cast<Decimal64DataType *>(lExpr.GetReturnType().get())->GetScale());
            literalValue = llvmTypes->CreateConstantLong(lExpr.longVal);
            break;
        }
        case OMNI_DECIMAL128: {
            std::string dec128String = isNullLiteral ? "0" : *lExpr.stringVal;
            __uint128_t dec128 = Decimal128Utils::StrToUint128_t(dec128String.c_str());
            dec128String = Decimal128Utils::Uint128_tToStr(dec128);
            precision = llvmTypes->CreateConstantInt(
                dynamic_cast<Decimal128DataType *>(lExpr.GetReturnType().get())->GetPrecision());
            scale = llvmTypes->CreateConstantInt(
                dynamic_cast<Decimal128DataType *>(lExpr.GetReturnType().get())->GetScale());
            literalValue = llvm::ConstantInt::get(llvm::Type::getInt128Ty(*context), dec128String, 10);
            break;
        }
        default: {
            LogWarn("Unsupported data type in LITERAL Expr %d", lExpr.GetReturnTypeId());
            return new CodeGenValue(nullptr, nullptr);
        }
    }

    std::vector<Value *> funcArgs;

    if (TypeUtil::IsStringType(lExpr.GetReturnTypeId())) {
        funcArgs = { this->batchCodegenContext->executionContext,
            literalArrayPtr,
            nullArrayPtr,
            lengthArrayPtr,
            literalValue,
            isNull,
            length,
            this->batchCodegenContext->rowCnt };
        llvmEngine->CallExternFunction("batch_fill_literal", { lExpr.GetReturnTypeId() }, lExpr.GetReturnTypeId(),
            funcArgs, nullptr, "fill_literal_array");
        return new CodeGenValue(literalArrayPtr, nullArrayPtr, lengthArrayPtr);
    } else if (TypeUtil::IsDecimalType(lExpr.GetReturnTypeId())) {
        funcArgs = { literalArrayPtr, nullArrayPtr, literalValue, isNull, this->batchCodegenContext->rowCnt };
        llvmEngine->CallExternFunction("batch_fill_literal", { lExpr.GetReturnTypeId() }, lExpr.GetReturnTypeId(),
            funcArgs, nullptr, "fill_literal_array");
        return new DecimalValue(literalArrayPtr, nullArrayPtr, precision, scale);
    } else {
        funcArgs = { literalArrayPtr, nullArrayPtr, literalValue, isNull, this->batchCodegenContext->rowCnt };
        llvmEngine->CallExternFunction("batch_fill_literal", { lExpr.GetReturnTypeId() }, lExpr.GetReturnTypeId(),
            funcArgs, nullptr, "fill_literal_array");
        return new CodeGenValue(literalArrayPtr, nullArrayPtr);
    }
}

llvm::Constant *BatchExpressionCodeGen::CreateConstantString(std::string s)
{
    auto charType = Type::getInt8Ty(*context);
    std::vector<llvm::Constant *> chars(s.size());
    for (unsigned int i = 0; i < s.size(); i++) {
        chars[i] = ConstantInt::get(charType, s[i]);
    }
    chars.push_back(llvm::ConstantInt::get(charType, 0));
    auto stringType = llvm::ArrayType::get(charType, chars.size());

    // Create the declaration statement
    this->numGlobalValues++;
    auto globalDeclaration = static_cast<llvm::GlobalVariable *>(
        module->getOrInsertGlobal("string" + std::to_string(this->numGlobalValues), stringType));
    globalDeclaration->setInitializer(llvm::ConstantArray::get(stringType, chars));
    globalDeclaration->setConstant(true);
    globalDeclaration->setLinkage(llvm::GlobalValue::LinkageTypes::PrivateLinkage);
    globalDeclaration->setUnnamedAddr(llvm::GlobalValue::UnnamedAddr::Global);

    // Return a cast to an i8*
    auto stringPtr = llvm::ConstantExpr::getBitCast(globalDeclaration, charType->getPointerTo());
    return stringPtr;
}

void BatchExpressionCodeGen::Visit(const FieldExpr &fExpr)
{
    Value *rowCnt = this->batchCodegenContext->rowCnt;
    Value *vecBatch = this->batchCodegenContext->data;
    Value *bitmap = this->batchCodegenContext->nullBitmap;
    Value *offsets = this->batchCodegenContext->offsets;
    Value *dictionaryVectors = this->batchCodegenContext->dictionaryVectors;
    Value *rowIdxArray = this->batchCodegenContext->rowIdxArray;

    Value *colIdx = llvmTypes->CreateConstantInt(fExpr.colVal);
    Value *gep = builder->CreateGEP(vecBatch, colIdx);

    auto dictionaryVectorGEP = builder->CreateGEP(dictionaryVectors, colIdx);
    Value *dictionaryVectorPtr = builder->CreateLoad(dictionaryVectorGEP);
    auto condition = builder->CreateIsNotNull(dictionaryVectorPtr);

    BasicBlock *trueBlock = BasicBlock::Create(*context, "DICTIONARY_NOT_NULL", func);
    BasicBlock *falseBlock = BasicBlock::Create(*context, "DICTIONARY_IS_NULL");
    BasicBlock *mergeBlock = BasicBlock::Create(*context, "field_data");

    builder->CreateCondBr(condition, trueBlock, falseBlock);

    builder->SetInsertPoint(trueBlock);
    AllocaInst *dicLengthArray = builder->CreateAlloca(llvmTypes->I32Type(), rowCnt, "dic_varchar_length");

    auto dicArrayPtr = this->GetDictionaryVectorValue(*(fExpr.GetReturnType()), rowIdxArray, rowCnt,
        dictionaryVectorPtr, dicLengthArray);

    builder->CreateBr(mergeBlock);
    trueBlock = builder->GetInsertBlock();

    func->getBasicBlockList().push_back(falseBlock);
    builder->SetInsertPoint(falseBlock);
    Value *elementAddr = builder->CreateLoad(gep);
    AllocaInst *lengthArray = builder->CreateAlloca(llvmTypes->I32Type(), rowCnt, "varchar_length");

    Value *dataArrayPtr = nullptr;

    if (TypeUtil::IsStringType(fExpr.GetReturnTypeId())) {
        auto offsetsGEP = builder->CreateGEP(offsets, colIdx);
        Value *offsetPtr = builder->CreateLoad(offsetsGEP);
        offsetPtr = builder->CreateIntToPtr(offsetPtr, llvmTypes->I32PtrType());
        dataArrayPtr =
            this->GetVectorValue(*(fExpr.GetReturnType()), rowIdxArray, rowCnt, elementAddr, offsetPtr, lengthArray);
    } else {
        dataArrayPtr =
            this->GetVectorValue(*(fExpr.GetReturnType()), rowIdxArray, rowCnt, elementAddr, nullptr, nullptr);
    }

    builder->CreateBr(mergeBlock);
    falseBlock = builder->GetInsertBlock();

    int32_t numReservedValues = 2;
    Type *phiType = llvmTypes->ToDataPointerType(fExpr.GetReturnTypeId());
    func->getBasicBlockList().push_back(mergeBlock);
    builder->SetInsertPoint(mergeBlock);

    PHINode *phiValue = builder->CreatePHI(phiType, numReservedValues, "data");
    phiValue->addIncoming(dicArrayPtr, trueBlock);
    phiValue->addIncoming(dataArrayPtr, falseBlock);

    PHINode *phiLength = nullptr;
    if (TypeUtil::IsStringType(fExpr.GetReturnTypeId())) {
        phiLength = builder->CreatePHI(llvmTypes->I32PtrType(), numReservedValues, "length");
        phiLength->addIncoming(dicLengthArray, trueBlock);
        phiLength->addIncoming(lengthArray, falseBlock);
    }

    // Get isNull value
    auto bitmapGEP = builder->CreateGEP(bitmap, colIdx);
    Value *nullArrayPtr = builder->CreateLoad(bitmapGEP);
    nullArrayPtr = builder->CreateIntToPtr(nullArrayPtr, llvmTypes->I1PtrType());
    auto nullArray = GetResultArray(OMNI_BOOLEAN, rowCnt);
    llvmEngine->CallExternFunction("batch_copy", { OMNI_BOOLEAN }, OMNI_BOOLEAN, { nullArray, nullArrayPtr, rowCnt },
        nullptr, "copy_null");

    if (TypeUtil::IsDecimalType(fExpr.GetReturnTypeId())) {
        Value *precision =
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetPrecision());
        Value *scale =
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetScale());
        this->value = make_shared<DecimalValue>(phiValue, nullArray, precision, scale);
    } else if (TypeUtil::IsStringType(fExpr.GetReturnTypeId())) {
        this->value = make_shared<CodeGenValue>(phiValue, nullArray, phiLength);
    } else {
        this->value = make_shared<CodeGenValue>(phiValue, nullArray);
    }
}

Value *BatchExpressionCodeGen::GetDictionaryVectorValue(const DataType &dataType, Value *rowIdxArray,
    llvm::Value *rowCnt, llvm::Value *dictionaryVectorPtr, AllocaInst *lengthArrayPtr)
{
    std::vector<DataTypeId> paramTypes = { OMNI_LONG };
    DataTypeId typeId = dataType.GetId();
    AllocaInst *dataArrayPtr = GetResultArray(typeId, rowCnt);
    std::vector<Value *> funcArgs { dictionaryVectorPtr, rowIdxArray, rowCnt, dataArrayPtr };

    if (TypeUtil::IsStringType(typeId)) {
        funcArgs = { batchCodegenContext->executionContext,
            dictionaryVectorPtr,
            rowIdxArray,
            rowCnt,
            dataArrayPtr,
            lengthArrayPtr };
    } else {
        funcArgs = { dictionaryVectorPtr, rowIdxArray, rowCnt, dataArrayPtr };
    }

    llvmEngine->CallExternFunction("batch_GetDic", { OMNI_LONG }, typeId, funcArgs, nullptr, "get_dictionary_value");
    return dataArrayPtr;
}

Value *BatchExpressionCodeGen::GetVectorValue(const DataType &dataType, Value *rowIdxArray, llvm::Value *rowCnt,
    llvm::Value *dataVectorPtr, Value *offsetArrayPtr, llvm::Value *lengthArrayPtr)
{
    std::vector<DataTypeId> paramTypes = { OMNI_LONG };
    DataTypeId typeId = dataType.GetId();
    AllocaInst *dataArrayPtr = GetResultArray(typeId, rowCnt);
    std::vector<Value *> funcArgs;

    if (TypeUtil::IsStringType(typeId)) {
        funcArgs = { batchCodegenContext->executionContext,
            offsetArrayPtr,
            dataVectorPtr,
            rowIdxArray,
            rowCnt,
            dataArrayPtr,
            lengthArrayPtr };
    } else {
        funcArgs = { dataVectorPtr, rowIdxArray, rowCnt, dataArrayPtr };
    }

    llvmEngine->CallExternFunction("batch_GetData", { OMNI_LONG }, typeId, funcArgs, nullptr, "get_vector_value");

    return dataArrayPtr;
}

CodeGenValuePtr CreateBatchInvalidCodeGenValue()
{
    return make_shared<CodeGenValue>(nullptr, nullptr);
}

void BatchExpressionCodeGen::Visit(const UnaryExpr &uExpr)
{
    auto val = VisitExpr(*(uExpr.exp));
    if (!val->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    switch (uExpr.op) {
        case omniruntime::expressions::Operator::NOT: {
            std::vector<Value *> funcArgs { val->data, this->batchCodegenContext->rowCnt };
            llvmEngine->CallExternFunction("batch_not", { uExpr.exp->GetReturnTypeId() }, uExpr.GetReturnTypeId(),
                funcArgs, nullptr, "logical_not");

            this->value = make_shared<CodeGenValue>(val->data, val->isNull);
            break;
        }
        default: {
            this->value = CreateBatchInvalidCodeGenValue();
            break;
        }
    }
}

void BatchExpressionCodeGen::Visit(const BinaryExpr &binaryExpr)
{
    auto *bExpr = const_cast<BinaryExpr *>(&binaryExpr);

    CodeGenValuePtr left = VisitExpr(*(bExpr->left));
    if (!left->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }
    Value *leftValue = left->data;
    Value *leftLen = left->length;
    Value *leftNull = left->isNull;

    CodeGenValuePtr right = VisitExpr(*(bExpr->right));
    if (!right->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }
    Value *rightValue = right->data;
    Value *rightLen = right->length;
    Value *rightNull = right->isNull;

    if (bExpr->op == omniruntime::expressions::Operator::AND) {
        std::vector<omniruntime::type::DataTypeId> boolParams { OMNI_BOOLEAN, OMNI_BOOLEAN };
        vector<Value *> orFuncParams { leftValue, leftNull, rightValue, rightNull, this->batchCodegenContext->rowCnt };
        llvmEngine->CallExternFunction("batch_and_expr", boolParams, OMNI_BOOLEAN, orFuncParams, nullptr, "and_expr");
        this->value = make_shared<CodeGenValue>(leftValue, leftNull);
        return;
    }

    if (bExpr->op == omniruntime::expressions::Operator::OR) {
        std::vector<omniruntime::type::DataTypeId> boolParams { OMNI_BOOLEAN, OMNI_BOOLEAN };
        vector<Value *> orFuncParams { leftValue, leftNull, rightValue, rightNull, this->batchCodegenContext->rowCnt };
        llvmEngine->CallExternFunction("batch_or_expr", boolParams, OMNI_BOOLEAN, orFuncParams, nullptr, "or_expr");
        this->value = make_shared<CodeGenValue>(leftValue, leftNull);
        return;
    }

    if (bExpr->left->GetReturnTypeId() == OMNI_INT || bExpr->left->GetReturnTypeId() == OMNI_DATE32) {
        this->BinaryExprIntHelper(bExpr, leftValue, rightValue, leftNull, rightNull);
        return;
    } else if (bExpr->left->GetReturnTypeId() == OMNI_LONG) {
        this->BinaryExprLongHelper(bExpr, leftValue, rightValue, leftNull, rightNull);
        return;
    } else if (bExpr->left->GetReturnTypeId() == OMNI_DOUBLE) {
        this->BinaryExprDoubleHelper(bExpr, leftValue, rightValue, leftNull, rightNull);
        return;
    } else if (TypeUtil::IsStringType(bExpr->left->GetReturnTypeId())) {
        this->BinaryExprStringHelper(bExpr, leftValue, leftLen, rightValue, rightLen, leftNull, rightNull);
        return;
    } else if (TypeUtil::IsDecimalType(bExpr->left->GetReturnTypeId())) {
        this->BinaryExprDecimalHelper(bExpr, static_cast<DecimalValue &>(*left.get()),
            static_cast<DecimalValue &>(*right.get()), leftNull, rightNull);
        return;
    }

    LogWarn("Unsupported binary operator %d", bExpr->op);
    this->value = CreateBatchInvalidCodeGenValue();
}

void BatchExpressionCodeGen::BinaryExprIntHelper(const omniruntime::expressions::BinaryExpr *binaryExpr,
    llvm::Value *left, llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull)
{
    std::vector<omniruntime::type::DataTypeId> intParams { OMNI_INT, OMNI_INT };
    std::vector<omniruntime::type::DataTypeId> boolParams { OMNI_BOOLEAN, OMNI_BOOLEAN };
    DataTypeId returnTypeId = binaryExpr->GetReturnTypeId();
    AllocaInst *logicalArrayPtr = nullptr;
    vector<Value *> logicalFuncParams;
    if (returnTypeId == OMNI_BOOLEAN) {
        logicalArrayPtr = builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "LOGICAL_PTR");
        logicalFuncParams = { left, right, logicalArrayPtr, this->batchCodegenContext->rowCnt };
    }
    vector<Value *> arithFuncParams { left, right, this->batchCodegenContext->rowCnt };

    vector<Value *> nullFuncParams { leftIsNull, rightIsNull, this->batchCodegenContext->rowCnt };
    llvmEngine->CallExternFunction("batch_or", boolParams, OMNI_BOOLEAN, nullFuncParams, nullptr, "either_null");

    switch (binaryExpr->op) {
        case omniruntime::expressions::Operator::LT:
            llvmEngine->CallExternFunction("batch_lessThan", intParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_lt");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::GT:
            llvmEngine->CallExternFunction("batch_greaterThan", intParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_gt");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::LTE:
            llvmEngine->CallExternFunction("batch_lessThanEqual", intParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_le");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::GTE:
            llvmEngine->CallExternFunction("batch_greaterThanEqual", intParams, OMNI_BOOLEAN, logicalFuncParams,
                nullptr, "relational_ge");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::EQ:
            llvmEngine->CallExternFunction("batch_equal", intParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_eq");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::NEQ:
            llvmEngine->CallExternFunction("batch_notEqual", intParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_neq");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::ADD:
            llvmEngine->CallExternFunction("batch_add", intParams, OMNI_INT, arithFuncParams, nullptr,
                "arithmetic_add");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::SUB:
            llvmEngine->CallExternFunction("batch_subtract", intParams, OMNI_INT, arithFuncParams, nullptr,
                "arithmetic_sub");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::MUL:
            llvmEngine->CallExternFunction("batch_multiply", intParams, OMNI_INT, arithFuncParams, nullptr,
                "arithmetic_mul");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::DIV:
            llvmEngine->CallExternFunction("batch_divide", intParams, OMNI_INT, arithFuncParams,
                batchCodegenContext->executionContext, "arithmetic_div");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::MOD:
            llvmEngine->CallExternFunction("batch_modulus", intParams, OMNI_INT, arithFuncParams,
                batchCodegenContext->executionContext, "arithmetic_mod");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        default: {
            LogError("Unsupported int binary operator %d", static_cast<uint32_t>(binaryExpr->op));
            this->value = CreateBatchInvalidCodeGenValue();
            return;
        }
    }
}

void BatchExpressionCodeGen::BinaryExprLongHelper(const omniruntime::expressions::BinaryExpr *binaryExpr,
    llvm::Value *left, llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull)
{
    std::vector<omniruntime::type::DataTypeId> longParams { OMNI_LONG, OMNI_LONG };
    std::vector<omniruntime::type::DataTypeId> boolParams { OMNI_BOOLEAN, OMNI_BOOLEAN };
    DataTypeId returnTypeId = binaryExpr->GetReturnTypeId();
    AllocaInst *logicalArrayPtr = nullptr;
    vector<Value *> logicalFuncParams;
    if (returnTypeId == OMNI_BOOLEAN) {
        logicalArrayPtr = builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "LOGICAL_PTR");
        logicalFuncParams = { left, right, logicalArrayPtr, this->batchCodegenContext->rowCnt };
    }
    vector<Value *> arithFuncParams { left, right, this->batchCodegenContext->rowCnt };

    vector<Value *> nullFuncParams { leftIsNull, rightIsNull, this->batchCodegenContext->rowCnt };
    llvmEngine->CallExternFunction("batch_or", boolParams, OMNI_BOOLEAN, nullFuncParams, nullptr, "either_null");

    switch (binaryExpr->op) {
        case omniruntime::expressions::Operator::LT:
            llvmEngine->CallExternFunction("batch_lessThan", longParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_lt");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::GT:
            llvmEngine->CallExternFunction("batch_greaterThan", longParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_gt");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::LTE:
            llvmEngine->CallExternFunction("batch_lessThanEqual", longParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_le");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::GTE:
            llvmEngine->CallExternFunction("batch_greaterThanEqual", longParams, OMNI_BOOLEAN, logicalFuncParams,
                nullptr, "relational_ge");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::EQ:
            llvmEngine->CallExternFunction("batch_equal", longParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_eq");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::NEQ:
            llvmEngine->CallExternFunction("batch_notEqual", longParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_neq");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::ADD:
            llvmEngine->CallExternFunction("batch_add", longParams, OMNI_LONG, arithFuncParams, nullptr,
                "arithmetic_add");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::SUB:
            llvmEngine->CallExternFunction("batch_subtract", longParams, OMNI_LONG, arithFuncParams, nullptr,
                "arithmetic_sub");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::MUL:
            llvmEngine->CallExternFunction("batch_multiply", longParams, OMNI_LONG, arithFuncParams, nullptr,
                "arithmetic_mul");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::DIV:
            llvmEngine->CallExternFunction("batch_divide", longParams, OMNI_LONG, arithFuncParams,
                batchCodegenContext->executionContext, "arithmetic_div");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::MOD:
            llvmEngine->CallExternFunction("batch_modulus", longParams, OMNI_LONG, arithFuncParams,
                batchCodegenContext->executionContext, "arithmetic_mod");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        default: {
            LogError("Unsupported long binary operator %d", static_cast<uint32_t>(binaryExpr->op));
            this->value = CreateBatchInvalidCodeGenValue();
            return;
        }
    }
}

void BatchExpressionCodeGen::BinaryExprDoubleHelper(const omniruntime::expressions::BinaryExpr *binaryExpr,
    llvm::Value *left, llvm::Value *right, llvm::Value *leftIsNull, llvm::Value *rightIsNull)
{
    std::vector<omniruntime::type::DataTypeId> doubleParams { OMNI_DOUBLE, OMNI_DOUBLE };
    std::vector<omniruntime::type::DataTypeId> boolParams { OMNI_BOOLEAN, OMNI_BOOLEAN };
    DataTypeId returnTypeId = binaryExpr->GetReturnTypeId();
    AllocaInst *logicalArrayPtr = nullptr;
    vector<Value *> logicalFuncParams;
    if (returnTypeId == OMNI_BOOLEAN) {
        logicalArrayPtr = builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "LOGICAL_PTR");
        logicalFuncParams = { left, right, logicalArrayPtr, this->batchCodegenContext->rowCnt };
    }
    vector<Value *> arithFuncParams { left, right, this->batchCodegenContext->rowCnt };

    vector<Value *> nullFuncParams { leftIsNull, rightIsNull, this->batchCodegenContext->rowCnt };
    llvmEngine->CallExternFunction("batch_or", boolParams, OMNI_BOOLEAN, nullFuncParams, nullptr, "either_null");

    switch (binaryExpr->op) {
        case omniruntime::expressions::Operator::LT:
            llvmEngine->CallExternFunction("batch_lessThan", doubleParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_lt");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::GT:
            llvmEngine->CallExternFunction("batch_greaterThan", doubleParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_gt");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::LTE:
            llvmEngine->CallExternFunction("batch_lessThanEqual", doubleParams, OMNI_BOOLEAN, logicalFuncParams,
                nullptr, "relational_le");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::GTE:
            llvmEngine->CallExternFunction("batch_greaterThanEqual", doubleParams, OMNI_BOOLEAN, logicalFuncParams,
                nullptr, "relational_ge");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::EQ:
            llvmEngine->CallExternFunction("batch_equal", doubleParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_eq");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::NEQ:
            llvmEngine->CallExternFunction("batch_notEqual", doubleParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_neq");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::ADD:
            llvmEngine->CallExternFunction("batch_add", doubleParams, OMNI_DOUBLE, arithFuncParams, nullptr,
                "arithmetic_add");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::SUB:
            llvmEngine->CallExternFunction("batch_subtract", doubleParams, OMNI_DOUBLE, arithFuncParams, nullptr,
                "arithmetic_sub");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::MUL:
            llvmEngine->CallExternFunction("batch_multiply", doubleParams, OMNI_DOUBLE, arithFuncParams, nullptr,
                "arithmetic_mul");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::DIV:
            llvmEngine->CallExternFunction("batch_divide", doubleParams, OMNI_DOUBLE, arithFuncParams, nullptr,
                "arithmetic_div");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        case omniruntime::expressions::Operator::MOD:
            llvmEngine->CallExternFunction("batch_modulus", doubleParams, OMNI_DOUBLE, arithFuncParams, nullptr,
                "arithmetic_mod");
            this->value = make_shared<CodeGenValue>(left, leftIsNull);
            return;
        default: {
            LogError("Unsupported double binary operator %d", static_cast<uint32_t>(binaryExpr->op));
            this->value = CreateBatchInvalidCodeGenValue();
            return;
        }
    }
}

void BatchExpressionCodeGen::BinaryExprDecimalHelper(const omniruntime::expressions::BinaryExpr *binaryExpr,
    DecimalValue &left, DecimalValue &right, llvm::Value *leftIsNull, llvm::Value *rightIsNull)
{
    //  inputType can be 64 + 128, returnType can be 64 or 128
    std::vector<DataTypeId> decimal64Params { binaryExpr->left->GetReturnTypeId(),
        binaryExpr->right->GetReturnTypeId() };
    std::vector<DataTypeId> boolParams { OMNI_BOOLEAN, OMNI_BOOLEAN };
    DataTypeId returnTypeId = binaryExpr->GetReturnTypeId();
    std::shared_ptr<DecimalValue> returnDecimalValue = nullptr;
    AllocaInst *logicalArrayPtr = nullptr;
    AllocaInst *arithArrayPtr = nullptr;
    vector<Value *> logicalFuncParams;
    vector<Value *> arithFuncParams;

    if (returnTypeId == OMNI_BOOLEAN) {
        logicalArrayPtr = builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "LOGICAL_PTR");
        logicalFuncParams = {
            left.data,       const_cast<Value *>(left.GetPrecision()),  const_cast<Value *>(left.GetScale()),
            right.data,      const_cast<Value *>(right.GetPrecision()), const_cast<Value *>(right.GetScale()),
            logicalArrayPtr, this->batchCodegenContext->rowCnt
        };
    } else if (TypeUtil::IsDecimalType(returnTypeId)) {
        returnDecimalValue = decimalIRBuilder->BuildDecimalValue(nullptr, *binaryExpr->GetReturnType(), nullptr);
        arithFuncParams = { left.data,
            const_cast<Value *>(left.GetPrecision()),
            const_cast<Value *>(left.GetScale()),
            right.data,
            const_cast<Value *>(right.GetPrecision()),
            const_cast<Value *>(right.GetScale()),
            const_cast<Value *>(returnDecimalValue->GetPrecision()),
            const_cast<Value *>(returnDecimalValue->GetScale()),
            this->batchCodegenContext->rowCnt };
        if (decimal64Params == std::vector<DataTypeId> { OMNI_DECIMAL128, OMNI_DECIMAL128 } and
            returnTypeId == OMNI_DECIMAL64) {
            arithArrayPtr = builder->CreateAlloca(llvmTypes->I64Type(), this->batchCodegenContext->rowCnt, "ARITH_PTR");
            arithFuncParams.insert(std::begin(arithFuncParams) + 6, arithArrayPtr);
        } else if (decimal64Params == std::vector<DataTypeId> { OMNI_DECIMAL64, OMNI_DECIMAL64 } and
            returnTypeId == OMNI_DECIMAL128) {
            arithArrayPtr =
                builder->CreateAlloca(llvmTypes->I128Type(), this->batchCodegenContext->rowCnt, "ARITH_PTR");
            arithFuncParams.insert(std::begin(arithFuncParams) + 6, arithArrayPtr);
        }
    }

    vector<Value *> nullFuncParams { leftIsNull, rightIsNull, this->batchCodegenContext->rowCnt };
    llvmEngine->CallExternFunction("batch_or", boolParams, OMNI_BOOLEAN, nullFuncParams, nullptr, "either_null");

    Value *falseValue = llvmTypes->CreateConstantBool(false);
    AllocaInst *overflowNull =
        builder->CreateAlloca(Type::getInt1Ty(*context), this->batchCodegenContext->rowCnt, "OVERFLOW_NULL_PTR");
    vector<Value *> funcArgs { overflowNull, falseValue, this->batchCodegenContext->rowCnt };
    auto ret = llvmEngine->CallExternFunction("batch_fill_null", { OMNI_BOOLEAN }, OMNI_BOOLEAN, funcArgs, nullptr,
        "fill_null_array");
    switch (binaryExpr->op) {
        case omniruntime::expressions::Operator::LT:
            llvmEngine->CallExternFunction("batch_lessThan", decimal64Params, returnTypeId, logicalFuncParams, nullptr,
                "relational_lt");
            break;
        case omniruntime::expressions::Operator::GT:
            llvmEngine->CallExternFunction("batch_greaterThan", decimal64Params, returnTypeId, logicalFuncParams,
                nullptr, "relational_gt");
            break;
        case omniruntime::expressions::Operator::LTE:
            llvmEngine->CallExternFunction("batch_lessThanEqual", decimal64Params, returnTypeId, logicalFuncParams,
                nullptr, "relational_le");
            break;
        case omniruntime::expressions::Operator::GTE:
            llvmEngine->CallExternFunction("batch_greaterThanEqual", decimal64Params, returnTypeId, logicalFuncParams,
                nullptr, "relational_ge");
            break;
        case omniruntime::expressions::Operator::EQ:
            llvmEngine->CallExternFunction("batch_equal", decimal64Params, returnTypeId, logicalFuncParams, nullptr,
                "relational_eq");
            break;
        case omniruntime::expressions::Operator::NEQ:
            llvmEngine->CallExternFunction("batch_notEqual", decimal64Params, returnTypeId, logicalFuncParams, nullptr,
                "relational_neq");
            break;
        case omniruntime::expressions::Operator::ADD:
            llvmEngine->CallExternFunction("batch_add", decimal64Params, returnTypeId, arithFuncParams,
                batchCodegenContext->executionContext, "arithmetic_add", this->overflowConfig, overflowNull);
            break;
        case omniruntime::expressions::Operator::SUB:
            llvmEngine->CallExternFunction("batch_subtract", decimal64Params, returnTypeId, arithFuncParams,
                batchCodegenContext->executionContext, "arithmetic_sub", this->overflowConfig, overflowNull);
            break;
        case omniruntime::expressions::Operator::MUL:
            llvmEngine->CallExternFunction("batch_multiply", decimal64Params, returnTypeId, arithFuncParams,
                batchCodegenContext->executionContext, "arithmetic_mul", this->overflowConfig, overflowNull);
            break;
        case omniruntime::expressions::Operator::DIV:
            llvmEngine->CallExternFunction("batch_divide", decimal64Params, returnTypeId, arithFuncParams,
                batchCodegenContext->executionContext, "arithmetic_div", this->overflowConfig, overflowNull);
            break;
        case omniruntime::expressions::Operator::MOD:
            llvmEngine->CallExternFunction("batch_modulus", decimal64Params, returnTypeId, arithFuncParams,
                batchCodegenContext->executionContext, "arithmetic_mod", this->overflowConfig, overflowNull);
            break;
        default: {
            LogError("Unsupported decimal64 binary operator %d", static_cast<uint32_t>(binaryExpr->op));
            this->value = CreateBatchInvalidCodeGenValue();
            return;
        }
    }

    vector<Value *> isAnyNullFuncParams { overflowNull, leftIsNull, this->batchCodegenContext->rowCnt };
    llvmEngine->CallExternFunction("batch_or", boolParams, OMNI_BOOLEAN, isAnyNullFuncParams, nullptr, "either_null");

    if (TypeUtil::IsDecimalType(returnTypeId)) {
        if (returnTypeId == omniruntime::type::OMNI_DECIMAL128) {
            llvm::Value *dataValue = nullptr;
            if (decimal64Params[0] == OMNI_DECIMAL128) {
                dataValue = left.data;
            } else if (decimal64Params[1] == OMNI_DECIMAL128) {
                dataValue = right.data;
            } else {
                dataValue = arithArrayPtr;
            }
            this->value = make_shared<DecimalValue>(dataValue, overflowNull,
                const_cast<Value *>(returnDecimalValue->GetPrecision()),
                const_cast<Value *>(returnDecimalValue->GetScale()));
        } else if (returnTypeId == omniruntime::type::OMNI_DECIMAL64) {
            llvm::Value *dataValue = nullptr;
            if (decimal64Params[0] == OMNI_DECIMAL64) {
                dataValue = left.data;
            } else if (decimal64Params[1] == OMNI_DECIMAL64) {
                dataValue = right.data;
            } else {
                dataValue = arithArrayPtr;
            }
            this->value = make_shared<DecimalValue>(dataValue, overflowNull,
                const_cast<Value *>(returnDecimalValue->GetPrecision()),
                const_cast<Value *>(returnDecimalValue->GetScale()));
        } else {
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, overflowNull);
        }
    }
}

void BatchExpressionCodeGen::BinaryExprStringHelper(const omniruntime::expressions::BinaryExpr *binaryExpr,
    llvm::Value *left, llvm::Value *leftLen, llvm::Value *right, llvm::Value *rightLen, llvm::Value *leftIsNull,
    llvm::Value *rightIsNull)
{
    std::vector<omniruntime::type::DataTypeId> strParams { OMNI_VARCHAR, OMNI_VARCHAR };
    std::vector<omniruntime::type::DataTypeId> boolParams { OMNI_BOOLEAN, OMNI_BOOLEAN };
    auto logicalArrayPtr = builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "LOGICAL_PTR");
    vector<Value *> logicalFuncParams { left,     leftLen,         right,
        rightLen, logicalArrayPtr, this->batchCodegenContext->rowCnt };

    vector<Value *> nullFuncParams { leftIsNull, rightIsNull, this->batchCodegenContext->rowCnt };
    llvmEngine->CallExternFunction("batch_or", boolParams, OMNI_BOOLEAN, nullFuncParams, nullptr, "either_null");

    switch (binaryExpr->op) {
        case omniruntime::expressions::Operator::LT:
            llvmEngine->CallExternFunction("batch_lessThan", strParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_lt");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::GT:
            llvmEngine->CallExternFunction("batch_greaterThan", strParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_gt");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::LTE:
            llvmEngine->CallExternFunction("batch_lessThanEqual", strParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_le");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::GTE:
            llvmEngine->CallExternFunction("batch_greaterThanEqual", strParams, OMNI_BOOLEAN, logicalFuncParams,
                nullptr, "relational_ge");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::EQ:
            llvmEngine->CallExternFunction("batch_equal", strParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_eq");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        case omniruntime::expressions::Operator::NEQ:
            llvmEngine->CallExternFunction("batch_notEqual", strParams, OMNI_BOOLEAN, logicalFuncParams, nullptr,
                "relational_neq");
            this->value = make_shared<CodeGenValue>(logicalArrayPtr, leftIsNull);
            return;
        default: {
            LogError("Unsupported double binary operator %d", static_cast<uint32_t>(binaryExpr->op));
            this->value = CreateBatchInvalidCodeGenValue();
            return;
        }
    }
}

void BatchExpressionCodeGen::Visit(const BetweenExpr &btExpr)
{
    auto bExpr = const_cast<BetweenExpr *>(&btExpr);

    DataTypeId valueTypeId = bExpr->value->GetReturnTypeId();
    if (AreInvalidDataTypes(valueTypeId, bExpr->lowerBound->GetReturnTypeId()) &&
        AreInvalidDataTypes(valueTypeId, bExpr->upperBound->GetReturnTypeId())) {
        LogError("Value, lower bound, and upper bound must have the same type");
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    auto val = VisitExpr(*(bExpr->value));
    if (!val->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    auto lowerVal = VisitExpr(*(bExpr->lowerBound));
    if (!lowerVal->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    auto upperVal = VisitExpr(*(bExpr->upperBound));
    if (!upperVal->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    AllocaInst *cmpLeft = builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "cmpLeft");
    AllocaInst *cmpRight = builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "cmpRight");
    std::pair<AllocaInst **, AllocaInst **> cmpPair = std::make_pair(&cmpLeft, &cmpRight);

    bool supportedType = VisitBetweenExprHelper(*bExpr, val, lowerVal, upperVal, cmpPair);
    if (supportedType) {
        std::vector<omniruntime::type::DataTypeId> boolParams { OMNI_BOOLEAN, OMNI_BOOLEAN };
        vector<Value *> nullFuncParams { lowerVal->isNull, val->isNull, this->batchCodegenContext->rowCnt };
        llvmEngine->CallExternFunction("batch_or", boolParams, OMNI_BOOLEAN, nullFuncParams, nullptr, "either_null");
        nullFuncParams = { lowerVal->isNull, upperVal->isNull, this->batchCodegenContext->rowCnt };
        llvmEngine->CallExternFunction("batch_or", boolParams, OMNI_BOOLEAN, nullFuncParams, nullptr, "either_null");

        vector<Value *> betweenFuncParams = { cmpLeft, cmpRight, this->batchCodegenContext->rowCnt };
        llvmEngine->CallExternFunction("batch_and", boolParams, OMNI_BOOLEAN, betweenFuncParams, nullptr,
            "between_pass");
        this->value = make_shared<CodeGenValue>(cmpLeft, lowerVal->isNull);
        return;
    }

    LogError("Unsupported data type for between %d", valueTypeId);
    this->value = CreateBatchInvalidCodeGenValue();
}

bool BatchExpressionCodeGen::VisitBetweenExprHelper(BetweenExpr &bExpr, const shared_ptr<CodeGenValue> &val,
    const shared_ptr<CodeGenValue> &lowerVal, const shared_ptr<CodeGenValue> &upperVal,
    std::pair<AllocaInst **, AllocaInst **> cmpPair)
{
    auto cmpLeft = cmpPair.first;
    auto cmpRight = cmpPair.second;
    std::vector<omniruntime::type::DataTypeId> params { bExpr.value->GetReturnTypeId(),
        bExpr.value->GetReturnTypeId() };
    std::vector<Value *> logicalFuncParams1 { lowerVal->data, val->data, *cmpLeft, this->batchCodegenContext->rowCnt };
    std::vector<Value *> logicalFuncParams2 { val->data, upperVal->data, *cmpRight, this->batchCodegenContext->rowCnt };

    if (bExpr.value->GetReturnTypeId() == OMNI_INT || bExpr.value->GetReturnTypeId() == OMNI_LONG ||
        bExpr.value->GetReturnTypeId() == OMNI_DATE32 || bExpr.value->GetReturnTypeId() == OMNI_DOUBLE) {
        llvmEngine->CallExternFunction("batch_lessThanEqual", params, OMNI_BOOLEAN, logicalFuncParams1, nullptr,
            "relational_le");
        llvmEngine->CallExternFunction("batch_lessThanEqual", params, OMNI_BOOLEAN, logicalFuncParams2, nullptr,
            "relational_le");
        return true;
    } else if (TypeUtil::IsStringType(bExpr.value->GetReturnTypeId())) {
        llvmEngine->CallExternFunction("batch_lessThanEqual", params, OMNI_BOOLEAN, logicalFuncParams1, nullptr,
            "relational_le");
        llvmEngine->CallExternFunction("batch_lessThanEqual", params, OMNI_BOOLEAN, logicalFuncParams2, nullptr,
            "relational_le");
        return true;
    } else if (TypeUtil::IsDecimalType(bExpr.value->GetReturnTypeId())) {
        logicalFuncParams1 = { lowerVal->data,
            const_cast<Value *>(static_cast<DecimalValue &>(*lowerVal).GetPrecision()),
            const_cast<Value *>(static_cast<DecimalValue &>(*lowerVal).GetScale()),
            val->data,
            const_cast<Value *>(static_cast<DecimalValue &>(*val).GetPrecision()),
            const_cast<Value *>(static_cast<DecimalValue &>(*val).GetScale()),
            *cmpLeft,
            this->batchCodegenContext->rowCnt };
        logicalFuncParams2 = { val->data,
            const_cast<Value *>(static_cast<DecimalValue &>(*val).GetPrecision()),
            const_cast<Value *>(static_cast<DecimalValue &>(*val).GetScale()),
            upperVal->data,
            const_cast<Value *>(static_cast<DecimalValue &>(*upperVal).GetPrecision()),
            const_cast<Value *>(static_cast<DecimalValue &>(*upperVal).GetScale()),
            *cmpRight,
            this->batchCodegenContext->rowCnt };

        llvmEngine->CallExternFunction("batch_lessThanEqual", params, OMNI_BOOLEAN, logicalFuncParams1, nullptr,
            "relational_le");
        llvmEngine->CallExternFunction("batch_lessThanEqual", params, OMNI_BOOLEAN, logicalFuncParams2, nullptr,
            "relational_le");
        return true;
    }
    return false;
}

void BatchExpressionCodeGen::Visit(const IsNullExpr &isNullExpr)
{
    Expr *valueExpr = isNullExpr.value;
    auto isNullValue = VisitExpr(*valueExpr);
    if (!isNullValue->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    std::vector<Value *> funcArgs { isNullValue->isNull, llvmTypes->CreateConstantBool(true),
        this->batchCodegenContext->rowCnt };
    llvmEngine->CallExternFunction("batch_equal", { OMNI_BOOLEAN, OMNI_BOOLEAN }, OMNI_BOOLEAN, funcArgs, nullptr,
        "is_null");

    AllocaInst *nullArrayPtr =
        builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "IS_NULL_PTR");
    funcArgs = { nullArrayPtr, llvmTypes->CreateConstantBool(false), this->batchCodegenContext->rowCnt };
    llvmEngine->CallExternFunction("batch_fill_null", { OMNI_BOOLEAN }, OMNI_BOOLEAN, funcArgs, nullptr,
        "batch_fill_null");

    this->value = make_shared<CodeGenValue>(isNullValue->isNull, nullArrayPtr);
}


std::vector<llvm::Value *> BatchExpressionCodeGen::GetDefaultFunctionArgValues(const FuncExpr &fExpr,
    AllocaInst *isAnyNull, bool &isInvalidExpr)
{
    std::vector<Value *> argVals;
    CodeGenValuePtr resultPtr;
    int numArgs = fExpr.arguments.size();
    vector<Value *> nullFuncParams;

    if (fExpr.function->IsExecutionContextSet()) {
        argVals.push_back(this->batchCodegenContext->executionContext);
    }
    for (int i = 0; i < numArgs; i++) {
        Expr *argN = fExpr.arguments[i];
        resultPtr = VisitExpr(*argN);
        if (!resultPtr->IsValidValue()) {
            isInvalidExpr = true;
            return argVals;
        }
        argVals.push_back(resultPtr->data);

        nullFuncParams = { isAnyNull, resultPtr->isNull, this->batchCodegenContext->rowCnt };
        llvmEngine->CallExternFunction("batch_or", { OMNI_BOOLEAN, OMNI_BOOLEAN }, OMNI_BOOLEAN, nullFuncParams,
            nullptr, "either_null");

        if ((TypeUtil::IsStringType(fExpr.arguments[i]->GetReturnTypeId()))) {
            if (fExpr.arguments[i]->GetReturnTypeId() == OMNI_CHAR) {
                argVals.push_back(llvmTypes->CreateConstantInt(
                    static_cast<CharDataType *>(fExpr.arguments[i]->GetReturnType().get())->GetWidth()));
            }
            argVals.push_back(this->value->length);
        }
        if (TypeUtil::IsDecimalType(argN->GetReturnTypeId())) {
            argVals.push_back(llvmTypes->CreateConstantInt(
                static_cast<DecimalDataType *>(fExpr.arguments[i]->GetReturnType().get())->GetPrecision()));
            argVals.push_back(llvmTypes->CreateConstantInt(
                static_cast<DecimalDataType *>(fExpr.arguments[i]->GetReturnType().get())->GetScale()));
        }
    }
    return argVals;
}

std::vector<llvm::Value *> BatchExpressionCodeGen::GetDataArgs(const omniruntime::expressions::FuncExpr &fExpr,
    AllocaInst *isAnyNull, bool &isInvalidExpr)
{
    return GetDefaultFunctionArgValues(fExpr, isAnyNull, isInvalidExpr);
}

std::vector<llvm::Value *> BatchExpressionCodeGen::GetDataAndNullArgs(const FuncExpr &fExpr, AllocaInst *isAnyNull,
    bool &isInvalidExpr)
{
    std::vector<Value *> argVals;
    CodeGenValuePtr resultPtr;
    int numArgs = fExpr.arguments.size();
    vector<Value *> nullFuncParams;

    if (fExpr.function->IsExecutionContextSet()) {
        argVals.push_back(this->batchCodegenContext->executionContext);
    }
    for (int i = 0; i < numArgs; i++) {
        Expr *argN = fExpr.arguments[i];
        resultPtr = VisitExpr(*argN);
        if (!resultPtr->IsValidValue()) {
            isInvalidExpr = true;
            return argVals;
        }
        argVals.push_back(resultPtr->data);

        nullFuncParams = { isAnyNull, resultPtr->isNull, this->batchCodegenContext->rowCnt };
        llvmEngine->CallExternFunction("batch_or", { OMNI_BOOLEAN, OMNI_BOOLEAN }, OMNI_BOOLEAN, nullFuncParams,
            nullptr, "either_null");

        if ((TypeUtil::IsStringType(fExpr.arguments[i]->GetReturnTypeId()))) {
            if (fExpr.arguments[i]->GetReturnTypeId() == OMNI_CHAR) {
                argVals.push_back(llvmTypes->CreateConstantInt(
                    static_cast<CharDataType *>(fExpr.arguments[i]->GetReturnType().get())->GetWidth()));
            }
            argVals.push_back(this->value->length);
        }
        if (TypeUtil::IsDecimalType(argN->GetReturnTypeId())) {
            argVals.push_back(llvmTypes->CreateConstantInt(
                static_cast<DecimalDataType *>(fExpr.arguments[i]->GetReturnType().get())->GetPrecision()));
            argVals.push_back(llvmTypes->CreateConstantInt(
                static_cast<DecimalDataType *>(fExpr.arguments[i]->GetReturnType().get())->GetScale()));
        }
        argVals.push_back(this->value->isNull);
    }
    return argVals;
}

std::vector<llvm::Value *> BatchExpressionCodeGen::GetFunctionArgValues(const omniruntime::expressions::FuncExpr &fExpr,
    AllocaInst *isAnyNull, bool &isInvalidExpr)
{
    switch (fExpr.function->GetNullableResultType()) {
        case INPUT_DATA:
            return GetDataArgs(fExpr, isAnyNull, isInvalidExpr);
        case INPUT_DATA_AND_NULL:
            return GetDataAndNullArgs(fExpr, isAnyNull, isInvalidExpr);
        default:
            return GetDefaultFunctionArgValues(fExpr, isAnyNull, isInvalidExpr);
    }
}

llvm::AllocaInst *BatchExpressionCodeGen::GetResultArray(omniruntime::type::DataTypeId dataTypeId, Value *rowCnt)
{
    AllocaInst *resultArray = nullptr;
    switch (dataTypeId) {
        case OMNI_INT:
        case OMNI_DATE32: {
            resultArray = builder->CreateAlloca(llvmTypes->I32Type(), rowCnt, "DATA_PTR");
            break;
        }
        case OMNI_DECIMAL64:
        case OMNI_LONG: {
            resultArray = builder->CreateAlloca(llvmTypes->I64Type(), rowCnt, "DATA_PTR");
            break;
        }
        case OMNI_DOUBLE: {
            resultArray = builder->CreateAlloca(llvmTypes->DoubleType(), rowCnt, "DATA_PTR");
            break;
        }
        case OMNI_CHAR:
        case OMNI_VARCHAR: {
            resultArray = builder->CreateAlloca(llvmTypes->I8PtrType(), rowCnt, "DATA_PTR");
            break;
        }
        case OMNI_BOOLEAN: {
            resultArray = builder->CreateAlloca(llvmTypes->I1Type(), rowCnt, "DATA_PTR");
            break;
        }
        case OMNI_DECIMAL128: {
            resultArray = builder->CreateAlloca(llvmTypes->I128Type(), rowCnt, "DATA_PTR");
            break;
        }
        default: {
            LogWarn("Unsupported return type when creating array %d", dataTypeId);
        }
    }
    return resultArray;
}

string BatchChangeFuncNameToNull(string signature)
{
    size_t pos = signature.find_first_of('_');
    return signature.insert(pos, "_null");
}

std::vector<llvm::Value *> BatchExpressionCodeGen::GetDataAndOverflowNullArgs(
    const omniruntime::expressions::FuncExpr &fExpr, AllocaInst *isAnyNull, bool &isInvalidExpr,
    AllocaInst *overflowNull)
{
    std::vector<Value *> argVals;
    argVals.push_back(overflowNull);
    CodeGenValuePtr resultPtr;
    int numArgs = fExpr.arguments.size();
    vector<Value *> nullFuncParams;

    auto signature = fExpr.function->GetSignatures()[0];
    if (FunctionRegistry::IsNullExecutionContextSet(&signature)) {
        argVals.push_back(this->batchCodegenContext->executionContext);
    }
    for (int i = 0; i < numArgs; i++) {
        Expr *argN = fExpr.arguments[i];
        resultPtr = VisitExpr(*argN);
        if (!resultPtr->IsValidValue()) {
            isInvalidExpr = true;
            return argVals;
        }
        argVals.push_back(resultPtr->data);

        nullFuncParams = { isAnyNull, resultPtr->isNull, this->batchCodegenContext->rowCnt };
        llvmEngine->CallExternFunction("batch_or", { OMNI_BOOLEAN, OMNI_BOOLEAN }, OMNI_BOOLEAN, nullFuncParams,
            nullptr, "either_null");

        if ((TypeUtil::IsStringType(fExpr.arguments[i]->GetReturnTypeId()))) {
            if (fExpr.arguments[i]->GetReturnTypeId() == OMNI_CHAR) {
                argVals.push_back(llvmTypes->CreateConstantInt(
                    static_cast<CharDataType *>(fExpr.arguments[i]->GetReturnType().get())->GetWidth()));
            }
            argVals.push_back(this->value->length);
        }
        if (TypeUtil::IsDecimalType(argN->GetReturnTypeId())) {
            argVals.push_back(llvmTypes->CreateConstantInt(
                static_cast<DecimalDataType *>(fExpr.arguments[i]->GetReturnType().get())->GetPrecision()));
            argVals.push_back(llvmTypes->CreateConstantInt(
                static_cast<DecimalDataType *>(fExpr.arguments[i]->GetReturnType().get())->GetScale()));
        }
    }
    return argVals;
}

void BatchExpressionCodeGen::FuncExprOverflowNullHelper(const FuncExpr &fExpr)
{
    Value *falseValue = llvmTypes->CreateConstantBool(false);
    AllocaInst *isAnyNull =
        builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "IS_NULL_PTR");
    vector<Value *> funcArgs { isAnyNull, falseValue, this->batchCodegenContext->rowCnt };
    auto ret = llvmEngine->CallExternFunction("batch_fill_null", { OMNI_BOOLEAN }, OMNI_BOOLEAN, funcArgs, nullptr,
        "fill_null_array");
    AllocaInst *overflowNull =
        builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "OVERFLOW_NULL_PTR");
    funcArgs = { overflowNull, falseValue, this->batchCodegenContext->rowCnt };
    llvmEngine->CallExternFunction("batch_fill_null", { OMNI_BOOLEAN }, OMNI_BOOLEAN, funcArgs, nullptr,
        "fill_overflow_null_array");

    DataTypeId funcRetType = fExpr.GetReturnTypeId();
    bool isInvalidExpr = false;

    auto argVals = GetDataAndOverflowNullArgs(fExpr, isAnyNull, isInvalidExpr, overflowNull);
    if (isInvalidExpr) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    AllocaInst *resultArray = GetResultArray(funcRetType, this->batchCodegenContext->rowCnt);
    argVals.push_back(resultArray);
    AllocaInst *outputLenPtr = nullptr;

    if (TypeUtil::IsStringType(funcRetType)) {
        outputLenPtr = builder->CreateAlloca(llvmTypes->I32Type(), this->batchCodegenContext->rowCnt, "output_len");
        argVals.push_back(outputLenPtr);
    } else if (TypeUtil::IsDecimalType(funcRetType)) {
        argVals.push_back(
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetPrecision()));
        argVals.push_back(
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetScale()));
    }
    argVals.push_back(this->batchCodegenContext->rowCnt);

    auto f = module->getFunction("batch_" + BatchChangeFuncNameToNull(fExpr.function->GetId()));
    if (f) {
        ret = llvmEngine->CreateCall(f, argVals, fExpr.function->GetId());
        InlineFunctionInfo inlineFunctionInfo;
        llvm::InlineFunction(*((CallInst *)ret), inlineFunctionInfo);
    } else {
        LogWarn("Unable to generate function : %s", fExpr.funcName.c_str());
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    llvmEngine->CallExternFunction("batch_or", { OMNI_BOOLEAN, OMNI_BOOLEAN }, OMNI_BOOLEAN,
        { isAnyNull, overflowNull, this->batchCodegenContext->rowCnt }, nullptr);

    if (TypeUtil::IsDecimalType(funcRetType)) {
        this->value = make_shared<DecimalValue>(resultArray, isAnyNull,
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetPrecision()),
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetScale()));
    } else {
        this->value = make_shared<CodeGenValue>(resultArray, isAnyNull, outputLenPtr);
    }
}

void BatchExpressionCodeGen::Visit(const FuncExpr &fExpr)
{
    if (this->overflowConfig != nullptr &&
        this->overflowConfig->getOverflowConfigId() == omniruntime::op::OVERFLOW_CONFIG_NULL) {
        auto signature = fExpr.function->GetSignatures()[0];
        if (FunctionRegistry::LookupNullFunction(&signature)) {
            FuncExprOverflowNullHelper(fExpr);
            return;
        }
    }

    Value *falseValue = llvmTypes->CreateConstantBool(false);
    AllocaInst *isAnyNull =
        builder->CreateAlloca(llvmTypes->I1Type(), this->batchCodegenContext->rowCnt, "IS_NULL_PTR");
    vector<Value *> funcArgs { isAnyNull, falseValue, this->batchCodegenContext->rowCnt };
    llvmEngine->CallExternFunction("batch_fill_null", { OMNI_BOOLEAN }, OMNI_BOOLEAN, funcArgs, nullptr,
        "fill_null_array");

    DataTypeId funcRetType = fExpr.GetReturnTypeId();
    bool isInvalidExpr = false;

    auto argVals = GetFunctionArgValues(fExpr, isAnyNull, isInvalidExpr);
    if (isInvalidExpr) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    AllocaInst *resultArray = GetResultArray(funcRetType, this->batchCodegenContext->rowCnt);
    argVals.push_back(isAnyNull);
    argVals.push_back(resultArray);
    AllocaInst *outputLenPtr = nullptr;

    if (TypeUtil::IsStringType(funcRetType)) {
        outputLenPtr = builder->CreateAlloca(llvmTypes->I32Type(), this->batchCodegenContext->rowCnt, "output_len");
        argVals.push_back(outputLenPtr);
    } else if (TypeUtil::IsDecimalType(funcRetType)) {
        argVals.push_back(
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetPrecision()));
        argVals.push_back(
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetScale()));
    }
    argVals.push_back(this->batchCodegenContext->rowCnt);

    auto f = module->getFunction("batch_" + fExpr.function->GetId());
    if (f) {
        auto ret = llvmEngine->CreateCall(f, argVals, fExpr.function->GetId());
        InlineFunctionInfo inlineFunctionInfo;
        llvm::InlineFunction(*((CallInst *)ret), inlineFunctionInfo);
    } else {
        LogWarn("Unable to generate function : %s", fExpr.funcName.c_str());
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    if (TypeUtil::IsDecimalType(funcRetType)) {
        this->value = make_shared<DecimalValue>(resultArray, isAnyNull,
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetPrecision()),
            llvmTypes->CreateConstantInt(static_cast<DecimalDataType *>(fExpr.GetReturnType().get())->GetScale()));
    } else {
        this->value = make_shared<CodeGenValue>(resultArray, isAnyNull, outputLenPtr);
    }
}

bool BatchExpressionCodeGen::AreInvalidDataTypes(omniruntime::type::DataTypeId type1,
    omniruntime::type::DataTypeId type2)
{
    return type1 != type2 && !(TypeUtil::IsStringType(type1) && TypeUtil::IsStringType(type2));
}

void BatchExpressionCodeGen::Visit(const IfExpr &ifExpr)
{
    Expr *cond = ifExpr.condition;
    Expr *ifTrue = ifExpr.trueExpr;
    Expr *ifFalse = ifExpr.falseExpr;

    auto baseType = ifExpr.GetReturnTypeId();

    CodeGenValuePtr evCond = VisitExpr(*cond);
    if (!evCond->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    auto evTrue = VisitExpr(*ifTrue);
    if (!evTrue->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }
    Value *evTrueValue = evTrue->data;
    Value *evTrueLength = evTrue->length;
    Value *evTrueNull = evTrue->isNull;

    auto evFalse = VisitExpr(*ifFalse);
    if (!evFalse->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }
    Value *evFalseValue = evFalse->data;
    Value *evFalseLength = evFalse->length;
    Value *evFalseNull = evFalse->isNull;

    switch (baseType) {
        case OMNI_INT:
        case OMNI_DATE32:
        case OMNI_LONG:
        case OMNI_DOUBLE:
        case OMNI_BOOLEAN: {
            llvmEngine->CallExternFunction("batch_if", { baseType }, baseType,
                { evCond->data, evCond->isNull, evTrueValue, evTrueNull, evFalseValue, evFalseNull,
                this->batchCodegenContext->rowCnt },
                nullptr);
            this->value = make_shared<CodeGenValue>(evTrueValue, evTrueNull);
            return;
        }
        case OMNI_DECIMAL64:
        case OMNI_DECIMAL128: {
            DecimalValue &left = static_cast<DecimalValue &>(*evTrue);
            DecimalValue &right = static_cast<DecimalValue &>(*evFalse);

            std::vector<Value *> argValsCmp { evCond->data,
                evCond->isNull,
                left.data,
                left.isNull,
                right.data,
                right.isNull,
                this->batchCodegenContext->rowCnt };

            llvmEngine->CallExternFunction("batch_if", { baseType }, baseType, argValsCmp, nullptr);
            this->value = make_shared<DecimalValue>(left.data, left.isNull, const_cast<Value *>(left.GetPrecision()),
                const_cast<Value *>(left.GetScale()));
            return;
        }
        case OMNI_CHAR:
        case OMNI_VARCHAR: {
            llvmEngine->CallExternFunction("batch_if", { baseType }, baseType,
                { evCond->data, evCond->isNull, evTrueValue, evTrueNull, evTrueLength, evFalseValue, evFalseNull,
                evFalseLength, this->batchCodegenContext->rowCnt },
                nullptr);
            this->value = make_shared<CodeGenValue>(evTrueValue, evTrueNull, evTrueLength);
            return;
        }
        default: {
            LogWarn("Unsupported data type in IF expr %d", baseType);
            this->value = CreateBatchInvalidCodeGenValue();
            return;
        }
    }
}

void BatchExpressionCodeGen::Visit(const CoalesceExpr &cExpr)
{
    Expr *value1Expr = cExpr.value1;
    Expr *value2Expr = cExpr.value2;
    CodeGenValuePtr value1 = VisitExpr(*value1Expr);
    if (!value1->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }
    auto value2 = VisitExpr(*value2Expr);
    if (!value2->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    if (cExpr.GetReturnTypeId() == OMNI_INT || cExpr.GetReturnTypeId() == OMNI_LONG ||
        cExpr.GetReturnTypeId() == OMNI_DOUBLE || cExpr.GetReturnTypeId() == OMNI_DATE32) {
        llvmEngine->CallExternFunction("batch_coalesce", { cExpr.GetReturnTypeId(), cExpr.GetReturnTypeId() },
            cExpr.GetReturnTypeId(),
            { value1->data, value1->isNull, value2->data, value2->isNull, this->batchCodegenContext->rowCnt }, nullptr);
        this->value = make_shared<CodeGenValue>(value1->data, value1->isNull);
    } else if (TypeUtil::IsStringType(cExpr.GetReturnTypeId())) {
        llvmEngine->CallExternFunction("batch_coalesce", { cExpr.GetReturnTypeId(), cExpr.GetReturnTypeId() },
            cExpr.GetReturnTypeId(),
            { value1->data, value1->isNull, value1->length, value2->data, value2->isNull, value2->length,
            this->batchCodegenContext->rowCnt },
            nullptr);
        this->value = make_shared<CodeGenValue>(value1->data, value1->isNull, value1->length);
    } else if (TypeUtil::IsDecimalType(cExpr.GetReturnTypeId())) {
        CodeGenValue &valueDecimal1 = *value1.get();
        auto value1Precision = (Value *)static_cast<DecimalValue &>(valueDecimal1).GetPrecision();
        auto value1Scale = (Value *)static_cast<DecimalValue &>(valueDecimal1).GetScale();

        llvmEngine->CallExternFunction("batch_coalesce", { cExpr.GetReturnTypeId(), cExpr.GetReturnTypeId() },
            cExpr.GetReturnTypeId(),
            { value1->data, value1->isNull, value2->data, value2->isNull, this->batchCodegenContext->rowCnt }, nullptr);
        this->value = make_shared<DecimalValue>(value1->data, value1->isNull, value1Precision, value1Scale);
    } else {
        LogWarn("Unsupported data type in COALESCE expr %d", cExpr.GetReturnTypeId());
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }
}

void BatchExpressionCodeGen::Visit(const InExpr &inExpr)
{
    auto iExpr = const_cast<InExpr *>(&inExpr);
    Expr *toCompare = iExpr->arguments[0];
    auto baseType = iExpr->arguments[0]->GetReturnTypeId();
    int32_t size = iExpr->arguments.size();

    auto valueToCompare = VisitExpr(*toCompare);
    if (!valueToCompare->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    Value *inArray = GetResultArray(OMNI_BOOLEAN, this->batchCodegenContext->rowCnt);
    Value *isNull = GetResultArray(OMNI_BOOLEAN, this->batchCodegenContext->rowCnt);

    vector<CodeGenValuePtr> cmps(size - 1);
    for (int i = 1; i < size; ++i) {
        Expr *cmp = iExpr->arguments[i];
        cmps[i - 1] = VisitExpr(*cmp);
        if (!cmps[i - 1]->IsValidValue()) {
            this->value = CreateBatchInvalidCodeGenValue();
            return;
        }
    }

    auto cmpValues = GetResultArray(OMNI_LONG, llvmTypes->CreateConstantInt(size - 1));
    auto cmpBools = GetResultArray(OMNI_LONG, llvmTypes->CreateConstantInt(size - 1));

    for (int i = 0; i < size - 1; ++i) {
        Value *gep = builder->CreateGEP(cmpValues, llvmTypes->CreateConstantInt(i), "cmp_value_address");
        builder->CreateStore(cmps[i]->data, gep);

        gep = builder->CreateGEP(cmpBools, llvmTypes->CreateConstantInt(i), "cmp_null_address");
        builder->CreateStore(cmps[i]->isNull, gep);
    }

    vector<Value *> args;
    switch (baseType) {
        case OMNI_BOOLEAN:
        case OMNI_INT:
        case OMNI_DATE32:
        case OMNI_LONG:
        case OMNI_DOUBLE:
        case OMNI_DECIMAL64:
        case OMNI_DECIMAL128: {
            args = { llvmTypes->CreateConstantInt(size - 1),
                cmpValues,
                cmpBools,
                valueToCompare->data,
                valueToCompare->isNull,
                inArray,
                isNull,
                this->batchCodegenContext->rowCnt };
            llvmEngine->CallExternFunction("batch_in", { baseType }, OMNI_BOOLEAN, args, nullptr);
            break;
        }
        case OMNI_CHAR:
        case OMNI_VARCHAR: {
            auto cmpLengths = GetResultArray(OMNI_LONG, llvmTypes->CreateConstantInt(size - 1));
            for (int i = 0; i < size - 1; ++i) {
                Value *gep = builder->CreateGEP(cmpLengths, llvmTypes->CreateConstantInt(i), "cmp_length_address");
                builder->CreateStore(cmps[i]->length, gep);
            }
            args = { llvmTypes->CreateConstantInt(size - 1),
                cmpValues,
                cmpBools,
                cmpLengths,
                valueToCompare->data,
                valueToCompare->isNull,
                valueToCompare->length,
                inArray,
                isNull,
                this->batchCodegenContext->rowCnt };
            llvmEngine->CallExternFunction("batch_in", { baseType }, OMNI_BOOLEAN, args, nullptr);
            break;
        }
        default: {
            LogWarn("Unsupported data type in IN expr %d", baseType);
            this->value = CreateBatchInvalidCodeGenValue();
            return;
        }
    }

    this->value = make_shared<CodeGenValue>(inArray, isNull);
}

void BatchExpressionCodeGen::Visit(const SwitchExpr &switchExpr)
{
    auto switchDataType = switchExpr.GetReturnTypeId();
    Expr *elseExpr = switchExpr.falseExpr;
    std::vector<std::pair<Expr *, Expr *>> whenClause = switchExpr.whenClause;
    const int size = whenClause.size();

    AllocaInst *finalValue = GetResultArray(switchDataType, this->batchCodegenContext->rowCnt);
    AllocaInst *finalNull = GetResultArray(OMNI_BOOLEAN, this->batchCodegenContext->rowCnt);

    vector<CodeGenValuePtr> conditions(size);
    vector<CodeGenValuePtr> results(size);

    for (int i = 0; i < size; ++i) {
        Expr *cond = whenClause[i].first;
        Expr *resExpr = whenClause[i].second;
        conditions[i] = VisitExpr(*cond);
        if (!conditions[i]->IsValidValue()) {
            this->value = CreateBatchInvalidCodeGenValue();
            return;
        }
        results[i] = VisitExpr(*resExpr);
        if (!results[i]->IsValidValue()) {
            this->value = CreateBatchInvalidCodeGenValue();
            return;
        }
    }
    auto whenClauses = GetResultArray(OMNI_LONG, llvmTypes->CreateConstantInt(size));
    auto whenBools = GetResultArray(OMNI_LONG, llvmTypes->CreateConstantInt(size));
    auto resultValues = GetResultArray(OMNI_LONG, llvmTypes->CreateConstantInt(size));
    auto resultNulls = GetResultArray(OMNI_LONG, llvmTypes->CreateConstantInt(size));
    auto resultLengths = GetResultArray(OMNI_LONG, llvmTypes->CreateConstantInt(size));

    for (int i = 0; i < size; ++i) {
        Value *gep = builder->CreateGEP(whenClauses, llvmTypes->CreateConstantInt(i), "when_value_address");
        builder->CreateStore(conditions[i]->data, gep);

        gep = builder->CreateGEP(whenBools, llvmTypes->CreateConstantInt(i), "when_null_address");
        builder->CreateStore(conditions[i]->isNull, gep);

        gep = builder->CreateGEP(resultValues, llvmTypes->CreateConstantInt(i), "result_value_address");
        builder->CreateStore(results[i]->data, gep);

        gep = builder->CreateGEP(resultNulls, llvmTypes->CreateConstantInt(i), "result_null_address");
        builder->CreateStore(results[i]->isNull, gep);

        gep = builder->CreateGEP(resultLengths, llvmTypes->CreateConstantInt(i), "result_length_address");
        builder->CreateStore(results[i]->length, gep);
    }

    auto evFalse = VisitExpr(*elseExpr);
    if (!evFalse->IsValidValue()) {
        this->value = CreateBatchInvalidCodeGenValue();
        return;
    }

    vector<Value *> args;
    switch (switchDataType) {
        case OMNI_INT:
        case OMNI_BOOLEAN:
        case OMNI_DATE32:
        case OMNI_LONG:
        case OMNI_DOUBLE: {
            args = { llvmTypes->CreateConstantInt(size),
                whenClauses,
                whenBools,
                resultValues,
                resultNulls,
                evFalse->data,
                evFalse->isNull,
                finalValue,
                finalNull,
                this->batchCodegenContext->rowCnt };
            llvmEngine->CallExternFunction("batch_switch", { switchDataType }, switchDataType, args, nullptr);
            this->value = make_shared<CodeGenValue>(finalValue, finalNull);
            return;
        }
        case OMNI_DECIMAL64:
        case OMNI_DECIMAL128: {
            auto returnDecimalValue =
                decimalIRBuilder->BuildDecimalValue(nullptr, *switchExpr.GetReturnType(), nullptr);
            args = { llvmTypes->CreateConstantInt(size),
                whenClauses,
                whenBools,
                resultValues,
                resultNulls,
                evFalse->data,
                evFalse->isNull,
                finalValue,
                finalNull,
                this->batchCodegenContext->rowCnt };
            llvmEngine->CallExternFunction("batch_switch", { switchDataType }, switchDataType, args, nullptr);
            this->value = make_shared<DecimalValue>(finalValue, finalNull,
                const_cast<Value *>(returnDecimalValue->GetPrecision()),
                const_cast<Value *>(returnDecimalValue->GetScale()));
            break;
        }
        case OMNI_CHAR:
        case OMNI_VARCHAR: {
            AllocaInst *finalLength = GetResultArray(OMNI_INT, this->batchCodegenContext->rowCnt);
            args = { llvmTypes->CreateConstantInt(size),
                whenClauses,
                whenBools,
                resultValues,
                resultNulls,
                resultLengths,
                evFalse->data,
                evFalse->isNull,
                evFalse->length,
                finalValue,
                finalNull,
                finalLength,
                this->batchCodegenContext->rowCnt };
            llvmEngine->CallExternFunction("batch_switch", { switchDataType }, switchDataType, args, nullptr);
            this->value = make_shared<CodeGenValue>(finalValue, finalNull, finalLength);
            return;
        }
        default: {
            LogWarn("Unsupported data type in SWITCH expr %d", switchDataType);
            this->value = CreateBatchInvalidCodeGenValue();
            return;
        }
    }
}
