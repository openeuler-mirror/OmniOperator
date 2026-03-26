/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once
#include "util/compiler_util.h"
#include <iostream>
#include "codegen/functions/murmur3_hash.h"
#include "type/decimal128.h"
#include "../registration/SimpleFunctionRegistry.h"
#include "codegen/functions/xxhash64_hash.h"

namespace omniruntime::vectorization {

class MurMur3HashFunction : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        auto seedVec = args.top();
        args.pop();
        auto valVec = args.top();
        args.pop();
        const auto size = context->GetResultRowSize();
        if (result == nullptr) {
            result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
        }
        auto seed = reinterpret_cast<ConstVector<int32_t> *>(seedVec)->GetConstValue();
        auto resultVector = reinterpret_cast<Vector<int32_t> *>(result);

        const auto valTypeId = valVec->GetTypeId();
        for (size_t i = 0; i < size; i++) {
            resultVector->SetNotNull(i);
            if (valVec->IsNull(i)) {
                resultVector->SetValue(i, seed);
                continue;
            }
            switch (valTypeId) {
                case OMNI_BOOLEAN: {
                    auto valVector = reinterpret_cast<Vector<bool> *>(valVec);
                    resultVector->SetValue(i, codegen::function::Mm3Boolean(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_BYTE: {
                    auto valVector = reinterpret_cast<Vector<int8_t> *>(valVec);
                    resultVector->SetValue(i, codegen::function::Mm3Int8(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_SHORT: {
                    auto valVector = reinterpret_cast<Vector<int16_t> *>(valVec);
                    resultVector->SetValue(i, codegen::function::Mm3Int16(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_INT:
                case OMNI_DATE32: {
                    auto valVector = reinterpret_cast<Vector<int32_t> *>(valVec);
                        resultVector->SetValue(i, codegen::function::Mm3Int32(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64: {
                    auto valVector = reinterpret_cast<Vector<int64_t> *>(valVec);
                        resultVector->SetValue(i, codegen::function::Mm3Int64(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_FLOAT: {
                    auto valVector = reinterpret_cast<Vector<float> *>(valVec);
                    resultVector->SetValue(i, codegen::function::Mm3Float(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_DOUBLE: {
                    auto valVector = reinterpret_cast<Vector<double> *>(valVec);
                    resultVector->SetValue(i, codegen::function::Mm3Double(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_CHAR:
                case OMNI_VARCHAR:
                case OMNI_VARBINARY: {
                    auto valVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(valVec);
                    resultVector->SetValue(i, codegen::function::Mm3String1(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_DECIMAL128: {
                    auto valVector = reinterpret_cast<Vector<Decimal128> *>(valVec);
                    Decimal128 val = valVector->GetValue(i);
                    resultVector->SetValue(i, codegen::function::Mm3Decimal128(val.HighBits(), val.LowBits(), 0, 0, false, seed, false));
                    break;
                }
            }
        }
        delete seedVec;
        delete valVec;
    }

};

void RegisterMurMur3HashFunction(const std::string &name)
{
    VectorFunction::RegisterVectorFunction(name, {OMNI_BOOLEAN, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_BYTE, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_SHORT, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_INT, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_LONG, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_FLOAT, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_DOUBLE, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_VARCHAR, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_DATE32, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_TIMESTAMP, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_DECIMAL64, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_DECIMAL128, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_VARBINARY, OMNI_INT}, OMNI_INT, std::make_shared<MurMur3HashFunction>());
}

class XxHash64Function : public VectorFunction {
public:
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        auto seedVec = args.top();
        args.pop();
        auto valVec = args.top();
        args.pop();
        const auto size = valVec->GetSize();
        if (result == nullptr) {
            result = VectorHelper::CreateFlatVector(outputType->GetId(), size);
        }
        auto seed = reinterpret_cast<ConstVector<int64_t> *>(seedVec)->GetConstValue();
        auto resultVector = reinterpret_cast<Vector<int64_t> *>(result);

        const auto valTypeId = valVec->GetTypeId();
        for (size_t i = 0; i < size; i++) {
            resultVector->SetNotNull(i);
            if (valVec->IsNull(i)) {
                resultVector->SetValue(i, seed);
                continue;
            }
            switch (valTypeId) {
                case OMNI_BOOLEAN: {
                    auto valVector = reinterpret_cast<Vector<bool> *>(valVec);
                    resultVector->SetValue(i, codegen::function::XxH64Boolean(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_BYTE: {
                    auto valVector = reinterpret_cast<Vector<int8_t> *>(valVec);
                    resultVector->SetValue(i, codegen::function::XxH64Int8(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_SHORT: {
                    auto valVector = reinterpret_cast<Vector<int16_t> *>(valVec);
                    resultVector->SetValue(i, codegen::function::XxH64Int16(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_INT:
                case OMNI_DATE32: {
                    auto valVector = reinterpret_cast<Vector<int32_t> *>(valVec);
                        resultVector->SetValue(i, codegen::function::XxH64Int32(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64: {
                    auto valVector = reinterpret_cast<Vector<int64_t> *>(valVec);
                        resultVector->SetValue(i, codegen::function::XxH64Int64(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_FLOAT: {
                    auto valVector = reinterpret_cast<Vector<float> *>(valVec);
                    resultVector->SetValue(i, codegen::function::XxH64Float(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_DOUBLE: {
                    auto valVector = reinterpret_cast<Vector<double> *>(valVec);
                    resultVector->SetValue(i, codegen::function::XxH64Double(valVector->GetValue(i), false, seed, false));
                    break;
                }
                case OMNI_CHAR:
                case OMNI_VARCHAR:
                case OMNI_VARBINARY: {
                    auto valVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(valVec);
                    std::string_view valString = valVector->GetValue(i);
                    resultVector->SetValue(i, codegen::function::XxH64String(valString.data(), valString.size(), false, seed, false));
                    break;
                }
                case OMNI_DECIMAL128: {
                    auto valVector = reinterpret_cast<Vector<Decimal128> *>(valVec);
                    Decimal128 val = valVector->GetValue(i);
                    resultVector->SetValue(i, codegen::function::XxH64Decimal128(val.HighBits(), val.LowBits(), 0, 0, false, seed, false));
                    break;
                }
            }
        }
        delete seedVec;
        delete valVec;
    }

};

void RegisterXxHash64Function(const std::string &name)
{
    VectorFunction::RegisterVectorFunction(name, {OMNI_BOOLEAN, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_BYTE, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_SHORT, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_INT, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_LONG, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_FLOAT, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_DOUBLE, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_VARCHAR, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_DATE32, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_TIMESTAMP, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_DECIMAL64, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_DECIMAL128, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
    VectorFunction::RegisterVectorFunction(name, {OMNI_VARBINARY, OMNI_LONG}, OMNI_LONG, std::make_shared<XxHash64Function>());
}
}