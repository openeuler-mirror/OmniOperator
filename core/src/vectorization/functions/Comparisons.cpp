/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#include "Comparisons.h"
#include "vectorization/SelectivityVector.h"

namespace omniruntime::vectorization {
using namespace omniruntime::vec;

template <typename Cmp, DataTypeId kind>
class ComparisonFunction final : public VectorFunction {
    using T = typename NativeType<kind>::type;

    bool SupportsFlatNoNullsFastPath() const override
    {
        return true;
    }

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        const Cmp cmp;
        auto rightArg = args.top();
        args.pop();
        auto leftArg = args.top();
        args.pop();
        auto rowSize = context->GetResultRowSize();
        auto *flatResult = reinterpret_cast<Vector<bool> *>(VectorHelper::CreateFlatVector(OMNI_BOOLEAN, rowSize));
        auto rows = std::make_shared<SelectivityVector>(rowSize);
        if (leftArg->GetEncoding() == OMNI_FLAT && rightArg->GetEncoding() == OMNI_FLAT) {
            // Fast path for (flat, flat).
            const auto *rawA = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<T> *>(leftArg));
            const auto *rawB = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<T> *>(rightArg));
            rows->applyToSelected([&](vector_size_t i) { flatResult->SetValue(i, cmp(rawA[i], rawB[i])); });
        } else if (leftArg->GetEncoding() == OMNI_ENCODING_CONST && rightArg->GetEncoding() == OMNI_FLAT) {
            // Fast path for (const, flat).
            auto constant = reinterpret_cast<ConstVector<T> *>(leftArg)->GetConstValue();
            const auto *rawValues = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<T> *>(rightArg));
            rows->applyToSelected([&](vector_size_t i) {
                flatResult->SetValue(i, cmp(constant, rawValues[i]));
            });
        } else if (leftArg->GetEncoding() == OMNI_FLAT && rightArg->GetEncoding() == OMNI_ENCODING_CONST) {
            // Fast path for (flat, const).
            const auto *rawValues = unsafe::UnsafeVector::GetRawValues(reinterpret_cast<Vector<T> *>(leftArg));
            auto constant = reinterpret_cast<ConstVector<T> *>(rightArg)->GetConstValue();
            rows->applyToSelected([&](vector_size_t i) {
                flatResult->SetValue(i, cmp(rawValues[i], constant));
            });
        } else {
            // Path if one or more arguments are encoded.
            OMNI_THROW("ComparisonFunction Error:", "Not support decoded vector");
        }
        result = flatResult;
        delete rightArg;
        delete leftArg;
    }
};

template <template <typename> class Cmp, typename StdCmp>
std::shared_ptr<VectorFunction> MakeImpl(const std::string &functionName, const std::vector<DataTypeId> &args)
{
    OMNI_CHECK(args.size()==2, "Compare must have 2 arg!");
    for (size_t i = 1; i < args.size(); i++) {
        OMNI_CHECK(args[i] == args[0], "Compare must have the same type!");
    }
    switch (args[0]) {
        case OMNI_BYTE:
            return std::make_shared<ComparisonFunction<StdCmp, OMNI_BYTE>>();
        case OMNI_SHORT:
            return std::make_shared<ComparisonFunction<StdCmp, OMNI_SHORT>>();
        case OMNI_INT:
            return std::make_shared<ComparisonFunction<StdCmp, OMNI_INT>>();
        case OMNI_LONG:
            return std::make_shared<ComparisonFunction<StdCmp, OMNI_LONG>>();
        case OMNI_DOUBLE:
            return std::make_shared<ComparisonFunction<Cmp<double>, OMNI_DOUBLE>>();
        default: OMNI_THROW("Compare error:", "{} Not support type!", functionName);
    }
}

std::shared_ptr<VectorFunction> makeEqualTo(const std::string &name, const std::vector<DataTypeId> &inputArgs,
    const config::QueryConfig &)
{
    return MakeImpl<Equal, std::equal_to<>>(name, inputArgs);
}

std::shared_ptr<VectorFunction> makeLessThan(const std::string &name, const std::vector<DataTypeId> &inputArgs,
    const config::QueryConfig &)
{
    return MakeImpl<Less, std::less<>>(name, inputArgs);
}

std::shared_ptr<VectorFunction> makeGreaterThan(const std::string &name, const std::vector<DataTypeId> &inputArgs,
    const config::QueryConfig &)
{
    return MakeImpl<Greater, std::greater<>>(name, inputArgs);
}

std::shared_ptr<VectorFunction> makeLessThanOrEqual(const std::string &name, const std::vector<DataTypeId> &inputArgs,
    const config::QueryConfig &)
{
    return MakeImpl<LessOrEqual, std::less_equal<>>(name, inputArgs);
}

std::shared_ptr<VectorFunction> makeGreaterThanOrEqual(const std::string &name,
    const std::vector<DataTypeId> &inputArgs, const config::QueryConfig &)
{
    return MakeImpl<GreaterOrEqual, std::greater_equal<>>(name, inputArgs);
}
}
