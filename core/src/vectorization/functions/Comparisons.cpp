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

    using DictionaryVectorType = std::conditional_t<kind == OMNI_VARCHAR, Vector<DictionaryContainer<std::string_view, LargeStringContainer>>, Vector<DictionaryContainer<T>>>;
    using FlatVectorType = std::conditional_t<kind == OMNI_VARCHAR, Vector<LargeStringContainer<std::string_view>>, Vector<T>>;

    bool SupportsFlatNoNullsFastPath() const override
    {
        return true;
    }

    // this method is used to compare between columns which one is dictionary and another is flat
    // if leftArg is DICTIONARY comparison is cmp(left, right) corresponding to comparison order
    static void handleColumnWithDiffEncodingComparison(BaseVector *leftArg, BaseVector* rightArg, Vector<bool> * comparedResult, int32_t rowSize) {
        const Cmp cmp;
        if (leftArg->GetEncoding() == OMNI_DICTIONARY) {
            auto leftVector = static_cast<DictionaryVectorType *>(leftArg);
            auto rightVector = static_cast<FlatVectorType *>(rightArg);
            auto leftSelectivity = std::make_shared<SelectivityVector>(rowSize);
            const auto leftNullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(leftVector));
            leftSelectivity->setFromBitsNegate(leftNullBits, rowSize);
            auto rightSelectivity = std::make_shared<SelectivityVector>(rowSize);
            const auto rightNullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(leftVector));
            leftSelectivity->setFromBitsNegate(rightNullBits, rowSize);
            leftSelectivity->intersect(*rightSelectivity);
            leftSelectivity->applyToSelected([&](vector_size_t i) {
                comparedResult->SetValue(i, cmp(leftVector->GetValue(i), rightVector->GetValue(i)));
            });

        } else {
            auto leftVector = static_cast<FlatVectorType *>(leftArg);
            auto rightVector = static_cast<DictionaryVectorType *>(rightArg);
            auto leftSelectivity = std::make_shared<SelectivityVector>(rowSize);
            const auto leftNullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(leftVector));
            leftSelectivity->setFromBitsNegate(leftNullBits, rowSize);
            auto rightSelectivity = std::make_shared<SelectivityVector>(rowSize);
            const auto rightNullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(leftVector));
            leftSelectivity->setFromBitsNegate(rightNullBits, rowSize);
            leftSelectivity->intersect(*rightSelectivity);
            leftSelectivity->applyToSelected([&](vector_size_t i) {
                comparedResult->SetValue(i, cmp(rightVector->GetValue(i), leftVector->GetValue(i)));
            });
        }
    }

    // this method is used to compare between columns
    // this is also an example of how to use Selectivity
    static void handleDictColumnComparison(BaseVector* leftArg, BaseVector* rightArg, Vector<bool> * comparedResult, int32_t rowSize) {
        const Cmp cmp;
        auto leftVector = static_cast<DictionaryVectorType *>(leftArg);
        auto rightVector = static_cast<DictionaryVectorType *>(rightArg);
        auto leftSelectivity = std::make_shared<SelectivityVector>(rowSize);
        const auto leftNullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(leftVector));
        leftSelectivity->setFromBitsNegate(leftNullBits, rowSize);
        auto rightSelectivity = std::make_shared<SelectivityVector>(rowSize);
        const auto rightNullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(leftVector));
        leftSelectivity->setFromBitsNegate(rightNullBits, rowSize);
        leftSelectivity->intersect(*rightSelectivity);
        leftSelectivity->applyToSelected([&](vector_size_t i) {
            comparedResult->SetValue(i, cmp(leftVector->GetValue(i), rightVector->GetValue(i)));
        });
    }


    static void handleFlatColumnComparison(BaseVector* leftArg, BaseVector* rightArg, Vector<bool> * comparedResult, int32_t rowSize) {
        const Cmp cmp;
        auto leftVector = static_cast<FlatVectorType *>(leftArg);
        auto rightVector = static_cast<FlatVectorType *>(rightArg);
        auto leftSelectivity = std::make_shared<SelectivityVector>(rowSize);
        const auto leftNullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(leftVector));
        leftSelectivity->setFromBitsNegate(leftNullBits, rowSize);
        auto rightSelectivity = std::make_shared<SelectivityVector>(rowSize);
        const auto rightNullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(leftVector));
        leftSelectivity->setFromBitsNegate(rightNullBits, rowSize);
        leftSelectivity->intersect(*rightSelectivity);
        leftSelectivity->applyToSelected([&](vector_size_t i) {
            comparedResult->SetValue(i, cmp(leftVector->GetValue(i), rightVector->GetValue(i)));
        });
    }


    // this method is used to compare between column and constant
    /**
     *
     * @param leftIsConst ture means original expresion is original expression is cmp(const, rightArg), false means cmp(leftArg, constant)
     */
    static void handleConstComparison(BaseVector *vectorArg, ConstVector<T> *constantVector, Vector<bool> * comparedResult, int32_t rowSize, bool leftIsConst) {
        const Cmp cmp;
        auto selectivity = std::make_shared<SelectivityVector>(rowSize);

        // we always think vlaue type is same with const type
        using ValueType = std::conditional_t<kind == OMNI_VARCHAR, std::string_view, T>;
        ValueType constValue;
        if constexpr (kind == OMNI_VARCHAR) {
            constValue = reinterpret_cast<ConstVector<std::string_view>*>(constantVector)->GetConstValue();
        } else {
            constValue = constantVector->GetConstValue();
        }

        if (vectorArg->GetEncoding() == OMNI_DICTIONARY) {
            auto* vector = static_cast<DictionaryVectorType*>(vectorArg);
            const auto nullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(vector));
            selectivity->setFromBitsNegate(nullBits, rowSize);
            selectivity->applyToSelected([&](vector_size_t i) {
                comparedResult->SetValue(i, leftIsConst ? cmp(constValue, vector->GetValue(i)) : cmp(vector->GetValue(i), constValue));
            });
        } else {
            auto* vector = static_cast<FlatVectorType*>(vectorArg);
            const auto nullBits = reinterpret_cast<uint64_t *>(unsafe::UnsafeBaseVector::GetNulls(vector));
            selectivity->setFromBitsNegate(nullBits, rowSize);
            selectivity->applyToSelected([&](vector_size_t i) {
                comparedResult->SetValue(i, leftIsConst ? cmp(constValue, vector->GetValue(i)) : cmp(vector->GetValue(i), constValue));
            });
        }
    }

    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        auto rightArg = args.top();
        args.pop();
        auto leftArg = args.top();
        args.pop();
        auto rowSize = context->GetResultRowSize();
        auto *flatResult = reinterpret_cast<Vector<bool> *>(VectorHelper::CreateFlatVector(OMNI_BOOLEAN, rowSize));
        auto alignedBuffer = unsafe::UnsafeVector::GetValues(flatResult)->GetBuffer();
        // set defualt compared result is 0 which means the result is filtered for there might exists some row not compare when it's null
        memset_sp(alignedBuffer, rowSize, 0, rowSize);
        if (leftArg->GetEncoding() == OMNI_ENCODING_CONST || rightArg->GetEncoding() == OMNI_ENCODING_CONST) {
            // Fast path for (flat, const).
            auto leftIsConst = leftArg->GetEncoding() == OMNI_ENCODING_CONST;
            auto constant =  leftIsConst ? reinterpret_cast<ConstVector<T> *>(leftArg) : reinterpret_cast<ConstVector<T> *>(rightArg);
            handleConstComparison(leftIsConst ? rightArg : leftArg, constant, flatResult, rowSize, leftIsConst);
        } else if (leftArg->GetEncoding() == OMNI_FLAT && rightArg->GetEncoding() == OMNI_FLAT) {
            // Fast path for (flat, flat).
            handleFlatColumnComparison(leftArg, rightArg, flatResult, rowSize);
        } else if (leftArg->GetEncoding() == OMNI_DICTIONARY && rightArg->GetEncoding() == OMNI_DICTIONARY) {
            // Fast path for (dict, dict).
            handleDictColumnComparison(leftArg, rightArg, flatResult, rowSize);
        } else if (leftArg->GetEncoding() == OMNI_DICTIONARY && rightArg->GetEncoding() == OMNI_FLAT
            || leftArg->GetEncoding() == OMNI_FLAT && rightArg->GetEncoding() == OMNI_DICTIONARY) {
            // Fast path for (dict, flat) or (flat, dict)
            handleColumnWithDiffEncodingComparison(leftArg, rightArg, flatResult, rowSize);
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
        case OMNI_VARCHAR:
            return std::make_shared<ComparisonFunction<StdCmp, OMNI_VARCHAR>>();
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
