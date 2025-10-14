/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: visitor class for expressions
 */

#pragma once

#include "../VectorFunction.h"
#include "../VectorReaders.h"
#include "../SelectivityVector.h"
#include "type/data_type.h"
#include "util/config/QueryConfig.h"
#include "vector/unsafe_vector.h"

namespace omniruntime::vectorization {

template<typename T>
inline constexpr bool is_string_type_v = std::is_same_v<T, std::string_view>;

template <typename FUNC>
class SimpleFunction final : public VectorFunction {
    using T = typename FUNC::exec_return_type;
    using return_type_traits = typename FUNC::return_type;

    template <int32_t POSITION>
    using exec_arg_at = std::tuple_element_t<POSITION, typename FUNC::exec_arg_types>;
    template <int32_t POSITION>
    using arg_at = std::tuple_element_t<POSITION, typename FUNC::arg_types>;
    std::unique_ptr<FUNC> fn_;

    template <typename Callable>
    void applyToSelectedNoThrow(Callable func)
    {
        rows_->applyToSelected([&](auto row) INLINE_LAMBDA {
            try {
                func(row);
            } catch (const exception::OmniException &e) {
                OMNI_THROW("Express Error:", e.what());
            } catch (const std::exception &e) {
                OMNI_THROW("Express Unknown Error:", e.what());
            }
        });
    }

public:
    SimpleFunction(const std::vector<type::DataTypePtr> &inputTypes, const config::QueryConfig &config,
        const std::vector<VectorPtr> &constantInputs)
        : fn_{std::make_unique<FUNC>()} {}

    explicit SimpleFunction() {}

    void apply(std::stack<VectorPtr> &args, const type::DataTypePtr &outputType, vec::BaseVector *&result,
        op::ExecutionContext *context) const override
    {
        unpackSpecializeForAllEncodings<0>(context, result, args);
    }

private:
    std::shared_ptr<SelectivityVector> rows_;

    void IntersectNull(vec::BaseVector *baseVector)
    {
        auto size = baseVector->GetSize();
        auto nullBits = reinterpret_cast<uint64_t *>(vec::unsafe::UnsafeBaseVector::GetNulls(baseVector));
        if (rows_ == nullptr) {
            rows_ = std::make_shared<SelectivityVector>(size);
            rows_->setFromBitsNegate(nullBits, size);
        } else {
            SelectivityVector rows(size);
            rows.setFromBitsNegate(nullBits, size);
            rows_->intersect(rows);
        }
    }

    // This is called only when we know that all args are flat or constant and are
    // eligible for the optimization and the optimization is enabled.
    template <int32_t POSITION, typename... TReader>
    void unpackSpecializeForAllEncodings(op::ExecutionContext *context, vec::BaseVector *result,
        std::stack<VectorPtr> &rawArgs, TReader &... readers) const
    {
        if constexpr (POSITION == FUNC::num_args) {
            iterate(context, result, readers...);
        } else {
            auto arg = rawArgs.top();
            rawArgs.pop();
            using type = exec_arg_at<POSITION>;
            if (arg->GetEncoding() != OMNI_ENCODING_CONST) {
                IntersectNull(arg);
            }
            if constexpr (is_string_type_v<type>) {
                // string_view use StringVectorReader
                auto reader = StringVectorReader(arg);
                unpackSpecializeForAllEncodings<POSITION + 1>(context, result, rawArgs, readers..., reader);
            } else {
                switch (arg->GetEncoding()) {
                    case OMNI_FLAT: {
                        auto reader = FlatVectorReader<type>(arg);
                        unpackSpecializeForAllEncodings<POSITION + 1>(context, result, rawArgs, reader, readers...);
                        break;
                    }
                    case OMNI_DICTIONARY: {
                        auto reader = FlatVectorReader<type>(arg);
                        unpackSpecializeForAllEncodings<POSITION + 1>(context, result, rawArgs, reader, readers...);
                        break;
                    }
                    case OMNI_ENCODING_CONST: {
                        auto reader = ConstVectorReader<type>(arg);
                        unpackSpecializeForAllEncodings<POSITION + 1>(context, result, rawArgs, reader, readers...);
                        break;
                    }
                    default: {}
                }
            }
        }
    }

    template <typename... TReader>
    void iterate(op::ExecutionContext *context, vec::BaseVector *result, TReader &... readers) const
    {
        auto rowSize = context->GetResultRowSize();
        auto resultAddr = static_cast<return_type_traits *>(vec::VectorHelper::UnsafeGetValues(result));
        auto nullBuffer = reinterpret_cast<uint64_t *>(vec::unsafe::UnsafeBaseVector::GetNulls(result));
        if (context->hasFilter) {
            auto isSelect = context->GetIsSelectRow();
            int selectRow = 0;
            applyToSelectedNoThrow([&](auto row) INLINE_LAMBDA {
                if (!isSelect[row]) {
                    return;
                }
                // Passing a stack variable have shown to be boost the performance
                // of functions that repeatedly update the output. The opposite
                // optimization (eliminating the temp) is easier to do by the
                // compiler (assuming the function call is inlined).
                return_type_traits out{};
                bool notNull;
                auto status = doApplyNotNull<0>(row, out, notNull, readers...);
                if (status) {
                    return;
                }

                if (!notNull) {
                    BitUtil::SetBit(nullBuffer, selectRow);
                }
                resultAddr[selectRow] = out;
                ++selectRow;
            });
            memcpy_s(nullBuffer, rowSize, rows_->allBits(), rowSize);
            BitUtil::Negate(nullBuffer, rowSize);
        } else {
            applyToSelectedNoThrow([&](auto row) INLINE_LAMBDA {
                // Passing a stack variable have shown to be boost the performance
                // of functions that repeatedly update the output. The opposite
                // optimization (eliminating the temp) is easier to do by the
                // compiler (assuming the function call is inlined).
                return_type_traits out{};
                bool notNull;
                auto status = doApplyNotNull<0>(row, out, notNull, readers...);
                if (status) {
                    return;
                }
                if (!notNull) {
                    BitUtil::SetBit(nullBuffer, row);
                }
                resultAddr[row] = out;
            });
            memcpy_s(nullBuffer, rowSize, rows_->allBits(), rowSize);
            BitUtil::Negate(nullBuffer, rowSize);
        }
    }

    // If we're guaranteed not to have any nulls, pass all parameters as
    // references.
    //
    // Note that (*fn_).call() will internally dispatch the call to either
    // call() or callNullable() (whichever is implemented by the user
    // function). Default null behavior or not does not matter in this path
    // since we don't have any nulls.
    template <size_t POSITION, typename R0, typename... TStuff, std::enable_if_t<POSITION != FUNC::num_args, int32_t>  =
        0>
    ALWAYS_INLINE int doApplyNotNull(size_t idx, T &target, bool &notNull, R0 &currentReader,
        const TStuff &... extra) const
    {
        if constexpr (std::is_same_v<R0, StringVectorReader>) {
            std::string_view v0 = currentReader[idx];
            return doApplyNotNull<POSITION + 1>(idx, target, notNull, extra..., v0);
        } else {
            decltype(currentReader[idx]) v0 = currentReader[idx];
            return doApplyNotNull<POSITION + 1>(idx, target, notNull, extra..., v0);
        }
    }

    // For default null behavior, Terminate by with UDFHolder::call.
    template <size_t POSITION, typename... Values, std::enable_if_t<POSITION == FUNC::num_args, int32_t>  = 0>
    ALWAYS_INLINE int doApplyNotNull(size_t /*idx*/, T &target, bool &notNull, const Values &... values) const
    {
        return (*fn_).call(target, notNull, values...);
    }
};
}
