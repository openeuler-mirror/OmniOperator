/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: Cast function for vectorized type conversion
 */

#pragma once
#include "vectorization/VectorFunction.h"
#include "vector/vector_helper.h"
#include <string_view>
#include "CastHooks.h"
#include <type/decimal_operations.h>

namespace omniruntime::vectorization {
    using namespace omniruntime::type;
    using namespace omniruntime::vec;
    using namespace omniruntime::op;
 
class CastFunction : public VectorFunction {
public:
    explicit CastFunction() {}

    CastFunction(const DataTypePtr& fromType, const DataTypePtr& toType): fromType_(fromType), toType_(toType) {}
 
    void Apply(std::stack<BaseVector *> &args, const DataTypePtr &outputType, BaseVector *&result, ExecutionContext *context) const override;

private:
    // Main dispatch function
    void DispatchCast(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    // cast(Xxx as boolean)
    void CastToBoolean(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    // cast(Xxx as byte)
    void CastToByte(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    // cast(Xxx as short)
    void CastToShort(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    // cast(Xxx as int)
    void CastToInt(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    // cast(Xxx as long)
    void CastToLong(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    // cast(Xxx as float)
    void CastToFloat(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    // cast(Xxx as double)
    void CastToDouble(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    // cast(Xxx as string)
    void CastToString(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    template <typename T>
    void CastNumericToString(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    void CastDateToString(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    void CastTimestampToString(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    void CastDecimal64ToString(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    void CastDecimal128ToString(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    // cast(Xxx as date)
    void CastToDate(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    // cast(Xxx as timestamp)
    void CastToTimestamp(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    // cast(Xxx as decimal64)
    void CastToDecimal64(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    int64_t StringToDecimal64(std::string str, bool *isNull, int32_t outPrecision, int32_t outScale) const;
    // cast(Xxx as Decimal128)
    void CastToDecimal128(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    int128_t StringToDecimal128(std::string str, bool *isNull, int32_t outPrecision, int32_t outScale) const;
    // cast(Xxx as binary)
    void CastToBinary(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;
    template <typename TInput>
    void CastIntegerToBinary(BaseVector* input, BaseVector*& result, ExecutionContext* context) const;

    struct DecimalComponents {
        std::string_view wholeDigits;
        std::string_view fractionalDigits;
        std::optional<int32_t> exponent = std::nullopt;
        int8_t sign = 1;
    };

    void fixDecimalStringInPlace(std::string& str) const;

    std::string_view extractDigits(const char *s, size_t start, size_t size) const;

    Status parseDecimalComponents(const char *s, size_t size, DecimalComponents &out) const;

    Status parseHugeInt(const DecimalComponents &decimalComponents, int128_t &out) const;

    template <typename T>
    Status toDecimalValue(const std::string_view s, int toPrecision, int toScale, T &decimalValue) const;

    DataTypePtr fromType_;
    DataTypePtr toType_;
    std::shared_ptr<CastHooks> hooks_;
 };
 }
