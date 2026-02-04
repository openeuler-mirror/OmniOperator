
/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: Mathematical functions for vectorization framework
 */

#pragma once
#include "util/compiler_util.h"
#include "vectorization/Status.h"
#include <cmath>
#include <utility>
#include "type/decimal128.h"
#include "type/data_type.h"
#include "vectorization/functions/Arithmetic.h"

namespace omniruntime::vectorization {
    template <typename T>
    struct AcoshFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::acosh(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct AcosFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::acos(a);
            return Status::OK();
        }
    };

	template <typename T>
    struct AsinFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::asin(a);
            return Status::OK();
        }
    };

	template <typename T>
    struct AsinhFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::asinh(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct AtanFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::atan(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct Atan2Function {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b)
        {
            result = std::atan2(a + 0.0, b + 0.0);
            return Status::OK();
        }
    };

    template <typename T>
    struct CosFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::cos(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct CoshFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::cosh(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct CbrtFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::cbrt(a);
            return Status::OK();
        }
    };

	constexpr double kMinDoubleAboveInt64Max = 9223372036854775808.0;

	inline int64_t safeDoubleToInt64(const int64_t& arg) {
		return arg;
	}

	inline int64_t safeDoubleToInt64(const double& arg) {
		if (std::isnan(arg)) {
			return 0;
		}
		static const int64_t kMax = std::numeric_limits<int64_t>::max();
		static const int64_t kMin = std::numeric_limits<int64_t>::min();

		if (arg >= kMinDoubleAboveInt64Max) {
			return kMax;
		}

		if (arg  < kMin) {
		    return kMin;
		}
		return arg;
	}

  	template <typename T>
	struct CeilFunction {
		template <typename TInput>
        ALWAYS_INLINE Status call(int64_t &result, const TInput &a){
			if constexpr (std::is_integral_v<TInput>) {
				result = a;
			} else {
            	result = safeDoubleToInt64(std::ceil(a));
			}
            return Status::OK();
		}
	};

    template <typename T>
        struct NegativeFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = -a;
            return Status::OK();
        }
    };

    template <typename T>
    struct SignFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            if constexpr (std::is_floating_point<TInput>::value) {
               if (std::isnan(a)) {
                   result = std::numeric_limits<TInput>::quiet_NaN();
               } else {
                   result = (a == 0.0) ? 0.0 : (a > 0.0) ? 1.0 : -1.0;
               }
            } else {
               result = (a == 0) ? 0 : (a > 0) ? 1 : -1;
            }
            return Status::OK();
        }
    };

    template <typename T>
    struct SinhFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::sinh(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct SqrtFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::sqrt(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct ExpFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::exp(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct SecFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = 1 / std::cos(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct Log10Function {
         template <typename TInput>
         ALWAYS_INLINE Status call(TInput &result, const TInput &a) {
             result = std::log10(a);
             return Status::OK();
         }
     };

    template <typename T>
    struct Log1pFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a) {
            result = std::log1p(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct Log2Function {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a) {
            result = std::log2(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct LogarithmFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a, const TInput &b) {
            result = std::log(b) / std::log(a);
            return Status::OK();
        }
    };

    template <typename T>
    struct PModIntFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput& result, const TInput a, const TInput n) {
            TInput r;
            Status status = RemainderFunction<T>().call(r, a, n);
            if (!status.ok()) {
                return status;
            }
            bool keepR = (r > 0 && n > 0) || (r <= 0 && n < 0);
            result = keepR ? r : (r + n) % n;
            return Status::OK();
        }
    };

    template <typename T>
    struct PModFloatFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput& result, const TInput a, const TInput n) {
            if (n == (TInput)0) {
                return Status::UserError("Division by zero");
            }
            TInput r = std::fmod(a, n);
            bool keepR = (r > 0 && n > 0) || (r <= 0 && n < 0);
            result = keepR ? r : std::fmod(r + n, n);
            return Status::OK();
        }
    };

    // Positive function: returns the input value unchanged (identity function)
    // Supports: byte, short, int, long, float, double, decimal64, decimal128
    template <typename T>
    struct PositiveFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = a;
            return Status::OK();
        }
    };

    // Specialization for Decimal128
    template <>
    struct PositiveFunction<type::Decimal128> {
		template <typename R, typename A>
		ALWAYS_INLINE Status call(R &&result, A &&a)
        {
            result = a;
            return Status::OK();
        }
    };


    // Power function: returns x raised to the power of y
    // Supports: double
    template <typename T>
    struct PowerFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &x, const TInput &y)
        {
            result = std::pow(x, y);
            return Status::OK();
        }
    };

    // Rint function: rounds to nearest integer using current rounding mode
    // Supports: double
    template <typename T>
    struct RintFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            result = std::rint(a);
            return Status::OK();
        }
    };

    // Round function: round(expr) default scale=0; round(expr, scale). byte/short/int/long/float/double only.
    template <typename T>
    struct RoundFunction {
        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a)
        {
            return call(result, a, 0);
        }

        template <typename TInput>
        ALWAYS_INLINE Status call(TInput &result, const TInput &a, const int32_t &scale)
        {
            if constexpr (std::is_integral_v<TInput>) {
                result = a;
            } else {
                const double factor = std::pow(10, scale);
                result = static_cast<TInput>(std::round(a * factor) / factor);
            }
            return Status::OK();
        }
    };

}
