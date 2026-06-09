/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 * Description: DateTrunc function unit tests
 */

#include <gtest/gtest.h>
#include <iostream>
#include <vector>
#include <ctime>
#include <stack>
#include <string>

#include "test/util/test_util.h"
#include "vectorization/registration/Register.h"
#include "vectorization/functions/DateTrunc.h"
#include "vectorization/VectorFunction.h"
#include "codegen/func_signature.h"
#include "vector/vector_helper.h"
#include "vector/vector.h"
#include "type/Timestamp.h"
#include "type/date32.h"

using namespace omniruntime;
using namespace omniruntime::vec;
using namespace omniruntime::vectorization;
using namespace omniruntime::op;
using namespace omniruntime::type;
using namespace omniruntime::codegen;

class DateTruncTestEnvironment : public ::testing::Environment {
public:
    void SetUp() override {
        RegisterFunctions::RegisterAllFunctions("");
    }
};

::testing::Environment* const date_trunc_test_env =
    ::testing::AddGlobalTestEnvironment(new DateTruncTestEnvironment);

class DateTruncTestHelper {
public:
    static void ValidateNullResult(BaseVector* result, int rowSize,
                                    const std::vector<bool>& expectedNull) {
        for (int i = 0; i < rowSize; ++i) {
            EXPECT_EQ(result->IsNull(i), expectedNull[i])
                << "Row " << i << " null mismatch";
        }
    }

    static void ValidateInt64Result(BaseVector* result,
                                     const std::vector<int64_t>& expected,
                                     int rowSize) {
        auto* resultVec = dynamic_cast<Vector<int64_t>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector type mismatch";
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                continue;
            }
            int64_t actual = resultVec->GetValue(i);
            int64_t expVal = expected[i];
            EXPECT_EQ(actual, expVal)
                << "Row " << i << ": Expected=" << expVal
                << ", Actual=" << actual;
        }
    }

    static void ValidateInt32Result(BaseVector* result,
                                     const std::vector<int32_t>& expected,
                                     int rowSize) {
        auto* resultVec = dynamic_cast<Vector<int32_t>*>(result);
        ASSERT_NE(resultVec, nullptr) << "Result vector type mismatch";
        for (int i = 0; i < rowSize; ++i) {
            if (result->IsNull(i)) {
                continue;
            }
            int32_t actual = resultVec->GetValue(i);
            int32_t expVal = expected[i];
            EXPECT_EQ(actual, expVal)
                << "Row " << i << ": Expected=" << expVal
                << ", Actual=" << actual;
        }
    }

    static BaseVector* CreateTimestampVector(const std::vector<int64_t>& values) {
        BaseVector* vec = VectorHelper::CreateFlatVector(OMNI_TIMESTAMP, values.size());
        auto* typedVec = static_cast<Vector<int64_t>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            typedVec->SetValue(i, values[i]);
            typedVec->SetNotNull(i);
        }
        return vec;
    }

    static BaseVector* CreateDate32Vector(const std::vector<int32_t>& values) {
        BaseVector* vec = VectorHelper::CreateFlatVector(OMNI_DATE32, values.size());
        auto* typedVec = static_cast<Vector<int32_t>*>(vec);
        for (size_t i = 0; i < values.size(); ++i) {
            typedVec->SetValue(i, values[i]);
            typedVec->SetNotNull(i);
        }
        return vec;
    }

    /// Build microsecond-precision UTC timestamps.
    /// Uses Timestamp::calendarUtcToEpoch so the value represents exactly the
    /// given UTC date/time — required for correct DAY+ truncation expectations.
    static int64_t MakeTimestampUtc(int year, int month, int day,
                                     int hour, int minute, int second,
                                     int microsecond = 0) {
        std::tm tm = {};
        tm.tm_year = year - 1900;
        tm.tm_mon  = month - 1;
        tm.tm_mday = day;
        tm.tm_hour = hour;
        tm.tm_min  = minute;
        tm.tm_sec  = second;
        int64_t epochSec = Timestamp::calendarUtcToEpoch(tm);
        return epochSec * 1000000LL + microsecond;
    }

    static int32_t MakeDateUtc(int year, int month, int day) {
        return static_cast<int32_t>(
            MakeTimestampUtc(year, month, day, 0, 0, 0) / 86400000000LL);
    }

    /// Execute date_trunc for TIMESTAMP input.
    /// The format and input vectors are consumed by Apply — caller must NOT
    /// delete them.
    static void ExecuteTimestamp(const std::string& format,
                                  BaseVector* inputVec, BaseVector*& result) {
        int32_t size = inputVec->GetSize();
        auto* fmtVec = VectorHelper::CreateFlatVector(OMNI_VARCHAR, size);
        auto* typedFmt =
            static_cast<Vector<LargeStringContainer<std::string_view>>*>(fmtVec);
        for (int32_t i = 0; i < size; ++i) {
            typedFmt->SetValue(i,
                std::string_view(format.data(), format.size()));
            typedFmt->SetNotNull(i);
        }

        auto sig = std::make_shared<FunctionSignature>(
            "date_trunc",
            std::vector<DataTypeId>{OMNI_VARCHAR, OMNI_TIMESTAMP},
            OMNI_TIMESTAMP);
        auto fn = VectorFunction::Find(sig);
        ASSERT_NE(fn, nullptr) << "date_trunc TIMESTAMP signature not found";

        auto outType = std::make_shared<TimestampDataType>();
        ExecutionContext ctx;
        ctx.SetResultRowSize(size);
        std::stack<BaseVector*> args;
        args.push(fmtVec);
        args.push(inputVec);

        ASSERT_NO_THROW(fn->Apply(args, outType, result, &ctx))
            << "date_trunc TIMESTAMP threw exception";
    }

    static void ExecuteDate(const std::string& format,
                             BaseVector* inputVec, BaseVector*& result) {
        int32_t size = inputVec->GetSize();
        auto* fmtVec = VectorHelper::CreateFlatVector(OMNI_VARCHAR, size);
        auto* typedFmt =
            static_cast<Vector<LargeStringContainer<std::string_view>>*>(fmtVec);
        for (int32_t i = 0; i < size; ++i) {
            typedFmt->SetValue(i,
                std::string_view(format.data(), format.size()));
            typedFmt->SetNotNull(i);
        }

        auto sig = std::make_shared<FunctionSignature>(
            "date_trunc",
            std::vector<DataTypeId>{OMNI_VARCHAR, OMNI_DATE32},
            OMNI_DATE32);
        auto fn = VectorFunction::Find(sig);
        ASSERT_NE(fn, nullptr) << "date_trunc DATE32 signature not found";

        auto outType = std::make_shared<Date32DataType>();
        ExecutionContext ctx;
        ctx.SetResultRowSize(size);
        std::stack<BaseVector*> args;
        args.push(fmtVec);
        args.push(inputVec);

        ASSERT_NO_THROW(fn->Apply(args, outType, result, &ctx))
            << "date_trunc DATE32 threw exception";
    }
};

// =========================================================================
// TIMESTAMP — all precision levels
// =========================================================================

TEST(DateTruncTest, TimestampMicrosecond) {
    auto t1 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45, 123456);
    auto t2 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45, 0);
    std::vector<int64_t> in = {t1, t2}, ex = {t1, t2};

    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("MICROSECOND", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampMillisecond) {
    auto t1 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45, 123456);
    auto t2 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45, 999000);
    std::vector<int64_t> in = {t1, t2};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45, 123000),
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45, 999000),
    };

    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("MILLISECOND", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampSecond) {
    auto t1 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45, 500000);
    auto t2 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 30, 0);
    std::vector<int64_t> in = {t1, t2};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45, 0),
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 30, 0),
    };

    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("SECOND", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampMinute) {
    auto t1 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    auto t2 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 23, 59, 59);
    std::vector<int64_t> in = {t1, t2};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 0),
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 23, 59, 0),
    };

    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("MINUTE", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampHour) {
    auto t1 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    auto t2 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 23, 59, 59);
    std::vector<int64_t> in = {t1, t2};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 0, 0),
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 23, 0, 0),
    };

    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("HOUR", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampDay) {
    auto t1 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    auto t2 = DateTruncTestHelper::MakeTimestampUtc(2024, 1, 1, 0, 0, 0);
    std::vector<int64_t> in = {t1, t2};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 0, 0, 0),
        DateTruncTestHelper::MakeTimestampUtc(2024, 1, 1, 0, 0, 0),
    };

    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("DAY", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampWeek) {
    // 2024-06-15 is Saturday → Monday is 2024-06-10
    auto t1 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    std::vector<int64_t> in = {t1};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 10, 0, 0, 0),
    };

    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("WEEK", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampMonth) {
    auto t1 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    auto t2 = DateTruncTestHelper::MakeTimestampUtc(2024, 12, 31, 23, 59, 59);
    std::vector<int64_t> in = {t1, t2};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 1, 0, 0, 0),
        DateTruncTestHelper::MakeTimestampUtc(2024, 12, 1, 0, 0, 0),
    };

    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("MONTH", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampQuarter) {
    auto t1 = DateTruncTestHelper::MakeTimestampUtc(2024, 5, 15, 12, 0, 0);
    auto t2 = DateTruncTestHelper::MakeTimestampUtc(2024, 10, 1, 0, 0, 0);
    std::vector<int64_t> in = {t1, t2};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 4, 1, 0, 0, 0),
        DateTruncTestHelper::MakeTimestampUtc(2024, 10, 1, 0, 0, 0),
    };

    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("QUARTER", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampYear) {
    auto t1 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    auto t2 = DateTruncTestHelper::MakeTimestampUtc(2023, 1, 1, 0, 0, 0);
    std::vector<int64_t> in = {t1, t2};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 1, 1, 0, 0, 0),
        DateTruncTestHelper::MakeTimestampUtc(2023, 1, 1, 0, 0, 0),
    };

    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("YEAR", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

// =========================================================================
// Format aliases
// =========================================================================

TEST(DateTruncTest, TimestampAliasMM) {
    auto t = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    std::vector<int64_t> in = {t};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 1, 0, 0, 0),
    };
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("MM", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampAliasMON) {
    auto t = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    std::vector<int64_t> in = {t};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 1, 0, 0, 0),
    };
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("MON", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampAliasYYYY) {
    auto t = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    std::vector<int64_t> in = {t};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 1, 1, 0, 0, 0),
    };
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("YYYY", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampAliasYY) {
    auto t = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    std::vector<int64_t> in = {t};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 1, 1, 0, 0, 0),
    };
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("YY", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, TimestampAliasDD) {
    auto t = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    std::vector<int64_t> in = {t};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 0, 0, 0),
    };
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("DD", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

// =========================================================================
// DATE32
// =========================================================================

TEST(DateTruncTest, Date32Day) {
    int32_t d1 = DateTruncTestHelper::MakeDateUtc(2024, 6, 15);
    int32_t d2 = DateTruncTestHelper::MakeDateUtc(2024, 1, 1);
    std::vector<int32_t> in = {d1, d2}, ex = {d1, d2};
    auto* iv = DateTruncTestHelper::CreateDate32Vector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteDate("DAY", iv, rv);
    DateTruncTestHelper::ValidateInt32Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, Date32Week) {
    int32_t d = DateTruncTestHelper::MakeDateUtc(2024, 6, 15);
    std::vector<int32_t> in = {d};
    std::vector<int32_t> ex = {
        DateTruncTestHelper::MakeDateUtc(2024, 6, 10),
    };
    auto* iv = DateTruncTestHelper::CreateDate32Vector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteDate("WEEK", iv, rv);
    DateTruncTestHelper::ValidateInt32Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, Date32Month) {
    int32_t d1 = DateTruncTestHelper::MakeDateUtc(2024, 6, 15);
    int32_t d2 = DateTruncTestHelper::MakeDateUtc(2024, 12, 31);
    std::vector<int32_t> in = {d1, d2};
    std::vector<int32_t> ex = {
        DateTruncTestHelper::MakeDateUtc(2024, 6, 1),
        DateTruncTestHelper::MakeDateUtc(2024, 12, 1),
    };
    auto* iv = DateTruncTestHelper::CreateDate32Vector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteDate("MONTH", iv, rv);
    DateTruncTestHelper::ValidateInt32Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, Date32Quarter) {
    int32_t d1 = DateTruncTestHelper::MakeDateUtc(2024, 5, 15);
    int32_t d2 = DateTruncTestHelper::MakeDateUtc(2024, 10, 15);
    std::vector<int32_t> in = {d1, d2};
    std::vector<int32_t> ex = {
        DateTruncTestHelper::MakeDateUtc(2024, 4, 1),
        DateTruncTestHelper::MakeDateUtc(2024, 10, 1),
    };
    auto* iv = DateTruncTestHelper::CreateDate32Vector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteDate("QUARTER", iv, rv);
    DateTruncTestHelper::ValidateInt32Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, Date32Year) {
    int32_t d1 = DateTruncTestHelper::MakeDateUtc(2024, 6, 15);
    int32_t d2 = DateTruncTestHelper::MakeDateUtc(2023, 12, 31);
    std::vector<int32_t> in = {d1, d2};
    std::vector<int32_t> ex = {
        DateTruncTestHelper::MakeDateUtc(2024, 1, 1),
        DateTruncTestHelper::MakeDateUtc(2023, 1, 1),
    };
    auto* iv = DateTruncTestHelper::CreateDate32Vector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteDate("YEAR", iv, rv);
    DateTruncTestHelper::ValidateInt32Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, Date32TimeLevelIdentity) {
    int32_t d = DateTruncTestHelper::MakeDateUtc(2024, 6, 15);
    std::vector<int32_t> in = {d}, ex = {d};
    auto* iv = DateTruncTestHelper::CreateDate32Vector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteDate("HOUR", iv, rv);
    DateTruncTestHelper::ValidateInt32Result(rv, ex, in.size());
    delete rv;
}

// =========================================================================
// NULL / invalid format
// =========================================================================

TEST(DateTruncTest, TimestampNullInput) {
    auto t = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    std::vector<int64_t> in = {t, t, t};
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    iv->SetNull(1);

    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("MONTH", iv, rv);
    EXPECT_FALSE(rv->IsNull(0));
    EXPECT_TRUE(rv->IsNull(1));
    EXPECT_FALSE(rv->IsNull(2));
    delete rv;
}

TEST(DateTruncTest, TimestampInvalidFormat) {
    auto t = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    std::vector<int64_t> in = {t};
    std::vector<bool> exNull = {true};
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("INVALID", iv, rv);
    DateTruncTestHelper::ValidateNullResult(rv, in.size(), exNull);
    delete rv;
}

TEST(DateTruncTest, TimestampEmptyFormat) {
    auto t = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    std::vector<int64_t> in = {t};
    std::vector<bool> exNull = {true};
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("", iv, rv);
    DateTruncTestHelper::ValidateNullResult(rv, in.size(), exNull);
    delete rv;
}

// =========================================================================
// Negative timestamps (FloorDiv)
// =========================================================================

TEST(DateTruncTest, NegativeMinute) {
    int64_t ts = -30000000;
    std::vector<int64_t> in = {ts};
    std::vector<int64_t> ex = {-60000000};
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("MINUTE", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, NegativeHour) {
    int64_t ts = -30LL * 60000000LL;
    std::vector<int64_t> in = {ts};
    std::vector<int64_t> ex = {-3600000000LL};
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("HOUR", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, NegativeSecond) {
    int64_t ts = -500000;
    std::vector<int64_t> in = {ts};
    std::vector<int64_t> ex = {-1000000};
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("SECOND", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

TEST(DateTruncTest, NegativeDay) {
    int64_t ts = -12LL * 3600LL * 1000000LL;
    std::vector<int64_t> in = {ts};
    std::vector<int64_t> ex = {-86400000000LL};
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("DAY", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}

// =========================================================================
// Multi-row
// =========================================================================

TEST(DateTruncTest, MultiRowMonth) {
    auto t1 = DateTruncTestHelper::MakeTimestampUtc(2024, 1, 15, 12, 30, 45);
    auto t2 = DateTruncTestHelper::MakeTimestampUtc(2024, 6, 15, 12, 30, 45);
    auto t3 = DateTruncTestHelper::MakeTimestampUtc(2024, 12, 15, 12, 30, 45);
    std::vector<int64_t> in = {t1, t2, t3};
    std::vector<int64_t> ex = {
        DateTruncTestHelper::MakeTimestampUtc(2024, 1, 1, 0, 0, 0),
        DateTruncTestHelper::MakeTimestampUtc(2024, 6, 1, 0, 0, 0),
        DateTruncTestHelper::MakeTimestampUtc(2024, 12, 1, 0, 0, 0),
    };
    auto* iv = DateTruncTestHelper::CreateTimestampVector(in);
    BaseVector* rv = nullptr;
    DateTruncTestHelper::ExecuteTimestamp("MONTH", iv, rv);
    DateTruncTestHelper::ValidateInt64Result(rv, ex, in.size());
    delete rv;
}
