/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef __OMNI_BENCHMARK_COMMON_BASE__
#define __OMNI_BENCHMARK_COMMON_BASE__

#include <benchmark/benchmark.h>
#include "vector/vector_batch.h"
#include "vector/vector_helper.h"
#include "operator/operator.h"
#include "operator/operator_factory.h"

namespace om_benchmark {
#define OMNI_BENCHMARK_PARAM(type, name, ...)                                                \
    om_benchmark::Arg<type> *bm_param_##name =                                               \
        new om_benchmark::Arg<type>(&om_benchmark::ArgToS, #name, { __VA_ARGS__ }, argList); \
    type name(const benchmark::State &state)                                                 \
    {                                                                                        \
        return bm_param_##name->GetValue(state);                                             \
    }

#define OMNI_BENCHMARK_PRIVATE_DECLARE_F(BaseClass, Method)          \
    class BaseClass##_##Method##_Benchmark : public BaseClass {      \
    public:                                                          \
        BaseClass##_##Method##_Benchmark()                           \
        {                                                            \
            this->SetName(#BaseClass "/" #Method);                   \
            auto argProducts = this->ArgProducts();                  \
            if (!argProducts.empty()) {                              \
                this->ArgsProduct(argProducts);                      \
            }                                                        \
            this->Initialize();                                      \
        }                                                            \
        virtual ~BaseClass##_##Method##_Benchmark() = default;       \
                                                                     \
    protected:                                                       \
        void BenchmarkCase(::benchmark::State &) BENCHMARK_OVERRIDE; \
    }

#define OMNI_BENCHMARK_DECLARE(BaseClass, Method)                         \
    OMNI_BENCHMARK_PRIVATE_DECLARE_F(BaseClass, Method);                  \
    BENCHMARK_REGISTER_F(BaseClass, Method); /* NOLINT(cert-err58-cpp) */ \
    void BENCHMARK_PRIVATE_CONCAT_NAME(BaseClass, Method)::BenchmarkCase

#define OMNI_BENCHMARK_DECLARE_F(BaseClass, Method)      \
    OMNI_BENCHMARK_PRIVATE_DECLARE_F(BaseClass, Method); \
    void BENCHMARK_PRIVATE_CONCAT_NAME(BaseClass, Method)::BenchmarkCase

#define OMNI_BENCHMARK_DECLARE_R(BaseClass, Method)                   \
    BENCHMARK_REGISTER_F(BaseClass, Method) /* NOLINT(cert-err58-cpp) \
                                             */

#define OMNI_BENCHMARK_DECLARE_SIMPLE(BaseClass, Method)                  \
    class BaseClass : public om_benchmark::BaseOmniFixture {};            \
    OMNI_BENCHMARK_PRIVATE_DECLARE_F(BaseClass, Method);                  \
    BENCHMARK_REGISTER_F(BaseClass, Method); /* NOLINT(cert-err58-cpp) */ \
    void BENCHMARK_PRIVATE_CONCAT_NAME(BaseClass, Method)::BenchmarkCase

#define OMNI_BENCHMARK_DECLARE_SIMPLE_F(BaseClass, Method)     \
    class BaseClass : public om_benchmark::BaseOmniFixture {}; \
    OMNI_BENCHMARK_PRIVATE_DECLARE_F(BaseClass, Method);       \
    void BENCHMARK_PRIVATE_CONCAT_NAME(BaseClass, Method)::BenchmarkCase

std::string ArgToS(bool arg);

std::string ArgToS(int32_t arg);

std::string ArgToS(int64_t arg);

std::string ArgToS(double_t arg);

std::string ArgToS(std::string arg);

class BasicArg {
public:
    int index;
    std::string argName;
    std::vector<std::string> valueStr;
    std::vector<int64_t> valueIndex;

    std::string GetValueString(const benchmark::State &state);
};

template <typename T> class Arg : public BasicArg {
    using ToSFunc = std::string(T);

public:
    Arg(ToSFunc *tsf, const std::string &argName, const std::vector<T> &values, std::vector<BasicArg *> &argList)
        : values(values)
    {
        this->index = argList.size();
        this->argName = argName;
        int ix = 0;
        for (unsigned int i = 0; i < values.size(); ++i) {
            valueIndex.push_back(ix++);
            valueStr.push_back(tsf(values.at(i)));
        }
        argList.push_back(this);
    }

    virtual ~Arg() = default;

    T GetValue(const benchmark::State &state)
    {
        return values.at(state.range(index));
    }

private:
    std::vector<T> values;
};

class BaseOmniFixture : public benchmark::Fixture {
protected:
    virtual std::string MessageWhenSkip(const benchmark::State &state)
    {
        return "";
    }

    std::vector<BasicArg *> argList;
    std::vector<std::vector<int64_t>> ArgProducts();
    virtual void Initialize() {}
    void SetUp(benchmark::State &state) override;
};

#define OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(BaseClass)                          \
    OMNI_BENCHMARK_DECLARE_F(BaseClass, DefaultBenchmark)(benchmark::State & state) \
    {                                                                               \
        RunDefaultBenchmark(state);                                                 \
    }                                                                               \
    /* * NOLINT(readability-container-size-empty) * */                              \
    OMNI_BENCHMARK_DECLARE_R(BaseClass, DefaultBenchmark)->UseManualTime()->Unit(benchmark::kMillisecond)->Iterations(1)

enum BaseFixtureGetOutputStrategy {
    AFTER_ALL_INPUT_FINISHED = 0,
    AFTER_EACH_INPUT_FINISHED = 1
};

using VectorBatchSupplier = std::function<omniruntime::vec::VectorBatch *()>;

class BaseOperatorFixture : public BaseOmniFixture {
protected:
    virtual omniruntime::op::OperatorFactory *createOperatorFactory(const benchmark::State &state) = 0;

    virtual std::vector<VectorBatchSupplier> createVecBatch(const benchmark::State &state) = 0;

    virtual BaseFixtureGetOutputStrategy GetOutputStrategy()
    {
        return AFTER_ALL_INPUT_FINISHED;
    }

    void SetUp(benchmark::State &state) override;

    void TearDown(benchmark::State &state) override;

    void RunDefaultBenchmark(benchmark::State &state);

private:
    omniruntime::op::OperatorFactory *operatorFactory;
};

// This is used for benchmarks that generate too many rows
// It does not create all VectorBatches at once to reduce memory usage
// VectorBatch is created per iteration.
// PerUtil does not work when number of iterations is > 1, so we have to define new Fixure which does not use PerfUtil
class BaseOperatorLargeFixture : public BaseOmniFixture {
protected:
    virtual omniruntime::op::OperatorFactory *createOperatorFactory(const benchmark::State &state) = 0;

    virtual omniruntime::vec::VectorBatch *createSingleVecBatch(const benchmark::State &state) = 0;

    virtual BaseFixtureGetOutputStrategy GetOutputStrategy()
    {
        return AFTER_ALL_INPUT_FINISHED;
    }

    void SetUp(benchmark::State &state) override;

    void TearDown(benchmark::State &state) override;

    void RunDefaultBenchmark(benchmark::State &state);

protected:
    omniruntime::op::OperatorFactory *operatorFactory;
};

#define OMNI_BENCHMARK_DECLARE_OPERATOR_LARGE(BaseClass)                            \
    OMNI_BENCHMARK_DECLARE_F(BaseClass, DefaultBenchmark)(benchmark::State & state) \
    {                                                                               \
        RunDefaultBenchmark(state);                                                 \
    }                                                                               \
    /* * NOLINT(readability-container-size-empty) * */                              \
    OMNI_BENCHMARK_DECLARE_R(BaseClass, DefaultBenchmark)->UseManualTime()->Unit(benchmark::kMillisecond)
}
#endif
