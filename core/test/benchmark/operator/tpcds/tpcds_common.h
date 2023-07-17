/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef OMNI_BENCHMARK_OPERATOR_TPC_DS_BASE
#define OMNI_BENCHMARK_OPERATOR_TPC_DS_BASE

#include <nlohmann/json.hpp>
#include <benchmark/benchmark.h>
#include "../common/common.h"
#include "expression/expressions.h"
#include "expression/jsonparser/jsonparser.h"


namespace om_benchmark {
// tool functions
namespace tpc_ds_data_loader {
nlohmann::json LoadBenchmarkMetaFromPath(const std::string &path);

std::string LoadDependentDataPathFromPath(const std::string &path);

std::vector<VectorBatchSupplier> LoadVectorBatchFromPath(const std::string &path,
    const std::vector<omniruntime::type::DataTypePtr> &dataTypes);

std::vector<omniruntime::type::DataTypePtr> LoadDataTypesFromJson(const nlohmann::json &metaJson,
    const std::string &filedName);

std::vector<omniruntime::expressions::Expr *> GetExprsFromJson(const std::vector<std::string>& exprs);
}

class TpcDsOperatorFixture : public BaseOperatorFixture {
protected:
    virtual omniruntime::op::OperatorFactory *createOperatorFactory(const nlohmann::json &metaJson) = 0;

    // a valid data dir show contain file named <operatorIdentifier>
    virtual std::string OperatorIdentifier() = 0;

    // invoked by SetUp(benchmark::State &state)
    virtual void SetUpPath(const std::string &dataPath){};

    // path can be change to other
    virtual std::string GetRealDataPath(const std::string &dataPath){ return dataPath; }

    // get data types of input vector batch
    virtual std::vector<omniruntime::type::DataTypePtr> loadVectorBatchTypes(const nlohmann::json &metaJson)
    {
        return tpc_ds_data_loader::LoadDataTypesFromJson(metaJson, "sourceTypes");
    };

    omniruntime::op::OperatorFactory *createOperatorFactory(const benchmark::State &state) override;

    std::vector<VectorBatchSupplier> createVecBatch(const benchmark::State &state) override;

    std::string MessageWhenSkip(const benchmark::State &state) override;

    void SetUp(benchmark::State &state) override;

    void Initialize() override;

private:
    std::vector<std::string> dataPathList;
};
}
#endif // OMNI_BENCHMARK_OPERATOR_TPC_DS_BASE