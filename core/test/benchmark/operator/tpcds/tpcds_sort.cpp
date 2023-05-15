/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include <nlohmann/json.hpp>
#include "../common/common.h"
#include "../operator/sort/sort_expr.h"
#include "tpcds_common.h"


using namespace benchmark;
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace om_benchmark::tpc_ds_data_loader;

namespace om_benchmark {
class TpcDsSort : public TpcDsOperatorFixture {
protected:
    OperatorFactory *createOperatorFactory(const nlohmann::json &metaJson) override
    {
        auto dataTypes = loadVectorBatchTypes(metaJson);
        auto output = metaJson["outputColumns"].get<std::vector<int>>();
        auto asc = metaJson["sortAscendings"].get<std::vector<int>>();
        auto nullFirst = metaJson["sortNullFirsts"].get<std::vector<int>>();
        auto sortKeys = GetExprsFromJson(metaJson["sortKeys"].get<std::vector<std::string>>());

        auto factory = SortWithExprOperatorFactory::CreateSortWithExprOperatorFactory(DataTypes(dataTypes),
            output.data(), (int32_t)output.size(), sortKeys, asc.data(), nullFirst.data(), (int32_t)sortKeys.size());

        Expr::DeleteExprs(sortKeys);

        return factory;
    }

    std::string OperatorIdentifier() override
    {
        return "OmniSortWithExpr.id";
    }
};

OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(TpcDsSort);
}
