/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "common/common.h"
#include "common/vector_util.h"
#include "operator/sort/sort.h"

using namespace benchmark;
using namespace omniruntime::op;
using namespace omniruntime::vec;
using namespace omniruntime::type;

namespace om_benchmark {
class Sort : public BaseOperatorFixture {
protected:
    OperatorFactory *createOperatorFactory(const State &state) override
    {
        std::vector<int32_t> asc(SORT_ORDERS[TestGroup(state)].size());
        std::vector<int32_t> nullFirst(SORT_ORDERS[TestGroup(state)].size());
        for (unsigned int i = 0; i < SORT_ORDERS[TestGroup(state)].size(); ++i) {
            asc[i] = SORT_ORDERS[TestGroup(state)].at(i)[0];
            nullFirst[i] = SORT_ORDERS[TestGroup(state)].at(i)[1];
        }
        return SortOperatorFactory::CreateSortOperatorFactory(DataTypes(INPUT_TYPES[TestGroup(state)]),
            SORT_CHANNELS[TestGroup(state)].data(), (int32_t)SORT_CHANNELS[TestGroup(state)].size(),
            SORT_CHANNELS[TestGroup(state)].data(), asc.data(), nullFirst.data(),
            (int32_t)SORT_CHANNELS[TestGroup(state)].size());
    }

    std::vector<VectorBatchSupplier> createVecBatch(const State &state) override
    {
        std::vector<VectorBatch *> vvb(totalPages);

        for (int i = 0; i < totalPages; ++i) {
            if (DictionaryBlocks(state)) {
                vvb[i] = CreateSequenceVectorBatchWithDictionaryVector(INPUT_TYPES[TestGroup(state)], rowsPerPage);
            } else {
                vvb[i] = CreateSequenceVectorBatch(INPUT_TYPES[TestGroup(state)], rowsPerPage);
            }
        }

        return VectorBatchToVectorBatchSupplier(vvb);
    }

private:
    int totalPages = 100;
    int rowsPerPage = 10000;
    std::map<std::string, std::vector<DataTypePtr>> INPUT_TYPES = {
        { "group1", { VarcharType(16) } },
        { "group2", { IntType(), IntType() } },
        { "group3", { IntType(), IntType(), DoubleType() } },
        { "group4", { IntType(), LongType() } },
        { "group5", { VarcharType(16) } },
        { "group6", { IntType(), LongType(), Decimal128Type(38, 0) } },
        { "group7", { VarcharType(20), VarcharType(30), VarcharType(50) } },
        { "group8", { VarcharType(50), IntType() } },
        { "group9", { IntType(), VarcharType(60), VarcharType(20), VarcharType(30) } },
        { "group10", { IntType(), VarcharType(50), IntType(), DoubleType(), VarcharType(50) } }
    };

    std::map<std::string, std::vector<int32_t>> SORT_CHANNELS = {
        { "group1", { 0 } },          { "group2", { 0, 1 } },
        { "group3", { 0, 1, 2 } },    { "group4", { 0, 1 } },
        { "group5", { 0 } },          { "group6", { 0, 1, 2 } },
        { "group7", { 0, 1, 2 } },    { "group8", { 0, 1 } },
        { "group9", { 0, 1, 2, 3 } }, { "group10", { 0, 1, 2, 3, 4 } }
    };

    std::map<std::string, std::vector<std::vector<int32_t>>> SORT_ORDERS = {
        { "group1", { { 1, 1 } } },
        { "group2", { { 1, 1 }, { 1, 1 } } },
        { "group3", { { 1, 1 }, { 1, 1 }, { 1, 1 } } },
        { "group4", { { 0, 1 }, { 1, 1 } } },
        { "group5", { { 1, 1 } } },
        { "group6", { { 1, 1 }, { 1, 1 }, { 1, 1 } } },
        { "group7", { { 1, 1 }, { 1, 1 }, { 1, 1 } } },
        { "group8", { { 1, 1 }, { 1, 1 } } },
        { "group9", { { 1, 1 }, { 1, 1 }, { 1, 1 }, { 1, 1 } } },
        { "group10", { { 0, 1 }, { 1, 1 }, { 1, 1 }, { 1, 1 }, { 1, 1 } } }
    };

    OMNI_BENCHMARK_PARAM(std::string, TestGroup, "group1", "group2", "group3", "group4", "group5", "group6", "group7",
        "group8", "group9", "group10");
    OMNI_BENCHMARK_PARAM(bool, DictionaryBlocks, false, true);
};

OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(Sort);
}
