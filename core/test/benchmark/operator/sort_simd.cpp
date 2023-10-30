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
class SortSimd : public BaseOperatorFixture {
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
    int totalPages = 1;
    int rowsPerPage = 100;
    std::map<std::string, std::vector<DataTypePtr>> INPUT_TYPES = { { "group1", { LongType() } } };

    std::map<std::string, std::vector<int32_t>> SORT_CHANNELS = { { "group1", { 0 } } };

    std::map<std::string, std::vector<std::vector<int32_t>>> SORT_ORDERS = { { "group1", { { 1, 1 } } } };

    OMNI_BENCHMARK_PARAM(std::string, TestGroup, "group1");

    OMNI_BENCHMARK_PARAM(bool, DictionaryBlocks, false, true);
};

OMNI_BENCHMARK_DECLARE_OPERATOR_DEFAULT(SortSimd);
}
