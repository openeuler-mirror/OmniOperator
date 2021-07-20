//
// Created by root on 6/21/21.
//

#ifndef OMNI_RUNTIME_TOPN_H
#define OMNI_RUNTIME_TOPN_H

#include "../operator.h"
#include "../operator_factory.h"
#include <queue>
#include "../../vector/int_vector.h"

using namespace std;
namespace omniruntime {
    namespace op {
        class RowComparator {
        public:
            RowComparator(int32_t *sourceTypes, int32_t *sortCols, int32_t *sortAscendings, int32_t sortColCount,
                           VectorBatch *vectorBatch);

            RowComparator() {};

            ~RowComparator();


            int32_t *getSourceTypes() const;

            int32_t *getSortAscendings() const;

            int32_t getSortColCount() const;

            VectorBatch *getVecBatch() const;


        private:
            int32_t *sourceTypes = nullptr;
            int32_t *sortCols = nullptr;
            int32_t *sortAscendings = nullptr;
            int32_t sortColCount = 0;
            VectorBatch *vectorBatch = nullptr;
        };

        bool operator<(const RowComparator &left, const RowComparator &right);

        class TopNOperatorFactory : public OperatorFactory {
        public:
            TopNOperatorFactory(int32_t *sourceTypes, int32_t typesCount, int32_t n, int32_t *sortCols,
                    int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount);

            ~TopNOperatorFactory();


            Operator *createOperator();

        private:
            int32_t *sourceTypes = nullptr;
            int32_t sourceTypesCount = 0;
            int32_t *sortCols = nullptr;
            int32_t n = 0;
            int32_t *sortAscendings = nullptr;
            int32_t *sortNullFirsts = nullptr;
            int32_t sortColCount = 0;
        };

        class TopNOperator : public Operator {
        public:
            TopNOperator(int32_t *sourceTypes, int32_t typesCount, int32_t n, int32_t *sortCols,
                         int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount);

            ~TopNOperator();

            int32_t AddInput(VectorBatch *data) override;

            int32_t GetOutput(std::vector<VectorBatch *> &outputVecBatch) override;

            int32_t
            compare(int32_t position, VectorBatch *table, VectorBatch *currentMaxVectorBatch, int32_t sortColCount, int32_t *sourceTypes, int32_t *sortAscendings);

        private:
            int32_t *sourceTypes = nullptr;
            int32_t sourceTypesCount = 0;
            int32_t *sortCols = nullptr;
            int32_t n = 0;
            int32_t *sortAscendings = nullptr;
            int32_t *sortNullFirsts = nullptr;
            int32_t sortColCount = 0;
            priority_queue<RowComparator, vector<RowComparator>, less<vector<RowComparator>::value_type>> pq;

        };
    }
}
#endif //OMNI_RUNTIME_TOPN_H
