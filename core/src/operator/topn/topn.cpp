//
// Created by root on 6/21/21.
//

#include "topn.h"
#include "../sort/sort.h"
#include "../../jit/annotation.h"
#include "../optimization.h"
#include "../../vector/vector_helper.h"
#include "../../vector/long_vector.h"
#include "../util/operator_util.h"
#include <vector>

using namespace std;
namespace omniruntime {
    namespace op {
        TopNOperatorFactory::TopNOperatorFactory(int32_t *sourceTypes, int32_t sourceTypesCount, int32_t n,
                                                 int32_t *sortCols,
                                                 int32_t *sortAscendings, int32_t *sortNullFirsts,
                                                 int32_t sortColCount) {
            this->sourceTypes = sourceTypes;
            this->sourceTypesCount = sourceTypesCount;
            this->n = n;
            this->sortCols = sortCols;
            this->sortAscendings = sortAscendings;
            this->sortNullFirsts = sortNullFirsts;
            this->sortColCount = sortColCount;
        }

        TopNOperatorFactory::~TopNOperatorFactory() {}

        Operator *TopNOperatorFactory::CreateOperator() {
            return new TopNOperator(sourceTypes, sourceTypesCount, n, sortCols, sortAscendings, sortNullFirsts,
                                    sortColCount);
        }

        TopNOperator::TopNOperator(int32_t *sourceTypes, int32_t sourceTypesCount, int32_t n, int32_t *sortCols,
                                   int32_t *sortAscendings, int32_t *sortNullFirsts, int32_t sortColCount) {
            this->sourceTypes = sourceTypes;
            this->sourceTypesCount = sourceTypesCount;
            this->n = n;
            this->sortCols = sortCols;
            this->sortAscendings = sortAscendings;
            this->sortNullFirsts = sortNullFirsts;
            this->sortColCount = sortColCount;
        }

        TopNOperator::~TopNOperator() {}

        int32_t TopNOperator::AddInput(VectorBatch *vectorBatch) {
            for (int32_t position = 0; position < vectorBatch->GetRowCount(); ++position) {
                if ((pq.size() < n) || compare(position, vectorBatch, pq.top().getVecBatch(), sortColCount, sourceTypes,
                                               sortAscendings) < 0) {
                    VectorBatch *singleRowTable = new VectorBatch(sourceTypesCount, 1);
                    singleRowTable->SetVectors(sourceTypes);

                    for (int i = 0; i < sourceTypesCount; ++i) {
                        switch ((VecType) sourceTypes[i]) {
                            case OMNI_VEC_TYPE_INT:
                                ((IntVector *) singleRowTable->GetVector(i))->SetValue(0,
                                                                                       ((IntVector *) vectorBatch->GetVector(
                                                                                               i))->GetValue(position));
                                break;
                            case OMNI_VEC_TYPE_LONG:
                                ((LongVector *) singleRowTable->GetVector(i))->SetValue(0,
                                                                                        ((LongVector *) vectorBatch->GetVector(
                                                                                                i))->GetValue(
                                                                                                position));
                                break;
                            case OMNI_VEC_TYPE_DOUBLE:
                                ((DoubleVector *) singleRowTable->GetVector(i))->SetValue(0,
                                                                                          ((DoubleVector *) vectorBatch->GetVector(
                                                                                                  i))->GetValue(
                                                                                                  position));
                                break;
                            default:
                                break;
                        }
                    }

                    RowComparator *rowComparator = new RowComparator(sourceTypes, sortCols, sortAscendings,
                                                                     sortColCount, singleRowTable);
                    pq.push(*rowComparator);
                    while (pq.size() > n) {
                        pq.pop();
                    }
                }
            }
            return 0;
        }

        int32_t TopNOperator::GetOutput(std::vector<VectorBatch *> &outputVecBatch) {
            int64_t positionCount = pq.size();
            if (positionCount <= 0) {
                return 0;
            }
            VectorBatch *tmpVecBatch = new VectorBatch(sourceTypesCount, pq.size());
            tmpVecBatch->SetVectors(sourceTypes);
            int32_t outputCols[sourceTypesCount];
            for (int32_t i = 0; i < sourceTypesCount; ++i) {
                outputCols[i] = i;
            }
            int64_t rowNum = 0;

            while (!pq.empty()) {
                VectorBatch *pqVecBatch = pq.top().getVecBatch();

                for (int i = 0; i < sourceTypesCount; ++i) {
                    VectorHelper::SetValue(tmpVecBatch->GetVector(i), positionCount - rowNum - 1,
                                           pqVecBatch->GetVector(i)->GetValues());
                }
                rowNum++;
                pq.pop();
            }

            outputVecBatch.push_back(tmpVecBatch);
            return 0;
        }

        SPECIALIZE(OMNIJIT_TOPN_COMPARE)
        int32_t TopNOperator::compare(int32_t position, VectorBatch *vectorBatch, VectorBatch *currentMaxVectorBatch,
                                      int32_t sortColCount, int32_t *sourceTypes, int32_t *sortAscendings) {
            int compare = 0;

            for (int i = 0; i < sortColCount; ++i) {
                int32_t colType = sourceTypes[i];
                compare = OperatorUtil::compareVectorAtPosition((VecType) colType, vectorBatch->GetVector(i), position,
                                                                currentMaxVectorBatch->GetVector(i), 0);

                if (sortAscendings[i] == 0) {
                    compare = -compare;
                }

                if (compare != 0) {
                    break;
                }
            }
            return compare;
        }


        RowComparator::RowComparator(int32_t *sourceTypes, int32_t *sortCols, int32_t *sortAscendings,
                                     int32_t sortColCount, VectorBatch *vectorBatch) {
            this->sourceTypes = sourceTypes;
            this->sortCols = sortCols;
            this->sortAscendings = sortAscendings;
            this->sortColCount = sortColCount;
            this->vectorBatch = vectorBatch;
        }

        RowComparator::~RowComparator() {}


        int32_t *RowComparator::getSourceTypes() const {
            return sourceTypes;
        }

        int32_t *RowComparator::getSortAscendings() const {
            return sortAscendings;
        }

        int32_t RowComparator::getSortColCount() const {
            return sortColCount;
        }

        VectorBatch *RowComparator::getVecBatch() const {
            return vectorBatch;
        }

        bool operator<(const RowComparator &left, const RowComparator &right) {
            int compare = 0;

            for (int i = 0; i < left.getSortColCount(); ++i) {

                int32_t colType = left.getSourceTypes()[i];
                compare = OperatorUtil::compareVectorAtPosition((VecType) colType, left.getVecBatch()->GetVector(i), 0,
                                                                right.getVecBatch()->GetVector(i), 0);

                if (left.getSortAscendings()[i] == 0 && right.getSortAscendings()[i] == 0) {
                    compare = -compare;
                }

                if (compare != 0) {
                    break;
                }
            }
            //priority_queue is desc, return 1 means left smaller than right,so priority_queue will swap
            //suppose output is asc,compare>0 means left bigger than right so pq shouldn't swap,so return 0
            if (compare > 0) {
                return 0;
            } else {
                return 1;
            }
        }
    }
}