#ifndef __OMNI_OPERATOR_H__
#define __OMNI_OPERATOR_H__

#include "../vector/vector_batch.h"
#include "status.h"
#include <vector>

const int32_t MAX_VEC_BATCH_SIZE_IN_BYTES = 1024 * 1024;
namespace omniruntime {
namespace op {
    class Operator {
    public:
        Operator() : status(0) {}

        virtual ~Operator() {}

        virtual int32_t addInput(VectorBatch *vecBatch) = 0;

        virtual int32_t getOutput(std::vector<VectorBatch *> &data) = 0;

        virtual int32_t *getSourceTypes() { return sourceTypes; }

        int getStatus() { return status; }

        void setStatus(OmniStatus status) { this->status = status; };

        virtual void close() {}

    private:
        int status;
    protected:
        int32_t* sourceTypes;
    };
}
}
#endif