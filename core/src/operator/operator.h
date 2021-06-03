#ifndef __OMNI_OPERATOR_H__
#define __OMNI_OPERATOR_H__

#include "../vector/table.h"
#include <vector>

namespace omni {
    class Operator {
    public:
        Operator() : status(0) {}

        virtual ~Operator() {}

        // TBD addInput return ErrNo
        virtual int32_t addInput(Table *data, int32_t rowCount) = 0;

        // orderby needs an array to sort
        virtual int32_t addInput(Table **data, int32_t *rowCount, int32_t pageCount) = 0;

        virtual int32_t getOutput(std::vector<Table *> &data) = 0;

        virtual int32_t *getSourceTypes() = 0;

        int getStatus() { return status; }

    protected:
        int status;
    };
}
#endif