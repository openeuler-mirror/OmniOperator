#include "table.h"

Layout Table::getLayout() {
    // if layout exists return it. otherwise get layout from table.
    return *(new Layout());
}

void Table::printTable()
{
    uint32_t columnCount = this->getColumnNumber();

    for (uint32_t i = 0; i < positionCount; i++) {
        for (uint32_t j = 0; j < columnCount; j++) {
            Column *column = data.at(j);
            void *valuePtr = column->getValue(i);
            switch (column->getType()) {
                case INT32: {
                    printf("%d\t", *((int32_t *)valuePtr));
                    break;
                }
                case INT64: {
                    printf("%ld\t", *((int64_t *)valuePtr));
                    break;
                }
                case DOUBLE: {
                    printf("%f\t", *((double *)valuePtr));
                    break;
                }
                default: {
                    printf("no such type in table %d", column->getType());
                }
            }
        }
        printf("\n");
    }
}

ColumnType getColumnType(int32_t colTypeIdx)
{
    if (colTypeIdx == 1) {
        return INT32;
    }
    else if (colTypeIdx == 2) {
        return INT64;
    }
    else if (colTypeIdx == 3) {
        return DOUBLE;
    }
    else {
        return INT32;
    }
}

int32_t getColTypeIdx(ColumnType type)
{
    if (type == INT32) {
        return 1;
    }
    else if (type == INT64) {
        return 2;
    }
    else if (type == DOUBLE) {
        return 3;
    }
    else {
        return 1;
    }
}
