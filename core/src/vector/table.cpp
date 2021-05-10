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