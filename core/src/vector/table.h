#ifndef __TABLE_H__
#define __TABLE_H__

#include "type.h"
#include "../util/type_infer.h"
#include <stdint.h>
#include <vector>
#include <iostream>

using namespace opt;

class Layout{};

typedef struct ColumnIndex {
    uint32_t idx;
    ColumnType type;
} ColumnIndex;

typedef struct PrepareContext {
    uint32_t* context;
    size_t len;
} PrepareContext;

class Column {
public:
    Column(void* d, ColumnType t, size_t s) : data(d), type(t), size(s) {}
    Column(void *d, ColumnType t, size_t s, int *n) : data(d), type(t), size(s), nulls(n) {}
    virtual ~Column() {}
    ColumnType getType() {
        return type;
    }
    void* getValue(uint32_t rowIdx) {
        void* res = nullptr;
        switch (type)
        {
            case INT32:{
                int32_t* ptr = (int32_t*)data + rowIdx;
                res = (void*)(ptr);
                break;
            }
            case INT64:{
                int64_t* ptr = (int64_t*)data + rowIdx;
                res = (void*)(ptr);
                break;
            }
            case DOUBLE:{
                double* ptr = (double*)data + rowIdx;
                res = (void*)(ptr);
                break;
            }
            default:
                break;
        }
        return res;
    }

    void setValue(uint32_t rowIdx, void *valuePtr) {
        switch (type) {
            case INT32: {
                int32_t value = *((int32_t *)valuePtr);
                int32_t *ptr = (int32_t *)data + rowIdx;
                *ptr = value;
                break;
            }
            case INT64: {
                int64_t value = *((int64_t *)valuePtr);
                int64_t *ptr = (int64_t *)data + rowIdx;
                *ptr = value;
                break;
            }
            case DOUBLE: {
                double value = *((double *)valuePtr);
                double *ptr = (double *)data + rowIdx;
                *ptr = value;
                break;
            }
            default:
                break;
        }
    }

    //to-do we should add isNull function here
    bool isNull(uint32_t rowIdx) {
        return false;
    }

    int *getNulls() {
        return nulls;
    }

    void *getData() {
        return data;
    }

    size_t getSize() {
        return size;
    }

    void printColumn() {
        if (size <= 0)
        {   // TODO throw error code
            std::cout << "error" << std::endl;
        }
        for (int32_t rowIdx = 0; rowIdx < size; ++rowIdx) {
            switch (type)
            {
                case INT32:{
                    int32_t* ptr = (int32_t*)data + rowIdx;
                    std::cout << *ptr << " ";
                    break;
                }
                case INT64:{
                    int64_t* ptr = (int64_t*)data + rowIdx;
                    std::cout << *ptr << " ";
                    break;
                }
                case DOUBLE:{
                    double* ptr = (double*)data + rowIdx;
                    std::cout << *ptr << " ";
                    break;
                }
                default:{
                    std::cout << "No such type " << type << " in column" << std::endl;
                    break;
                }
            }
        }
        std::cout << std::endl;
    }
private:
    void *data;
    int *nulls;
    ColumnType type;
    size_t size;
};

class Table{
public:
    Table(uint32_t positionCount, uint32_t columnCount) {
        this->positionCount = positionCount;
        this->columnCount = columnCount;
        this->types = new ColumnType[columnCount];
        this->columnSize = 0;
    }
    virtual ~Table() {
        for (int32_t i = 0; i < columnSize; i++) {
            delete data[i];
        }
        delete []types; 
    }
    Layout getLayout();
    void setColumn(Column* column, ColumnType type) {
        types[columnSize] = type;
        columnSize++;
        data.push_back(column);
    }
    Column *getColumn(uint32_t idx) {
        return data[idx];
    }

    std::vector<Column *>& getColumns() {
        return data;
    }
    uint32_t getPositionCount() {
        return this->positionCount;
    }
    uint32_t getColumnNumber() {
        return columnCount;
    }
    char** getHeads() {
        uint32_t len = data.size();
        char** heads = new char*[len];
        for (int32_t i = 0; i < len; ++i) {
            heads[i] = (char*)(data[i]->getData());
        }
        return heads;
    }
    ColumnType *getColumnTypes() {
        return types;
    }
    void printTable();
private:
    Layout layout;
    std::vector<Column *> data;
    ColumnType *types;
    uint32_t positionCount;
    uint32_t columnCount;
    uint32_t columnSize;
};

ColumnType getColumnType(int32_t colTypeIdx);
int32_t getColTypeIdx(ColumnType type);

#endif
