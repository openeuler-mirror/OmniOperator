//
// Created by root on 5/26/21.
//

#include <iostream>
#include <vector>
#include <algorithm>
#include "jni_operator.h"
#include "jni_common_def.h"
#include "../vector/table.h"
#include "../operator/omni_operator_factory.h"
#include "../util/debug.h"

jobject transformTableToResult(JNIEnv *env, Table *outputTable)
{
    uint32_t columnNum = outputTable->getColumnNumber();
    uint32_t positionCount = outputTable->getPositionCount();
#ifdef DEBUG_LEVEL_LOW
    DebugPrint("Result table column number: %d, position count: %d", columnNum, positionCount);
#endif
    jobjectArray bufs = env->NewObjectArray(columnNum, bufCls, NULL);
    int* types = new int[columnNum];
    for (int cIndex = 0;cIndex < columnNum;cIndex++) {
        Column *column = outputTable->getColumn(cIndex);
        // column->printColumn();
        jobject buf;
        switch (column->getType())
        {
            case INT32: {
                buf = env->NewDirectByteBuffer(column->getData(), column->getSize() * sizeof(int32_t));
                break;
            }
            case INT64: {
                buf = env->NewDirectByteBuffer(column->getData(),  column->getSize() * sizeof(int64_t));
                break;
            }
            case DOUBLE: {
                buf = env->NewDirectByteBuffer(column->getData(),  column->getSize() * sizeof(double));
                break;
            }
            default: {
                std::cout << "unsupport the data type:" << column->getType() << std::endl;
                break;
            }
        }
        env->SetObjectArrayElement(bufs, cIndex, buf);
        types[cIndex] = column->getType();
    }
    jintArray jintArray = env->NewIntArray(columnNum);
    env->SetIntArrayRegion(jintArray, 0, columnNum, types);
    uint32_t rowCount = outputTable->getPositionCount();
    jobject omResultObj = env->NewObject(vecBatchCls, vecBatchInitMethodId, bufs, jintArray, rowCount);
    return omResultObj;
}

jobjectArray transform(JNIEnv *env, std::vector<Table*>& result)
{
    jobjectArray res = env->NewObjectArray(result.size(), vecBatchCls, NULL);
    int32_t idx = 0;
    for (auto table : result) {
        jobject obj = transformTableToResult(env, table);
        env->SetObjectArrayElement(res, idx++, obj);
    }
    return res;
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

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    addInput
 * Signature: (JJIJI)I
 */
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_addInput
        (JNIEnv *env, jobject jObj, jlong jOperatorAddr, jlong jInputDataAddress, jint jInputDataAddrCnt, jlong jRowCountAddress, jint jRowCountAddrCnt)
{
    JNI_DEBUG_LOG("add input starting.");
    auto start = START();
    OmniOperator *nativeOperator = (OmniOperator *)jOperatorAddr;
    int32_t *sourceTypes = nativeOperator->getSourceTypes();
    int64_t *inputDatas = (int64_t *)jInputDataAddress;
    int32_t *rowCounts = (int32_t *)jRowCountAddress;
    int32_t pageCount = jRowCountAddrCnt;
    int32_t columnCount = jInputDataAddrCnt / jRowCountAddrCnt;
    Table *table;
    Column *column;
    int32_t rowCount;
    int32_t startOffset = 0;

    ColumnType columnTypes[columnCount];
    ColumnType columnType;
    for (int32_t colIdx = 0; colIdx < columnCount; colIdx++) {
        columnTypes[colIdx] = getColumnType(sourceTypes[colIdx]);
    }

    Table **inputTables = (Table **)malloc(pageCount * sizeof(Table *));
    for (int32_t tableIdx = 0; tableIdx < pageCount; tableIdx++) {
        startOffset = tableIdx * columnCount;
        rowCount = rowCounts[tableIdx];
        table = new Table(rowCount, columnCount);
        for (int32_t colIdx = 0; colIdx < columnCount; colIdx++) {
            columnType = columnTypes[colIdx];
            column = new Column((void *)(inputDatas[startOffset + colIdx]), columnType, rowCount);
            table->setColumn(column, columnType);
        }
        inputTables[tableIdx] = table;
    }

    nativeOperator->addInput(inputTables, rowCounts, pageCount);
    JNI_DEBUG_LOG("add input finished, elapsed time: %ld ms.", END(start));
    return 0;
}

/*
 * Class:     nova_hetu_omniruntime_operator_OmniOperator
 * Method:    getOutput
 * Signature: (J)[Lnova/hetu/omniruntime/operator/OMResult;
 */
JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_getOutput
        (JNIEnv *env, jobject jObj, jlong jOperatorAddr)
{
    JNI_DEBUG_LOG("get output starting.");
    auto start = START();
    OmniOperator *nativeOperator = (OmniOperator *)jOperatorAddr;
    std::vector<Table *> outputTables;
    int32_t errNo = nativeOperator->getOutput(outputTables);
    JNI_DEBUG_LOG("getOutput finished, elapsed time: %ld ms.", END(start));
    jobjectArray result = transform(env, outputTables);
    JNI_DEBUG_LOG("transform finished, elapsed time: %ld ms.", END(start));
    int32_t tablesCount = outputTables.size();
    for (int32_t tableIdx = 0; tableIdx < tablesCount; tableIdx++) {
        delete outputTables[tableIdx];
    }
    return env->NewObject(omniResultsCls, omniResultsInitMethodId, result, 2);
}

/*
* Class:     nova_hetu_omniruntime_operator_OmniOperator
* Method:    close
* Signature: (J)[Lnova/hetu/omniruntime/operator/void;
*/
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_operator_OmniOperator_closeNative
        (JNIEnv *env, jobject jObj, jlong jOperatorAddr)
{
    JNI_DEBUG_LOG("close starting.");
    OmniOperator *nativeOperator = (OmniOperator *)jOperatorAddr;
    delete nativeOperator;
    JNI_DEBUG_LOG("close finished, elapsed time: %ld ms.", END(start));
}
