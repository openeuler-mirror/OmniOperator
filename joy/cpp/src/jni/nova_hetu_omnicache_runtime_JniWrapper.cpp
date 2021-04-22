//
// passing expression and generate code, can the expression be generic without schema info?
// e.g. the input is just arrays and the output would be another array
//
#include "../data/table.h"
#include "../data/type.h"
#include "api.h"
#include "sort_api.h"
#include "nova_hetu_omnicache_runtime_JniWrapper.h"
#include "../memory_pool/memory_pool.h"
#include "../util/op_template_cache.h"
#include "../util/debug.h"

#include <iostream>
#include <cstring>
#include <vector>
#include <time.h>
#include <thread>

jobject transformTableToResult(JNIEnv *env, Table *outputTable);
jobjectArray tranform(JNIEnv *env, std::vector<Table*>& result);
jobject transformTableToResultV2(JNIEnv *env, Table **outputTables, int32_t tableCount);
OpTemplateCache<uint32_t *> g_typeCache;

#define CLOCKS_PER_MILLISECOND 1000

static jclass omResultCls;
static jmethodID methodId;
static jmethodID setbufMethodId;
static jmethodID setLengthMethodId;
static jmethodID setKeyMethodId;
static jclass bufCls;
static jint JNI_VERSION = JNI_VERSION_1_6; 

ColumnType buildColumnType(jint type)
{
  ColumnType dataType = INT32;
  switch (type) {
    case 1: {
      dataType = INT32;
      break;
    }
    case 2: {
      dataType = INT64;
      break;
    }
    case 3: {
      dataType = DOUBLE;
       break;
    }
    default: {
      break;
    }
  }
  return dataType;
}

jclass createGlobalClassRef(JNIEnv* env, const char *className) {
  jclass local_class = env->FindClass(className);
  jclass global_class = (jclass)env->NewGlobalRef(local_class);
  env->DeleteLocalRef(local_class);
  return global_class;
}

 jint JNI_OnLoad(JavaVM *vm, void *reserved) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
    return JNI_ERR;
  }
  omResultCls = createGlobalClassRef(env,"nova/hetu/omnicache/runtime/OMResult");
  methodId = env->GetMethodID(omResultCls, "<init>", "()V");
  setbufMethodId = env->GetMethodID(omResultCls, "setBuffers", "([Ljava/nio/ByteBuffer;)V");
  setLengthMethodId = env->GetMethodID(omResultCls, "setLength", "(I)V");
  setKeyMethodId = env->GetMethodID(omResultCls, "setKey", "(Ljava/lang/String;)V");
  bufCls = createGlobalClassRef(env, "java/nio/ByteBuffer");
  return JNI_VERSION;
}

void JNI_OnUnload(JavaVM *vm, void *reserved) {
  JNIEnv* env;
  vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_6);
  env->DeleteGlobalRef(omResultCls);
  env->DeleteGlobalRef(bufCls);
}


void transformValueFromPrepareInfo(uint32_t* prepareInfo, uint32_t* target, int targetlen, int* offset) {
      for (int idx = 0;idx < targetlen;idx++) {
      target[idx] = prepareInfo[(*offset)++];
    }
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_prepareAgg
(JNIEnv *env, jobject jObj, jlong jPrepareInfo, jint jGroupByChannelLen,jint jGroupByTypeLen,
jint jAggChannelLen, jint jAggTypeLen, jint jAggFuncTypeLen, jint jOutPutTyeLen)
{
  int totalLen = jGroupByChannelLen + jGroupByTypeLen + jAggChannelLen + jAggTypeLen + jAggFuncTypeLen + jOutPutTyeLen;
    // groupby channel and type
    size_t groupByChannelLen = (size_t)jGroupByChannelLen;
    uint32_t* prepareInfo = (uint32_t*)jPrepareInfo;
    uint32_t* groupByChannels =  new uint32_t[groupByChannelLen];
    int index = 0;
    transformValueFromPrepareInfo(prepareInfo, groupByChannels, jGroupByChannelLen, &index);
    PrepareContext groupByColContext = {groupByChannels, groupByChannelLen};
    size_t groupByTypeLen = (size_t)jGroupByTypeLen;
    uint32_t* groupByTypes = new uint32_t[groupByTypeLen];
    transformValueFromPrepareInfo(prepareInfo, groupByTypes, groupByTypeLen, &index);
    PrepareContext groupByTypeContext = {groupByTypes, groupByTypeLen};

    // agg channel and type
    size_t aggChannelLen = (size_t)jAggChannelLen;
    uint32_t* aggChannels = new uint32_t[aggChannelLen];
    transformValueFromPrepareInfo(prepareInfo, aggChannels, aggChannelLen, &index);
    PrepareContext aggColContext = {aggChannels, aggChannelLen};
    size_t aggTypeLen = (size_t)jAggTypeLen;
    uint32_t* aggTypes = new uint32_t[aggTypeLen];
    transformValueFromPrepareInfo(prepareInfo, aggTypes, aggTypeLen, &index);
    PrepareContext aggTypeContext = {aggTypes, aggTypeLen};

    // agg function type
    size_t aggFuncTypeLen = (size_t)jAggFuncTypeLen;
    uint32_t* aggFuncTypes = new uint32_t[aggFuncTypeLen];
    transformValueFromPrepareInfo(prepareInfo, aggFuncTypes, aggFuncTypeLen, &index);
    PrepareContext aggFuncTypeContext = {aggFuncTypes, aggFuncTypeLen};
    size_t outPutTypeLen = (size_t)jOutPutTyeLen;
    uint32_t* outPutTypes= new uint32_t[outPutTypeLen];
    transformValueFromPrepareInfo(prepareInfo, outPutTypes, outPutTypeLen, &index);
    PrepareContext outPutTypeContext = {outPutTypes, outPutTypeLen};

#ifdef OPTIMIZE_BY_ASYNC
#ifdef DEBUG_LEVEL_LOW
    DebugPrint("StageId:%ld, OpId:%ld Async codegen optimizing.", stageId, opId);
#endif
    std::thread t(prepareHashGroupBy, stageId,opId, groupByColContext,groupByTypeContext,aggColContext,aggTypeContext,aggFuncTypeContext, outPutTypeContext);
    t.detach();
#else 
    return prepareHashGroupBy(groupByColContext,groupByTypeContext,aggColContext,aggTypeContext,aggFuncTypeContext, outPutTypeContext);
#endif
}


JNIEXPORT jlong JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_createOperator
(JNIEnv *env, jobject jObj, jlong jModuleId, jint jSize, jlong jPrepareInfo, jint jGroupByChannelLen, jint jGroupByTypeLen,
 jint jAggChannelLen, jint jAggTypeLen, jint jAggFuncTypeLen, jint jOutPutTyeLen)
  {
      int totalLen = jGroupByChannelLen + jGroupByTypeLen + jAggChannelLen + jAggTypeLen + jAggFuncTypeLen + jOutPutTyeLen;
  // if (totalLen != jSize) {
  //   std::cerr << "mismatch the input prepare info" << totalLen << "," << jSize;
  //   }
    // groupby channel and type
    size_t groupByChannelLen = (size_t)jGroupByChannelLen;
    uint32_t* prepareInfo = (uint32_t*)jPrepareInfo;
    uint32_t* groupByChannels =  new uint32_t[groupByChannelLen];
    int index = 0;
    transformValueFromPrepareInfo(prepareInfo, groupByChannels, jGroupByChannelLen, &index);
    PrepareContext groupByColContext = {groupByChannels, groupByChannelLen};
    size_t groupByTypeLen = (size_t)jGroupByTypeLen;
    uint32_t* groupByTypes = new uint32_t[groupByTypeLen];
    transformValueFromPrepareInfo(prepareInfo, groupByTypes, groupByTypeLen, &index);
    PrepareContext groupByTypeContext = {groupByTypes, groupByTypeLen};

    // agg channel and type
    size_t aggChannelLen = (size_t)jAggChannelLen;
    uint32_t* aggChannels = new uint32_t[aggChannelLen];
    transformValueFromPrepareInfo(prepareInfo, aggChannels, aggChannelLen, &index);
    PrepareContext aggColContext = {aggChannels, aggChannelLen};
    size_t aggTypeLen = (size_t)jAggTypeLen;
    uint32_t* aggTypes = new uint32_t[aggTypeLen];
    transformValueFromPrepareInfo(prepareInfo, aggTypes, aggTypeLen, &index);
    PrepareContext aggTypeContext = {aggTypes, aggTypeLen};

    // agg function type
    size_t aggFuncTypeLen = (size_t)jAggFuncTypeLen;
    uint32_t* aggFuncTypes = new uint32_t[aggFuncTypeLen];
    transformValueFromPrepareInfo(prepareInfo, aggFuncTypes, aggFuncTypeLen, &index);
    PrepareContext aggFuncTypeContext = {aggFuncTypes, aggFuncTypeLen};
    size_t outPutTypeLen = (size_t)jOutPutTyeLen;
    uint32_t* outPutTypes= new uint32_t[outPutTypeLen];
    transformValueFromPrepareInfo(prepareInfo, outPutTypes, outPutTypeLen, &index);
    PrepareContext outPutTypeContext = {outPutTypes, outPutTypeLen};

    return createOperator(jModuleId, groupByColContext,groupByTypeContext,aggColContext,aggTypeContext,aggFuncTypeContext, outPutTypeContext);
  }

JNIEXPORT jlong JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_executeAggIntermediate
(JNIEnv * env, jobject jObj, jlong jOperatorId, jlong jInputDataAddress, jlong jTotalColumn, jint
jColumnCout, jlong jRowAddress, jint jRowNums, jlong inputTypeAddr)
{
    int64_t opId =reinterpret_cast<int64_t>(jOperatorId);
    size_t totalColumnCount = (size_t)jTotalColumn;
    int64_t* address = reinterpret_cast<int64_t*>(jInputDataAddress);
    int32_t* rowNums = reinterpret_cast<int32_t*>(jRowAddress); 

    if (totalColumnCount % jColumnCout != 0) {
      // need handle the error
      std::cout << "input data error,total count:" << totalColumnCount << ",columnCout:" << jColumnCout << std::endl;
    }

    // uint32_t* inputTypes = g_typeCache.get(opId);
    uint32_t* inputTypes = reinterpret_cast<uint32_t*>(inputTypeAddr);
    
    // build table
    char** table = new char*[jColumnCout];
    uint32_t* colTypes = new uint32_t[jColumnCout];
    uint64_t opAddr;
    for (int cIndex = 0;cIndex < totalColumnCount;cIndex++) {
      void* data = reinterpret_cast<void *>(address[cIndex]);
      int cIdx =  cIndex % jColumnCout;
      colTypes[cIdx] = buildColumnType(inputTypes[cIdx]);
      table[cIdx] = (char*)data;

      if ((cIndex + 1) % jColumnCout == 0) {
        opAddr = executeHashGroupByLlvm(jOperatorId, colTypes, jColumnCout, (void**)table, jColumnCout, rowNums[cIndex / jColumnCout]);
    }
}

    // release memory
    delete[] table;
    delete[] colTypes;
    // std::cout << "exit of execute" << std::endl;
    return opAddr;
  }

JNIEXPORT jobjectArray JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_executeAggFinal
  (JNIEnv* env, jobject jObj, jlong jOperatorId)
{
#ifdef DEBUG_LEVEL_LOW
	DebugFuncEntry;
#endif
	int64_t opId =reinterpret_cast<int64_t>(jOperatorId);
    // execute agg final
    // Table* outputTable = executeAggFinal(opId);
    std::vector<Table*> result;
    int32_t pageCount = executeAggFinal(opId, result);

    // jobject omResultObj = transformTableToResult(env, outputTable);
    // release memory
    // g_typeCache.remove(opId);
#ifdef DEBUG_LEVEL_LOW
	DebugFuncExit;
#endif
    // return omResultObj;
    return tranform(env, result);
}

/*
 * Class:     nova_hetu_omnicache_runtime_JniWrapper
 * Method:    sortPrepare
 * Signature: ([II[II[I[I[II)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_sortPrepare
  (JNIEnv *env, jobject jObj, jintArray jSourceTypes, jint jTypeCount, jintArray jOutputCols, jint jOutputColCount,
    jintArray jSortCols, jintArray jAscendings, jintArray jNullFirsts, jint jSortColCount)
{
    auto start = START();
    jint *sourceTypesArr = env->GetIntArrayElements(jSourceTypes, JNI_FALSE);
    jint *outputColsArr = env->GetIntArrayElements(jOutputCols, JNI_FALSE);
    jint *sortColsArr = env->GetIntArrayElements(jSortCols, JNI_FALSE);
    jint *ascendingsArr = env->GetIntArrayElements(jAscendings, JNI_FALSE);
    jint *nullFirstsArr = env->GetIntArrayElements(jNullFirsts, JNI_FALSE);

    PRINT_JNI("before sortPrepare call elapsed time: %ld ms\n", END(start));
    int64_t contextAddress = sortPrepare(
      sourceTypesArr,
      jTypeCount,
      outputColsArr,
      jOutputColCount,
      sortColsArr,
      ascendingsArr,
      nullFirstsArr,
      jSortColCount);
    PRINT_JNI("after sortPrepare call elapsed time: %ld ms\n", END(start));
    return contextAddress;
}

/*
 * Class:     nova_hetu_omnicache_runtime_JniWrapper
 * Method:    sortCreateOperator
 * Signature: (J[II[II[I[I[II)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_sortCreateOperator
  (JNIEnv *env, jobject jObj, jlong jContextAddress, jintArray jSourceTypes, jint jTypeCount, jintArray jOutputCols,
    jint jOutputColCount, jintArray jSortCols, jintArray jAscendings, jintArray jNullFirsts, jint jSortColCount)
{
    auto start = START();
    jint *sourceTypesArr = env->GetIntArrayElements(jSourceTypes, JNI_FALSE);
    jint *outputColsArr = env->GetIntArrayElements(jOutputCols, JNI_FALSE);
    jint *sortColsArr = env->GetIntArrayElements(jSortCols, JNI_FALSE);
    jint *ascendingsArr = env->GetIntArrayElements(jAscendings, JNI_FALSE);
    jint *nullFirstsArr = env->GetIntArrayElements(jNullFirsts, JNI_FALSE);

    PRINT_JNI("before sortCreateOperator call elapsed time: %ld ms\n", END(start));
    int64_t sortAddress = sortCreateOperator(
      jContextAddress,
      sourceTypesArr,
      jTypeCount,
      outputColsArr,
      jOutputColCount,
      sortColsArr,
      ascendingsArr,
      nullFirstsArr,
      jSortColCount);
    PRINT_JNI("after sortCreateOperator call elapsed time: %ld ms\n", END(start));
    return sortAddress;
}

/*
 * Class:     nova_hetu_omnicache_runtime_JniWrapper
 * Method:    sortAddInput
 * Signature: (JJ[J[JI[JJ)V
 */
JNIEXPORT void JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_sortAddInput
  (JNIEnv *env, jobject jObj, jlong jContextAddress, jlong jSortAddress, jlongArray jInputAddr, jlongArray jInputNulls,
   jint jPageCount, jintArray jRowCounts, jint jTotalRowCount)
{
    auto start = START();
    jlong *inputAddr = env->GetLongArrayElements(jInputAddr, JNI_FALSE);
    jlong *nullAddr = env->GetLongArrayElements(jInputNulls, JNI_FALSE);
    jint *rowCounts = env->GetIntArrayElements(jRowCounts, JNI_FALSE);

    PRINT_JNI("before sortAddInput call elapsed time: %ld ms\n", END(start));
    sortAddInput(jContextAddress, jSortAddress, inputAddr, nullAddr, jPageCount, rowCounts, jTotalRowCount);
    PRINT_JNI("after sortAddInput call elapsed time: %ld ms\n", END(start));
}

/*
 * Class:     nova_hetu_omnicache_runtime_JniWrapper
 * Method:    sortExecute
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_sortExecute
  (JNIEnv *env, jobject jObj, jlong jContextAddress, jlong jSortAddress)
{
    auto start = START();
    sortExecute(jContextAddress, jSortAddress);
    PRINT_JNI("after sortExecute call elapsed time: %ld ms\n", END(start));
}

/*
 * Class:     nova_hetu_omnicache_runtime_JniWrapper
 * Method:    sortGetOutput
 * Signature: (JJ)Lnova/hetu/omnicache/runtime/OMResult;
 */
JNIEXPORT jobject JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_sortGetOutput
  (JNIEnv *env, jobject jObj, jlong jContextAddress, jlong jSortAddress)
{
    auto start = START();
    int32_t tableCount = 0;
    Table **outputTable = sortGetOutput(jContextAddress, jSortAddress, &tableCount);
    PRINT_JNI("after sortGetOutput call elapsed time: %ld ms\n", END(start));
    jobject output = transformTableToResultV2(env, outputTable, tableCount);
    PRINT_JNI("after transformTableToResult call elapsed time: %ld ms\n", END(start));
    
    freeOutputTable(outputTable, tableCount);
    return output;
}

jobjectArray tranform(JNIEnv *env, std::vector<Table*>& result)
{
  jobjectArray res = env->NewObjectArray(result.size(), omResultCls, NULL);
  int32_t idx = 0;
  for (auto table : result) {
    jobject obj = transformTableToResult(env, table);
    env->SetObjectArrayElement(res, idx++, obj);
  }
  return res;
}

jobject transformTableToResultV2(JNIEnv *env, Table **outputTables, int32_t tableCount)
{
  //std::cout << "outputTables=" << outputTables << ", tableCount=" << tableCount << endl;
  if (tableCount == 0) {
    jobject omResultObj = env->NewObject(omResultCls, methodId);
    env->CallObjectMethod(omResultObj, setLengthMethodId, 0);
    return omResultObj;
  }

  int32_t columnCount = outputTables[0]->getColumnNumber();
  int32_t positionCount = 0;
  Table *outputTable;
  jobject buf;
  Column *column;
  int32_t columnSize = 0;
  int32_t columnIdx = 0;
  int32_t tableIdx = 0;

  int32_t bufCount = tableCount * columnCount;
  jobjectArray bufs = env->NewObjectArray(bufCount, bufCls, NULL);
  for (int cIndex = 0; cIndex < bufCount; cIndex++) {
    columnIdx = cIndex % columnCount;
    if (columnIdx == 0) {
      tableIdx = cIndex / columnCount;
      outputTable = outputTables[tableIdx];
      column = outputTable->getColumn(columnIdx);
      columnSize = column->getSize();
      positionCount += columnSize;
    }
    else {
      column = outputTable->getColumn(columnIdx);
    }

    switch (column->getType())
    {
      case INT32: {
        buf = env->NewDirectByteBuffer(column->getData(), columnSize * sizeof(int32_t));
        break;
      }
      case INT64: {
          buf = env->NewDirectByteBuffer(column->getData(),  columnSize * sizeof(int64_t));
          break;
      }
      case DOUBLE: {
        buf = env->NewDirectByteBuffer(column->getData(),  columnSize * sizeof(double));
        break;
      }
      default: {
        cout << "unsupport the data type:" << column->getType() << endl;
        break;
      }
    }
    env->SetObjectArrayElement(bufs, cIndex, buf);
  }

  jobject omResultObj = env->NewObject(omResultCls, methodId);
  env->CallObjectMethod(omResultObj, setbufMethodId, bufs);
  env->CallObjectMethod(omResultObj, setLengthMethodId, positionCount);

  return omResultObj;
}

jobject transformTableToResult(JNIEnv *env, Table *outputTable)
{
  uint32_t columnNum = outputTable->getColumnNumber();
  uint32_t positionCount = outputTable->getPositionCount();
#ifdef DEBUG_LEVEL_LOW
  DebugPrint("Result table column number: %d, position count: %d", columnNum, positionCount);
#endif
  jobjectArray bufs = env->NewObjectArray(columnNum, bufCls, NULL);
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
        cout << "unsupport the data type:" << column->getType() << endl;
        break;
      }
    }
    env->SetObjectArrayElement(bufs, cIndex, buf);
  }

  jobject omResultObj = env->NewObject(omResultCls, methodId);
  env->CallObjectMethod(omResultObj, setbufMethodId, bufs);
  uint32_t rowCount = outputTable->getPositionCount();
  env->CallObjectMethod(omResultObj, setLengthMethodId, rowCount);
  return omResultObj;
}

JNIEXPORT jobject JNICALL Java_nova_hetu_omnicache_vector_OMVectorBase_allocate
  (JNIEnv *env, jclass jcls, jlong jsize) {
    void* addr = omni_allocate(jsize);
    jobject jbuf = env->NewDirectByteBuffer(addr, jsize);
    return jbuf;
  }

JNIEXPORT void JNICALL Java_nova_hetu_omnicache_vector_OMVectorBase_release
  (JNIEnv *env, jclass jcls, jlong jAddress) {
    if (jAddress < 0) {
      std::cout << "release address is error:" << jAddress << std::endl;
      return;
    }
    omni_release(jAddress);
  }