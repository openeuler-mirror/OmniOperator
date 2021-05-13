//
// passing expression and generate code, can the expression be generic without schema info?
// e.g. the input is just arrays and the output would be another array
//
#include "nova_hetu_omnicache_runtime_JniWrapper.h"

#include "../operator/aggregator/hash_groupby.h"
#include "../operator/sort/sort.h"
#include "../jit/hammer.h"
#include "../memory/memory_pool.h"
#include "../util/debug.h"

#include <iostream>
#include <cstring>
#include <vector>
#include <time.h>
#include <thread>

jobject transformTableToResult(JNIEnv *env, Table *outputTable);
jobjectArray transform(JNIEnv *env, std::vector<Table*>& result);
jobject transformTableToResultV2(JNIEnv *env, Table **outputTables, int32_t tableCount);

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
  omResultCls = createGlobalClassRef(env,"nova/hetu/omniruntime/operator/OMResult");
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

/**
 * Return an NativeOmniHashAggregationFactory object address.
 * 
 * 
 **/
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_createHashAggregationOperatorFactory
(JNIEnv *env, jobject jObj, jintArray jGroupByChannel, jintArray jGroupByType, jintArray jAggChannel, jintArray jAggType, jintArray jAggFuncType, jintArray jOutPutTye)
{
    // groupby channel and type
    jint *groupByCols = env->GetIntArrayElements(jGroupByChannel, JNI_FALSE);
    jint *groupByTypes = env->GetIntArrayElements(jGroupByType, JNI_FALSE);
    jint *aggCols = env->GetIntArrayElements(jAggChannel, JNI_FALSE);
    jint *aggTypes = env->GetIntArrayElements(jAggType, JNI_FALSE);
    jint *aggFuncTypes = env->GetIntArrayElements(jAggFuncType, JNI_FALSE);

    size_t groupByNum = (size_t)env->GetArrayLength(jGroupByChannel);
    size_t aggNum = (size_t)env->GetArrayLength(jAggChannel);

    PrepareContext groupByColContext = {(uint32_t*)groupByCols, groupByNum};
    PrepareContext groupByTypeContext = {(uint32_t*)groupByTypes, groupByNum};
    PrepareContext aggColContext = {(uint32_t*)aggCols, aggNum};
    PrepareContext aggTypeContext = {(uint32_t*)aggTypes, aggNum};
    PrepareContext aggFuncTypeContext = {(uint32_t*)aggFuncTypes, aggNum};

    // return prepareHashGroupBy(groupByColContext,groupByTypeContext,aggColContext,aggTypeContext,aggFuncTypeContext, outPutTypeContext);
    using namespace codegen;
    std::map<std::string, ParamValue *> testParam;
    std::list<Hammer *> deps = std::list<Hammer *>();
    int32_t groupColNum = groupByColContext.len;
    int32_t aggColNum = aggColContext.len;
    int32_t colNum = groupByColContext.len + aggColContext.len;
    int32_t* colTypes = new int32_t[colNum];
    
    for (int i = 0; i < groupColNum; ++i) {
        colTypes[groupByColContext.context[i]] = groupByTypeContext.context[i];
    }
    for (int i = 0; i < aggColNum; ++i) {
        colTypes[aggColContext.context[i]] = aggTypeContext.context[i];
    }

    ParamValue p_col_type = ParamValue(colTypes, colNum);
    ParamValue p_col_count = ParamValue(&colNum);
    ParamValue p_groupByColIdx = ParamValue((int32_t*)groupByColContext.context, groupColNum);
    ParamValue p_group_num = ParamValue(&groupColNum);
    ParamValue p_aggColIdx = ParamValue((int32_t*)aggColContext.context, aggColNum);
    ParamValue p_agg_num = ParamValue(&aggColNum);
    ParamValue p_agg_data_type = ParamValue((int32_t*)aggTypeContext.context, aggColNum);
    ParamValue p_agg_types = ParamValue((int32_t*)aggFuncTypeContext.context, aggColNum);

    testParam["_ZN33NativeOmniHashAggregationOperator6inloopEPPcjPiiS2_iS2_iS2_@3"] = &p_col_type;
    testParam["_ZN33NativeOmniHashAggregationOperator6inloopEPPcjPiiS2_iS2_iS2_@4"] = &p_col_count;
    testParam["_ZN33NativeOmniHashAggregationOperator6inloopEPPcjPiiS2_iS2_iS2_@6"] = &p_group_num;
    testParam["_ZN33NativeOmniHashAggregationOperator6inloopEPPcjPiiS2_iS2_iS2_@8"] = &p_agg_num;
    testParam["_ZN33NativeOmniHashAggregationOperator6inloopEPPcjPiiS2_iS2_iS2_@9"] = &p_agg_types;
    
    testParam["processAgg@2"] =  &p_agg_types;
    testParam["processAgg@3"] =  &p_agg_num;
    testParam["processAgg@4"] =  &p_col_type;

    testParam["_ZN33NativeOmniHashAggregationOperator15constructColumnEP5TablePijjiR8Iterator@2"] = &p_col_type;
    testParam["_ZN33NativeOmniHashAggregationOperator15constructColumnEP5TablePijjiR8Iterator@3"] = &p_group_num;
    testParam["_ZN33NativeOmniHashAggregationOperator15constructColumnEP5TablePijjiR8Iterator@4"] = &p_agg_num;
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/local/lib/libjemalloc.so.2");
    Hammer hammer1("/opt/lib/ir/memory_pool.ll", testParam);
    Hammer hammer2("/opt/lib/ir/hash_groupby.ll", testParam);
    Hammer hammer3("/opt/lib/ir/aggregator.ll", testParam);
    hammer1.harden();
    hammer2.harden();
    hammer3.harden();

    deps.push_back(&hammer3);
    deps.push_back(&hammer2);
    HammerConfig hammerConfig;
    auto jitter = hammer1.create_jitter(deps, hammerConfig);
    
    auto func = (opt_module)(jitter->lookup("_ZN40NativeOmniHashAggregationOperatorFactory18createOmniOperatorEv")->getAddress());
    JitContext* jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(func);
    jitContext->jitter = reinterpret_cast<uintptr_t>(jitter.release());
    
    NativeOmniHashAggregationOperatorFactory* nativeOperatorFactory = new NativeOmniHashAggregationOperatorFactory(groupByColContext, groupByTypeContext, aggColContext, aggTypeContext, aggFuncTypeContext);
    nativeOperatorFactory->setJitContext(jitContext); 
    return reinterpret_cast<uint64_t>(nativeOperatorFactory);
}

JitContext *createSortJitContext(
  int32_t *sourceTypes,
  int32_t typesCount,
  int32_t *outputCols,
  int32_t outputColsCount,
  int32_t *sortCols,
  int32_t *sortAscendings,
  int32_t *sortNullFirsts,
  int32_t sortColsCount);

/*
 * Class:     nova_hetu_omniruntime_operator_JniWrapper
 * Method:    createSortOperatorFactory
 * Signature: ([I[I[I[I[I)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_createSortOperatorFactory
  (JNIEnv *env, jobject jObj, jintArray jSourceTypes, jintArray jOutputCols, jintArray jSortCols, jintArray jAscendings, jintArray jNullFirsts)
{
    JNI_DEBUG_LOG("create sort operator factory starting.");
    auto start = START();
    jint *sourceTypesArr = env->GetIntArrayElements(jSourceTypes, JNI_FALSE);
    jint *outputColsArr = env->GetIntArrayElements(jOutputCols, JNI_FALSE);
    jint *sortColsArr = env->GetIntArrayElements(jSortCols, JNI_FALSE);
    jint *ascendingsArr = env->GetIntArrayElements(jAscendings, JNI_FALSE);
    jint *nullFirstsArr = env->GetIntArrayElements(jNullFirsts, JNI_FALSE);

    jint sourceTypesCount = env->GetArrayLength(jSourceTypes);
    jint outputColsCount = env->GetArrayLength(jOutputCols);
    jint sortColsCount = env->GetArrayLength(jSortCols);

    JNI_DEBUG_LOG("before create sort operator factory elapsed time: %ld ms.", END(start));
    NativeOmniSortOperatorFactory *sortOperatorFactory = NativeOmniSortOperatorFactory::createNativeOmniSortOperatorFactory(
      sourceTypesArr,
      sourceTypesCount,
      outputColsArr,
      outputColsCount,
      sortColsArr,
      ascendingsArr,
      nullFirstsArr,
      sortColsCount);
    JitContext *jitContext = createSortJitContext(
      sortOperatorFactory->getSourceTypes(),
      sortOperatorFactory->getSourceTypeCount(),
      sortOperatorFactory->getOutputCols(),
      sortOperatorFactory->getOutputColCount(),
      sortOperatorFactory->getSortCols(),
      sortOperatorFactory->getSortAscendings(),
      sortOperatorFactory->getSortNullFirsts(),
      sortOperatorFactory->getSortColCount());
    sortOperatorFactory->setJitContext(jitContext);
    JNI_DEBUG_LOG("create sort operator factory finished, elapsed time: %ld ms.", END(start));
    return (int64_t)sortOperatorFactory;
}

JitContext *createSortJitContext(
  int32_t *sourceTypes,
  int32_t typesCount,
  int32_t *outputCols,
  int32_t outputColsCount,
  int32_t *sortCols,
  int32_t *sortAscendings,
  int32_t *sortNullFirsts,
  int32_t sortColsCount)
{
    JNI_DEBUG_LOG("create jit sort context starting.");
    auto start = START();
    using namespace codegen;
    std::map<std::string, ParamValue *> testParam;
    std::list<Hammer *> deps = std::list<Hammer *>();
    int sortColTypes[sortColsCount];

    for (int32_t i = 0; i < sortColsCount; ++i) {
        sortColTypes[i] = sourceTypes[sortCols[i]];
    }

    ParamValue p_sourceTypes = ParamValue(sourceTypes, typesCount);
    ParamValue p_typeCount = ParamValue(&typesCount);
    ParamValue p_outputCols = ParamValue(outputCols, outputColsCount);
    ParamValue p_outputColCount = ParamValue(&outputColsCount);
    ParamValue p_sortCols = ParamValue(sortCols, sortColsCount);
    ParamValue p_sortColTypes = ParamValue(sortColTypes, sortColsCount);
    ParamValue p_sortAscendings = ParamValue(sortAscendings, sortColsCount);
    ParamValue p_sortNullFirsts = ParamValue(sortNullFirsts, sortColsCount);
    ParamValue p_sortColCount = ParamValue(&sortColsCount);

    testParam["_Z9compareTolPiS_S_S_iii@1"] = &p_sortCols;
    testParam["_Z9compareTolPiS_S_S_iii@2"] = &p_sortColTypes;
    testParam["_Z9compareTolPiS_S_S_iii@3"] = &p_sortAscendings;
    testParam["_Z9compareTolPiS_S_S_iii@4"] = &p_sortNullFirsts;
    testParam["_Z9compareTolPiS_S_S_iii@5"] = &p_sortColCount;

    testParam["_Z12allocColumnslPiS_ii@1"] = &p_sourceTypes;
    testParam["_Z12allocColumnslPiS_ii@2"] = &p_outputCols;
    testParam["_Z12allocColumnslPiS_ii@3"] = &p_outputColCount;

    testParam["_ZN10PagesIndex9getOutputEPiilS0_ii@1"] = &p_outputCols;
    testParam["_ZN10PagesIndex9getOutputEPiilS0_ii@2"] = &p_outputColCount;
    testParam["_ZN10PagesIndex9getOutputEPiilS0_ii@4"] = &p_sourceTypes;

    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/lib/gcc/x86_64-linux-gnu/7/libstdc++.so");
    llvm::sys::DynamicLibrary::LoadLibraryPermanently("/usr/local/lib/libjemalloc.so.2");

    Hammer hammer1("/opt/lib/ir/sort.ll", testParam);
    Hammer hammer2("/opt/lib/ir/memory_pool.ll", testParam);
    hammer1.harden();
    hammer2.harden();
    deps.push_back(&hammer2);

    HammerConfig hammerConfig;
    auto jitter = hammer1.create_jitter(deps, hammerConfig);
    auto func = (opt_module)(jitter->lookup("_ZN29NativeOmniSortOperatorFactory18createOmniOperatorEv")->getAddress());

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(func);;
    jitContext->jitter = reinterpret_cast<uintptr_t>(jitter.release());

    JNI_DEBUG_LOG("create jit sort context finished, elapsed time: %ld ms.", END(start));
    return jitContext;
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_createOperator
(JNIEnv *env, jobject jObj, jlong jNativeFactoryObj)
{
    JNI_DEBUG_LOG("create omni operator starting.");
    auto start = START();
    NativeOmniOperatorFactory *nativeOperatorFactory = (NativeOmniOperatorFactory *)jNativeFactoryObj;
    JitContext *jitContext = nativeOperatorFactory->getJitContext();
    NativeOmniOperator *nativeOperator = NULL;

    if (jitContext == NULL) {
      nativeOperator = nativeOperatorFactory->createOmniOperator();
      JNI_DEBUG_LOG("ORIGINAL create omni operator finished, elapsed time: %ld ms.", END(start));
    }
    else {
      opt_module opModule = (opt_module)(jitContext->func);
      nativeOperator = opModule(nativeOperatorFactory);
      JNI_DEBUG_LOG("JIT create omni operator finished, elapsed time: %ld ms.", END(start));
    }

    return reinterpret_cast<int64_t>(nativeOperator);
}

JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_addInput
  (JNIEnv *env, jobject jObj, jlong jOperatorAddr, jlong jInputDataAddress, jint jInputDataAddrCnt, jlong jRowCountAddress, jint jRowCountAddrCnt)
{
    JNI_DEBUG_LOG("add input starting.");
    auto start = START();
    NativeOmniOperator *nativeOperator = (NativeOmniOperator *)jOperatorAddr;
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

JNIEXPORT jobjectArray JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_getOutput
  (JNIEnv *env, jobject jObj, jlong jOperatorAddr)
{
    JNI_DEBUG_LOG("get output starting.");
    auto start = START();
    NativeOmniOperator *nativeOperator = (NativeOmniOperator *)jOperatorAddr;
    std::vector<Table *> outputTables;
    int32_t errNo = nativeOperator->getOutput(outputTables);
    JNI_DEBUG_LOG("getOutput finished, elapsed time: %ld ms.", END(start));
    jobjectArray result = transform(env, outputTables);
    JNI_DEBUG_LOG("transform finished, elapsed time: %ld ms.", END(start));
    freeOutputTable(outputTables);
    delete nativeOperator;
    return result;
}

jobjectArray transform(JNIEnv *env, std::vector<Table*>& result)
{
  jobjectArray res = env->NewObjectArray(result.size(), omResultCls, NULL);
  int32_t idx = 0;
  for (auto table : result) {
    jobject obj = transformTableToResult(env, table);
    env->SetObjectArrayElement(res, idx++, obj);
  }
  return res;
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

/*
 * Class:     nova_hetu_omniruntime_vector_OMVectorBase
 * Method:    allocate
 * Signature: (I)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_nova_hetu_omniruntime_vector_OMVectorBase_allocate
  (JNIEnv *env, jclass jcls, jint jsize)
{
    void* addr = omni_allocate(jsize);
    jobject jbuf = env->NewDirectByteBuffer(addr, jsize);
    return jbuf;
}

/*
 * Class:     nova_hetu_omniruntime_vector_OMVectorBase
 * Method:    release
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_vector_OMVectorBase_release
  (JNIEnv *env, jclass jcls, jlong jAddress)
{
    if (jAddress < 0) {
      std::cout << "release address is error:" << jAddress << std::endl;
      return;
    }
    omni_release(jAddress);
}
