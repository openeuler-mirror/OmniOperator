//
// passing expression and generate code, can the expression be generic without schema info?
// e.g. the input is just arrays and the output would be another array
//
#include "../vector/table.h"
#include "../vector/type.h"
#include "nova_hetu_omnicache_runtime_JniWrapper.h"
#include "../jit/hammer.h"
#include "../memory/memory_pool.h"
#include "../util/op_template_cache.h"
#include "../util/debug.h"

#include "../operator/aggregator/hash_groupby.h"
#include "../operator/sort/sort.h"
#include "../operator/filter/filter.h"

#include <iostream>
#include <cstring>
#include <vector>
#include <time.h>
#include <thread>

jobject transformTableToResult(JNIEnv *env, Table *outputTable);
jobjectArray transform(JNIEnv *env, std::vector<Table*>& result);
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

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_createOperator
(JNIEnv *env, jobject jObj, jlong jNativeFactoryObj)
{
    NativeOmniOperatorFactory* nativeOperatorFactory  = reinterpret_cast<NativeOmniOperatorFactory*>(jNativeFactoryObj);
    auto objAddr = reinterpret_cast<opt_module>(nativeOperatorFactory->getJitContext()->func)(nativeOperatorFactory);
    return reinterpret_cast<uint64_t>(objAddr);
}

JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_addInput
(JNIEnv* env, jobject jObj, jlong jOperatorAddr, jlong jInputDataAddress, jlong jInputDataAddrCnt, jlong jRowAddress, jint jRowNums)
{
    int64_t opAddr = static_cast<int64_t>(jOperatorAddr);
    size_t totalColumnCount = static_cast<size_t>(jInputDataAddrCnt);
    int64_t* address = reinterpret_cast<int64_t*>(jInputDataAddress);
    int32_t* rowNums = reinterpret_cast<int32_t*>(jRowAddress); 
    int32_t columnCount = totalColumnCount / jRowNums;
    int32_t pageCount = static_cast<int32_t>(jRowNums);
    
    NativeOmniOperator* op = reinterpret_cast<NativeOmniOperator*>(opAddr);
    int32_t *colTypes = op->getSourceTypes();
    
    // build table
    char** table = new char*[columnCount];
    int32_t pageIndex = 0;
    for (int cIndex = 0; cIndex < totalColumnCount; cIndex++) {
        int32_t rowNum = rowNums[pageIndex];
        void* data = reinterpret_cast<void *>(address[cIndex]);
        int cIdx =  cIndex % columnCount;
        table[cIdx] = (char*)data;

        if ((cIndex + 1) % columnCount == 0) {
            pageIndex++;
            Table* t = new Table(rowNum, columnCount);
            for (int i = 0; i < columnCount; i++) {
                void* data = table[i];
                ColumnType columnType = static_cast<ColumnType>(colTypes[i]);
                Column* col = new Column(data, columnType, rowNum);
                t->setColumn(col, columnType);
            }

            op->addInput(t, t->getPositionCount());
            opAddr = reinterpret_cast<uint64_t>(op);
        }
    }
    // release memory
    delete[] table;
    return opAddr;
}

JNIEXPORT jobjectArray JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_getOutput
  (JNIEnv* env, jobject jObj, jlong jOperatorAddr)
{
#ifdef DEBUG_LEVEL_LOW
	DebugFuncEntry;
#endif
    // execute agg final
    std::vector<Table*> result;
    NativeOmniOperator* op = reinterpret_cast<NativeOmniOperator*>(jOperatorAddr);
    if (op == nullptr) {
        DebugError("No operator is null pointer %ld.", 0x0);
    }
    int32_t errNo = op->getOutput(result);
    delete op;

#ifdef DEBUG_LEVEL_LOW
	DebugFuncExit;
#endif
    return transform(env, result);
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
    auto start = START();
    jint *sourceTypesArr = env->GetIntArrayElements(jSourceTypes, JNI_FALSE);
    jint *outputColsArr = env->GetIntArrayElements(jOutputCols, JNI_FALSE);
    jint *sortColsArr = env->GetIntArrayElements(jSortCols, JNI_FALSE);
    jint *ascendingsArr = env->GetIntArrayElements(jAscendings, JNI_FALSE);
    jint *nullFirstsArr = env->GetIntArrayElements(jNullFirsts, JNI_FALSE);

    jint sourceTypesCount = env->GetArrayLength(jSourceTypes);
    jint outputColsCount = env->GetArrayLength(jOutputCols);
    jint sortColsCount = env->GetArrayLength(jSortCols);

    PRINT_JNI("before create sort operator factory elapsed time: %ld ms\n", END(start));
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
    PRINT_JNI("after create sort operator factory elapsed time: %ld ms\n", END(start));
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
    auto func = (sort_module)(jitter->lookup("_ZN29NativeOmniSortOperatorFactory18createOmniOperatorEv")->getAddress());

    JitContext *jitContext = new JitContext;
    jitContext->func = reinterpret_cast<uintptr_t>(func);;
    jitContext->jitter = reinterpret_cast<uintptr_t>(jitter.release());

    PRINT_JNI("create jit sort context elapsed time: %ld ms\n", END(start));
    return jitContext;
}

/*
 * Class:     nova_hetu_omniruntime_operator_JniWrapper
 * Method:    createSortOperator
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_createSortOperator
  (JNIEnv *env, jobject jObj, jlong jNativeSortOperatorFactory)
{
    auto start = START();
    NativeOmniSortOperatorFactory *sortOperatorFactory = (NativeOmniSortOperatorFactory *)jNativeSortOperatorFactory;
    JitContext *jitContext = sortOperatorFactory->getJitContext();
    NativeOmniOperator *sortOperator = NULL;

    if (jitContext == NULL) {
      sortOperator = sortOperatorFactory->createOmniOperator();
      PRINT_JNI("ORIGINAL create omni operator elapsed time: %ld ms\n", END(start));
    }
    else {
      sort_module sortModule = (sort_module)(jitContext->func);
      sortOperator = sortModule(sortOperatorFactory);
      PRINT_JNI("JIT create omni operator elapsed time: %ld ms\n", END(start));
    }

    return (int64_t)sortOperator;
}  

/*
 * Class:     nova_hetu_omniruntime_operator_JniWrapper
 * Method:    addSortInput
 * Signature: (J[J[II)V
 */
JNIEXPORT void JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_addSortInput
  (JNIEnv *env, jobject jObj, jlong jSortOperator, jlongArray jDataAddr, jintArray jRowCounts, jint jPageCount)
{
    auto start = START();
    jlong *inputAddr = env->GetLongArrayElements(jDataAddr, JNI_FALSE);
    jint *rowCounts = env->GetIntArrayElements(jRowCounts, JNI_FALSE);

    PRINT_JNI("before add input elapsed time: %ld ms\n", END(start));
    NativeOmniSortOperator *sortOperator = (NativeOmniSortOperator *)jSortOperator;
    int32_t *sourceTypes = sortOperator->getSourceTypes();
    int32_t columnCount = sortOperator->getTypescount();
    Table *table;
    Column *column;
    int32_t rowCount;
    int32_t startOffset = 0;

    ColumnType columnTypes[columnCount];
    ColumnType columnType;
    for (int32_t colIdx = 0; colIdx < columnCount; colIdx++) {
      columnTypes[colIdx] = getColumnType(sourceTypes[colIdx]);
    }

    Table **inputTables = (Table **)malloc(jPageCount * sizeof(Table *));
    for (int32_t tableIdx = 0; tableIdx < jPageCount; tableIdx++) {
      startOffset = tableIdx * columnCount;
      rowCount = rowCounts[tableIdx];
      table = new Table(rowCount, columnCount);
      for (int32_t colIdx = 0; colIdx < columnCount; colIdx++) {
        columnType = columnTypes[colIdx];
        column = new Column((void *)(inputAddr[startOffset + colIdx]), columnType, rowCount);
        table->setColumn(column, columnType);
      }
      inputTables[tableIdx] = table;
    }

    sortOperator->addInput(inputTables, rowCounts, jPageCount);
    PRINT_JNI("after add input elapsed time: %ld ms\n", END(start));
}  

/*
 * Class:     nova_hetu_omniruntime_operator_JniWrapper
 * Method:    getSortOutput
 * Signature: (J)[Lnova/hetu/omniruntime/operator/OMResult;
 */
JNIEXPORT jobjectArray JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_getSortOutput
  (JNIEnv *env, jobject jObj, jlong jSortOperator)
{
    auto start = START();
    NativeOmniSortOperator *sortOperator = (NativeOmniSortOperator *)jSortOperator;
    vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);
    PRINT_JNI("after getOutput elapsed time: %ld ms\n", END(start));
    jobjectArray result = transform(env, outputTables);
    PRINT_JNI("after transform elapsed time: %ld ms\n", END(start));
    freeOutputTable(outputTables);
    return result;
}

/*
 * Class:     nova_hetu_omniruntime_operator_JniWrapper
 * Method:    createFilterAndProjectOperatorFactory
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_createFilterAndProjectOperatorFactory
  (JNIEnv *env, jobject jObj, jstring jFilterExpression , jintArray jInputTypes, jint jInputVecCount, jintArray jProjectIdx, jint jProjectVecCount) {

    std::string filterExpression = std::string(env->GetStringUTFChars(jFilterExpression, JNI_FALSE));
    jint *inputTypes = env->GetIntArrayElements(jInputTypes, JNI_FALSE);
    int32_t vecCount = (int32_t) jInputVecCount;
    jint *projectIdx = env->GetIntArrayElements(jProjectIdx, JNI_FALSE);
    int32_t projectVecCount = (int32_t) jProjectVecCount;
    // TODO: add context
    NativeOmniFilterOperatorFactory* factory = new NativeOmniFilterOperatorFactory(filterExpression, inputTypes, vecCount, projectIdx, projectVecCount);
    env->ReleaseStringUTFChars(jFilterExpression, filterExpression.c_str());
    env->ReleaseIntArrayElements(jInputTypes, inputTypes, JNI_FALSE);
    return (int64_t)factory;
  }

  /*
 * Class:     nova_hetu_omniruntime_operator_JniWrapper
 * Method:    createFilterAndProjectOperator
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_createFilterAndProjectOperator
  (JNIEnv *env, jobject jObj, jlong jFilterAndProjectOperatorFactory)
{
    NativeOmniFilterOperatorFactory *filterAndProjectOperatorFactory = (NativeOmniFilterOperatorFactory *)jFilterAndProjectOperatorFactory;
    return (int64_t)filterAndProjectOperatorFactory->createOmniOperator();
}

Table *getTableFromDataAddress(int64_t *dataAddress, int32_t rowNumber, int32_t vecCount, int32_t *inputTypes)
{
    Table *table = new Table(rowNumber, vecCount);
    uint32_t *colTypes = new uint32_t[vecCount];
    for (int vecIndex = 0; vecIndex < vecCount; vecIndex++)
    {
        void *data = reinterpret_cast<void *>(dataAddress[vecIndex]);
        ColumnType type = buildColumnType(inputTypes[vecIndex]);
        Column *column = new Column(data, type, rowNumber);
        table->setColumn(column, type);
    }

    return table;
}

/*
 * Class:     nova_hetu_omniruntime_operator_JniWrapper
 * Method:    filterAndProjectAddInput
 * Signature: (J[J[II)V
 */
JNIEXPORT jint JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_filterAndProjectAddInput
  (JNIEnv *env, jobject jObj, jlong jFilterAndProjectOperator, jlongArray jInputData, jint jRowCounts)
{
    NativeOmniFilterOperator *filterAndProjectOperator = (NativeOmniFilterOperator *) jFilterAndProjectOperator;
    jlong *inputData = env->GetLongArrayElements(jInputData, JNI_FALSE);
    int32_t rowNumber = (int32_t) jRowCounts;
    Table *table = getTableFromDataAddress(inputData, rowNumber, filterAndProjectOperator->getVecCount(), filterAndProjectOperator->getInputTypes());
    
    return filterAndProjectOperator->addInput(table, rowNumber);
}


/*
 * Class:     nova_hetu_omniruntime_operator_JniWrapper
 * Method:    filterAndProjectGetOutput
 * Signature: (J)[Lnova/hetu/omniruntime/operator/OMResult;
 */
JNIEXPORT jobjectArray JNICALL Java_nova_hetu_omniruntime_operator_JniWrapper_filterAndProjectGetOutput
  (JNIEnv *env, jobject jObj, jlong jFilterAndProjectOperator)
{
    NativeOmniFilterOperator *filterAndProjectOperator = (NativeOmniFilterOperator *) jFilterAndProjectOperator;
    std::vector<Table*> result;

    if (filterAndProjectOperator == nullptr) {
        DebugError("No operator %ld exists.", 0x11111);
    }
    int32_t errNo = filterAndProjectOperator->getOutput(result);
    delete filterAndProjectOperator;

    return transform(env, result);
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
