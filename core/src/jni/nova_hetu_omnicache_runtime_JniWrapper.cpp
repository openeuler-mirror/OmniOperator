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

/**
 * Return an NativeOmniHashAggregationFactory object address.
 * 
 * 
 **/
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


JNIEXPORT jlong JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_createOperator
(JNIEnv *env, jobject jObj, jlong jNativeFactoryObj)
{

    // return createOperator(jModuleId, groupByColContext,groupByTypeContext,aggColContext,aggTypeContext,aggFuncTypeContext, outPutTypeContext);

    NativeOmniHashAggregationOperatorFactory* nativeOperatorFactory  = reinterpret_cast<NativeOmniHashAggregationOperatorFactory*>(jNativeFactoryObj);
    return reinterpret_cast<opt_module>(nativeOperatorFactory->getJitContext()->func)(nativeOperatorFactory);
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
        int32_t rowNum = rowNums[cIndex / jColumnCout];

        void* data = reinterpret_cast<void *>(address[cIndex]);
        int cIdx =  cIndex % jColumnCout;
        colTypes[cIdx] = buildColumnType(inputTypes[cIdx]);
        table[cIdx] = (char*)data;

        if ((cIndex + 1) % jColumnCout == 0) {
            Table* t = new Table(rowNum, jColumnCout);
            for (int i = 0; i < jColumnCout; i++) {
                void* data = table[i];
                ColumnType columnType = static_cast<ColumnType>(colTypes[i]);
                Column* col = new Column(data, columnType, rowNum);
                t->setColumn(col, columnType);
            }
        
            NativeOmniHashAggregationOperator* groupBy = reinterpret_cast<NativeOmniHashAggregationOperator*>(opId);
            groupBy->addInput(t, t->getPositionCount());
            opAddr = reinterpret_cast<uint64_t>(groupBy);
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
    std::vector<Table*> result;
    // int32_t pageCount = executeAggFinal(opId, result);

    NativeOmniHashAggregationOperator* groupBy = reinterpret_cast<NativeOmniHashAggregationOperator*>(opId);
    if (groupBy == nullptr) {
        DebugError("No operator %ld exists.", 0x11111);
    }
    int32_t errNo = groupBy->getOutput(result);
    delete groupBy;

#ifdef DEBUG_LEVEL_LOW
	DebugFuncExit;
#endif
    return tranform(env, result);
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
 * Class:     nova_hetu_omnicache_runtime_JniWrapper
 * Method:    createSortOperatorFactory
 * Signature: ([II[II[I[I[II)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_createSortOperatorFactory
  (JNIEnv *env, jobject jObj, jintArray jSourceTypes, jint jTypeCount, jintArray jOutputCols, jint jOutputColCount,
   jintArray jSortCols, jintArray jAscendings, jintArray jNullFirsts, jint jSortColCount)
{
    auto start = START();
    jint *sourceTypesArr = env->GetIntArrayElements(jSourceTypes, JNI_FALSE);
    jint *outputColsArr = env->GetIntArrayElements(jOutputCols, JNI_FALSE);
    jint *sortColsArr = env->GetIntArrayElements(jSortCols, JNI_FALSE);
    jint *ascendingsArr = env->GetIntArrayElements(jAscendings, JNI_FALSE);
    jint *nullFirstsArr = env->GetIntArrayElements(jNullFirsts, JNI_FALSE);

    PRINT_JNI("before create sort operator factory call elapsed time: %ld ms\n", END(start));
    NativeOmniSortOperatorFactory *sortOperatorFactory = NativeOmniSortOperatorFactory::createNativeOmniSortOperatorFactory(
      sourceTypesArr,
      jTypeCount,
      outputColsArr,
      jOutputColCount,
      sortColsArr,
      ascendingsArr,
      nullFirstsArr,
      jSortColCount);
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
    PRINT_JNI("after create sort operator factory call elapsed time: %ld ms\n", END(start));
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

    PRINT_API("create jit sort context elapsed time: %ld ms\n", END(start));
    return jitContext;
}

/*
 * Class:     nova_hetu_omnicache_runtime_JniWrapper
 * Method:    createSortOperator
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_createSortOperator
  (JNIEnv *env, jobject jObj, jlong nativeSortOperatorFactory)
{
    NativeOmniSortOperatorFactory *sortOperatorFactory = (NativeOmniSortOperatorFactory *)nativeSortOperatorFactory;
    JitContext *jitContext = sortOperatorFactory->getJitContext();
    NativeOmniOperator *sortOperator = NULL;

    if (jitContext == NULL) {
      sortOperator = sortOperatorFactory->createOmniOperator();
    }
    else {
      sort_module sortModule = (sort_module)(jitContext->func);
      sortOperator = sortModule(sortOperatorFactory);
    }

    return (int64_t)sortOperator;
}  

/*
 * Class:     nova_hetu_omnicache_runtime_JniWrapper
 * Method:    addSortInput
 * Signature: (J[J[II)V
 */
JNIEXPORT void JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_addSortInput
  (JNIEnv *env, jobject jObj, jlong jSortOperator, jlongArray jDataAddr, jintArray jRowCounts, jint jPageCount)
{
    auto start = START();
    jlong *inputAddr = env->GetLongArrayElements(jDataAddr, JNI_FALSE);
    jint *rowCounts = env->GetIntArrayElements(jRowCounts, JNI_FALSE);

    PRINT_JNI("before sortAddInput call elapsed time: %ld ms\n", END(start));
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
    PRINT_JNI("after sortAddInput call elapsed time: %ld ms\n", END(start));   
}  

/*
 * Class:     nova_hetu_omnicache_runtime_JniWrapper
 * Method:    getSortOutput
 * Signature: (J)[Lnova/hetu/omnicache/runtime/OMResult;
 */
JNIEXPORT jobjectArray JNICALL Java_nova_hetu_omnicache_runtime_JniWrapper_getSortOutput
  (JNIEnv *env, jobject jObj, jlong jSortOperator)
{
    auto start = START();
    NativeOmniSortOperator *sortOperator = (NativeOmniSortOperator *)jSortOperator;
    vector<Table *> outputTables;
    sortOperator->getOutput(outputTables);
    PRINT_JNI("after getOutput call elapsed time: %ld ms\n", END(start));
    jobjectArray result = tranform(env, outputTables);
    PRINT_JNI("after transform call elapsed time: %ld ms\n", END(start));
    freeOutputTable(outputTables);
    return result;
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