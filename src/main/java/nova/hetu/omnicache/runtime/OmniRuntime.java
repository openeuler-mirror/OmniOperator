/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nova.hetu.omnicache.runtime;

import nova.hetu.omnicache.vector.AggType;
import nova.hetu.omnicache.vector.DoubleVec;
import nova.hetu.omnicache.vector.IntVec;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;
import nova.hetu.omnicache.vector.VecType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

public class OmniRuntime
{
    private final JniWrapper jniWrapper;
    private final ConcurrentHashMap<String, OMResult> CACHE_STATS = new ConcurrentHashMap<>();

    public OmniRuntime()
    {
        jniWrapper = new JniWrapper();
    }

    protected JniWrapper getJniWrapper()
    {
        return jniWrapper;
    }

    public String compile(String code)
    {
        return jniWrapper.compile(code);
    }

    private static int count = 0;

    /**
     * Core OmniRuntime Api, use high-performance Vectorized Execution to computing intermediate and final vectors;
     *
     * @param neid compile Api result neid
     * @param key stateful operator execution key
     * @param inputs input Vectors data
     * @param inputRowSize input vectors row size
     * @param outputTypes result output vectors data type
     * @param step For stateful operator, represent op step,currently support only two type
     * @return if step = {@link OmniOpStep#INTERMEDIATE} , omni runtime execute while result native execute id,result type is {@link String};
     * if step ={@link OmniOpStep#FINAL} omni runtime while result final execution data,result type is {@link Vec[]}
     */
    public Object execute(String neid, String key, Vec[] inputs, int inputRowSize, VecType[] outputTypes, OmniOpStep step)
    {
        ByteBuffer[] buffers = null;
        int[] inputTypes = null;

        if (inputs != null) {
            buffers = new ByteBuffer[inputs.length];
            inputTypes = new int[inputs.length];
            for (int idx = 0; idx < buffers.length; idx++) {
                buffers[idx] = inputs[idx].getData();
                inputTypes[idx] = inputs[idx].getType().getValue();
            }
        }


        int[] outputTypeArr = new int[outputTypes.length];
        for (int idx = 0; idx < outputTypes.length; idx++) {
            outputTypeArr[idx] = outputTypes[idx].getValue();
        }
        OMResult stats = CACHE_STATS.get(key);

        ByteBuffer[] statBufs = null;
        int statRowSize = 0;
        if (stats != null) {
            statBufs = stats.getBuffers();
            statRowSize = stats.getLength();

        }
        OMResult result = jniWrapper.executeV1(neid, key, buffers, inputRowSize, statBufs, statRowSize, inputTypes, outputTypeArr);
        // free inputs
//        if (inputs != null) {
//            for (Vec input : inputs) {
//                input.release();
//            }
//        }
        CACHE_STATS.put(key, result);

        Vec[] res = generateOMVec(result, outputTypes);
//        printStats(res);
        return res;
//        switch (step) {
//            case INTERMEDIATE:
//                return result.getKey();
//            case FINAL:
//                return generateOMVec(result, outputTypes);
//            default:
//                throw new IllegalArgumentException(format("Not Support OmniOpState %s", step));
//        }
    }
    private static void printStats(Vec[] result){
        long key=((LongVec)result[0]).get(0);
        long value=((LongVec)result[1]).get(0);
        System.out.println(key+" "+value);
    }

    /**
     * Get omni runtime processed results
     *
     * @param key stateful operator execution key
     * @param outputTypes result output vectors data type
     * @return omni runtime processed result,result type is {@link Vec[]}
     */
    public Object getResults(String key, VecType[] outputTypes)
    {
        int[] outputTypeArr = new int[outputTypes.length];
        for (int idx = 0; idx < outputTypes.length; idx++) {
            outputTypeArr[idx] = outputTypes[idx].getValue();
        }

//        OMResult result = jniWrapper.getFinalResult(key, outputTypeArr);

        return generateOMVec(CACHE_STATS.get(key), outputTypes);
    }

    private Vec[] generateOMVec(OMResult result, VecType[] outputTypes)
    {
        Vec[] output = new Vec[outputTypes.length];
        int length = result.getLength();
        for (int idx = 0; idx < outputTypes.length; idx++) {
            ByteBuffer vecData = result.getBuffers()[idx];
            //TODO: Need Byte Order Configurable
            vecData.order(ByteOrder.LITTLE_ENDIAN);
            switch (outputTypes[idx]) {
                case INT:
                    output[idx] = new IntVec(vecData, length);
                    break;
                case LONG:
                    output[idx] = new LongVec(vecData, length);
                    break;
                case DOUBLE:
                    output[idx] = new DoubleVec(vecData, length);
                    break;
                default:
                    throw new IllegalArgumentException(format("Not Support Vec Type %s", outputTypes[idx]));
            }
        }
        return output;
    }

    public void prepareAgg(long stageId, long operatorId, int totalChannel, int[] groupByChannels, VecType[] groupByTypes,
            int[] aggregationChannels, VecType[] aggregationTypes, AggType[] aggregationFunctionTypes,
            VecType[] returnType, VecType[] inputTypes)
    {
        int[] groupByTypeValues = transformVecType(groupByTypes);
        int[] aggTypeValues = transformVecType(aggregationTypes);
        int[] aggFunctionTypeValues = transformAggType(aggregationFunctionTypes);
        int[] outputTypeValues = transformVecType(returnType);
        int[] inputTypeValues = transformVecType(inputTypes);

        int groupByLen = groupByChannels.length;
        int groupByTypeLen = groupByTypeValues.length;
        int aggChannelLen = aggregationChannels.length;
        int aggTypeLen = aggTypeValues.length;
        int aggFunctionTypeLen = aggFunctionTypeValues.length;
        int outputTypeLen = outputTypeValues.length;
        int inputTypeLen = inputTypeValues.length;

        int size = groupByLen + groupByTypeLen + aggChannelLen + aggTypeLen + aggFunctionTypeLen + outputTypeLen + inputTypeLen;
        IntVec prepareInfo = new IntVec(size);
        int offset = 0;
        offset = transformPrepareInfoToVec(prepareInfo, groupByChannels, offset);
        offset = transformPrepareInfoToVec(prepareInfo, groupByTypeValues, offset);
        offset = transformPrepareInfoToVec(prepareInfo, aggregationChannels, offset);
        offset = transformPrepareInfoToVec(prepareInfo, aggTypeValues, offset);
        offset = transformPrepareInfoToVec(prepareInfo, aggFunctionTypeValues, offset);
        offset = transformPrepareInfoToVec(prepareInfo, outputTypeValues, offset);
        offset = transformPrepareInfoToVec(prepareInfo, inputTypeValues, offset);

        if (offset != size) {
            throw new IllegalArgumentException(format("agg prepare input info is error: %s,%s", size, offset));
        }

        try {
            jniWrapper.prepareAgg(
                    stageId,
                    operatorId,
                    size,
                    prepareInfo.getAddress(),
                    groupByLen,
                    groupByTypeLen,
                    aggChannelLen,
                    aggTypeLen,
                    aggFunctionTypeLen,
                    outputTypeLen,
                    inputTypeLen);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("execute prepare agg failed", e);
        } finally {
            prepareInfo.close();
        }
    }

    public int transformPrepareInfoToVec(IntVec prepareInfo, int[] values, int offset) {
        for (int value : values) {
            prepareInfo.set(offset++, value);
        }
        return offset;
    }

    public void executeAggIntermediate(long stageId, long operatorId, List<Vec> inputData, int columnCount)
    {
        LongVec inputDataAddr = null;
        IntVec inputRowSize = null;

        try {
            inputDataAddr = transformVecAddress(inputData);
            inputRowSize = getRowNumbers(inputData, columnCount);
            jniWrapper.executeAggIntermediate(
                    stageId,
                    operatorId,
                    inputDataAddr.getAddress(),
                    inputDataAddr.size(),
                    columnCount,
                    inputRowSize.getAddress(),
                    inputRowSize.size());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("execute agg intermediate failed.", e);
        } finally {
            if (inputDataAddr != null) {
                inputDataAddr.close();
            }
            if (inputRowSize != null) {
                inputRowSize.close();
            }
        }
    }

    public Vec[] executeAggFinal(long operatorId, VecType[] outputTypes) {
        OMResult result = jniWrapper.executeAggFinal(operatorId);
        return generateOMVec(result, outputTypes);
    }

    private IntVec getRowNumbers(List<Vec> inputs, int columnCount) {
        int totalColumn = inputs.size();
        if (totalColumn % columnCount != 0) {
            throw new IllegalArgumentException(format("input vec error: %s,%s", totalColumn, columnCount));
        }

        int pageNum = totalColumn / columnCount;
        IntVec rowNums = new IntVec(pageNum);
        for (int idx = 0; idx < pageNum; idx++) {
            rowNums.set(idx, inputs.get(idx * columnCount).size());
        }
        return rowNums;
    }

    private LongVec transformVecAddress(List<Vec> inputs) {
        LongVec address = new LongVec(inputs.size());
        for (int idx = 0; idx < inputs.size(); idx++) {
            address.set(idx, inputs.get(idx).getAddress());
        }
        return address;
    }

    private int[] transformVecType(VecType[] vecTypes) {
        int[] vecTypeValue = new int[vecTypes.length];
        for (int idx = 0; idx < vecTypes.length; idx++) {
            vecTypeValue[idx] = vecTypes[idx].getValue();
        }
        return vecTypeValue;
    }

    private int[] transformAggType(AggType[] aggTypes) {
        int[] aggTypeValue = new int[aggTypes.length];
        for (int idx = 0; idx < aggTypes.length; idx++) {
            aggTypeValue[idx] = aggTypes[idx].getValue();
        }
        return aggTypeValue;
    }
}
