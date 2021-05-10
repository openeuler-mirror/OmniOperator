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
package nova.hetu.omniruntime.operator;

import java.nio.ByteBuffer;

public class JniWrapper
{
    public enum OperatorType{
        HASH_AGGREGATION(0);
        private int value;
        OperatorType(int v){
            this.value = v;
        }

        public int getValue()
        {
            return value;
        }
    };
    static {
        System.loadLibrary("omruntime");
    }
    public native String compile(String code);
    public native OMResult executeV1(String function, String key, ByteBuffer[] inputs, long rowNum, ByteBuffer[] stats, long statRowNum, int[] inpuTypes, int[] outputTypes);
    public native OMResult execute(String function, String key, ByteBuffer[] inputs, int[] inpuTypes, long rowNum, int[] outputTypes);
    public native OMResult getFinalResult(String key, int[] outputTypes);
    public native long prepareAgg(long prepareInfoAddress, int groupByLen, int groupByTypeLen, int aggregationChannelLen, int aggregationTypeLen,
            int aggregationFunctionTypeLen, int outputTypeLen);
    public native long createOperator(long moduleId, int size, long prepareInfoAddress, int groupByLen,
                                  int groupByTypeLen, int aggregationChannelLen, int aggregationTypeLen,
                                  int aggregationFunctionTypeLen, int outputTypeLen);
    public native long executeAggIntermediate(long operatorId, long dataAddress, long totalColumn, int columnCount, long rowAddress, int rowNum, long inputTypeAdress);
//    public native OMResult executeAggFinal(long operatorId);
    public native OMResult[] executeAggFinal(long operatorId);
    public native long createSortOperator(long factoryAddress);
    public native void addSortInput(long operatorAddress, long[] dataAddrs, int[] positionCounts, int pageCount);
    public native OMResult[] getSortOutput(long operatorAddress);
    public native long filterCompile(String expression, long clsTypeAddress, int inputVecCount);
    public native int filterExecute(long handle, long[] inputVecArrayAddress, long clsTypeAddress, int inputVecCount, long selectedPositionsAddress, int inputRowSize);
    public native int filterExecuteV1(long handle, long[] inputVecArrayAddress, long clsTypeAddress, int inputVecCount, int inputRowSize,long[] projectVecArrayAddress,int[] projectIdx,int projectVecCount);
    public native void filterFinished(long handle);

    // Only createOperatorFactory JNIs have difference
    public native long createSortOperatorFactory(int[] sourceTypes, int[] outputCols, int[] sortCols, int[] ascendings, int[] nullFirsts);
    public native long createHashAggregationOperatorFactory(int[] groupByChanel, int[] groupByTypes, int[] aggChannels, int[] aggTypes, int[] aggFunctionTypes, int[] aggOutputTypes);
    // createOperator
    public native long createOperator(long factoryAddress, int operatorType);
    // addInput
    public native long addInput(long operatorAddress, long[] dataAddress, int[] positionCount, int pageCount);
    // getOutput
    public native OMResult[] getOutput(long operatorAddress);
}
