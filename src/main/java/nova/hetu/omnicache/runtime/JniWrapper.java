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

import java.nio.ByteBuffer;

public class JniWrapper
{
    static {
        System.loadLibrary("joy");
        System.loadLibrary("omvector");
    }

    public native String compile(String code);

    public native OMResult executeV1(String function, String key, ByteBuffer[] inputs, long rowNum, ByteBuffer[] stats, long statRowNum, int[] inpuTypes, int[] outputTypes);

    public native OMResult execute(String function, String key, ByteBuffer[] inputs, int[] inpuTypes, long rowNum, int[] outputTypes);

    public native OMResult getFinalResult(String key, int[] outputTypes);

    public native void prepareAgg(String operatorId,int totalChannel, int[] groupByChanel, int[] groupByTypes,
            int[] aggregationChannels, int[] aggregationTypes, int[] aggregationFunctionTypes, int[] outputTypes);

    public native void executeAggIntermediate(String operatorId, ByteBuffer[] inputData, int[] inputTypes, long rowNum);

    public native OMResult executeAggFinal(String operatorId);

    public native long allocAndInitSort(int[] sourceTypes, int typeCount, int[] outputCols, int outputColCount, int[] sortCols, int[] ascendings, int[] nullFirsts, int sortColCount);

    public native void addTable(long sortAddress, long[] inputAddrs, long nulls, int columnNum, long rowNum);

    public native long sort(long sortAddress);

    public native OMResult getResult(long sortAddress, long addressesAddr);
}
