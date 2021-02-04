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

import nova.hetu.omnicache.OMVectorBase;
import nova.hetu.omnicache.vector.DoubleVec;
import nova.hetu.omnicache.vector.IntVec;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;
import nova.hetu.omnicache.vector.VecType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OmniRuntime {
    private JniWrapper jniWrapper;

    public OmniRuntime() {
        jniWrapper = new JniWrapper();
    }

    public String compile(String code) {
        return jniWrapper.compile(code);
    }

    /**
     * Core OmniRuntime Api, use high-performance Vectorized Execution to computing intermediate and final vectors;
     *
     * @param neid         compile Api result neid
     * @param key          stateful operator execution key
     * @param inputs       input Vectors data
     * @param inputRowSize input vectors row size
     * @param outputTypes  result output vectors data type
     * @param step         For stateful operator, represent op step,currently support only two type
     * @return if step = {@link OmniOpStep#INTERMEDIATE} , omni runtime execute while result native execute id,result type is {@link String};
     * if step ={@link OmniOpStep#FINAL} omni runtime while result final execution data,result type is {@link Vec<?>[]}
     */
    public Object execute(String neid, String key, Vec<?>[] inputs, int inputRowSize, VecType[] outputTypes, OmniOpStep step) {
        ByteBuffer[] buffers = null;
        int[] inputTypes = null;
        long rowSize = inputRowSize;

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

        OMResult result = jniWrapper.execute(neid, key, buffers, inputTypes, rowSize, outputTypeArr, step.getState());
        // free inputs
        if (inputs != null) {
            for (int idx = 0; idx < inputs.length; ++idx) {
                OMVectorBase.free(inputs[idx].getData());
            }
        }
        switch (step) {
            case INTERMEDIATE:
                return result.getKey();
            case FINAL:
                return generateOMVec(result, outputTypes);
            default:
                throw new IllegalArgumentException(format("Not Support OmniOpState %s", step));
        }
    }

    private Vec<?>[] generateOMVec(OMResult result, VecType[] outputTypes) {
        Vec<?>[] output = new Vec[outputTypes.length];
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
}
