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

    public Vec<?>[] execute(String nativeExecId, Vec<?>[] inputs, int inputRowLength, VecType[] outputTypes)
    {
        requireNonNull(inputs, "inputs is null");
        checkArgument(inputs.length > 0, "input vector length must >0");
        ByteBuffer[] buffers = new ByteBuffer[inputs.length];
        int[] inputTypes = new int[inputs.length];
        long rowSize = inputRowLength;

        for (int idx = 0; idx < buffers.length; idx++) {
            buffers[idx] = inputs[idx].getData();
            inputTypes[idx] = inputs[idx].getType().getValue();
        }
        int[] outputTypeArr = new int[outputTypes.length];
        for (int idx = 0; idx < outputTypes.length; idx++) {
            outputTypeArr[idx] = outputTypes[idx].getValue();
        }
        OMResult result = jniWrapper.execute(nativeExecId, buffers, inputTypes, rowSize, outputTypeArr);
        return generateOMVec(result, outputTypes);
    }

    private Vec<?>[] generateOMVec(OMResult result, VecType[] outputTypes)
    {
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
