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
package nova.hetu.omnicache.vector;

import nova.hetu.omnicache.utils.OmniErrorType;
import nova.hetu.omnicache.utils.OmniRuntimeException;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

/**
 * Representing a floating point number
 */
public class DoubleVec
        extends Vec
{
    public DoubleVec(int size)
    {
        super(size, size * Double.BYTES);
    }

    private DoubleVec(OMChunk buf, int offset, int size)
    {
        super(buf, offset, size);
    }

    public DoubleVec(ByteBuffer buffer, int length) {super(buffer, length);}

    @Override
    public DoubleVec slice(int startIdx, int endIdx)
    {

        return new DoubleVec(this.omniChunk, startIdx * Double.BYTES + offset, endIdx);
    }

    public double get(int idx)
    {
        return this.getData().getDouble(idx * Double.BYTES + offset);
    }

    public void set(int idx, double value)
    {
        if (isWritable) {
            this.getData().putDouble(idx * Double.BYTES + offset, value);
        }
        else {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, "Not support set api");
        }
    }

    public void put(double[] values, int offset, int start, int length)
    {
        if (isWritable) {
            DoubleBuffer buffer = getData().asDoubleBuffer();
            buffer.position(offset);
            buffer.put(values, start, length);
        }
        else {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, "Not support set api");
        }
    }

    @Override
    public VecType getType()
    {
        return VecType.DOUBLE;
    }
}