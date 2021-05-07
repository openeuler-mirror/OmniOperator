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
package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class LongVec
        extends Vec
{
    public LongVec(int size)
    {
        super(size, size * Long.BYTES);
    }

    public LongVec(ByteBuffer buffer, int size)
    {
        super(buffer, size);
    }

    private LongVec(OMChunk buf, int offset, int size)
    {
        super(buf, offset, size);
    }

    public void set(int idx, long value)
    {
        if (isWritable) {
            this.getData().putLong(idx * Long.BYTES + offset, value);
        }
        else {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, "Not support set api");
        }
    }

    public void put(long[] values, int offset, int start, int length)
    {
        if (isWritable) {
            LongBuffer buffer = getData().asLongBuffer();
            buffer.position(offset);
            buffer.put(values, start, length);
        }
        else {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, "Not support set api");
        }
    }

    @Override
    public LongVec slice(int startIdx, int endIdx)
    {
        return new LongVec(this.omniChunk, startIdx * Long.BYTES + offset, endIdx - startIdx);
    }

    public long get(int idx)
    {
        return this.getData().getLong(idx * Long.BYTES + offset);
    }

    @Override
    public VecType getType()
    {
        return VecType.LONG;
    }
}
