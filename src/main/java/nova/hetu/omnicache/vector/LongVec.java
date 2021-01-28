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

import nova.hetu.omnicache.OMVectorBase;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class LongVec
        extends Vec<Long>
    implements AutoCloseable
{

    public LongVec(int size) {
        super(size * Long.BYTES);
        this.size = size;
    }

    @Override
    public Vec hash()
    {
        return null;
    }

    @Override
    public Vec mul(Long other)
    {
        base.mul(OMVectorBase.LONG_DATA_TYPE, data, other.intValue());
        return null;
    }

    @Override
    public Vec mmul(Vec<Long> other)
    {
        return null;
    }

    @Override
    public Vec filter()
    {
        return null;
    }

    @Override
    public void set(int idx, Long value)
    {
        data.putLong(idx * Long.BYTES, value);
    }

    @Override
    public LongVec slice(int startIdx, int endIdx)
    {
        byte[] regionData = new byte[(endIdx - startIdx) * Long.BYTES];
        LongVec newVec = new LongVec((endIdx - startIdx));
        data.reset();
        data.get(regionData, 0, regionData.length);
        newVec.data.put(regionData);
        return newVec;
    }

    @Override
    public void addValues(Long[] values)
    {
        for (Long value: values) {
            data.putLong(value);
        }
    }

    @Override
    public Long get(int idx)
    {
        return data.getLong(idx * Long.BYTES);
    }

    @Override
    public Vec groupby()
    {
        return null;
    }

    @Override
    public Vec join(Vec other)
    {
        return null;
    }

    @Override
    public void close()
            throws Exception
    {
        OMVectorBase.free(data);
        data=null;
    }
}
