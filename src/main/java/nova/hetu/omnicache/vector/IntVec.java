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
import java.nio.ByteOrder;
import java.nio.IntBuffer;

public class IntVec
        extends Vec<Integer>
{
    public IntVec(int size)
    {
        super(size * Integer.BYTES);
        this.size = size;
    }

    public IntVec(ByteBuffer buffer, int length)
    {
        super(buffer, length);
    }

    @Override
    public void set(int idx, Integer value)
    {
        data.putInt(idx * Integer.BYTES, value);
    }

    @Override
    public IntVec slice(int startIdx, int endIdx)
    {
        byte[] regionData = new byte[(endIdx - startIdx) * Integer.BYTES];
        IntVec newVec = new IntVec((endIdx - startIdx));
        data.reset();
        data.get(regionData, 0, regionData.length);
        newVec.data.put(regionData);
        return newVec;
    }

    @Override
    public void addValues(Integer[] values)
    {
        for (Integer val : values) {
            data.putInt(val);
        }
    }

    @Override
    public Integer get(int idx)
    {
        return data.getInt(idx * Integer.BYTES);
    }

    @Override
    public Vec hash()
    {
        return null;
    }

    @Override
    public Vec mul(Integer other)
    {
        base.mul(OMVectorBase.INT_DATA_TYPE, data, other.intValue());
        return this;
    }

    @Override
    public Vec mmul(Vec<Integer> other)
    {
        return null;
    }

    @Override
    public Vec filter()
    {
        return null;
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
    public VecType getType()
    {
        return VecType.INT;
    }

    @Override
    public Vec concat(Vec other)
    {
        ByteBuffer newBuffer = OMVectorBase.concat(this.data, other.data, this.size * Integer.BYTES, other.size * Integer.BYTES);
        newBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return new IntVec(newBuffer, this.size + other.size);
    }
}
