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

/**
 * wrapper of the off-heap values to be used by blocks, this is also the place to implement vectorized operations.
 * each subclass implements its own vectorized operation as appropriate
 *
 * The design purpose is to enable:
 * 1. SIMD
 * 2. method fusion (e.g. function invocation is merged into 1 single function
 *
 * Each supported data type will subclass this class to create the type specific operations
 */
public abstract class Vec<T>
{
    protected ByteBuffer data;
    protected OMVectorBase base = new OMVectorBase();

    public Vec(int raw_size) {
        data = OMVectorBase.allocate(raw_size).order(ByteOrder.LITTLE_ENDIAN);
    }

    public Vec(ByteBuffer data, int length) {
        this.data = data;
        this.size = length;
    }

    protected int size = 0;

    public abstract void set(int idx, T value);

    /**
     * Creates a vector from a slice of the underlying buffer.
     * @param startIdx
     * @param endIdx
     * @return
     */
    public abstract Vec<T> slice(int startIdx, int endIdx);

    public abstract void addValues(T[] values);

    public abstract T get(int idx);

    /**
     * returns the hash of all elements in the vec
     * This is an example of in-situ operations that can be implemented enabling SIMD
     * @return
     *
     */
    public abstract Vec hash();

    /**
     * Another potential SIMD in-situ operation
     * @param other
     * @return
     */
    public abstract Vec mul(T other);

    /**
     * Another potential SIMD in-situ operation
     * @param other
     * @return
     */
    public abstract Vec mmul(Vec<T> other);

    /**
     * Another potential SIMD in-situ operation
     * @return
     */
    public abstract Vec filter();

    /**
     * Another potential SIMD in-situ operation
     */
    public abstract Vec groupby(/** how to pass in group by parameters? the columns to be used for group by */);

    /**
     * Another potential SIMD in-situ operation
     * @param other
     * @return
     */
    public abstract Vec join(Vec other /** how to pass in the join conditions? might require many other columns*/);

    public int size()
    {
        return size;
    }

    public int capacity()
    {
        return data.capacity();
    }
    public int remaining()
    {
        return data.remaining();
    }

    public abstract VecType getType();

    public ByteBuffer getData()
    {
        return this.data;
    }
}
