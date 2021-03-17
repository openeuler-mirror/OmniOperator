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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * wrapper of the off-heap values to be used by blocks, this is also the place to implement vectorized operations.
 * each subclass implements its own vectorized operation as appropriate
 * <p>
 * The design purpose is to enable:
 * 1. SIMD
 * 2. method fusion (e.g. function invocation is merged into 1 single function
 * <p>
 * Each supported data type will subclass this class to create the type specific operations
 */
public abstract class Vec
{
    protected ByteBuffer data;
    protected OMVectorBase base = new OMVectorBase();
    private final AtomicInteger referenceCount = new AtomicInteger(0);
    protected int size;

    public Vec(int rowSize, int alloc_size)
    {
        this.data = OMVectorBase.allocate(alloc_size).order(ByteOrder.LITTLE_ENDIAN);
        this.size = rowSize;
    }

    public Vec(ByteBuffer data, int length)
    {
        this.data = data;
        this.size = length;
    }

    /**
     * Creates a vector from a slice of the underlying buffer.
     *
     * @param startIdx
     * @param endIdx
     * @return
     */
    public abstract Vec slice(int startIdx, int endIdx);

    /**
     * returns the hash of all elements in the vec
     * This is an example of in-situ operations that can be implemented enabling SIMD
     *
     * @return
     */
    public abstract Vec hash();

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public abstract Vec mul(int other);

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public abstract Vec mmul(Vec other);

    /**
     * Another potential SIMD in-situ operation
     *
     * @return
     */
    public abstract Vec filter();

    /**
     * Another potential SIMD in-situ operation
     */
    public abstract Vec groupby(/** how to pass in group by parameters? the columns to be used for group by */);

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public abstract Vec join(Vec other /** how to pass in the join conditions? might require many other columns*/);

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public abstract Vec concat(Vec other);

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

    public void close()
    {
        if (data != null) {
            OMVectorBase.release(data);
            data = null;
        }
    }
}
