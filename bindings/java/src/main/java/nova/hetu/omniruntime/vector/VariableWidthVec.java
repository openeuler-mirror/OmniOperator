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

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class VariableWidthVec<T>
{
    protected int MAX_BUFFER_SIZE = 5 * 1024 * 1024;
    protected int[] offsets;
    protected int[] lengths;
    int lastOffsetPosition;
    private final AtomicInteger referenceCount = new AtomicInteger(0);
    protected ByteBuffer data;
    int used;
    int capacity;

    public VariableWidthVec(int capcity, int elements)
    {
        offsets = new int[elements];
        lengths = new int[elements];
        this.data = OMVectorBase.allocate(capcity).order(ByteOrder.LITTLE_ENDIAN);
        this.capacity = capcity;
        lastOffsetPosition = -1;
    }

    public VariableWidthVec(ByteBuffer buffer)
    {
        this.data = buffer;
    }

    public void incrRefCount()
    {
        this.incrRefCount(1);
    }

    public void incrRefCount(int increment)
    {
        this.referenceCount.addAndGet(increment);
    }

    public void release()
    {
        this.release(1);
    }

    public void release(int decrement)
    {
        if (referenceCount.addAndGet(-decrement) == 0) {
            close();
        }
    }

    /**
     * Creates a vector from a slice of the underlying buffer.
     *
     * @param startIdx
     * @param endIdx
     * @return
     */
    public abstract VariableWidthVec slice(int startIdx, int endIdx);

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
        return offsets.length;
    }

    public int capacity()
    {
        return capacity;
    }

    public int remaining()
    {
        return capacity - used;
    }

    public abstract VecType getType();

    public abstract ByteBuffer getData();

    public synchronized void close()
    {
        if (data != null) {
            long address = ((DirectBuffer) data).address();
            OMVectorBase.release(address);
            data = null;
        }
    }
}