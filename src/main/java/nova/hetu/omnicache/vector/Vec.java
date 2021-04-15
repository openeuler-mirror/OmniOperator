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
import java.nio.ByteOrder;

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
    protected OMVectorBase base = new OMVectorBase();
    //default memory offset is zero
    protected int offset = 0;
    protected OMChunk omniChunk;
    protected int size;

    protected boolean isWritable = true;

    public Vec(int rowSize, int alloc_size)
    {
        if (rowSize == 0 && alloc_size == 0) {
            this.omniChunk = null;
            this.size = 0;
            return;
        }
        this.omniChunk = new OMChunk(OMVectorBase.allocate(alloc_size).order(ByteOrder.LITTLE_ENDIAN));
        this.size = rowSize;
    }

    /**
     * For native execute result return to java ByteBuffer data.
     *
     * @param data
     * @param length
     */
    public Vec(ByteBuffer data, int length)
    {
        this.omniChunk = new OMChunk(data);
        this.size = length;
    }

    public Vec(OMChunk buf, int offset, int length)
    {
        this.omniChunk = new OMChunk(buf);
        this.size = length;
        this.offset = offset;
        this.isWritable = false;
    }

    public boolean isWritable()
    {
        return isWritable;
    }

    public void setWritable(boolean writable)
    {
        isWritable = writable;
    }

    /**
     * Creates a vector from a slice of the underlying buffer.
     *
     * @param startIdx
     * @param endIdx
     * @return
     */
    public Vec slice(int startIdx, int endIdx)
    {
        throw new OmniRuntimeException(OmniErrorType.OMNI_UNDIFINED, "OmniVec not default slice()");
    }

    /**
     * returns the hash of all elements in the vec
     * This is an example of in-situ operations that can be implemented enabling SIMD
     *
     * @return
     */
    public Vec hash()
    {
        throw new OmniRuntimeException(OmniErrorType.OMNI_UNDIFINED, "OmniVec not default hash()");
    }

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public Vec mul(int other)
    {
        throw new OmniRuntimeException(OmniErrorType.OMNI_UNDIFINED, "OmniVec not default mul()");
    }

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public Vec mmul(Vec other)
    {
        throw new OmniRuntimeException(OmniErrorType.OMNI_UNDIFINED, "OmniVec not default mmul()");
    }

    /**
     * Another potential SIMD in-situ operation
     *
     * @return
     */
    public Vec filter()
    {
        throw new OmniRuntimeException(OmniErrorType.OMNI_UNDIFINED, "OmniVec not default filter()");
    }

    /**
     * Another potential SIMD in-situ operation
     */
    public Vec groupby(/** how to pass in group by parameters? the columns to be used for group by */)
    {
        throw new OmniRuntimeException(OmniErrorType.OMNI_UNDIFINED, "OmniVec not default groupby()");
    }

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public Vec join(Vec other /** how to pass in the join conditions? might require many other columns*/)
    {
        throw new OmniRuntimeException(OmniErrorType.OMNI_UNDIFINED, "OmniVec not default join()");
    }

    /**
     * Another potential SIMD in-situ operation
     *
     * @param other
     * @return
     */
    public Vec concat(Vec other)
    {
        throw new OmniRuntimeException(OmniErrorType.OMNI_UNDIFINED, "OmniVec not default concat()");
    }

    public int size()
    {
        return size;
    }

    public int capacity()
    {
        if (omniChunk == null) {
            return 0;
        }
        //Todo:now since we use the reference count a ByteBuffer,so this may not accuracy of calculate the capacity.
        return getData().capacity();
//        throw new OmniRuntimeException(OmniErrorType.OMNI_UNDIFINED, "OmniVec not default capacity()");
    }

    public int remaining()
    {
        throw new OmniRuntimeException(OmniErrorType.OMNI_UNDIFINED, "OmniVec not default remaining()");
    }

    public void copy(Vec other, int[] elementsToCopy, int offset, int length, int thisOffset)
    {
        OMVectorBase.copy(this.getType().getValue(), this.getAddress(), this.size, other.getAddress(), elementsToCopy, offset, length, thisOffset);
    }

    public abstract VecType getType();

    public ByteBuffer getData()
    {
        if (omniChunk == null) {
            return null;
        }
        return this.omniChunk.getData();
    }

    public long getAddress()
    {
        if (omniChunk == null) {
            return 0;
        }
        return this.omniChunk.getAddress() + offset;
    }

    public boolean close()
    {
        if (omniChunk == null) {
            return true;
        }
        return this.omniChunk.release();
    }

    //For OmniFilter result selected row size
    public void setSize(int size)
    {
        this.size = size;
    }
}
