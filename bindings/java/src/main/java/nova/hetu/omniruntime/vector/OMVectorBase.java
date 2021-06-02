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

import com.google.common.annotations.VisibleForTesting;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

/**
 * 1. representing a vector
 * 2. sample: create a block with off heap memory and perform a filter using bloomfiltering
 * <p>
 * need to allow seamless access to the data, e.g. access from both Java and C/C++ without any overhead
 * all primitive data types and string will be supported
 * need to consider the encoding and endian used
 * <p>
 * the only way to pass data between on-heap and off-heap is via byte buffer
 */
@VisibleForTesting
public class OMVectorBase
{
    public static final int INT_DATA_TYPE = 1;
    public static final int LONG_DATA_TYPE = 2;
    public static final int DOUBLE_DATA_TYPE = 3;
    public static final int BOOLEAN_DATA_TYPE = 4;

    static {
        System.loadLibrary("omruntime");
    }

    public OMVectorBase() {}

    /**
     * multiplies the content of the vector at the address vec_addr by a scalar value m
     *
     * @param m
     */
    public native void mul(int datatype, ByteBuffer data, int m);

    /**
     * pair wise multiply the content of the curernt vector with the parameter vector, the size of the two vectors must match
     *
     * @param data2
     */
    public native void mmul(int datatype, ByteBuffer data2);

    /**
     * aggreate the result after applying the filter
     *
     * @param filter
     * @return
     */
    public native long agg(int datatype, String filter);

    /**
     * use jemalloc for memory allocate
     *
     * @param size
     * @return
     */
    public static native ByteBuffer allocate(int size);

    /**
     * release vec memory to jemalloc
     *
     * @param address vec address
     */
    public static native void release(long address);

    /**
     * Concatenate two arrays of memory via {@link OMVectorBase#concat(ByteBuffer, ByteBuffer, int, int)}
     *
     * @param buffer1
     * @param buffer2
     * @return
     */
    public static ByteBuffer concat(ByteBuffer buffer1, ByteBuffer buffer2, int size1, int size2)
    {
        long leftAddr = ((DirectBuffer) buffer1).address();
        long rightAddr = ((DirectBuffer) buffer2).address();
        return concat(leftAddr, rightAddr, size1, size2);
    }

    private static native ByteBuffer concat(long leftAddr, long rightAddr, int leftSize, int rightSize);

    public static native void invoke(String func_id, int[] d_types, ByteBuffer[] args);

    public static native void copy(int dataType, long thisAddress, int thisSize, long otherAddress, int[] elementsToCopy, int offset, int length, int thisOffset);
}
