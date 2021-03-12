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

import com.google.common.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

    public static AtomicLong FromPoolCount = new AtomicLong(0);
    public static AtomicLong FromAllocCount = new AtomicLong(0);
    public static AtomicLong ReleaseCount = new AtomicLong(0);
    public static AtomicLong ALL_ALLOC_COUNT = new AtomicLong(0);
    private static int DEFAULT_POOL_SIZE = 1 << 20;
    public static ConcurrentLinkedQueue<ByteBuffer> BUFFERPOOL = new ConcurrentLinkedQueue();

    private static int DEFAULT_VEC_CAPACITY = 2048 * Long.BYTES;
    private static ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    static {
        System.loadLibrary("omvector");
        for (int i = 0; i < DEFAULT_POOL_SIZE; i++) {
            BUFFERPOOL.add(allocate(DEFAULT_VEC_CAPACITY).order(ByteOrder.LITTLE_ENDIAN));
        }
        executorService.scheduleAtFixedRate(() -> {
            System.out.println("Use Ring Buffer: Total alloc count:" + ALL_ALLOC_COUNT.get() + ",From pool alloc count: " + FromPoolCount.get() + ",Direct alloc count:" + FromAllocCount.get() + ",Release to pool count:" + ReleaseCount.get());
        }, 0, 5, TimeUnit.SECONDS);
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
     * Allocate the off-heap native byte buffer
     *
     * @param capacity the type of the data
     * @return
     */
    public static ByteBuffer alloc(int capacity)
    {
        ALL_ALLOC_COUNT.incrementAndGet();
        if (capacity > 128 && capacity < DEFAULT_VEC_CAPACITY) {
            ByteBuffer buffer = BUFFERPOOL.poll();
            if (buffer != null) {
                FromPoolCount.incrementAndGet();
                return buffer;
            }
        }
        FromAllocCount.incrementAndGet();
        return allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
    }

    public static native ByteBuffer allocate(int data_type);

    /**
     * Free the memory allocate via {@link OMVectorBase#alloc(int)}
     *
     * @param buffer
     * @return
     */
    public static void release(ByteBuffer buffer)
    {
        if (buffer != null) {
            if (buffer.capacity() == DEFAULT_VEC_CAPACITY && !BUFFERPOOL.add(buffer)) {
                free(buffer);
            }
            else {
                ReleaseCount.incrementAndGet();
            }
        }
    }

    public static native void free(ByteBuffer buffer);

    /**
     * Concatenate two arrays of memory via {@link OMVectorBase#concat(ByteBuffer, ByteBuffer, int, int)}
     *
     * @param buffer1
     * @param buffer2
     * @return
     */
    public static native ByteBuffer concat(ByteBuffer buffer1, ByteBuffer buffer2, int size1, int size2);

    public static native void invoke(String func_id, int[] d_types, ByteBuffer[] args);
}
