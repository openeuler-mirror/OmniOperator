/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.LongVecType;

import java.nio.ByteBuffer;

/**
 * long vec
 *
 * @since 2021-07-17
 */
public class LongVec extends FixedWidthVec {
    private static final int BYTES = Long.BYTES;

    public LongVec(int size) {
        super(size * BYTES, size, LongVecType.LONG);
    }

    public LongVec(VecAllocator allocator, int size) {
        super(allocator, size * BYTES, size, LongVecType.LONG);
    }

    public LongVec(long nativeVector) {
        super(nativeVector, LongVecType.LONG);
    }

    public LongVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress,
                   long nativeVectorAllocator, int capacityInBytes, int size, int offset) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator, capacityInBytes,
            size, offset, LongVecType.LONG);
    }

    private LongVec(LongVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    private LongVec(LongVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    /**
     * This constructor of vector is just for shuffle compilation to pass, it will be removed later
     *
     * @param data            data of vector
     * @param capacityInBytes size in bytes of data
     */
    @Deprecated
    public LongVec(ByteBuffer data, int capacityInBytes) {
        super(capacityInBytes, data.limit(), LongVecType.LONG);
    }

    /**
     * get the specified long at the specified absolute
     *
     * @param index the element offset in vec
     * @return long value
     */
    public long get(int index) {
        return valuesBuf.getLong((index + offset) * BYTES);
    }

    /**
     * get long values from the specified position
     *
     * @param index  the position of element
     * @param length the number of element
     * @return long value array
     */
    public long[] get(int index, int length) {
        long[] target = new long[length];
        valuesBuf.getLongArray((index + offset) * BYTES, target, 0, length * BYTES);
        return target;
    }

    /**
     * Sets the specified long at the specified absolute
     *
     * @param index the element offset in vec
     * @param value the value of vec
     */
    public void set(int index, long value) {
        valuesBuf.setLong(index * BYTES, value);
    }

    /**
     * Batch sets the specified long at the specified absolute
     *
     * @param values the value of the element to be written
     * @param offset the element offset in vec
     * @param start  the element index in values
     * @param length the number of elements that need to written
     */
    public void put(long[] values, int offset, int start, int length) {
        valuesBuf.setLongArray(offset * BYTES, values, start * BYTES, length * BYTES);
    }

    @Override
    public LongVec slice(int start, int end) {
        return new LongVec(this, start, end - start, true);
    }

    @Override
    public LongVec copy() {
        return null;
    }

    @Override
    public LongVec copyPositions(int[] positions, int offset, int length) {
        return new LongVec(this, positions, offset, length);
    }

    @Override
    public LongVec copyRegion(int positionOffset, int length) {
        return new LongVec(this, positionOffset, length, false);
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return size * BYTES;
    }
}
