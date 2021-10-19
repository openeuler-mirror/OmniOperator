/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.ShortVecType;

import java.nio.ShortBuffer;

/**
 * short vec
 *
 * @since 2021-07-17
 */
public class ShortVec extends FixedWidthVec {
    private static final int BYTES = Short.BYTES;

    public ShortVec(int size) {
        super(size * BYTES, size, ShortVecType.SHORT);
    }

    public ShortVec(VecAllocator allocator, int size) {
        super(allocator, size * BYTES, size, ShortVecType.SHORT);
    }

    public ShortVec(long nativeVector) {
        super(nativeVector, ShortVecType.SHORT);
    }

    public ShortVec(long nativeVector, long nativeVectorAllocator, int capacityInBytes, int size, int offset) {
        super(nativeVector, nativeVectorAllocator, capacityInBytes, size, offset, ShortVecType.SHORT);
    }

    private ShortVec(ShortVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    /**
     * get the specified short at the specified absolute
     *
     * @param index the element offset in vec
     * @return int value
     */
    public short get(int index) {
        return valuesBuf.getShort((index + offset) * BYTES);
    }

    /**
     * Sets the specified short at the specified absolute
     *
     * @param index the element offset in vec
     * @param value the value of vec
     */
    public void set(int index, short value) {
        valuesBuf.setShort(index * BYTES, value);
    }

    /**
     * Batch sets the specified short at the specified absolute
     *
     * @param values the value of the element to be written
     * @param offset the element offset in vec
     * @param start  the element index in values
     * @param length the number of elements that need to written
     */
    public void put(short[] values, int offset, int start, int length) {
        valuesBuf.setShortArray(offset * BYTES, values, start * BYTES, length * BYTES);
    }

    @Override
    public ShortVec slice(int start, int end) {
        return new ShortVec(this, start, end - start, true);
    }

    @Override
    public ShortVec copy() {
        return null;
    }

    @Override
    public ShortVec copyPositions(int[] positions, int offset, int length) {
        return null;
    }

    @Override
    public ShortVec copyRegion(int positionOffset, int length) {
        return null;
    }
}
