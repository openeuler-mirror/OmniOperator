/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
        return getValues().getShort((index + offset) * BYTES);
    }

    /**
     * Sets the specified short at the specified absolute
     *
     * @param index the element offset in vec
     * @param value the value of vec
     */
    public void set(int index, short value) {
        getValues().putShort(index * BYTES, value);
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
        ShortBuffer buffer = getValues().asShortBuffer();
        buffer.position(offset);
        buffer.put(values, start, length);
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
