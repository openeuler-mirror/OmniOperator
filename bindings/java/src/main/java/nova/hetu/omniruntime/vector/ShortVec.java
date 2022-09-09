/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.ShortDataType;

/**
 * short vec.
 *
 * @since 2021-07-17
 */
public class ShortVec extends FixedWidthVec {
    private static final int BYTES = Short.BYTES;

    public ShortVec(int size) {
        super(size * BYTES, size, VecEncoding.OMNI_VEC_ENCODING_FLAT, ShortDataType.SHORT);
    }

    public ShortVec(VecAllocator allocator, int size) {
        super(allocator, size * BYTES, size, VecEncoding.OMNI_VEC_ENCODING_FLAT, ShortDataType.SHORT);
    }

    public ShortVec(long nativeVector) {
        super(nativeVector, ShortDataType.SHORT);
    }

    public ShortVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress,
            long nativeVectorAllocator, int capacityInBytes, int size, int offset) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator, capacityInBytes,
                size, offset, ShortDataType.SHORT);
    }

    private ShortVec(ShortVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    private ShortVec(ShortVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    /**
     * get the specified short at the specified absolute.
     *
     * @param index the element offset in vec
     * @return short value
     */
    public short get(int index) {
        return valuesBuf.getShort((index + offset) * BYTES);
    }

    /**
     * get short values from the specified position.
     *
     * @param index the position of element
     * @param length the number of element
     * @return short value array
     */
    public short[] get(int index, int length) {
        short[] target = new short[length];
        valuesBuf.getShortArray((index + offset) * BYTES, target, 0, length * BYTES);
        return target;
    }

    /**
     * Sets the specified short at the specified absolute.
     *
     * @param index the element offset in vec
     * @param value the value of vec
     */
    public void set(int index, short value) {
        valuesBuf.setShort(index * BYTES, value);
    }

    /**
     * Batch sets the specified short at the specified absolute.
     *
     * @param values the value of the element to be written
     * @param offset the element offset in vec
     * @param start the element index in values
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
        return new ShortVec(this, positions, offset, length);
    }

    @Override
    public ShortVec copyRegion(int positionOffset, int length) {
        return new ShortVec(this, positionOffset, length, false);
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return size * BYTES;
    }
}
