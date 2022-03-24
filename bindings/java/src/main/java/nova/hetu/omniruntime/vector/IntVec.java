/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.IntDataType;

/**
 * int vec
 *
 * @since 2021-07-17
 */
public class IntVec extends FixedWidthVec {
    private static final int BYTES = Integer.BYTES;

    public IntVec(int size) {
        super(size * BYTES, size, VecEncoding.OMNI_VEC_ENCODING_FLAT, IntDataType.INTEGER);
    }

    public IntVec(VecAllocator allocator, int size) {
        super(allocator, size * BYTES, size, VecEncoding.OMNI_VEC_ENCODING_FLAT, IntDataType.INTEGER);
    }

    public IntVec(long nativeVector) {
        super(nativeVector, IntDataType.INTEGER);
    }

    public IntVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress,
            long nativeVectorAllocator, int capacityInBytes, int size, int offset) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator, capacityInBytes,
                size, offset, IntDataType.INTEGER);
    }

    private IntVec(IntVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    private IntVec(IntVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    /**
     * get the specified integer at the specified absolute
     *
     * @param index the element offset in vec
     * @return int value
     */
    public int get(int index) {
        return valuesBuf.getInt((index + offset) * BYTES);
    }

    /**
     * get int values from the specified position
     *
     * @param index the position of element
     * @param length the number of element
     * @return int value array
     */
    public int[] get(int index, int length) {
        int[] target = new int[length];
        valuesBuf.getIntArray((index + offset) * BYTES, target, 0, length * BYTES);
        return target;
    }

    /**
     * Sets the specified integer at the specified absolute
     *
     * @param index the element offset in vec
     * @param value the value of vec
     */
    public void set(int index, int value) {
        valuesBuf.setInt(index * BYTES, value);
    }

    /**
     * Batch sets the specified integer at the specified absolute
     *
     * @param values the value of the element to be written
     * @param offset the element offset in vec
     * @param start the element index in values
     * @param length the number of elements that need to written
     */
    public void put(int[] values, int offset, int start, int length) {
        valuesBuf.setIntArray(offset * BYTES, values, start * BYTES, length * BYTES);
    }

    @Override
    public IntVec slice(int start, int end) {
        return new IntVec(this, start, end - start, true);
    }

    @Override
    public IntVec copy() {
        return null;
    }

    @Override
    public IntVec copyPositions(int[] positions, int offset, int length) {
        return new IntVec(this, positions, offset, length);
    }

    @Override
    public IntVec copyRegion(int positionOffset, int length) {
        return new IntVec(this, positionOffset, length, false);
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return size * BYTES;
    }
}
