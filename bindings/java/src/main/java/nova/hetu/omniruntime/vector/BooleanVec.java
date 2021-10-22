/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.BooleanVecType;

/**
 * boolean vec
 *
 * @since 2021-07-17
 */
public class BooleanVec extends FixedWidthVec {
    private static final int BYTES = 1;

    public BooleanVec(int size) {
        super(size * BYTES, size, BooleanVecType.BOOLEAN);
    }

    public BooleanVec(VecAllocator allocator, int size) {
        super(allocator, size * BYTES, size, BooleanVecType.BOOLEAN);
    }

    public BooleanVec(long nativeVector) {
        super(nativeVector, BooleanVecType.BOOLEAN);
    }

    public BooleanVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress,
                      long nativeVectorAllocator, int capacityInBytes, int size, int offset) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator, capacityInBytes,
            size, offset, BooleanVecType.BOOLEAN);
    }

    private BooleanVec(BooleanVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    private BooleanVec(BooleanVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    /**
     * Sets the specified boolean at the specified absolute
     *
     * @param index the element offset in vec
     * @param value the value of vec
     */
    public void set(int index, boolean value) {
        valuesBuf.setByte(index, value ? (byte) 1 : (byte) 0);
    }

    /**
     * get the specified boolean at the specified absolute
     *
     * @param index the element offset in vec
     * @return if the value of 1 returns true, otherwise it returns false
     */
    public boolean get(int index) {
        return valuesBuf.getByte((index + offset) * BYTES) == 1;
    }

    /**
     * get boolean values from the specified position
     *
     * @param index  the position of element
     * @param length the number of element
     * @return boolean value array
     */
    public boolean[] get(int index, int length) {
        byte[] target = valuesBuf.getBytes((index + offset) * BYTES, length);
        return transformByteToBoolean(target, 0, length);
    }

    /**
     * Batch sets the specified boolean at the specified absolute
     *
     * @param values the value of the element to be written
     * @param offset the element offset in vec
     * @param start  the element index in values
     * @param length the number of elements that need to written
     */
    public void put(boolean[] values, int offset, int start, int length) {
        byte[] data = transformBooleanToByte(values, start, length);
        valuesBuf.setBytes(offset, data, 0, length);
    }

    @Override
    public BooleanVec slice(int startIdx, int endIdx) {
        return new BooleanVec(this, startIdx, endIdx - startIdx, true);
    }

    @Override
    public Vec copy() {
        return null;
    }

    @Override
    public BooleanVec copyPositions(int[] positions, int offset, int length) {
        return new BooleanVec(this, positions, offset, length);
    }

    @Override
    public BooleanVec copyRegion(int positionOffset, int length) {
        return new BooleanVec(this, positionOffset, length, false);
    }
}
