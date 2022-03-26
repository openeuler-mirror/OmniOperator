/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DoubleDataType;

/**
 * double vec.
 *
 * @since 2021-07-17
 */
public class DoubleVec extends FixedWidthVec {
    private static final int BYTES = Double.BYTES;

    public DoubleVec(int size) {
        super(size * BYTES, size, VecEncoding.OMNI_VEC_ENCODING_FLAT, DoubleDataType.DOUBLE);
    }

    public DoubleVec(VecAllocator allocator, int size) {
        super(allocator, size * BYTES, size, VecEncoding.OMNI_VEC_ENCODING_FLAT, DoubleDataType.DOUBLE);
    }

    public DoubleVec(long nativeVector) {
        super(nativeVector, DoubleDataType.DOUBLE);
    }

    public DoubleVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress,
            long nativeVectorAllocator, int capacityInBytes, int size, int offset) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator, capacityInBytes,
                size, offset, DoubleDataType.DOUBLE);
    }

    private DoubleVec(DoubleVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    private DoubleVec(DoubleVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    /**
     * get the specified double at the specified absolute.
     *
     * @param index the element offset in vec
     * @return double value
     */
    public double get(int index) {
        return valuesBuf.getDouble((index + offset) * BYTES);
    }

    /**
     * get double values from the specified position.
     *
     * @param index the position of element
     * @param length the number of element
     * @return double value array
     */
    public double[] get(int index, int length) {
        double[] target = new double[length];
        valuesBuf.getDoubleArray((index + offset) * BYTES, target, 0, length * BYTES);
        return target;
    }

    /**
     * Sets the specified double at the specified absolute.
     *
     * @param index the element offset in vec
     * @param value the value of vec
     */
    public void set(int index, double value) {
        valuesBuf.setDouble(index * BYTES, value);
    }

    /**
     * Batch sets the specified double at the specified absolute.
     *
     * @param values the value of the element to be written
     * @param offset the element offset in vec
     * @param start the element index in values
     * @param length the number of elements that need to written
     */
    public void put(double[] values, int offset, int start, int length) {
        valuesBuf.setDoubleArray(offset * BYTES, values, start * BYTES, length * BYTES);
    }

    @Override
    public DoubleVec slice(int start, int end) {
        return new DoubleVec(this, start, end - start, true);
    }

    @Override
    public DoubleVec copy() {
        return null;
    }

    @Override
    public DoubleVec copyPositions(int[] positions, int offset, int length) {
        return new DoubleVec(this, positions, offset, length);
    }

    @Override
    public DoubleVec copyRegion(int positionOffset, int length) {
        return new DoubleVec(this, positionOffset, length, false);
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return size * BYTES;
    }
}
