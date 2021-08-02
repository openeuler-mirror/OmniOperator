/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
package nova.hetu.omniruntime.vector;

import java.nio.DoubleBuffer;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_DOUBLE;

/**
 * double vec
 *
 * @since 2021-07-17
 */
public class DoubleVec
        extends FixedWidthVec {
    private static final int BYTES = Double.BYTES;

    public DoubleVec(int size) {
        super(size * BYTES, size, OMNI_VEC_TYPE_DOUBLE);
    }

    public DoubleVec(VecAllocator allocator, int size) {
        super(allocator, size * BYTES, size, OMNI_VEC_TYPE_DOUBLE);
    }

    public DoubleVec(long nativeVector) {
        super(nativeVector);
    }

    private DoubleVec(DoubleVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    private DoubleVec(DoubleVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    /**
     *  get the specified double at the specified absolute
     *
     * @param index the element offset in vec
     * @return double value
     */
    public double get(int index) {
        return values.getDouble((index + offset) * BYTES);
    }

    /**
     * Sets the specified double at the specified absolute
     *
     * @param index the element offset in vec
     * @param value the value of vec
     */
    public void set(int index, double value) {
        values.putDouble(index * BYTES, value);
    }

    /**
     * Batch sets the specified double at the specified absolute
     *
     * @param values the value of the element to be written
     * @param offset the element offset in vec
     * @param start the element index in values
     * @param length the number of elements that need to written
     */
    public void put(double[] values, int offset, int start, int length) {
        DoubleBuffer buffer = this.values.asDoubleBuffer();
        buffer.position(offset);
        buffer.put(values, start, length);
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
}
