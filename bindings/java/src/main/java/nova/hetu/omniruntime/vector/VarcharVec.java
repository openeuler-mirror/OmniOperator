/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */
package nova.hetu.omniruntime.vector;

import static nova.hetu.omniruntime.constants.VecType.OMNI_VEC_TYPE_VARCHAR;

/**
 *
 * varchar vec
 *
 * @since 2021-07-17
 */
public class VarcharVec
        extends VariableWidthVec {
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

    private int lastOffsetPosition = -1;

    public VarcharVec(int capacityInBytes, int size) {
        super(capacityInBytes, size, OMNI_VEC_TYPE_VARCHAR);
    }

    public VarcharVec(VecAllocator allocator, int capacityInBytes, int size) {
        super(allocator, capacityInBytes, size, OMNI_VEC_TYPE_VARCHAR);
    }

    private VarcharVec(VarcharVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    private VarcharVec(VarcharVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    public VarcharVec(long nativeVector) {
        super(nativeVector);
    }

    /**
     * get the specified bytes at the specified absolute
     *
     * @param index the element offset in vec
     * @return byte array
     */
    public byte[] getValue(int index) {
        final int actualIndex = index + offset;
        final int startOffset = getValueOffset(actualIndex);
        final int dataLen = getValueOffset(actualIndex + 1) - startOffset;
        return getData(startOffset, dataLen);
    }

    /**
     * according to the specified offset and length, read data from the buffer
     * @param offsetInBytes offset bytes in buffer
     * @param length the length of the data to be read
     * @return byte array
     */
    public byte[] getData(int offsetInBytes, int length) {
        byte[] data = new byte[length];
        values.position(offsetInBytes);
        values.get(data, 0, length);
        return data;
    }

    /**
     * Sets the specified bytes at the specified absolute
     *
     * @param index the element offset in vec
     * @param value byte array
     */
    public void setValue(int index, byte[] value) {
        fillSlots(index);
        final int startOffset = getValueOffset(index);
        setValueOffset(index + 1, startOffset + value.length);
        setData(startOffset, value, 0, value.length);
        lastOffsetPosition = index;
    }

    public void put(int index, byte[] values, int offsetInArray, int[] offsets, int offsetsIndex, int length) {
        if (values == null || length == 0) {
            return;
        }
        int[] newOffsets = compactOffsets(index, offsets, offsetsIndex, length);
        int dataLength = offsets[offsetsIndex + length] - offsets[offsetsIndex];
        // set offsets
        valueOffsets.put(index, newOffsets);
        // set data
        setData(getValueOffset(index), values, offsetInArray + offsets[offsetsIndex], dataLength);
        lastOffsetPosition = index + length - 1;
    }

    private int[] compactOffsets(int index, int[] srcOffsets, int offsetIndex, int length) {
        int originalOffset = getValueOffset(index);
        int[] newOffsets = new int[length + 1];
        for (int i = 1; i <= length; i++) {
            newOffsets[i] = srcOffsets[offsetIndex + i] - srcOffsets[offsetIndex] + originalOffset;
        }
        return newOffsets;
    }

    private void fillSlots(int index) {
        for (int i = lastOffsetPosition + 1; i < index; i++) {
            setData(i, EMPTY_BYTE_ARRAY, 0, EMPTY_BYTE_ARRAY.length);
        }
        lastOffsetPosition = index - 1;
    }

    private void setData(int offsetInBytes, byte[] data, int start, int length) {
        values.position(offsetInBytes);
        values.put(data, start, length);
    }

    @Override
    public VarcharVec slice(int start, int end) {
        return new VarcharVec(this, start, end - start, true);
    }

    @Override
    public Vec copy() {
        return null;
    }

    @Override
    public VarcharVec copyPositions(int[] positions, int offset, int length) {
        return new VarcharVec(this, positions, offset, length);
    }

    @Override
    public VarcharVec copyRegion(int positionOffset, int length) {
        return new VarcharVec(this, positionOffset, length, false);
    }
}
