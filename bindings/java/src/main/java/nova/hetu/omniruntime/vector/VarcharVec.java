/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.VarcharVecType;

/**
 * varchar vec
 *
 * @since 2021-07-17
 */
public class VarcharVec extends VariableWidthVec {
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[] {};

    private int lastOffsetPosition = -1;

    public VarcharVec(int capacityInBytes, int size) {
        super(capacityInBytes, size, VarcharVecType.VARCHAR);
    }

    public VarcharVec(VecAllocator allocator, int capacityInBytes, int size) {
        super(allocator, capacityInBytes, size, VarcharVecType.VARCHAR);
    }

    private VarcharVec(VarcharVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    private VarcharVec(VarcharVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    public VarcharVec(long nativeVector) {
        super(nativeVector, VarcharVecType.VARCHAR);
    }

    /**
     * get the specified bytes at the specified absolute
     *
     * @param index the element offset in vec
     * @return byte array
     */
    public byte[] get(int index) {
        final int startOffset = getValueOffset(index);
        final int dataLen = getValueOffset(index + 1) - startOffset;
        return getData(startOffset, dataLen);
    }

    /**
     * Batch gets the specified bytes at the specified absolute
     *
     * @param index the element offset in vec
     * @param length the number of element
     * @return byte array
     */
    public byte[] get(int index, int length) {
        final int startOffset = getValueOffset(index);
        final int dataLen = getValueOffset(index + length) - startOffset;
        return getData(startOffset, dataLen);
    }

    /**
     * according to the specified offset and length, read data from the buffer
     *
     * @param offsetInBytes offset bytes in buffer
     * @param length        the length of the data to be read
     * @return byte array
     */
    public byte[] getData(int offsetInBytes, int length) {
        return valuesBuf.getBytes(offsetInBytes, length);
    }

    /**
     * Sets the specified bytes at the specified absolute
     *
     * @param index the element offset in vec
     * @param value byte array
     */
    public void set(int index, byte[] value) {
        fillSlots(index);
        final int startOffset = getValueOffset(index);
        setValueOffset(index + 1, startOffset + value.length);
        setData(startOffset, value, 0, value.length);
        lastOffsetPosition = index;
    }

    /**
     * Batch sets the specified bytes at the specified absolute
     *
     * @param index the value of the element to be written
     * @param values the bytes array
     * @param offsetInArray the element offset in bytes array
     * @param offsets offsets array
     * @param offsetsIndex the offset of the offsets array
     * @param length the number of elements
     */
    public void put(int index, byte[] values, int offsetInArray, int[] offsets, int offsetsIndex, int length) {
        if (values == null || length == 0) {
            return;
        }
        fillSlots(index);
        int startOffset = getValueOffset(index);
        int[] newOffsets = compactOffsets(startOffset, offsets, offsetsIndex, length);
        int dataLength = offsets[offsetsIndex + length] - offsets[offsetsIndex];
        // set offsets
        offsetsBuf.setIntArray((index + 1) * Integer.BYTES, newOffsets, Integer.BYTES,
                (newOffsets.length - 1) * Integer.BYTES);
        // set data
        setData(startOffset, values, offsetInArray + offsets[offsetsIndex], dataLength);
        lastOffsetPosition = index + length - 1;
    }

    private int[] compactOffsets(int startOffset, int[] srcOffsets, int offsetIndex, int length) {
        int[] newOffsets = new int[length + 1];
        for (int i = 1; i <= length; i++) {
            newOffsets[i] = srcOffsets[offsetIndex + i] - srcOffsets[offsetIndex] + startOffset;
        }
        return newOffsets;
    }

    private void fillSlots(int index) {
        for (int i = lastOffsetPosition + 1; i < index; i++) {
            int startOffset = getValueOffset(i);
            setValueOffset(i + 1, startOffset);
        }
        lastOffsetPosition = index - 1;
    }

    private void setData(int offsetInBytes, byte[] data, int start, int length) {
        valuesBuf.setBytes(offsetInBytes, data, start, length);
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
