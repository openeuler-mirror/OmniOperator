/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.VecType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * base class of variable width vec
 *
 * @since 2021-07-17
 */
public abstract class VariableWidthVec extends Vec {
    /**
     * offsets buffer
     */
    protected  OmniBuf offsetsBuf;

    public VariableWidthVec(int capacityInBytes, int size, VecType type) {
        super(capacityInBytes, size, type);
        this.offsetsBuf = OmniBufFactory.create(
                JvmUtils.directBuffer(getValueOffsetsNative(getNativeVector()),
                        (size + 1) * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN));
    }

    public VariableWidthVec(VecAllocator allocator, int capacityInBytes, int size, VecType type) {
        super(allocator, capacityInBytes, size, type);
        this.offsetsBuf = OmniBufFactory.create(
                JvmUtils.directBuffer(getValueOffsetsNative(getNativeVector()),
                        (size + 1) * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN));
    }

    protected VariableWidthVec(Vec vec, int offset, int length, boolean isSlice) {
        super(vec, offset, length, isSlice);
        int offsetsBufCapacityInBytes;
        if (isSlice) {
            offsetsBufCapacityInBytes = vec instanceof VariableWidthVec ? ((VariableWidthVec) vec).offsetsBuf
                    .getBuffer().capacity() : 0;
        } else {
            offsetsBufCapacityInBytes = (length + 1) * Integer.BYTES;
        }

        this.offsetsBuf = OmniBufFactory.create(
                JvmUtils.directBuffer(getValueOffsetsNative(getNativeVector()), offsetsBufCapacityInBytes)
                        .order(ByteOrder.LITTLE_ENDIAN));
    }

    protected VariableWidthVec(Vec vec, int[] positions, int offset, int length) {
        super(vec, positions, offset, length);
        this.offsetsBuf = OmniBufFactory.create(
                JvmUtils.directBuffer(getValueOffsetsNative(getNativeVector()),
                        (length + 1) * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN));
    }

    protected VariableWidthVec(long nativeVector, VecType vecType) {
        super(nativeVector, vecType);
        this.offsetsBuf = OmniBufFactory.create(
                JvmUtils.directBuffer(getValueOffsetsNative(getNativeVector()),
                        (size + 1) * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN));
    }

    /**
     * get value offset buffer from native vector
     *
     * @param nativeVector native vector address
     * @return value offset buffer
     */
    protected static native long getValueOffsetsNative(long nativeVector);

    /**
     * get the offset value of the specified position
     *
     * @param index the element offset in vec
     * @return offset value
     */
    public int getValueOffset(int index) {
        return offsetsBuf.getInt((index + offset) * Integer.BYTES);
    }

    /**
     * set the offset value of the specified position
     *
     * @param index  the element offset in vec
     * @param offset offset value
     */
    public void setValueOffset(int index, int offset) {
        offsetsBuf.setInt(index * Integer.BYTES, offset);
    }

    /**
     * get the data length of the specified position
     *
     * @param index the element offset in vec
     * @return data length in bytes
     */
    public int getDataLength(int index) {
        return getValueOffset(index + 1) - getValueOffset(index);
    }

    /**
     * return null value array from 0 to size + offset length
     *
     * @return raw value of offsets
     */
    public int[] getRawValueOffset() {
        // the length of the array is size + offset, so that the caller
        // and vec can have the same offset.
        int[] rawValueOffset = new int[offset + size + 1];
        offsetsBuf.getIntArray(0, rawValueOffset, 0, rawValueOffset.length * Integer.BYTES);
        return rawValueOffset;
    }

    /**
     * get the value offset of the specified position
     *
     * @param index the offset of element
     * @param length the number of element
     * @return the offsets
     */
    public int[] getValueOffset(int index, int length) {
        int startOffset = getValueOffset(index);
        int[] offsets = new int[length + 1];
        for (int i = 1; i <= length; i++) {
            offsets[i] = getValueOffset(index + i) - startOffset;
        }
        return offsets;
    }

    /**
     * get the offsets of variablewidthvec
     *
     * @return offsets byte buffer
     */
    public ByteBuffer getOffsets() {
        return offsetsBuf.getBuffer();
    }

    /**
     * set offsets buffer
     *
     * @param buf buf of offset
     */
    public void setOffsetsBuf(byte[] buf) {
        offsetsBuf.setBytes(0, buf, 0, buf.length);
    }
}
