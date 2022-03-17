/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DataType;

import static nova.hetu.omniruntime.vector.VecEncoding.OMNI_VEC_ENCODING_FLAT;

/**
 * base class of variable width vec
 *
 * @since 2021-07-17
 */
public abstract class VariableWidthVec extends Vec {
    /**
     * offsets buffer
     */
    protected OmniBuf offsetsBuf;

    /**
     * last set index
     */
    protected int lastOffsetPosition = -1;

    public VariableWidthVec(int capacityInBytes, int size, DataType type) {
        super(capacityInBytes, size, OMNI_VEC_ENCODING_FLAT, type);
        this.offsetsBuf = OmniBufFactory.create(getValueOffsetsNative(getNativeVector()), (size + 1) * Integer.BYTES);
    }

    public VariableWidthVec(VecAllocator allocator, int capacityInBytes, int size, DataType type) {
        super(allocator, capacityInBytes, size, OMNI_VEC_ENCODING_FLAT, type);
        this.offsetsBuf = OmniBufFactory.create(getValueOffsetsNative(getNativeVector()), (size + 1) * Integer.BYTES);
    }

    protected VariableWidthVec(Vec vec, int offset, int length, boolean isSlice) {
        super(vec, offset, length, isSlice);
        int offsetsBufCapacityInBytes;
        if (isSlice) {
            offsetsBufCapacityInBytes = vec instanceof VariableWidthVec
                ? ((VariableWidthVec) vec).offsetsBuf.getCapacity()
                : 0;
        } else {
            offsetsBufCapacityInBytes = (length + 1) * Integer.BYTES;
        }

        this.offsetsBuf = OmniBufFactory.create(getValueOffsetsNative(getNativeVector()), offsetsBufCapacityInBytes);
    }

    protected VariableWidthVec(Vec vec, int[] positions, int offset, int length) {
        super(vec, positions, offset, length);
        this.offsetsBuf = OmniBufFactory.create(getValueOffsetsNative(getNativeVector()), (length + 1) * Integer.BYTES);
    }

    protected VariableWidthVec(long nativeVector, DataType dataType) {
        super(nativeVector, dataType);
        this.offsetsBuf = OmniBufFactory.create(getValueOffsetsNative(getNativeVector()), (size + 1) * Integer.BYTES);
    }

    protected VariableWidthVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress,
                               long nativeVectorOffsetBufAddress, long nativeVectorAllocator, int capacityInBytes,
                               int size, int offset, DataType dataType) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator, capacityInBytes,
            size, offset, dataType);
        this.offsetsBuf = OmniBufFactory.create(nativeVectorOffsetBufAddress, (size + 1) * Integer.BYTES);
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
     * @param index  the offset of element
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
    public OmniBuf getOffsetsBuf() {
        return offsetsBuf;
    }

    /**
     * set offsets buffer
     *
     * @param buf buf of offset
     */
    public void setOffsetsBuf(byte[] buf) {
        offsetsBuf.setBytes(0, buf, 0, buf.length);
    }

    @Override
    public void setNull(int index) {
        fillSlots(index);
        nullsBuf.setByte(index + offset, (byte) 1);
        setValueOffset(index + 1, getValueOffset(index));
        lastOffsetPosition = index;
    }

    /**
     * fill offset
     *
     * @param index index of want to set
     */
    protected void fillSlots(int index) {
        for (int i = lastOffsetPosition + 1; i < index; i++) {
            setValueOffset(i + 1, getValueOffset(i));
        }
        lastOffsetPosition = index - 1;
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return getValueOffset(size) - getValueOffset(0);
    }

    /**
     * returns the number of bytes of the offsets
     *
     * @return length in bytes
     */
    @Override
    public int getRealOffsetBufCapacityInBytes() {
        return (size + 1) * Integer.BYTES;
    }
}
