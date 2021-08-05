/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
    protected final ValueOffsets valueOffsets;

    public VariableWidthVec(int capacityInBytes, int size, VecType type) {
        super(capacityInBytes, size, type);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()).order(ByteOrder.LITTLE_ENDIAN));
    }

    public VariableWidthVec(VecAllocator allocator, int capacityInBytes, int size, VecType type) {
        super(allocator, capacityInBytes, size, type);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()).order(ByteOrder.LITTLE_ENDIAN));
    }

    protected VariableWidthVec(Vec vec, int offset, int length, boolean isSlice) {
        super(vec, offset, length, isSlice);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()).order(ByteOrder.LITTLE_ENDIAN));
    }

    protected VariableWidthVec(Vec vec, int[] positions, int offset, int length) {
        super(vec, positions, offset, length);
        this.valueOffsets = new ValueOffsets((getValueOffsetsNative(getNativeVector()).order(ByteOrder.LITTLE_ENDIAN)));
    }

    protected VariableWidthVec(long nativeVector, VecType vecType) {
        super(nativeVector, vecType);
        this.valueOffsets = new ValueOffsets(getValueOffsetsNative(getNativeVector()).order(ByteOrder.LITTLE_ENDIAN));
    }

    /**
     * get value offset buffer from native vector
     *
     * @param nativeVector native vector address
     * @return value offset buffer
     */
    protected static native ByteBuffer getValueOffsetsNative(long nativeVector);

    /**
     * get the offset value of the specified position
     *
     * @param index the element offset in vec
     * @return offset value
     */
    protected int getValueOffset(int index) {
        return valueOffsets.get(index);
    }

    /**
     * set the offset value of the specified position
     *
     * @param index  the element offset in vec
     * @param offset offset value
     */
    protected void setValueOffset(int index, int offset) {
        valueOffsets.set(index, offset);
    }

    public int[] getRawValueOffset() {
        int[] rawValueOffset = new int[offset + size + 1];
        valueOffsets.getOffsets(0, rawValueOffset, 0, rawValueOffset.length);
        return rawValueOffset;
    }
}
