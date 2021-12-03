/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_PARAM_ERROR;

/**
 * base class of decimal vec
 *
 * @since 2021-07-17
 */
public abstract class DecimalVec extends FixedWidthVec {
    private final int typeWidth;

    public DecimalVec(int size, int typeLength, VecType type) {
        super(size * typeLength, size, type);
        this.typeWidth = getTypeWidth(typeLength);
    }

    public DecimalVec(VecAllocator allocator, int size, int typeLength, VecType type) {
        super(allocator, size * typeLength, size, type);
        this.typeWidth = getTypeWidth(typeLength);
    }

    public DecimalVec(long nativeVector, int typeLength, VecType type) {
        super(nativeVector, type);
        this.typeWidth = getTypeWidth(typeLength);
    }

    public DecimalVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress,
                      long nativeVectorAllocator, int capacityInBytes, int size, int offset, int typeLength,
                      VecType type) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator, capacityInBytes,
            size, offset, type);
        this.typeWidth = getTypeWidth(typeLength);
    }

    protected DecimalVec(DecimalVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
        this.typeWidth = vector.typeWidth;
    }

    protected DecimalVec(DecimalVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
        this.typeWidth = vector.typeWidth;
    }

    private int getTypeWidth(int typeLength) {
        return typeLength / Long.BYTES;
    }

    /**
     * get the specified decimal at the specified absolute
     *
     * @param index the element offset in vec
     * @return decimal value, from high to low
     */
    public long[] get(int index) {
        long[] value = new long[this.typeWidth];
        int offset = (this.offset + index) * this.typeWidth;
        for (int i = 0; i < this.typeWidth; i++) {
            value[i] = valuesBuf.getLong((offset + i) * Long.BYTES);
        }
        return value;
    }

    /**
     * Sets the specified decimal value at the specified absolute
     *
     * @param index the element offset in vec
     * @param value the value of the element to be written, from high to low
     */
    public void set(int index, long[] value) {
        int offset = index * this.typeWidth;
        for (int i = 0; i < this.typeWidth; i++) {
            valuesBuf.setLong((offset + i) * Long.BYTES, value[i]);
        }
    }

    /**
     * Batch sets the specified long at the specified absolute
     *
     * @param values the value of the element to be written
     * @param offset the element offset in vec
     * @param start  the element index in values
     * @param length the number of elements that need to written
     */
    public void put(long[] values, int offset, int start, int length) {
        if (length % this.typeWidth != 0) {
            throw new OmniRuntimeException(OMNI_PARAM_ERROR, "length " + length + "is error.");
        }
        valuesBuf.setLongArray(offset * typeWidth * Long.BYTES, values, start * Long.BYTES, length * Long.BYTES);
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return size * typeWidth * Long.BYTES;
    }
}
