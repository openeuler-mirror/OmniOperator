/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static nova.hetu.omniruntime.vector.VecEncoding.OMNI_ENCODING_ARRAY;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.ArrayDataType;

/**
 * array vec.
 *
 * @since 2025-09-01
 */
public class ArrayVec extends ComplexVec {
    private Vec elementVec;

    public ArrayVec(ArrayDataType type, int size) {
        this(type, size, false);
    }

    public ArrayVec(ArrayDataType type, int size, boolean isEmpty) {
        this(isEmpty ? newEmptyComplexVectorNative(size, OMNI_ENCODING_ARRAY.ordinal(), new DataType[]{type.getElementType()})
                : newComplexVectorNative(size, OMNI_ENCODING_ARRAY.ordinal(), new DataType[]{type.getElementType()}), type, size, isEmpty);
    }

    public ArrayVec(long nativeVector, ArrayDataType type) {
        this(nativeVector, type, getSizeNative(nativeVector), false);
    }

    public ArrayVec(long nativeVector, ArrayDataType type, int size) {
        this(nativeVector, type, size, false);
    }

    public ArrayVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress, int size, ArrayDataType type) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress,
            getComplexCapacityNative(nativeVector, OMNI_ENCODING_ARRAY.ordinal()), size, type);
        this.elementVec = createVec(getElementsAddrNative(nativeVector), type.getElementType());
    }

    public ArrayVec(long nativeVector, ArrayDataType type, int size, boolean isEmpty) {
        super(nativeVector, getComplexCapacityNative(nativeVector, OMNI_ENCODING_ARRAY.ordinal()), size, type);
        if (!isEmpty){
            this.elementVec = createVec(getElementsAddrNative(nativeVector), type.getElementType());
        }
    }

    private ArrayVec(ArrayVec vector, int offset, int length) {
        super(vector, offset, length, getComplexCapacityNative(vector.getNativeVector(), OMNI_ENCODING_ARRAY.ordinal()));
        this.elementVec = createVec(getElementsAddrNative(nativeVector), ((ArrayDataType) getType()).getElementType());
    }

    @Override
    public ArrayVec slice(int start, int length) {
        return new ArrayVec(this, start, length);
    }

    @Override
    public Vec copyPositions(int[] positions, int offset, int length) {
        return null;
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return 0;
    }

    public long getOffset(long rowId) {
        return getOffsetNative(nativeVector, rowId);
    }

    public long getSize(long rowId) {
        return getSizeNative(nativeVector, rowId);
    }

    public void addElements(Vec elements) {
        addElementsNative(this.nativeVector, elements.nativeVector);
    }

    public void addOffsets(int[] offsets) {
        addOffsetsNative(this.nativeVector, offsets);
    }

    public void setSize(int index, int size) {
        setSizeByIndexNative(this.nativeVector, index, size);
    }

    protected static native long setSizeByIndexNative(long nativeVector, int index, int size);

    protected static native long getElementsAddrNative(long nativeVector);

    protected static native long getOffsetNative(long nativeVector, long rowId);

    protected static native long getSizeNative(long nativeVector, long rowId);

    protected static native void addElementsNative(long nativeVector, long elements);

    protected static native void addOffsetsNative(long nativeVector, int[] offsets);

    public Vec getElementVec() {
        return elementVec;
    }
}


