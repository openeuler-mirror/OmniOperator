/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static nova.hetu.omniruntime.vector.VecEncoding.OMNI_ENCODING_STRUCT;

import nova.hetu.omniruntime.type.StructDataType;

/**
 * struct vec.
 *
 * @since 2025-8-29
 */
public class StructVec extends ComplexVec {

    private Vec[] children;

    public StructVec(StructDataType type, int size) {
        this(type, size, false);
    }

    public StructVec(StructDataType type, int size, boolean isEmpty) {
        this(isEmpty ? newEmptyComplexVectorNative(size, OMNI_ENCODING_STRUCT.ordinal(), type.getFieldTypes())
                : newComplexVectorNative(size, OMNI_ENCODING_STRUCT.ordinal(), type.getFieldTypes()), type, size, isEmpty);
    }
    
    public StructVec(long nativeVector, StructDataType type) {
        this(nativeVector, type, getSizeNative(nativeVector), false);
    }

    public StructVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress, int size, StructDataType type) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress,
            getComplexCapacityNative(nativeVector, OMNI_ENCODING_STRUCT.ordinal()), size, type);
        initChildren(type);
    }

    public StructVec(long nativeVector, StructDataType type, int size) {
        this(nativeVector, type, size, false);
    }

    public StructVec(long nativeVector, StructDataType type, int size, boolean isEmpty) {
        super(nativeVector, getComplexCapacityNative(nativeVector, OMNI_ENCODING_STRUCT.ordinal()), size, type);
        if (!isEmpty) {
            initChildren(type);
        }
    }

    private StructVec(StructVec vector, int offset, int length) {
        super(vector, offset, length, getComplexCapacityNative(vector.getNativeVector(), OMNI_ENCODING_STRUCT.ordinal()));
        initChildren((StructDataType) getType());
    }

    private void initChildren(StructDataType type) {
        children = new Vec[type.getFieldTypes().length];
        for (int i = 0; i < children.length; i++) {
            children[i] = createVec(getChildAddrNative(nativeVector, i), type.getFieldType(i));
        }
    }

    @Override
    public StructVec slice(int start, int length) {
        return new StructVec(this, start, length);
    }

    @Override
    public Vec copyPositions(int[] positions, int offset, int length) {
        return null;
    }

    @Override
    public int getRealValueBufCapacityInBytes() {
        return 0;
    }

    public void add(int index, Vec addedVec) {
        addVecNative(this.nativeVector, index, addedVec.nativeVector);
    }

    public void append(Vec appendedVec) {
        appendVecNative(this.nativeVector,appendedVec.nativeVector);
    }

    protected static native void addVecNative(long nativeVector, int index, long addedVec);

    protected static native void appendVecNative(long nativeVector, long appendedVec);

    protected static native long getChildAddrNative(long nativeVector, long addedVec);

    public Vec getChild(int index) {
        return children[index];
    }
}
