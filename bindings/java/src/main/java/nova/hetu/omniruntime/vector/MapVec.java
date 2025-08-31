/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.MapDataType;

import static nova.hetu.omniruntime.vector.VecEncoding.OMNI_ENCODING_MAP;

/**
 * map vec.
 *
 * @since 2025-8-29
 */
public class MapVec extends ComplexVec {

    protected OmniBuffer keysBuf;

    public MapVec(MapDataType type, int size) {
        this(newComplexVectorNative(size, OMNI_ENCODING_MAP.ordinal(), new DataType[]{type.getKeyType(), type.getValueType()}), type, size);
    }

    public MapVec(long nativeVector, MapDataType type) {
        this(nativeVector, type, getSizeNative(nativeVector));
    }

    public MapVec(long nativeVector, MapDataType type, int size) {
        super(nativeVector, getComplexCapacityNative(nativeVector, OMNI_ENCODING_MAP.ordinal()), size, type);
        this.keysBuf = OmniBufferFactory.create(getKeysAddrNative(nativeVector), capacityInBytes);
        this.valuesBuf = OmniBufferFactory.create(getValuesAddrNative(nativeVector), capacityInBytes);
    }

    @Override
    public Vec slice(int start, int length) {
        return null;
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

    protected static native long getKeysAddrNative(long nativeVector);

    protected static native long getValuesAddrNative(long nativeVector);

    protected static native long getOffsetNative(long nativeVector, long rowId);

    protected static native long getSizeNative(long nativeVector, long rowId);
}
