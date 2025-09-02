/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.MapDataType;

import static nova.hetu.omniruntime.vector.VecEncoding.OMNI_ENCODING_MAP;
import static nova.hetu.omniruntime.vector.VecEncoding.OMNI_ENCODING_STRUCT;

/**
 * map vec.
 *
 * @since 2025-8-29
 */
public class MapVec extends ComplexVec {

    private Vec keyVec;
    private Vec valueVec;

    public MapVec(MapDataType type, int size) {
        this(type, size, false);
    }

    public MapVec(MapDataType type, int size, boolean isEmpty) {
        this(isEmpty ? newEmptyComplexVectorNative(size, OMNI_ENCODING_MAP.ordinal(), new DataType[]{type.getKeyType(), type.getValueType()})
                : newComplexVectorNative(size, OMNI_ENCODING_MAP.ordinal(), new DataType[]{type.getKeyType(), type.getValueType()}), type, size, isEmpty);
    }

    public MapVec(long nativeVector, MapDataType type) {
        this(nativeVector, type, getSizeNative(nativeVector), false);
    }

    public MapVec(long nativeVector, MapDataType type, int size) {
        this(nativeVector, type, size, false);
    }

    public MapVec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress, int size, MapDataType type) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress,
            getComplexCapacityNative(nativeVector, OMNI_ENCODING_MAP.ordinal()), size, type);
        this.keyVec = createVec(getKeysAddrNative(nativeVector), type.getKeyType());
        this.valueVec = createVec(getValuesAddrNative(nativeVector), type.getValueType());
    }

    public MapVec(long nativeVector, MapDataType type, int size, boolean isEmpty) {
        super(nativeVector, getComplexCapacityNative(nativeVector, OMNI_ENCODING_MAP.ordinal()), size, type);
        if (!isEmpty){
            this.keyVec = createVec(getKeysAddrNative(nativeVector), type.getKeyType());
            this.valueVec = createVec(getValuesAddrNative(nativeVector), type.getValueType());
        }
    }

    private MapVec(MapVec vector, int offset, int length) {
        super(vector, offset, length, getComplexCapacityNative(vector.getNativeVector(), OMNI_ENCODING_MAP.ordinal()));
        this.keyVec = createVec(getKeysAddrNative(nativeVector), ((MapDataType) getType()).getKeyType());
        this.valueVec = createVec(getValuesAddrNative(nativeVector), ((MapDataType) getType()).getValueType());
    }

    @Override
    public MapVec slice(int start, int length) {
        return new MapVec(this, start, length);
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

    public void AddKeys(Vec keys){
        AddKeysNative(this.nativeVector, keys.nativeVector);
    }

    public void AddValues(Vec values){
        AddValuesNative(this.nativeVector, values.nativeVector);
    }

    public void AddOffsets(int[] offsets){
        AddOffsetsNative(this.nativeVector, offsets);
    }

    public void setSize(int index, int size){
        setSizeByIndexNative(this.nativeVector, index, size);
    }

    protected static native long setSizeByIndexNative(long nativeVector, int index, int size);

    protected static native long getKeysAddrNative(long nativeVector);

    protected static native long getValuesAddrNative(long nativeVector);

    protected static native long getOffsetNative(long nativeVector, long rowId);

    protected static native long getSizeNative(long nativeVector, long rowId);

    protected static native void AddKeysNative(long nativeVector, long keys);

    protected static native void AddValuesNative(long nativeVector, long values);

    protected static native void AddOffsetsNative(long nativeVector, int[] offsets);

    public Vec getKeyVec() {
        return keyVec;
    }

    public Vec getValueVec() {
        return valueVec;
    }
}
