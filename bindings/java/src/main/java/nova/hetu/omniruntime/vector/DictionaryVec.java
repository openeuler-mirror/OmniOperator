/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DictionaryVecType;
import nova.hetu.omniruntime.type.VecType;

/**
 * dictionary vec
 *
 * @since 2021-07-17
 */
public class DictionaryVec extends FixedWidthVec {
    private static final int BYTES = Integer.BYTES;

    private Vec dictionary;

    private int[] ids;

    public DictionaryVec(long nativeVector) {
        super(nativeVector, DictionaryVecType.DICTIONARY);
        loadDictionaryAndIds(size);
    }

    public DictionaryVec(Vec dictionary, int[] ids) {
        super(dictionary.getAllocator(), ids.length * BYTES, ids.length, DictionaryVecType.DICTIONARY);
        // set ids
        valuesBuf.setIntArray(0, ids, 0, ids.length * BYTES);
        // set dictionary vector
        setDictionaryNative(getNativeVector(), dictionary.getNativeVector());

        this.ids = ids;
        loadDictionary();
    }

    private DictionaryVec(DictionaryVec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
        loadDictionaryAndIds(isSlice == true ? vector.getSize() : length);
    }

    private DictionaryVec(DictionaryVec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
        loadDictionaryAndIds(length);
    }

    private static native long getDictionaryNative(long nativeVector);

    public Vec getDictionary() {
        return dictionary;
    }

    public int[] getIds() {
        return ids;
    }

    private void loadDictionaryAndIds(int idsCount) {
        loadIds(idsCount);
        loadDictionary();
    }

    private void loadIds(int idsCount) {
        this.ids = new int[idsCount];
        valuesBuf.getIntArray(0, ids, 0, ids.length * BYTES);
    }

    private void loadDictionary() {
        long dictionaryNative = getDictionaryNative(getNativeVector());
        VecType type = VecType.create(getTypeIdNative(dictionaryNative));
        this.dictionary = VecFactory.create(dictionaryNative, type);
    }

    /**
     * get the specified integer at the specified absolute
     *
     * @param index the element offset in vec
     * @return int value
     */
    public int getId(int index) {
        return ids[index + offset];
    }

    /**
     * get the specified integer at the specified absolute
     *
     * @param index the element offset in vec
     * @return integer value
     */
    public int getInt(int index) {
        if (dictionary.getType().getId() != VecType.VecTypeId.OMNI_VEC_TYPE_DICTIONARY) {
            return ((IntVec) dictionary).get(getId(index));
        } else {
            return ((DictionaryVec) dictionary).getInt(getId(index));
        }
    }

    /**
     * get the specified long at the specified absolute
     *
     * @param index the element offset in vec
     * @return long value
     */
    public long getLong(int index) {
        int dicIndex = getId(index);
        if (dictionary.getType().getId() != VecType.VecTypeId.OMNI_VEC_TYPE_DICTIONARY) {
            return ((LongVec) dictionary).get(dicIndex);
        } else {
            return ((DictionaryVec) dictionary).getLong(dicIndex);
        }
    }

    /**
     * get the specified double at the specified absolute
     *
     * @param index the element offset in vec
     * @return double value
     */
    public double getDouble(int index) {
        if (dictionary.getType().getId() != VecType.VecTypeId.OMNI_VEC_TYPE_DICTIONARY) {
            return ((DoubleVec) dictionary).get(getId(index));
        } else {
            return ((DictionaryVec) dictionary).getDouble(getId(index));
        }
    }

    /**
     * get the specified boolean at the specified absolute
     *
     * @param index the element offset in vec
     * @return boolean value
     */
    public boolean getBoolean(int index) {
        if (dictionary.getType().getId() != VecType.VecTypeId.OMNI_VEC_TYPE_DICTIONARY) {
            return ((BooleanVec) dictionary).get(getId(index));
        } else {
            return ((DictionaryVec) dictionary).getBoolean(getId(index));
        }
    }

    /**
     * get the specified bytes at the specified absolute
     *
     * @param index the element offset in vec
     * @return byte array
     */
    public byte[] getBytes(int index) {
        if (dictionary.getType().getId() != VecType.VecTypeId.OMNI_VEC_TYPE_DICTIONARY) {
            return ((VarcharVec) dictionary).get(getId(index));
        } else {
            return ((DictionaryVec) dictionary).getBytes(getId(index));
        }
    }

    /**
     * get the specified decimal at the specified absolute
     *
     * @param index the element offset in vec
     * @return long array
     */
    public long[] getDecimal128(int index) {
        if (dictionary.getType().getId() != VecType.VecTypeId.OMNI_VEC_TYPE_DICTIONARY) {
            return ((Decimal128Vec) dictionary).get(getId(index));
        } else {
            return ((DictionaryVec) dictionary).getDecimal128(getId(index));
        }
    }

    @Override
    public boolean isNull(int index) {
        return dictionary.isNull(getId(index));
    }

    @Override
    public DictionaryVec slice(int start, int end) {
        return new DictionaryVec(this, start, end - start, true);
    }

    @Override
    public Vec copy() {
        return null;
    }

    @Override
    public DictionaryVec copyPositions(int[] positions, int offset, int length) {
        return new DictionaryVec(this, positions, offset, length);
    }

    @Override
    public DictionaryVec copyRegion(int positionOffset, int length) {
        return new DictionaryVec(this, positionOffset, length, false);
    }

    private static native void setDictionaryNative(long nativeVector, long nativeDictionaryVector);
}
