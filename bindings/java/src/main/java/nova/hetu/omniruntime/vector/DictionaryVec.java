/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.type.DictionaryVecType;
import nova.hetu.omniruntime.type.VecType;
import nova.hetu.omniruntime.type.VecTypeSerializer;

/**
 * dictionary vec
 *
 * @since 2021-07-17
 */
public class DictionaryVec extends Vec {
    private Vec dictionary;

    private int[] ids;

    public DictionaryVec(long nativeVector) {
        super(nativeVector, DictionaryVecType.DICTIONARY);
        long dictionaryNative = getDictionaryNative(nativeVector);
        VecType type = VecTypeSerializer.deserializeSingle(getTypeNative(dictionaryNative));
        this.dictionary = VecFactory.create(dictionaryNative, type);
        this.ids = getIdsNative(nativeVector);
    }

    public DictionaryVec(Vec dictionary, int[] ids) {
        super(dictionary.getNativeVector(), DictionaryVecType.DICTIONARY);
        this.dictionary = dictionary;
        this.ids = ids;
    }

    private static native long getDictionaryNative(long nativeVector);

    private static native int[] getIdsNative(long nativeVector);

    public Vec getDictionary() {
        return dictionary;
    }

    public int[] getIds() {
        return ids;
    }

    /**
     * position array length
     *
     * @return position array length
     */
    public int getSize() {
        return ids.length;
    }

    /**
     * get the specified integer at the specified absolute
     *
     * @param index the element offset in vec
     * @return integer value
     */
    public int getInt(int index) {
        return ((IntVec) dictionary).get(ids[index]);
    }

    /**
     * get the specified long at the specified absolute
     *
     * @param index the element offset in vec
     * @return long value
     */
    public long getLong(int index) {
        return ((LongVec) dictionary).get(ids[index]);
    }

    @Override
    public boolean isNull(int index) {
        return dictionary.isNull(ids[index]);
    }

    @Override
    public Vec slice(int start, int length) {
        return null;
    }

    @Override
    public Vec copy() {
        return null;
    }

    @Override
    public Vec copyPositions(int[] positions, int offset, int length) {
        return null;
    }

    @Override
    public Vec copyRegion(int positionOffset, int length) {
        return null;
    }
}
