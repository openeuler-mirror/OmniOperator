/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.tool;

import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.BooleanVec;

/**
 * The type Block utils.
 *
 * @since 20210630
 */
public class BlockUtils {
    /**
     * Compact vec boolean vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the boolean vec
     */
    public static BooleanVec compactVec(BooleanVec vec, int index, int length) {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }

        BooleanVec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close? Use put(bytebuffer) for better performance
        vec.close();
        return newValues;
    }

    /**
     * Compact vec int vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the int vec
     */
    public static IntVec compactVec(IntVec vec, int index, int length) {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }

        IntVec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close? Use put(bytebuffer) for better performance
        vec.close();
        return newValues;
    }

    /**
     * Compact vec long vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the long vec
     */
    public static LongVec compactVec(LongVec vec, int index, int length) {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }

        LongVec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close? Use put(bytebuffer) for better performance
        vec.close();
        return newValues;
    }

    /**
     * Compact vec double vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the double vec
     */
    public static DoubleVec compactVec(DoubleVec vec, int index, int length) {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }

        DoubleVec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close? Use put(bytebuffer) for better performance
        vec.close();
        return newValues;
    }

    /**
     * Compact vec varchar vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the varchar vec
     */
    public static VarcharVec compactVec(VarcharVec vec, int index, int length) {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }

        VarcharVec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close original vec?
        vec.close();
        return newValues;
    }

    /**
     * Compact vec decimal 128 vec.
     *
     * @param vec the vec
     * @param index the index
     * @param length the length
     * @return the decimal 128 vec
     */
    public static Decimal128Vec compactVec(Decimal128Vec vec, int index, int length) {
        if (index == 0 && length == vec.getSize() && vec.getOffset() == 0) {
            return vec;
        }

        Decimal128Vec newValues = vec.copyRegion(index, length);
        // TODO: is there any other place to close original vec?
        vec.close();
        return newValues;
    }
}
