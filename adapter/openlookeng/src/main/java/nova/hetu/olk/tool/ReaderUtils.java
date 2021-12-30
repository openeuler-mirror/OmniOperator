/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.tool;

import nova.hetu.omniruntime.vector.Vec;

/**
 * the reader utils
 *
 * @since 2021-10-29
 */
public class ReaderUtils {
    private ReaderUtils() {}

    /**
     * generate a not null long values
     *
     * @param values long values
     * @param isNull is a value null flag
     * @return long values
     */
    public static long[] unpackLongNulls(long[] values, byte[] isNull) {
        long[] result = new long[isNull.length];

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            result[i] = values[position];
            if (isNull[i] != Vec.NULL) {
                position++;
            }
        }
        return result;
    }

    /**
     * generate a not null int values
     *
     * @param values int values
     * @param isNull is a value null flag
     * @return int values
     */
    public static int[] unpackIntNulls(int[] values, byte[] isNull) {
        int[] result = new int[isNull.length];

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            result[i] = values[position];
            if (isNull[i] != Vec.NULL) {
                position++;
            }
        }
        return result;
    }

    /**
     * generate a not null double values
     *
     * @param values double values
     * @param isNull is a value null flag
     * @return double values
     */
    public static double[] unpackDoubleNulls(double[] values, byte[] isNull) {
        double[] result = new double[isNull.length];

        int position = 0;
        for (int i = 0; i < isNull.length; i++) {
            result[i] = values[position];
            if (isNull[i] != Vec.NULL) {
                position++;
            }
        }
        return result;
    }

    /**
     * unpack length nulls with null array
     *
     * @param values offset array
     * @param isNull null array
     * @param nonNullCount not null count
     */
    public static void unpackLengthNulls(int[] values, byte[] isNull, int nonNullCount) {
        int nullSuppressedPosition = nonNullCount - 1;
        for (int outputPosition = isNull.length - 1; outputPosition >= 0; outputPosition--) {
            if (isNull[outputPosition] == Vec.NULL) {
                values[outputPosition] = 0;
            } else {
                values[outputPosition] = values[nullSuppressedPosition];
                nullSuppressedPosition--;
            }
        }
    }

    /**
     * generate a not null int128 values
     *
     * @param values long values
     * @param isNull is a value null flag
     * @return long values
     */
    public static long[] unpackInt128Nulls(long[] values, byte[] isNull) {
        long[] result = new long[isNull.length * 2];

        int position = 0;
        int outputPosition = 0;
        for (int i = 0; i < isNull.length; i++) {
            result[outputPosition] = values[position];
            result[outputPosition + 1] = values[position + 1];
            if (isNull[i] != Vec.NULL) {
                position += 2;
            }
            outputPosition += 2;
        }
        return result;
    }
}
