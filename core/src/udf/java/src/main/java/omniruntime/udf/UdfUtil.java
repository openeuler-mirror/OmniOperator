/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package omniruntime.udf;

import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import sun.misc.Unsafe;

import java.io.StringWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

/**
 * udf util.
 * if any exception is thrown, the C++ side will handle the exception.
 *
 * @since 2022-07-25
 */
public class UdfUtil {
    /**
     * The Unsafe field for access off-heap memory.
     */
    public static final Unsafe UNSAFE;
    private static final int BYTE_ARRAY_OFFSET;
    private static final int LONG_ARRAY_OFFSET;
    private static final long LONG_BYTE_SIZE = Long.BYTES;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_JAVA_UDF_ERROR, "get theUnsafe field failed.");
        }

        BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
    }

    /**
     * put string value into gaven base address and offset.
     *
     * @param base the base address
     * @param offset the offset
     * @param value the value
     */
    public static void putString(long base, int offset, String value) {
        byte[] chars = value.getBytes(StandardCharsets.UTF_8);
        UNSAFE.copyMemory(chars, BYTE_ARRAY_OFFSET, null, base + offset, chars.length);
    }

    /**
     * get long values from gaven base address and offset.
     *
     * @param base the base address
     * @param offset the offset
     * @param length the length
     * @return return the result long array
     */
    public static long[] getLongs(long base, int offset, int length) {
        long[] values = new long[length];
        UNSAFE.copyMemory(null, base + offset * LONG_BYTE_SIZE, values, LONG_ARRAY_OFFSET, length * LONG_BYTE_SIZE);
        return values;
    }

    /**
     * get string value from gaven base address and offset.
     *
     * @param base the base address
     * @param offset the offset
     * @param length the length
     * @return return the string result
     */
    public static String getString(long base, int offset, int length) {
        byte[] target = new byte[length];
        UNSAFE.copyMemory(null, base + offset, target, BYTE_ARRAY_OFFSET, length);
        return new String(target, StandardCharsets.UTF_8);
    }

    /**
     * transform throwable to string.
     *
     * @param throwable the throwable object
     * @return return the string
     */
    public static String throwableToString(Throwable throwable) {
        StringWriter stringWriter = new StringWriter();
        stringWriter.write(String.format("%s", throwable.getMessage()));
        Throwable cause = throwable;
        while ((cause = cause.getCause()) != null) {
            stringWriter.write(String.format("%sCAUSED BY: %s: %s", System.lineSeparator(),
                    cause.getClass().getSimpleName(), cause.getMessage()));
        }
        return stringWriter.toString();
    }
}
