/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * jvm utils
 *
 * @since 2021-08-05
 *
 */
final class JvmUtils {
    /**
     * jvm unsafe
     */
    public static final Unsafe UNSAFE;

    private static void assertArrayIndexScale(String name, int actualIndexScale, int expectedIndexScale) {
        if (actualIndexScale != expectedIndexScale) {
            throw new IllegalStateException(name + " array index scale must be " + expectedIndexScale + ", but is " +
                    actualIndexScale);
        }
    }

    private JvmUtils() {
    }

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Object obj = field.get(null);
            if (obj instanceof Unsafe) {
                UNSAFE = (Unsafe) obj;
            } else {
                UNSAFE = null;
            }
            if (UNSAFE == null) {
                throw new OmniRuntimeException(OmniErrorType.OMNI_NATIVE_ERROR, "Unsafe access not available");
            } else {
                assertArrayIndexScale("Boolean", Unsafe.ARRAY_BOOLEAN_INDEX_SCALE, 1);
                assertArrayIndexScale("Byte", Unsafe.ARRAY_BYTE_INDEX_SCALE, 1);
                assertArrayIndexScale("Short", Unsafe.ARRAY_SHORT_INDEX_SCALE, 2);
                assertArrayIndexScale("Int", Unsafe.ARRAY_INT_INDEX_SCALE, 4);
                assertArrayIndexScale("Long", Unsafe.ARRAY_LONG_INDEX_SCALE, 8);
                assertArrayIndexScale("Float", Unsafe.ARRAY_FLOAT_INDEX_SCALE, 4);
                assertArrayIndexScale("Double", Unsafe.ARRAY_DOUBLE_INDEX_SCALE, 8);
            }
        } catch (ReflectiveOperationException var1) {
            throw new OmniRuntimeException(OmniErrorType.OMNI_NATIVE_ERROR, var1);
        }
    }
}
