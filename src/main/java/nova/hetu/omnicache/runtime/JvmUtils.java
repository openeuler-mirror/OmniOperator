package nova.hetu.omnicache.runtime;

import java.lang.reflect.Field;
import java.nio.Buffer;

import sun.misc.Unsafe;

final class JvmUtils {
    public static final Unsafe unsafe;

    private static void assertArrayIndexScale(String name, int actualIndexScale, int expectedIndexScale) {
        if (actualIndexScale != expectedIndexScale) {
            throw new IllegalStateException(name + " array index scale must be " + expectedIndexScale + ", but is " + actualIndexScale);
        }
    }

    private JvmUtils() {
    }

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe)field.get((Object)null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
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
            throw new RuntimeException(var1);
        }
    }
}
