package nova.hetu.omnicache;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 1. representing a vector
 * 2. sample: create a block with off heap memory and perform a filter using bloomfiltering
 *
 * need to allow seamless access to the data, e.g. access from both Java and C/C++ without any overhead
 * all primitive data types and string will be supported
 * need to consider the encoding and endian used
 *
 * the only way to pass data between on-heap and off-heap is via byte buffer
 *
 */
public class OMVectorBase
{
    ByteBuffer data;

    public OMVectorBase() {
        data = allocate(10).order(ByteOrder.LITTLE_ENDIAN);
    }
    static {
        System.load("/opt/kkrazy/hetu-core/omni-cache/src/main/java/omnicache.so");
    }

    /**
     * return an array of size 3: [start offset, unit size, count]
     * @return
     */
    public native long[] get();

    /**
     * multiplies the content of the vecotr at the address vec_addr by m
     * @param m
     */
    public static native void mul(ByteBuffer data, int m);

    public static native void mmul(ByteBuffer data1, ByteBuffer data2);

    public static native long agg(ByteBuffer data);

    public static native ByteBuffer allocate(int size);
}
