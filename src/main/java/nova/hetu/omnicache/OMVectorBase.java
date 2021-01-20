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
    public static final int INT_DATA_TYPE = 1;
    public static final int LONG_DATA_TYPE = 2;
    public static final int Double_DATA_TYPE = 3;

    ByteBuffer data;

    public OMVectorBase() {}

    static {
        System.loadLibrary("omnicache");
        //System.loadLibrary("omvector");
    }

    /**
     * multiplies the content of the vector at the address vec_addr by a scalar value m
     * @param m
     */
    public native void mul(int datatype, ByteBuffer data, int m);

    /**
     * pair wise multiply the content of the curernt vector with the parameter vector, the size of the two vectors must match
     * @param data2
     */
    public native void mmul(int datatype, ByteBuffer data2);

    /**
     * aggreate the result after applying the filter
     * @param filter
     * @return
     */
    public native long agg(int datatype, String filter);

    /**
     * Allocate the off-heap native byte buffer
     * @param size number of bytes, each specific type fo vector need to multiply it by the element size of the type
     * @return
     */
    public static native ByteBuffer allocate(int size);
}
