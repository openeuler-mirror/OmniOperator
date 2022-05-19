/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package nova.hetu.omniruntime.vector;

import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_INNER_ERROR;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.utils.OmniErrorType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * 128-bit decimal vec.
 *
 * @since 2021-07-17
 */
public class Decimal128Vec extends DecimalVec {
    private static final int BYTES = Long.BYTES * 2;

    public Decimal128Vec(int size) {
        super(size, BYTES, Decimal128DataType.DECIMAL128);
    }

    public Decimal128Vec(VecAllocator allocator, int size) {
        super(allocator, size, BYTES, Decimal128DataType.DECIMAL128);
    }

    public Decimal128Vec(long nativeVector, DataType type) {
        super(nativeVector, BYTES, type);
    }

    public Decimal128Vec(long nativeVector, long nativeValueBufAddress, long nativeVectorNullBufAddress,
            long nativeVectorAllocator, int capacityInBytes, int size, int offset, DataType type) {
        super(nativeVector, nativeValueBufAddress, nativeVectorNullBufAddress, nativeVectorAllocator, capacityInBytes,
                size, offset, BYTES, type);
    }

    private Decimal128Vec(Decimal128Vec vector, int offset, int length, boolean isSlice) {
        super(vector, offset, length, isSlice);
    }

    private Decimal128Vec(Decimal128Vec vector, int[] positions, int offset, int length) {
        super(vector, positions, offset, length);
    }

    /**
     * split a vec into two vec according to the specified index and length.
     *
     * @param start starting index
     * @param end ending index
     * @return new vec
     */
    @Override
    public Decimal128Vec slice(int start, int end) {
        return new Decimal128Vec(this, start, end - start, true);
    }

    /**
     * copy a new vec according to the vec.
     *
     * @return new vec
     */
    @Override
    public Decimal128Vec copy() {
        throw new OmniRuntimeException(OmniErrorType.OMNI_NOSUPPORT, "Decimal128Vec is not supported");
    }

    /**
     * copy a new vec based on the positions.
     *
     * @param positions all positions in vec
     * @param offset position offset
     * @param length the number of elements to be copied
     * @return new vec
     */
    @Override
    public Decimal128Vec copyPositions(int[] positions, int offset, int length) {
        return new Decimal128Vec(this, positions, offset, length);
    }

    /**
     * copy a vec based on the starting position and the number of elements.
     *
     * @param positionOffset staring position
     * @param length the number of elements
     * @return new vec
     */
    @Override
    public Decimal128Vec copyRegion(int positionOffset, int length) {
        return new Decimal128Vec(this, positionOffset, length, false);
    }

    /**
     * transfer long to bytes
     *
     * @param input input long
     * @return new bytes
     */
    public static byte[] longToBytes(long input) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(input);
        return buffer.array();
    }

    /**
     * transfer bytes to long
     *
     * @param bytes input bytes
     * @return new long
     */
    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip(); // need flip
        return buffer.getLong();
    }

    /**
     * get long array from 128-bit BigInteger
     *
     * @param bigInteger input
     * @return new long array
     */
    public static long[] putDecimal(BigInteger bigInteger) {
        byte[] bytes = bigInteger.abs().toByteArray();
        int byteArrayLength = bytes.length;

        if (byteArrayLength > 2 * Long.BYTES) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Decimal overflow.");
        }
        // the array is big endian
        byte[] highBytes = new byte[Long.BYTES];
        byte[] lowBytes = new byte[Long.BYTES];
        if (byteArrayLength <= Long.BYTES) {
            System.arraycopy(bytes, 0, lowBytes, Long.BYTES - byteArrayLength, Math.min(byteArrayLength, Long.BYTES));
        } else {
            System.arraycopy(bytes, 0, highBytes, 2 * Long.BYTES - byteArrayLength,
                    Math.min(byteArrayLength - Long.BYTES, Long.BYTES));
            System.arraycopy(bytes, 8, lowBytes, 0, Long.BYTES);
        }
        boolean isNegative = bigInteger.compareTo(new BigInteger("0")) == -1;
        if (isNegative) {
            long signBit = bytesToLong(highBytes) | (1L << 63);
            highBytes = longToBytes(signBit);
        }

        long[] longs = new long[2];
        longs[1] = bytesToLong(highBytes);
        longs[0] = bytesToLong(lowBytes);
        return longs;
    }

    /**
     * get 128-bit BigInteger from long array
     *
     * @param longs input
     * @return new BigInteger
     */
    public static BigInteger getDecimal(long[] longs) {
        boolean isNegative = longs[1] < 0;
        if (isNegative) {
            longs[1] = longs[1] & 0x7FFFFFFFFFFFFFFFL;
        }
        byte[] bytes = new byte[Long.BYTES * 2];
        byte[] highBytes = longToBytes(longs[1]);
        byte[] lowBytes = longToBytes(longs[0]);
        System.arraycopy(highBytes, 0, bytes, 0, Long.BYTES);
        System.arraycopy(lowBytes, 0, bytes, 8, Long.BYTES);
        return isNegative ? new BigInteger(bytes).multiply(new BigInteger("-1")) : new BigInteger(bytes);
    }

    /**
     * please use this method to set jdk BigInteger to Decimal128Vec
     *
     * @param index row index
     * @param decimal input value
     */
    public void setBigInteger(int index, BigInteger decimal) {
        super.set(index, putDecimal(decimal));
    }

    /**
     * please use this method to get jdk BigInteger from Decimal128Vec
     *
     * @param index row index
     * @return new BigInteger
     */
    public BigInteger getBigInteger(int index) {
        return getDecimal(super.get(index));
    }
}
