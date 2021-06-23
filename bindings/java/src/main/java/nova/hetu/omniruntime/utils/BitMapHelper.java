package nova.hetu.omniruntime.utils;

import java.nio.ByteBuffer;

public class BitMapHelper
{
    private static final int ROUND_8_MASK_INT = 0xFFFFFFF8;

    private BitMapHelper()
    {
    }

    /**
     * Get the index of byte corresponding to bit index in bitmap.
     */
    public static int byteIndex(int absoluteBitIndex)
    {
        return absoluteBitIndex >> 3;
    }

    /**
     * Get the relative index of bit within the byte in bitmap.
     */
    public static int bitIndex(int absoluteBitIndex)
    {
        return absoluteBitIndex & 7;
    }

    public static void set(ByteBuffer bitMap, int index)
    {
        final int byteIndex = byteIndex(index);
        final int bitIndex = bitIndex(index);

        final int position = byteIndex * Byte.BYTES;
        byte currentByte = bitMap.get(position);
        final int bitMask = 1 << bitIndex;
        currentByte |= bitMask;
        bitMap.put(position, currentByte);
    }

    public static void unset(ByteBuffer bitMap, int index)
    {
        final int byteIndex = byteIndex(index);
        final int bitIndex = bitIndex(index);

        final int position = byteIndex * Byte.BYTES;
        byte currentByte = bitMap.get(position);
        final int bitMask = 1 << bitIndex;
        currentByte &= ~bitMask;
        bitMap.put(position, currentByte);
    }

    public static int get(ByteBuffer bitMap, int index)
    {
        final int byteIndex = byteIndex(index);
        final int bitIndex = bitIndex(index);
        final byte currentByte = bitMap.get(byteIndex);
        return (currentByte >> bitIndex) & 0x01;
    }

    /**
     * Calculate the nearest number of bytes based on the number of elements
     *
     * @param size the number of element
     * @return Number of bytes required
     */
    public static int computeSizeInBytes(int size)
    {
        return ((size + 7) & ROUND_8_MASK_INT) >> 3;
    }
}
