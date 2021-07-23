/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.omniruntime.utils;

import java.nio.ByteBuffer;

/**
 * The type Bit map helper.
 *
 * @since 20210630
 */
public class BitMapHelper {
    private static final int ROUND_8_MASK_INT = 0xFFFFFFF8;

    private BitMapHelper() {
    }

    /**
     * Byte index int.
     *
     * @param absoluteBitIndex the absolute bit index
     * @return the int
     */
    public static int byteIndex(int absoluteBitIndex) {
        return absoluteBitIndex >> 3;
    }

    /**
     * Bit index int.
     *
     * @param absoluteBitIndex the absolute bit index
     * @return the int
     */
    public static int bitIndex(int absoluteBitIndex) {
        return absoluteBitIndex & 7;
    }

    /**
     * Set.
     *
     * @param bitMap the bit map
     * @param index the index
     */
    public static void set(ByteBuffer bitMap, int index) {
        final int byteIndex = byteIndex(index);
        final int bitIndex = bitIndex(index);

        final int position = byteIndex * Byte.BYTES;
        byte currentByte = bitMap.get(position);
        final int bitMask = 1 << bitIndex;
        currentByte |= bitMask;
        bitMap.put(position, currentByte);
    }

    /**
     * Unset.
     *
     * @param bitMap the bit map
     * @param index the index
     */
    public static void unset(ByteBuffer bitMap, int index) {
        final int byteIndex = byteIndex(index);
        final int bitIndex = bitIndex(index);

        final int position = byteIndex * Byte.BYTES;
        byte currentByte = bitMap.get(position);
        final int bitMask = 1 << bitIndex;
        currentByte &= ~bitMask;
        bitMap.put(position, currentByte);
    }

    /**
     * Get int.
     *
     * @param bitMap the bit map
     * @param index the index
     * @return the int
     */
    public static int get(ByteBuffer bitMap, int index) {
        final int byteIndex = byteIndex(index);
        final int bitIndex = bitIndex(index);
        final byte currentByte = bitMap.get(byteIndex);
        return (currentByte >> bitIndex) & 0x01;
    }

    /**
     * Compute size in bytes int.
     *
     * @param size the size
     * @return the int
     */
    public static int computeSizeInBytes(int size) {
        return ((size + 7) & ROUND_8_MASK_INT) >> 3;
    }
}
