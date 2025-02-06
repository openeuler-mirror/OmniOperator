/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package nova.hetu.omniruntime.utils;

import nova.hetu.omniruntime.vector.OmniBuffer;

public class NullsBufHelper {
    public static int byteIndex(int absoluteBitIndex) {
        return absoluteBitIndex >> 3;
    }

    public static int bitIndex(int absoluteBitIndex) {
        return absoluteBitIndex & 7;
    }

    public static int nBytes(int bits) {
        return (bits + (8 - 1)) / 8;
    }

    public static int isSet(OmniBuffer nullsBuf, int index) {
        int byteIndex = byteIndex(index);
        int bitIndex = bitIndex(index);
        int currentByte = nullsBuf.getByte(byteIndex);
        return currentByte >> bitIndex & 1;
    }

    public static void unsetBit(OmniBuffer nullsBuf, int index) {
        int byteIndex = byteIndex(index);
        int bitIndex = bitIndex(index);
        int currentByte = nullsBuf.getByte(byteIndex);
        int bitMask = 1 << bitIndex;
        currentByte &= ~bitMask;
        nullsBuf.setByte(byteIndex, (byte) currentByte);
    }

    public static void setBit(OmniBuffer nullsBuf, int index) {
        int byteIndex = byteIndex(index);
        int bitIndex = bitIndex(index);
        int currentByte = nullsBuf.getByte(byteIndex);
        int bitMask = 1 << bitIndex;
        currentByte |= bitMask;
        nullsBuf.setByte(byteIndex, (byte) currentByte);
    }

    public static void setBit(OmniBuffer nullsBuf, int index, byte[] isNulls, int srcStart, int length) {
        int i = 0;
        while (i < length) {
            setValidityBit(nullsBuf, index + i, isNulls[srcStart + i]);
            i++;
        }
    }

    public static void getBytes(OmniBuffer nullsBuf, int index, byte[] nullsArray, int targetStart, int length) {
        int i = 0;
        while (i < length) {
            nullsArray[targetStart + i] = (byte) isSet(nullsBuf, index + i);
            i++;
        }
    }

    public static void setValidityBit(OmniBuffer nullsBuf, int index, int value) {
        int byteIndex = byteIndex(index);
        int bitIndex = bitIndex(index);
        int currentByte = nullsBuf.getByte(byteIndex);
        int bitMask = 1 << bitIndex;
        if (value != 0) {
            currentByte |= bitMask;
        } else {
            currentByte &= ~bitMask;
        }
        nullsBuf.setByte(byteIndex, (byte) currentByte);
    }
}
