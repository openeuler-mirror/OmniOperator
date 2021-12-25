/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.tool;

import io.airlift.slice.SliceInput;

import java.util.Optional;

/**
 * encoder util
 *
 * @since 2021-1030
 */
public class EncoderUtil {
    private EncoderUtil() {
    }

    /**
     * Decode the bit stream created by encodeNullsAsBits.
     *
     * @param sliceInput slice input
     * @param positionCount position count
     * @return nulls with byte array
     */
    public static Optional<byte[]> decodeNullBits(SliceInput sliceInput, int positionCount) {
        if (!sliceInput.readBoolean()) {
            return Optional.empty();
        }

        // read null bits 8 at a time
        byte[] valueIsNull = new byte[positionCount];
        for (int position = 0; position < (positionCount & ~0b111); position += 8) {
            byte value = sliceInput.readByte();
            valueIsNull[position] = (byte) ((value & 0b1000_0000) >>> 7);
            valueIsNull[position + 1] = (byte) ((value & 0b0100_0000) >>> 6);
            valueIsNull[position + 2] = (byte) ((value & 0b0010_0000) >>> 5);
            valueIsNull[position + 3] = (byte) ((value & 0b0001_0000) >>> 4);
            valueIsNull[position + 4] = (byte) ((value & 0b0000_1000) >>> 3);
            valueIsNull[position + 5] = (byte) ((value & 0b0000_0100) >>> 2);
            valueIsNull[position + 6] = (byte) ((value & 0b0000_0010) >>> 1);
            valueIsNull[position + 7] = (byte) ((value & 0b0000_0001) >>> 0);
        }

        // read last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = sliceInput.readByte();
            int mask = 0b1000_0000;
            int shiftCount = 7;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                valueIsNull[position] = (byte) ((value & mask) >>> shiftCount);
                mask >>>= 1;
                shiftCount--;
            }
        }

        return Optional.of(valueIsNull);
    }
}
