/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import io.airlift.slice.SliceInput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.Int128ArrayBlockEncoding;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;

import static nova.hetu.olk.tool.EncoderUtil.decodeNullBits;

/**
 * The type Int 128 array omni block encoding.
 *
 * @since 20210630
 */
public class Int128ArrayOmniBlockEncoding extends Int128ArrayBlockEncoding {
    private final VecAllocator vecAllocator;

    public Int128ArrayOmniBlockEncoding(VecAllocator vecAllocator) {
        this.vecAllocator = vecAllocator;
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        byte[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        long[] values = new long[positionCount * 2];
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || valueIsNull[position] == Vec.NOT_NULL) {
                values[position * 2] = sliceInput.readLong();
                values[(position * 2) + 1] = sliceInput.readLong();
            }
        }

        return new Int128ArrayOmniBlock(vecAllocator, 0, positionCount, valueIsNull, values);
    }
}
