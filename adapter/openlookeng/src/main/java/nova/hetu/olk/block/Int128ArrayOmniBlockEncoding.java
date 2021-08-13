/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.block.EncoderUtil.decodeNullBits;

import io.airlift.slice.SliceInput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.Int128ArrayBlockEncoding;

/**
 * The type Int 128 array omni block encoding.
 *
 * @since 20210630
 */
public class Int128ArrayOmniBlockEncoding extends Int128ArrayBlockEncoding {
    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        long[] values = new long[positionCount * 2];
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values[position * 2] = sliceInput.readLong();
                values[(position * 2) + 1] = sliceInput.readLong();
            }
        }

        return new Int128ArrayOmniBlock(0, positionCount, valueIsNull, values);
    }
}
