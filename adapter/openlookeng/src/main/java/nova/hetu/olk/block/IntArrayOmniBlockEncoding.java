/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.block.EncoderUtil.decodeNullBits;

import io.airlift.slice.SliceInput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.IntArrayBlockEncoding;

/**
 * The type Int array omni block encoding.
 *
 * @since 20210630
 */
public class IntArrayOmniBlockEncoding extends IntArrayBlockEncoding {
    @Override
    public Block<Integer> readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int[] values = new int[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values[position] = sliceInput.readInt();
            }
        }

        return new IntArrayOmniBlock(0, positionCount, valueIsNull, values);
    }
}