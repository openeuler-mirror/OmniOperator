/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.block.EncoderUtil.decodeNullBits;

import io.airlift.slice.SliceInput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.LongArrayBlockEncoding;
import nova.hetu.omniruntime.vector.VecAllocator;

/**
 * The type Long array omni block encoding.
 *
 * @since 20210630
 */
public class LongArrayOmniBlockEncoding extends LongArrayBlockEncoding {
    private final VecAllocator vecAllocator;

    public LongArrayOmniBlockEncoding(VecAllocator vecAllocator) {
        this.vecAllocator = vecAllocator;
    }

    @Override
    public Block<Long> readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        long[] values = new long[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values[position] = sliceInput.readLong();
            }
        }
        return new LongArrayOmniBlock(vecAllocator, 0, positionCount, valueIsNull, values);
    }
}