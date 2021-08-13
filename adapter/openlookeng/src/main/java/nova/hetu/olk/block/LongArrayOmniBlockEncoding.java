/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.block.EncoderUtil.decodeNullBits;

import io.airlift.slice.SliceInput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.LongArrayBlockEncoding;
import nova.hetu.omniruntime.vector.LongVec;

/**
 * The type Long array omni block encoding.
 *
 * @since 20210630
 */
public class LongArrayOmniBlockEncoding extends LongArrayBlockEncoding {
    @Override
    public Block<Long> readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        LongVec values = new LongVec(positionCount);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values.set(position, sliceInput.readLong());
            }
        }
        return new LongArrayOmniBlock(0, positionCount, valueIsNull, values);
    }
}