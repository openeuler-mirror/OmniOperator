/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.block.EncoderUtil.encodeNullsAsBits;
import static nova.hetu.olk.tool.EncoderUtil.decodeNullBits;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.BlockEncodingSerde;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;

/**
 * The type Double array omni block encoding.
 *
 * @since 20210630
 */
public class DoubleArrayOmniBlockEncoding implements BlockEncoding {
    /**
     * The constant NAME.
     */
    public static final String NAME = "DOUBLE_ARRAY";

    private final VecAllocator vecAllocator;

    public DoubleArrayOmniBlockEncoding(VecAllocator vecAllocator) {
        this.vecAllocator = vecAllocator;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block) {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        for (int position = 0; position < positionCount; position++) {
            if (!block.isNull(position)) {
                sliceOutput.writeDouble(block.getDouble(position, 0));
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        byte[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        double[] values = new double[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || valueIsNull[position] == Vec.NOT_NULL) {
                values[position] = sliceInput.readDouble();
            }
        }

        return new DoubleArrayOmniBlock(vecAllocator, 0, positionCount, valueIsNull, values);
    }
}
