/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static nova.hetu.olk.tool.EncoderUtil.decodeNullBits;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.VariableWidthBlockEncoding;
import nova.hetu.omniruntime.vector.VecAllocator;

/**
 * The type Variable width omni block encoding.
 *
 * @since 20210630
 */
public class VariableWidthOmniBlockEncoding extends VariableWidthBlockEncoding {
    private final VecAllocator vecAllocator;

    public VariableWidthOmniBlockEncoding(VecAllocator vecAllocator) {
        this.vecAllocator = vecAllocator;
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        int[] offsets = new int[positionCount + 1];
        sliceInput.readBytes(Slices.wrappedIntArray(offsets), SIZE_OF_INT, positionCount * SIZE_OF_INT);

        byte[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int blockSize = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(blockSize);

        return new VariableWidthOmniBlock(vecAllocator, 0, positionCount, slice, offsets, valueIsNull);
    }
}
