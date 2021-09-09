/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.airlift.slice.Slices.wrappedIntArray;
import static io.prestosql.spi.block.RowBlock.validateConstructorArguments;

import io.airlift.slice.SliceInput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.EncoderUtil;
import io.prestosql.spi.block.RowBlockEncoding;

import javax.annotation.Nullable;

/**
 * The type Row omni block encoding.
 *
 * @since 20210630
 */
public class RowOmniBlockEncoding extends RowBlockEncoding {
    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput) {
        int numFields = sliceInput.readInt();
        Block[] fieldBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldBlocks[i] = blockEncodingSerde.readBlock(sliceInput);
        }

        int positionCount = sliceInput.readInt();
        int[] fieldBlockOffsets = new int[positionCount + 1];
        sliceInput.readBytes(wrappedIntArray(fieldBlockOffsets));
        boolean[] rowIsNull = EncoderUtil.decodeNullBits(sliceInput, positionCount)
            .orElseGet(() -> new boolean[positionCount]);
        return createRowBlockInternal(0, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }

    /**
     * Create row block internal row omni block.
     *
     * @param startOffset the start offset
     * @param positionCount the position count
     * @param rowIsNull the row is null
     * @param fieldBlockOffsets the field block offsets
     * @param fieldBlocks the field blocks
     * @return the row omni block
     */
    static RowOmniBlock createRowBlockInternal(int startOffset, int positionCount, @Nullable boolean[] rowIsNull,
        int[] fieldBlockOffsets, Block[] fieldBlocks) {
        validateConstructorArguments(startOffset, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
        return new RowOmniBlock(startOffset, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }
}
