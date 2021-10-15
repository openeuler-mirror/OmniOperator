/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.DictionaryBlockEncoding;
import io.prestosql.spi.block.DictionaryId;
import nova.hetu.omniruntime.vector.VecAllocator;

/**
 * The type Dictionary omni block encoding.
 *
 * @since 20210630
 */
public class DictionaryOmniBlockEncoding extends DictionaryBlockEncoding {
    private final VecAllocator vecAllocator;

    public DictionaryOmniBlockEncoding(VecAllocator vecAllocator) {
        this.vecAllocator = vecAllocator;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block) {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        DictionaryOmniBlock dictionaryBlock;
        if (block instanceof DictionaryBlock) {
            dictionaryBlock = new DictionaryOmniBlock(((DictionaryBlock) block).getDictionary(),
                ((DictionaryBlock) block).getIdsArray());
        } else {
            dictionaryBlock = (DictionaryOmniBlock) block;
        }

        DictionaryOmniBlock compactDictionaryBlock = dictionaryBlock.compact();

        // positionCount
        int positionCount = compactDictionaryBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // dictionary
        Block dictionary = compactDictionaryBlock.getDictionary();
        blockEncodingSerde.writeBlock(sliceOutput, dictionary);

        // ids
        sliceOutput.writeBytes(compactDictionaryBlock.getIds());

        // instance id
        sliceOutput.appendLong(compactDictionaryBlock.getDictionarySourceId().getMostSignificantBits());
        sliceOutput.appendLong(compactDictionaryBlock.getDictionarySourceId().getLeastSignificantBits());
        sliceOutput.appendLong(compactDictionaryBlock.getDictionarySourceId().getSequenceId());

        // release compact block
        if (compactDictionaryBlock != dictionaryBlock) {
            compactDictionaryBlock.getDictionary().close();
            compactDictionaryBlock.close();
        }
        if (dictionaryBlock != block) {
            dictionaryBlock.close();
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput) {
        // positionCount
        int positionCount = sliceInput.readInt();

        // dictionary
        Block dictionaryBlock = blockEncodingSerde.readBlock(sliceInput);

        // ids
        int[] ids = new int[positionCount];
        sliceInput.readBytes(Slices.wrappedIntArray(ids));

        // instance id
        long mostSignificantBits = sliceInput.readLong();
        long leastSignificantBits = sliceInput.readLong();
        long sequenceId = sliceInput.readLong();

        // We always compact the dictionary before we send it. However, dictionaryBlock comes from sliceInput, which may over-retain memory.
        // As a result, setting dictionaryIsCompacted to true is not appropriate here.
        // TODO: fix DictionaryBlock so that dictionaryIsCompacted can be set to true when the underlying block over-retains memory.
        return new DictionaryOmniBlock(positionCount, dictionaryBlock, ids, false,
            new DictionaryId(mostSignificantBits, leastSignificantBits, sequenceId));
    }
}
