/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.reader;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.orc.reader.ReaderUtils.minNonNullValueSize;
import static java.util.Arrays.fill;

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.reader.SliceDictionaryColumnReader;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import nova.hetu.olk.block.VariableWidthOmniBlock;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;

import java.io.IOException;
import java.util.Optional;

/**
 * The type Omni slice dictionary column reader.
 *
 * @since 20210630
 */
public class OmniSliceDictionaryColumnReader extends SliceDictionaryColumnReader {
    /**
     * The Dictionary block.
     */
    protected VariableWidthOmniBlock dictionaryBlock;

    private final VecAllocator vecAllocator;

    /**
     * Instantiates a new Omni slice dictionary column reader.
     *
     * @param column the column
     * @param systemMemoryContext the system memory context
     * @param maxCodePointCount the max code point count
     * @param isCharType the is char type
     */
    public OmniSliceDictionaryColumnReader(VecAllocator vecAllocator, OrcColumn column,
            LocalMemoryContext systemMemoryContext, int maxCodePointCount, boolean isCharType) {
        super(column, systemMemoryContext, maxCodePointCount, isCharType);
        this.vecAllocator = vecAllocator;
        this.dictionaryBlock = new VariableWidthOmniBlock(vecAllocator, 1, wrappedBuffer(EMPTY_DICTIONARY_DATA),
                EMPTY_DICTIONARY_OFFSETS, Optional.of(new byte[]{Vec.NULL}));
    }

    @Override
    public Block readBlock() throws IOException {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(column.getOrcDataSourceId(),
                            "Value is not null but data stream is missing");
                }
                dataStream.skip(readOffset);
            }
        }

        Block block;
        if (dataStream == null) {
            if (presentStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(),
                        "Value is null but present stream is missing");
            }
            presentStream.skip(nextBatchSize);
            block = readAllNullsBlock();
        } else if (presentStream == null) {
            block = readNonNullBlock();
        } else {
            byte[] isNull = new byte[nextBatchSize];
            int nullCount = presentStream.getUnsetBits(nextBatchSize, isNull);
            if (nullCount == 0) {
                block = readNonNullBlock();
            } else if (nullCount != nextBatchSize) {
                block = readNullBlock(isNull, nextBatchSize - nullCount);
            } else {
                block = readAllNullsBlock();
            }
        }

        readOffset = 0;
        nextBatchSize = 0;
        return block;
    }

    @Override
    public Block readNonNullBlock() throws IOException {
        verify(dataStream != null);
        int[] values = new int[nextBatchSize];
        dataStream.next(values, nextBatchSize);

        VarcharVec varcharVec = (VarcharVec) dictionaryBlock.getValues();
        VarcharVec slice = varcharVec.slice(0, varcharVec.getSize());
        return new DictionaryBlock(nextBatchSize, new VariableWidthOmniBlock(dictionaryBlock.getPositionCount(), slice),
                values);
    }

    private Block readNullBlock(byte[] isNull, int nonNullCount) throws IOException {
        verify(dataStream != null);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (nonNullValueTemp.length < minNonNullValueSize) {
            nonNullValueTemp = new int[minNonNullValueSize];
            nonNullPositionList = new int[minNonNullValueSize];
            systemMemoryContext.setBytes(sizeOf(nonNullValueTemp) + sizeOf(nonNullPositionList));
        }

        dataStream.next(nonNullValueTemp, nonNullCount);

        int nonNullPosition = 0;
        for (int i = 0; i < isNull.length; i++) {
            nonNullPositionList[nonNullPosition] = i;
            if (isNull[i] != Vec.NULL) {
                nonNullPosition++;
            }
        }

        int[] result = new int[isNull.length];
        fill(result, dictionarySize);

        for (int i = 0; i < nonNullPosition; i++) {
            result[nonNullPositionList[i]] = nonNullValueTemp[i];
        }
        VarcharVec varcharVec = (VarcharVec) dictionaryBlock.getValues();
        VarcharVec slice = varcharVec.slice(0, varcharVec.getSize());
        return new DictionaryBlock(nextBatchSize, new VariableWidthOmniBlock(dictionaryBlock.getPositionCount(), slice),
                result);
    }

    @Override
    public void setDictionaryBlockData(byte[] dictionaryData, int[] dictionaryOffsets, int positionCount) {
        verify(positionCount > 0);
        // only update the block if the array changed to prevent creation of new Block objects, since
        // the engine currently uses identity equality to test if dictionaries are the same
        if (currentDictionaryData != dictionaryData) {
            byte[] isNullVector = new byte[positionCount];
            isNullVector[positionCount - 1] = 1;
            dictionaryOffsets[positionCount] = dictionaryOffsets[positionCount - 1];
            dictionaryBlock.close();
            dictionaryBlock = new VariableWidthOmniBlock(vecAllocator, positionCount, wrappedBuffer(dictionaryData),
                    dictionaryOffsets, Optional.of(isNullVector));
            currentDictionaryData = dictionaryData;
        }
    }

    private RunLengthEncodedBlock readAllNullsBlock() {
        return new RunLengthEncodedBlock(
                new VariableWidthBlock(1, EMPTY_SLICE, new int[2], Optional.of(new boolean[]{true})), nextBatchSize);
    }

    @Override
    public void close() {
        super.close();
        dictionaryBlock.close();
    }

}
