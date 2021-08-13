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
import io.prestosql.orc.reader.SliceDictionaryColumnReader;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import nova.hetu.olk.block.VariableWidthOmniBlock;

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
    protected VariableWidthOmniBlock dictionaryBlock = new VariableWidthOmniBlock(1,
        wrappedBuffer(EMPTY_DICTIONARY_DATA), EMPTY_DICTIONARY_OFFSETS, Optional.of(new boolean[] {true}));

    /**
     * Instantiates a new Omni slice dictionary column reader.
     *
     * @param column the column
     * @param systemMemoryContext the system memory context
     * @param maxCodePointCount the max code point count
     * @param isCharType the is char type
     */
    public OmniSliceDictionaryColumnReader(OrcColumn column, LocalMemoryContext systemMemoryContext,
        int maxCodePointCount, boolean isCharType) {
        super(column, systemMemoryContext, maxCodePointCount, isCharType);
    }

    @Override
    public Block readNonNullBlock() throws IOException {
        verify(dataStream != null);
        int[] values = new int[nextBatchSize];
        dataStream.next(values, nextBatchSize);
        return new DictionaryBlock(nextBatchSize, dictionaryBlock, values);
    }

    @Override
    public Block readNullBlock(boolean[] isNull, int nonNullCount) throws IOException {
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
            if (!isNull[i]) {
                nonNullPosition++;
            }
        }

        int[] result = new int[isNull.length];
        fill(result, dictionarySize);

        for (int i = 0; i < nonNullPosition; i++) {
            result[nonNullPositionList[i]] = nonNullValueTemp[i];
        }

        return new DictionaryBlock(nextBatchSize, dictionaryBlock, result);
    }

    @Override
    public void setDictionaryBlockData(byte[] dictionaryData, int[] dictionaryOffsets, int positionCount) {
        verify(positionCount > 0);
        // only update the block if the array changed to prevent creation of new Block objects, since
        // the engine currently uses identity equality to test if dictionaries are the same
        if (currentDictionaryData != dictionaryData) {
            boolean[] isNullVector = new boolean[positionCount];
            isNullVector[positionCount - 1] = true;
            dictionaryOffsets[positionCount] = dictionaryOffsets[positionCount - 1];
            dictionaryBlock = new VariableWidthOmniBlock(positionCount, wrappedBuffer(dictionaryData),
                dictionaryOffsets, Optional.of(isNullVector));
            currentDictionaryData = dictionaryData;
        }
    }

    private RunLengthEncodedBlock readAllNullsBlock() {
        return new RunLengthEncodedBlock(
            new VariableWidthOmniBlock(1, EMPTY_SLICE, new int[2], Optional.of(new boolean[] {true})), nextBatchSize);
    }
}
