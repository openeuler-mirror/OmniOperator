/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.reader;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.orc.reader.ReaderUtils.minNonNullValueSize;
import static nova.hetu.olk.tool.ReaderUtils.unpackLongNulls;
import static nova.hetu.olk.tool.VecAllocatorHelper.getVecAllocatorFromExtensionProperties;

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.reader.LongColumnReader;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import java.util.Map;
import nova.hetu.olk.block.LongArrayOmniBlock;

import java.io.IOException;
import java.util.Optional;

import nova.hetu.omniruntime.vector.VecAllocator;

/**
 * The type Omni long column reader.
 *
 * @since 20210630
 */
public class OmniLongColumnReader extends LongColumnReader {
    private final VecAllocator vecAllocator;

    /**
     * Instantiates a new Omni long column reader.
     *
     * @param type the type
     * @param column the column
     * @param systemMemoryContext the system memory context
     * @throws OrcCorruptionException the orc corruption exception
     */
    public OmniLongColumnReader(Type type, OrcColumn column, LocalMemoryContext systemMemoryContext, Map<String, String> extensionColumnReadersProperties)
        throws OrcCorruptionException {
        super(type, column, systemMemoryContext);
        vecAllocator = getVecAllocatorFromExtensionProperties(extensionColumnReadersProperties);
    }

    @Override
    public Block readBlock()
            throws IOException {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
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
            block = RunLengthEncodedBlock.create(BigintType.BIGINT, null, nextBatchSize);
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
                block = RunLengthEncodedBlock.create(BigintType.BIGINT, null, nextBatchSize);
            }
        }

        readOffset = 0;
        nextBatchSize = 0;

        return block;
    }

    @Override
    public Block readNonNullBlock() throws IOException {
        verify(dataStream != null);
        long[] values = new long[nextBatchSize];
        dataStream.next(values, nextBatchSize);
        return new LongArrayOmniBlock(vecAllocator, nextBatchSize, Optional.empty(), values);
    }

    private Block readNullBlock(byte[] isNull, int nonNullCount) throws IOException {
        return longReadNullBlock(isNull, nonNullCount);
    }

    private Block longReadNullBlock(byte[] isNull, int nonNullCount) throws IOException {
        verify(dataStream != null);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (longNonNullValueTemp.length < minNonNullValueSize) {
            longNonNullValueTemp = new long[minNonNullValueSize];
            systemMemoryContext.setBytes(sizeOf(longNonNullValueTemp));
        }

        dataStream.next(longNonNullValueTemp, nonNullCount);

        long[] result = unpackLongNulls(longNonNullValueTemp, isNull);

        return new LongArrayOmniBlock(vecAllocator, nextBatchSize, Optional.of(isNull), result);
    }
}
