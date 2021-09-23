/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.reader;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.orc.reader.ReaderUtils.minNonNullValueSize;
import static io.prestosql.orc.reader.ReaderUtils.unpackInt128Nulls;
import static io.prestosql.orc.reader.ReaderUtils.unpackLongNulls;
import static nova.hetu.olk.tool.VecAllocatorHelper.getVecAllocatorFromExtensionProperties;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.reader.DecimalColumnReader;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.UnscaledDecimal128Arithmetic;
import java.util.Map;
import nova.hetu.olk.block.Int128ArrayOmniBlock;
import nova.hetu.olk.block.LongArrayOmniBlock;

import java.io.IOException;
import java.util.Optional;
import nova.hetu.omniruntime.vector.VecAllocator;

/**
 * The type Omni decimal column reader.
 *
 * @since 20210630
 */
public class OmniDecimalColumnReader extends DecimalColumnReader {
    private final VecAllocator vecAllocator;

    /**
     * Instantiates a new Omni decimal column reader.
     *
     * @param type the type
     * @param column the column
     * @param systemMemoryContext the system memory context
     * @throws OrcCorruptionException the orc corruption exception
     */
    public OmniDecimalColumnReader(Type type, OrcColumn column, LocalMemoryContext systemMemoryContext, Map<String, String> extensionColumnReadersProperties)
        throws OrcCorruptionException {
        super(type, column, systemMemoryContext);
        vecAllocator = getVecAllocatorFromExtensionProperties(extensionColumnReadersProperties);
    }

    @Override
    public Block readNonNullBlock() throws IOException {
        Block block;
        if (type.isShort()) {
            block = readShortNotNullBlock();
        } else {
            block = readLongNotNullBlock();
        }
        return block;
    }

    private Block readShortNotNullBlock() throws IOException {
        verify(scaleStream != null);
        verify(decimalStream != null);

        long[] data = new long[nextBatchSize];
        decimalStream.nextShortDecimal(data, nextBatchSize);

        for (int i = 0; i < nextBatchSize; i++) {
            long sourceScale = scaleStream.next();
            if (sourceScale != type.getScale()) {
                data[i] = Decimals.rescale(data[i], (int) sourceScale, type.getScale());
            }
        }
        return new LongArrayOmniBlock(vecAllocator, nextBatchSize, Optional.empty(), data);
    }

    private Block readLongNotNullBlock() throws IOException {
        verify(decimalStream != null);
        verify(scaleStream != null);

        long[] data = new long[nextBatchSize * 2];
        decimalStream.nextLongDecimal(data, nextBatchSize);

        for (int offset = 0; offset < data.length; offset += 2) {
            long sourceScale = scaleStream.next();
            if (sourceScale != type.getScale()) {
                Slice decimal = Slices.wrappedLongArray(data[offset], data[offset + 1]);
                UnscaledDecimal128Arithmetic.rescale(decimal, (int) (type.getScale() - sourceScale),
                    Slices.wrappedLongArray(data, offset, 2));
            }
        }
        return new Int128ArrayOmniBlock(vecAllocator, nextBatchSize, Optional.empty(), data);
    }

    @Override
    protected Block readNullBlock(boolean[] isNull, int nonNullCount) throws IOException {
        Block block;
        if (type.isShort()) {
            block = readShortNullBlock(isNull, nonNullCount);
        } else {
            block = readLongNullBlock(isNull, nonNullCount);
        }
        return block;
    }

    private Block readShortNullBlock(boolean[] isNull, int nonNullCount) throws IOException {
        verify(decimalStream != null);
        verify(scaleStream != null);

        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (nonNullValueTemp.length < minNonNullValueSize) {
            nonNullValueTemp = new long[minNonNullValueSize];
            systemMemoryContext.setBytes(sizeOf(nonNullValueTemp));
        }

        decimalStream.nextShortDecimal(nonNullValueTemp, nonNullCount);

        for (int i = 0; i < nonNullCount; i++) {
            long sourceScale = scaleStream.next();
            if (sourceScale != type.getScale()) {
                nonNullValueTemp[i] = Decimals.rescale(nonNullValueTemp[i], (int) sourceScale, type.getScale());
            }
        }

        long[] result = unpackLongNulls(nonNullValueTemp, isNull);

        return new LongArrayOmniBlock(vecAllocator, nextBatchSize, Optional.of(isNull), result);
    }

    private Block readLongNullBlock(boolean[] isNull, int nonNullCount) throws IOException {
        verify(decimalStream != null);
        verify(scaleStream != null);

        int minTempSize = minNonNullValueSize(nonNullCount) * 2;
        if (nonNullValueTemp.length < minTempSize) {
            nonNullValueTemp = new long[minTempSize];
            systemMemoryContext.setBytes(sizeOf(nonNullValueTemp));
        }

        decimalStream.nextLongDecimal(nonNullValueTemp, nonNullCount);

        // rescale if necessary
        for (int offset = 0; offset < nonNullCount * 2; offset += 2) {
            long sourceScale = scaleStream.next();
            if (sourceScale != type.getScale()) {
                Slice decimal = Slices.wrappedLongArray(nonNullValueTemp[offset], nonNullValueTemp[offset + 1]);
                UnscaledDecimal128Arithmetic.rescale(decimal, (int) (type.getScale() - sourceScale),
                    Slices.wrappedLongArray(nonNullValueTemp, offset, 2));
            }
        }

        long[] result = unpackInt128Nulls(nonNullValueTemp, isNull);

        return new Int128ArrayOmniBlock(vecAllocator, nextBatchSize, Optional.of(isNull), result);
    }
}
