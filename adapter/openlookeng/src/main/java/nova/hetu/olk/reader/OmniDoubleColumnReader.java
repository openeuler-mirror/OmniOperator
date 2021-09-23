/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.reader;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.orc.reader.ReaderUtils.minNonNullValueSize;
import static io.prestosql.orc.reader.ReaderUtils.unpackDoubleNulls;
import static nova.hetu.olk.tool.VecAllocatorHelper.getVecAllocatorFromExtensionProperties;

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.reader.DoubleColumnReader;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import java.util.Map;
import nova.hetu.olk.block.DoubleArrayOmniBlock;

import java.io.IOException;
import java.util.Optional;
import nova.hetu.omniruntime.vector.VecAllocator;

/**
 * The type Omni double column reader.
 *
 * @since 20210630
 */
public class OmniDoubleColumnReader extends DoubleColumnReader {
    private final VecAllocator vecAllocator;

    /**
     * The Non null value temp.
     */
    protected double[] nonNullValueTemp = new double[0];

    /**
     * Instantiates a new Omni double column reader.
     *
     * @param type the type
     * @param column the column
     * @param systemMemoryContext the system memory context
     * @throws OrcCorruptionException the orc corruption exception
     */
    public OmniDoubleColumnReader(Type type, OrcColumn column, LocalMemoryContext systemMemoryContext, Map<String, String> extensionColumnReadersProperties)
        throws OrcCorruptionException {
        super(type, column, systemMemoryContext);
        vecAllocator = getVecAllocatorFromExtensionProperties(extensionColumnReadersProperties);
    }

    @Override
    public Block readNonNullBlock() throws IOException {
        verify(dataStream != null);
        double[] values = new double[nextBatchSize];
        dataStream.next(values, nextBatchSize);
        return new DoubleArrayOmniBlock(vecAllocator, nextBatchSize, Optional.empty(), values);
    }

    @Override
    public Block readNullBlock(boolean[] isNull, int nonNullCount) throws IOException {
        verify(dataStream != null);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (nonNullValueTemp.length < minNonNullValueSize) {
            nonNullValueTemp = new double[minNonNullValueSize];
            systemMemoryContext.setBytes(sizeOf(nonNullValueTemp));
        }

        dataStream.next(nonNullValueTemp, nonNullCount);

        double[] result = unpackDoubleNulls(nonNullValueTemp, isNull);

        return new DoubleArrayOmniBlock(vecAllocator, isNull.length, Optional.of(isNull), result);
    }
}
