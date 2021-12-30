/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.reader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.prestosql.orc.reader.ReaderUtils.convertLengthVectorToOffsetVector;
import static io.prestosql.orc.reader.SliceColumnReader.computeTruncatedLength;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static nova.hetu.olk.tool.ReaderUtils.unpackLengthNulls;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.reader.SliceDirectColumnReader;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import nova.hetu.olk.block.VariableWidthOmniBlock;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecAllocator;

import java.io.IOException;
import java.util.Optional;

/**
 * The type Omni slice direct column reader.
 *
 * @since 20210630
 */
public class OmniSliceDirectColumnReader extends SliceDirectColumnReader {
    private final VecAllocator vecAllocator;

    /**
     * Instantiates a new Omni slice direct column reader.
     *
     * @param column the column
     * @param maxCodePointCount the max code point count
     * @param isCharType the is char type
     */
    public OmniSliceDirectColumnReader(VecAllocator vecAllocator, OrcColumn column, int maxCodePointCount,
            boolean isCharType) {
        super(column, maxCodePointCount, isCharType);
        this.vecAllocator = vecAllocator;
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
                if (lengthStream == null) {
                    throw new OrcCorruptionException(column.getOrcDataSourceId(),
                            "Value is not null but length stream is missing");
                }
                long dataSkipSize = lengthStream.sum(readOffset);
                if (dataSkipSize > 0) {
                    if (dataStream == null) {
                        throw new OrcCorruptionException(column.getOrcDataSourceId(),
                                "Value is not null but data stream is missing");
                    }
                    dataStream.skip(dataSkipSize);
                }
            }
        }

        if (lengthStream == null) {
            if (presentStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(),
                        "Value is null but present stream is missing");
            }
            presentStream.skip(nextBatchSize);
            Block nullValueBlock = readAllNullsBlock();
            readOffset = 0;
            nextBatchSize = 0;
            return nullValueBlock;
        }

        // create new isNullVector and offsetVector for VariableWidthBlock
        byte[] isNullVector = null;

        // We will use the offsetVector as the buffer to read the length values from lengthStream,
        // and the length values will be converted in-place to an offset vector.
        int[] offsetVector = new int[nextBatchSize + 1];

        if (presentStream == null) {
            lengthStream.next(offsetVector, nextBatchSize);
        } else {
            isNullVector = new byte[nextBatchSize];
            int nullCount = presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (nullCount == nextBatchSize) {
                // all nulls
                Block nullValueBlock = readAllNullsBlock();
                readOffset = 0;
                nextBatchSize = 0;
                return nullValueBlock;
            }

            if (lengthStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(),
                        "Value is not null but length stream is missing");
            }
            if (nullCount == 0) {
                isNullVector = null;
                lengthStream.next(offsetVector, nextBatchSize);
            } else {
                lengthStream.next(offsetVector, nextBatchSize - nullCount);
                unpackLengthNulls(offsetVector, isNullVector, nextBatchSize - nullCount);
            }
        }

        // Calculate the total length for all entries. Note that the values in the
        // offsetVector are still length values now.
        long totalLength = 0;
        for (int i = 0; i < nextBatchSize; i++) {
            totalLength += offsetVector[i];
        }

        int currentBatchSize = nextBatchSize;
        readOffset = 0;
        nextBatchSize = 0;
        if (totalLength == 0) {
            return new VariableWidthOmniBlock(vecAllocator, currentBatchSize, EMPTY_SLICE, offsetVector,
                    Optional.ofNullable(isNullVector));
        }
        if (totalLength > ONE_GIGABYTE) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format(
                    "Values in column \"%s\" are too large to process for Presto. %s column values are larger than 1GB [%s]",
                    column.getPath(), nextBatchSize, column.getOrcDataSourceId()));
        }
        if (dataStream == null) {
            throw new OrcCorruptionException(column.getOrcDataSourceId(),
                    "Value is not null but data stream is missing");
        }

        // allocate enough space to read
        byte[] data = new byte[toIntExact(totalLength)];
        Slice slice = Slices.wrappedBuffer(data);

        if (maxCodePointCount < 0) {
            // unbounded, simply read all data in on shot
            dataStream.next(data, 0, data.length);
            convertLengthVectorToOffsetVector(offsetVector);
        } else {
            // We do the following operations together in the for loop:
            // * truncate strings
            // * convert original length values in offsetVector into truncated offset values
            int currentLength = offsetVector[0];
            offsetVector[0] = 0;
            for (int i = 1; i <= currentBatchSize; i++) {
                int nextLength = offsetVector[i];
                if (isNullVector != null && isNullVector[i - 1] == Vec.NULL) {
                    checkState(currentLength == 0,
                            "Corruption in slice direct stream: length is non-zero for null entry");
                    offsetVector[i] = offsetVector[i - 1];
                    currentLength = nextLength;
                    continue;
                }
                int offset = offsetVector[i - 1];

                // read data without truncation
                dataStream.next(data, offset, offset + currentLength);

                // adjust offsetVector with truncated length
                int truncatedLength = computeTruncatedLength(slice, offset, currentLength, maxCodePointCount,
                        isCharType);
                verify(truncatedLength >= 0);
                offsetVector[i] = offset + truncatedLength;

                currentLength = nextLength;
            }
        }

        // this can lead to over-retention but unlikely to happen given truncation
        // rarely happens
        return new VariableWidthOmniBlock(vecAllocator, currentBatchSize, slice, offsetVector,
                Optional.ofNullable(isNullVector));
    }

    private RunLengthEncodedBlock readAllNullsBlock() {
        return new RunLengthEncodedBlock(
                new VariableWidthOmniBlock(vecAllocator, 1, EMPTY_SLICE, new int[2], Optional.of(new byte[]{Vec.NULL})),
                nextBatchSize);
    }
}
