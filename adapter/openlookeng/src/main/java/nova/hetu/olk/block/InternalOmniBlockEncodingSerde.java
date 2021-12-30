/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.execution.TaskId;
import io.prestosql.metadata.InternalBlockEncodingSerde;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.block.ArrayBlockEncoding;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.ByteArrayBlockEncoding;
import io.prestosql.spi.block.LazyBlockEncoding;
import io.prestosql.spi.block.RunLengthBlockEncoding;
import io.prestosql.spi.block.ShortArrayBlockEncoding;
import io.prestosql.spi.block.SingleRowBlockEncoding;
import nova.hetu.omniruntime.vector.VecAllocator;
import nova.hetu.omniruntime.vector.VecAllocatorFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The type Internal omni block encoding serde.
 *
 * @since 20210630
 */
public class InternalOmniBlockEncodingSerde extends InternalBlockEncodingSerde {
    private final ConcurrentMap<String, BlockEncoding> blockEncodings = new ConcurrentHashMap<>();

    /**
     * Instantiates a new Internal omni block encoding serde.
     *
     * @param metadata the metadata
     * @param taskId the task id
     */
    public InternalOmniBlockEncodingSerde(Metadata metadata, TaskId taskId) {
        super(metadata);
        VecAllocator vecAllocator = VecAllocatorFactory.get(taskId.getFullId());
        addBlockEncoding(new VariableWidthOmniBlockEncoding(vecAllocator));
        addBlockEncoding(new IntArrayOmniBlockEncoding(vecAllocator));
        addBlockEncoding(new DoubleArrayOmniBlockEncoding(vecAllocator));
        addBlockEncoding(new LongArrayOmniBlockEncoding(vecAllocator));
        addBlockEncoding(new Int128ArrayOmniBlockEncoding(vecAllocator));
        addBlockEncoding(new DictionaryOmniBlockEncoding(vecAllocator));
        addBlockEncoding(new RowOmniBlockEncoding(vecAllocator));

        addBlockEncoding(new ByteArrayBlockEncoding());
        addBlockEncoding(new ShortArrayBlockEncoding());
        addBlockEncoding(new ArrayBlockEncoding());
        addBlockEncoding(new SingleRowBlockEncoding());
        addBlockEncoding(new RunLengthBlockEncoding());
        addBlockEncoding(new LazyBlockEncoding());
    }

    private static String readLengthPrefixedString(SliceInput input) {
        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        return new String(bytes, UTF_8);
    }

    /**
     * Add block encoding.
     *
     * @param blockEncoding the block encoding
     */
    public void addBlockEncoding(BlockEncoding blockEncoding) {
        requireNonNull(blockEncoding, "blockEncoding is null");
        BlockEncoding existingEntry = blockEncodings.putIfAbsent(blockEncoding.getName(), blockEncoding);
        checkArgument(existingEntry == null, "Encoding already registered: %s", blockEncoding.getName());
    }

    @Override
    public Block readBlock(SliceInput input) {
        // read the encoding name
        String encodingName = readLengthPrefixedString(input);

        // look up the encoding factory
        BlockEncoding blockEncoding = getBlockEncoding(encodingName);

        return blockEncoding.readBlock(this, input);
    }

    @Override
    public void writeBlock(SliceOutput output, Block block) {
        while (true) {
            // get the encoding name
            String encodingName = block.getEncodingName();

            // look up the BlockEncoding
            BlockEncoding blockEncoding = getBlockEncoding(encodingName);

            // see if a replacement block should be written instead
            Optional<Block> replacementBlock = blockEncoding.replacementBlockForWrite(block);
            if (replacementBlock.isPresent()) {
                block = replacementBlock.get();
                continue;
            }

            // write the name to the output
            writeLengthPrefixedString(output, encodingName);

            // write the block to the output
            blockEncoding.writeBlock(this, output, block);

            break;
        }
    }

    /**
     * Gets block encoding.
     *
     * @param encodingName the encoding name
     * @return the block encoding
     */
    public BlockEncoding getBlockEncoding(String encodingName) {
        BlockEncoding blockEncoding = blockEncodings.get(encodingName);
        checkArgument(blockEncoding != null, "Unknown block encoding: %s", encodingName);
        return blockEncoding;
    }
}
