package nova.hetu.olk.block;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.execution.EmptyMockMetadata;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.VecAllocator;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRowOmniBlock {
    private final BlockEncodingSerde blockEncodingSerde = new InternalOmniBlockEncodingSerde(new EmptyMockMetadata(),
        new TaskId("test"));

    @Test
    public void testBasicFunc() {
        // build vec through vec
        RowOmniBlock baseBlock = (RowOmniBlock) buildBlockByBuilder();
        RowOmniBlock loadedBlock = (RowOmniBlock) baseBlock.getLoadedBlock();
        Block varcharBlock = baseBlock.getRawFieldBlocks()[0];
        Block varcharBlock2 = loadedBlock.getRawFieldBlocks()[0];
        assertBlockEquals(VARCHAR, varcharBlock, varcharBlock2);

        int[] fieldBlockOffsets = {0, 1};
        long sizes = sizeOf(fieldBlockOffsets);
        AtomicBoolean isIdentical = new AtomicBoolean(false);
        loadedBlock.retainedBytesForEachPart((part, size) -> {
            if (size.equals(sizes)) {
                isIdentical.set(true);
            }
        });
        Assert.assertTrue(isIdentical.get());

        Block rawFieldBlock = loadedBlock.getRawFieldBlocks()[0].getRegion(2, 2);
        assertEquals(rawFieldBlock.getPositionCount(), 2);
        for (int i = 0; i < rawFieldBlock.getPositionCount(); i++) {
            assertEquals(rawFieldBlock.get(i), varcharBlock2.get(i + 2));
        }

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, baseBlock);
        RowOmniBlock actualBlock = (RowOmniBlock) blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(actualBlock.getRawFieldBlocks()[0],
            (VarcharVec) baseBlock.getRawFieldBlocks()[0].getValues());

        loadedBlock.close();
        rawFieldBlock.close();
        actualBlock.close();
    }

    @Test
    public void testMultipleValuesWithNull() {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 10);
        blockBuilder.appendNull();
        VARCHAR.writeString(blockBuilder, "alice");
        blockBuilder.appendNull();
        VARCHAR.writeString(blockBuilder, "bob");
        Block block = OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, blockBuilder.build());

        assertTrue(block.isNull(0));
        assertEquals(VARCHAR.getObjectValue(SESSION, block, 1), "alice");
        assertTrue(block.isNull(2));
        assertEquals(VARCHAR.getObjectValue(SESSION, block, 3), "bob");

        // build block from vec
        Block block1 = new VariableWidthOmniBlock(4, (VarcharVec) block.getValues());
        assertTrue(block1.isNull(0));
        assertEquals(VARCHAR.getObjectValue(SESSION, block1, 1), "alice");
        assertTrue(block1.isNull(2));
        assertEquals(VARCHAR.getObjectValue(SESSION, block1, 3), "bob");
        block.close();
    }

    @Test
    public void testInvalidInput() {
        byte[] rowIsNull = {};
        int[] fieldBlockOffsets = {};
        Block[] fieldBlocks = {};
        assertThatThrownBy(() -> new RowOmniBlock(-1, 4, rowIsNull, fieldBlockOffsets, fieldBlocks)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("Number of fields in RowBlock must be positive");
    }

    private Block buildBlockByBuilder() {
        BlockBuilder RowBlockBuilder = VARCHAR.createBlockBuilder(null, 4);
        VARCHAR.writeString(RowBlockBuilder, "alice");
        VARCHAR.writeString(RowBlockBuilder, "bob");
        VARCHAR.writeString(RowBlockBuilder, "charlie");
        VARCHAR.writeString(RowBlockBuilder, "dave");
        Block varCharBlock = OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR,
            RowBlockBuilder.build());

        byte[] rowIsNull = new byte[1];
        int[] fieldBlockOffsets = {0, 1};
        Block[] blocks = new Block[1];
        blocks[0] = varCharBlock;

        return new RowOmniBlock(0, 1, rowIsNull, fieldBlockOffsets, blocks);
    }

    private static void assertBlockEquals(Block actual, VarcharVec expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(new String((byte[]) actual.get(position)), new String(expected.get(position)));
        }
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position),
                type.getObjectValue(SESSION, expected, position));
        }
    }
}
