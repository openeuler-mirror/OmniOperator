/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.execution.EmptyMockMetadata;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.VarcharVec;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.Random;

public class TestVariableWidthOmniBlock
{
    private final BlockEncodingSerde blockEncodingSerde = new InternalOmniBlockEncodingSerde(new EmptyMockMetadata());

    @Test
    public void testCreateBlock()
    {
        Block block = buildBlock();
        VarcharVec expected = (VarcharVec) block.getValues();
        assertEquals(block.getPositionCount(), expected.getSize());
        assertBlockEquals(block, expected);
        expected.close();

        // build vec through vec
        Block baseBlock = buildBlock();
        Block newBlock1 = new VariableWidthOmniBlock(baseBlock.getPositionCount(), (VarcharVec) baseBlock.getValues());
        assertBlockEquals(VARCHAR, newBlock1, baseBlock);

        Block newBlock2 = newBlock1.getRegion(2, 2);
        assertEquals(newBlock2.getPositionCount(), 2);
        for (int i = 0; i < newBlock2.getPositionCount(); i++) {
            assertEquals(newBlock2.get(i), newBlock1.get(i + 2));
        }

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, baseBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(actualBlock, (VarcharVec) baseBlock.getValues());
        newBlock1.close();
        newBlock2.close();
        actualBlock.close();
    }

    @Test
    public void testCopyRegion()
    {
        Block baseBlock = buildBlock();
        Block newBlock1 = new VariableWidthOmniBlock(baseBlock.getPositionCount(), (VarcharVec) baseBlock.getValues());
        Block copyRegionBlock = newBlock1.copyRegion(0, newBlock1.getPositionCount());
        assertBlockEquals(copyRegionBlock, (VarcharVec) newBlock1.getValues());
        copyRegionBlock.close();
    }

    @Test
    public void testCopyPosition()
    {
        Block baseBlock = buildBlock();
        Block newBlock1 = new VariableWidthOmniBlock(baseBlock.getPositionCount(), (VarcharVec) baseBlock.getValues());

        int[] positions = {0, 2, 3};
        Block copyRegionBlock = newBlock1.copyPositions(positions, 0, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(copyRegionBlock.getString(i, 0, 0), newBlock1.getString(positions[i], 0, 0));
        }
        newBlock1.close();
        copyRegionBlock.close();
    }

    @Test
    public void testFilter()
    {
        int count = 1024;
        int size = 1000;
        boolean[] valid = new boolean[count];
        Arrays.fill(valid, Boolean.TRUE);
        VariableWidthOmniBlock block = getBlock(count);
        String[] values = new String[block.getPositionCount()];

        BloomFilter bf = getBf(size);
        for (int i = 0; i < block.getPositionCount(); i++) {
            values[i] = block.getString(i, 0, 0);
        }

        boolean[] actualValidPositions = block.filter(bf, valid);
        assertEquals(actualValidPositions, valid);
        block.close();
    }

    @Test
    public void testMultipleValuesWithNull()
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 10);
        blockBuilder.appendNull();
        VARCHAR.writeString(blockBuilder, "alice");
        blockBuilder.appendNull();
        VARCHAR.writeString(blockBuilder, "bob");
        Block block =OperatorUtils.getOffHeapBlock(blockBuilder.build());

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

    private Block buildBlock()
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 4);
        VARCHAR.writeString(blockBuilder, "alice");
        VARCHAR.writeString(blockBuilder, "bob");
        VARCHAR.writeString(blockBuilder, "charlie");
        VARCHAR.writeString(blockBuilder, "dave");
        return OperatorUtils.getOffHeapBlock(blockBuilder.build());
    }

    private VariableWidthOmniBlock getBlock(int count)
    {
        Random rnd = new Random();

        //returns test data
        int[] offsets = new int[count + 1];
        int offset = 0;
        StringBuilder buffer = new StringBuilder();

        for (int i = 0; i < count; i++) {
            offsets[i + 1] = offset;
            String value = "value" + i;
            buffer.append(value);
            offset += value.getBytes().length;
        }
        Slice slice = Slices.wrappedBuffer(buffer.toString().getBytes());

        return new VariableWidthOmniBlock(count, slice, offsets, Optional.empty());
    }

    private BloomFilter getBf(int size)
    {
        Random rnd = new Random();

        BloomFilter bf = new BloomFilter(size, 0.01);
        for (int i = 0; i < 100; i++) {
            bf.test(("value" + rnd.nextLong()).getBytes());
        }
        return bf;
    }

    private static void assertBlockEquals(Block actual, VarcharVec expected)
    {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(new String((byte[]) actual.get(position)), new String(expected.get(position)));
        }
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
        }
    }
}
