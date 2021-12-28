/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package nova.hetu.olk.block;

import static io.prestosql.spi.block.TestingSession.SESSION;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.airlift.slice.DynamicSliceOutput;

import io.prestosql.execution.EmptyMockMetadata;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.type.Type;
import nova.hetu.olk.tool.OperatorUtils;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.VecAllocator;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class TestIntArrayOmniBlock {
    private final BlockEncodingSerde blockEncodingSerde = new InternalOmniBlockEncodingSerde(new EmptyMockMetadata(),
        new TaskId("test"));

    @Test
    public void testMultipleValuesWithNull() {
        BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, 10);
        blockBuilder.appendNull();
        INTEGER.writeLong(blockBuilder, 42);
        blockBuilder.appendNull();
        INTEGER.writeLong(blockBuilder, Integer.MAX_VALUE);
        Block onHeapBlock = blockBuilder.build();
        Block block = OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, onHeapBlock);

        assertTrue(block.isNull(0));
        assertEquals(INTEGER.getLong(block, 1), 42);
        assertTrue(block.isNull(2));
        assertEquals(INTEGER.getLong(block, 3), Integer.MAX_VALUE);

        // build block from vec
        Block NullOmniBlock = new IntArrayOmniBlock(4, (IntVec) block.getValues());
        assertTrue(NullOmniBlock.isNull(0));
        assertEquals(INTEGER.getLong(NullOmniBlock, 1), 42);
        assertTrue(NullOmniBlock.isNull(2));
        assertEquals(INTEGER.getLong(NullOmniBlock, 3), Integer.MAX_VALUE);

        block.close();
    }

    @Test
    public void testBasicFunc() {
        // build vec through vec
        Block baseBlock = buildBlockByBuilder();
        IntArrayOmniBlock intArrayOmniBlock = new IntArrayOmniBlock(baseBlock.getPositionCount(),
            (IntVec) baseBlock.getValues());
        assertBlockEquals(INTEGER, intArrayOmniBlock, baseBlock);
        assertEquals(baseBlock.toString(), intArrayOmniBlock.toString());

        AtomicBoolean isIdentical = new AtomicBoolean(false);
        intArrayOmniBlock.retainedBytesForEachPart((part, size) -> {
            if (size == ((IntVec) baseBlock.getValues()).getCapacityInBytes()) {
                isIdentical.set(true);
            }
        });
        Assert.assertTrue(isIdentical.get());

        Block regionOmniBlock = intArrayOmniBlock.getRegion(2, 2);
        assertEquals(regionOmniBlock.getPositionCount(), 2);
        for (int i = 0; i < regionOmniBlock.getPositionCount(); i++) {
            assertEquals(regionOmniBlock.get(i), intArrayOmniBlock.get(i + 2));
        }

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        blockEncodingSerde.writeBlock(sliceOutput, baseBlock);
        Block actualBlock = blockEncodingSerde.readBlock(sliceOutput.slice().getInput());
        assertBlockEquals(actualBlock, (IntVec) baseBlock.getValues());

        intArrayOmniBlock.close();
        regionOmniBlock.close();
        actualBlock.close();
    }

    @Test
    public void testCopyPosition() {
        Block baseBlock = buildBlockByBuilder();
        Block intArrayOmniBlock = new IntArrayOmniBlock(baseBlock.getPositionCount(), (IntVec) baseBlock.getValues());

        int[] positions = {0, 2, 3};
        Block copyPositionsBlock = intArrayOmniBlock.copyPositions(positions, 0, 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(copyPositionsBlock.getInt(i, 0), intArrayOmniBlock.getInt(positions[i], 0));
        }
        intArrayOmniBlock.close();
        copyPositionsBlock.close();
    }

    @Test
    public void testCopyRegion() {
        Block baseBlock = buildBlockByBuilder();
        Block intArrayOmniBlock = new IntArrayOmniBlock(baseBlock.getPositionCount(), (IntVec) baseBlock.getValues());
        Block copyRegionBlock = intArrayOmniBlock.copyRegion(0, intArrayOmniBlock.getPositionCount());
        assertBlockEquals(copyRegionBlock, (IntVec) intArrayOmniBlock.getValues());

        Block copyNotEqualRegionBlock = intArrayOmniBlock.copyRegion(0, 3);
        assertBlockEquals(copyNotEqualRegionBlock, (IntVec) intArrayOmniBlock.getValues());

        copyNotEqualRegionBlock.close();
    }

    @Test
    public void testInvalidInput() {
        byte[] bytes = {};
        int[] values = {};
        assertThatThrownBy(
            () -> new IntArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, -1, 1, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("arrayOffset is negative");

        assertThatThrownBy(
            () -> new IntArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, -1, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("positionCount is negative");

        assertThatThrownBy(
            () -> new IntArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, 4, bytes, values)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("values length is less than positionCount");

        int[] values2len = new int[6];
        assertThatThrownBy(
            () -> new IntArrayOmniBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, 1, 4, bytes, values2len)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("isNull length is less than positionCount");

        Block baseBlock = buildBlockByBuilder();
        IntVec expected = (IntVec) baseBlock.getValues();
        byte[] bytes2array = {};
        assertThatThrownBy(() -> new IntArrayOmniBlock(-1, 4, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("arrayOffset is negative");

        assertThatThrownBy(() -> new IntArrayOmniBlock(1, -1, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("positionCount is negative");

        assertThatThrownBy(() -> new IntArrayOmniBlock(1, 6, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("values length is less than positionCount");

        assertThatThrownBy(() -> new IntArrayOmniBlock(1, 4, bytes2array, expected)).isInstanceOfAny(
            IllegalArgumentException.class).hasMessageMatching("isNull length is less than positionCount");

        baseBlock.close();
    }

    @Test
    public void testGet() {
        Block baseBlock = buildBlockByBuilder();
        Block intArrayOmniBlock = new IntArrayOmniBlock(baseBlock.getPositionCount(), (IntVec) baseBlock.getValues());
        long expect = 5;
        long expectSizeBytes = 20;
        long expectStates = 4;
        boolean[] position = {true, true, true, true};
        assertEquals(intArrayOmniBlock.getRegionSizeInBytes(0, 1), expect);
        assertEquals(intArrayOmniBlock.getRegionSizeInBytes(0, 4), expectSizeBytes);
        assertEquals(intArrayOmniBlock.getEstimatedDataSizeForStats(0), expectStates);
        assertEquals(intArrayOmniBlock.getPositionsSizeInBytes(position), expectSizeBytes);

        intArrayOmniBlock.close();
    }

    private Block buildBlockByBuilder() {
        BlockBuilder blockBuilder = INTEGER.createBlockBuilder(null, 4);
        INTEGER.writeLong(blockBuilder, 42);
        INTEGER.writeLong(blockBuilder, 43);
        INTEGER.writeLong(blockBuilder, 44);
        INTEGER.writeLong(blockBuilder, 45);
        return OperatorUtils.buildOffHeapBlock(VecAllocator.GLOBAL_VECTOR_ALLOCATOR, blockBuilder.build());
    }

    private static void assertBlockEquals(Block actual, IntVec expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals((Integer) actual.get(position), new Integer(expected.get(position)));
        }
    }

    private static void assertBlockEquals(Type type, Block actual, Block expected) {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position),
                type.getObjectValue(SESSION, expected, position));
        }
    }
}
